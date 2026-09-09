package mongodb

import (
	"context"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/models"
)

// Index names. Hints reference these, so a rename here must be paired with
// the hint sites (grep the constant).
const (
	idxTxStatusHeight   = "tx_status_block_height_id"
	idxTxStatusTS       = "tx_status_timestamp"
	idxTxBlockHash      = "tx_block_hash"
	idxTxTimestamp      = "tx_timestamp"
	idxTxRetryReady     = "tx_retry_ready"
	idxSubTxIDToken     = "sub_txid_token" //nolint:gosec // an index name, not a credential
	idxSubTokenTxID     = "sub_token_txid" //nolint:gosec // an index name, not a credential
	idxSubRetryReady    = "sub_retry_ready"
	idxBPHeightDesc     = "bp_height_desc"
	idxBPStatusHeight   = "bp_status_height"
	idxBPStaleSeen      = "bp_stale_seen"
	idxBPOrphaned       = "bp_orphaned_unreconciled"
	idxLeaseExpires     = "lease_expires_ttl"
	idxDHNetwork        = "dh_network"
	idxDHLastSeen       = "dh_last_seen"
	idxPPNetwork        = "pp_network"
	idxPPLastSeen       = "pp_last_seen"
	idxBumpsBlockHash   = "bumps_block_hash"
	idxStumpsBlockHash  = "stumps_block_hash_subtree"
	idxGridFSFiles      = "gridfs_filename_uploadDate"
	idxGridFSChunks     = "gridfs_files_id_n"
	partialFilterOption = "partialFilterExpression"
)

// EnsureIndexes provisions every secondary index the queries in this package
// rely on. createIndexes is idempotent for an identical spec and fails
// loudly (IndexKeySpecsConflict) when a named index exists with a different
// spec, which is the behaviour we want: a silently coexisting stale index
// would let a hint point at the wrong plan.
//
// The index set mirrors store/postgres/schema.sql one-for-one; partial
// indexes stand in for Postgres' partial indexes where MongoDB can express
// the predicate (equality, $gt, $exists:true). `IS NULL` predicates cannot be
// partial-index conditions, so the block_processing indexes are compound
// over the nullable field instead — a missing field indexes as null, so the
// query {status:"active", processed_at:{$exists:false}} still gets tight
// bounds. GridFS's own indexes are created explicitly too: the driver only
// auto-creates them when the files collection is empty at first write, which
// a restore or migration can defeat.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	type spec struct {
		coll   *mongo.Collection
		models []mongo.IndexModel
	}
	specs := []spec{
		{s.tx, []mongo.IndexModel{
			idx(idxTxStatusHeight, bson.D{{Key: fStatus, Value: 1}, {Key: fBlockHeight, Value: 1}, {Key: fID, Value: 1}}),
			idx(idxTxStatusTS, bson.D{{Key: fStatus, Value: 1}, {Key: fTimestamp, Value: 1}}),
			idxPartial(idxTxBlockHash, bson.D{{Key: fBlockHash, Value: 1}},
				bson.D{{Key: fBlockHash, Value: bson.D{{Key: opExists, Value: true}}}}),
			idx(idxTxTimestamp, bson.D{{Key: fTimestamp, Value: 1}}),
			idxPartial(idxTxRetryReady, bson.D{{Key: fNextRetryAt, Value: 1}},
				bson.D{{Key: fStatus, Value: string(models.StatusPendingRetry)}}),
		}},
		{s.subs, []mongo.IndexModel{
			idx(idxSubTxIDToken, bson.D{{Key: fTxID, Value: 1}, {Key: fCallbackToken, Value: 1}}),
			idx(idxSubTokenTxID, bson.D{{Key: fCallbackToken, Value: 1}, {Key: fTxID, Value: 1}}),
			idxPartial(idxSubRetryReady, bson.D{{Key: fNextRetryAt, Value: 1}},
				bson.D{{Key: fRetryCount, Value: bson.D{{Key: opGt, Value: 0}}}}),
		}},
		{s.blocks, []mongo.IndexModel{
			idx(idxBPHeightDesc, bson.D{{Key: fBlockHeight, Value: -1}, {Key: fID, Value: 1}}),
			idx(idxBPStatusHeight, bson.D{{Key: fStatus, Value: 1}, {Key: fBlockHeight, Value: -1}}),
			idx(idxBPStaleSeen, bson.D{{Key: fStatus, Value: 1}, {Key: fProcessedAt, Value: 1}, {Key: fHeaderSeenAt, Value: 1}}),
			idx(idxBPOrphaned, bson.D{{Key: fStatus, Value: 1}, {Key: fReconciledAt, Value: 1}, {Key: fOrphanedAt, Value: 1}}),
		}},
		{s.leases, []mongo.IndexModel{{
			Keys:    bson.D{{Key: fExpiresAt, Value: 1}},
			Options: options.Index().SetName(idxLeaseExpires).SetExpireAfterSeconds(0),
		}}},
		{s.datahubs, []mongo.IndexModel{
			idx(idxDHNetwork, bson.D{{Key: fNetwork, Value: 1}}),
			idx(idxDHLastSeen, bson.D{{Key: fLastSeen, Value: 1}}),
		}},
		{s.peers, []mongo.IndexModel{
			idx(idxPPNetwork, bson.D{{Key: fNetwork, Value: 1}}),
			idx(idxPPLastSeen, bson.D{{Key: fLastSeen, Value: 1}}),
		}},
		{s.bumps.GetFilesCollection(), append(gridfsFilesIndexes(),
			idx(idxBumpsBlockHash, bson.D{{Key: fMetaBlockHash, Value: 1}, {Key: fUploadDate, Value: -1}}))},
		{s.bumps.GetChunksCollection(), gridfsChunksIndexes()},
		{s.stumps.GetFilesCollection(), append(gridfsFilesIndexes(),
			idx(idxStumpsBlockHash, bson.D{{Key: fMetaBlockHash, Value: 1}, {Key: fMetaSubtreeIndex, Value: 1}, {Key: fUploadDate, Value: -1}}))},
		{s.stumps.GetChunksCollection(), gridfsChunksIndexes()},
	}
	// Collections are independent, so provision them concurrently: eleven
	// sequential createIndexes round-trips are the dominant cost of a boot
	// against an already-indexed database.
	errs := make(chan error, len(specs))
	var wg sync.WaitGroup
	for _, sp := range specs {
		wg.Add(1)
		go func(sp spec) {
			defer wg.Done()
			if _, err := sp.coll.Indexes().CreateMany(ctx, sp.models); err != nil {
				errs <- fmt.Errorf("mongodb: create indexes on %s: %w", sp.coll.Name(), err)
			}
		}(sp)
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func idx(name string, keys bson.D) mongo.IndexModel {
	return mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name)}
}

func idxPartial(name string, keys, filter bson.D) mongo.IndexModel {
	return mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name).SetPartialFilterExpression(filter)}
}

// gridfsFilesIndexes is the {filename, uploadDate} index the GridFS spec
// requires on <bucket>.files.
func gridfsFilesIndexes() []mongo.IndexModel {
	return []mongo.IndexModel{idx(idxGridFSFiles, bson.D{{Key: fFilename, Value: 1}, {Key: fUploadDate, Value: 1}})}
}

// gridfsChunksIndexes is the unique {files_id, n} index the GridFS spec
// requires on <bucket>.chunks.
func gridfsChunksIndexes() []mongo.IndexModel {
	return []mongo.IndexModel{{
		Keys:    bson.D{{Key: fFilesID, Value: 1}, {Key: fChunkN, Value: 1}},
		Options: options.Index().SetName(idxGridFSChunks).SetUnique(true),
	}}
}
