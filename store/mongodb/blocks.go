package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// UpsertBlockHeaderSeen implements store.Store in one upsert: block_height
// and status are overwritten, orphaned_at/reconciled_at cleared, and
// header_seen_at written only on insert so a re-arrival keeps the original
// observation and every later milestone.
func (s *Store) UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error {
	update := doc(
		kv(opSet, doc(kv(fBlockHeight, heightToInt64(blockHeight)), kv(fStatus, string(models.BlockStatusActive)))),
		kv(opUnset, doc(kv(fOrphanedAt, ""), kv(fReconciledAt, ""))),
		kv(opSetOnInsert, doc(kv(fHeaderSeenAt, msTrunc(seenAt)))),
	)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.blocks.UpdateOne(octx, idFilter(blockHash), update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert block header seen %s: %w", blockHash, err)
	}
	return nil
}

// markBlockMilestone stamps one milestone field, inserting the row with
// header_seen_at synthesized from the milestone when the callback beat the
// header. On an existing row only the milestone field changes.
func (s *Store) markBlockMilestone(ctx context.Context, blockHash string, blockHeight uint64, at time.Time, field string) error {
	at = msTrunc(at)
	update := doc(
		kv(opSet, doc(kv(field, at))),
		kv(opSetOnInsert, doc(kv(fBlockHeight, heightToInt64(blockHeight)), kv(fHeaderSeenAt, at), kv(fStatus, string(models.BlockStatusActive)))),
	)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.blocks.UpdateOne(octx, idFilter(blockHash), update, options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("mark block %s %s: %w", field, blockHash, err)
	}
	return nil
}

// MarkBlockProcessed implements store.Store.
func (s *Store) MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error {
	return s.markBlockMilestone(ctx, blockHash, blockHeight, processedAt, fProcessedAt)
}

// MarkBlockBUMPBuilt implements store.Store.
func (s *Store) MarkBlockBUMPBuilt(ctx context.Context, blockHash string, blockHeight uint64, builtAt time.Time) error {
	return s.markBlockMilestone(ctx, blockHash, blockHeight, builtAt, fBUMPBuiltAt)
}

// MarkBlocksOrphaned implements store.Store; hashes without a row are skipped.
func (s *Store) MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) error {
	if len(blockHashes) == 0 {
		return nil
	}
	update := doc(kv(opSet, doc(kv(fStatus, string(models.BlockStatusOrphaned)), kv(fOrphanedAt, msTrunc(orphanedAt)))))
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.blocks.UpdateMany(octx, doc(kv(fID, doc(kv(opIn, blockHashes)))), update); err != nil {
		return fmt.Errorf("mark blocks orphaned: %w", err)
	}
	return nil
}

// MarkBlockReconciled implements store.Store; a missing row is a no-op.
func (s *Store) MarkBlockReconciled(ctx context.Context, blockHash string, at time.Time) error {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.blocks.UpdateOne(octx, idFilter(blockHash), doc(kv(opSet, doc(kv(fReconciledAt, msTrunc(at)))))); err != nil {
		return fmt.Errorf("mark block reconciled %s: %w", blockHash, err)
	}
	return nil
}

// MarkBlocksParked implements store.Store; only active rows park.
func (s *Store) MarkBlocksParked(ctx context.Context, blockHashes []string) error {
	if len(blockHashes) == 0 {
		return nil
	}
	filter := doc(kv(fID, doc(kv(opIn, blockHashes))), kv(fStatus, string(models.BlockStatusActive)))
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.blocks.UpdateMany(octx, filter, doc(kv(opSet, doc(kv(fStatus, string(models.BlockStatusParked)))))); err != nil {
		return fmt.Errorf("mark blocks parked: %w", err)
	}
	return nil
}

// GetBlockProcessingStatus implements store.Store.
func (s *Store) GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var d blockProcessingDoc
	err := s.blocks.FindOne(octx, idFilter(blockHash)).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get block processing %s: %w", blockHash, err)
	}
	return d.toModel(), nil
}

// listBlocks runs a bounded block_processing query and maps the rows.
func (s *Store) listBlocks(ctx context.Context, filter bson.D, opts *options.FindOptionsBuilder) ([]*models.BlockProcessingStatus, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.blocks.Find(qctx, filter, opts)
	if err != nil {
		return nil, err
	}
	var docs []blockProcessingDoc
	if err := cur.All(qctx, &docs); err != nil {
		return nil, err
	}
	out := make([]*models.BlockProcessingStatus, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.toModel())
	}
	return out, nil
}

// ListBlockProcessingStatus implements store.Store: height-descending keyset
// page with _id as the tiebreaker so paging is deterministic.
func (s *Store) ListBlockProcessingStatus(ctx context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if limit <= 0 {
		return nil, nil
	}
	filter := doc()
	if beforeHeight > 0 {
		filter = doc(kv(fBlockHeight, doc(kv(opLt, heightToInt64(beforeHeight)))))
	}
	out, err := s.listBlocks(ctx, filter, options.Find().SetSort(doc(kv(fBlockHeight, -1), kv(fID, 1))).SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("list block processing: %w", err)
	}
	return out, nil
}

// GetActiveTipBlockHeight implements store.Store.
func (s *Store) GetActiveTipBlockHeight(ctx context.Context) (uint64, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var d struct {
		BlockHeight int64 `bson:"block_height"`
	}
	err := s.blocks.FindOne(octx, doc(kv(fStatus, string(models.BlockStatusActive))),
		options.FindOne().SetSort(doc(kv(fBlockHeight, -1))).SetProjection(doc(kv(fBlockHeight, 1)))).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get active tip height: %w", err)
	}
	return heightFromInt64(d.BlockHeight), nil
}

// ListStaleBlockProcessingStatus implements store.Store: active rows with no
// processed_at, seen before olderThan, at or above minHeight, oldest first.
func (s *Store) ListStaleBlockProcessingStatus(ctx context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if limit <= 0 {
		return nil, nil
	}
	filter := doc(
		kv(fStatus, string(models.BlockStatusActive)),
		kv(fProcessedAt, doc(kv(opExists, false))),
		kv(fHeaderSeenAt, doc(kv(opLt, msTrunc(olderThan)))),
		kv(fBlockHeight, doc(kv(opGte, heightToInt64(minHeight)))),
	)
	out, err := s.listBlocks(ctx, filter, options.Find().
		SetSort(doc(kv(fHeaderSeenAt, 1))).SetLimit(int64(limit)).SetHint(idxBPStaleSeen))
	if err != nil {
		return nil, fmt.Errorf("list stale block processing: %w", err)
	}
	return out, nil
}

// ListOrphanedBlocksToReconcile implements store.Store as one aggregation
// that orders and limits on the server, like the Postgres query: orphaned
// rows not yet reconciled, re-anchorable first (an active row at the same
// height whose block has a BUMP manifest), then oldest orphaned_at with
// missing values last. Only `limit` rows cross the wire, so a large reorg
// backlog costs the server a bounded sort rather than the process a
// materialized copy of the whole queue.
func (s *Store) ListOrphanedBlocksToReconcile(ctx context.Context, limit int) ([]*models.BlockProcessingStatus, error) {
	if limit <= 0 {
		return nil, errors.New("limit must be > 0")
	}
	// Sentinel for "no orphaned_at": far enough out to sort after any real
	// value, so absent stamps land last (Postgres NULLS LAST) instead of
	// first as MongoDB's native null ordering would put them.
	nullsLast := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	pipeline := mongo.Pipeline{
		doc(kv(opMatch, doc(kv(fStatus, string(models.BlockStatusOrphaned)), kv(fReconciledAt, doc(kv(opExists, false)))))),
		// The active (canonical) row at the orphan's height, if any.
		doc(kv("$lookup", doc(
			kv("from", collBlockProcessing),
			kv("let", doc(kv("h", "$"+fBlockHeight))),
			kv("pipeline", bson.A{
				doc(kv(opMatch, doc(kv("$expr", doc(kv("$and", bson.A{
					doc(kv("$eq", bson.A{"$" + fStatus, string(models.BlockStatusActive)})),
					doc(kv("$eq", bson.A{"$" + fBlockHeight, "$$h"})),
				})))))),
				doc(kv("$project", doc(kv(fID, 1)))),
			}),
			kv("as", "active"),
		))),
		// Whether that canonical block has a stored compound BUMP.
		doc(kv("$lookup", doc(
			kv("from", collBumpManifests),
			kv("localField", "active._id"),
			kv("foreignField", fID),
			kv("as", "bumps"),
		))),
		doc(kv("$addFields", doc(
			kv("reanchorable", doc(kv("$gt", bson.A{doc(kv("$size", "$bumps")), 0}))),
			kv("orphaned_sort", doc(kv("$ifNull", bson.A{"$" + fOrphanedAt, nullsLast}))),
		))),
		doc(kv("$sort", doc(kv("reanchorable", -1), kv("orphaned_sort", 1), kv(fID, 1)))),
		doc(kv("$limit", int64(limit))),
		doc(kv("$project", doc(kv("active", 0), kv("bumps", 0), kv("reanchorable", 0), kv("orphaned_sort", 0)))),
	}
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.blocks.Aggregate(qctx, pipeline, options.Aggregate().SetHint(idxBPOrphaned).SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("list orphaned blocks to reconcile: %w", err)
	}
	var docs []blockProcessingDoc
	if err := cur.All(qctx, &docs); err != nil {
		return nil, fmt.Errorf("list orphaned blocks to reconcile: %w", err)
	}
	out := make([]*models.BlockProcessingStatus, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.toModel())
	}
	return out, nil
}
