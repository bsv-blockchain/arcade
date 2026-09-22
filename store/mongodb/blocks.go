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
// observation and every later milestone. Two concurrent first upserts for a
// hash can collide on _id; the loser retries and updates the winner's row.
func (s *Store) UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error {
	update := doc(
		kv(opSet, doc(kv(fBlockHeight, heightToInt64(blockHeight)), kv(fStatus, string(models.BlockStatusActive)))),
		kv(opUnset, doc(kv(fOrphanedAt, ""), kv(fOrphanedGen, ""), kv(fReconciledAt, ""))),
		kv(opSetOnInsert, doc(kv(fHeaderSeenAt, msTrunc(seenAt)))),
	)
	err := s.withDupKeyRetry(ctx, func(octx context.Context) error {
		_, err := s.blocks.UpdateOne(octx, idFilter(blockHash), update, options.UpdateOne().SetUpsert(true))
		return err
	})
	if err != nil {
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
	err := s.withDupKeyRetry(ctx, func(octx context.Context) error {
		_, err := s.blocks.UpdateOne(octx, idFilter(blockHash), update, options.UpdateOne().SetUpsert(true))
		return err
	})
	if err != nil {
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
//
// It clears reconciled_at as well as setting the status. A block that was
// orphaned, reconciled, resurrected and orphaned again would otherwise carry
// its old reconciled_at into the new orphaning and never re-enter
// ListOrphanedBlocksToReconcile — the issue #339 queue trap. Each orphaning
// is a new reconciliation job.
//
// The returned count is applied status TRANSITIONS and comes from the write
// itself: the first UpdateMany is filtered on status != orphaned, and the
// server evaluates that filter atomically with the update per document, so
// a row matches — and is counted — exactly when THIS call flipped it. A row
// a concurrent writer orphaned first does not match and is never
// double-counted.
//
// The second UpdateMany refreshes the generation and clears the stamp on
// rows that were already orphaned; that is not a transition and its count is
// discarded. It is guarded to move the generation FORWARD only (orphaned_gen
// below this call's), because the two statements are not one atomic step:
// between them another writer can reactivate a row this call just
// transitioned and orphan it again with a newer generation, and an
// unguarded refresh would then overwrite that newer generation with this
// call's older one — letting a reconciler holding the older token pass the
// CAS for a generation it never processed. The single-write backends cannot
// interleave like that, so only this one needs the guard.
func (s *Store) MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) (int, error) {
	if len(blockHashes) == 0 {
		return 0, nil
	}
	gen := orphanedAt.UnixNano()
	update := doc(
		kv(opSet, doc(
			kv(fStatus, string(models.BlockStatusOrphaned)),
			kv(fOrphanedAt, msTrunc(orphanedAt)),
			kv(fOrphanedGen, gen),
		)),
		kv(opUnset, doc(kv(fReconciledAt, ""))),
	)
	transitions, err := s.updateBlocksIn(ctx, blockHashes,
		doc(kv(fStatus, doc(kv(opNe, string(models.BlockStatusOrphaned))))), update, "mark blocks orphaned")
	if err != nil {
		return int(transitions), err
	}
	refresh := doc(
		kv(fStatus, string(models.BlockStatusOrphaned)),
		kv(opOr, bson.A{
			doc(kv(fOrphanedGen, doc(kv(opLt, gen)))),
			doc(kv(fOrphanedGen, doc(kv(opExists, false)))), // written before the field existed
		}),
	)
	if _, err := s.updateBlocksIn(ctx, blockHashes, refresh, update, "refresh orphaned generation"); err != nil {
		return int(transitions), err
	}
	return int(transitions), nil
}

// orphanGenerationFilter matches the orphan generation a caller read back:
// orphaned_gen at full precision, or — for a row written before that field
// existed, whose model carried the millisecond orphaned_at — the datetime.
// A zero orphanedAt matches any generation (status check only).
func orphanGenerationFilter(orphanedAt time.Time) bson.D {
	if orphanedAt.IsZero() {
		return doc()
	}
	return doc(kv(opOr, bson.A{
		doc(kv(fOrphanedGen, orphanedAt.UnixNano())),
		doc(kv(fOrphanedGen, doc(kv(opExists, false))), kv(fOrphanedAt, msTrunc(orphanedAt))),
	}))
}

// updateBlocksIn applies one update to every row named in blockHashes, in
// batch_size chunks under the bulk-write deadline, and returns how many
// documents the update actually modified.
//
// The chunking is not cosmetic: an $in list is one command, and the driver
// does not split it, so a deep reorg's hash list would eventually exceed the
// 16 MB command limit and fail the whole call rather than degrade. Chunking
// also makes the caller's slice the only unbounded thing in play — each
// command is bounded by the same batch_size knob the rest of this package's
// multi-row writes honour. queryCtx, not opCtx: these are bulk writes whose
// cost scales with the chunk, and op_timeout_ms is the point-operation budget.
// extra, when non-nil, is ANDed onto the _id predicate.
func (s *Store) updateBlocksIn(ctx context.Context, blockHashes []string, extra, update bson.D, what string) (int64, error) {
	var modified int64
	for _, chunk := range chunks(dedupe(blockHashes), s.batchSize) {
		filter := doc(kv(fID, doc(kv(opIn, chunk))))
		filter = append(filter, extra...)
		qctx, cancel := s.queryCtx(ctx)
		res, err := s.blocks.UpdateMany(qctx, filter, update)
		cancel()
		if err != nil {
			return modified, fmt.Errorf("%s: %w", what, err)
		}
		modified += res.ModifiedCount
	}
	return modified, nil
}

// MarkBlockReconciled implements store.Store as a compare-and-set on the
// orphan generation: the filter requires status == orphaned and, unless
// orphanedAt is zero, the orphan generation (orphaned_gen, see
// orphanGenerationFilter) equals orphanedAt, so a row the block-status
// tracker reactivated — or orphaned again with a newer generation — while
// the reconciler was working is left untouched, as is a missing row (issue
// #339). The server evaluates filter and update atomically per document, so
// no version compare is needed. Returns whether the stamp applied.
func (s *Store) MarkBlockReconciled(ctx context.Context, blockHash string, orphanedAt, at time.Time) (bool, error) {
	filter := doc(kv(fID, blockHash), kv(fStatus, string(models.BlockStatusOrphaned)))
	filter = append(filter, orphanGenerationFilter(orphanedAt)...)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	res, err := s.blocks.UpdateOne(octx, filter, doc(kv(opSet, doc(kv(fReconciledAt, msTrunc(at))))))
	if err != nil {
		return false, fmt.Errorf("mark block reconciled %s: %w", blockHash, err)
	}
	return res.MatchedCount == 1, nil
}

// ReactivateBlock implements store.Store as a compare-and-set on the orphan
// generation (issue #339): the filter requires status == orphaned and, unless
// orphanedAt is zero, the orphan generation equals orphanedAt (see
// orphanGenerationFilter), so a missing, active, parked
// or re-orphaned row is left untouched and reported as not applied. The
// server evaluates filter and update atomically per document; no upsert, so
// a missing row is never created.
func (s *Store) ReactivateBlock(ctx context.Context, blockHash string, blockHeight uint64, orphanedAt time.Time) (bool, error) {
	filter := doc(kv(fID, blockHash), kv(fStatus, string(models.BlockStatusOrphaned)))
	filter = append(filter, orphanGenerationFilter(orphanedAt)...)
	update := doc(
		kv(opSet, doc(kv(fBlockHeight, heightToInt64(blockHeight)), kv(fStatus, string(models.BlockStatusActive)))),
		kv(opUnset, doc(kv(fOrphanedAt, ""), kv(fOrphanedGen, ""), kv(fReconciledAt, ""))),
	)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	res, err := s.blocks.UpdateOne(octx, filter, update)
	if err != nil {
		return false, fmt.Errorf("reactivate block %s: %w", blockHash, err)
	}
	return res.MatchedCount == 1, nil
}

// MarkBlocksParked implements store.Store; only active rows park.
func (s *Store) MarkBlocksParked(ctx context.Context, blockHashes []string) error {
	if len(blockHashes) == 0 {
		return nil
	}
	_, err := s.updateBlocksIn(ctx, blockHashes,
		doc(kv(fStatus, string(models.BlockStatusActive))),
		doc(kv(opSet, doc(kv(fStatus, string(models.BlockStatusParked))))),
		"mark blocks parked")
	return err
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
		kv(fProcessedAt, nil),                               // matches absent AND explicit null (a migration's NULL), unlike $exists:false
		kv(fHeaderSeenAt, doc(kv(opLt, msCeil(olderThan)))), // exclusive bound rounds up, see msCeil
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
		// reconciled_at: null matches absent and explicit null alike, so a row
		// a migration wrote with NULL is queued like one this package unset.
		doc(kv(opMatch, doc(kv(fStatus, string(models.BlockStatusOrphaned)), kv(fReconciledAt, nil)))),
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
