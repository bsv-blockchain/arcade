package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Outcome labels for metrics.StoreUpdateStatusDuration, matching Pebble.
const (
	outcomeApplied        = "applied"
	outcomeSkippedLattice = "skipped_lattice"
	outcomeNotFound       = "not_found"
	outcomeError          = "error"
)

// Projections. IterateTrackerRows and IterateStatusesByToken are bound by the
// interface to never read raw_tx / merkle_path / orphaned_anchors.
var (
	projNoRawTx     = doc(kv(fRawTx, 0))
	projID          = doc(kv(fID, 1))
	projTracker     = doc(kv(fStatus, 1), kv(fBlockHeight, 1))
	projTokenReplay = doc(kv(fStatus, 1), kv(fTimestamp, 1), kv(fBlockHash, 1), kv(fBlockHeight, 1))
	projRetry       = doc(kv(fRawTx, 1), kv(fRetryCount, 1), kv(fNextRetryAt, 1))
)

// incVersion is the $inc clause every transactions write carries. The
// counter is not used as a CAS token by this package any more, but every
// write still bumps it so an operator (or a future guard) can tell rewrites
// apart cheaply.
func incVersion() bson.E { return kv(opInc, doc(kv(fVersion, 1))) }

// --- inserts ---

// GetOrInsertStatus implements store.Store. An insert that loses the race
// to an existing document re-reads and returns the winner's row.
func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	if status == nil || status.TxID == "" {
		return nil, false, errors.New("mongodb: get or insert status: empty txid")
	}
	now := time.Now()
	if status.Timestamp.IsZero() {
		status.Timestamp = now
	}
	if status.Status == "" {
		status.Status = models.StatusReceived
	}
	status.CreatedAt = now
	d := txDocFromStatus(status)
	d.Version = 1

	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.tx.InsertOne(octx, d); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return nil, false, fmt.Errorf("insert tx %s: %w", status.TxID, err)
		}
		existing, err := s.GetStatus(ctx, status.TxID)
		if err != nil {
			return nil, false, err
		}
		if existing == nil {
			return nil, false, fmt.Errorf("insert tx %s: lost the insert race but the row is gone", status.TxID)
		}
		return existing, false, nil
	}
	return status, true, nil
}

// BatchGetOrInsertStatus implements store.Store via the shared bounded-
// concurrency loop, like the Aerospike and Pebble backends. Duplicate txids
// within one batch are collapsed first so the result is deterministic: the
// first occurrence carries the insert outcome and every later occurrence
// reports the row as existing (Postgres does the same).
func (s *Store) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	firstIdx := make(map[string]int, len(statuses))
	uniq := make([]*models.TransactionStatus, 0, len(statuses))
	for i, st := range statuses {
		if st == nil {
			continue
		}
		if _, dup := firstIdx[st.TxID]; dup {
			continue
		}
		firstIdx[st.TxID] = i
		uniq = append(uniq, st)
	}
	if len(uniq) == len(statuses) {
		return store.BatchGetOrInsertStatusParallel(ctx, s, statuses)
	}
	uniqRes, err := store.BatchGetOrInsertStatusParallel(ctx, s, uniq)
	if err != nil {
		return nil, err
	}
	byTx := make(map[string]store.BatchInsertResult, len(uniq))
	for i, st := range uniq {
		byTx[st.TxID] = uniqRes[i]
	}
	out := make([]store.BatchInsertResult, len(statuses))
	for i, st := range statuses {
		if st == nil {
			continue
		}
		r := byTx[st.TxID]
		if firstIdx[st.TxID] == i {
			out[i] = r
			continue
		}
		existing := r.Existing
		if r.Inserted {
			existing = statuses[firstIdx[st.TxID]]
		}
		out[i] = store.BatchInsertResult{Existing: existing, Inserted: false}
	}
	return out, nil
}

// --- partial updates ---

// statusUpdate builds the UpdateStatus write: only non-zero fields are set,
// timestamp defaults to now, and the version is bumped. Times are truncated
// to ms and written back into st so the caller holds what was stored.
func statusUpdate(st *models.TransactionStatus, now time.Time) bson.D {
	if st.Timestamp.IsZero() {
		st.Timestamp = now
	}
	st.Timestamp = msTrunc(st.Timestamp)
	st.MerkleRegisteredAt = msTrunc(st.MerkleRegisteredAt)

	set := doc(kv(fTimestamp, st.Timestamp))
	if st.Status != "" {
		set = append(set, kv(fStatus, string(st.Status)))
	}
	if st.StatusCode != 0 {
		set = append(set, kv(fStatusCode, st.StatusCode))
	}
	if st.BlockHash != "" {
		set = append(set, kv(fBlockHash, st.BlockHash))
	}
	if st.BlockHeight > 0 {
		set = append(set, kv(fBlockHeight, heightToInt64(st.BlockHeight)))
	}
	if st.ExtraInfo != "" {
		set = append(set, kv(fExtraInfo, st.ExtraInfo))
	}
	if len(st.MerklePath) > 0 {
		set = append(set, kv(fMerklePath, []byte(st.MerklePath)))
	}
	if !st.MerkleRegisteredAt.IsZero() {
		set = append(set, kv(fMerkleRegisteredAt, st.MerkleRegisteredAt))
	}
	return doc(kv(opSet, set), incVersion())
}

// UpdateStatus implements store.Store. Returns store.ErrNotFound for an
// unknown txid and nil (no write) when the status lattice forbids the
// transition — see models.Status.CanTransitionFrom.
func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	_, err := s.UpdateStatusReturning(ctx, status)
	return err
}

// UpdateStatusReturning is UpdateStatus plus the row as it stood before the
// write (or the row the lattice rejected against). One round trip on the
// common path: the lattice guard rides in the filter and findAndModify
// returns the pre-image. A zero match with a guard needs one probe to tell
// "absent" from "blocked".
func (s *Store) UpdateStatusReturning(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, error) {
	if status == nil || status.TxID == "" {
		return nil, errors.New("mongodb: update status: empty txid")
	}
	start := time.Now()
	fromLabel, outcome := "", outcomeError
	defer func() {
		metrics.StoreUpdateStatusDuration.WithLabelValues(fromLabel, string(status.Status), outcome).Observe(time.Since(start).Seconds())
	}()

	guard := latticeFilter(status.Status)
	filter := append(idFilter(status.TxID), guard...)
	update := statusUpdate(status, time.Now())

	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var before txDoc
	err := s.tx.FindOneAndUpdate(octx, filter, update,
		options.FindOneAndUpdate().SetReturnDocument(options.Before).SetProjection(projNoRawTx)).Decode(&before)
	if err == nil {
		fromLabel, outcome = before.Status, outcomeApplied
		return before.toStatus(), nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("update tx %s: %w", status.TxID, err)
	}
	if len(guard) == 0 {
		outcome = outcomeNotFound
		return nil, store.ErrNotFound
	}
	err = s.tx.FindOne(octx, idFilter(status.TxID), options.FindOne().SetProjection(projNoRawTx)).Decode(&before)
	if errors.Is(err, mongo.ErrNoDocuments) {
		outcome = outcomeNotFound
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("update tx %s: existence probe: %w", status.TxID, err)
	}
	fromLabel, outcome = before.Status, outcomeSkippedLattice
	return before.toStatus(), nil
}

// BatchUpdateStatus implements store.Store as one unordered bulk write per
// chunk. Unknown txids and lattice-blocked rows are silent no-ops.
func (s *Store) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	now := time.Now()
	writes := make([]mongo.WriteModel, 0, len(statuses))
	for _, st := range statuses {
		if st == nil || st.TxID == "" {
			continue
		}
		filter := append(idFilter(st.TxID), latticeFilter(st.Status)...)
		writes = append(writes, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(statusUpdate(st, now)))
	}
	for start := 0; start < len(writes); start += s.batchSize {
		end := min(start+s.batchSize, len(writes))
		qctx, cancel := s.queryCtx(ctx)
		_, err := s.tx.BulkWrite(qctx, writes[start:end], options.BulkWrite().SetOrdered(false))
		cancel()
		if err != nil {
			return fmt.Errorf("batch update: %w", err)
		}
	}
	return nil
}

// BatchUpdateStatusReturning implements store.Store over the fused
// UpdateStatusReturning, so each row costs one round trip, not two.
func (s *Store) BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]*models.TransactionStatus, error) {
	return store.BatchUpdateStatusReturningParallel(ctx, s, statuses)
}

// --- reads ---

// GetStatus implements store.Store. A missing row is (nil, nil), the
// convention shared by every backend.
func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var d txDoc
	err := s.tx.FindOne(octx, idFilter(txid)).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get tx %s: %w", txid, err)
	}
	st := d.toStatus()
	s.enrichMerklePath(ctx, st)
	s.enrichOrphanedProofs(ctx, st)
	return st, nil
}

// sinceFilter is the "updated at or after since" clause. Stored timestamps
// are millisecond-aligned, so an inclusive lower bound rounds UP (msCeil): a
// row stored at 12 ms is not >= 12.345 ms and must not match.
func sinceFilter(since time.Time) bson.D {
	if since.IsZero() {
		return doc()
	}
	return doc(kv(fTimestamp, doc(kv(opGte, msCeil(since)))))
}

// GetStatusesSince implements store.Store: full rows updated at or after
// since, newest first.
func (s *Store) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	var out []*models.TransactionStatus
	err := s.IterateStatusesSince(ctx, since, func(st *models.TransactionStatus) error {
		out = append(out, st)
		return nil
	})
	return out, err
}

// IterateStatusesSince implements store.Store, streaming full rows newest
// first under the caller's context. The hint pins the timestamp index so the
// planner can never fall back to a collection scan plus in-memory sort.
func (s *Store) IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error {
	cur, err := s.tx.Find(ctx, sinceFilter(since), options.Find().
		SetSort(doc(kv(fTimestamp, -1))).SetHint(idxTxTimestamp).SetBatchSize(s.cursorBatch()))
	if err != nil {
		return fmt.Errorf("iterate statuses since: %w", err)
	}
	defer closeCursor(ctx, cur)
	for cur.Next(ctx) {
		var d txDoc
		if err := cur.Decode(&d); err != nil {
			return fmt.Errorf("iterate statuses since: decode: %w", err)
		}
		if err := fn(d.toStatus()); err != nil {
			return err
		}
	}
	return cur.Err()
}

// trackerRowDoc is the three-field projection IterateTrackerRows decodes.
type trackerRowDoc struct {
	TxID        string `bson:"_id"`
	Status      string `bson:"status"`
	BlockHeight int64  `bson:"block_height"`
}

// IterateTrackerRows implements store.Store. The filter is trackerFilter,
// the projection is covered by the {status, block_height, _id} index, there
// is no sort, and store.TrackerScan.Keep is re-applied to every row so an
// over-permissive pushdown can only cost time, never correctness.
func (s *Store) IterateTrackerRows(ctx context.Context, scan store.TrackerScan, fn func(store.TrackerRow) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cur, err := s.tx.Find(ctx, trackerFilter(scan), options.Find().
		SetProjection(projTracker).SetHint(idxTxStatusHeight).SetBatchSize(s.cursorBatch()))
	if err != nil {
		return fmt.Errorf("iterate tracker rows: %w", err)
	}
	defer closeCursor(ctx, cur)
	for cur.Next(ctx) {
		var d trackerRowDoc
		if err := cur.Decode(&d); err != nil {
			return fmt.Errorf("iterate tracker rows: decode: %w", err)
		}
		st, h := models.Status(d.Status), heightFromInt64(d.BlockHeight)
		if !scan.Keep(st, h) {
			continue
		}
		if err := fn(store.TrackerRow{TxID: d.TxID, Status: st, BlockHeight: h}); err != nil {
			return err
		}
	}
	return cur.Err()
}

// censusRow is one $group output of CensusStatusesSince.
type censusRow struct {
	Status string    `bson:"_id"`
	Count  int64     `bson:"count"`
	Oldest time.Time `bson:"oldest"`
}

// CensusStatusesSince implements store.Store as one aggregate: count and
// min(timestamp) per status over [since, stuckDeadline).
func (s *Store) CensusStatusesSince(ctx context.Context, since, stuckDeadline time.Time, statuses []models.Status) (map[models.Status]store.StatusCensus, error) {
	out := make(map[models.Status]store.StatusCensus, len(statuses))
	if len(statuses) == 0 {
		return out, nil
	}
	names := make([]string, 0, len(statuses))
	for _, st := range statuses {
		out[st] = store.StatusCensus{}
		names = append(names, string(st))
	}
	// Both bounds round up: >= since and < stuckDeadline against
	// millisecond-aligned rows (see msCeil).
	window := doc(kv(opLt, msCeil(stuckDeadline)))
	if !since.IsZero() {
		window = append(doc(kv(opGte, msCeil(since))), window...)
	}
	pipeline := mongo.Pipeline{
		doc(kv(opMatch, doc(kv(fStatus, doc(kv(opIn, names))), kv(fTimestamp, window)))),
		doc(kv(opGroup, doc(
			kv(fID, "$"+fStatus),
			kv("count", doc(kv(opSum, 1))),
			kv("oldest", doc(kv(opMin, "$"+fTimestamp))),
		))),
	}
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.tx.Aggregate(qctx, pipeline, options.Aggregate().SetHint(idxTxStatusTS))
	if err != nil {
		return nil, fmt.Errorf("census statuses since: %w", err)
	}
	defer closeCursor(ctx, cur)
	for cur.Next(qctx) {
		var r censusRow
		if err := cur.Decode(&r); err != nil {
			return nil, fmt.Errorf("census statuses since: decode: %w", err)
		}
		out[models.Status(r.Status)] = store.StatusCensus{Count: r.Count, Oldest: r.Oldest}
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("census statuses since: %w", err)
	}
	return out, nil
}

// GetTxIDsByBlockHash implements store.Store. Cursor errors are surfaced, not
// skipped: a partial affected set would silently under-reconcile a reorg.
func (s *Store) GetTxIDsByBlockHash(ctx context.Context, blockHash string) ([]string, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.tx.Find(qctx, doc(kv(fBlockHash, blockHash)), options.Find().SetProjection(projID))
	if err != nil {
		return nil, fmt.Errorf("get txids by block hash: %w", err)
	}
	defer closeCursor(ctx, cur)
	var txids []string
	for cur.Next(qctx) {
		var d struct {
			TxID string `bson:"_id"`
		}
		if err := cur.Decode(&d); err != nil {
			return txids, fmt.Errorf("get txids by block hash: decode: %w", err)
		}
		txids = append(txids, d.TxID)
	}
	return txids, cur.Err()
}

// --- block-scoped rewrites ---
//
// SetMinedByTxIDs and SetStatusByBlockHash rewrite many rows under a guard
// (not IMMUTABLE; still anchored to this block) and must report exactly the
// rows they changed, each with its pre-image. Both run one findAndModify per
// row: the guard rides in the filter, the anchor-history bookkeeping is an
// aggregation-pipeline update evaluated server-side against the row as it
// stands at write time, and the pre-image comes back with the result. One
// round trip per row, run with bounded parallelism like the other backends'
// batch loops — and exact: no snapshot to go stale, no version to race, and
// no aggregate bulk count to disambiguate.

// projPreimage is the pre-image the rewrites return: the fields the
// transition-age metric and the reorg consumers read.
var projPreimage = doc(kv(fStatus, 1), kv(fTimestamp, 1), kv(fBlockHash, 1), kv(fBlockHeight, 1))

func preimage(d txDoc) *models.TransactionStatus {
	return &models.TransactionStatus{
		TxID:        d.TxID,
		Status:      models.Status(d.Status),
		Timestamp:   d.Timestamp,
		BlockHash:   d.BlockHash,
		BlockHeight: heightFromInt64(d.BlockHeight),
	}
}

// versionBump is the pipeline form of $inc version.
func versionBump() bson.D {
	return doc(kv("$add", bson.A{doc(kv("$ifNull", bson.A{"$" + fVersion, 0})), 1}))
}

// anchorHistoryExpr is models.AppendOrphanedAnchor as a pipeline expression:
// the row's current anchor appended to orphaned_anchors, unless the row has
// no anchor or the last entry already names it, keeping the newest
// MaxOrphanedAnchors. It reads the row's fields BEFORE the same update
// overwrites them, so it must run in a stage ahead of the field rewrite.
func anchorHistoryExpr(now time.Time) bson.D {
	hist := doc(kv("$ifNull", bson.A{"$" + fOrphanedAnchors, bson.A{}}))
	entry := doc(kv(fBlockHash, "$"+fBlockHash), kv(fBlockHeight, "$"+fBlockHeight), kv(fOrphanedAt, now))
	unchanged := doc(kv("$or", bson.A{
		doc(kv("$eq", bson.A{doc(kv("$ifNull", bson.A{"$" + fBlockHash, ""})), ""})),
		doc(kv("$eq", bson.A{"$$last." + fBlockHash, "$" + fBlockHash})),
	}))
	appended := doc(kv("$slice", bson.A{doc(kv("$concatArrays", bson.A{"$$hist", bson.A{entry}})), -models.MaxOrphanedAnchors}))
	return doc(kv("$let", doc(
		kv("vars", doc(kv("hist", hist))),
		kv("in", doc(kv("$let", doc(
			kv("vars", doc(kv("last", doc(kv("$arrayElemAt", bson.A{"$$hist", -1}))))),
			kv("in", doc(kv("$cond", bson.A{unchanged, "$$hist", appended}))),
		)))),
	)))
}

// minedPipeline is the SetMinedByTxIDs write: a row already MINED on a
// different block records that anchor in its history first (issue #279),
// then status, anchor (both fields, always — a zero height is a literal 0
// like the other backends store) and timestamp are overwritten.
func minedPipeline(blockHash string, blockHeight uint64, now time.Time) mongo.Pipeline {
	reanchored := doc(kv("$and", bson.A{
		doc(kv("$eq", bson.A{"$" + fStatus, string(models.StatusMined)})),
		doc(kv("$ne", bson.A{doc(kv("$ifNull", bson.A{"$" + fBlockHash, ""})), ""})),
		doc(kv("$ne", bson.A{"$" + fBlockHash, doc(kv("$literal", blockHash))})),
	}))
	return mongo.Pipeline{
		doc(kv(opSet, doc(kv(fOrphanedAnchors, doc(kv("$cond", bson.A{reanchored, anchorHistoryExpr(now), "$" + fOrphanedAnchors})))))),
		doc(kv(opSet, doc(
			kv(fStatus, string(models.StatusMined)),
			kv(fBlockHash, doc(kv("$literal", blockHash))),
			kv(fBlockHeight, heightToInt64(blockHeight)),
			kv(fTimestamp, now),
			kv(fVersion, versionBump()),
		))),
	}
}

// blockRewritePipeline is the SetStatusByBlockHash write. A revert records
// the current anchor in the history and clears the block fields; any other
// target keeps them.
func blockRewritePipeline(newStatus models.Status, clearBlock bool, now time.Time) mongo.Pipeline {
	set := doc(kv(fStatus, string(newStatus)), kv(fTimestamp, now), kv(fVersion, versionBump()))
	if !clearBlock {
		return mongo.Pipeline{doc(kv(opSet, set))}
	}
	return mongo.Pipeline{
		doc(kv(opSet, doc(kv(fOrphanedAnchors, anchorHistoryExpr(now))))),
		doc(kv(opUnset, bson.A{fBlockHash, fBlockHeight})),
		doc(kv(opSet, set)),
	}
}

// SetMinedByTxIDs implements store.Store: one guarded findAndModify per
// txid, in parallel. Unknown txids and IMMUTABLE rows produce no entry.
// Results keep input order; on error the rows written so far are returned
// with it, like the Postgres backend's partial RETURNING scan.
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	if len(txids) == 0 {
		return nil, nil, nil
	}
	if blockHash == "" {
		return nil, nil, errors.New("set mined: empty block hash")
	}
	now := msNow()
	uniq := dedupe(txids)
	update := minedPipeline(blockHash, blockHeight, now)
	notImmutable := doc(kv(opNe, string(models.StatusImmutable)))
	slots := make([]*models.TransactionStatus, len(uniq))
	loopErr := forEach(ctx, len(uniq), func(i int) error {
		octx, cancel := s.opCtx(ctx)
		defer cancel()
		var before txDoc
		err := s.tx.FindOneAndUpdate(octx, doc(kv(fID, uniq[i]), kv(fStatus, notImmutable)), update,
			options.FindOneAndUpdate().SetReturnDocument(options.Before).SetProjection(projPreimage)).Decode(&before)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil // unknown txid or IMMUTABLE: silently skipped
		}
		if err != nil {
			return fmt.Errorf("set mined %s: %w", uniq[i], err)
		}
		slots[i] = preimage(before)
		return nil
	})
	prevs := make([]*models.TransactionStatus, 0, len(uniq))
	mined := make([]*models.TransactionStatus, 0, len(uniq))
	for i, prev := range slots {
		if prev == nil {
			continue
		}
		prevs = append(prevs, prev)
		mined = append(mined, &models.TransactionStatus{
			TxID: uniq[i], Status: models.StatusMined, BlockHash: blockHash, BlockHeight: blockHeight, Timestamp: now,
		})
	}
	return prevs, mined, loopErr
}

// SetStatusByBlockHash implements store.Store. The block's rows are walked
// in keyset pages of batchSize ids (bounded memory for any block size) and
// each row is rewritten by one guarded update: the filter re-checks, at write
// time, that the row is still anchored to this block and not IMMUTABLE, so a
// row concurrently re-anchored elsewhere falls out of the match instead of
// being reverted from a stale read (issue #279). Rewritten rows leave the
// page predicate (a revert clears block_hash; a promotion sets IMMUTABLE),
// and the _id cursor guarantees progress regardless.
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	if blockHash == "" {
		return nil, errors.New("set status by block hash: empty block hash")
	}
	clearBlock := newStatus == models.StatusSeenOnNetwork
	update := blockRewritePipeline(newStatus, clearBlock, msNow())
	notImmutable := doc(kv(opNe, string(models.StatusImmutable)))
	var txids []string
	after := ""
	for {
		page, err := s.blockPage(ctx, blockHash, after)
		if err != nil {
			return txids, fmt.Errorf("set status by block hash: %w", err)
		}
		if len(page) == 0 {
			return txids, nil
		}
		applied := make([]bool, len(page))
		loopErr := forEach(ctx, len(page), func(i int) error {
			octx, cancel := s.opCtx(ctx)
			defer cancel()
			filter := doc(kv(fID, page[i]), kv(fBlockHash, blockHash), kv(fStatus, notImmutable))
			res, err := s.tx.UpdateOne(octx, filter, update)
			if err != nil {
				return fmt.Errorf("set status by block hash %s: %w", page[i], err)
			}
			applied[i] = res.MatchedCount == 1
			return nil
		})
		for i, ok := range applied {
			if ok {
				txids = append(txids, page[i])
			}
		}
		if loopErr != nil {
			return txids, loopErr
		}
		after = page[len(page)-1]
	}
}

// blockPage returns up to batchSize txids anchored to blockHash and not
// IMMUTABLE with _id > after, ascending — one keyset page over the
// {block_hash, _id} index.
func (s *Store) blockPage(ctx context.Context, blockHash, after string) ([]string, error) {
	filter := doc(kv(fBlockHash, blockHash), kv(fStatus, doc(kv(opNe, string(models.StatusImmutable)))))
	if after != "" {
		filter = append(filter, kv(fID, doc(kv(opGt, after))))
	}
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.tx.Find(qctx, filter, options.Find().
		SetProjection(projID).SetSort(doc(kv(fID, 1))).SetLimit(int64(s.batchSize)).SetHint(idxTxBlockHash))
	if err != nil {
		return nil, err
	}
	var rows []struct {
		TxID string `bson:"_id"`
	}
	if err := cur.All(qctx, &rows); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.TxID)
	}
	return ids, nil
}

// --- durable retry ---

// BumpRetryCount implements store.Store as one atomic $inc that returns the
// new value. Unknown txids are an error, never an upsert.
func (s *Store) BumpRetryCount(ctx context.Context, txid string) (int, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var d struct {
		RetryCount int `bson:"retry_count"`
	}
	err := s.tx.FindOneAndUpdate(octx, idFilter(txid),
		doc(kv(opInc, doc(kv(fRetryCount, 1), kv(fVersion, 1)))),
		options.FindOneAndUpdate().SetReturnDocument(options.After).SetProjection(doc(kv(fRetryCount, 1)))).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, store.ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, err)
	}
	return d.RetryCount, nil
}

// SetPendingRetryFields implements store.Store. The lattice guard is
// load-bearing: the park path writes twice, and without it this second write
// could drag a MINED / REJECTED row back to PENDING_RETRY for rebroadcast.
// Blocked rows are silently skipped; unknown txids return ErrNotFound.
func (s *Store) SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time) error {
	filter := append(idFilter(txid), latticeFilter(models.StatusPendingRetry)...)
	set := doc(kv(fStatus, string(models.StatusPendingRetry)), kv(fNextRetryAt, msTrunc(nextRetryAt)), kv(fTimestamp, msNow()))
	update := doc(incVersion())
	if len(rawTx) > 0 {
		set = append(set, kv(fRawTx, rawTx))
	} else {
		update = append(update, kv(opUnset, doc(kv(fRawTx, ""))))
	}
	update = append(update, kv(opSet, set))

	octx, cancel := s.opCtx(ctx)
	defer cancel()
	res, err := s.tx.UpdateOne(octx, filter, update)
	if err != nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, err)
	}
	if res.MatchedCount == 1 {
		return nil
	}
	exists, err := s.exists(octx, txid)
	if err != nil {
		return fmt.Errorf("set pending retry fields %s: existence probe: %w", txid, err)
	}
	if !exists {
		return fmt.Errorf("set pending retry fields %s: %w", txid, store.ErrNotFound)
	}
	return nil
}

// retryDoc is the GetReadyRetries projection.
type retryDoc struct {
	TxID        string    `bson:"_id"`
	RawTx       []byte    `bson:"raw_tx"`
	RetryCount  int       `bson:"retry_count"`
	NextRetryAt time.Time `bson:"next_retry_at"`
}

// GetReadyRetries implements store.Store via the partial retry index. Rows
// without a raw_tx (legacy) are skipped like every other backend.
func (s *Store) GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	if limit <= 0 {
		return nil, nil
	}
	filter := doc(
		kv(fStatus, string(models.StatusPendingRetry)),
		kv(fNextRetryAt, doc(kv(opLte, msTrunc(now)))),
		kv(fRawTx, doc(kv(opExists, true))),
	)
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.tx.Find(qctx, filter, options.Find().
		SetSort(doc(kv(fNextRetryAt, 1))).SetLimit(int64(limit)).SetProjection(projRetry).SetHint(idxTxRetryReady))
	if err != nil {
		return nil, fmt.Errorf("get ready retries: %w", err)
	}
	defer closeCursor(ctx, cur)
	var out []*store.PendingRetry
	for cur.Next(qctx) {
		var d retryDoc
		if err := cur.Decode(&d); err != nil {
			return out, fmt.Errorf("get ready retries: decode: %w", err)
		}
		if len(d.RawTx) == 0 {
			continue
		}
		out = append(out, &store.PendingRetry{TxID: d.TxID, RawTx: d.RawTx, RetryCount: d.RetryCount, NextRetryAt: d.NextRetryAt})
	}
	return out, cur.Err()
}

// ClearRetryState implements store.Store. Missing rows are a no-op.
func (s *Store) ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	set := doc(kv(fStatus, string(finalStatus)), kv(fTimestamp, msNow()))
	if extraInfo != "" {
		set = append(set, kv(fExtraInfo, extraInfo))
	}
	update := doc(kv(opSet, set), kv(opUnset, doc(kv(fRawTx, ""), kv(fNextRetryAt, ""))), incVersion())
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.tx.UpdateOne(octx, idFilter(txid), update); err != nil {
		return fmt.Errorf("clear retry state %s: %w", txid, err)
	}
	return nil
}

// MarkMerkleRegisteredByTxIDs implements store.Store; unknown txids are
// silently skipped.
func (s *Store) MarkMerkleRegisteredByTxIDs(ctx context.Context, txids []string, ts time.Time) error {
	update := doc(kv(opSet, doc(kv(fMerkleRegisteredAt, msTrunc(ts)))), incVersion())
	for _, chunk := range chunks(dedupe(txids), s.batchSize) {
		qctx, cancel := s.queryCtx(ctx)
		_, err := s.tx.UpdateMany(qctx, doc(kv(fID, doc(kv(opIn, chunk)))), update)
		cancel()
		if err != nil {
			return fmt.Errorf("mark merkle registered: %w", err)
		}
	}
	return nil
}

// exists reports whether a transaction document is present.
func (s *Store) exists(ctx context.Context, txid string) (bool, error) {
	var d struct {
		TxID string `bson:"_id"`
	}
	err := s.tx.FindOne(ctx, idFilter(txid), options.FindOne().SetProjection(projID)).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return err == nil, err
}

// cursorBatch is the per-getMore batch for streaming reads.
func (s *Store) cursorBatch() int32 {
	if s.batchSize > 1<<30 {
		return 1 << 30
	}
	return int32(s.batchSize) //nolint:gosec // bounded just above
}
