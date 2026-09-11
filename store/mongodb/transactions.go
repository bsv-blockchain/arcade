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

	// maxCASAttempts bounds the per-document retry loop of the block-scoped
	// rewrites. Contention on one txid comes from a handful of concurrent
	// writers, so hitting the cap means something is looping, not racing.
	maxCASAttempts = 16
)

// Projections. IterateTrackerRows and IterateStatusesByToken are bound by the
// interface to never read raw_tx / merkle_path / orphaned_anchors; the
// snapshot projection is what the CAS rewrites need and nothing wider.
var (
	projNoRawTx     = doc(kv(fRawTx, 0))
	projID          = doc(kv(fID, 1))
	projTracker     = doc(kv(fStatus, 1), kv(fBlockHeight, 1))
	projTokenReplay = doc(kv(fStatus, 1), kv(fTimestamp, 1), kv(fBlockHash, 1), kv(fBlockHeight, 1))
	projSnapshot    = doc(kv(fStatus, 1), kv(fTimestamp, 1), kv(fBlockHash, 1), kv(fBlockHeight, 1), kv(fOrphanedAnchors, 1), kv(fVersion, 1))
	projRetry       = doc(kv(fRawTx, 1), kv(fRetryCount, 1), kv(fNextRetryAt, 1))
)

// incVersion is the $inc clause every transactions write carries so the CAS
// rewrites observe concurrent updates.
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

// sinceFilter is the "updated at or after since" clause. Query-side times
// are truncated to the millisecond like every stored timestamp, so a caller
// passing a sub-millisecond time.Now() compares against the same boundary
// the writer persisted rather than against a value the store cannot hold.
func sinceFilter(since time.Time) bson.D {
	if since.IsZero() {
		return doc()
	}
	return doc(kv(fTimestamp, doc(kv(opGte, msTrunc(since)))))
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
	window := doc(kv(opLt, msTrunc(stuckDeadline)))
	if !since.IsZero() {
		window = append(doc(kv(opGte, msTrunc(since))), window...)
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

// --- block-scoped rewrites (optimistic concurrency) ---

// casOp is one version-guarded update in a block-scoped rewrite.
type casOp struct {
	txid    string
	version int64
	update  bson.D
}

func casFilter(op casOp) bson.D { return doc(kv(fID, op.txid), kv(fVersion, op.version)) }

// settleFunc is consulted for an op whose CAS write did not match. It
// re-reads the document and reports whether the intended end state now
// holds (the bulk write landed after all, or an identical concurrent write
// did), driving a fresh CAS itself when the row simply moved.
type settleFunc func(ctx context.Context, op casOp) (applied bool, err error)

// applyCAS writes ops in one unordered bulk. A matched count equal to
// len(ops) proves every write landed against its snapshot. A short count
// means at least one document moved between snapshot and write — bulkWrite
// reports only the aggregate, so each op is then retried alone: the same
// {_id, version} write applies it if the bulk had not, and a zero match hands
// the op to settle. The fast path is one round trip per chunk; the slow path
// runs only under a genuine race.
func (s *Store) applyCAS(ctx context.Context, ops []casOp, settle settleFunc) (map[string]bool, error) {
	applied := make(map[string]bool, len(ops))
	if len(ops) == 0 {
		return applied, nil
	}
	writes := make([]mongo.WriteModel, 0, len(ops))
	for _, op := range ops {
		writes = append(writes, mongo.NewUpdateOneModel().SetFilter(casFilter(op)).SetUpdate(op.update))
	}
	qctx, cancel := s.queryCtx(ctx)
	res, err := s.tx.BulkWrite(qctx, writes, options.BulkWrite().SetOrdered(false))
	cancel()
	if err != nil {
		return nil, fmt.Errorf("cas bulk write: %w", err)
	}
	if res.MatchedCount == int64(len(ops)) {
		for _, op := range ops {
			applied[op.txid] = true
		}
		return applied, nil
	}
	for _, op := range ops {
		ok, err := s.retryCAS(ctx, op, settle)
		if err != nil {
			return nil, err
		}
		if ok {
			applied[op.txid] = true
		}
	}
	return applied, nil
}

func (s *Store) retryCAS(ctx context.Context, op casOp, settle settleFunc) (bool, error) {
	octx, cancel := s.opCtx(ctx)
	res, err := s.tx.UpdateOne(octx, casFilter(op), op.update)
	cancel()
	if err != nil {
		return false, fmt.Errorf("cas write %s: %w", op.txid, err)
	}
	if res.MatchedCount == 1 {
		return true, nil
	}
	return settle(ctx, op)
}

// snapshot reads the CAS projection of every document matching filter.
func (s *Store) snapshot(ctx context.Context, filter bson.D) ([]txDoc, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.tx.Find(qctx, filter, options.Find().SetProjection(projSnapshot).SetBatchSize(s.cursorBatch()))
	if err != nil {
		return nil, err
	}
	var docs []txDoc
	if err := cur.All(qctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// snapshotOne reads one document's CAS projection; found is false when absent.
func (s *Store) snapshotOne(ctx context.Context, txid string) (txDoc, bool, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var d txDoc
	err := s.tx.FindOne(octx, idFilter(txid), options.FindOne().SetProjection(projSnapshot)).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return d, false, nil
	}
	if err != nil {
		return d, false, fmt.Errorf("snapshot tx %s: %w", txid, err)
	}
	return d, true, nil
}

// prevFromSnapshot is the projected pre-image the rewrites return: the five
// fields the transition-age metric and reorg consumers read.
func prevFromSnapshot(d txDoc) *models.TransactionStatus {
	return &models.TransactionStatus{
		TxID:        d.TxID,
		Status:      models.Status(d.Status),
		Timestamp:   d.Timestamp,
		BlockHash:   d.BlockHash,
		BlockHeight: heightFromInt64(d.BlockHeight),
	}
}

// minedOp builds the MINED write for one snapshot row. Both anchor fields are
// always written — the interface requires the height to be persisted with
// the hash, and the other backends store a literal 0 rather than dropping
// the field, so the row stays distinguishable from a never-anchored one. A
// row already MINED on a different block appends its previous anchor to
// orphaned_anchors (issue #279).
func minedOp(d txDoc, blockHash string, blockHeight uint64, now time.Time) casOp {
	set := doc(
		kv(fStatus, string(models.StatusMined)),
		kv(fBlockHash, blockHash),
		kv(fBlockHeight, heightToInt64(blockHeight)),
		kv(fTimestamp, now),
	)
	update := doc()
	if d.Status == string(models.StatusMined) && d.BlockHash != "" && d.BlockHash != blockHash {
		hist := models.AppendOrphanedAnchor(anchorsFromDocs(d.OrphanedAnchors), models.OrphanedAnchor{
			BlockHash: d.BlockHash, BlockHeight: heightFromInt64(d.BlockHeight), OrphanedAt: now,
		})
		set = append(set, kv(fOrphanedAnchors, anchorsToDocs(hist)))
	}
	update = append(update, kv(opSet, set), incVersion())
	return casOp{txid: d.TxID, version: d.Version, update: update}
}

// SetMinedByTxIDs implements store.Store. Per chunk: snapshot the rows,
// build a version-guarded MINED write for every eligible one (IMMUTABLE and
// unknown txids are skipped), bulk-write, and settle any that raced.
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	if len(txids) == 0 {
		return nil, nil, nil
	}
	if blockHash == "" {
		return nil, nil, errors.New("set mined: empty block hash")
	}
	now := msNow()
	var prevs, mined []*models.TransactionStatus
	for _, chunk := range chunks(dedupe(txids), s.batchSize) {
		p, m, err := s.setMinedChunk(ctx, blockHash, blockHeight, chunk, now)
		if err != nil {
			return prevs, mined, err
		}
		prevs = append(prevs, p...)
		mined = append(mined, m...)
	}
	return prevs, mined, nil
}

func (s *Store) setMinedChunk(ctx context.Context, blockHash string, blockHeight uint64, chunk []string, now time.Time) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	docs, err := s.snapshot(ctx, doc(kv(fID, doc(kv(opIn, chunk)))))
	if err != nil {
		return nil, nil, fmt.Errorf("set mined: snapshot: %w", err)
	}
	ops := make([]casOp, 0, len(docs))
	prevByTx := make(map[string]*models.TransactionStatus, len(docs))
	for _, d := range docs {
		if d.Status == string(models.StatusImmutable) {
			continue
		}
		ops = append(ops, minedOp(d, blockHash, blockHeight, now))
		prevByTx[d.TxID] = prevFromSnapshot(d)
	}
	settle := func(ctx context.Context, op casOp) (bool, error) {
		return s.settleMined(ctx, op.txid, blockHash, blockHeight, now, prevByTx)
	}
	applied, err := s.applyCAS(ctx, ops, settle)
	if err != nil {
		return nil, nil, fmt.Errorf("set mined: %w", err)
	}
	prevs := make([]*models.TransactionStatus, 0, len(ops))
	mined := make([]*models.TransactionStatus, 0, len(ops))
	for _, op := range ops {
		if !applied[op.txid] {
			continue
		}
		prevs = append(prevs, prevByTx[op.txid])
		mined = append(mined, &models.TransactionStatus{
			TxID: op.txid, Status: models.StatusMined, BlockHash: blockHash, BlockHeight: blockHeight, Timestamp: now,
		})
	}
	return prevs, mined, nil
}

// minedLanded reports whether the row carries exactly this call's write:
// MINED on this block at this height with this call's timestamp. A row mined
// on the same block by a concurrent call has a different timestamp and is
// deliberately NOT treated as landed — it is re-applied so the persisted row
// equals the `mined` snapshot this call returns, as the other backends do.
func minedLanded(d txDoc, blockHash string, blockHeight uint64, now time.Time) bool {
	return d.Status == string(models.StatusMined) && d.BlockHash == blockHash &&
		heightFromInt64(d.BlockHeight) == blockHeight && d.Timestamp.Equal(now)
}

// settleMined resolves one MINED write whose CAS missed. The row is re-read:
// if it already carries exactly this call's write (the bulk landed it and a
// sibling op in the chunk raced), it is applied and the snapshot pre-image
// stands; if it is gone or IMMUTABLE it is skipped; otherwise it moved under
// us — a concurrent status update, or a same-block MINED write with another
// timestamp — and the write is recomputed against the fresh row so the
// returned prev/mined pair matches what is persisted.
func (s *Store) settleMined(ctx context.Context, txid, blockHash string, blockHeight uint64, now time.Time, prevByTx map[string]*models.TransactionStatus) (bool, error) {
	for attempt := 0; attempt < maxCASAttempts; attempt++ {
		d, found, err := s.snapshotOne(ctx, txid)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		if minedLanded(d, blockHash, blockHeight, now) {
			return true, nil
		}
		if d.Status == string(models.StatusImmutable) {
			return false, nil
		}
		prevByTx[txid] = prevFromSnapshot(d)
		op := minedOp(d, blockHash, blockHeight, now)
		octx, cancel := s.opCtx(ctx)
		res, err := s.tx.UpdateOne(octx, casFilter(op), op.update)
		cancel()
		if err != nil {
			return false, fmt.Errorf("cas write %s: %w", txid, err)
		}
		if res.MatchedCount == 1 {
			return true, nil
		}
	}
	return false, fmt.Errorf("set mined %s: cas contention exceeded %d attempts", txid, maxCASAttempts)
}

// blockRewriteOp builds the SetStatusByBlockHash write for one snapshot row.
// A revert clears the anchor and records it in orphaned_anchors; any other
// target keeps the block fields.
func blockRewriteOp(d txDoc, blockHash string, newStatus models.Status, clearBlock bool, now time.Time) casOp {
	set := doc(kv(fStatus, string(newStatus)), kv(fTimestamp, now))
	update := doc()
	if clearBlock {
		hist := models.AppendOrphanedAnchor(anchorsFromDocs(d.OrphanedAnchors), models.OrphanedAnchor{
			BlockHash: blockHash, BlockHeight: heightFromInt64(d.BlockHeight), OrphanedAt: now,
		})
		set = append(set, kv(fOrphanedAnchors, anchorsToDocs(hist)))
		update = append(update, kv(opUnset, doc(kv(fBlockHash, ""), kv(fBlockHeight, ""))))
	}
	update = append(update, kv(opSet, set), incVersion())
	return casOp{txid: d.TxID, version: d.Version, update: update}
}

// SetStatusByBlockHash implements store.Store. Block fields are cleared on a
// SEEN_ON_NETWORK revert and kept otherwise; IMMUTABLE rows are never
// touched. The version CAS doubles as the stale-index guard: a row that was
// concurrently re-anchored elsewhere fails its write and, on re-read, no
// longer carries this block hash, so it is skipped (issue #279).
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	if blockHash == "" {
		return nil, errors.New("set status by block hash: empty block hash")
	}
	clearBlock := newStatus == models.StatusSeenOnNetwork
	now := msNow()
	docs, err := s.snapshot(ctx, doc(kv(fBlockHash, blockHash), kv(fStatus, doc(kv(opNe, string(models.StatusImmutable))))))
	if err != nil {
		return nil, fmt.Errorf("set status by block hash: snapshot: %w", err)
	}
	settle := func(ctx context.Context, op casOp) (bool, error) {
		return s.settleBlockRewrite(ctx, op.txid, blockHash, newStatus, clearBlock, now)
	}
	txids := make([]string, 0, len(docs))
	for start := 0; start < len(docs); start += s.batchSize {
		end := min(start+s.batchSize, len(docs))
		ops := make([]casOp, 0, end-start)
		for _, d := range docs[start:end] {
			ops = append(ops, blockRewriteOp(d, blockHash, newStatus, clearBlock, now))
		}
		applied, err := s.applyCAS(ctx, ops, settle)
		if err != nil {
			return txids, fmt.Errorf("set status by block hash: %w", err)
		}
		for _, op := range ops {
			if applied[op.txid] {
				txids = append(txids, op.txid)
			}
		}
	}
	return txids, nil
}

// settleBlockRewrite is settleMined's counterpart for SetStatusByBlockHash.
// The landed check runs before the IMMUTABLE guard because a landed
// IMMUTABLE promotion of our own must count as applied; only a row that is
// IMMUTABLE for another reason, or no longer anchored to this block, is
// skipped. A same-status rewrite by a concurrent call carries a different
// timestamp and is re-applied.
func (s *Store) settleBlockRewrite(ctx context.Context, txid, blockHash string, newStatus models.Status, clearBlock bool, now time.Time) (bool, error) {
	for attempt := 0; attempt < maxCASAttempts; attempt++ {
		d, found, err := s.snapshotOne(ctx, txid)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		anchored := d.BlockHash == blockHash
		landed := d.Status == string(newStatus) && d.Timestamp.Equal(now) &&
			((clearBlock && d.BlockHash == "") || (!clearBlock && anchored))
		if landed {
			return true, nil
		}
		if d.Status == string(models.StatusImmutable) || !anchored {
			// IMMUTABLE rows are never touched, and a row concurrently
			// re-anchored (or already reverted) elsewhere must not be
			// rewritten from a stale index read.
			return false, nil
		}
		op := blockRewriteOp(d, blockHash, newStatus, clearBlock, now)
		octx, cancel := s.opCtx(ctx)
		res, err := s.tx.UpdateOne(octx, casFilter(op), op.update)
		cancel()
		if err != nil {
			return false, fmt.Errorf("cas write %s: %w", txid, err)
		}
		if res.MatchedCount == 1 {
			return true, nil
		}
	}
	return false, fmt.Errorf("set status by block hash %s: cas contention exceeded %d attempts", txid, maxCASAttempts)
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
