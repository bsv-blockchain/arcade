// Package pebble implements store.Store and store.Leaser on top of
// github.com/cockroachdb/pebble, a pure-Go LSM KV. It's the recommended
// backend for arcade's zero-dependency standalone mode: no external
// services, single binary, durable on disk.
//
// Layout: see keys.go for the key namespaces. Primary records hold
// JSON-encoded values; secondary indexes are empty-value prefix keys whose
// suffix names the primary row. All mutations go through an IndexedBatch
// so stale index entries are removed atomically with the primary write.
//
// Single-process: Pebble takes an exclusive file lock on its data directory.
// Running two arcade instances against the same path will fail at Open —
// acceptable for standalone, mirrors the plan's single-node constraint.
package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// getOrInsertShards serializes GetOrInsertStatus per-txid so concurrent
// inserts of the same txid collapse to a single write. Pebble has no CAS,
// and a read-then-write pattern races between goroutines. 64 shards keeps
// contention cheap.
const getOrInsertShards = 64

var _ store.Store = (*Store)(nil)
var _ store.Leaser = (*Store)(nil)

// Store is a Pebble-backed implementation of store.Store and store.Leaser.
type Store struct {
	db         *pebbledb.DB
	writeOpts  *pebbledb.WriteOptions
	cfg        config.Pebble
	insertMu   [getOrInsertShards]sync.Mutex
	leaseMu    sync.Mutex
}

// storedStatus is the persistence shape for TransactionStatus rows. We keep
// it parallel to models.TransactionStatus but redefine Time fields as int64
// unix-nanoseconds to dodge JSON time-zone drift and to keep retry index
// keys byte-for-byte consistent.
type storedStatus struct {
	TxID            string   `json:"txid"`
	Status          string   `json:"status"`
	StatusCode      int      `json:"status_code,omitempty"`
	BlockHash       string   `json:"block_hash,omitempty"`
	BlockHeight     uint64   `json:"block_height,omitempty"`
	MerklePath      []byte   `json:"merkle_path,omitempty"`
	ExtraInfo       string   `json:"extra_info,omitempty"`
	CompetingTxs    []string `json:"competing_txs,omitempty"`
	RawTx           []byte   `json:"raw_tx,omitempty"`
	RetryCount      int      `json:"retry_count,omitempty"`
	TimestampUnixNs int64    `json:"ts"`
	CreatedUnixNs   int64    `json:"created_at,omitempty"`
	NextRetryUnixNs int64    `json:"next_retry_at,omitempty"`
}

func (s storedStatus) toModel() *models.TransactionStatus {
	out := &models.TransactionStatus{
		TxID:         s.TxID,
		Status:       models.Status(s.Status),
		StatusCode:   s.StatusCode,
		BlockHash:    s.BlockHash,
		BlockHeight:  s.BlockHeight,
		MerklePath:   models.HexBytes(s.MerklePath),
		ExtraInfo:    s.ExtraInfo,
		CompetingTxs: s.CompetingTxs,
		RawTx:        models.HexBytes(s.RawTx),
		RetryCount:   s.RetryCount,
	}
	if s.TimestampUnixNs != 0 {
		out.Timestamp = time.Unix(0, s.TimestampUnixNs)
	}
	if s.CreatedUnixNs != 0 {
		out.CreatedAt = time.Unix(0, s.CreatedUnixNs)
	}
	if s.NextRetryUnixNs != 0 {
		out.NextRetryAt = time.Unix(0, s.NextRetryUnixNs)
	}
	return out
}

func fromModel(m *models.TransactionStatus) storedStatus {
	out := storedStatus{
		TxID:         m.TxID,
		Status:       string(m.Status),
		StatusCode:   m.StatusCode,
		BlockHash:    m.BlockHash,
		BlockHeight:  m.BlockHeight,
		MerklePath:   []byte(m.MerklePath),
		ExtraInfo:    m.ExtraInfo,
		CompetingTxs: m.CompetingTxs,
		RawTx:        []byte(m.RawTx),
		RetryCount:   m.RetryCount,
	}
	if !m.Timestamp.IsZero() {
		out.TimestampUnixNs = m.Timestamp.UnixNano()
	}
	if !m.CreatedAt.IsZero() {
		out.CreatedUnixNs = m.CreatedAt.UnixNano()
	}
	if !m.NextRetryAt.IsZero() {
		out.NextRetryUnixNs = m.NextRetryAt.UnixNano()
	}
	return out
}

type storedSubmission struct {
	SubmissionID        string `json:"submission_id"`
	TxID                string `json:"txid"`
	CallbackURL         string `json:"callback_url,omitempty"`
	CallbackToken       string `json:"callback_token,omitempty"`
	FullStatusUpdates   bool   `json:"full_status_updates,omitempty"`
	LastDeliveredStatus string `json:"last_delivered_status,omitempty"`
	RetryCount          int    `json:"retry_count,omitempty"`
	NextRetryUnixNs     int64  `json:"next_retry_at,omitempty"`
	CreatedUnixNs       int64  `json:"created_at"`
}

func (s storedSubmission) toModel() *models.Submission {
	out := &models.Submission{
		SubmissionID:        s.SubmissionID,
		TxID:                s.TxID,
		CallbackURL:         s.CallbackURL,
		CallbackToken:       s.CallbackToken,
		FullStatusUpdates:   s.FullStatusUpdates,
		LastDeliveredStatus: models.Status(s.LastDeliveredStatus),
		RetryCount:          s.RetryCount,
	}
	if s.CreatedUnixNs != 0 {
		out.CreatedAt = time.Unix(0, s.CreatedUnixNs)
	}
	if s.NextRetryUnixNs != 0 {
		t := time.Unix(0, s.NextRetryUnixNs)
		out.NextRetryAt = &t
	}
	return out
}

type storedLease struct {
	Holder          string `json:"holder"`
	ExpiresUnixNs   int64  `json:"expires_at"`
}

type storedBump struct {
	BlockHeight uint64 `json:"block_height"`
	BumpData    []byte `json:"bump_data"`
}

type storedDatahubEndpoint struct {
	URL              string `json:"url"`
	Source           string `json:"source"`
	LastSeenUnixNs   int64  `json:"last_seen"`
}

// New opens a Pebble database at cfg.Path and returns a Store ready to use.
// If the directory does not exist it's created. The returned Store takes an
// exclusive file lock on the directory — closing the Store releases it.
func New(cfg config.Pebble) (*Store, error) {
	opts := &pebbledb.Options{}
	if cfg.MemTableSizeMB > 0 {
		opts.MemTableSize = uint64(cfg.MemTableSizeMB) << 20
	}
	if cfg.L0CompactionThreshold > 0 {
		opts.L0CompactionThreshold = cfg.L0CompactionThreshold
	}

	db, err := pebbledb.Open(cfg.Path, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble at %s: %w", cfg.Path, err)
	}

	return &Store{
		db:        db,
		writeOpts: &pebbledb.WriteOptions{Sync: cfg.SyncWrites},
		cfg:       cfg,
	}, nil
}

// Close flushes any in-memory writes and releases the file lock.
func (s *Store) Close() error {
	if err := s.db.Flush(); err != nil {
		// Best-effort flush; always try to close.
		_ = s.db.Close()
		return fmt.Errorf("flush on close: %w", err)
	}
	return s.db.Close()
}

// EnsureIndexes is a no-op — Pebble's indexes are just prefix key ranges
// that are written atomically with primary rows. There's nothing to
// provision at startup.
func (s *Store) EnsureIndexes() error { return nil }

func (s *Store) shardFor(txid string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(txid))
	return &s.insertMu[h.Sum32()%getOrInsertShards]
}

// --- Transaction status ---

func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Serialise per-txid so concurrent callers don't both see "not found"
	// and both insert. Pebble has no CAS, so the mutex is our ordering.
	mu := s.shardFor(status.TxID)
	mu.Lock()
	defer mu.Unlock()

	if existing, err := s.readStatus(status.TxID); err != nil {
		return nil, false, err
	} else if existing != nil {
		return existing, false, nil
	}

	now := time.Now()
	if status.Timestamp.IsZero() {
		status.Timestamp = now
	}
	if status.Status == "" {
		status.Status = models.StatusReceived
	}
	status.CreatedAt = now

	if err := s.writeStatusNew(status); err != nil {
		return nil, false, err
	}
	return status, true, nil
}

func (s *Store) readStatus(txid string) (*models.TransactionStatus, error) {
	v, closer, err := s.db.Get(txKey(txid))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get tx %s: %w", txid, err)
	}
	defer closer.Close()
	var st storedStatus
	if err := json.Unmarshal(v, &st); err != nil {
		return nil, fmt.Errorf("unmarshal tx %s: %w", txid, err)
	}
	return st.toModel(), nil
}

// writeStatusNew writes a brand-new TransactionStatus row together with all
// of its secondary index entries in a single atomic batch.
func (s *Store) writeStatusNew(status *models.TransactionStatus) error {
	st := fromModel(status)
	payload, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal tx %s: %w", status.TxID, err)
	}

	b := s.db.NewBatch()
	defer b.Close()

	if err := b.Set(txKey(status.TxID), payload, nil); err != nil {
		return err
	}
	s.addStatusIndexes(b, st)
	return b.Commit(s.writeOpts)
}

// addStatusIndexes records the secondary index entries for a status row.
// Caller must write these atomically with the primary row.
func (s *Store) addStatusIndexes(b *pebbledb.Batch, st storedStatus) {
	_ = b.Set(idxTxStatusKey(st.Status, st.TxID), nil, nil)
	if st.BlockHash != "" {
		_ = b.Set(idxTxBlockKey(st.BlockHash, st.TxID), nil, nil)
	}
	if st.Status == string(models.StatusPendingRetry) && st.NextRetryUnixNs != 0 {
		_ = b.Set(idxTxRetryReadyKey(st.NextRetryUnixNs, st.TxID), nil, nil)
	}
	if st.TimestampUnixNs != 0 {
		_ = b.Set(idxTxUpdatedKey(st.TimestampUnixNs, st.TxID), nil, nil)
	}
}

// removeStatusIndexes removes the secondary index entries for a status row.
func (s *Store) removeStatusIndexes(b *pebbledb.Batch, st storedStatus) {
	_ = b.Delete(idxTxStatusKey(st.Status, st.TxID), nil)
	if st.BlockHash != "" {
		_ = b.Delete(idxTxBlockKey(st.BlockHash, st.TxID), nil)
	}
	if st.Status == string(models.StatusPendingRetry) && st.NextRetryUnixNs != 0 {
		_ = b.Delete(idxTxRetryReadyKey(st.NextRetryUnixNs, st.TxID), nil)
	}
	if st.TimestampUnixNs != 0 {
		_ = b.Delete(idxTxUpdatedKey(st.TimestampUnixNs, st.TxID), nil)
	}
}

// UpdateStatus replaces the status row for status.TxID. It's a full rewrite:
// any existing secondary index entries for the previous version are deleted
// BatchGetOrInsertStatus runs GetOrInsertStatus concurrently for each row.
// Pebble is single-process and operations are already memory-fast; the
// parallel-loop fallback is the simplest correct path here.
func (s *Store) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return store.BatchGetOrInsertStatusParallel(ctx, s, statuses)
}

// BatchUpdateStatus runs UpdateStatus concurrently for each row.
func (s *Store) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	return store.BatchUpdateStatusParallel(ctx, s, statuses)
}

// and the new set is written in the same batch, so an intermediate query
// never sees a stale index pointing at a row with a different status.
func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Lock per-txid for consistent read-modify-write. The caller may be
	// partially-filled (e.g., only Status + Timestamp set) — we merge with
	// the existing row so other fields are preserved.
	mu := s.shardFor(status.TxID)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(status.TxID)
	if err != nil {
		return err
	}

	merged := mergeStatus(existing, status)
	payload, err := json.Marshal(merged)
	if err != nil {
		return err
	}

	b := s.db.NewBatch()
	defer b.Close()
	if existing != nil {
		s.removeStatusIndexes(b, *existing)
	}
	if err := b.Set(txKey(status.TxID), payload, nil); err != nil {
		return err
	}
	s.addStatusIndexes(b, merged)
	return b.Commit(s.writeOpts)
}

// mergeStatus applies the fields set on update onto existing. Empty strings,
// zero timestamps, and zero heights are treated as "keep current" — matching
// the Aerospike backend's BinMap behavior where a missing bin is a no-op.
func mergeStatus(existing *storedStatus, update *models.TransactionStatus) storedStatus {
	var out storedStatus
	if existing != nil {
		out = *existing
	}
	out.TxID = update.TxID
	if update.Status != "" {
		out.Status = string(update.Status)
	}
	if update.StatusCode != 0 {
		out.StatusCode = update.StatusCode
	}
	if update.BlockHash != "" {
		out.BlockHash = update.BlockHash
	}
	if update.BlockHeight > 0 {
		out.BlockHeight = update.BlockHeight
	}
	if len(update.MerklePath) > 0 {
		out.MerklePath = []byte(update.MerklePath)
	}
	if update.ExtraInfo != "" {
		out.ExtraInfo = update.ExtraInfo
	}
	if len(update.CompetingTxs) > 0 {
		out.CompetingTxs = update.CompetingTxs
	}
	if len(update.RawTx) > 0 {
		out.RawTx = []byte(update.RawTx)
	}
	if update.RetryCount > 0 {
		out.RetryCount = update.RetryCount
	}
	if !update.Timestamp.IsZero() {
		out.TimestampUnixNs = update.Timestamp.UnixNano()
	}
	if !update.CreatedAt.IsZero() {
		out.CreatedUnixNs = update.CreatedAt.UnixNano()
	}
	if !update.NextRetryAt.IsZero() {
		out.NextRetryUnixNs = update.NextRetryAt.UnixNano()
	}
	return out
}

func (s *Store) readStoredStatus(txid string) (*storedStatus, error) {
	v, closer, err := s.db.Get(txKey(txid))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get tx %s: %w", txid, err)
	}
	defer closer.Close()
	var st storedStatus
	if err := json.Unmarshal(v, &st); err != nil {
		return nil, fmt.Errorf("unmarshal tx %s: %w", txid, err)
	}
	return &st, nil
}

func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st, err := s.readStatus(txid)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, nil
	}
	s.enrichMerklePath(ctx, st)
	return st, nil
}

// GetStatusesSince walks the updated-at index newest-first. The since filter
// is client-side because index keys are ordered ascending; a ReverseIter
// cutoff at the since timestamp would require a descending encoding we don't
// need given the expected cardinality.
func (s *Store) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := idxTxUpdatedPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []*models.TransactionStatus
	sinceNs := since.UnixNano()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		txid := lastSegment(iter.Key())
		st, err := s.readStatus(txid)
		if err != nil || st == nil {
			continue
		}
		if !since.IsZero() && st.Timestamp.UnixNano() < sinceNs {
			continue
		}
		results = append(results, st)
	}
	return results, nil
}

// SetStatusByBlockHash walks idx:tx:block:<blockHash>:* and rewrites each
// referenced row with the new status. For SEEN_ON_NETWORK transitions block
// fields are cleared (matches Aerospike contract); for IMMUTABLE they're kept.
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := idxTxBlockPrefix(blockHash)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}

	var txids []string
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			iter.Close()
			return txids, err
		}
		txids = append(txids, lastSegment(iter.Key()))
	}
	if err := iter.Close(); err != nil {
		return txids, err
	}

	clearBlock := newStatus == models.StatusSeenOnNetwork
	for _, txid := range txids {
		mu := s.shardFor(txid)
		mu.Lock()

		existing, err := s.readStoredStatus(txid)
		if err != nil || existing == nil {
			mu.Unlock()
			continue
		}

		updated := *existing
		updated.Status = string(newStatus)
		updated.TimestampUnixNs = time.Now().UnixNano()
		if clearBlock {
			updated.BlockHash = ""
			updated.BlockHeight = 0
		}

		payload, err := json.Marshal(updated)
		if err != nil {
			mu.Unlock()
			return txids, err
		}

		b := s.db.NewBatch()
		s.removeStatusIndexes(b, *existing)
		if err := b.Set(txKey(txid), payload, nil); err != nil {
			b.Close()
			mu.Unlock()
			return txids, err
		}
		s.addStatusIndexes(b, updated)
		err = b.Commit(s.writeOpts)
		b.Close()
		mu.Unlock()
		if err != nil {
			return txids, err
		}
	}
	return txids, nil
}

// BumpRetryCount is read-modify-write under the per-txid lock. The mutex is
// load-bearing: two concurrent bumps without it would both read the same
// count and both increment to count+1 instead of count+2.
func (s *Store) BumpRetryCount(ctx context.Context, txid string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	mu := s.shardFor(txid)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(txid)
	if err != nil {
		return 0, err
	}
	if existing == nil {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, store.ErrNotFound)
	}
	existing.RetryCount++
	payload, err := json.Marshal(existing)
	if err != nil {
		return 0, err
	}
	if err := s.db.Set(txKey(txid), payload, s.writeOpts); err != nil {
		return 0, err
	}
	return existing.RetryCount, nil
}

func (s *Store) SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(txid)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(txid)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, store.ErrNotFound)
	}

	updated := *existing
	updated.Status = string(models.StatusPendingRetry)
	updated.RawTx = rawTx
	updated.NextRetryUnixNs = nextRetryAt.UnixNano()
	updated.TimestampUnixNs = time.Now().UnixNano()

	payload, err := json.Marshal(updated)
	if err != nil {
		return err
	}

	b := s.db.NewBatch()
	defer b.Close()
	s.removeStatusIndexes(b, *existing)
	if err := b.Set(txKey(txid), payload, nil); err != nil {
		return err
	}
	s.addStatusIndexes(b, updated)
	return b.Commit(s.writeOpts)
}

// GetReadyRetries uses a snapshot so concurrent BumpRetryCount or index
// rewrites don't produce duplicate or missing entries during the scan.
// The snapshot is held only for the scan window.
func (s *Store) GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snap := s.db.NewSnapshot()
	defer snap.Close()

	prefix := idxTxRetryReadyPrefix()
	iter, err := snap.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	nowNs := now.UnixNano()
	results := make([]*store.PendingRetry, 0, limit)
	for iter.First(); iter.Valid() && len(results) < limit; iter.Next() {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		// Key shape: idx:tx:retry_ready:<hex-ns>:<txid>. Parse the hex prefix
		// to drop entries that aren't yet due.
		key := iter.Key()
		rest := key[len(prefix):]
		if len(rest) < 17 || rest[16] != ':' {
			continue
		}
		var nextNs int64
		_, err := fmt.Sscanf(string(rest[:16]), "%x", &nextNs)
		if err != nil {
			continue
		}
		if nextNs > nowNs {
			break // remaining entries are in the future
		}
		txid := string(rest[17:])

		v, closer, err := snap.Get(txKey(txid))
		if errors.Is(err, pebbledb.ErrNotFound) {
			continue
		}
		if err != nil {
			return results, err
		}
		var st storedStatus
		if err := json.Unmarshal(v, &st); err != nil {
			closer.Close()
			return results, err
		}
		closer.Close()
		if len(st.RawTx) == 0 {
			continue
		}
		results = append(results, &store.PendingRetry{
			TxID:        st.TxID,
			RawTx:       st.RawTx,
			RetryCount:  st.RetryCount,
			NextRetryAt: time.Unix(0, st.NextRetryUnixNs),
		})
	}
	return results, nil
}

func (s *Store) ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mu := s.shardFor(txid)
	mu.Lock()
	defer mu.Unlock()

	existing, err := s.readStoredStatus(txid)
	if err != nil {
		return err
	}
	if existing == nil {
		return nil
	}

	updated := *existing
	updated.Status = string(finalStatus)
	updated.TimestampUnixNs = time.Now().UnixNano()
	updated.RawTx = nil
	updated.NextRetryUnixNs = 0
	if extraInfo != "" {
		updated.ExtraInfo = extraInfo
	}

	payload, err := json.Marshal(updated)
	if err != nil {
		return err
	}

	b := s.db.NewBatch()
	defer b.Close()
	s.removeStatusIndexes(b, *existing)
	if err := b.Set(txKey(txid), payload, nil); err != nil {
		return err
	}
	s.addStatusIndexes(b, updated)
	return b.Commit(s.writeOpts)
}

// SetMinedByTxIDs updates only rows that already exist — matching the
// Aerospike contract where absent txids are silently skipped.
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, txids []string) ([]*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now()
	var out []*models.TransactionStatus
	for _, txid := range txids {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		mu := s.shardFor(txid)
		mu.Lock()

		existing, err := s.readStoredStatus(txid)
		if err != nil {
			mu.Unlock()
			return out, err
		}
		if existing == nil {
			mu.Unlock()
			continue
		}

		updated := *existing
		updated.Status = string(models.StatusMined)
		updated.BlockHash = blockHash
		updated.TimestampUnixNs = now.UnixNano()

		payload, err := json.Marshal(updated)
		if err != nil {
			mu.Unlock()
			return out, err
		}

		b := s.db.NewBatch()
		s.removeStatusIndexes(b, *existing)
		if err := b.Set(txKey(txid), payload, nil); err != nil {
			b.Close()
			mu.Unlock()
			return out, err
		}
		s.addStatusIndexes(b, updated)
		err = b.Commit(s.writeOpts)
		b.Close()
		mu.Unlock()
		if err != nil {
			return out, err
		}

		out = append(out, &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			BlockHash: blockHash,
			Timestamp: now,
		})
	}
	return out, nil
}

// --- BUMP / STUMP ---

func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(storedBump{BlockHeight: blockHeight, BumpData: bumpData})
	if err != nil {
		return err
	}
	return s.db.Set(bumpKey(blockHash), payload, s.writeOpts)
}

func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	v, closer, err := s.db.Get(bumpKey(blockHash))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return 0, nil, store.ErrNotFound
	}
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	defer closer.Close()
	var b storedBump
	if err := json.Unmarshal(v, &b); err != nil {
		return 0, nil, err
	}
	return b.BlockHeight, b.BumpData, nil
}

func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(stump)
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(stumpKey(stump.BlockHash, stump.SubtreeIndex), payload, nil); err != nil {
		return err
	}
	if err := b.Set(idxStumpBlockKey(stump.BlockHash, stump.SubtreeIndex), nil, nil); err != nil {
		return err
	}
	return b.Commit(s.writeOpts)
}

func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := stumpBlockPrefix(blockHash)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var stumps []*models.Stump
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return stumps, err
		}
		var st models.Stump
		if err := json.Unmarshal(iter.Value(), &st); err != nil {
			continue
		}
		stumps = append(stumps, &st)
	}
	return stumps, nil
}

func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prefix := stumpBlockPrefix(blockHash)
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return err
	}

	var toDelete []*models.Stump
	for iter.First(); iter.Valid(); iter.Next() {
		var st models.Stump
		if err := json.Unmarshal(iter.Value(), &st); err != nil {
			continue
		}
		toDelete = append(toDelete, &st)
	}
	if err := iter.Close(); err != nil {
		return err
	}

	b := s.db.NewBatch()
	defer b.Close()
	for _, st := range toDelete {
		_ = b.Delete(stumpKey(st.BlockHash, st.SubtreeIndex), nil)
		_ = b.Delete(idxStumpBlockKey(st.BlockHash, st.SubtreeIndex), nil)
	}
	return b.Commit(s.writeOpts)
}

// --- Submissions ---

func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stored := storedSubmission{
		SubmissionID:      sub.SubmissionID,
		TxID:              sub.TxID,
		CallbackURL:       sub.CallbackURL,
		CallbackToken:     sub.CallbackToken,
		FullStatusUpdates: sub.FullStatusUpdates,
		RetryCount:        sub.RetryCount,
	}
	if !sub.CreatedAt.IsZero() {
		stored.CreatedUnixNs = sub.CreatedAt.UnixNano()
	}
	if sub.LastDeliveredStatus != "" {
		stored.LastDeliveredStatus = string(sub.LastDeliveredStatus)
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(subKey(sub.SubmissionID), payload, nil); err != nil {
		return err
	}
	if sub.TxID != "" {
		_ = b.Set(idxSubTxIDKey(sub.TxID, sub.SubmissionID), nil, nil)
	}
	if sub.CallbackToken != "" {
		_ = b.Set(idxSubTokenKey(sub.CallbackToken, sub.SubmissionID), nil, nil)
	}
	return b.Commit(s.writeOpts)
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	return s.submissionsByIndex(ctx, idxSubTxIDPrefix(txid))
}

func (s *Store) GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error) {
	return s.submissionsByIndex(ctx, idxSubTokenPrefix(callbackToken))
}

func (s *Store) submissionsByIndex(ctx context.Context, prefix []byte) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var subs []*models.Submission
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return subs, err
		}
		id := lastSegment(iter.Key())
		v, closer, err := s.db.Get(subKey(id))
		if errors.Is(err, pebbledb.ErrNotFound) {
			continue
		}
		if err != nil {
			return subs, err
		}
		var ss storedSubmission
		if err := json.Unmarshal(v, &ss); err != nil {
			closer.Close()
			continue
		}
		closer.Close()
		subs = append(subs, ss.toModel())
	}
	return subs, nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v, closer, err := s.db.Get(subKey(submissionID))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var ss storedSubmission
	if err := json.Unmarshal(v, &ss); err != nil {
		closer.Close()
		return err
	}
	closer.Close()

	ss.LastDeliveredStatus = string(lastStatus)
	ss.RetryCount = retryCount
	if nextRetry != nil {
		ss.NextRetryUnixNs = nextRetry.UnixNano()
	} else {
		ss.NextRetryUnixNs = 0
	}

	payload, err := json.Marshal(ss)
	if err != nil {
		return err
	}
	return s.db.Set(subKey(submissionID), payload, s.writeOpts)
}

// --- Leaser ---

// TryAcquireOrRenew uses a single mutex across all lease names — standalone
// mode is single-process so contention is negligible, and a process-wide
// mutex is easier to reason about than striping across names. The TTL is
// enforced on read: a lease whose ExpiresAt has elapsed is treated as vacant.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	now := time.Now()
	expires := now.Add(ttl)

	v, closer, err := s.db.Get(leaseKey(name))
	if err != nil && !errors.Is(err, pebbledb.ErrNotFound) {
		return time.Time{}, fmt.Errorf("read lease %s: %w", name, err)
	}
	if err == nil {
		var cur storedLease
		if jerr := json.Unmarshal(v, &cur); jerr == nil {
			closer.Close()
			if cur.Holder != holder && cur.ExpiresUnixNs > now.UnixNano() {
				return time.Time{}, nil
			}
		} else {
			closer.Close()
		}
	}

	payload, err := json.Marshal(storedLease{Holder: holder, ExpiresUnixNs: expires.UnixNano()})
	if err != nil {
		return time.Time{}, err
	}
	if err := s.db.Set(leaseKey(name), payload, s.writeOpts); err != nil {
		return time.Time{}, err
	}
	return expires, nil
}

func (s *Store) Release(ctx context.Context, name, holder string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()

	v, closer, err := s.db.Get(leaseKey(name))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var cur storedLease
	if jerr := json.Unmarshal(v, &cur); jerr == nil {
		closer.Close()
		if cur.Holder != holder {
			return nil
		}
	} else {
		closer.Close()
	}
	return s.db.Delete(leaseKey(name), s.writeOpts)
}

// --- Datahub endpoint registry ---

func (s *Store) UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ep.URL == "" {
		return fmt.Errorf("upsert datahub endpoint: empty url")
	}
	stored := storedDatahubEndpoint{
		URL:    ep.URL,
		Source: ep.Source,
	}
	if !ep.LastSeen.IsZero() {
		stored.LastSeenUnixNs = ep.LastSeen.UnixNano()
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	return s.db.Set(datahubEndpointKey(ep.URL), payload, s.writeOpts)
}

func (s *Store) ListDatahubEndpoints(ctx context.Context) ([]store.DatahubEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := datahubEndpointPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: endOfPrefix(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var out []store.DatahubEndpoint
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		var row storedDatahubEndpoint
		if err := json.Unmarshal(iter.Value(), &row); err != nil {
			continue
		}
		ep := store.DatahubEndpoint{
			URL:    row.URL,
			Source: row.Source,
		}
		if row.LastSeenUnixNs != 0 {
			ep.LastSeen = time.Unix(0, row.LastSeenUnixNs)
		}
		out = append(out, ep)
	}
	return out, nil
}

// --- Helpers ---

// enrichMerklePath attaches the per-tx minimal merkle path for mined/immutable
// rows, extracting it from the compound BUMP. Matches the Aerospike behavior.
func (s *Store) enrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	if status == nil || len(status.MerklePath) > 0 || status.BlockHash == "" {
		return
	}
	if status.Status != models.StatusMined && status.Status != models.StatusImmutable {
		return
	}
	_, bumpData, err := s.GetBUMP(ctx, status.BlockHash)
	if err != nil || len(bumpData) == 0 {
		return
	}
	status.MerklePath = extractMinimalPathForTx(bumpData, status.TxID)
}

func extractMinimalPathForTx(bumpData []byte, txid string) []byte {
	compound, err := transaction.NewMerklePathFromBinary(bumpData)
	if err != nil {
		return nil
	}
	txHash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return nil
	}

	var txOffset uint64
	found := false
	if len(compound.Path) > 0 {
		for _, leaf := range compound.Path[0] {
			if leaf.Hash != nil && *leaf.Hash == *txHash {
				txOffset = leaf.Offset
				found = true
				break
			}
		}
	}
	if !found {
		return nil
	}

	mp := &transaction.MerklePath{
		BlockHeight: compound.BlockHeight,
		Path:        make([][]*transaction.PathElement, len(compound.Path)),
	}
	offset := txOffset
	for level := 0; level < len(compound.Path); level++ {
		if level == 0 {
			for _, leaf := range compound.Path[level] {
				if leaf.Offset == offset {
					mp.Path[level] = append(mp.Path[level], leaf)
					break
				}
			}
		}
		sibOffset := offset ^ 1
		for _, leaf := range compound.Path[level] {
			if leaf.Offset == sibOffset {
				mp.Path[level] = append(mp.Path[level], leaf)
				break
			}
		}
		offset = offset >> 1
	}
	return mp.Bytes()
}

