package store

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/arcade/models"
)

const (
	// ConfirmationsRequired is the number of blocks after mining before removing from tracker
	ConfirmationsRequired = 100

	// loadFromStoreBatchSize is the number of statuses LoadFromStore processes
	// per batch before handing the kept rows to the tracker. Streaming through
	// the store one row at a time is enough to bound peak memory; batching
	// just amortizes the lock acquisition. 10k keeps the lock-held window
	// short while still covering hundreds of thousands of rows in a handful
	// of acquisitions.
	loadFromStoreBatchSize = 10000
)

// TrackedTx holds the status for a tracked transaction
type TrackedTx struct {
	Status      models.Status
	MinedHeight uint64 // 0 if not yet mined
}

// TxTracker maintains an in-memory set of tracked transaction IDs for O(1) lookups.
// This avoids unnecessary database queries when processing subtrees where most
// txids won't be in our system. Stores the current known status for each txid.
type TxTracker struct {
	mu    sync.RWMutex
	txids map[chainhash.Hash]TrackedTx
}

// NewTxTracker creates a new transaction tracker
func NewTxTracker() *TxTracker {
	return &TxTracker{
		txids: make(map[chainhash.Hash]TrackedTx),
	}
}

// TrackerRowIterator narrows the Store surface LoadFromStore actually needs so
// tests can supply a fake without standing up every Store method. Any
// implementation of Store satisfies this implicitly.
type TrackerRowIterator interface {
	IterateTrackerRows(ctx context.Context, scan TrackerScan, fn func(TrackerRow) error) error
}

// LoadStats reports what one hydration pass did.
//
// Scanned counts rows the store emitted; Loaded counts rows that reached the
// tracker map. Skipped counts rows the store emitted that TrackerScan.Keep
// then rejected — with a correct server-side pushdown this is ~0, so a
// nonzero value in production is the signal that a backend's filter has
// drifted from the Go predicate (see store/storetest).
type LoadStats struct {
	Scanned int
	Loaded  int
	Skipped int
}

// LoadFromStore populates the tracker from the store, streaming rows in
// fixed-size batches. The store filters server-side per scan, so peak memory
// is bounded by the kept set rather than the full history depth — which is
// what matters at startup on systems with months of accumulated transactions.
//
// scan carries the MINED pruning cutoff. Build it with NewTrackerScan so an
// unknown chain height cannot be mistaken for genesis: a zero height silently
// disables pruning and loads the entire mined history (issue #276).
func (t *TxTracker) LoadFromStore(ctx context.Context, st TrackerRowIterator, scan TrackerScan) (LoadStats, error) {
	return t.loadFromStore(ctx, st, scan, loadFromStoreBatchSize)
}

// loadFromStore is the batchSize-parameterized form of LoadFromStore so tests
// can drive the batching boundary without inflating fixture sizes.
func (t *TxTracker) loadFromStore(
	ctx context.Context, st TrackerRowIterator, scan TrackerScan, batchSize int,
) (LoadStats, error) {
	if batchSize <= 0 {
		batchSize = loadFromStoreBatchSize
	}

	type kept struct {
		hash chainhash.Hash
		tx   TrackedTx
	}
	batch := make([]kept, 0, batchSize)
	stats := LoadStats{}

	flush := func() {
		if len(batch) == 0 {
			return
		}
		t.mu.Lock()
		for _, k := range batch {
			t.txids[k.hash] = k.tx
		}
		t.mu.Unlock()
		stats.Loaded += len(batch)
		batch = batch[:0]
	}

	err := st.IterateTrackerRows(ctx, scan, func(row TrackerRow) error {
		stats.Scanned++

		// Re-apply the predicate the store was asked to push down. Backends
		// may legitimately over-emit (an index that cannot express the height
		// cutoff), so this is what keeps the kept set exact regardless of how
		// much of the filter a given backend managed server-side.
		if !scan.Keep(row.Status, row.BlockHeight) {
			stats.Skipped++
			return nil
		}

		hash, err := chainhash.NewHashFromHex(row.TxID)
		if err != nil {
			return nil //nolint:nilerr // malformed txid: skip the row, keep loading.
		}
		batch = append(batch, kept{
			hash: *hash,
			tx: TrackedTx{
				Status:      row.Status,
				MinedHeight: row.BlockHeight,
			},
		})
		if len(batch) >= batchSize {
			flush()
		}
		return nil
	})
	if err != nil {
		// Surface the error but keep whatever we already merged so the
		// tracker isn't left empty on a transient store hiccup mid-scan.
		flush()
		return stats, err
	}
	flush()
	return stats, nil
}

// Add adds a txid to the tracker with initial status (hex string)
func (t *TxTracker) Add(txid string, status models.Status) {
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return
	}
	t.mu.Lock()
	t.txids[*hash] = TrackedTx{Status: status}
	t.mu.Unlock()
}

// AddHash adds a chainhash.Hash to the tracker with status
func (t *TxTracker) AddHash(hash chainhash.Hash, status models.Status) {
	t.mu.Lock()
	t.txids[hash] = TrackedTx{Status: status}
	t.mu.Unlock()
}

// UpdateStatus updates the status for a tracked txid
func (t *TxTracker) UpdateStatus(txid string, status models.Status) {
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return
	}
	t.mu.Lock()
	if tx, ok := t.txids[*hash]; ok {
		tx.Status = status
		t.txids[*hash] = tx
	}
	t.mu.Unlock()
}

// UpdateStatusHash updates the status for a tracked hash
func (t *TxTracker) UpdateStatusHash(hash chainhash.Hash, status models.Status) {
	t.mu.Lock()
	if tx, ok := t.txids[hash]; ok {
		tx.Status = status
		t.txids[hash] = tx
	}
	t.mu.Unlock()
}

// SetMined marks a transaction as mined at the given block height
func (t *TxTracker) SetMined(txid string, blockHeight uint64) {
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return
	}
	t.mu.Lock()
	if tx, ok := t.txids[*hash]; ok {
		tx.Status = models.StatusMined
		tx.MinedHeight = blockHeight
		t.txids[*hash] = tx
	}
	t.mu.Unlock()
}

// SetMinedHash marks a transaction as mined at the given block height
func (t *TxTracker) SetMinedHash(hash chainhash.Hash, blockHeight uint64) {
	t.mu.Lock()
	if tx, ok := t.txids[hash]; ok {
		tx.Status = models.StatusMined
		tx.MinedHeight = blockHeight
		t.txids[hash] = tx
	}
	t.mu.Unlock()
}

// PruneConfirmed finds transactions that have been mined for at least 100 blocks,
// returning their hashes so they can be marked as IMMUTABLE before removal.
func (t *TxTracker) PruneConfirmed(currentHeight uint64) []chainhash.Hash {
	t.mu.Lock()
	defer t.mu.Unlock()

	var immutable []chainhash.Hash
	for hash, tx := range t.txids {
		if tx.Status == models.StatusMined && tx.MinedHeight > 0 {
			if currentHeight >= tx.MinedHeight+ConfirmationsRequired {
				immutable = append(immutable, hash)
				delete(t.txids, hash)
			}
		}
	}
	return immutable
}

// Remove removes a txid from the tracker (hex string)
func (t *TxTracker) Remove(txid string) {
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return
	}
	t.mu.Lock()
	delete(t.txids, *hash)
	t.mu.Unlock()
}

// RemoveHash removes a chainhash.Hash from the tracker
func (t *TxTracker) RemoveHash(hash chainhash.Hash) {
	t.mu.Lock()
	delete(t.txids, hash)
	t.mu.Unlock()
}

// Contains checks if a txid is being tracked (hex string)
func (t *TxTracker) Contains(txid string) bool {
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return false
	}
	t.mu.RLock()
	_, ok := t.txids[*hash]
	t.mu.RUnlock()
	return ok
}

// ContainsHash checks if a chainhash.Hash is being tracked
func (t *TxTracker) ContainsHash(hash chainhash.Hash) bool {
	t.mu.RLock()
	_, ok := t.txids[hash]
	t.mu.RUnlock()
	return ok
}

// GetStatus returns the current status for a txid, or empty string if not tracked
func (t *TxTracker) GetStatus(txid string) (models.Status, bool) {
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return "", false
	}
	t.mu.RLock()
	tx, ok := t.txids[*hash]
	t.mu.RUnlock()
	return tx.Status, ok
}

// GetStatusHash returns the current status for a hash, or empty string if not tracked
func (t *TxTracker) GetStatusHash(hash chainhash.Hash) (models.Status, bool) {
	t.mu.RLock()
	tx, ok := t.txids[hash]
	t.mu.RUnlock()
	return tx.Status, ok
}

// FilterTrackedHashes returns only the hashes that are being tracked.
// Optimized for batch processing - locks once for the entire batch.
func (t *TxTracker) FilterTrackedHashes(hashes []chainhash.Hash) []chainhash.Hash {
	t.mu.RLock()
	defer t.mu.RUnlock()

	matched := make([]chainhash.Hash, 0)
	for _, hash := range hashes {
		if _, ok := t.txids[hash]; ok {
			matched = append(matched, hash)
		}
	}
	return matched
}

// FilterTrackedTxids is the hex-string form of FilterTrackedHashes. Returns
// the subset of txids the tracker knows about (preserving input order) plus
// the count of unknown txids — callers use the count to surface "STUMP
// contained N leaves not watched by this arcade" without re-scanning.
//
// Malformed hex inputs are counted as unknown rather than rejected; the
// caller is the BUMP builder, which receives txids derived from on-disk
// stump bytes already validated upstream, so a parse failure here is
// programmer error worth a metric tick but not a hard stop.
func (t *TxTracker) FilterTrackedTxids(txids []string) (tracked []string, unknown int) {
	if len(txids) == 0 {
		return nil, 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	tracked = make([]string, 0, len(txids))
	for _, s := range txids {
		hash, err := chainhash.NewHashFromHex(s)
		if err != nil {
			unknown++
			continue
		}
		if _, ok := t.txids[*hash]; ok {
			tracked = append(tracked, s)
		} else {
			unknown++
		}
	}
	return tracked, unknown
}

// Count returns the number of tracked txids
func (t *TxTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.txids)
}
