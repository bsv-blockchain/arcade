package store

import (
	"context"
	"sync"
	"time"

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

// statusIterator narrows the Store surface LoadFromStore actually needs so
// tests can supply a fake without standing up every Store method. Any
// implementation of Store satisfies this implicitly.
type statusIterator interface {
	IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error
}

// LoadFromStore populates the tracker from the store, streaming rows in
// fixed-size batches and dropping deeply-confirmed transactions before they
// reach the tracker map. Peak memory is bounded by loadFromStoreBatchSize
// rather than the full history depth, which matters at startup on systems
// with months of accumulated transactions.
func (t *TxTracker) LoadFromStore(ctx context.Context, store Store, currentHeight uint64) (int, error) {
	return t.loadFromStore(ctx, store, currentHeight, loadFromStoreBatchSize)
}

// loadFromStore is the batchSize-parameterized form of LoadFromStore so tests
// can drive the batching boundary without inflating fixture sizes.
func (t *TxTracker) loadFromStore(ctx context.Context, store statusIterator, currentHeight uint64, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = loadFromStoreBatchSize
	}

	type kept struct {
		hash chainhash.Hash
		tx   TrackedTx
	}
	batch := make([]kept, 0, batchSize)
	count := 0

	flush := func() {
		if len(batch) == 0 {
			return
		}
		t.mu.Lock()
		for _, k := range batch {
			t.txids[k.hash] = k.tx
		}
		t.mu.Unlock()
		count += len(batch)
		batch = batch[:0]
	}

	err := store.IterateStatusesSince(ctx, time.Time{}, func(status *models.TransactionStatus) error {
		// Skip transactions that are deeply confirmed — these would only be
		// pruned moments later, so never let them touch the tracker map.
		if status.Status == models.StatusMined && status.BlockHeight > 0 {
			if currentHeight >= status.BlockHeight+ConfirmationsRequired {
				return nil
			}
		}

		hash, err := chainhash.NewHashFromHex(status.TxID)
		if err != nil {
			return nil //nolint:nilerr // malformed txid: skip the row, keep loading.
		}
		batch = append(batch, kept{
			hash: *hash,
			tx: TrackedTx{
				Status:      status.Status,
				MinedHeight: status.BlockHeight,
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
		return count, err
	}
	flush()
	return count, nil
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

// Count returns the number of tracked txids
func (t *TxTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.txids)
}
