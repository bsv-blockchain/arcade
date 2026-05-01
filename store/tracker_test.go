package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/arcade/models"
)

// fakeIterStore is a minimal statusIterator stub that yields a fixed set of
// rows and tracks the maximum number of rows held in memory by the tracker
// during the scan, so tests can assert peak memory is bounded by batch size
// rather than total history depth.
type fakeIterStore struct {
	rows   []*models.TransactionStatus
	tr     *TxTracker
	maxLen int
	yields int
	err    error
}

func (f *fakeIterStore) IterateStatusesSince(_ context.Context, _ time.Time, fn func(*models.TransactionStatus) error) error {
	for _, r := range f.rows {
		f.yields++
		if err := fn(r); err != nil {
			return err
		}
		// Snapshot the tracker map size after each yield. If LoadFromStore
		// truly batches+flushes, the size walks up in plateaus rather than
		// monotonically tracking f.yields.
		if f.tr != nil {
			if n := f.tr.Count(); n > f.maxLen {
				f.maxLen = n
			}
		}
	}
	return f.err
}

// txidHex turns a small int into a deterministic 32-byte hex txid for tests.
func txidHex(i int) string {
	return fmt.Sprintf("%064x", i+1)
}

func TestTxTracker_AddAndContains(t *testing.T) {
	tracker := NewTxTracker()
	tracker.Add("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)

	if !tracker.Contains("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f") {
		t.Error("expected tracker to contain added txid")
	}

	if tracker.Contains("ff01020304050607ff090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f") {
		t.Error("expected tracker not to contain unknown txid")
	}
}

func TestTxTracker_FilterTrackedHashes(t *testing.T) {
	tracker := NewTxTracker()

	hash1, _ := chainhash.NewHashFromHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	hash2, _ := chainhash.NewHashFromHex("ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	hash3, _ := chainhash.NewHashFromHex("aa0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")

	tracker.AddHash(*hash1, models.StatusReceived)
	tracker.AddHash(*hash3, models.StatusSeenOnNetwork)

	input := []chainhash.Hash{*hash1, *hash2, *hash3}
	tracked := tracker.FilterTrackedHashes(input)

	if len(tracked) != 2 {
		t.Fatalf("expected 2 tracked hashes, got %d", len(tracked))
	}
}

func TestTxTracker_UpdateStatus(t *testing.T) {
	tracker := NewTxTracker()
	tracker.Add("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)

	tracker.UpdateStatus("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusMined)

	status, ok := tracker.GetStatus("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	if !ok {
		t.Fatal("expected to find status")
	}
	if status != models.StatusMined {
		t.Errorf("expected MINED, got %s", status)
	}
}

func TestTxTracker_Count(t *testing.T) {
	tracker := NewTxTracker()
	if tracker.Count() != 0 {
		t.Errorf("expected 0, got %d", tracker.Count())
	}

	tracker.Add("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)
	tracker.Add("ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", models.StatusReceived)

	if tracker.Count() != 2 {
		t.Errorf("expected 2, got %d", tracker.Count())
	}
}

func TestTxTracker_LoadFromStore_DropsDeeplyConfirmed(t *testing.T) {
	const currentHeight = uint64(1_000_000)
	rows := []*models.TransactionStatus{
		// Old + deeply confirmed: should be dropped.
		{TxID: txidHex(0), Status: models.StatusMined, BlockHeight: currentHeight - ConfirmationsRequired - 1},
		{TxID: txidHex(1), Status: models.StatusMined, BlockHeight: currentHeight - ConfirmationsRequired - 5000},
		// Recently mined but not deeply confirmed: keep.
		{TxID: txidHex(2), Status: models.StatusMined, BlockHeight: currentHeight - 10},
		// In flight (no block yet): keep.
		{TxID: txidHex(3), Status: models.StatusSeenOnNetwork},
	}

	tracker := NewTxTracker()
	store := &fakeIterStore{rows: rows, tr: tracker}

	got, err := tracker.loadFromStore(context.Background(), store, currentHeight, 2)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if got != 2 {
		t.Fatalf("expected 2 kept rows, got %d", got)
	}
	if tracker.Count() != 2 {
		t.Fatalf("expected tracker to contain 2 rows, got %d", tracker.Count())
	}
	if tracker.Contains(txidHex(0)) || tracker.Contains(txidHex(1)) {
		t.Fatal("deeply confirmed rows should not be in tracker")
	}
	if !tracker.Contains(txidHex(2)) || !tracker.Contains(txidHex(3)) {
		t.Fatal("expected recent rows to be tracked")
	}
}

func TestTxTracker_LoadFromStore_BoundedPeakMemory(t *testing.T) {
	// Many old rows that will be pruned + a small tail of recent rows. If
	// LoadFromStore materialized the full history before pruning, the
	// tracker map would briefly hold all of them. With paged loading the
	// peak is bounded by batchSize because deeply confirmed rows are
	// filtered before they ever land in the map.
	const (
		currentHeight = uint64(1_000_000)
		oldRows       = 1000
		recentRows    = 5
		batchSize     = 10
	)

	rows := make([]*models.TransactionStatus, 0, oldRows+recentRows)
	for i := 0; i < oldRows; i++ {
		rows = append(rows, &models.TransactionStatus{
			TxID:        txidHex(i),
			Status:      models.StatusMined,
			BlockHeight: currentHeight - ConfirmationsRequired - 100,
		})
	}
	for i := 0; i < recentRows; i++ {
		rows = append(rows, &models.TransactionStatus{
			TxID:   txidHex(oldRows + i),
			Status: models.StatusSeenOnNetwork,
		})
	}

	tracker := NewTxTracker()
	store := &fakeIterStore{rows: rows, tr: tracker}

	got, err := tracker.loadFromStore(context.Background(), store, currentHeight, batchSize)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if got != recentRows {
		t.Fatalf("expected %d kept rows, got %d", recentRows, got)
	}
	if tracker.Count() != recentRows {
		t.Fatalf("expected tracker count %d, got %d", recentRows, tracker.Count())
	}
	if store.yields != oldRows+recentRows {
		t.Fatalf("expected store to stream %d rows, got %d", oldRows+recentRows, store.yields)
	}
	// Peak in-tracker count must never exceed what we kept — deeply
	// confirmed rows are dropped before the map mutation.
	if store.maxLen > recentRows {
		t.Fatalf("peak tracker size %d exceeded kept rows %d (paged prune leaked old rows)", store.maxLen, recentRows)
	}
}

func TestTxTracker_LoadFromStore_FlushesOnBatchBoundary(t *testing.T) {
	// All rows are recent so every row is kept. With batchSize=4 and 10
	// rows the tracker map should grow in batched steps (4, 8, 10) rather
	// than per-row. fakeIterStore samples the size after each yield, so a
	// minimum sampled size of <= batchSize-after-first-flush proves the
	// tracker doesn't merge until a batch is full.
	const (
		currentHeight = uint64(500)
		total         = 10
		batchSize     = 4
	)

	rows := make([]*models.TransactionStatus, 0, total)
	for i := 0; i < total; i++ {
		rows = append(rows, &models.TransactionStatus{
			TxID:   txidHex(i),
			Status: models.StatusSeenOnNetwork,
		})
	}

	tracker := NewTxTracker()
	store := &fakeIterStore{rows: rows, tr: tracker}

	got, err := tracker.loadFromStore(context.Background(), store, currentHeight, batchSize)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if got != total {
		t.Fatalf("expected %d kept rows, got %d", total, got)
	}
	if tracker.Count() != total {
		t.Fatalf("expected tracker count %d, got %d", total, tracker.Count())
	}
	// fakeIterStore samples right after each yield (before the post-iter
	// flush), so we should see batched plateaus: the size grows by
	// batchSize at a time, never one-by-one. With total=10 and batch=4
	// the largest mid-scan plateau is 8 (two flushes); the trailing 2 rows
	// land in the final flush after iteration completes.
	if store.maxLen != 8 {
		t.Fatalf("expected mid-scan peak of 8 (two batched flushes), got %d", store.maxLen)
	}
}

func TestTxTracker_LoadFromStore_PropagatesIterError(t *testing.T) {
	wantErr := errors.New("boom")
	rows := []*models.TransactionStatus{
		{TxID: txidHex(0), Status: models.StatusSeenOnNetwork},
	}
	tracker := NewTxTracker()
	store := &fakeIterStore{rows: rows, tr: tracker, err: wantErr}

	_, err := tracker.loadFromStore(context.Background(), store, 100, 4)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error %v, got %v", wantErr, err)
	}
	// Even on iter error we should have flushed the rows we already saw.
	if tracker.Count() != 1 {
		t.Fatalf("expected partial flush of 1, got %d", tracker.Count())
	}
}

func TestTxTracker_SetMinedAndPrune(t *testing.T) {
	tracker := NewTxTracker()
	txid := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	tracker.Add(txid, models.StatusReceived)

	tracker.SetMined(txid, 1000)

	// Not deep enough to prune
	pruned := tracker.PruneConfirmed(1050)
	if len(pruned) != 0 {
		t.Error("should not prune before 100 confirmations")
	}

	// Deep enough to prune
	pruned = tracker.PruneConfirmed(1101)
	if len(pruned) != 1 {
		t.Errorf("expected 1 pruned, got %d", len(pruned))
	}

	if tracker.Contains(txid) {
		t.Error("expected txid removed after pruning")
	}
}
