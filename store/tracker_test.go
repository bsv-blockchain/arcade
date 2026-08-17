package store

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/arcade/models"
)

// fakeIterStore is a minimal TrackerRowIterator stub that yields a fixed set of
// rows and tracks the maximum number of rows held in memory by the tracker
// during the scan, so tests can assert peak memory is bounded by batch size
// rather than total history depth.
//
// By default it emits every row WITHOUT applying scan.Keep, modelling the
// least selective backend the store contract permits (one whose index cannot
// express the height cutoff). That is deliberately the adversarial case: it
// exercises the client-side re-check in loadFromStore, which is what keeps the
// kept set exact no matter how much filtering a backend managed server-side.
// Set pushdown to model a backend that filters server-side instead.
type fakeIterStore struct {
	rows     []*models.TransactionStatus
	tr       *TxTracker
	pushdown bool
	maxLen   int
	yields   int
	err      error
}

func (f *fakeIterStore) IterateTrackerRows(_ context.Context, scan TrackerScan, fn func(TrackerRow) error) error {
	for _, r := range f.rows {
		if f.pushdown && !scan.Keep(r.Status, r.BlockHeight) {
			continue
		}
		f.yields++
		if err := fn(TrackerRow{
			TxID:        r.TxID,
			Status:      r.Status,
			BlockHeight: r.BlockHeight,
		}); err != nil {
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

	stats, err := tracker.loadFromStore(context.Background(), store, NewTrackerScan(currentHeight, true), 2)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if stats.Loaded != 2 {
		t.Fatalf("expected 2 kept rows, got %d", stats.Loaded)
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

func TestTxTracker_LoadFromStore_DropsTerminalStatuses(t *testing.T) {
	// Regression for the arcade-v2 OOM: hydration loaded terminal rows
	// (REJECTED was ~44% of a 5M-row store) into the tracker map even though a
	// terminal tx can never appear in a mined subtree. Only in-flight statuses
	// and recently-mined rows (needed for PruneConfirmed) belong in the tracker.
	const currentHeight = uint64(1_000_000)
	rows := []*models.TransactionStatus{
		// Terminal, never mined: must be dropped.
		{TxID: txidHex(0), Status: models.StatusRejected},
		{TxID: txidHex(1), Status: models.StatusDoubleSpendAttempted},
		{TxID: txidHex(2), Status: models.StatusImmutable, BlockHeight: currentHeight - 10},
		// Deeply confirmed mined: dropped (existing behavior).
		{TxID: txidHex(3), Status: models.StatusMined, BlockHeight: currentHeight - ConfirmationsRequired - 1},
		// Keep: in-flight + recently mined (still needs pruning tracking).
		{TxID: txidHex(4), Status: models.StatusSeenOnNetwork},
		{TxID: txidHex(5), Status: models.StatusReceived},
		{TxID: txidHex(6), Status: models.StatusMined, BlockHeight: currentHeight - 10},
	}

	tracker := NewTxTracker()
	store := &fakeIterStore{rows: rows, tr: tracker}

	stats, err := tracker.loadFromStore(context.Background(), store, NewTrackerScan(currentHeight, true), 100)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if stats.Loaded != 3 {
		t.Fatalf("expected 3 kept rows, got %d", stats.Loaded)
	}
	for _, drop := range []int{0, 1, 2, 3} {
		if tracker.Contains(txidHex(drop)) {
			t.Errorf("txid %d (%s) should NOT be tracked", drop, rows[drop].Status)
		}
	}
	for _, keep := range []int{4, 5, 6} {
		if !tracker.Contains(txidHex(keep)) {
			t.Errorf("txid %d (%s) should be tracked", keep, rows[keep].Status)
		}
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

	stats, err := tracker.loadFromStore(context.Background(), store, NewTrackerScan(currentHeight, true), batchSize)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if stats.Loaded != recentRows {
		t.Fatalf("expected %d kept rows, got %d", recentRows, stats.Loaded)
	}
	if tracker.Count() != recentRows {
		t.Fatalf("expected tracker count %d, got %d", recentRows, tracker.Count())
	}
	if store.yields != oldRows+recentRows {
		t.Fatalf("expected store to stream %d rows, got %d", oldRows+recentRows, store.yields)
	}
	// This fake does no server-side filtering, so every deeply-confirmed row it
	// emitted must have been rejected by the client-side re-check.
	if stats.Scanned != oldRows+recentRows {
		t.Fatalf("expected %d scanned, got %d", oldRows+recentRows, stats.Scanned)
	}
	if stats.Skipped != oldRows {
		t.Fatalf("expected %d skipped by the client-side re-check, got %d", oldRows, stats.Skipped)
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

	stats, err := tracker.loadFromStore(context.Background(), store, NewTrackerScan(currentHeight, true), batchSize)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if stats.Loaded != total {
		t.Fatalf("expected %d kept rows, got %d", total, stats.Loaded)
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

	_, err := tracker.loadFromStore(context.Background(), store, NewTrackerScan(100, true), 4)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error %v, got %v", wantErr, err)
	}
	// Even on iter error we should have flushed the rows we already saw.
	if tracker.Count() != 1 {
		t.Fatalf("expected partial flush of 1, got %d", tracker.Count())
	}
}

func TestTxTracker_LoadFromStore_UnknownHeightKeepsAllMined(t *testing.T) {
	// The v0.13.0 production condition: no chain height available. Every MINED
	// row must survive, however deeply confirmed — under-pruning is safe, and
	// silently pruning against a bogus height would not be.
	rows := []*models.TransactionStatus{
		{TxID: txidHex(0), Status: models.StatusMined, BlockHeight: 1},
		{TxID: txidHex(1), Status: models.StatusMined, BlockHeight: 500_000},
		{TxID: txidHex(2), Status: models.StatusSeenOnNetwork},
		// Terminal rows are still dropped: that filter does not depend on height.
		{TxID: txidHex(3), Status: models.StatusRejected},
	}

	tracker := NewTxTracker()
	store := &fakeIterStore{rows: rows, tr: tracker}

	stats, err := tracker.loadFromStore(context.Background(), store, NewTrackerScan(900_000, false), 100)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if stats.Loaded != 3 {
		t.Fatalf("expected 3 kept rows with unknown height, got %d", stats.Loaded)
	}
	for _, keep := range []int{0, 1, 2} {
		if !tracker.Contains(txidHex(keep)) {
			t.Errorf("txid %d should be kept when the chain height is unknown", keep)
		}
	}
	if tracker.Contains(txidHex(3)) {
		t.Error("REJECTED must be dropped regardless of height availability")
	}
}

func TestTxTracker_LoadFromStore_MinedWithoutHeightSurvivesHighCutoff(t *testing.T) {
	// A MINED row with no recorded block height cannot be judged deeply
	// confirmed, so no cutoff may sweep it away. Easy to lose in a
	// `WHERE block_height >= $cutoff` pushdown.
	rows := []*models.TransactionStatus{
		{TxID: txidHex(0), Status: models.StatusMined, BlockHeight: 0},
	}
	tracker := NewTxTracker()
	store := &fakeIterStore{rows: rows, tr: tracker}

	stats, err := tracker.loadFromStore(context.Background(), store, NewTrackerScan(900_000, true), 100)
	if err != nil {
		t.Fatalf("loadFromStore: %v", err)
	}
	if stats.Loaded != 1 || !tracker.Contains(txidHex(0)) {
		t.Fatalf("MINED row with block_height 0 must be kept, loaded=%d", stats.Loaded)
	}
}

func TestTxTracker_LoadFromStore_PushdownMatchesClientSideFilter(t *testing.T) {
	// The store contract lets a backend push the predicate down or not. Both
	// must produce byte-identical tracker contents; only the Scanned/Skipped
	// counters may differ. This is the in-package half of the anti-drift
	// guarantee (store/storetest covers the real backends).
	const currentHeight = uint64(1_000_000)
	rows := []*models.TransactionStatus{
		{TxID: txidHex(0), Status: models.StatusMined, BlockHeight: currentHeight - ConfirmationsRequired - 1},
		{TxID: txidHex(1), Status: models.StatusMined, BlockHeight: currentHeight - 10},
		{TxID: txidHex(2), Status: models.StatusMined, BlockHeight: 0},
		{TxID: txidHex(3), Status: models.StatusSeenOnNetwork},
		{TxID: txidHex(4), Status: models.StatusRejected},
		{TxID: txidHex(5), Status: models.StatusImmutable, BlockHeight: currentHeight - 5},
		{TxID: txidHex(6), Status: models.StatusUnknown},
	}
	scan := NewTrackerScan(currentHeight, true)

	lax := NewTxTracker()
	laxStats, err := lax.loadFromStore(context.Background(), &fakeIterStore{rows: rows}, scan, 3)
	if err != nil {
		t.Fatalf("lax loadFromStore: %v", err)
	}

	strict := NewTxTracker()
	strictStats, err := strict.loadFromStore(context.Background(), &fakeIterStore{rows: rows, pushdown: true}, scan, 3)
	if err != nil {
		t.Fatalf("pushdown loadFromStore: %v", err)
	}

	if laxStats.Loaded != strictStats.Loaded {
		t.Fatalf("loaded differs: no-pushdown=%d pushdown=%d", laxStats.Loaded, strictStats.Loaded)
	}
	for _, r := range rows {
		if lax.Contains(r.TxID) != strict.Contains(r.TxID) {
			t.Errorf("txid %s: no-pushdown=%v pushdown=%v", r.TxID, lax.Contains(r.TxID), strict.Contains(r.TxID))
		}
	}
	// The counters are exactly where the two differ.
	if strictStats.Skipped != 0 {
		t.Errorf("a pushdown backend should emit nothing Keep rejects, got Skipped=%d", strictStats.Skipped)
	}
	if laxStats.Skipped == 0 {
		t.Error("the no-pushdown fake must exercise the client-side re-check")
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
