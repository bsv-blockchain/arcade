package pebble

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
)

// All-new batch insert: every txid is new, every result is Inserted=true,
// every row exists after the call.
func TestBatchGetOrInsertStatus_AllNew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := make([]*models.TransactionStatus, 50)
	for i := range in {
		in[i] = &models.TransactionStatus{TxID: makeBatchTxID(i), Status: models.StatusReceived}
	}

	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	if len(results) != 50 {
		t.Fatalf("len(results) = %d, want 50", len(results))
	}
	for i, r := range results {
		if !r.Inserted {
			t.Errorf("results[%d].Inserted = false, want true", i)
		}
	}
	// Confirm every row landed in the store via single-row reads.
	for _, st := range in {
		got, err := s.GetStatus(ctx, st.TxID)
		if err != nil || got == nil {
			t.Errorf("GetStatus(%s) = (%v, %v)", st.TxID, got, err)
		}
	}
}

// All-duplicates: every txid was already inserted by a previous batch. Each
// result must be Inserted=false with Existing populated.
func TestBatchGetOrInsertStatus_AllDuplicates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := []*models.TransactionStatus{
		{TxID: "dup-1", Status: models.StatusReceived},
		{TxID: "dup-2", Status: models.StatusReceived},
	}
	if _, err := s.BatchGetOrInsertStatus(ctx, in); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Mutate one row out-of-band so we can confirm Existing reflects the
	// stored row, not the input.
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{TxID: "dup-1", Status: models.StatusSeenOnNetwork}); err != nil {
		t.Fatalf("update: %v", err)
	}

	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("second batch: %v", err)
	}
	for i, r := range results {
		if r.Inserted {
			t.Errorf("results[%d].Inserted = true, want false (txid %s)", i, in[i].TxID)
		}
		if r.Existing == nil {
			t.Errorf("results[%d].Existing is nil", i)
		}
	}
	// The mutated row should report SEEN_ON_NETWORK, not the input's RECEIVED.
	if results[0].Existing.Status != models.StatusSeenOnNetwork {
		t.Errorf("dup-1.Existing.Status = %q, want %q",
			results[0].Existing.Status, models.StatusSeenOnNetwork)
	}
}

// Mixed batch (some new, some duplicates) must preserve input ordering in
// the output. tx_validator's phaseDedup relies on this for per-position
// stitching.
func TestBatchGetOrInsertStatus_MixedPreservesOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Seed two of the five txids first.
	if _, err := s.BatchGetOrInsertStatus(ctx, []*models.TransactionStatus{
		{TxID: "mix-2"},
		{TxID: "mix-4"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	in := []*models.TransactionStatus{
		{TxID: "mix-1"}, {TxID: "mix-2"}, {TxID: "mix-3"}, {TxID: "mix-4"}, {TxID: "mix-5"},
	}
	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	wantInserted := []bool{true, false, true, false, true}
	for i, r := range results {
		if r.Inserted != wantInserted[i] {
			t.Errorf("results[%d].Inserted = %v, want %v", i, r.Inserted, wantInserted[i])
		}
	}
}

// Race: concurrent batches with overlapping txids should leave each txid
// inserted exactly once. Surfaces any locking gap in the per-record path
// the parallel-loop helper depends on.
func TestBatchGetOrInsertStatus_ConcurrentRace(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const goroutines = 8
	const batchSize = 30
	in := make([]*models.TransactionStatus, batchSize)
	for i := range in {
		in[i] = &models.TransactionStatus{TxID: makeBatchTxID(i)}
	}

	var insertedTotal int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := s.BatchGetOrInsertStatus(ctx, in)
			if err != nil {
				t.Errorf("BatchGetOrInsertStatus: %v", err)
				return
			}
			n := 0
			for _, r := range results {
				if r.Inserted {
					n++
				}
			}
			mu.Lock()
			insertedTotal += n
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Across all goroutines, exactly batchSize total inserts must have
	// happened — every txid is new the first time it's seen, then a
	// duplicate forever after.
	if insertedTotal != batchSize {
		t.Errorf("insertedTotal = %d, want %d", insertedTotal, batchSize)
	}
}

// Empty input must be a no-op.
func TestBatchGetOrInsertStatus_Empty(t *testing.T) {
	s := newTestStore(t)
	if r, err := s.BatchGetOrInsertStatus(context.Background(), nil); err != nil || r != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", r, err)
	}
}

// BatchUpdateStatus: every row's Status must be reflected in subsequent
// GetStatus calls.
func TestBatchUpdateStatus_All(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Seed three rows.
	in := []*models.TransactionStatus{
		{TxID: "upd-1"}, {TxID: "upd-2"}, {TxID: "upd-3"},
	}
	if _, err := s.BatchGetOrInsertStatus(ctx, in); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Bulk-update them all to REJECTED with a reason.
	updates := []*models.TransactionStatus{
		{TxID: "upd-1", Status: models.StatusRejected, ExtraInfo: "bad-1"},
		{TxID: "upd-2", Status: models.StatusRejected, ExtraInfo: "bad-2"},
		{TxID: "upd-3", Status: models.StatusRejected, ExtraInfo: "bad-3"},
	}
	if err := s.BatchUpdateStatus(ctx, updates); err != nil {
		t.Fatalf("BatchUpdateStatus: %v", err)
	}

	for _, u := range updates {
		got, err := s.GetStatus(ctx, u.TxID)
		if err != nil || got == nil {
			t.Fatalf("GetStatus(%s): err=%v got=%v", u.TxID, err, got)
		}
		if got.Status != models.StatusRejected {
			t.Errorf("%s.Status = %q, want REJECTED", u.TxID, got.Status)
		}
		if got.ExtraInfo != u.ExtraInfo {
			t.Errorf("%s.ExtraInfo = %q, want %q", u.TxID, got.ExtraInfo, u.ExtraInfo)
		}
	}
}

func makeBatchTxID(i int) string {
	const hex = "0123456789abcdef"
	var b [8]byte
	for k := 0; k < 8; k++ {
		b[k] = hex[(i>>(k*4))&0xf]
	}
	return "batch-" + string(b[:])
}
