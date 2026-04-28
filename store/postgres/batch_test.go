//go:build postgres

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// All-new batch insert: every txid is fresh, every result has Inserted=true.
// The xmax trick depends on RETURNING firing for every row, including ones
// that hit ON CONFLICT — this test pins down the inserted=true side.
func TestBatchGetOrInsertStatus_AllNew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := make([]*models.TransactionStatus, 50)
	for i := range in {
		in[i] = &models.TransactionStatus{TxID: pgBatchTxID(i)}
	}

	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	if len(results) != len(in) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(in))
	}
	for i, r := range results {
		if !r.Inserted {
			t.Errorf("results[%d].Inserted = false, want true (%s)", i, in[i].TxID)
		}
	}
}

// All-duplicates: every txid is already present. Results must report
// Inserted=false and Existing must reflect the persisted row, not the input
// — verified by mutating the row out-of-band before re-running the batch.
func TestBatchGetOrInsertStatus_AllDuplicates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := []*models.TransactionStatus{
		{TxID: "pg-dup-1"}, {TxID: "pg-dup-2"},
	}
	if _, err := s.BatchGetOrInsertStatus(ctx, in); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID: "pg-dup-1", Status: models.StatusSeenOnNetwork,
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	for i, r := range results {
		if r.Inserted {
			t.Errorf("results[%d].Inserted = true, want false", i)
		}
		if r.Existing == nil {
			t.Fatalf("results[%d].Existing is nil", i)
		}
	}
	if results[0].Existing.Status != models.StatusSeenOnNetwork {
		t.Errorf("pg-dup-1 status = %q, want %q",
			results[0].Existing.Status, models.StatusSeenOnNetwork)
	}
}

// Mixed batch: input ordering must be preserved. Postgres doesn't guarantee
// RETURNING order matches input order, so the implementation explicitly
// reassembles by txid. Regressing the reassembly silently breaks tx_validator.
func TestBatchGetOrInsertStatus_MixedPreservesOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, err := s.BatchGetOrInsertStatus(ctx, []*models.TransactionStatus{
		{TxID: "pg-mix-2"}, {TxID: "pg-mix-4"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	in := []*models.TransactionStatus{
		{TxID: "pg-mix-1"}, {TxID: "pg-mix-2"}, {TxID: "pg-mix-3"},
		{TxID: "pg-mix-4"}, {TxID: "pg-mix-5"},
	}
	results, err := s.BatchGetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("BatchGetOrInsertStatus: %v", err)
	}
	wantInserted := []bool{true, false, true, false, true}
	for i, r := range results {
		if r.Inserted != wantInserted[i] {
			t.Errorf("results[%d].Inserted = %v, want %v (%s)",
				i, r.Inserted, wantInserted[i], in[i].TxID)
		}
	}
}

// Concurrent batches with overlapping txids: each unique txid is inserted
// exactly once across all goroutines. Surfaces any gap in the
// ON CONFLICT DO UPDATE semantics under contention.
func TestBatchGetOrInsertStatus_ConcurrentRace(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const goroutines = 8
	const batchSize = 30
	in := make([]*models.TransactionStatus, batchSize)
	for i := range in {
		in[i] = &models.TransactionStatus{TxID: pgBatchTxID(i)}
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

	if insertedTotal != batchSize {
		t.Errorf("insertedTotal = %d, want %d (each txid must insert exactly once across %d goroutines)",
			insertedTotal, batchSize, goroutines)
	}
}

// BatchUpdateStatus: bulk reject. Every row's Status, ExtraInfo, and
// timestamp must be persisted in a single round-trip. Verified by reading
// each row back through the single-row API.
func TestBatchUpdateStatus_BulkReject(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := []*models.TransactionStatus{
		{TxID: "pg-upd-1"}, {TxID: "pg-upd-2"}, {TxID: "pg-upd-3"},
	}
	if _, err := s.BatchGetOrInsertStatus(ctx, in); err != nil {
		t.Fatalf("seed: %v", err)
	}

	updates := []*models.TransactionStatus{
		{TxID: "pg-upd-1", Status: models.StatusRejected, ExtraInfo: "bad-1", Timestamp: time.Now()},
		{TxID: "pg-upd-2", Status: models.StatusRejected, ExtraInfo: "bad-2", Timestamp: time.Now()},
		{TxID: "pg-upd-3", Status: models.StatusRejected, ExtraInfo: "bad-3", Timestamp: time.Now()},
	}
	if err := s.BatchUpdateStatus(ctx, updates); err != nil {
		t.Fatalf("BatchUpdateStatus: %v", err)
	}

	for _, u := range updates {
		got, err := s.GetStatus(ctx, u.TxID)
		if err != nil || got == nil {
			t.Fatalf("GetStatus(%s): %v / %v", u.TxID, got, err)
		}
		if got.Status != models.StatusRejected {
			t.Errorf("%s.Status = %q, want REJECTED", u.TxID, got.Status)
		}
		if got.ExtraInfo != u.ExtraInfo {
			t.Errorf("%s.ExtraInfo = %q, want %q", u.TxID, got.ExtraInfo, u.ExtraInfo)
		}
	}
}

// BatchUpdateStatus partial-update semantics: empty fields don't overwrite
// existing values. Mirrors the single-row UpdateStatus contract via
// COALESCE/NULLIF in the multi-row UPDATE FROM VALUES query.
func TestBatchUpdateStatus_PartialDoesNotClobber(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, err := s.BatchGetOrInsertStatus(ctx, []*models.TransactionStatus{
		{TxID: "pg-pres-1", Status: models.StatusReceived, BlockHash: "block-AAA", BlockHeight: 42},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Update with an empty BlockHash / zero BlockHeight — the existing values
	// must survive.
	if err := s.BatchUpdateStatus(ctx, []*models.TransactionStatus{
		{TxID: "pg-pres-1", Status: models.StatusSeenOnNetwork, Timestamp: time.Now()},
	}); err != nil {
		t.Fatalf("BatchUpdateStatus: %v", err)
	}

	got, err := s.GetStatus(ctx, "pg-pres-1")
	if err != nil || got == nil {
		t.Fatalf("GetStatus: %v / %v", got, err)
	}
	if got.Status != models.StatusSeenOnNetwork {
		t.Errorf("Status = %q, want SEEN_ON_NETWORK", got.Status)
	}
	if got.BlockHash != "block-AAA" {
		t.Errorf("BlockHash = %q, want block-AAA (must not be clobbered)", got.BlockHash)
	}
	if got.BlockHeight != 42 {
		t.Errorf("BlockHeight = %d, want 42 (must not be clobbered)", got.BlockHeight)
	}
}

// Empty input must be a no-op.
func TestBatchOps_Empty(t *testing.T) {
	s := newTestStore(t)
	if r, err := s.BatchGetOrInsertStatus(context.Background(), nil); err != nil || r != nil {
		t.Errorf("expected (nil, nil), got (%v, %v)", r, err)
	}
	if err := s.BatchUpdateStatus(context.Background(), nil); err != nil {
		t.Errorf("expected nil err, got %v", err)
	}
}

// BenchmarkBatchVsSerialInsert compares the batched xmax SQL against the
// per-row INSERT loop the old tx_validator path used. Run with:
//
//	go test -tags=postgres -bench=BatchVsSerialInsert ./store/postgres/...
//
// Results inform the issue's "benchmark" acceptance criterion. Postgres
// embedded is single-threaded so absolute numbers are conservative; what
// matters is the ratio.
func BenchmarkBatchVsSerialInsert(b *testing.B) {
	s := newTestStoreB(b)
	ctx := context.Background()

	rows := make([]*models.TransactionStatus, 200)
	for i := range rows {
		rows[i] = &models.TransactionStatus{TxID: pgBatchTxID(i)}
	}

	b.Run("batched_xmax", func(b *testing.B) {
		// Reset the table before each iteration so every run hits the
		// all-new path.
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			if _, err := s.pool.Exec(ctx, `TRUNCATE transactions`); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if _, err := s.BatchGetOrInsertStatus(ctx, rows); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("serial_loop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			if _, err := s.pool.Exec(ctx, `TRUNCATE transactions`); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			for _, r := range rows {
				if _, _, err := s.GetOrInsertStatus(ctx, r); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// newTestStoreB is the *testing.B counterpart of newTestStore.
func newTestStoreB(b *testing.B) *Store {
	b.Helper()
	dir := b.TempDir()
	cfg := newEmbeddedCfg(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := New(ctx, cfg)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	if err := s.EnsureIndexes(); err != nil {
		_ = s.Close()
		b.Fatalf("EnsureIndexes: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

func pgBatchTxID(i int) string {
	const hex = "0123456789abcdef"
	var buf [8]byte
	for k := 0; k < 8; k++ {
		buf[k] = hex[(i>>(k*4))&0xf]
	}
	return "pg-batch-" + string(buf[:])
}
