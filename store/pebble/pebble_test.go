package pebble

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := New(config.Pebble{
		Path:                  dir,
		MemTableSizeMB:        4,
		L0CompactionThreshold: 2,
		SyncWrites:            false,
	})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGetOrInsertStatus_InsertsNew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	in := &models.TransactionStatus{TxID: "abc", Status: models.StatusReceived}
	got, inserted, err := s.GetOrInsertStatus(ctx, in)
	if err != nil {
		t.Fatalf("GetOrInsertStatus: %v", err)
	}
	if !inserted {
		t.Fatal("expected inserted=true for new txid")
	}
	if got.TxID != "abc" || got.Status != models.StatusReceived {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestGetOrInsertStatus_ReturnsExisting(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	first := &models.TransactionStatus{TxID: "abc", Status: models.StatusReceived}
	_, inserted, err := s.GetOrInsertStatus(ctx, first)
	if err != nil || !inserted {
		t.Fatalf("first insert: inserted=%v err=%v", inserted, err)
	}

	second := &models.TransactionStatus{TxID: "abc", Status: models.StatusSentToNetwork}
	got, inserted, err := s.GetOrInsertStatus(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("expected inserted=false for existing txid")
	}
	if got.Status != models.StatusReceived {
		t.Fatalf("expected existing status RECEIVED, got %s", got.Status)
	}
}

// TestGetOrInsertStatus_ConcurrentRace verifies that N goroutines inserting
// the same txid collapse to a single insert — proving the per-txid mutex
// actually serializes read-modify-write.
func TestGetOrInsertStatus_ConcurrentRace(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const N = 100
	var wg sync.WaitGroup
	var insertedCount int64
	var mu sync.Mutex

	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, ok, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID: "racey", Status: models.StatusReceived,
			})
			if err != nil {
				t.Errorf("concurrent insert: %v", err)
				return
			}
			if ok {
				mu.Lock()
				insertedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if insertedCount != 1 {
		t.Fatalf("expected exactly 1 successful insert, got %d", insertedCount)
	}
}

func TestUpdateStatus_ClearsOldStatusIndex(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Seed with RECEIVED then transition to SENT_TO_NETWORK.
	txid := "tx1"
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusSentToNetwork, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	// The RECEIVED index entry should no longer resolve to this txid.
	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != models.StatusSentToNetwork {
		t.Fatalf("expected SENT_TO_NETWORK, got %s", got.Status)
	}

	// Manually verify no ghost index entry by scanning RECEIVED index.
	v, closer, err := s.db.Get(idxTxStatusKey(string(models.StatusReceived), txid))
	if err == nil {
		closer.Close()
		t.Fatal("old status index entry was not cleaned up — ghost row risk")
	}
	_ = v
}

func TestPendingRetryLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	txid := "retry-tx"
	rawTx := []byte{0x01, 0x02}
	nextRetry := time.Now().Add(-time.Second) // already due

	// Seed the row first.
	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: txid, Status: models.StatusReceived}); err != nil {
		t.Fatal(err)
	}

	n, err := s.BumpRetryCount(ctx, txid)
	if err != nil || n != 1 {
		t.Fatalf("BumpRetryCount: n=%d err=%v", n, err)
	}

	if err := s.SetPendingRetryFields(ctx, txid, rawTx, nextRetry); err != nil {
		t.Fatal(err)
	}

	ready, err := s.GetReadyRetries(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || ready[0].TxID != txid {
		t.Fatalf("GetReadyRetries: %+v", ready)
	}
	if ready[0].RetryCount != 1 {
		t.Fatalf("expected RetryCount=1, got %d", ready[0].RetryCount)
	}

	if err := s.ClearRetryState(ctx, txid, models.StatusRejected, "final"); err != nil {
		t.Fatal(err)
	}
	ready, err = s.GetReadyRetries(ctx, time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready retries after clear, got %d", len(ready))
	}
}

func TestGetReadyRetries_SkipsFutureEntries(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now()
	cases := []struct {
		txid    string
		delay   time.Duration
		isReady bool
	}{
		{"past-1", -2 * time.Second, true},
		{"past-2", -time.Second, true},
		{"future-1", time.Hour, false},
	}
	for _, c := range cases {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{TxID: c.txid, Status: models.StatusReceived}); err != nil {
			t.Fatal(err)
		}
		if err := s.SetPendingRetryFields(ctx, c.txid, []byte{0xff}, now.Add(c.delay)); err != nil {
			t.Fatal(err)
		}
	}

	ready, err := s.GetReadyRetries(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	gotReady := map[string]bool{}
	for _, r := range ready {
		gotReady[r.TxID] = true
	}
	for _, c := range cases {
		if gotReady[c.txid] != c.isReady {
			t.Errorf("%s: isReady=%v, got=%v", c.txid, c.isReady, gotReady[c.txid])
		}
	}
}

func TestSetStatusByBlockHash_UpdatesAllInBlock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	blockHash := "bh-1"
	txids := []string{"t1", "t2", "t3"}
	for _, txid := range txids {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusMined, BlockHash: blockHash, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Transition to SEEN_ON_NETWORK (reorg path) — block fields should clear.
	updated, err := s.SetStatusByBlockHash(ctx, blockHash, models.StatusSeenOnNetwork)
	if err != nil {
		t.Fatal(err)
	}
	if len(updated) != 3 {
		t.Fatalf("expected 3 updated txids, got %d", len(updated))
	}
	for _, txid := range txids {
		got, _ := s.GetStatus(ctx, txid)
		if got == nil || got.Status != models.StatusSeenOnNetwork {
			t.Errorf("%s: expected SEEN_ON_NETWORK, got %+v", txid, got)
		}
		if got.BlockHash != "" {
			t.Errorf("%s: expected empty BlockHash after reorg, got %s", txid, got.BlockHash)
		}
	}
}

func TestSubmissions_InsertAndQueryByTxID(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	sub := &models.Submission{
		SubmissionID: "sub-1",
		TxID:         "tx-a",
		CallbackURL:  "https://example.test/cb",
		CreatedAt:    time.Now(),
	}
	if err := s.InsertSubmission(ctx, sub); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetSubmissionsByTxID(ctx, "tx-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SubmissionID != "sub-1" {
		t.Fatalf("GetSubmissionsByTxID: %+v", got)
	}
}

func TestLease_AcquireAndRenew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	held, err := s.TryAcquireOrRenew(ctx, "reaper", "holder-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if held.IsZero() {
		t.Fatal("expected non-zero heldUntil for fresh lease")
	}

	// Same holder can renew.
	renewed, err := s.TryAcquireOrRenew(ctx, "reaper", "holder-a", time.Second)
	if err != nil || renewed.IsZero() {
		t.Fatalf("renew: heldUntil=%v err=%v", renewed, err)
	}

	// Different holder is blocked.
	blocked, err := s.TryAcquireOrRenew(ctx, "reaper", "holder-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !blocked.IsZero() {
		t.Fatal("expected zero heldUntil for contention")
	}
}

// Verify BumpRetryCount returns ErrNotFound for unknown txids so callers can
// distinguish real errors from a row that was cleared concurrently.
func TestBumpRetryCount_UnknownTxID(t *testing.T) {
	s := newTestStore(t)
	_, err := s.BumpRetryCount(context.Background(), "ghost")
	if err == nil {
		t.Fatal("expected error for unknown txid")
	}
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDatahubEndpoints_UpsertAndList(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	in := []store.DatahubEndpoint{
		{URL: "https://a.example", Source: store.DatahubEndpointSourceConfigured, LastSeen: now},
		{URL: "https://b.example", Source: store.DatahubEndpointSourceDiscovered, LastSeen: now.Add(time.Minute)},
	}
	for _, ep := range in {
		if err := s.UpsertDatahubEndpoint(ctx, ep); err != nil {
			t.Fatalf("upsert %s: %v", ep.URL, err)
		}
	}

	out, err := s.ListDatahubEndpoints(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 endpoints, got %d: %+v", len(out), out)
	}
	got := map[string]store.DatahubEndpoint{}
	for _, ep := range out {
		got[ep.URL] = ep
	}
	for _, want := range in {
		gotEp, ok := got[want.URL]
		if !ok {
			t.Fatalf("missing endpoint %s", want.URL)
		}
		if gotEp.Source != want.Source {
			t.Errorf("%s source: got %q want %q", want.URL, gotEp.Source, want.Source)
		}
		if !gotEp.LastSeen.Equal(want.LastSeen) {
			t.Errorf("%s last_seen: got %v want %v", want.URL, gotEp.LastSeen, want.LastSeen)
		}
	}
}

func TestDatahubEndpoints_UpsertOverwrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t1 := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	if err := s.UpsertDatahubEndpoint(ctx, store.DatahubEndpoint{
		URL: "https://a.example", Source: store.DatahubEndpointSourceConfigured, LastSeen: t1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDatahubEndpoint(ctx, store.DatahubEndpoint{
		URL: "https://a.example", Source: store.DatahubEndpointSourceDiscovered, LastSeen: t2,
	}); err != nil {
		t.Fatal(err)
	}

	out, err := s.ListDatahubEndpoints(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 endpoint after upsert overwrite, got %d", len(out))
	}
	if out[0].Source != store.DatahubEndpointSourceDiscovered {
		t.Errorf("source not overwritten: %q", out[0].Source)
	}
	if !out[0].LastSeen.Equal(t2) {
		t.Errorf("last_seen not overwritten: got %v want %v", out[0].LastSeen, t2)
	}
}

func TestDatahubEndpoints_RejectsEmptyURL(t *testing.T) {
	s := newTestStore(t)
	err := s.UpsertDatahubEndpoint(context.Background(), store.DatahubEndpoint{
		URL: "", Source: store.DatahubEndpointSourceDiscovered, LastSeen: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for empty URL")
	}
}
