package pebble

import (
	"context"
	"errors"
	"fmt"
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
		_ = closer.Close()
		t.Fatal("old status index entry was not cleaned up — ghost row risk")
	}
	_ = v
}

// TestUpdateStatus_TerminalNotOverwritten is the regression for F-003 (#61):
// once a tx is in a strictly-terminal status (MINED, IMMUTABLE,
// DOUBLE_SPEND_ATTEMPTED), a later lower-priority UpdateStatus call (e.g. a
// stray SEEN_ON_NETWORK callback) must be a silent no-op rather than a clobber.
// REJECTED is covered separately in TestUpdateStatus_RejectedRecovery — it
// blocks regressions to pre-broadcast states but allows forward recovery to
// ACCEPTED_BY_NETWORK / SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES so a late peer
// acceptance can correct an earlier rejection.
func TestUpdateStatus_TerminalNotOverwritten(t *testing.T) {
	terminals := []models.Status{
		models.StatusMined,
		models.StatusImmutable,
		models.StatusDoubleSpendAttempted,
	}
	regressions := []models.Status{
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
		models.StatusSentToNetwork,
		models.StatusPendingRetry,
	}
	for _, terminal := range terminals {
		for _, regression := range regressions {
			name := string(terminal) + "_then_" + string(regression)
			t.Run(name, func(t *testing.T) {
				s := newTestStore(t)
				ctx := context.Background()
				txid := "tx-" + name

				// Seed a row in the terminal state.
				if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
					TxID: txid, Status: models.StatusReceived,
				}); err != nil {
					t.Fatal(err)
				}
				if err := s.UpdateStatus(ctx, &models.TransactionStatus{
					TxID: txid, Status: terminal, Timestamp: time.Now(),
				}); err != nil {
					t.Fatalf("seed terminal: %v", err)
				}

				// A late lower-priority callback must not regress the row.
				if err := s.UpdateStatus(ctx, &models.TransactionStatus{
					TxID: txid, Status: regression, Timestamp: time.Now(),
				}); err != nil {
					t.Fatalf("regression update: %v", err)
				}

				got, err := s.GetStatus(ctx, txid)
				if err != nil {
					t.Fatal(err)
				}
				if got.Status != terminal {
					t.Fatalf("terminal status %s was overwritten by %s (got %s)",
						terminal, regression, got.Status)
				}
			})
		}
	}
}

// TestUpdateStatus_RejectedRecovery pins REJECTED's partial-terminal behavior
// at the store layer: forward recovery (ACCEPTED_BY_NETWORK / SEEN_ON_NETWORK
// / SEEN_MULTIPLE_NODES) is allowed so a late callback from a peer that did
// accept the tx can correct the status, but regressions to pre-broadcast or
// retry states must still be silently dropped.
func TestUpdateStatus_RejectedRecovery(t *testing.T) {
	allowedForward := []models.Status{
		models.StatusAcceptedByNetwork,
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
	}
	blockedRegressions := []models.Status{
		models.StatusSentToNetwork,
		models.StatusPendingRetry,
	}
	for _, next := range allowedForward {
		t.Run("REJECTED_then_"+string(next), func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			txid := "tx-rejrec-" + string(next)

			if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusReceived,
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusRejected, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("seed REJECTED: %v", err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: next, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("forward update: %v", err)
			}
			got, err := s.GetStatus(ctx, txid)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != next {
				t.Fatalf("REJECTED → %s wrongly dropped (got %s)", next, got.Status)
			}
		})
	}
	for _, next := range blockedRegressions {
		t.Run("REJECTED_then_"+string(next)+"_blocked", func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			txid := "tx-rejblk-" + string(next)

			if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusReceived,
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: models.StatusRejected, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("seed REJECTED: %v", err)
			}
			if err := s.UpdateStatus(ctx, &models.TransactionStatus{
				TxID: txid, Status: next, Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("regression update: %v", err)
			}
			got, err := s.GetStatus(ctx, txid)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != models.StatusRejected {
				t.Fatalf("REJECTED was overwritten by %s (got %s)", next, got.Status)
			}
		})
	}
}

// TestUpdateStatus_UnknownTxidReturnsErrNotFound is the regression for F-033
// (#91): UpdateStatus on a txid that has no existing row must return
// store.ErrNotFound and must NOT create a phantom record. Previously,
// callbacks pointing at unknown txids could synthesize bogus rows in the
// store, turning the callback endpoint into a write-anywhere primitive.
func TestUpdateStatus_UnknownTxidReturnsErrNotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "ghost-tx"

	err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound for unknown txid, got %v", err)
	}

	// And critically: no phantom row was created.
	got, gerr := s.GetStatus(ctx, txid)
	if gerr != nil {
		t.Fatalf("GetStatus after rejected update: %v", gerr)
	}
	if got != nil {
		t.Fatalf("expected nil status for ghost txid, got %+v", got)
	}

	// The status index must also be empty — i.e. no idx:tx:status:SEEN_ON_NETWORK:ghost-tx.
	v, closer, gErr := s.db.Get(idxTxStatusKey(string(models.StatusSeenOnNetwork), txid))
	if gErr == nil {
		_ = closer.Close()
		t.Fatalf("phantom secondary index entry was written for ghost txid: %x", v)
	}
}

// TestUpdateStatus_ExistingTxidStillWorks is the regression for the happy
// path: the F-033 guard must not break legitimate updates against rows that
// were created via GetOrInsertStatus.
func TestUpdateStatus_ExistingTxidStillWorks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "real-tx"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusReceived,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := s.UpdateStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("UpdateStatus on existing row: %v", err)
	}

	got, err := s.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("expected SEEN_ON_NETWORK, got %+v", got)
	}
}

// TestUpdateStatus_ForwardTransitionsStillWork sanity-checks that the lattice
// guard does not break legitimate forward transitions.
func TestUpdateStatus_ForwardTransitionsStillWork(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	txid := "tx-forward"

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusReceived,
	}); err != nil {
		t.Fatal(err)
	}
	for _, st := range []models.Status{
		models.StatusSentToNetwork,
		models.StatusAcceptedByNetwork,
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
		models.StatusMined,
		models.StatusImmutable,
	} {
		if err := s.UpdateStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: st, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("forward transition to %s: %v", st, err)
		}
		got, err := s.GetStatus(ctx, txid)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != st {
			t.Fatalf("forward transition lost: expected %s, got %s", st, got.Status)
		}
	}
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

	if sErr := s.SetPendingRetryFields(ctx, txid, rawTx, nextRetry); sErr != nil {
		t.Fatal(sErr)
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

	if cErr := s.ClearRetryState(ctx, txid, models.StatusRejected, "final"); cErr != nil {
		t.Fatal(cErr)
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

func TestMarkMerkleRegisteredByTxIDs_UpdatesExistingRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	txids := []string{"mr-1", "mr-2", "mr-3"}
	for _, txid := range txids {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusReceived, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", txid, err)
		}
	}

	ts := time.Now().Add(-5 * time.Minute)
	if err := s.MarkMerkleRegisteredByTxIDs(ctx, txids, ts); err != nil {
		t.Fatalf("mark: %v", err)
	}

	for _, txid := range txids {
		got, err := s.GetStatus(ctx, txid)
		if err != nil {
			t.Fatalf("GetStatus %s: %v", txid, err)
		}
		if got == nil {
			t.Fatalf("%s: missing after mark", txid)
		}
		if !got.MerkleRegisteredAt.Equal(ts) {
			t.Errorf("%s: MerkleRegisteredAt=%v want %v", txid, got.MerkleRegisteredAt, ts)
		}
	}
}

func TestMarkMerkleRegisteredByTxIDs_SkipsUnknownTxIDs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: "known", Status: models.StatusReceived, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.MarkMerkleRegisteredByTxIDs(ctx, []string{"known", "ghost-a", "ghost-b"}, time.Now()); err != nil {
		t.Fatalf("mark: %v", err)
	}

	got, _ := s.GetStatus(ctx, "known")
	if got == nil || got.MerkleRegisteredAt.IsZero() {
		t.Errorf("known: MerkleRegisteredAt should be set, got %+v", got)
	}
	for _, txid := range []string{"ghost-a", "ghost-b"} {
		got, _ := s.GetStatus(ctx, txid)
		if got != nil {
			t.Errorf("%s: unknown txid should not have created a row", txid)
		}
	}
}

func TestMarkMerkleRegisteredByTxIDs_RoundTripsThroughIterate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: "iter-1", Status: models.StatusReceived, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	ts := time.Now()
	if err := s.MarkMerkleRegisteredByTxIDs(ctx, []string{"iter-1"}, ts); err != nil {
		t.Fatal(err)
	}

	var seen *models.TransactionStatus
	if err := s.IterateStatusesSince(ctx, time.Now().Add(-time.Hour), func(st *models.TransactionStatus) error {
		if st.TxID == "iter-1" {
			seen = st
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen == nil {
		t.Fatalf("iter-1 not seen")
	}
	if !seen.MerkleRegisteredAt.Equal(ts) {
		t.Errorf("MerkleRegisteredAt=%v want %v", seen.MerkleRegisteredAt, ts)
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
		{URL: "https://a.example", Network: "mainnet", Source: store.DatahubEndpointSourceConfigured, LastSeen: now},
		{URL: "https://b.example", Network: "mainnet", Source: store.DatahubEndpointSourceDiscovered, LastSeen: now.Add(time.Minute)},
	}
	for _, ep := range in {
		if err := s.UpsertDatahubEndpoint(ctx, ep); err != nil {
			t.Fatalf("upsert %s: %v", ep.URL, err)
		}
	}

	out, err := s.ListDatahubEndpoints(ctx, "mainnet")
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
		if gotEp.Network != want.Network {
			t.Errorf("%s network: got %q want %q", want.URL, gotEp.Network, want.Network)
		}
		if gotEp.Source != want.Source {
			t.Errorf("%s source: got %q want %q", want.URL, gotEp.Source, want.Source)
		}
		if !gotEp.LastSeen.Equal(want.LastSeen) {
			t.Errorf("%s last_seen: got %v want %v", want.URL, gotEp.LastSeen, want.LastSeen)
		}
	}
}

// TestDatahubEndpoints_NetworkScoped asserts that List returns only endpoints
// for the requested network. This is the regression for the bug where a
// regtest pod served mainnet URLs left in the store from a prior run.
func TestDatahubEndpoints_NetworkScoped(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)

	rows := []store.DatahubEndpoint{
		{URL: "https://main-a.example", Network: "mainnet", Source: store.DatahubEndpointSourceDiscovered, LastSeen: now},
		{URL: "https://main-b.example", Network: "mainnet", Source: store.DatahubEndpointSourceDiscovered, LastSeen: now},
		{URL: "https://regtest-a.example", Network: "regtest", Source: store.DatahubEndpointSourceConfigured, LastSeen: now},
	}
	for _, ep := range rows {
		if err := s.UpsertDatahubEndpoint(ctx, ep); err != nil {
			t.Fatalf("upsert %s: %v", ep.URL, err)
		}
	}

	regtest, err := s.ListDatahubEndpoints(ctx, "regtest")
	if err != nil {
		t.Fatalf("list regtest: %v", err)
	}
	if len(regtest) != 1 || regtest[0].URL != "https://regtest-a.example" {
		t.Fatalf("regtest list: got %+v", regtest)
	}

	mainnet, err := s.ListDatahubEndpoints(ctx, "mainnet")
	if err != nil {
		t.Fatalf("list mainnet: %v", err)
	}
	if len(mainnet) != 2 {
		t.Fatalf("mainnet list: got %d entries, want 2: %+v", len(mainnet), mainnet)
	}

	empty, err := s.ListDatahubEndpoints(ctx, "")
	if err != nil {
		t.Fatalf("list empty: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty network filter must not match scoped rows: %+v", empty)
	}
}

func TestDatahubEndpoints_UpsertOverwrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t1 := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	if err := s.UpsertDatahubEndpoint(ctx, store.DatahubEndpoint{
		URL: "https://a.example", Network: "mainnet", Source: store.DatahubEndpointSourceConfigured, LastSeen: t1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDatahubEndpoint(ctx, store.DatahubEndpoint{
		URL: "https://a.example", Network: "mainnet", Source: store.DatahubEndpointSourceDiscovered, LastSeen: t2,
	}); err != nil {
		t.Fatal(err)
	}

	out, err := s.ListDatahubEndpoints(ctx, "mainnet")
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

// --- Block processing status ---

func TestBlockProcessing_Upsert_Header_Then_Processed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "aa11"
	t0 := time.Unix(1700000000, 0).UTC()
	t1 := t0.Add(2 * time.Second)
	t2 := t0.Add(4 * time.Second)

	if err := s.UpsertBlockHeaderSeen(ctx, hash, 100, t0); err != nil {
		t.Fatalf("header seen: %v", err)
	}
	if err := s.MarkBlockProcessed(ctx, hash, 100, t1); err != nil {
		t.Fatalf("processed: %v", err)
	}
	if err := s.MarkBlockBUMPBuilt(ctx, hash, 100, t2); err != nil {
		t.Fatalf("bump built: %v", err)
	}

	got, err := s.GetBlockProcessingStatus(ctx, hash)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.BlockHeight != 100 {
		t.Errorf("BlockHeight=%d, want 100", got.BlockHeight)
	}
	if !got.HeaderSeenAt.Equal(t0) {
		t.Errorf("HeaderSeenAt=%v, want %v", got.HeaderSeenAt, t0)
	}
	if got.ProcessedAt == nil || !got.ProcessedAt.Equal(t1) {
		t.Errorf("ProcessedAt=%v, want %v", got.ProcessedAt, t1)
	}
	if got.BUMPBuiltAt == nil || !got.BUMPBuiltAt.Equal(t2) {
		t.Errorf("BUMPBuiltAt=%v, want %v", got.BUMPBuiltAt, t2)
	}
	if got.Status != models.BlockStatusActive {
		t.Errorf("Status=%q, want active", got.Status)
	}
}

func TestBlockProcessing_OutOfOrder_Processed_Then_Header(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "bb22"
	tProc := time.Unix(1700000010, 0).UTC()
	tSeen := tProc.Add(time.Second)

	// BLOCK_PROCESSED arrives first with height=0 (callback has no height).
	if err := s.MarkBlockProcessed(ctx, hash, 0, tProc); err != nil {
		t.Fatalf("processed: %v", err)
	}
	got, _ := s.GetBlockProcessingStatus(ctx, hash)
	if !got.HeaderSeenAt.Equal(tProc) {
		t.Errorf("synthesized HeaderSeenAt=%v, want %v", got.HeaderSeenAt, tProc)
	}
	if got.BlockHeight != 0 {
		t.Errorf("BlockHeight before header=%d, want 0", got.BlockHeight)
	}

	// Header lands later with the real height.
	if err := s.UpsertBlockHeaderSeen(ctx, hash, 200, tSeen); err != nil {
		t.Fatalf("header seen: %v", err)
	}
	got, _ = s.GetBlockProcessingStatus(ctx, hash)
	if got.BlockHeight != 200 {
		t.Errorf("BlockHeight after header=%d, want 200 (chaintracks authoritative)", got.BlockHeight)
	}
	// Original processed_at must be preserved.
	if got.ProcessedAt == nil || !got.ProcessedAt.Equal(tProc) {
		t.Errorf("ProcessedAt clobbered: got %v, want %v", got.ProcessedAt, tProc)
	}
	// Original synthesized header_seen_at must be preserved (not bumped to tSeen).
	if !got.HeaderSeenAt.Equal(tProc) {
		t.Errorf("HeaderSeenAt should be preserved at %v, got %v", tProc, got.HeaderSeenAt)
	}
}

func TestBlockProcessing_HeaderReArrival_Idempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "cc33"
	t0 := time.Unix(1700000020, 0).UTC()

	if err := s.UpsertBlockHeaderSeen(ctx, hash, 300, t0); err != nil {
		t.Fatalf("first seen: %v", err)
	}
	if err := s.MarkBlockProcessed(ctx, hash, 300, t0.Add(time.Second)); err != nil {
		t.Fatalf("processed: %v", err)
	}
	// Re-arrive with a later timestamp.
	if err := s.UpsertBlockHeaderSeen(ctx, hash, 300, t0.Add(time.Hour)); err != nil {
		t.Fatalf("second seen: %v", err)
	}
	got, _ := s.GetBlockProcessingStatus(ctx, hash)
	if !got.HeaderSeenAt.Equal(t0) {
		t.Errorf("HeaderSeenAt should be preserved at %v, got %v", t0, got.HeaderSeenAt)
	}
	if got.ProcessedAt == nil {
		t.Error("ProcessedAt cleared on header re-arrival")
	}
}

func TestBlockProcessing_MarkOrphaned_AndResurrection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	hash := "dd44"
	t0 := time.Unix(1700000030, 0).UTC()

	if err := s.UpsertBlockHeaderSeen(ctx, hash, 400, t0); err != nil {
		t.Fatalf("seen: %v", err)
	}
	if err := s.MarkBlocksOrphaned(ctx, []string{hash}, t0.Add(time.Minute)); err != nil {
		t.Fatalf("orphan: %v", err)
	}
	got, _ := s.GetBlockProcessingStatus(ctx, hash)
	if got.Status != models.BlockStatusOrphaned {
		t.Errorf("Status=%q, want orphaned", got.Status)
	}
	if got.OrphanedAt == nil {
		t.Error("OrphanedAt should be set")
	}

	// Resurrection: header re-arrives.
	if err := s.UpsertBlockHeaderSeen(ctx, hash, 400, t0.Add(time.Hour)); err != nil {
		t.Fatalf("resurrect: %v", err)
	}
	got, _ = s.GetBlockProcessingStatus(ctx, hash)
	if got.Status != models.BlockStatusActive {
		t.Errorf("Status after resurrection=%q, want active", got.Status)
	}
	if got.OrphanedAt != nil {
		t.Errorf("OrphanedAt should be cleared, got %v", got.OrphanedAt)
	}
}

func TestBlockProcessing_MarkOrphaned_MissingRow_NoOp(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.MarkBlocksOrphaned(ctx, []string{"never-seen"}, time.Now()); err != nil {
		t.Fatalf("orphan missing: %v", err)
	}
}

func TestBlockProcessing_NotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetBlockProcessingStatus(context.Background(), "missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err=%v, want ErrNotFound", err)
	}
}

func TestBlockProcessing_List_DescendingHeight_Pagination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700000100, 0).UTC()
	for i := uint64(1); i <= 75; i++ {
		hash := fmt.Sprintf("h%04d", i)
		if err := s.UpsertBlockHeaderSeen(ctx, hash, i, t0); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// Walk via cursor pages of 20.
	var seen []uint64
	cursor := uint64(0)
	for {
		page, err := s.ListBlockProcessingStatus(ctx, cursor, 20)
		if err != nil {
			t.Fatalf("list cursor=%d: %v", cursor, err)
		}
		if len(page) == 0 {
			break
		}
		for _, bp := range page {
			seen = append(seen, bp.BlockHeight)
		}
		// Next page starts strictly before the lowest height we just saw.
		cursor = page[len(page)-1].BlockHeight
		if len(page) < 20 {
			break
		}
	}
	if len(seen) != 75 {
		t.Fatalf("walked %d rows, want 75", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i-1] <= seen[i] {
			t.Fatalf("not strictly descending at i=%d: %d <= %d", i, seen[i-1], seen[i])
		}
	}
	if seen[0] != 75 || seen[len(seen)-1] != 1 {
		t.Errorf("unexpected endpoints first=%d last=%d", seen[0], seen[len(seen)-1])
	}
}

func TestBlockProcessing_List_BeforeHeight_ExcludesEqualAndAbove(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	t0 := time.Unix(1700000200, 0).UTC()
	for _, h := range []uint64{10, 20, 30, 40, 50} {
		if err := s.UpsertBlockHeaderSeen(ctx, fmt.Sprintf("h%d", h), h, t0); err != nil {
			t.Fatalf("seed %d: %v", h, err)
		}
	}
	page, err := s.ListBlockProcessingStatus(ctx, 30, 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("got %d rows, want 2 (heights 10, 20)", len(page))
	}
	if page[0].BlockHeight != 20 || page[1].BlockHeight != 10 {
		t.Errorf("got heights %d,%d want 20,10", page[0].BlockHeight, page[1].BlockHeight)
	}
}

func TestBlockProcessing_GetActiveTipBlockHeight(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if h, err := s.GetActiveTipBlockHeight(ctx); err != nil || h != 0 {
		t.Fatalf("empty: got h=%d err=%v want 0/nil", h, err)
	}

	t0 := time.Unix(1700003000, 0).UTC()
	for _, h := range []uint64{100, 200, 150} {
		if err := s.UpsertBlockHeaderSeen(ctx, fmt.Sprintf("h%d", h), h, t0); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetActiveTipBlockHeight(ctx)
	if err != nil || got != 200 {
		t.Errorf("tip=%d err=%v want 200/nil", got, err)
	}

	// Orphaning the highest row drops the tip to the next active row.
	if oErr := s.MarkBlocksOrphaned(ctx, []string{"h200"}, t0); oErr != nil {
		t.Fatal(oErr)
	}
	got, err = s.GetActiveTipBlockHeight(ctx)
	if err != nil || got != 150 {
		t.Errorf("after orphan tip=%d err=%v want 150/nil", got, err)
	}
}

func TestBlockProcessing_ListStale_FiltersAndOrders(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	baseSeen := time.Unix(1700004000, 0).UTC()
	threshold := baseSeen.Add(5 * time.Minute)

	if err := s.UpsertBlockHeaderSeen(ctx, "recent", 500, threshold.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "processed", 400, baseSeen); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkBlockProcessed(ctx, "processed", 400, baseSeen.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "orphaned", 410, baseSeen); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkBlocksOrphaned(ctx, []string{"orphaned"}, baseSeen.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "old", 100, baseSeen.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "stale-a", 450, baseSeen); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertBlockHeaderSeen(ctx, "stale-b", 460, baseSeen.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}

	rows, err := s.ListStaleBlockProcessingStatus(ctx, threshold, 200, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].BlockHash != "stale-a" || rows[1].BlockHash != "stale-b" {
		t.Errorf("got %v want [stale-a stale-b]", hashesOf(rows))
	}

	rows, err = s.ListStaleBlockProcessingStatus(ctx, threshold, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3 (%v)", len(rows), hashesOf(rows))
	}

	rows, err = s.ListStaleBlockProcessingStatus(ctx, threshold, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].BlockHash != "stale-a" {
		t.Errorf("limit truncation: %v", hashesOf(rows))
	}
}

func hashesOf(rows []*models.BlockProcessingStatus) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.BlockHash
	}
	return out
}
