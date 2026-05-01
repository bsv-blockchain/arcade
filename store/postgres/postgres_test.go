//go:build postgres

// Package postgres tests run against embedded-postgres by default. They're
// gated behind the "postgres" build tag because embedded-postgres downloads
// ~80MB of bundled binaries on first run, which is too costly for the default
// `go test ./...` invocation. Run with:
//
//	go test -tags=postgres ./store/postgres/...
//
// If ARCADE_POSTGRES_DSN is set the tests use that external Postgres instead
// of spinning up embedded — useful for CI or running against a real cluster.
package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// newEmbeddedCfg builds a Postgres config that points at either an external
// DSN (when ARCADE_POSTGRES_DSN is set) or a fresh embedded-postgres data
// directory under tempDir. Shared by newTestStore (testing.T) and the
// *testing.B helper used by benchmarks.
func newEmbeddedCfg(tempDir string) config.Postgres {
	if dsn := os.Getenv("ARCADE_POSTGRES_DSN"); dsn != "" {
		return config.Postgres{DSN: dsn, MaxConns: 4}
	}
	return config.Postgres{
		Embedded:         true,
		EmbeddedUser:     "arcade",
		EmbeddedPassword: "arcade",
		EmbeddedDatabase: "arcade",
		EmbeddedDataDir:  tempDir + "/data",
		EmbeddedCacheDir: tempDir + "/cache",
		MaxConns:         4,
	}
}

// Shared embedded-postgres state: one instance per `go test` invocation,
// reused across every test in the package. Spinning up embedded-postgres is
// dominated by binary extraction + initdb (~30s each), so creating a fresh
// instance per test pushed the package well past the 10-minute test timeout.
// Tests get isolation via TRUNCATE in newTestStore rather than per-test DBs.
var (
	sharedStore    *Store
	sharedStoreErr error
	sharedDir      string
)

func TestMain(m *testing.M) {
	code, cleanup := runWithSharedStore(m)
	cleanup()
	os.Exit(code)
}

func runWithSharedStore(m *testing.M) (int, func()) {
	dir, err := os.MkdirTemp("", "arcade-pg-test-")
	if err != nil {
		sharedStoreErr = err
		return m.Run(), func() {}
	}
	sharedDir = dir

	cfg := newEmbeddedCfg(sharedDir)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := New(ctx, cfg)
	if err != nil {
		sharedStoreErr = err
		return m.Run(), func() { _ = os.RemoveAll(sharedDir) }
	}
	if err := s.EnsureIndexes(); err != nil {
		_ = s.Close()
		sharedStoreErr = err
		return m.Run(), func() { _ = os.RemoveAll(sharedDir) }
	}
	sharedStore = s

	cleanup := func() {
		_ = sharedStore.Close()
		_ = os.RemoveAll(sharedDir)
	}
	return m.Run(), cleanup
}

const truncateSQL = `TRUNCATE transactions, bumps, stumps, submissions, leases, datahub_endpoints`

func newTestStore(t *testing.T) *Store {
	t.Helper()
	if sharedStore == nil {
		// If neither an external DSN nor a working embedded-postgres is
		// available (binary download failed, port unavailable, sandboxed CI),
		// skip rather than fail — these tests are infrastructure-gated by the
		// `postgres` build tag and require Postgres to actually be reachable.
		t.Skipf("postgres unavailable, skipping: %v", sharedStoreErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := sharedStore.pool.Exec(ctx, truncateSQL); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return sharedStore
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
	if _, inserted, err := s.GetOrInsertStatus(ctx, first); err != nil || !inserted {
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

// Postgres handles the CAS natively (ON CONFLICT DO NOTHING); the test still
// asserts that N concurrent inserts collapse to exactly one winner.
func TestGetOrInsertStatus_ConcurrentRace(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const N = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	var inserted int

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
				inserted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if inserted != 1 {
		t.Fatalf("expected exactly 1 successful insert, got %d", inserted)
	}
}

func TestPendingRetryLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	txid := "retry-tx"
	rawTx := []byte{0x01, 0x02}
	nextRetry := time.Now().Add(-time.Second) // already due

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
	got := map[string]bool{}
	for _, r := range ready {
		got[r.TxID] = true
	}
	for _, c := range cases {
		if got[c.txid] != c.isReady {
			t.Errorf("%s: isReady=%v, got=%v", c.txid, c.isReady, got[c.txid])
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

	// Different holder is blocked while the current lease is live.
	blocked, err := s.TryAcquireOrRenew(ctx, "reaper", "holder-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !blocked.IsZero() {
		t.Fatal("expected zero heldUntil for contention")
	}
}

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

func TestBUMPInsertAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.InsertBUMP(ctx, "bh-bump", 42, []byte{0xde, 0xad, 0xbe, 0xef}); err != nil {
		t.Fatal(err)
	}
	h, data, err := s.GetBUMP(ctx, "bh-bump")
	if err != nil {
		t.Fatal(err)
	}
	if h != 42 || len(data) != 4 {
		t.Fatalf("unexpected bump: h=%d data=%x", h, data)
	}

	if _, _, err := s.GetBUMP(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing bump, got %v", err)
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

// TestDatahubEndpoints_NetworkScoped is the regression for the bug where a
// regtest pod served mainnet URLs persisted from a prior run.
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

// TestUpdateStatus_TerminalNotOverwritten is the regression for F-003 (#61):
// once a tx is in a terminal status (MINED, IMMUTABLE, REJECTED,
// DOUBLE_SPEND_ATTEMPTED), a later lower-priority UpdateStatus call (e.g. a
// stray SEEN_ON_NETWORK callback) must be a silent no-op rather than a clobber.
func TestUpdateStatus_TerminalNotOverwritten(t *testing.T) {
	terminals := []models.Status{
		models.StatusMined,
		models.StatusImmutable,
		models.StatusRejected,
		models.StatusDoubleSpendAttempted,
	}
	regressions := []models.Status{
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
		models.StatusSentToNetwork,
		models.StatusPendingRetry,
	}
	s := newTestStore(t)
	ctx := context.Background()

	for _, terminal := range terminals {
		for _, regression := range regressions {
			name := string(terminal) + "_then_" + string(regression)
			t.Run(name, func(t *testing.T) {
				txid := "tx-" + name

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
					t.Fatalf("terminal status %s overwritten by %s (got %s)",
						terminal, regression, got.Status)
				}
			})
		}
	}
}

// TestBatchUpdateStatus_TerminalNotOverwritten covers the same F-003
// regression for the batched code path.
func TestBatchUpdateStatus_TerminalNotOverwritten(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	type row struct {
		txid     string
		seedTerm models.Status
		regress  models.Status
	}
	rows := []row{
		{"tx-mined", models.StatusMined, models.StatusSeenOnNetwork},
		{"tx-immutable", models.StatusImmutable, models.StatusSeenOnNetwork},
		{"tx-rejected", models.StatusRejected, models.StatusSeenMultipleNodes},
		{"tx-dsa", models.StatusDoubleSpendAttempted, models.StatusPendingRetry},
	}

	// Seed each row in its terminal status.
	for _, r := range rows {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: r.txid, Status: models.StatusReceived,
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.UpdateStatus(ctx, &models.TransactionStatus{
			TxID: r.txid, Status: r.seedTerm, Timestamp: time.Now(),
		}); err != nil {
			t.Fatalf("seed %s: %v", r.txid, err)
		}
	}

	// One batched lower-priority update for the whole set.
	updates := make([]*models.TransactionStatus, len(rows))
	for i, r := range rows {
		updates[i] = &models.TransactionStatus{
			TxID: r.txid, Status: r.regress, Timestamp: time.Now(),
		}
	}
	if err := s.BatchUpdateStatus(ctx, updates); err != nil {
		t.Fatalf("BatchUpdateStatus: %v", err)
	}

	for _, r := range rows {
		got, err := s.GetStatus(ctx, r.txid)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != r.seedTerm {
			t.Errorf("%s: terminal %s overwritten by batch %s (got %s)",
				r.txid, r.seedTerm, r.regress, got.Status)
		}
	}
}
