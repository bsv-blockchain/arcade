package propagation

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// TestBroadcastSingle_WaitsForSlowPeer verifies that once one endpoint
// returns 200, the broadcast still waits for the slow peer to finish its
// HTTP call rather than canceling it. Every Teranode in the healthy set
// must receive every tx so subsequent dependent broadcasts don't hit a
// node that's missing the parent.
func TestBroadcastSingle_WaitsForSlowPeer(t *testing.T) {
	fastSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer fastSrv.Close()

	const slowDelay = 2 * time.Second
	slowHit := make(chan struct{}, 1)
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slowHit <- struct{}{}:
		default:
		}
		select {
		case <-time.After(slowDelay):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			// If the broadcast cancels us early, fail the request loudly so
			// the assertion below catches it.
			w.WriteHeader(http.StatusGatewayTimeout)
		}
	}))
	defer slowSrv.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{fastSrv.URL, slowSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	start := time.Now()
	if err := handleAndFlush(t, p, makePropMsg("tx-race")); err != nil {
		t.Fatalf("flush error: %v", err)
	}
	elapsed := time.Since(start)

	select {
	case <-slowHit:
	default:
		t.Fatalf("slow peer never received the broadcast")
	}

	if elapsed < slowDelay {
		t.Fatalf("broadcast wall-time %v < slow peer delay %v — slow peer was canceled early", elapsed, slowDelay)
	}
}

// TestBroadcast_RecordsEndpointOutcomes verifies that per-endpoint failure
// and success outcomes that do reach the aggregation loop are recorded, and
// that cancellation-induced errors from the winning race are NOT recorded
// as failures (the ok endpoint should remain healthy across many broadcasts
// even though its sibling loses the race every time).
func TestBroadcast_RecordsEndpointOutcomes(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	// Second fast endpoint — both respond quickly, but only one wins the race
	// per broadcast. The loser's result arrives just after the winner; the
	// loser's error/success is still processed (it's not cancellation).
	okSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv2.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	// Low threshold. If cancellation were incorrectly counted as failure, the
	// losing endpoint would be tripped within three broadcasts.
	tc := teranode.NewClient([]string{okSrv.URL, okSrv2.URL}, "", teranode.HealthConfig{FailureThreshold: 3})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	// Ten broadcasts — far more than the failure threshold would allow if
	// cancellation were being miscounted.
	for i := 0; i < 10; i++ {
		if err := handleAndFlush(t, p, makePropMsg("tx-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("flush error: %v", err)
		}
	}

	if len(tc.GetHealthyEndpoints()) != 2 {
		t.Fatalf("both ok endpoints should remain healthy; cancellation must not count as failure — got %v", tc.GetHealthyEndpoints())
	}
}

// TestPeerReturning500_NotSidelinedWhenUnanimous documents the resilience
// tunable from the 02:07 EDT incident: when EVERY responding peer returns
// non-2xx for the same tx, that's a network-consensus signal (the tx is
// bad — typically a double-spend or invalid sig), not a peer-health signal.
// Penalizing the peers in that case progressively sidelines the entire
// fleet until "no healthy teranode endpoints" and ~1.6M txs sit in
// RECEIVED. With network-aware breaker logic, peers stay healthy when
// they agree, so the tx flows through to UpdateStatus(REJECTED) and the
// fleet remains usable for whatever the generator broadcasts next.
//
// Single-peer setup represents the limiting case (one peer = the whole
// network); the all-agreement rule still applies.
func TestPeerReturning500_NotSidelinedWhenUnanimous(t *testing.T) {
	alwaysFiveHundred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing inputs for tx", http.StatusInternalServerError)
	}))
	defer alwaysFiveHundred.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{alwaysFiveHundred.URL}, "", teranode.HealthConfig{
		FailureThreshold:          2,
		BroadcastFailureThreshold: 5, // intentionally low — sub-threshold drives most peer-health tests
	})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	// Many broadcasts, all unanimous 500. With the old per-result penalty
	// the peer would be sidelined long before this — the new logic keeps
	// it healthy because nobody else disagreed.
	for i := 0; i < 20; i++ {
		if err := handleAndFlush(t, p, makePropMsg(fmt.Sprintf("tx-%02d", i))); err != nil {
			t.Fatalf("flush error: %v", err)
		}
	}
	if len(tc.GetHealthyEndpoints()) != 1 {
		t.Fatalf("unanimous-reject peer must stay healthy under network-aware breaker; healthy=%v", tc.GetHealthyEndpoints())
	}
}

// TestPeerReturning500_SidelinedWhenOthersAccept covers the complementary
// case: when one peer rejects but a sibling accepts, the rejecting peer
// IS the outlier and slow-track sidelining must still fire. This is the
// per-peer health signal the breaker was designed to catch.
func TestPeerReturning500_SidelinedWhenOthersAccept(t *testing.T) {
	// Bad peer responds instantly with 500. Good peer responds after a
	// short delay so the loop deterministically records the bad peer's
	// non-2xx outcome before the good peer's 200 cancels siblings.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing inputs for tx", http.StatusInternalServerError)
	}))
	defer bad.Close()

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer good.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{bad.URL, good.URL}, "", teranode.HealthConfig{
		FailureThreshold:          2,
		BroadcastFailureThreshold: 5,
	})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	// Drive the bad peer past its slow threshold. Each broadcast produces a
	// 200 from `good` AND a 500 from `bad` — not unanimous → bad gets
	// penalized, good gets credit.
	for i := 0; i < 10; i++ {
		if err := handleAndFlush(t, p, makePropMsg(fmt.Sprintf("tx-%02d", i))); err != nil {
			t.Fatalf("flush error: %v", err)
		}
	}
	healthy := tc.GetHealthyEndpoints()
	if len(healthy) != 1 || !strings.HasPrefix(healthy[0], good.URL) {
		t.Fatalf("expected only the 200-returning peer to remain healthy; healthy=%v", healthy)
	}
}

// TestPeerUnreachable_Trips verifies that the circuit-breaker still fires for
// genuine transport failures — a peer whose TCP port is closed (no process
// listening) should trip after FailureThreshold consecutive broadcasts.
func TestPeerUnreachable_Trips(t *testing.T) {
	// Grab a free port, then close the listener so dials are refused.
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("grabbing port: %v", err)
	}
	unreachableURL := "http://" + ln.Addr().String()
	_ = ln.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{unreachableURL}, "", teranode.HealthConfig{FailureThreshold: 3})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	for i := 0; i < 3; i++ {
		if err := handleAndFlush(t, p, makePropMsg("tx-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("flush error: %v", err)
		}
	}

	if len(tc.GetHealthyEndpoints()) != 0 {
		t.Fatalf("unreachable peer should have tripped, healthy=%v", tc.GetHealthyEndpoints())
	}
}

// TestBadPeer_SkippedAfterTrip verifies that once the bad endpoint is tripped
// to unhealthy, subsequent broadcasts do not send it any traffic. The trip is
// induced deterministically via RecordFailure calls so the assertion doesn't
// depend on the worker pool's per-call scheduling.
func TestBadPeer_SkippedAfterTrip(t *testing.T) {
	var okHits, badHits atomic.Int32

	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		okHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		badHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{okSrv.URL, badSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 3})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	// Deterministically trip bad to unhealthy.
	tc.RecordFailure(badSrv.URL)
	tc.RecordFailure(badSrv.URL)
	tc.RecordFailure(badSrv.URL)
	if len(tc.GetHealthyEndpoints()) != 1 {
		t.Fatalf("expected bad to be tripped, healthy=%v", tc.GetHealthyEndpoints())
	}

	// Five broadcasts — bad should receive zero traffic because it's excluded
	// from the healthy view.
	for i := 0; i < 5; i++ {
		if err := handleAndFlush(t, p, makePropMsg("after-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("flush error: %v", err)
		}
	}

	if badHits.Load() != 0 {
		t.Fatalf("bad endpoint received %d hits after trip, expected 0", badHits.Load())
	}
	if okHits.Load() < 5 {
		t.Fatalf("ok endpoint should have received all 5 broadcasts, got %d", okHits.Load())
	}
}

// TestBatchPropagatedLog_IncludesSuccessEndpoint verifies that the
// "batch propagated" summary log surfaces which datahub URL actually accepted
// the batch. This is the operator-visible signal for "who took our traffic".
func TestBatchPropagatedLog_IncludesSuccessEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, logger, nil, nil, ms, nil, tc, nil)

	if err := handleAndFlush(t, p, makePropMsg("tx-logged")); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	entries := recorded.FilterMessage("batch propagated").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'batch propagated' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	got, ok := fields["success_endpoints"].([]interface{})
	if !ok {
		// zap observer unmarshals []string as []interface{}; fall back to direct [].
		if ss, ok := fields["success_endpoints"].([]string); ok {
			if len(ss) != 1 || ss[0] != srv.URL {
				t.Fatalf("expected success_endpoints=[%q], got %v", srv.URL, ss)
			}
			return
		}
		t.Fatalf("success_endpoints field missing or wrong type: %#v", fields["success_endpoints"])
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 success endpoint, got %v", got)
	}
	if got[0] != srv.URL {
		t.Fatalf("expected success endpoint %q, got %q", srv.URL, got[0])
	}
}

// TestMinHealthyWarning_FiresOnlyOnCrossing verifies that the min-healthy
// warning log is emitted exactly once when the healthy count crosses below
// the threshold, regardless of how many further trips happen afterwards.
func TestMinHealthyWarning_FiresOnlyOnCrossing(t *testing.T) {
	core, recorded := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	c := teranode.NewClient(
		[]string{"https://a.example", "https://b.example", "https://c.example"},
		"",
		teranode.HealthConfig{
			FailureThreshold:    1,
			MinHealthyEndpoints: 2,
			Logger:              logger,
		},
	)

	// Initial state: 3 healthy ≥ 2, no warning.
	if got := recorded.FilterMessage("healthy endpoint count below minimum").Len(); got != 0 {
		t.Fatalf("no warning expected before any trip, got %d", got)
	}

	// Trip a: 2 healthy ≥ 2 — still at threshold, no crossing yet.
	c.RecordFailure("https://a.example")
	if got := recorded.FilterMessage("healthy endpoint count below minimum").Len(); got != 0 {
		t.Fatalf("no warning expected at threshold, got %d", got)
	}

	// Trip b: 1 healthy < 2 — crossing, exactly one warning.
	c.RecordFailure("https://b.example")
	if got := recorded.FilterMessage("healthy endpoint count below minimum").Len(); got != 1 {
		t.Fatalf("expected 1 warning on crossing, got %d", got)
	}

	// Trip c: 0 healthy < 2 — still below, must NOT fire again.
	c.RecordFailure("https://c.example")
	if got := recorded.FilterMessage("healthy endpoint count below minimum").Len(); got != 1 {
		t.Fatalf("expected still 1 warning when further below, got %d", got)
	}

	// Recover one: 1 healthy < 2 — still below, no warning.
	c.RecordSuccess("https://a.example")
	if got := recorded.FilterMessage("healthy endpoint count below minimum").Len(); got != 1 {
		t.Fatalf("recovery should not fire warning, got %d", got)
	}

	// Recover another: 2 healthy ≥ 2 — clears the flag.
	c.RecordSuccess("https://b.example")

	// Now trip back below: should fire again (new crossing).
	c.RecordFailure("https://a.example")
	c.RecordFailure("https://a.example")
	// FailureThreshold is 1, so one failure trips. But a is already healthy
	// from the recovery, so one failure will re-trip it. Let's just verify
	// the second crossing happened.
	if got := recorded.FilterMessage("healthy endpoint count below minimum").Len(); got < 2 {
		t.Fatalf("expected 2 warnings (second crossing), got %d", got)
	}
}

// TestMinHealthyWarning_ZeroDisables verifies the default MinHealthyEndpoints=0
// never emits the warning regardless of how few endpoints are healthy.
func TestMinHealthyWarning_ZeroDisables(t *testing.T) {
	core, recorded := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	c := teranode.NewClient(
		[]string{"https://a.example", "https://b.example"},
		"",
		teranode.HealthConfig{
			FailureThreshold:    1,
			MinHealthyEndpoints: 0,
			Logger:              logger,
		},
	)
	c.RecordFailure("https://a.example")
	c.RecordFailure("https://b.example")

	if got := recorded.FilterMessage("healthy endpoint count below minimum").Len(); got != 0 {
		t.Fatalf("warning must be disabled with MinHealthyEndpoints=0, got %d", got)
	}
}

// TestBroadcast_PerPeerAggregation_AcceptanceWins covers the per-tx
// aggregation across peer responses: when one peer rejects a tx but
// another peer accepts it (by 500-ing without naming it), the acceptance
// must win. Two endpoints each return 500 with different failure maps
// such that each tx is rejected by one peer and implicitly accepted by
// the other. Both txs should land as ACCEPTED_BY_NETWORK.
func TestBroadcast_PerPeerAggregation_AcceptanceWins(t *testing.T) {
	const (
		txidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		txidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	// Peer A rejects txidA, implicitly accepts txidB.
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Failed to process transactions:\n" +
			"TX_INVALID (31): [ProcessTransaction][" + txidA + "] missing input\n"))
	}))
	defer srvA.Close()
	// Peer B rejects txidB, implicitly accepts txidA.
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Failed to process transactions:\n" +
			"TX_INVALID (31): [ProcessTransaction][" + txidB + "] missing input\n"))
	}))
	defer srvB.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srvA.URL, srvB.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	for _, txid := range []string{txidA, txidB} {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
			t.Fatalf("handleMessage(%s): %v", txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	for _, txid := range []string{txidA, txidB} {
		got := ms.lastUpdateForTxid(txid)
		if got == nil {
			t.Fatalf("no status update recorded for %s", txid)
		}
		if got.Status != models.StatusAcceptedByNetwork {
			t.Errorf("%s: expected ACCEPTED_BY_NETWORK, got %s (reason=%q)", txid, got.Status, got.ExtraInfo)
		}
	}
}

// TestBroadcast_422RejectionSurfacesExtraInfo pins the production gap where
// one peer returned HTTP 422 + a parseable Teranode failure list (the real
// validator verdict for a spent-UTXO / invalid tx) while other peers returned
// opaque gateway 502/500 with no body. Pre-fix the 422 body was discarded
// (client only parsed status 500), so the tx either requeued forever or
// never carried the TX_INVALID/UTXO_SPENT line into GET /tx ExtraInfo.
// Post-fix: REJECTED with the Teranode line as ExtraInfo. Opaque PROCESSING
// (4) without a nested verdict is a requeue, not this path.
func TestBroadcast_422RejectionSurfacesExtraInfo(t *testing.T) {
	const txid = "d018bc98d7828b7f095e5d616f7ccba607fc228368e82e74b25304e37699f1e9"
	verdictBody := "Failed to process transactions:\n" +
		"TX_INVALID (31): [ProcessTransaction][" + txid + "] tx is invalid because fee too low\n"

	// Peer A: the only peer that actually validated — 422 + failure list.
	srvVerdict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(verdictBody))
	}))
	defer srvVerdict.Close()
	// Peer B/C: gateway noise that must NOT become ExtraInfo and must NOT
	// silence the 422 verdict.
	srv502 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("error code: 502\n"))
	}))
	defer srv502.Close()
	srv503 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("no available server\n"))
	}))
	defer srv503.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient(
		[]string{srv502.URL, srv503.URL, srvVerdict.URL},
		"",
		teranode.HealthConfig{FailureThreshold: 1 << 20},
	)
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	got := ms.lastUpdateForTxid(txid)
	if got == nil {
		t.Fatal("no status update recorded")
	}
	if got.Status != models.StatusRejected {
		t.Fatalf("status=%s want REJECTED (extraInfo=%q)", got.Status, got.ExtraInfo)
	}
	if !strings.Contains(got.ExtraInfo, "TX_INVALID (31)") {
		t.Errorf("ExtraInfo=%q want Teranode TX_INVALID line (not gateway 502/503 body)", got.ExtraInfo)
	}
	if strings.Contains(got.ExtraInfo, "no available server") || strings.Contains(got.ExtraInfo, "502") {
		t.Errorf("ExtraInfo leaked opaque gateway text: %q", got.ExtraInfo)
	}
}

// TestBroadcast_OpaqueProcessing_RequeuesInsteadOfRejects pins the other
// half of PROCESSING: a 422 failure list that is only PROCESSING (4) with
// no nested TX_*/UTXO_* code is Teranode's catch-all wrapper, not a byte
// verdict. Pre-fix this terminalized REJECTED; txResultClassRequeue's
// comment already said PROCESSING requeues. The row stays off REJECTED so
// a later successful peer (or retry) can still accept the tx.
func TestBroadcast_OpaqueProcessing_RequeuesInsteadOfRejects(t *testing.T) {
	const txid = "d018bc98d7828b7f095e5d616f7ccba607fc228368e82e74b25304e37699f1e9"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte("Failed to process transactions:\n" +
			"PROCESSING (4): [ProcessTransaction][" + txid + "] failed to validate transaction\n"))
	}))
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 5, time.Millisecond)

	if err := handleAndFlush(t, p, makePropMsg(txid)); err != nil {
		t.Fatalf("handleAndFlush: %v", err)
	}

	if got := rejectedUpdates(ms); len(got) != 0 {
		t.Fatalf("opaque PROCESSING must not terminalize REJECTED, got %d REJECTED writes (first: %+v)", len(got), got[0])
	}
	waitForPending(t, p, 1)
}

// TestBroadcast_422PartialFailureList_ImplicitAccept pins the riskiest
// semantics the status-agnostic parse enables: a single 422 whose failure
// list names ONE tx is a whole-batch verdict — the named tx is REJECTED and
// every unnamed tx is terminally ACCEPTED_BY_NETWORK on that peer's implicit
// accept. Previously pinned only for HTTP 500.
func TestBroadcast_422PartialFailureList_ImplicitAccept(t *testing.T) {
	const (
		txidRejected = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		txidAccepted = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte("Failed to process transactions:\n" +
			"TX_INVALID (31): [ProcessTransaction][" + txidRejected + "] bad fee\n"))
	}))
	defer srv.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	for _, txid := range []string{txidRejected, txidAccepted} {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
			t.Fatalf("handleMessage(%s): %v", txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if got := ms.lastUpdateForTxid(txidRejected); got == nil || got.Status != models.StatusRejected {
		t.Errorf("txidRejected: got %v, want REJECTED", got)
	}
	if got := ms.lastUpdateForTxid(txidAccepted); got == nil || got.Status != models.StatusAcceptedByNetwork {
		t.Errorf("txidAccepted: got %v, want ACCEPTED_BY_NETWORK", got)
	}
}

// TestBatchBroadcastFailedLog_IncludesFailureLines pins the operator-
// visibility half of the fix: the "batch broadcast endpoint failed" warn for
// a peer whose response parsed into a failure list must carry
// failure_count/failure_lines (previously the structured verdict was hidden
// off err once parsed), while an opaque-body peer's warn must NOT grow those
// fields.
func TestBatchBroadcastFailedLog_IncludesFailureLines(t *testing.T) {
	const txid = "d018bc98d7828b7f095e5d616f7ccba607fc228368e82e74b25304e37699f1e9"
	srvVerdict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte("Failed to process transactions:\n" +
			"PROCESSING (4): [ProcessTransaction][" + txid + "] failed to validate transaction\n"))
	}))
	defer srvVerdict.Close()
	srvOpaque := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("error code: 502\n"))
	}))
	defer srvOpaque.Close()

	core, recorded := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srvVerdict.URL, srvOpaque.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, logger, nil, nil, ms, nil, tc, nil)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	entries := recorded.FilterMessage("batch broadcast endpoint failed").All()
	if len(entries) != 2 {
		t.Fatalf("want 2 'batch broadcast endpoint failed' warns, got %d", len(entries))
	}
	for _, e := range entries {
		fields := make(map[string]interface{}, len(e.Context))
		for _, f := range e.Context {
			fields[f.Key] = f
		}
		endpoint := ""
		for _, f := range e.Context {
			if f.Key == "endpoint" {
				endpoint = f.String
			}
		}
		_, hasLines := fields["failure_lines"]
		switch endpoint {
		case srvVerdict.URL:
			if !hasLines {
				t.Errorf("verdict peer warn missing failure_lines: %+v", e.Context)
			}
		case srvOpaque.URL:
			if hasLines {
				t.Errorf("opaque peer warn must not carry failure_lines: %+v", e.Context)
			}
		default:
			t.Errorf("unexpected endpoint %q in warn", endpoint)
		}
	}
}

// TestBroadcast_422RejectionLosesToAcceptance pins sticky acceptance across
// the NEW parse path: a peer that 200-accepts the batch must win over a peer
// that 422-rejects the tx with a parseable failure list. Guards against the
// status-agnostic parse accidentally making 4xx verdicts override real
// acceptance (rejection votes only matter when NO peer accepts).
func TestBroadcast_422RejectionLosesToAcceptance(t *testing.T) {
	const txid = "d018bc98d7828b7f095e5d616f7ccba607fc228368e82e74b25304e37699f1e9"
	srvAccept := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srvAccept.Close()
	srvReject := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte("Failed to process transactions:\n" +
			"TX_INVALID (31): [ProcessTransaction][" + txid + "] tx is invalid because fee too low\n"))
	}))
	defer srvReject.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srvReject.URL, srvAccept.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	got := ms.lastUpdateForTxid(txid)
	if got == nil {
		t.Fatal("no status update recorded")
	}
	if got.Status != models.StatusAcceptedByNetwork {
		t.Errorf("status=%s want ACCEPTED_BY_NETWORK (acceptance is sticky; extraInfo=%q)", got.Status, got.ExtraInfo)
	}
}

// TestBroadcast_409AlienConflict_SingleTxTerminalizes pins the mainnet
// incident shape end to end: batch_size=1, one peer answers HTTP 409 with a
// wrapper-less conflict verdict whose hashes name the spent OUTPOINT and the
// competing spender — NOT the submitted txid ("UTXO_SPENT (70): UTXO_SPENT
// (70): <outpoint>:2 utxo already spent by tx <spender>[0]"). The failure
// map therefore keys under an alien hash. A naive "absent from map ⇒
// accepted" read would mark the submitted tx ACCEPTED_BY_NETWORK off its own
// rejection; the single-tx attribution rule must instead terminalize it
// REJECTED with the conflict line as ExtraInfo and ARC 466 (StatusConflict).
func TestBroadcast_409AlienConflict_SingleTxTerminalizes(t *testing.T) {
	const (
		txid     = "e3b7a2c260168429b20ad85f746ae09c2e41f6a1d79877a8810832cdf732e703"
		outpoint = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		spender  = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	conflictBody := "Failed to process transactions:\n" +
		"UTXO_SPENT (70): UTXO_SPENT (70): " + outpoint + ":2 utxo already spent by tx " + spender + "[0]\n"

	srvConflict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(conflictBody))
	}))
	defer srvConflict.Close()
	srv502 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("error code: 502\n"))
	}))
	defer srv502.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient(
		[]string{srv502.URL, srvConflict.URL},
		"",
		teranode.HealthConfig{FailureThreshold: 1 << 20},
	)
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	got := ms.lastUpdateForTxid(txid)
	if got == nil {
		t.Fatal("no status update recorded")
	}
	if got.Status != models.StatusRejected {
		t.Fatalf("status=%s want REJECTED (extraInfo=%q)", got.Status, got.ExtraInfo)
	}
	if got.StatusCode != 466 {
		t.Errorf("StatusCode=%d want 466 (ARC StatusConflict)", got.StatusCode)
	}
	if !strings.Contains(got.ExtraInfo, "UTXO_SPENT") || !strings.Contains(got.ExtraInfo, spender) {
		t.Errorf("ExtraInfo=%q want the UTXO_SPENT line naming the competing spender", got.ExtraInfo)
	}
}

// TestBroadcast_409AlienConflict_MultiTxWithholdsAccepts covers the
// multi-tx half of the alien-line rule: a 409 failure list whose only line
// names no submitted tx means SOME tx in the batch was rejected but we can't
// tell which — so the peer must grant no implicit acceptance votes, and with
// no other peer voting, both txs requeue rather than either terminalizing
// (no fabricated ACCEPTED_BY_NETWORK, no misattributed REJECTED).
func TestBroadcast_409AlienConflict_MultiTxWithholdsAccepts(t *testing.T) {
	const (
		txidA    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		txidB    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		outpoint = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		spender  = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	srvConflict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("Failed to process transactions:\n" +
			"UTXO_SPENT (70): UTXO_SPENT (70): " + outpoint + ":2 utxo already spent by tx " + spender + "[0]\n"))
	}))
	defer srvConflict.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srvConflict.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	for _, txid := range []string{txidA, txidB} {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
			t.Fatalf("handleMessage(%s): %v", txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	for _, txid := range []string{txidA, txidB} {
		if got := ms.lastUpdateForTxid(txid); got != nil &&
			(got.Status == models.StatusAcceptedByNetwork || got.Status == models.StatusRejected) {
			t.Errorf("%s: terminalized as %s (extraInfo=%q); alien-keyed 409 must requeue, not vote", txid, got.Status, got.ExtraInfo)
		}
	}
}

// TestBroadcast_PerPeerAggregation_UnanimousRejection covers the other
// half: when every peer that gave us a parseable response named the same
// tx as failed, the tx is REJECTED. Two endpoints both 500 with the
// same txid in their failure maps; a second txid is absent from both
// and should still be ACCEPTED.
func TestBroadcast_PerPeerAggregation_UnanimousRejection(t *testing.T) {
	const (
		txidRejected = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		txidAccepted = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	)
	body := "Failed to process transactions:\n" +
		"TX_INVALID (31): [ProcessTransaction][" + txidRejected + "] tx is invalid\n"
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(body))
	}))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(body))
	}))
	defer srvB.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srvA.URL, srvB.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	for _, txid := range []string{txidRejected, txidAccepted} {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
			t.Fatalf("handleMessage(%s): %v", txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if got := ms.lastUpdateForTxid(txidRejected); got == nil || got.Status != models.StatusRejected {
		t.Errorf("%s: expected REJECTED, got %+v", txidRejected, got)
	}
	if got := ms.lastUpdateForTxid(txidAccepted); got == nil || got.Status != models.StatusAcceptedByNetwork {
		t.Errorf("%s: expected ACCEPTED_BY_NETWORK, got %+v", txidAccepted, got)
	}
}

// Ensure test helpers are kept from being dropped by goimports.
var _ = context.Background
