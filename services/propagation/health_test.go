package propagation

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/teranode"
)

// TestBroadcastSingle_FirstSuccessWins_DoesNotWaitForSlowPeer verifies that
// once one endpoint returns 200, the broadcast returns in ~fast-peer time
// rather than waiting for the slow peer. The slow peer's handler eventually
// observes context cancellation (its r.Context().Done() fires when the
// client closes the connection), but we measure the outcome via wall-time
// to avoid pinning the test to net/http server-side context propagation
// internals.
func TestBroadcastSingle_FirstSuccessWins_DoesNotWaitForSlowPeer(t *testing.T) {
	fastSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer fastSrv.Close()

	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep or abort-on-cancel, whichever comes first. The handler will
		// eventually return; the broadcast must not wait for it.
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slowSrv.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{fastSrv.URL, slowSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

	start := time.Now()
	if err := handleAndFlush(t, p, makePropMsg("tx-race")); err != nil {
		t.Fatalf("flush error: %v", err)
	}
	elapsed := time.Since(start)

	// Slow peer sleeps 5s. If we were waiting for it, elapsed would be ~5s.
	// With first-success early-cancel, elapsed should be well under 1s.
	if elapsed > 1*time.Second {
		t.Fatalf("broadcast wall-time %v — did not early-cancel slow peer", elapsed)
	}
}

// TestBroadcast_RecordsEndpointOutcomes verifies that per-endpoint failure
// and success outcomes that do reach the aggregation loop are recorded, and
// that cancellation-induced errors from the winning race are NOT recorded
// as failures (the ok endpoint should remain healthy across many broadcasts
// even though its sibling loses the race every time).
func TestBroadcast_RecordsEndpointOutcomes(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	// Second fast endpoint — both respond quickly, but only one wins the race
	// per broadcast. The loser's result arrives just after the winner; the
	// loser's error/success is still processed (it's not cancellation).
	okSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv2.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	// Low threshold. If cancellation were incorrectly counted as failure, the
	// losing endpoint would be tripped within three broadcasts.
	tc := teranode.NewClient([]string{okSrv.URL, okSrv2.URL}, "", teranode.HealthConfig{FailureThreshold: 3})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

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

// TestPeerReturning500_StaysHealthy verifies that an application-level
// rejection (peer returns HTTP 500 on every submission) does NOT trip the
// circuit-breaker. The peer is reachable — it's just telling us the tx is
// bad. Tripping on legitimate 500s would sideline a healthy peer.
func TestPeerReturning500_StaysHealthy(t *testing.T) {
	alwaysFiveHundred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing inputs for tx", http.StatusInternalServerError)
	}))
	defer alwaysFiveHundred.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	// Low failure threshold so the test would fail fast if any 500 were
	// miscounted as a transport failure.
	tc := teranode.NewClient([]string{alwaysFiveHundred.URL}, "", teranode.HealthConfig{FailureThreshold: 2})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

	// Ten broadcasts, all hitting a 500-returning peer.
	for i := 0; i < 10; i++ {
		if err := handleAndFlush(t, p, makePropMsg("tx-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("flush error: %v", err)
		}
	}

	if len(tc.GetHealthyEndpoints()) != 1 {
		t.Fatalf("peer returning 500 was wrongly tripped: healthy=%v", tc.GetHealthyEndpoints())
	}
}

// TestPeerUnreachable_Trips verifies that the circuit-breaker still fires for
// genuine transport failures — a peer whose TCP port is closed (no process
// listening) should trip after FailureThreshold consecutive broadcasts.
func TestPeerUnreachable_Trips(t *testing.T) {
	// Grab a free port, then close the listener so dials are refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("grabbing port: %v", err)
	}
	unreachableURL := "http://" + ln.Addr().String()
	ln.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{unreachableURL}, "", teranode.HealthConfig{FailureThreshold: 3})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

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
// induced deterministically via RecordFailure calls — inducing it via real
// broadcasts is racy because the early-cancel can abort bad's request before
// it reaches the server, preventing the hit from being recorded.
func TestBadPeer_SkippedAfterTrip(t *testing.T) {
	var okHits, badHits atomic.Int32

	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		okHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{okSrv.URL, badSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 3})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, logger, nil, ms, nil, tc, nil)

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

// Ensure test helpers are kept from being dropped by goimports.
var _ = context.Background
