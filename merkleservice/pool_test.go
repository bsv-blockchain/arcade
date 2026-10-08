package merkleservice

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/arcade/metrics"
)

// fakeMerkle is one merkle-service endpoint whose /watch, /reprocess and
// /health answers can be flipped at runtime.
type fakeMerkle struct {
	srv *httptest.Server

	mu      sync.Mutex
	watched []string // txids received on /watch
	// watchCode is the status for /watch (and /reprocess); healthCode for
	// /health. Both default to 200.
	watchCode  atomic.Int32
	healthCode atomic.Int32
	// failTxIDs are refused with watchCode-independent 500s.
	failTxIDs map[string]bool
}

func newFakeMerkle(t *testing.T) *fakeMerkle {
	t.Helper()
	f := &fakeMerkle{failTxIDs: map[string]bool{}}
	f.watchCode.Store(http.StatusOK)
	f.healthCode.Store(http.StatusOK)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(int(f.healthCode.Load()))
		case "/watch":
			var req struct {
				TxID string `json:"txid"`
			}
			decodeJSON(r, &req)
			f.mu.Lock()
			f.watched = append(f.watched, req.TxID)
			fail := f.failTxIDs[req.TxID]
			f.mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(int(f.watchCode.Load()))
		case "/reprocess":
			w.WriteHeader(int(f.watchCode.Load()))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeMerkle) watchedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.watched)
}

func (f *fakeMerkle) url() string { return f.srv.URL }

func decodeJSON(r *http.Request, v any) {
	_ = json.NewDecoder(r.Body).Decode(v)
}

func TestPool_RegisterSucceedsWhenAnyEndpointAccepts(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	b.watchCode.Store(http.StatusInternalServerError)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second)

	if err := p.Register(context.Background(), "tx1", testCallbackURL, ""); err != nil {
		t.Fatalf("one healthy endpoint is enough, got %v", err)
	}
	if a.watchedCount() != 1 || b.watchedCount() != 1 {
		t.Fatalf("both endpoints must be called: a=%d b=%d", a.watchedCount(), b.watchedCount())
	}
}

func TestPool_RegisterBatchWithResults_MergesPerTx(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	a.failTxIDs["tx2"] = true
	b.failTxIDs["tx2"] = true
	b.failTxIDs["tx3"] = true // tx3 still lands on a
	p := NewPool([]string{a.url(), b.url()}, "", time.Second)

	regs := []Registration{{TxID: "tx1", CallbackURL: testCallbackURL}, {TxID: "tx2", CallbackURL: testCallbackURL}, {TxID: "tx3", CallbackURL: testCallbackURL}}
	errs := p.RegisterBatchWithResults(context.Background(), regs, 2)
	if len(errs) != 3 {
		t.Fatalf("len(errs)=%d want 3", len(errs))
	}
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("tx1/tx3 accepted by at least one endpoint, got %v / %v", errs[0], errs[2])
	}
	var regErr *RegisterError
	if !errors.As(errs[1], &regErr) || regErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("tx2 refused everywhere must surface the 5xx, got %v", errs[1])
	}
	if a.watchedCount() != 3 || b.watchedCount() != 3 {
		t.Fatalf("every tx goes to every endpoint: a=%d b=%d", a.watchedCount(), b.watchedCount())
	}
}

func TestPool_RegisterBatch_ReturnsFirstFailureWithoutFailFast(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	a.failTxIDs["tx1"] = true
	b.failTxIDs["tx1"] = true
	p := NewPool([]string{a.url(), b.url()}, "", time.Second)

	regs := []Registration{{TxID: "tx1", CallbackURL: testCallbackURL}, {TxID: "tx2", CallbackURL: testCallbackURL}}
	if err := p.RegisterBatch(context.Background(), regs, 4); err == nil {
		t.Fatal("a tx refused everywhere must fail the batch")
	}
	// Not fail-fast: tx2 was still sent to both.
	if a.watchedCount() != 2 || b.watchedCount() != 2 {
		t.Fatalf("remaining txs must still be registered: a=%d b=%d", a.watchedCount(), b.watchedCount())
	}
}

// The merged error is the most retryable one so propagation's classification
// (claim_revoked > register_error > auth_error) keeps its meaning.
func TestPool_MergePrefersMostRetryableError(t *testing.T) {
	ctx := context.Background()
	net := errors.New("dial tcp: connection refused")
	auth := &RegisterError{StatusCode: http.StatusUnauthorized}
	srvErr := &RegisterError{StatusCode: http.StatusBadGateway}
	bad := &RegisterError{StatusCode: http.StatusBadRequest}

	cases := []struct {
		name string
		errs []error
		want error
	}{
		{"any success wins", []error{auth, nil, net}, nil},
		{"network over 5xx", []error{srvErr, net}, net},
		{"5xx over auth", []error{auth, srvErr}, srvErr},
		{"auth over other 4xx", []error{bad, auth}, auth},
		{"all auth", []error{auth, auth}, auth},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mergeErrors(ctx, tc.errs); !errors.Is(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestPool_AllAuthFailuresSurfaceAsAuth(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	a.watchCode.Store(http.StatusUnauthorized)
	b.watchCode.Store(http.StatusForbidden)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second)

	err := p.Register(context.Background(), "tx1", testCallbackURL, "")
	var regErr *RegisterError
	if !errors.As(err, &regErr) || (regErr.StatusCode != http.StatusUnauthorized && regErr.StatusCode != http.StatusForbidden) {
		t.Fatalf("want a 401/403 RegisterError, got %v", err)
	}
	// 4xx never trips the breaker: the service is reachable.
	for _, st := range p.EndpointStatuses() {
		if !st.Healthy || st.ConsecutiveFailures != 0 {
			t.Fatalf("auth rejection must not count toward the breaker: %+v", st)
		}
	}
}

func TestPool_CanceledContextIsNotAnEndpointFailure(t *testing.T) {
	a := newFakeMerkle(t)
	p := NewPool([]string{a.url()}, "", time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.Register(ctx, "tx1", testCallbackURL, "")
	if err == nil {
		t.Fatal("expected an error from a canceled context")
	}
	if st := p.EndpointStatuses()[0]; !st.Healthy || st.ConsecutiveFailures != 0 {
		t.Fatalf("caller cancellation must not move the breaker: %+v", st)
	}
}

func TestPool_BreakerOpensAfterThresholdAndSkipsEndpoint(t *testing.T) {
	a := newFakeMerkle(t)
	dead := "http://127.0.0.1:1" // nothing listens: connection refused
	p := NewPool([]string{a.url(), dead}, "", time.Second, WithFailureThreshold(3))

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := p.Register(ctx, "tx", testCallbackURL, ""); err != nil {
			t.Fatalf("call %d: healthy endpoint must carry the call, got %v", i, err)
		}
	}
	statuses := p.EndpointStatuses()
	if len(statuses) != 2 {
		t.Fatalf("statuses=%d", len(statuses))
	}
	if !statuses[0].Healthy {
		t.Fatalf("healthy endpoint reported unhealthy: %+v", statuses[0])
	}
	if statuses[1].Healthy || statuses[1].ConsecutiveFailures != 3 || statuses[1].OpenSince == nil {
		t.Fatalf("dead endpoint must be open after 3 failures: %+v", statuses[1])
	}
	if got := testutil.ToFloat64(metrics.MerkleEndpointHealthy.WithLabelValues(dead)); got != 0 {
		t.Fatalf("healthy gauge for open endpoint = %v want 0", got)
	}

	skippedBefore := testutil.ToFloat64(metrics.MerkleEndpointRequestsTotal.WithLabelValues(dead, opWatch, outcomeSkippedOpen))
	if err := p.Register(ctx, "tx", testCallbackURL, ""); err != nil {
		t.Fatalf("register with one open endpoint: %v", err)
	}
	if got := testutil.ToFloat64(metrics.MerkleEndpointRequestsTotal.WithLabelValues(dead, opWatch, outcomeSkippedOpen)) - skippedBefore; got != 1 {
		t.Fatalf("open endpoint must be skipped (and counted), delta=%v", got)
	}
	if a.watchedCount() != 4 {
		t.Fatalf("healthy endpoint calls=%d want 4", a.watchedCount())
	}
}

func TestPool_AllBreakersOpenFailsFast(t *testing.T) {
	p := NewPool([]string{"http://127.0.0.1:1"}, "", time.Second, WithFailureThreshold(1))
	ctx := context.Background()
	_ = p.Register(ctx, "tx", testCallbackURL, "")

	errs := p.RegisterBatchWithResults(ctx, []Registration{{TxID: "a"}, {TxID: "b"}}, 1)
	for i, err := range errs {
		if !errors.Is(err, ErrNoHealthyEndpoints) {
			t.Fatalf("errs[%d]=%v want ErrNoHealthyEndpoints", i, err)
		}
	}
	if err := p.Reprocess(ctx, "hash", testCallbackURL, ""); !errors.Is(err, ErrNoHealthyEndpoints) {
		t.Fatalf("reprocess=%v want ErrNoHealthyEndpoints", err)
	}
}

func TestPool_ProbeClosesBreakerAndFiresOnRecovered(t *testing.T) {
	a := newFakeMerkle(t)
	a.watchCode.Store(http.StatusServiceUnavailable)
	a.healthCode.Store(http.StatusServiceUnavailable)
	p := NewPool([]string{a.url()}, "", time.Second, WithFailureThreshold(2), WithProbeInterval(10*time.Millisecond))

	type recovered struct {
		endpoint string
		openedAt time.Time
	}
	got := make(chan recovered, 1)
	p.OnRecovered(func(endpoint string, openedAt time.Time) {
		got <- recovered{endpoint, openedAt}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	for i := 0; i < 2; i++ {
		_ = p.Register(ctx, "tx", testCallbackURL, "")
	}
	if st := p.EndpointStatuses()[0]; st.Healthy {
		t.Fatalf("breaker should be open after two 5xx: %+v", st)
	}

	// A 5xx /health keeps the breaker open.
	time.Sleep(50 * time.Millisecond)
	select {
	case r := <-got:
		t.Fatalf("recovered while /health still 503: %+v", r)
	default:
	}

	a.healthCode.Store(http.StatusOK)
	a.watchCode.Store(http.StatusOK)
	select {
	case r := <-got:
		if r.endpoint != a.url() || r.openedAt.IsZero() {
			t.Fatalf("hook args = %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnRecovered hook never fired")
	}
	if st := p.EndpointStatuses()[0]; !st.Healthy || st.OpenSince != nil {
		t.Fatalf("breaker should be closed after probe: %+v", st)
	}
	if err := p.Register(ctx, "tx", testCallbackURL, ""); err != nil {
		t.Fatalf("register after recovery: %v", err)
	}
}

func TestPool_ReprocessFansOutAndSucceedsOnAny(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	b.watchCode.Store(http.StatusNotFound)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second)
	if err := p.Reprocess(context.Background(), "hash", testCallbackURL, "tok"); err != nil {
		t.Fatalf("one 202 is enough, got %v", err)
	}

	a.watchCode.Store(http.StatusNotFound)
	err := p.Reprocess(context.Background(), "hash", testCallbackURL, "tok")
	var repErr *ReprocessError
	if !errors.As(err, &repErr) || repErr.StatusCode != http.StatusNotFound {
		t.Fatalf("all-4xx must surface a ReprocessError 404, got %v", err)
	}
}

func TestPool_EndpointsAndEndpoint(t *testing.T) {
	a := newFakeMerkle(t)
	p := NewPool([]string{a.url() + "/", a.url(), "", " "}, "tok", time.Second)
	if got := p.Endpoints(); len(got) != 1 || got[0] != a.url() {
		t.Fatalf("Endpoints()=%v want [%s] (deduped, trimmed)", got, a.url())
	}
	if p.Endpoint("http://nope.invalid") != nil {
		t.Fatal("unknown endpoint must be nil")
	}
	single := p.Endpoint(a.url() + "/")
	if single == nil {
		t.Fatal("known endpoint must be returned")
	}
	if err := single.Register(context.Background(), "tx", testCallbackURL, ""); err != nil {
		t.Fatalf("direct endpoint call: %v", err)
	}
	if a.watchedCount() != 1 {
		t.Fatalf("direct call reached endpoint %d times, want 1", a.watchedCount())
	}
}
