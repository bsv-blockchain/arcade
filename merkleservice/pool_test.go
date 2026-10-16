package merkleservice

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	// failTxIDs are refused with the mapped status, regardless of
	// watchCode; setFail uses 500.
	failTxIDs map[string]int
}

func newFakeMerkle(t *testing.T) *fakeMerkle {
	t.Helper()
	f := &fakeMerkle{failTxIDs: map[string]int{}}
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
			failCode := f.failTxIDs[req.TxID]
			f.mu.Unlock()
			if failCode != 0 {
				w.WriteHeader(failCode)
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

func (f *fakeMerkle) watchedSet() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.watched))
	for _, id := range f.watched {
		out[id]++
	}
	return out
}

func (f *fakeMerkle) setFail(txid string, fail bool) {
	if fail {
		f.setFailCode(txid, http.StatusInternalServerError)
	} else {
		f.setFailCode(txid, 0)
	}
}

// setFailCode refuses txid's /watch with code; 0 clears it.
func (f *fakeMerkle) setFailCode(txid string, code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if code == 0 {
		delete(f.failTxIDs, txid)
	} else {
		f.failTxIDs[txid] = code
	}
}

// pendingFor reads one endpoint's catch-up queue length from the pool's
// status surface.
func pendingFor(p *Pool, url string) int {
	for _, st := range p.EndpointStatuses() {
		if st.URL == url {
			return st.CatchupPending
		}
	}
	return -1
}

// waitFor polls cond for up to 3s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
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
	a.failTxIDs["tx2"] = http.StatusInternalServerError
	b.failTxIDs["tx2"] = http.StatusInternalServerError
	b.failTxIDs["tx3"] = http.StatusInternalServerError // tx3 still lands on a
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
	// tx3 was accepted by a but refused by b: b owes it. tx2 failed
	// everywhere (the caller requeues it), so nobody owes it.
	if got := pendingFor(p, b.url()); got != 1 {
		t.Fatalf("b catch-up pending = %d, want 1 (tx3)", got)
	}
	if got := pendingFor(p, a.url()); got != 0 {
		t.Fatalf("a catch-up pending = %d, want 0", got)
	}
}

// An isolated failure on one endpoint (below the breaker threshold) behind
// a pool-level success is not forgotten: the background loop re-sends it
// until the endpoint acknowledges, so both watch sets converge.
func TestPool_IsolatedFailureIsQueuedAndRedelivered(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	b.setFail("tx2", true)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second, WithProbeInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	regs := []Registration{{TxID: "tx1", CallbackURL: testCallbackURL}, {TxID: "tx2", CallbackURL: testCallbackURL}}
	for i, err := range p.RegisterBatchWithResults(ctx, regs, 2) {
		if err != nil {
			t.Fatalf("errs[%d]=%v, a accepted both", i, err)
		}
	}
	if st := p.EndpointStatuses()[1]; !st.Healthy || st.CatchupPending != 1 {
		t.Fatalf("b must stay healthy with tx2 owed: %+v", st)
	}

	b.setFail("tx2", false)
	waitFor(t, "b to acknowledge tx2", func() bool { return b.watchedSet()["tx2"] >= 2 })
	waitFor(t, "b's queue to drain", func() bool { return pendingFor(p, b.url()) == 0 })
	if got := testutil.ToFloat64(metrics.MerkleEndpointCatchupPending.WithLabelValues(b.url())); got != 0 {
		t.Fatalf("catchup_pending gauge = %v, want 0", got)
	}
}

// Registrations skipped while an endpoint's breaker was open are owed to it
// and delivered once the probe closes the breaker.
func TestPool_SkippedWhileOpenIsDeliveredAfterRecovery(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	b.watchCode.Store(http.StatusServiceUnavailable)
	b.healthCode.Store(http.StatusServiceUnavailable)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second, WithFailureThreshold(2), WithProbeInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	// Two refusals open b; both txs are owed to it.
	for _, id := range []string{"tx1", "tx2", "tx3"} {
		if err := p.Register(ctx, id, testCallbackURL, ""); err != nil {
			t.Fatalf("register %s: %v (a accepts)", id, err)
		}
	}
	if st := p.EndpointStatuses()[1]; st.Healthy || st.CatchupPending != 3 {
		t.Fatalf("b must be open and owe all three: %+v", st)
	}
	// A 5xx /health keeps it open and nothing drains.
	time.Sleep(50 * time.Millisecond)
	if st := p.EndpointStatuses()[1]; st.Healthy || st.CatchupPending != 3 {
		t.Fatalf("b must stay open while /health is 503: %+v", st)
	}
	if got := testutil.ToFloat64(metrics.MerkleEndpointRequestsTotal.WithLabelValues(b.url(), opProbe, outcome5xx)); got == 0 {
		t.Fatal("a 503 /health must be counted as err_5xx on the probe op")
	}

	b.healthCode.Store(http.StatusOK)
	b.watchCode.Store(http.StatusOK)
	waitFor(t, "b to recover and drain", func() bool {
		st := p.EndpointStatuses()[1]
		return st.Healthy && st.CatchupPending == 0
	})
	got := b.watchedSet()
	for _, id := range []string{"tx1", "tx2", "tx3"} {
		if got[id] == 0 {
			t.Fatalf("b never received %s after recovery: %v", id, got)
		}
	}
}

// Past the queue bound the oldest entries are dropped; once the queue
// drains, the pool asks for a full resync of that endpoint exactly once.
func TestPool_CatchupOverflowRequestsResync(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	b.watchCode.Store(http.StatusServiceUnavailable)
	b.healthCode.Store(http.StatusServiceUnavailable)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second,
		WithFailureThreshold(1), WithProbeInterval(10*time.Millisecond), WithCatchupQueueSize(2))
	resyncs := make(chan string, 4)
	p.OnResyncNeeded(func(endpoint string) { resyncs <- endpoint })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	for _, id := range []string{"tx1", "tx2", "tx3", "tx4", "tx5"} {
		if err := p.Register(ctx, id, testCallbackURL, ""); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	if got := pendingFor(p, b.url()); got != 2 {
		t.Fatalf("queue must be capped at 2, got %d", got)
	}
	select {
	case ep := <-resyncs:
		t.Fatalf("resync must not fire before the queue drains, got %s", ep)
	default:
	}

	b.healthCode.Store(http.StatusOK)
	b.watchCode.Store(http.StatusOK)
	select {
	case ep := <-resyncs:
		if ep != b.url() {
			t.Fatalf("resync for %s, want %s", ep, b.url())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resync hook never fired after the overflowed queue drained")
	}
	if got := pendingFor(p, b.url()); got != 0 {
		t.Fatalf("queue must be empty after drain, got %d", got)
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case ep := <-resyncs:
		t.Fatalf("resync must fire once per overflow, got a second one for %s", ep)
	default:
	}
}

// The Service from Endpoint() targets one endpoint, keeps its failures in
// the catch-up queue, and refuses fast while the breaker is open.
func TestPool_EndpointServiceQueuesFailures(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second, WithFailureThreshold(2))
	ctx := context.Background()
	single := p.Endpoint(b.url())
	if single == nil {
		t.Fatal("known endpoint must be returned")
	}

	b.setFail("tx2", true)
	errs := single.RegisterBatchWithResults(ctx, []Registration{{TxID: "tx1"}, {TxID: "tx2"}}, 2)
	if errs[0] != nil || errs[1] == nil {
		t.Fatalf("errs = %v, want tx1 ok / tx2 refused", errs)
	}
	if a.watchedCount() != 0 {
		t.Fatal("Endpoint() must not fan out to other endpoints")
	}
	if got := pendingFor(p, b.url()); got != 1 {
		t.Fatalf("refused tx must be queued for b, pending = %d", got)
	}

	b.watchCode.Store(http.StatusServiceUnavailable)
	_ = single.Register(ctx, "tx3", testCallbackURL, "")
	_ = single.Register(ctx, "tx4", testCallbackURL, "")
	if st := p.EndpointStatuses()[1]; st.Healthy {
		t.Fatalf("two 5xx through Endpoint() must open the breaker: %+v", st)
	}
	before := b.watchedCount()
	if err := single.Register(ctx, "tx5", testCallbackURL, ""); !errors.Is(err, ErrNoHealthyEndpoints) {
		t.Fatalf("open endpoint must refuse fast, got %v", err)
	}
	if b.watchedCount() != before {
		t.Fatal("open endpoint must not be called")
	}
	if got := pendingFor(p, b.url()); got != 4 {
		t.Fatalf("tx2..tx5 must all be owed to b, pending = %d", got)
	}
	if got := testutil.ToFloat64(metrics.MerkleEndpointCatchupPending.WithLabelValues(b.url())); got != 4 {
		t.Fatalf("catchup_pending gauge = %v, want 4 (refused while open must update it)", got)
	}
}

// An outright 4xx rejection through Endpoint() is not queued: re-sending it
// cannot succeed.
func TestPool_EndpointServiceDoesNotQueuePermanentRejections(t *testing.T) {
	b := newFakeMerkle(t)
	p := NewPool([]string{b.url()}, "", time.Second)
	b.setFailCode("tx1", http.StatusBadRequest)
	errs := p.Endpoint(b.url()).RegisterBatchWithResults(context.Background(), []Registration{{TxID: "tx1"}}, 1)
	if errs[0] == nil {
		t.Fatal("tx1 must be refused")
	}
	if got := pendingFor(p, b.url()); got != 0 {
		t.Fatalf("a 400 must not be queued, pending = %d", got)
	}
}

// A catch-up entry the endpoint refuses again goes back to the queue: it is
// still owed, and dropping it would leave the endpoint missing the txid.
func TestPool_FailedCatchupIsRequeued(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	b.setFail("tx2", true)
	// A high threshold keeps the breaker out of it: this is about the queue.
	p := NewPool([]string{a.url(), b.url()}, "", time.Second,
		WithFailureThreshold(1000), WithProbeInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	if err := p.Register(ctx, "tx2", testCallbackURL, ""); err != nil {
		t.Fatalf("register: %v (a accepts)", err)
	}
	// A second catch-up attempt can only come from the entry having been
	// put back after the first one failed.
	refused := metrics.MerkleEndpointRequestsTotal.WithLabelValues(b.url(), opCatchup, outcome5xx)
	waitFor(t, "a second catch-up attempt on b", func() bool { return testutil.ToFloat64(refused) >= 2 })
	if got := pendingFor(p, b.url()); got != 1 {
		t.Fatalf("tx2 must still be owed to b, pending = %d", got)
	}
}

// A registration the endpoint rejects outright (400) is dropped from the
// catch-up queue; a throttled one (429) is kept and retried.
func TestPool_CatchupDropsPermanentRejectionsOnly(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	b.setFailCode("bad", http.StatusBadRequest)
	b.setFailCode("slow", http.StatusTooManyRequests)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second, WithProbeInterval(10*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)

	regs := []Registration{{TxID: "bad", CallbackURL: testCallbackURL}, {TxID: "slow", CallbackURL: testCallbackURL}}
	for i, err := range p.RegisterBatchWithResults(ctx, regs, 2) {
		if err != nil {
			t.Fatalf("errs[%d]=%v, a accepted both", i, err)
		}
	}
	if got := pendingFor(p, b.url()); got != 2 {
		t.Fatalf("both refusals must be owed to b first, pending = %d", got)
	}
	waitFor(t, "a catch-up attempt for both", func() bool {
		got := b.watchedSet()
		return got["bad"] >= 2 && got["slow"] >= 2
	})
	waitFor(t, "only the 429 to stay owed", func() bool { return pendingFor(p, b.url()) == 1 })
	time.Sleep(100 * time.Millisecond)
	if got := b.watchedSet()["bad"]; got != 2 {
		t.Fatalf("a 400 must not be re-sent after the catch-up attempt, sent %d times", got)
	}

	b.setFailCode("slow", 0)
	waitFor(t, "the 429 entry to land", func() bool { return pendingFor(p, b.url()) == 0 })
}

// An endpoint that hangs (no answer until the HTTP timeout) must cost the
// batch that trips its breaker no more than the requests already in flight,
// not one timeout per maxConcurrency entries; everything it did not get is
// owed to it.
func TestPool_HangingEndpointDoesNotHoldTheBatch(t *testing.T) {
	a := newFakeMerkle(t)
	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(hang.Close)
	t.Cleanup(func() { close(release) }) // runs first: unblock handlers so Close returns

	const timeout = 200 * time.Millisecond
	p := NewPool([]string{a.url(), hang.URL}, "", timeout, WithFailureThreshold(3))
	regs := make([]Registration, 12)
	for i := range regs {
		regs[i] = Registration{TxID: "tx" + strconv.Itoa(i), CallbackURL: testCallbackURL}
	}

	start := time.Now()
	errs := p.RegisterBatchWithResults(context.Background(), regs, 2)
	elapsed := time.Since(start)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("errs[%d]=%v, a accepted every tx", i, err)
		}
	}
	// Concurrency 2, threshold 3: two rounds of timeouts trip the breaker,
	// then the other eight are skipped. Sending all twelve would take six.
	if elapsed > 4*timeout {
		t.Fatalf("first batch took %v; the breaker must stop the remaining sends (6 rounds = %v)", elapsed, 6*timeout)
	}
	if st := p.EndpointStatuses()[1]; st.Healthy || st.CatchupPending != len(regs) {
		t.Fatalf("hanging endpoint must be open and owed all %d txs: %+v", len(regs), st)
	}

	start = time.Now()
	_ = p.RegisterBatchWithResults(context.Background(), regs, 2)
	if elapsed := time.Since(start); elapsed > timeout {
		t.Fatalf("with the breaker open the batch must not wait on the endpoint, took %v", elapsed)
	}
}

func TestPool_RegisterBatch_ReturnsFirstFailureWithoutFailFast(t *testing.T) {
	a, b := newFakeMerkle(t), newFakeMerkle(t)
	a.failTxIDs["tx1"] = http.StatusInternalServerError
	b.failTxIDs["tx1"] = http.StatusInternalServerError
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

// A canceled context during a probe is counted as canceled, not as an
// endpoint failure.
func TestPool_ProbeCancellationIsNotNetworkFailure(t *testing.T) {
	a := newFakeMerkle(t)
	a.watchCode.Store(http.StatusServiceUnavailable)
	p := NewPool([]string{a.url()}, "", time.Second, WithFailureThreshold(1))
	_ = p.Register(context.Background(), "tx", testCallbackURL, "")
	if st := p.EndpointStatuses()[0]; st.Healthy {
		t.Fatalf("breaker should be open: %+v", st)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := testutil.ToFloat64(metrics.MerkleEndpointRequestsTotal.WithLabelValues(a.url(), opProbe, outcomeCanceled))
	p.probeOpenEndpoints(ctx)
	if got := testutil.ToFloat64(metrics.MerkleEndpointRequestsTotal.WithLabelValues(a.url(), opProbe, outcomeCanceled)) - before; got != 1 {
		t.Fatalf("probe under a canceled context must count as canceled, delta=%v", got)
	}
	if st := p.EndpointStatuses()[0]; st.Healthy {
		t.Fatalf("a canceled probe must not close the breaker: %+v", st)
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

// /health is shallow: an endpoint whose /health answers while /watch hangs
// must stay open (a trial /watch of one owed registration fails) instead of
// closing every probe tick and holding the next hot-path batch.
func TestPool_ProbeKeepsBreakerOpenWhileWatchHangs(t *testing.T) {
	a := newFakeMerkle(t)
	var watchOK atomic.Bool
	release := make(chan struct{})
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || watchOK.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(b.Close)
	t.Cleanup(func() { close(release) })

	p := NewPool([]string{a.url(), b.URL}, "", 100*time.Millisecond, WithFailureThreshold(1))
	ctx := context.Background()
	if err := p.Register(ctx, "tx1", testCallbackURL, ""); err != nil {
		t.Fatalf("register: %v (a accepts)", err)
	}
	if st := p.EndpointStatuses()[1]; st.Healthy || st.CatchupPending != 1 {
		t.Fatalf("b must be open and owe tx1: %+v", st)
	}

	p.probeOpenEndpoints(ctx)
	if st := p.EndpointStatuses()[1]; st.Healthy || st.CatchupPending != 1 {
		t.Fatalf("/health up but /watch hanging must keep b open and owing tx1: %+v", st)
	}

	watchOK.Store(true)
	p.probeOpenEndpoints(ctx)
	if st := p.EndpointStatuses()[1]; !st.Healthy || st.CatchupPending != 0 {
		t.Fatalf("b must close once the trial /watch lands: %+v", st)
	}
}

// hangingServer answers nothing until the request is canceled or release is
// closed: a black-holed merkle-service (paused container, dead IP).
func hangingServer(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(hang.Close)
	t.Cleanup(func() { close(release) }) // runs first: unblock handlers so Close returns
	return hang
}

func regsN(n int) []Registration {
	regs := make([]Registration, n)
	for i := range regs {
		regs[i] = Registration{TxID: "tx" + strconv.Itoa(i), CallbackURL: testCallbackURL}
	}
	return regs
}

// Found by chaos testing: with one of three merkle-services black-holed, every
// batch waited out the full HTTP timeout (30s in production) on that endpoint
// although the other two had accepted every tx in milliseconds, so broadcast
// stalled for the whole window before the breaker tripped. Once every tx is
// accepted somewhere, the stragglers get a short grace and are then canceled;
// the cancellation counts against their breaker and the txs they owe go to
// the catch-up queue.
func TestPool_StragglerDoesNotHoldTheBatchOnceEveryTxIsAccepted(t *testing.T) {
	a := newFakeMerkle(t)
	hang := hangingServer(t)
	const timeout, grace = 10 * time.Second, 100 * time.Millisecond
	p := NewPool([]string{a.url(), hang.URL}, "", timeout, WithFailureThreshold(3), WithStragglerGrace(grace))
	regs := regsN(12)

	start := time.Now()
	errs := p.RegisterBatchWithResults(context.Background(), regs, 4)
	elapsed := time.Since(start)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("errs[%d]=%v, a accepted every tx", i, err)
		}
	}
	if elapsed > 10*grace {
		t.Fatalf("batch took %v; it must return about one grace (%v) after a accepted everything, not wait for the %v timeout", elapsed, grace, timeout)
	}
	st := p.EndpointStatuses()[1]
	if st.Healthy {
		t.Fatalf("the canceled in-flight /watch calls must count against the hanging endpoint's breaker: %+v", st)
	}
	if st.CatchupPending != len(regs) {
		t.Fatalf("hanging endpoint must owe all %d txs, owes %d", len(regs), st.CatchupPending)
	}
}

// With no other endpoint to accept, nothing is covered and nothing is cut
// short: the batch waits for the endpoint's own answer (here its timeout).
func TestPool_SingleHangingEndpointStillWaitsForItsTimeout(t *testing.T) {
	hang := hangingServer(t)
	const timeout, grace = 300 * time.Millisecond, 20 * time.Millisecond
	p := NewPool([]string{hang.URL}, "", timeout, WithStragglerGrace(grace))
	start := time.Now()
	errs := p.RegisterBatchWithResults(context.Background(), regsN(2), 2)
	if elapsed := time.Since(start); elapsed < timeout {
		t.Fatalf("returned after %v, before the endpoint's %v timeout", elapsed, timeout)
	}
	for i, err := range errs {
		if err == nil {
			t.Fatalf("errs[%d] = nil, nothing accepted it", i)
		}
	}
}

// A slower endpoint that answers within the grace keeps its registrations and
// its health.
func TestPool_SlowEndpointWithinGraceIsNotPenalized(t *testing.T) {
	a := newFakeMerkle(t)
	b := newFakeMerkle(t)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		b.srv.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(slow.Close)
	p := NewPool([]string{a.url(), slow.URL}, "", 5*time.Second, WithStragglerGrace(2*time.Second))
	regs := regsN(6)
	for i, err := range p.RegisterBatchWithResults(context.Background(), regs, 6) {
		if err != nil {
			t.Fatalf("errs[%d]=%v", i, err)
		}
	}
	st := p.EndpointStatuses()[1]
	if !st.Healthy || st.CatchupPending != 0 || st.ConsecutiveFailures != 0 {
		t.Fatalf("slow-but-answering endpoint must be healthy and owe nothing: %+v", st)
	}
	if b.watchedCount() != len(regs) {
		t.Fatalf("slow endpoint got %d of %d registrations", b.watchedCount(), len(regs))
	}
}

// What a canceled straggler owes is not lost: once the endpoint answers again
// the probe closes its breaker and the catch-up drain delivers every tx.
func TestPool_CanceledStragglerOwedTxsDrainAfterRecovery(t *testing.T) {
	a := newFakeMerkle(t)
	b := newFakeMerkle(t)
	var hung atomic.Bool
	hung.Store(true)
	release := make(chan struct{})
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hung.Load() {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		b.srv.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(flaky.Close)
	t.Cleanup(func() { close(release) })

	p := NewPool([]string{a.url(), flaky.URL}, "", 10*time.Second,
		WithFailureThreshold(3), WithStragglerGrace(50*time.Millisecond), WithProbeInterval(20*time.Millisecond))
	regs := regsN(8)
	for i, err := range p.RegisterBatchWithResults(context.Background(), regs, 4) {
		if err != nil {
			t.Fatalf("errs[%d]=%v", i, err)
		}
	}
	if st := p.EndpointStatuses()[1]; st.Healthy || st.CatchupPending != len(regs) {
		t.Fatalf("straggler must be open and owe all %d: %+v", len(regs), st)
	}

	hung.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	waitFor(t, "straggler's owed registrations to drain", func() bool {
		st := p.EndpointStatuses()[1]
		return st.Healthy && st.CatchupPending == 0
	})
	got := b.watchedSet()
	for _, r := range regs {
		if got[r.TxID] == 0 {
			t.Fatalf("%s never reached the recovered endpoint: %v", r.TxID, got)
		}
	}
}

// selectiveHangServer accepts /watch for the txids in accept and hangs on
// every other one until the request is canceled.
func selectiveHangServer(t *testing.T, accept ...string) *httptest.Server {
	t.Helper()
	ok := make(map[string]bool, len(accept))
	for _, id := range accept {
		ok[id] = true
	}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID string `json:"txid"`
		}
		decodeJSON(r, &req)
		if ok[req.TxID] {
			w.WriteHeader(http.StatusOK)
			return
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv
}

// Neither endpoint finishes its own batch, but between them every tx is
// accepted: A takes tx0 and hangs on tx1, B the reverse. The grace timer must
// start from the per-tx acceptances, not wait for a whole endpoint batch.
func TestPool_ComplementaryAcceptanceStartsTheGrace(t *testing.T) {
	a := selectiveHangServer(t, "tx0")
	b := selectiveHangServer(t, "tx1")
	const timeout, grace = 10 * time.Second, 100 * time.Millisecond
	p := NewPool([]string{a.URL, b.URL}, "", timeout, WithFailureThreshold(3), WithStragglerGrace(grace))
	regs := regsN(2)

	start := time.Now()
	errs := p.RegisterBatchWithResults(context.Background(), regs, 2)
	elapsed := time.Since(start)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("errs[%d]=%v, one endpoint accepted each tx", i, err)
		}
	}
	if elapsed > 10*grace {
		t.Fatalf("batch took %v; complementary coverage must return about one grace (%v) after the last acceptance, not wait for the %v timeout", elapsed, grace, timeout)
	}
	if got := pendingFor(p, a.URL); got != 1 {
		t.Fatalf("a must owe tx1, owes %d", got)
	}
	if got := pendingFor(p, b.URL); got != 1 {
		t.Fatalf("b must owe tx0, owes %d", got)
	}
}

// An endpoint skipped because its breaker is already open counts one
// skipped_open per registration it would have received, matching the
// per-request help text and mid-batch skips; /reprocess counts one.
func TestPool_SkippedOpenCountsEveryRegistration(t *testing.T) {
	a := newFakeMerkle(t)
	b := newFakeMerkle(t)
	b.watchCode.Store(http.StatusInternalServerError)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second, WithFailureThreshold(1))
	if err := p.Register(context.Background(), "trip", testCallbackURL, ""); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if p.EndpointStatuses()[1].Healthy {
		t.Fatal("b's breaker must be open after one failure")
	}
	watch := metrics.MerkleEndpointRequestsTotal.WithLabelValues(b.url(), opWatch, outcomeSkippedOpen)
	before := testutil.ToFloat64(watch)
	p.RegisterBatchWithResults(context.Background(), regsN(25), 5)
	if got := testutil.ToFloat64(watch) - before; got != 25 {
		t.Fatalf("skipped_open grew by %v for a 25-entry batch, want 25", got)
	}
	reprocess := metrics.MerkleEndpointRequestsTotal.WithLabelValues(b.url(), opReprocess, outcomeSkippedOpen)
	before = testutil.ToFloat64(reprocess)
	_ = p.Reprocess(context.Background(), "blockhash", testCallbackURL, "")
	if got := testutil.ToFloat64(reprocess) - before; got != 1 {
		t.Fatalf("skipped_open grew by %v for one /reprocess, want 1", got)
	}
}

// Retain queues only retryable failures, and queues them for every endpoint.
func TestPool_RetainSkipsSuccessesAndPermanentRejections(t *testing.T) {
	a := newFakeMerkle(t)
	b := newFakeMerkle(t)
	p := NewPool([]string{a.url(), b.url()}, "", time.Second)
	regs := regsN(3)
	p.Retain(regs, []error{
		nil,
		&RegisterError{StatusCode: http.StatusBadRequest},
		&RegisterError{StatusCode: http.StatusServiceUnavailable},
	})
	for _, st := range p.EndpointStatuses() {
		if st.CatchupPending != 1 {
			t.Fatalf("endpoint %s must owe only the 503'd tx, got %+v", st.URL, st)
		}
	}
}
