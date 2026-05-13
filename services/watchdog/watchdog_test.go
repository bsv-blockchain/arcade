package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// watchdogStore is a hand-rolled store fake. Embeds the real store.Store
// interface so any methods we don't override panic loudly — keeps the
// surface area proportional to what the watchdog actually touches.
type watchdogStore struct {
	store.Store

	mu        sync.Mutex
	tipHeight uint64
	tipErr    error
	stale     []*models.BlockProcessingStatus
	staleErr  error

	listCalls []listStaleCall
}

type listStaleCall struct {
	olderThan time.Time
	minHeight uint64
	limit     int
}

func (s *watchdogStore) GetActiveTipBlockHeight(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tipHeight, s.tipErr
}

func (s *watchdogStore) ListStaleBlockProcessingStatus(_ context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls = append(s.listCalls, listStaleCall{olderThan: olderThan, minHeight: minHeight, limit: limit})
	if s.staleErr != nil {
		return nil, s.staleErr
	}
	out := make([]*models.BlockProcessingStatus, 0, len(s.stale))
	for _, r := range s.stale {
		if r.BlockHeight < minHeight {
			continue
		}
		if !r.HeaderSeenAt.Before(olderThan) {
			continue
		}
		if r.ProcessedAt != nil {
			continue
		}
		if r.Status != "" && r.Status != models.BlockStatusActive {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].HeaderSeenAt.Before(out[j].HeaderSeenAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *watchdogStore) markProcessed(hash string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.stale {
		if r.BlockHash == hash {
			t := at
			r.ProcessedAt = &t
			return
		}
	}
}

func (s *watchdogStore) captureListCalls() []listStaleCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]listStaleCall(nil), s.listCalls...)
}

type watchdogLeaser struct {
	mu        sync.Mutex
	responses []leaseResponse
	calls     int
}

type leaseResponse struct {
	heldUntil time.Time
	err       error
}

func alwaysLeader() *watchdogLeaser {
	return &watchdogLeaser{}
}

func scriptedLeaser(responses ...leaseResponse) *watchdogLeaser {
	return &watchdogLeaser{responses: append([]leaseResponse(nil), responses...)}
}

func (l *watchdogLeaser) TryAcquireOrRenew(_ context.Context, _, _ string, ttl time.Duration) (time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if len(l.responses) == 0 {
		return time.Now().Add(ttl), nil
	}
	r := l.responses[0]
	if len(l.responses) > 1 {
		l.responses = l.responses[1:]
	}
	return r.heldUntil, r.err
}

func (l *watchdogLeaser) Release(context.Context, string, string) error { return nil }

type recordingReprocessServer struct {
	mu      sync.Mutex
	calls   []reprocessCall
	server  *httptest.Server
	respond func(blockHash string) (status int, body string)
}

type reprocessCall struct {
	blockHash   string
	callbackURL string
}

func newReprocessServer(t *testing.T) *recordingReprocessServer {
	t.Helper()
	r := &recordingReprocessServer{
		respond: func(string) (int, string) { return http.StatusAccepted, "" },
	}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			BlockHash   string `json:"blockHash"`
			CallbackURL string `json:"callbackUrl"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.calls = append(r.calls, reprocessCall{blockHash: body.BlockHash, callbackURL: body.CallbackURL})
		respFn := r.respond
		r.mu.Unlock()

		status, payload := respFn(body.BlockHash)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(payload))
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *recordingReprocessServer) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingReprocessServer) setResponder(fn func(string) (int, string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.respond = fn
}

func (r *recordingReprocessServer) capturedCalls() []reprocessCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]reprocessCall(nil), r.calls...)
}

// --- tests ---

func newTestWatchdog(t *testing.T, st *watchdogStore, leaser *watchdogLeaser, mc *merkleservice.Client) *Watchdog {
	t.Helper()
	cfg := Config{
		Interval:        10 * time.Millisecond,
		StaleThreshold:  2 * time.Minute,
		RecencyDepth:    144,
		BatchSize:       100,
		MaxConcurrent:   4,
		LeaseTTL:        30 * time.Second,
		InitialBackoff:  time.Minute,
		MaxBackoff:      30 * time.Minute,
		TerminalBackoff: 4 * time.Hour,
	}
	w := New(st, leaser, mc, "http://arcade/callback", "tok", cfg, zap.NewNop())
	// Tests assert on exact backoff values; pin jitter to identity so the
	// production ±20% spread doesn't bleed into per-test equality checks.
	w.jitter = identityJitter
	return w
}

func TestWatchdog_TriggersReprocessForStaleRow(t *testing.T) {
	srv := newReprocessServer(t)
	mc := merkleservice.NewClient(srv.server.URL, "auth", 5*time.Second)

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	stale := time.Date(2026, 5, 7, 11, 50, 0, 0, time.UTC) // 10 min old

	st := &watchdogStore{
		tipHeight: 1000,
		stale: []*models.BlockProcessingStatus{
			{BlockHash: "blk-1", BlockHeight: 1000, HeaderSeenAt: stale, Status: models.BlockStatusActive},
		},
	}
	w := newTestWatchdog(t, st, alwaysLeader(), mc)
	w.now = func() time.Time { return now }

	w.tick(context.Background())

	if srv.callCount() != 1 {
		t.Fatalf("reprocess calls=%d want 1", srv.callCount())
	}
	calls := srv.capturedCalls()
	if calls[0].blockHash != "blk-1" || calls[0].callbackURL != "http://arcade/callback" {
		t.Errorf("got call=%+v", calls[0])
	}
}

func TestWatchdog_RespectsRecencyDepth(t *testing.T) {
	srv := newReprocessServer(t)
	mc := merkleservice.NewClient(srv.server.URL, "auth", 5*time.Second)

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * time.Minute)

	st := &watchdogStore{
		tipHeight: 1000,
		stale: []*models.BlockProcessingStatus{
			{BlockHash: "below-window", BlockHeight: 500, HeaderSeenAt: stale, Status: models.BlockStatusActive},
			{BlockHash: "recent-enough", BlockHeight: 900, HeaderSeenAt: stale, Status: models.BlockStatusActive},
		},
	}
	w := newTestWatchdog(t, st, alwaysLeader(), mc)
	w.cfg.RecencyDepth = 144 // tip 1000 → minHeight 856
	w.now = func() time.Time { return now }

	w.tick(context.Background())

	calls := srv.capturedCalls()
	if len(calls) != 1 || calls[0].blockHash != "recent-enough" {
		t.Errorf("calls=%+v want one for recent-enough only", calls)
	}
	listCalls := st.captureListCalls()
	if len(listCalls) != 1 || listCalls[0].minHeight != 1000-144 {
		t.Errorf("minHeight passed to store=%d want %d", listCalls[0].minHeight, 1000-144)
	}
}

func TestWatchdog_BacksOffOn5xx(t *testing.T) {
	srv := newReprocessServer(t)
	srv.setResponder(func(string) (int, string) { return http.StatusBadGateway, "" })
	mc := merkleservice.NewClient(srv.server.URL, "auth", 5*time.Second)

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * time.Minute)
	st := &watchdogStore{
		tipHeight: 1000,
		stale: []*models.BlockProcessingStatus{
			{BlockHash: "blk-1", BlockHeight: 1000, HeaderSeenAt: stale, Status: models.BlockStatusActive},
		},
	}
	w := newTestWatchdog(t, st, alwaysLeader(), mc)
	w.now = func() time.Time { return now }

	w.tick(context.Background())
	if srv.callCount() != 1 {
		t.Fatalf("first tick calls=%d want 1", srv.callCount())
	}

	w.now = func() time.Time { return now.Add(30 * time.Second) }
	w.tick(context.Background())
	if srv.callCount() != 1 {
		t.Fatalf("after 30s calls=%d want 1 (backoff not expired)", srv.callCount())
	}

	w.now = func() time.Time { return now.Add(2 * time.Minute) }
	w.tick(context.Background())
	if srv.callCount() != 2 {
		t.Fatalf("after backoff calls=%d want 2", srv.callCount())
	}
}

func TestWatchdog_BacksOffHeavilyOn4xx(t *testing.T) {
	srv := newReprocessServer(t)
	srv.setResponder(func(string) (int, string) { return http.StatusNotFound, "not on chain" })
	mc := merkleservice.NewClient(srv.server.URL, "auth", 5*time.Second)

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * time.Minute)
	st := &watchdogStore{
		tipHeight: 1000,
		stale: []*models.BlockProcessingStatus{
			{BlockHash: "blk-1", BlockHeight: 1000, HeaderSeenAt: stale, Status: models.BlockStatusActive},
		},
	}
	w := newTestWatchdog(t, st, alwaysLeader(), mc)
	w.now = func() time.Time { return now }

	w.tick(context.Background())

	w.now = func() time.Time { return now.Add(30 * time.Minute) }
	w.tick(context.Background())
	if srv.callCount() != 1 {
		t.Fatalf("4xx terminal backoff failed: calls=%d want 1", srv.callCount())
	}

	w.now = func() time.Time { return now.Add(5 * time.Hour) }
	w.tick(context.Background())
	if srv.callCount() != 2 {
		t.Fatalf("after terminal backoff calls=%d want 2", srv.callCount())
	}
}

func TestWatchdog_LeaseSkipsWhenNotLeader(t *testing.T) {
	srv := newReprocessServer(t)
	mc := merkleservice.NewClient(srv.server.URL, "auth", 5*time.Second)

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * time.Minute)
	st := &watchdogStore{
		tipHeight: 1000,
		stale: []*models.BlockProcessingStatus{
			{BlockHash: "blk-1", BlockHeight: 1000, HeaderSeenAt: stale, Status: models.BlockStatusActive},
		},
	}
	leaser := scriptedLeaser(leaseResponse{heldUntil: time.Time{}})
	w := newTestWatchdog(t, st, leaser, mc)
	w.now = func() time.Time { return now }

	w.tick(context.Background())

	if srv.callCount() != 0 {
		t.Fatalf("non-leader fired %d /reprocess calls, want 0", srv.callCount())
	}
	if len(st.captureListCalls()) != 0 {
		t.Errorf("non-leader still queried the store; want short-circuit before query")
	}
}

func TestWatchdog_LeaseInfraErrorSkipsTick(t *testing.T) {
	srv := newReprocessServer(t)
	mc := merkleservice.NewClient(srv.server.URL, "auth", 5*time.Second)

	leaser := scriptedLeaser(leaseResponse{err: errors.New("infra down")})
	st := &watchdogStore{tipHeight: 100}
	w := newTestWatchdog(t, st, leaser, mc)

	w.tick(context.Background())

	if srv.callCount() != 0 {
		t.Fatalf("infra error should skip tick; calls=%d", srv.callCount())
	}
}

func TestWatchdog_DoesNotRetryProcessedRows(t *testing.T) {
	srv := newReprocessServer(t)
	mc := merkleservice.NewClient(srv.server.URL, "auth", 5*time.Second)

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * time.Minute)
	st := &watchdogStore{
		tipHeight: 1000,
		stale: []*models.BlockProcessingStatus{
			{BlockHash: "blk-1", BlockHeight: 1000, HeaderSeenAt: stale, Status: models.BlockStatusActive},
		},
	}
	w := newTestWatchdog(t, st, alwaysLeader(), mc)
	w.now = func() time.Time { return now }

	w.tick(context.Background())
	if srv.callCount() != 1 {
		t.Fatalf("first tick: calls=%d want 1", srv.callCount())
	}

	st.markProcessed("blk-1", now.Add(time.Second))

	w.now = func() time.Time { return now.Add(10 * time.Minute) }
	w.tick(context.Background())
	if srv.callCount() != 1 {
		t.Fatalf("after processed_at set, watchdog still fired /reprocess (calls=%d)", srv.callCount())
	}
}

func TestProportionalJitter_StaysWithinSpread(t *testing.T) {
	const trials = 1000
	base := time.Minute
	spread := time.Duration(float64(base) * backoffJitterFraction)
	lo := base - spread
	hi := base + spread

	var sumDelta int64
	for i := 0; i < trials; i++ {
		got := proportionalJitter(base)
		if got < lo || got > hi {
			t.Fatalf("trial %d: got=%v out of bounds [%v..%v]", i, got, lo, hi)
		}
		sumDelta += int64(got) - int64(base)
	}

	// Mean of a uniform distribution centered on 0 with N samples should
	// hug 0; allow ±5% of the spread as a loose sanity check that the
	// jitter is genuinely centered and not biased high/low.
	meanDelta := time.Duration(sumDelta / trials)
	if meanDelta > spread/20 || meanDelta < -spread/20 {
		t.Errorf("mean jitter=%v leans too far from 0 (spread=%v)", meanDelta, spread)
	}
}

func TestProportionalJitter_ZeroPassesThrough(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if got := proportionalJitter(d); got != d {
			t.Errorf("proportionalJitter(%v)=%v want %v", d, got, d)
		}
	}
}

func TestWatchdog_TransientBackoffGrowsExponentially(t *testing.T) {
	got := []time.Duration{
		transientBackoff(time.Minute, 30*time.Minute, 1),
		transientBackoff(time.Minute, 30*time.Minute, 2),
		transientBackoff(time.Minute, 30*time.Minute, 3),
		transientBackoff(time.Minute, 30*time.Minute, 4),
		transientBackoff(time.Minute, 30*time.Minute, 10),
	}
	want := []time.Duration{
		time.Minute,
		2 * time.Minute,
		4 * time.Minute,
		8 * time.Minute,
		30 * time.Minute, // capped
	}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("failure=%d got=%v want=%v", i+1, g, want[i])
		}
	}
}

func TestWatchdog_NetworkErrorIsTransient(t *testing.T) {
	mc := merkleservice.NewClient("http://127.0.0.1:1", "auth", 50*time.Millisecond)

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-10 * time.Minute)
	st := &watchdogStore{
		tipHeight: 1000,
		stale: []*models.BlockProcessingStatus{
			{BlockHash: "blk-1", BlockHeight: 1000, HeaderSeenAt: stale, Status: models.BlockStatusActive},
		},
	}
	w := newTestWatchdog(t, st, alwaysLeader(), mc)
	w.now = func() time.Time { return now }
	w.tick(context.Background())

	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.attempts["blk-1"]
	if state == nil {
		t.Fatal("expected an attempt entry for blk-1")
	}
	if state.failures != 1 {
		t.Errorf("failures=%d want 1", state.failures)
	}
	gotDelay := state.nextEligibleAt.Sub(now)
	if gotDelay != w.cfg.InitialBackoff {
		t.Errorf("network failure backoff=%v want %v (initial)", gotDelay, w.cfg.InitialBackoff)
	}
}
