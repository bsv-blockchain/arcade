package api_server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// fakePublisher is a test-only events.Publisher. It captures published
// updates and lets tests synthesize subscriber-side delivery.
type fakePublisher struct {
	mu          sync.Mutex
	published   []*models.TransactionStatus
	subscribers []chan *models.TransactionStatus
}

func (p *fakePublisher) Publish(_ context.Context, status *models.TransactionStatus) error {
	p.mu.Lock()
	subs := append([]chan *models.TransactionStatus(nil), p.subscribers...)
	p.published = append(p.published, status)
	p.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- status:
		default:
		}
	}
	return nil
}

func (p *fakePublisher) Subscribe(_ context.Context, _ string) (<-chan *models.TransactionStatus, error) {
	ch := make(chan *models.TransactionStatus, 64)
	p.mu.Lock()
	p.subscribers = append(p.subscribers, ch)
	p.mu.Unlock()
	return ch, nil
}

func (p *fakePublisher) Close() error { return nil }

// sseStoreStub extends mockStore with submission/status fixtures so we can
// drive token filtering and Last-Event-ID catchup paths.
//
// The fixtures are mutated from test goroutines while the SSE handler reads
// them from its own goroutine (via httptest.Server), so accesses are guarded
// by a mutex to keep `go test -race` clean.
type sseStoreStub struct {
	mockStore

	mu          sync.RWMutex
	subsByToken map[string][]*models.Submission
	statusByTx  map[string]*models.TransactionStatus
}

func (s *sseStoreStub) GetSubmissionsByToken(_ context.Context, token string) ([]*models.Submission, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subsByToken[token], nil
}

func (s *sseStoreStub) GetStatus(_ context.Context, txid string) (*models.TransactionStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statusByTx[txid], nil
}

func (s *sseStoreStub) setStatus(txid string, status *models.TransactionStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusByTx[txid] = status
}

// setupSSEServer wires up an api_server.Server backed by a fakePublisher and
// the supplied store stub. The returned cancel must be deferred to release
// the manager goroutine.
func setupSSEServer(t *testing.T, st *sseStoreStub) (*Server, *gin.Engine, *fakePublisher, context.CancelFunc) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pub := &fakePublisher{}
	srv := &Server{
		cfg:       &config.Config{},
		logger:    zap.NewNop(),
		store:     st,
		publisher: pub,
	}
	ctx, cancel := context.WithCancel(t.Context())
	mgr, err := newSSEManager(ctx, pub, st, zap.NewNop())
	if err != nil {
		cancel()
		t.Fatalf("newSSEManager: %v", err)
	}
	srv.sse = mgr
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router, pub, cancel
}

// TestSSEFrameFormat verifies the wire format byte-for-byte against what the
// old arcade emits. Field order, separators, and trailing blank line all
// matter — clients parse SSE strictly per the spec.
func TestSSEFrameFormat(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			"tok-A": {{TxID: "abc", CallbackToken: "tok-A"}},
		},
	}
	_, router, pub, cancel := setupSSEServer(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken=tok-A", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", got)
	}

	// Drive one event into the publisher; the manager will fan it to the
	// connected client. Tiny sleep gives the handler goroutine time to
	// register before the publish lands.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = pub.Publish(t.Context(), &models.TransactionStatus{
			TxID:      "abc",
			Status:    models.StatusMined,
			Timestamp: time.Unix(0, 1700000000000000000).UTC(),
		})
	}()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}

	wantPrefix := "id: 1700000000000000000\nevent: status\ndata: "
	if !strings.HasPrefix(frame, wantPrefix) {
		t.Errorf("frame prefix mismatch:\n got %q\nwant %q...", frame, wantPrefix)
	}
	dataLine := strings.TrimPrefix(frame, wantPrefix)
	dataLine = strings.TrimSuffix(dataLine, "\n\n")
	var payload map[string]string
	if err := json.Unmarshal([]byte(dataLine), &payload); err != nil {
		t.Fatalf("unmarshal data: %v (raw=%q)", err, dataLine)
	}
	if payload["txid"] != "abc" || payload["txStatus"] != string(models.StatusMined) {
		t.Errorf("payload = %+v", payload)
	}
	if _, err := time.Parse(time.RFC3339, payload["timestamp"]); err != nil {
		t.Errorf("timestamp not RFC3339: %q", payload["timestamp"])
	}
}

// TestSSETokenFilter verifies that an event whose txid does not belong to
// the connecting client's callback token is suppressed.
func TestSSETokenFilter(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			"tok-A": {{TxID: "match", CallbackToken: "tok-A"}},
		},
	}
	_, router, pub, cancel := setupSSEServer(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken=tok-A", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	go func() {
		time.Sleep(50 * time.Millisecond)
		// First update is for a tx the token doesn't own — should be dropped.
		_ = pub.Publish(t.Context(), &models.TransactionStatus{TxID: "other", Status: models.StatusMined, Timestamp: time.Now()})
		// Second update should be delivered.
		_ = pub.Publish(t.Context(), &models.TransactionStatus{TxID: "match", Status: models.StatusMined, Timestamp: time.Now()})
	}()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"match"`) {
		t.Errorf("expected match-tx frame, got %q", frame)
	}
}

// TestSSECatchup replays a Last-Event-ID-driven catchup pass. The handler
// must emit any frame whose timestamp is after the supplied ns and skip the
// older one.
func TestSSECatchup(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	older := now.Add(-time.Hour)
	newer := now

	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			"tok-A": {
				{TxID: "old", CallbackToken: "tok-A"},
				{TxID: "new", CallbackToken: "tok-A"},
			},
		},
		statusByTx: map[string]*models.TransactionStatus{
			"old": {TxID: "old", Status: models.StatusMined, Timestamp: older},
			"new": {TxID: "new", Status: models.StatusMined, Timestamp: newer},
		},
	}
	_, router, _, cancel := setupSSEServer(t, st)
	defer cancel()

	srv := httptest.NewServer(router)
	defer srv.Close()

	since := older.Add(time.Minute).UnixNano() // strictly between older and newer
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/events?callbackToken=tok-A", nil)
	req.Header.Set("Last-Event-ID", fmt.Sprintf("%d", since))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	frame, err := readNextSSEFrame(resp.Body, 2*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"new"`) {
		t.Errorf("expected catchup of newer tx, got %q", frame)
	}
	if strings.Contains(frame, `"txid":"old"`) {
		t.Errorf("older tx should not have replayed: %q", frame)
	}
}

// TestSSENoPublisher verifies that the endpoint returns 503 when no
// Publisher is wired up — the degraded-deployment path.
func TestSSENoPublisher(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:    &config.Config{},
		logger: zap.NewNop(),
		store:  &sseStoreStub{},
		// publisher and sse intentionally nil
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/events", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
}

// TestSSEFanOutConcurrentUnregister stresses the F-020 fix: many clients
// register, then half are unregistered concurrently with a fan-out push. The
// previous implementation closed c.ch on unregister, which races a fan-out
// send and panics with "send on closed channel". With the fix, fan-out
// selects on the per-client ctx and bails cleanly when a client is gone, so
// no panic ever occurs even under aggressive interleaving.
func TestSSEFanOutConcurrentUnregister(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEServer(t, st)
	defer cancel()

	// Build a manager directly (bypass HTTP) so we can drive register /
	// unregister / fanOut on the same struct the production path uses.
	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newSSEManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newSSEManager: %v", err)
	}

	const N = 200
	clients := make([]*sseClient, N)
	for i := 0; i < N; i++ {
		c := mgr.newClient("") // empty token → no store filter
		mgr.register(c)
		clients[i] = c
	}

	startGoroutines := runtime.NumGoroutine()

	status := &models.TransactionStatus{
		TxID:      "race-tx",
		Status:    models.StatusMined,
		Timestamp: time.Unix(0, 1700000000000000000).UTC(),
	}

	// Race: half the clients unregister while a stream of fan-outs happens.
	// Repeat enough times to exercise the interleaving.
	const iterations = 50
	for it := 0; it < iterations; it++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i += 2 {
				mgr.unregister(clients[i].id)
			}
		}()
		go func() {
			defer wg.Done()
			// fanOut must not panic even though half the clients are
			// being unregistered (and previously had their ch closed)
			// concurrently.
			mgr.fanOut(ctx, status)
		}()
		wg.Wait()

		// Re-register fresh clients in those slots so the next iteration
		// has another batch to race.
		for i := 0; i < N; i += 2 {
			c := mgr.newClient("")
			mgr.register(c)
			clients[i] = c
		}
	}

	// Drain remaining: cancel ctx → manager goroutine exits, all client
	// ctxs cancel via parent. Allow scheduler a beat to clean up.
	mgrCancel()
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= startGoroutines+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Best-effort goroutine-leak check: we don't pin an exact count
	// (httptest / runtime can spawn helpers) but we tolerate a small
	// fudge. The real assertion is "no panic" above.
	t.Logf("goroutines start=%d end=%d (fudge tolerated)", startGoroutines, runtime.NumGoroutine())
}

// TestSSEFanOutSlowClientDrops verifies that a client whose buffer is full
// causes fan-out to take the drop arm rather than block. The slow_client
// counter must increment by exactly the number of drops, and the fan-out
// must complete promptly even though the slow client never drains.
func TestSSEFanOutSlowClientDrops(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEServer(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newSSEManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newSSEManager: %v", err)
	}

	slow := mgr.newClient("")
	mgr.register(slow)
	defer mgr.unregister(slow.id)

	// Fill the slow client's buffer to capacity so subsequent fan-outs
	// must take the default-drop arm.
	for i := 0; i < cap(slow.ch); i++ {
		slow.ch <- &models.TransactionStatus{TxID: "filler", Timestamp: time.Now()}
	}

	before := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client"))

	const drops = 5
	done := make(chan struct{})
	go func() {
		for i := 0; i < drops; i++ {
			mgr.fanOut(ctx, &models.TransactionStatus{
				TxID:      "drop",
				Status:    models.StatusMined,
				Timestamp: time.Now(),
			})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fanOut blocked on slow client (should have dropped)")
	}

	after := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("slow_client"))
	if got := after - before; got != drops {
		t.Errorf("slow_client drops = %v, want %d", got, drops)
	}
}

// TestSSEFanOutClientGoneDrops ensures a client whose ctx has been canceled
// (i.e. unregister already ran) falls through the ctx.Done() arm of the
// fan-out select rather than panicking on a closed channel. We invoke
// fanOut directly so the race is deterministic.
func TestSSEFanOutClientGoneDrops(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEServer(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newSSEManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newSSEManager: %v", err)
	}

	gone := mgr.newClient("")
	mgr.register(gone)
	// Cancel WITHOUT removing from the map so fanOut still sees this
	// client in its snapshot — exactly the race the bug describes.
	gone.cancel()

	before := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("client_gone"))

	// Must not panic.
	mgr.fanOut(ctx, &models.TransactionStatus{
		TxID:      "gone",
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	after := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("client_gone"))
	if got := after - before; got < 1 {
		t.Errorf("client_gone drops = %v, want >= 1", got)
	}
}

// readNextSSEFrame reads bytes from r until two consecutive newlines mark
// the end of an SSE frame, or until timeout. Returns the frame text
// including the terminating "\n\n".
func readNextSSEFrame(r io.Reader, timeout time.Duration) (string, error) {
	type result struct {
		s   string
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		br := bufio.NewReader(r)
		var buf bytes.Buffer
		for {
			line, err := br.ReadBytes('\n')
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			buf.Write(line)
			// SSE comments start with ":" — skip them so keepalives don't
			// satisfy the test prematurely.
			if len(line) > 0 && line[0] == ':' {
				buf.Reset()
				continue
			}
			if len(line) == 1 && line[0] == '\n' {
				if buf.Len() > 1 {
					resultCh <- result{s: buf.String()}
					return
				}
				buf.Reset()
			}
		}
	}()

	select {
	case r := <-resultCh:
		return r.s, r.err
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout waiting for SSE frame")
	}
}
