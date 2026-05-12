package sse

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
	"github.com/bsv-blockchain/arcade/store"
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

// sseStoreStub is a minimum-surface fake. The SSE service only touches
// GetSubmissionsByToken and GetStatus on the Store; embed the interface
// so every other method panics if accidentally called.
type sseStoreStub struct {
	store.Store

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

// setupSSEService wires a Service backed by a fakePublisher and the
// supplied store stub. Returns the service, a gin router with /events
// registered, the publisher, and a cancel that releases the manager
// goroutine.
func setupSSEService(t *testing.T, st *sseStoreStub) (*Service, *gin.Engine, *fakePublisher, context.CancelFunc) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pub := &fakePublisher{}
	svc := &Service{
		cfg:       &config.Config{SSE: config.SSEConfig{Enabled: true}},
		logger:    zap.NewNop(),
		store:     st,
		publisher: pub,
	}
	ctx, cancel := context.WithCancel(t.Context())
	mgr, err := newManager(ctx, pub, st, zap.NewNop())
	if err != nil {
		cancel()
		t.Fatalf("newManager: %v", err)
	}
	svc.manager = mgr
	router := gin.New()
	router.GET("/events", svc.handleEvents)
	return svc, router, pub, cancel
}

func TestSSEFrameFormat(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			"tok-A": {{TxID: "abc", CallbackToken: "tok-A"}},
		},
	}
	_, router, pub, cancel := setupSSEService(t, st)
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

func TestSSETokenFilter(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{
			"tok-A": {{TxID: "match", CallbackToken: "tok-A"}},
		},
	}
	_, router, pub, cancel := setupSSEService(t, st)
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
		_ = pub.Publish(t.Context(), &models.TransactionStatus{TxID: "other", Status: models.StatusMined, Timestamp: time.Now()})
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
	_, router, _, cancel := setupSSEService(t, st)
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

func TestSSENoPublisher(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &Service{
		cfg:    &config.Config{SSE: config.SSEConfig{Enabled: true}},
		logger: zap.NewNop(),
		store:  &sseStoreStub{},
		// publisher and manager intentionally nil
	}
	router := gin.New()
	router.GET("/events", svc.handleEvents)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/events", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSSEFanOutConcurrentUnregister(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}

	const N = 200
	clients := make([]*Client, N)
	for i := 0; i < N; i++ {
		c := mgr.NewClient("")
		mgr.Register(c)
		clients[i] = c
	}

	startGoroutines := runtime.NumGoroutine()

	status := &models.TransactionStatus{
		TxID:      "race-tx",
		Status:    models.StatusMined,
		Timestamp: time.Unix(0, 1700000000000000000).UTC(),
	}

	const iterations = 50
	for it := 0; it < iterations; it++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i += 2 {
				mgr.Unregister(clients[i].ID)
			}
		}()
		go func() {
			defer wg.Done()
			mgr.fanOut(ctx, status)
		}()
		wg.Wait()

		for i := 0; i < N; i += 2 {
			c := mgr.NewClient("")
			mgr.Register(c)
			clients[i] = c
		}
	}

	mgrCancel()
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= startGoroutines+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("goroutines start=%d end=%d (fudge tolerated)", startGoroutines, runtime.NumGoroutine())
}

func TestSSEFanOutSlowClientDrops(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}

	slow := mgr.NewClient("")
	mgr.Register(slow)
	defer mgr.Unregister(slow.ID)

	for i := 0; i < cap(slow.Ch); i++ {
		slow.Ch <- &models.TransactionStatus{TxID: "filler", Timestamp: time.Now()}
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

func TestSSEFanOutClientGoneDrops(t *testing.T) {
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	_, _, _, cancel := setupSSEService(t, st)
	defer cancel()

	ctx, mgrCancel := context.WithCancel(t.Context())
	defer mgrCancel()
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		t.Fatalf("newManager: %v", err)
	}

	gone := mgr.NewClient("")
	mgr.Register(gone)
	gone.Cancel()

	before := testutil.ToFloat64(metrics.APISSEDroppedTotal.WithLabelValues("client_gone"))

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
