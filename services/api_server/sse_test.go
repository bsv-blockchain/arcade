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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
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

func (p *fakePublisher) Subscribe(_ context.Context) (<-chan *models.TransactionStatus, error) {
	ch := make(chan *models.TransactionStatus, 64)
	p.mu.Lock()
	p.subscribers = append(p.subscribers, ch)
	p.mu.Unlock()
	return ch, nil
}

func (p *fakePublisher) Close() error { return nil }

// sseStoreStub extends mockStore with submission/status fixtures so we can
// drive token filtering and Last-Event-ID catchup paths.
type sseStoreStub struct {
	mockStore

	subsByToken map[string][]*models.Submission
	statusByTx  map[string]*models.TransactionStatus
}

func (s *sseStoreStub) GetSubmissionsByToken(_ context.Context, token string) ([]*models.Submission, error) {
	return s.subsByToken[token], nil
}

func (s *sseStoreStub) GetStatus(_ context.Context, txid string) (*models.TransactionStatus, error) {
	return s.statusByTx[txid], nil
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
