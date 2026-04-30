package api_server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

// e2eHarness wires the SSE handler against the real KafkaPublisher backed by
// the in-memory Kafka broker. Unlike fakePublisher, this exercises the same
// publish→Kafka→subscribe→fanOut path that runs in production, so the e2e
// tests catch regressions in Subscribe-group plumbing or message ordering
// that a fake would miss.
type e2eHarness struct {
	broker    kafka.Broker
	publisher *events.KafkaPublisher
	store     *sseStoreStub
	httpSrv   *httptest.Server
}

func (h *e2eHarness) Close() {
	h.httpSrv.Close()
	_ = h.broker.Close()
}

func newE2EHarness(t *testing.T) *e2eHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)

	broker := kafka.NewMemoryBroker(64)
	publisher := events.NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop())
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{},
		statusByTx:  map[string]*models.TransactionStatus{},
	}

	srv := &Server{
		cfg:       &config.Config{},
		logger:    zap.NewNop(),
		store:     st,
		publisher: publisher,
	}

	mgr, err := newSSEManager(t.Context(), publisher, st, zap.NewNop())
	if err != nil {
		_ = broker.Close()
		t.Fatalf("newSSEManager: %v", err)
	}
	srv.sse = mgr

	router := gin.New()
	srv.registerRoutes(router)
	httpSrv := httptest.NewServer(router)

	return &e2eHarness{
		broker:    broker,
		publisher: publisher,
		store:     st,
		httpSrv:   httpSrv,
	}
}

// publishAndPersist mirrors what tx_validator does in production: it writes
// the status row to the store first, then publishes onto the events stream.
// Tests use this to simulate a real status update.
func (h *e2eHarness) publishAndPersist(t *testing.T, status *models.TransactionStatus) {
	t.Helper()
	h.store.setStatus(status.TxID, status)
	if err := h.publisher.Publish(t.Context(), status); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// waitForSubscriberReady gives the KafkaPublisher's consumer goroutine a beat
// to wire up its claim before the test publishes. Without this the first
// publish can land on the topic before the sseManager's Subscribe has fully
// joined the group, and the message would be lost to the manager.
func waitForSubscriberReady() { time.Sleep(100 * time.Millisecond) }

// TestSSE_E2E_LiveDelivery — connect first, then publish. Baseline check
// that the live fan-out path works end-to-end through real Kafka transport.
func TestSSE_E2E_LiveDelivery(t *testing.T) {
	h := newE2EHarness(t)
	defer h.Close()
	waitForSubscriberReady()

	const txid = "tx-live-1"
	const token = "tok-live"
	h.store.subsByToken[token] = []*models.Submission{{TxID: txid, CallbackToken: token}}

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Give the SSE handler time to register its client (replay runs first
	// but emits nothing because statusByTx is empty for this txid).
	time.Sleep(100 * time.Millisecond)

	h.publishAndPersist(t, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusReceived,
		Timestamp: time.Now().UTC(),
	})

	frame, err := readNextSSEFrame(resp.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if !strings.Contains(frame, `"txid":"`+txid+`"`) ||
		!strings.Contains(frame, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("unexpected frame: %q", frame)
	}
}

// TestSSE_E2E_RaceCatchup — publish first, then connect. This is the bug the
// fix targets: a client that connects after arcade has already published its
// initial event must still see the current status via the initial-state
// replay (no Last-Event-ID required).
func TestSSE_E2E_RaceCatchup(t *testing.T) {
	h := newE2EHarness(t)
	defer h.Close()
	waitForSubscriberReady()

	const txid = "tx-race-1"
	const token = "tok-race"
	h.store.subsByToken[token] = []*models.Submission{{TxID: txid, CallbackToken: token}}

	// Publish RECEIVED BEFORE the client connects. The sseManager will fan
	// it out to zero clients (none registered yet), and the live feed for
	// this connection will never see it. The fix relies on the initial
	// replay reading the persisted status at connect time.
	h.publishAndPersist(t, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusReceived,
		Timestamp: time.Now().UTC(),
	})
	// Let the publish drain through the sseManager's subscription so we
	// know we're testing the catchup path, not the live path.
	time.Sleep(100 * time.Millisecond)

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	// Crucially: NO Last-Event-ID header.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	frame, err := readNextSSEFrame(resp.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("read frame (race catchup): %v", err)
	}
	if !strings.Contains(frame, `"txid":"`+txid+`"`) ||
		!strings.Contains(frame, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("expected initial-state replay of RECEIVED, got %q", frame)
	}
}

// TestSSE_E2E_ReconnectWithLastEventID — pin the reconnect contract. After a
// disconnect, replaying with Last-Event-ID must skip statuses already seen
// and only emit later ones.
func TestSSE_E2E_ReconnectWithLastEventID(t *testing.T) {
	h := newE2EHarness(t)
	defer h.Close()
	waitForSubscriberReady()

	const txid = "tx-reconnect-1"
	const token = "tok-reconnect"
	h.store.subsByToken[token] = []*models.Submission{{TxID: txid, CallbackToken: token}}

	// Persist the initial RECEIVED status. The first connection's replay
	// will surface it.
	receivedAt := time.Now().UTC().Truncate(time.Microsecond)
	h.publishAndPersist(t, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusReceived,
		Timestamp: receivedAt,
	})

	// First connect — read the RECEIVED frame, capture its id.
	req1, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("first GET /events: %v", err)
	}
	frame1, err := readNextSSEFrame(resp1.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	_ = resp1.Body.Close()
	if !strings.Contains(frame1, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("first frame should be RECEIVED, got %q", frame1)
	}

	// Now overwrite the persisted status to SEEN_ON_NETWORK with a strictly
	// later timestamp. Reconnect with Last-Event-ID set to the first frame's
	// id (== receivedAt nanos): replay must skip RECEIVED and emit only
	// SEEN_ON_NETWORK.
	seenAt := receivedAt.Add(time.Second)
	h.store.statusByTx[txid] = &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: seenAt,
	}

	req2, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+token, nil)
	req2.Header.Set("Last-Event-ID", fmt.Sprintf("%d", receivedAt.UnixNano()))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second GET /events: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	frame2, err := readNextSSEFrame(resp2.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if !strings.Contains(frame2, `"txStatus":"`+string(models.StatusSeenOnNetwork)+`"`) {
		t.Fatalf("second frame should be SEEN_ON_NETWORK, got %q", frame2)
	}
	if strings.Contains(frame2, `"txStatus":"`+string(models.StatusReceived)+`"`) {
		t.Fatalf("RECEIVED must not replay on reconnect: %q", frame2)
	}
}
