package api_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/services/propagation"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/pebble"
	"github.com/bsv-blockchain/arcade/teranode"
)

func init() {
	var _ store.Store = (*memStatusStore)(nil)
	var _ kafka.Broker = (*scriptedBroker)(nil)
	var _ kafka.Broker = (*forwardingBroker)(nil)
}

func fastRecoveryTiming() receivedRecoveryTiming {
	return receivedRecoveryTiming{
		workers:      4,
		maxAttempts:  40,
		initial:      2 * time.Millisecond,
		maxDelay:     5 * time.Millisecond,
		tick:         2 * time.Millisecond,
		stagger:      time.Millisecond,
		publishedTTL: time.Minute,
	}
}

func txWithSatoshis(n uint64) []byte {
	tx := sdkTx.NewTransaction()
	tx.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      n + 1,
		LockingScript: &script.Script{},
	})
	return tx.Bytes()
}

func txIDOf(t *testing.T, raw []byte) string {
	t.Helper()
	parsed, _, err := sdkTx.NewTransactionFromStream(raw)
	if err != nil {
		t.Fatalf("parse tx: %v", err)
	}
	return parsed.TxID().String()
}

type memStatusStore struct {
	mockStore
	mu   sync.Mutex
	rows map[string]*models.TransactionStatus
}

func newMemStatusStore() *memStatusStore {
	return &memStatusStore{rows: map[string]*models.TransactionStatus{}}
}

func (m *memStatusStore) GetOrInsertStatus(_ context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.rows[status.TxID]; ok {
		cp := *existing
		return &cp, false, nil
	}
	cp := *status
	cp.RawTx = append([]byte(nil), status.RawTx...)
	m.rows[status.TxID] = &cp
	return nil, true, nil
}

func (m *memStatusStore) GetStatus(_ context.Context, txid string) (*models.TransactionStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[txid]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *row
	return &cp, nil
}

func (m *memStatusStore) UpdateStatus(_ context.Context, status *models.TransactionStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.rows[status.TxID]
	if !ok {
		return store.ErrNotFound
	}
	if status.Status != "" {
		row.Status = status.Status
	}
	return nil
}

func (m *memStatusStore) statusOf(txid string) models.Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	row := m.rows[txid]
	if row == nil {
		return ""
	}
	return row.Status
}

// scriptedBroker fails produces until healthy, then records each ack.
// hold keeps a healthy Send in flight so tests can see the worker cap.
type scriptedBroker struct {
	mu          sync.Mutex
	healthy     bool
	failErr     error
	hold        time.Duration
	inFlight    int
	maxInFlight int
	successes   []recordedProduce
	onSuccess   func(topic, key string, value []byte)
}

type recordedProduce struct {
	topic string
	key   string
	value []byte
}

func (b *scriptedBroker) setHealthy(healthy bool) {
	b.mu.Lock()
	b.healthy = healthy
	b.mu.Unlock()
}

func (b *scriptedBroker) successCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.successes)
}

func (b *scriptedBroker) Send(_ context.Context, topic, key string, value []byte) error {
	b.mu.Lock()
	if !b.healthy {
		err := b.failErr
		b.mu.Unlock()
		if err == nil {
			err = errors.New("broker unavailable")
		}
		return err
	}
	b.inFlight++
	if b.inFlight > b.maxInFlight {
		b.maxInFlight = b.inFlight
	}
	hold := b.hold
	b.mu.Unlock()

	if hold > 0 {
		time.Sleep(hold)
	}

	cp := append([]byte(nil), value...)
	b.mu.Lock()
	b.inFlight--
	b.successes = append(b.successes, recordedProduce{topic: topic, key: key, value: cp})
	hook := b.onSuccess
	b.mu.Unlock()
	if hook != nil {
		hook(topic, key, cp)
	}
	return nil
}

func (b *scriptedBroker) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	return b.Send(ctx, topic, key, value)
}

func (b *scriptedBroker) SendBatch(ctx context.Context, topic string, msgs []kafka.KeyValue) error {
	for _, msg := range msgs {
		raw, err := json.Marshal(msg.Value)
		if err != nil {
			return err
		}
		if err := b.Send(ctx, topic, msg.Key, raw); err != nil {
			return err
		}
	}
	return nil
}

func (b *scriptedBroker) Subscribe(string, []string, kafka.StartOffset) (kafka.Subscription, error) {
	return nil, errors.New("scriptedBroker: subscribe not used")
}

func (b *scriptedBroker) PartitionCount(string) (int, error) { return 1, nil }

func (b *scriptedBroker) Close() error { return nil }

// forwardingBroker fails until healthy, then delegates to an inner broker
// that a real propagation consumer is subscribed to.
type forwardingBroker struct {
	inner       kafka.Broker
	mu          sync.Mutex
	healthy     bool
	failErr     error
	forwarded   atomic.Int32
	maxInFlight atomic.Int32
	inFlight    atomic.Int32
}

func (b *forwardingBroker) setHealthy(healthy bool) {
	b.mu.Lock()
	b.healthy = healthy
	b.mu.Unlock()
}

func (b *forwardingBroker) healthyNow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.healthy
}

func (b *forwardingBroker) Send(ctx context.Context, topic, key string, value []byte) error {
	if !b.healthyNow() {
		if b.failErr != nil {
			return b.failErr
		}
		return errors.New("broker unavailable")
	}
	cur := b.inFlight.Add(1)
	for {
		max := b.maxInFlight.Load()
		if cur <= max || b.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}
	defer b.inFlight.Add(-1)
	if topic == kafka.TopicPropagation {
		b.forwarded.Add(1)
	}
	return b.inner.Send(ctx, topic, key, value)
}

func (b *forwardingBroker) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	return b.Send(ctx, topic, key, value)
}

func (b *forwardingBroker) SendBatch(ctx context.Context, topic string, msgs []kafka.KeyValue) error {
	if !b.healthyNow() {
		if b.failErr != nil {
			return b.failErr
		}
		return errors.New("broker unavailable")
	}
	return b.inner.SendBatch(ctx, topic, msgs)
}

func (b *forwardingBroker) Subscribe(groupID string, topics []string, start kafka.StartOffset) (kafka.Subscription, error) {
	return b.inner.Subscribe(groupID, topics, start)
}

func (b *forwardingBroker) PartitionCount(topic string) (int, error) {
	return b.inner.PartitionCount(topic)
}

func (b *forwardingBroker) Close() error { return b.inner.Close() }

type admitSink struct {
	mu     sync.Mutex
	counts map[string]int
	bad    int
	st     *memStatusStore
}

func (a *admitSink) admit(topic, key string, value []byte) {
	var env struct {
		TxID  string `json:"txid"`
		RawTx []byte `json:"raw_tx"`
	}
	if topic != kafka.TopicPropagation || key == "" || json.Unmarshal(value, &env) != nil || env.TxID == "" || len(env.RawTx) == 0 || env.TxID != key {
		a.mu.Lock()
		a.bad++
		a.mu.Unlock()
		return
	}
	a.mu.Lock()
	if a.counts == nil {
		a.counts = map[string]int{}
	}
	a.counts[env.TxID]++
	n := a.counts[env.TxID]
	a.mu.Unlock()
	if n == 1 {
		_ = a.st.UpdateStatus(context.Background(), &models.TransactionStatus{
			TxID:   env.TxID,
			Status: models.StatusAcceptedByNetwork,
		})
	}
}

func (a *admitSink) count(txid string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.counts[txid]
}

func (a *admitSink) badCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bad
}

func newRecoveryServer(t *testing.T, broker kafka.Broker, st store.Store, timing receivedRecoveryTiming, logger *zap.Logger) (*Server, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	if logger == nil {
		logger = zap.NewNop()
	}
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         logger,
		producer:       kafka.NewProducer(broker),
		store:          st,
		txTracker:      store.NewTxTracker(),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
		recoveryTiming: timing,
	}
	t.Cleanup(func() { srv.stopReceivedRecovery() })
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

func postRawTx(router http.Handler, raw []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/tx", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func assertAcceptedReceived(t *testing.T, rec *httptest.ResponseRecorder, txid string) {
	t.Helper()
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["error"] != nil {
		t.Fatalf("error field = %v, want absent", resp["error"])
	}
	if resp["txid"] != txid {
		t.Fatalf("txid = %v, want %s", resp["txid"], txid)
	}
	if resp["txStatus"] != string(models.StatusReceived) {
		t.Fatalf("txStatus = %v, want RECEIVED", resp["txStatus"])
	}
	if resp["status"] != float64(http.StatusAccepted) {
		t.Fatalf("body status = %v, want 202", resp["status"])
	}
}

func connRefusedErr() error {
	return fmt.Errorf("dial tcp 127.0.0.1:9092: connect: %w", syscall.ECONNREFUSED)
}

func circuitOpenErr() error {
	return fmt.Errorf("sending message to arcade.propagation: %w", breaker.ErrBreakerOpen)
}

// runPromptRecovery is the shared outage fixture: one POST, durable
// RECEIVED, produce fails, no second POST required, broker recovery
// admits the envelope once and the row leaves RECEIVED.
func runPromptRecovery(t *testing.T, failErr error) {
	t.Helper()
	st := newMemStatusStore()
	sink := &admitSink{st: st}
	broker := &scriptedBroker{failErr: failErr, onSuccess: sink.admit}
	failuresBefore := testutil.ToFloat64(metrics.ReceivedPublishFailuresTotal)
	attemptsBefore := testutil.ToFloat64(metrics.ReceivedRecoveryAttemptsTotal)
	successBefore := testutil.ToFloat64(metrics.ReceivedRecoverySuccessTotal)
	exhaustedBefore := testutil.ToFloat64(metrics.ReceivedRecoveryExhaustedTotal)

	_, router := newRecoveryServer(t, broker, st, fastRecoveryTiming(), zap.NewNop())
	raw := txWithSatoshis(1)
	txid := txIDOf(t, raw)

	rec := postRawTx(router, raw)
	assertAcceptedReceived(t, rec, txid)
	if got := st.statusOf(txid); got != models.StatusReceived {
		t.Fatalf("row status = %s, want RECEIVED before broker recovery", got)
	}
	if broker.successCount() != 0 {
		t.Fatalf("acked produces = %d, want 0 while the broker is down", broker.successCount())
	}
	if got := testutil.ToFloat64(metrics.ReceivedPublishFailuresTotal) - failuresBefore; got != 1 {
		t.Fatalf("publish failures delta = %v, want 1", got)
	}

	broker.setHealthy(true)
	// onSuccess runs inside Send, before the recovery loop records the
	// success metric. Wait for both so the assertion cannot race the metric.
	waitFor(t, 2*time.Second, func() bool {
		return sink.count(txid) == 1 &&
			st.statusOf(txid) == models.StatusAcceptedByNetwork &&
			testutil.ToFloat64(metrics.ReceivedRecoverySuccessTotal)-successBefore >= 1
	})
	if broker.successCount() != 1 {
		t.Fatalf("acked produces = %d, want 1", broker.successCount())
	}
	if sink.badCount() != 0 {
		t.Fatalf("malformed propagation envelopes = %d", sink.badCount())
	}
	if got := testutil.ToFloat64(metrics.ReceivedRecoverySuccessTotal) - successBefore; got != 1 {
		t.Fatalf("recovery success delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ReceivedRecoveryAttemptsTotal) - attemptsBefore; got < 1 {
		t.Fatalf("recovery attempts delta = %v, want >= 1", got)
	}
	if got := testutil.ToFloat64(metrics.ReceivedRecoveryExhaustedTotal) - exhaustedBefore; got != 0 {
		t.Fatalf("exhausted delta = %v, want 0", got)
	}

	again := postRawTx(router, raw)
	if again.Code != http.StatusAccepted {
		t.Fatalf("resubmit status = %d, want 202", again.Code)
	}
	time.Sleep(30 * time.Millisecond)
	if broker.successCount() != 1 {
		t.Fatalf("acked produces after resubmit = %d, want 1", broker.successCount())
	}
	if sink.count(txid) != 1 {
		t.Fatalf("propagation admits = %d, want 1", sink.count(txid))
	}
	waitFor(t, time.Second, func() bool {
		return testutil.ToFloat64(metrics.ReceivedRecoveryPendingDepth) == 0
	})
}

func TestReceivedRecovery_KafkaDown_ConnectionRefused(t *testing.T) {
	if !errors.Is(connRefusedErr(), syscall.ECONNREFUSED) {
		t.Fatal("fixture error must be connection refused")
	}
	runPromptRecovery(t, connRefusedErr())
}

func TestReceivedRecovery_CircuitBreakerOpen(t *testing.T) {
	if !errors.Is(circuitOpenErr(), breaker.ErrBreakerOpen) {
		t.Fatal("fixture error must be the Sarama circuit-breaker sentinel")
	}
	runPromptRecovery(t, circuitOpenErr())
}

func TestReceivedRecovery_BrokerBecomesHealthy_OnePropagation(t *testing.T) {
	// Same intake failure as the connection-refused fixture, then the real
	// propagation pipeline: one TopicPropagation delivery, one Teranode
	// broadcast, status RECEIVED → ACCEPTED_BY_NETWORK.
	ctx, cancel := context.WithCancel(context.Background())

	dir := t.TempDir()
	st, err := pebble.New(config.Pebble{
		Path:                  dir,
		MemTableSizeMB:        4,
		L0CompactionThreshold: 2,
		SyncWrites:            false,
	})
	if err != nil {
		t.Fatalf("pebble: %v", err)
	}
	// Registered first so it runs last: the propagator must stop before the
	// store closes.
	t.Cleanup(func() { _ = st.Close() })

	inner := kafka.NewMemoryBroker(64)
	broker := &forwardingBroker{inner: inner, failErr: connRefusedErr()}
	var broadcasts atomic.Int32
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/txs" || r.URL.Path == "/tx" {
			broadcasts.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(teranodeSrv.Close)

	failuresBefore := testutil.ToFloat64(metrics.ReceivedPublishFailuresTotal)
	successBefore := testutil.ToFloat64(metrics.ReceivedRecoverySuccessTotal)

	_, router := newRecoveryServer(t, broker, st, fastRecoveryTiming(), zap.NewNop())
	cfg := &config.Config{}
	cfg.Kafka.ConsumerGroup = "received-recovery"
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	prop := propagation.New(cfg, zap.NewNop(), kafka.NewProducer(broker), nil, st, nil, tc, nil)
	go func() { _ = prop.Start(ctx) }()
	// Cancel first so the reaper and consumer leave their loops, then Stop
	// can wait for them. Stop-before-cancel blocks on the reaper interval.
	t.Cleanup(func() { _ = prop.Stop() })
	t.Cleanup(cancel)

	checker, ok := inner.(kafka.SubscriberChecker)
	if !ok {
		t.Fatal("memory broker must report subscribers")
	}
	waitFor(t, 2*time.Second, func() bool {
		return checker.HasSubscriber(kafka.TopicPropagation)
	})

	raw := txWithSatoshis(7)
	txid := txIDOf(t, raw)
	rec := postRawTx(router, raw)
	assertAcceptedReceived(t, rec, txid)
	row, err := st.GetStatus(context.Background(), txid)
	if err != nil {
		t.Fatalf("get status: %v", err)
	}
	if row.Status != models.StatusReceived {
		t.Fatalf("status = %s, want RECEIVED", row.Status)
	}
	if broker.forwarded.Load() != 0 {
		t.Fatalf("forwarded = %d, want 0 while kafka is down", broker.forwarded.Load())
	}
	if got := testutil.ToFloat64(metrics.ReceivedPublishFailuresTotal) - failuresBefore; got != 1 {
		t.Fatalf("publish failures delta = %v, want 1", got)
	}

	broker.setHealthy(true)
	waitFor(t, 5*time.Second, func() bool {
		got, gerr := st.GetStatus(context.Background(), txid)
		return gerr == nil && got != nil && got.Status == models.StatusAcceptedByNetwork && broadcasts.Load() == 1
	})
	if broker.forwarded.Load() != 1 {
		t.Fatalf("forwarded propagation produces = %d, want 1", broker.forwarded.Load())
	}
	if got := testutil.ToFloat64(metrics.ReceivedRecoverySuccessTotal) - successBefore; got != 1 {
		t.Fatalf("recovery success delta = %v, want 1", got)
	}
	time.Sleep(40 * time.Millisecond)
	if broadcasts.Load() != 1 || broker.forwarded.Load() != 1 {
		t.Fatalf("after settle broadcasts=%d forwarded=%d, want 1 and 1", broadcasts.Load(), broker.forwarded.Load())
	}
}

func TestReceivedRecovery_FiftyRowOutage(t *testing.T) {
	const n = 50
	st := newMemStatusStore()
	sink := &admitSink{st: st}
	broker := &scriptedBroker{
		failErr:   connRefusedErr(),
		hold:      8 * time.Millisecond,
		onSuccess: sink.admit,
	}
	_, router := newRecoveryServer(t, broker, st, fastRecoveryTiming(), zap.NewNop())

	raws := make([][]byte, n)
	ids := make([]string, n)
	for i := range n {
		raws[i] = txWithSatoshis(uint64(100 + i))
		ids[i] = txIDOf(t, raws[i])
		rec := postRawTx(router, raws[i])
		assertAcceptedReceived(t, rec, ids[i])
		if got := st.statusOf(ids[i]); got != models.StatusReceived {
			t.Fatalf("tx %d status = %s, want RECEIVED", i, got)
		}
	}
	if broker.successCount() != 0 {
		t.Fatalf("acked during outage = %d, want 0", broker.successCount())
	}

	broker.setHealthy(true)
	waitFor(t, 5*time.Second, func() bool {
		if broker.successCount() != n || sink.badCount() != 0 {
			return false
		}
		for _, id := range ids {
			if sink.count(id) != 1 || st.statusOf(id) != models.StatusAcceptedByNetwork {
				return false
			}
		}
		return true
	})
	broker.mu.Lock()
	maxInFlight := broker.maxInFlight
	broker.mu.Unlock()
	if maxInFlight > 4 {
		t.Fatalf("max in-flight produces = %d, want <= 4 workers", maxInFlight)
	}
	if maxInFlight < 2 {
		t.Fatalf("max in-flight produces = %d, want >= 2 (the cap must actually serialize the herd)", maxInFlight)
	}
	seen := map[string]int{}
	broker.mu.Lock()
	for _, send := range broker.successes {
		var env struct {
			TxID string `json:"txid"`
		}
		if err := json.Unmarshal(send.value, &env); err != nil {
			t.Fatalf("envelope: %v", err)
		}
		seen[env.TxID]++
	}
	broker.mu.Unlock()
	if len(seen) != n {
		t.Fatalf("distinct recovered txids = %d, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("txid %s recovered %d times, want 1", id, count)
		}
	}
	waitFor(t, time.Second, func() bool {
		return testutil.ToFloat64(metrics.ReceivedRecoveryPendingDepth) == 0
	})
}

func TestReceivedRecovery_ExhaustedRowStaysReceived(t *testing.T) {
	st := newMemStatusStore()
	broker := &scriptedBroker{failErr: connRefusedErr()}
	timing := fastRecoveryTiming()
	timing.maxAttempts = 2
	exhaustedBefore := testutil.ToFloat64(metrics.ReceivedRecoveryExhaustedTotal)
	_, router := newRecoveryServer(t, broker, st, timing, zap.NewNop())
	raw := txWithSatoshis(3)
	txid := txIDOf(t, raw)
	rec := postRawTx(router, raw)
	assertAcceptedReceived(t, rec, txid)
	waitFor(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(metrics.ReceivedRecoveryExhaustedTotal)-exhaustedBefore >= 1 &&
			testutil.ToFloat64(metrics.ReceivedRecoveryPendingDepth) == 0
	})
	if got := st.statusOf(txid); got != models.StatusReceived {
		t.Fatalf("exhausted row status = %s, want RECEIVED for the one-hour reaper", got)
	}
	if broker.successCount() != 0 {
		t.Fatalf("acked produces = %d, want 0", broker.successCount())
	}
}

func TestReceivedRecovery_BackpressureStillSheds(t *testing.T) {
	st := newMemStatusStore()
	broker := &kafka.RecordingBroker{SendErr: kafka.ErrBrokerBackpressure}
	depthBefore := testutil.ToFloat64(metrics.ReceivedRecoveryPendingDepth)
	_, router := newRecoveryServer(t, broker, st, fastRecoveryTiming(), zap.NewNop())
	raw := txWithSatoshis(4)
	rec := postRawTx(router, raw)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	if testutil.ToFloat64(metrics.ReceivedRecoveryPendingDepth) != depthBefore {
		t.Fatal("backpressure must not enter received-publish recovery")
	}
}

func TestReceivedRecovery_UndurableProduceFailureStays500(t *testing.T) {
	broker := &kafka.RecordingBroker{SendErr: connRefusedErr()}
	_, router := setupServer(broker)
	rec := postRawTx(router, txWithSatoshis(5))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when no RECEIVED row was persisted", rec.Code)
	}
}

func TestReceivedRecovery_SkipsRowThatLeftReceived(t *testing.T) {
	st := newMemStatusStore()
	broker := &scriptedBroker{healthy: true}
	srv, _ := newRecoveryServer(t, broker, st, fastRecoveryTiming(), zap.NewNop())
	const txid = "already-advanced"
	st.rows[txid] = &models.TransactionStatus{TxID: txid, Status: models.StatusAcceptedByNetwork}
	if !srv.scheduleReceivedRecovery(txid, kafka.TopicPropagation, txid, map[string]any{
		"txid":   txid,
		"raw_tx": []byte{0x01},
	}) {
		t.Fatal("schedule should accept the item so the status guard can drop it")
	}
	waitFor(t, time.Second, func() bool {
		return testutil.ToFloat64(metrics.ReceivedRecoveryPendingDepth) == 0
	})
	if broker.successCount() != 0 {
		t.Fatalf("acked produces = %d, want 0 once the row has left RECEIVED", broker.successCount())
	}
}

func TestReceivedRecovery_LogsOmitRawTransaction(t *testing.T) {
	core, recorded := observer.New(zap.DebugLevel)
	st := newMemStatusStore()
	broker := &scriptedBroker{failErr: connRefusedErr()}
	_, router := newRecoveryServer(t, broker, st, fastRecoveryTiming(), zap.New(core))
	raw := txWithSatoshis(9)
	rec := postRawTx(router, raw)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	for _, entry := range recorded.All() {
		if bytes.Contains([]byte(entry.Message), raw) {
			t.Fatalf("log message contains raw tx bytes: %s", entry.Message)
		}
		for _, field := range entry.Context {
			if field.Key == "raw_tx" || field.Key == "rawTx" {
				t.Fatalf("log field %s carries transaction bytes", field.Key)
			}
		}
	}
}
