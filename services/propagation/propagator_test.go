package propagation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
	"go.uber.org/zap"
)

// eventLog is a thread-safe ordered list of string events for verifying call ordering.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (e *eventLog) add(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
}

func (e *eventLog) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]string, len(e.events))
	copy(cp, e.events)
	return cp
}

func (e *eventLog) count(prefix string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, ev := range e.events {
		if strings.HasPrefix(ev, prefix) {
			n++
		}
	}
	return n
}

// mockStore implements store.Store with UpdateStatus and the durable-retry
// methods backed by in-memory maps. Everything else delegates to the embedded
// interface (panics on nil if called unexpectedly, surfacing missing stubs).
type mockStore struct {
	store.Store // embed interface — all unimplemented methods panic if called
	mu             sync.Mutex
	updates        []*models.TransactionStatus
	retryCounts    map[string]int
	pendingRetries map[string]*store.PendingRetry
	cleared        []clearedCall
}

type clearedCall struct {
	txid        string
	finalStatus models.Status
	extraInfo   string
}

func newMockStore() *mockStore {
	return &mockStore{
		retryCounts:    make(map[string]int),
		pendingRetries: make(map[string]*store.PendingRetry),
	}
}

func (m *mockStore) EnsureIndexes() error { return nil }

func (m *mockStore) UpdateStatus(_ context.Context, status *models.TransactionStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates = append(m.updates, status)
	return nil
}

func (m *mockStore) BumpRetryCount(_ context.Context, txid string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retryCounts[txid]++
	return m.retryCounts[txid], nil
}

func (m *mockStore) SetPendingRetryFields(_ context.Context, txid string, rawTx []byte, nextRetryAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingRetries[txid] = &store.PendingRetry{
		TxID:        txid,
		RawTx:       append([]byte(nil), rawTx...),
		RetryCount:  m.retryCounts[txid],
		NextRetryAt: nextRetryAt,
	}
	// Reflect PENDING_RETRY status in the updates stream so existing tests that
	// inspect status updates continue to observe the transition.
	m.updates = append(m.updates, &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusPendingRetry,
		Timestamp: time.Now(),
	})
	return nil
}

func (m *mockStore) GetReadyRetries(_ context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*store.PendingRetry, 0, len(m.pendingRetries))
	for _, pr := range m.pendingRetries {
		if !pr.NextRetryAt.After(now) {
			cp := *pr
			out = append(out, &cp)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (m *mockStore) ClearRetryState(_ context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pendingRetries, txid)
	m.cleared = append(m.cleared, clearedCall{txid: txid, finalStatus: finalStatus, extraInfo: extraInfo})
	m.updates = append(m.updates, &models.TransactionStatus{
		TxID:      txid,
		Status:    finalStatus,
		ExtraInfo: extraInfo,
		Timestamp: time.Now(),
	})
	return nil
}

// forceReady makes every pending retry eligible for the next reaper tick.
func (m *mockStore) forceReady() {
	m.mu.Lock()
	defer m.mu.Unlock()
	past := time.Now().Add(-time.Second)
	for _, pr := range m.pendingRetries {
		pr.NextRetryAt = past
	}
}

func (m *mockStore) pendingRetryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pendingRetries)
}

// mockLeaser implements store.Leaser with a scripted per-call outcome so tests
// can simulate "always leader", "never leader", "handover mid-run", and
// "infra error" without time-based flakes.
type mockLeaser struct {
	mu        sync.Mutex
	responses []leaseResponse
	calls     []leaseCall
	releases  []leaseCall
}

type leaseResponse struct {
	heldUntil time.Time
	err       error
}

type leaseCall struct {
	name   string
	holder string
	ttl    time.Duration
}

// alwaysLeader returns a mockLeaser that reports leadership for every call —
// used to keep existing reaper tests behaving as they did before leader
// election was introduced.
func alwaysLeader() *mockLeaser {
	return &mockLeaser{}
}

// scriptedLeaser returns a mockLeaser that replays the given responses in
// order. After the script is exhausted it continues returning the last entry.
func scriptedLeaser(responses ...leaseResponse) *mockLeaser {
	return &mockLeaser{responses: append([]leaseResponse(nil), responses...)}
}

func (m *mockLeaser) TryAcquireOrRenew(_ context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, leaseCall{name: name, holder: holder, ttl: ttl})
	if len(m.responses) == 0 {
		return time.Now().Add(ttl), nil
	}
	resp := m.responses[0]
	if len(m.responses) > 1 {
		m.responses = m.responses[1:]
	}
	return resp.heldUntil, resp.err
}

func (m *mockLeaser) Release(_ context.Context, name, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releases = append(m.releases, leaseCall{name: name, holder: holder})
	return nil
}

func (m *mockLeaser) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockStore) updateCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.updates)
}

func (m *mockStore) updatesForTxid(txid string) []*models.TransactionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*models.TransactionStatus
	for _, u := range m.updates {
		if u.TxID == txid {
			result = append(result, u)
		}
	}
	return result
}

func (m *mockStore) lastUpdateForTxid(txid string) *models.TransactionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.updates) - 1; i >= 0; i-- {
		if m.updates[i].TxID == txid {
			return m.updates[i]
		}
	}
	return nil
}

// helpers

func makePropMsg(txid string) []byte {
	msg := propagationMsg{
		TXID:  txid,
		RawTx: []byte{0xde, 0xad, 0xbe, 0xef},
	}
	b, _ := json.Marshal(msg)
	return b
}

func consumerMsg(payload []byte) *kafka.Message {
	return &kafka.Message{Value: payload}
}

func newMerkleServer(log *eventLog, statusCode int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID string `json:"txid"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		log.add("register:" + req.TxID)
		w.WriteHeader(statusCode)
	}))
}

func newTeranodeServer(log *eventLog, statusCode int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/txs" {
			log.add("broadcast-batch")
		} else {
			log.add("broadcast")
		}
		w.WriteHeader(statusCode)
	}))
}

func newPropagator(merkleSrvURL string, teranodeSrvURL string, st store.Store) *Propagator {
	cfg := &config.Config{
		CallbackURL: "http://localhost:8080/callback",
	}
	cfg.Propagation.MerkleConcurrency = 10

	var mc *merkleservice.Client
	if merkleSrvURL != "" {
		mc = merkleservice.NewClient(merkleSrvURL, "", 5*time.Second)
	}

	tc := teranode.NewClient([]string{teranodeSrvURL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	return New(cfg, zap.NewNop(), nil, st, nil, tc, mc)
}

// handleAndFlush is a helper that adds a message and flushes (simulating consumer behavior)
func handleAndFlush(t *testing.T, p *Propagator, payload []byte) error {
	t.Helper()
	if err := p.handleMessage(context.Background(), consumerMsg(payload)); err != nil {
		return err
	}
	return p.flushBatch(context.Background())
}

// Test 1: Registration happens before broadcast on success (single message)
func TestHandleMessage_RegistrationBeforeBroadcast(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	err := handleAndFlush(t, p, makePropMsg("abc123"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	events := log.all()
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events, got %d: %v", len(events), events)
	}
	if events[0] != "register:abc123" {
		t.Errorf("expected first event to be register, got: %s", events[0])
	}
	if events[1] != "broadcast" {
		t.Errorf("expected second event to be 'broadcast' (single /tx), got: %s", events[1])
	}

	if ms.updateCount() != 1 {
		t.Errorf("expected 1 UpdateStatus call, got %d", ms.updateCount())
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.updates[0].Status != models.StatusAcceptedByNetwork {
		t.Errorf("expected AcceptedByNetwork status, got %s", ms.updates[0].Status)
	}
}

// Test 2: Merkle failure returns error and prevents broadcast
func TestHandleMessage_MerkleFailure_NoBroadcast(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	merkleSrv := newMerkleServer(log, http.StatusInternalServerError)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	err := handleAndFlush(t, p, makePropMsg("abc123"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "merkle-service batch registration failed") {
		t.Errorf("expected error to contain 'merkle-service batch registration failed', got: %v", err)
	}

	if log.count("broadcast") != 0 {
		t.Error("teranode should not have received any requests")
	}
	if ms.updateCount() != 0 {
		t.Error("store should not have received any UpdateStatus calls")
	}
}

// Test 3: Merkle timeout returns error and prevents broadcast
func TestHandleMessage_MerkleTimeout_NoBroadcast(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	done := make(chan struct{})
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-done
	}))

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	cfg := &config.Config{
		CallbackURL: "http://localhost:8080/callback",
	}
	cfg.Propagation.MerkleConcurrency = 10
	mc := merkleservice.NewClient(merkleSrv.URL, "", 100*time.Millisecond)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, mc)

	err := handleAndFlush(t, p, makePropMsg("abc123"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if log.count("broadcast") != 0 {
		t.Error("teranode should not have received any requests")
	}

	close(done)
	merkleSrv.Close()
}

// Test 4: Batch — all 5 messages registered then broadcast in single call
func TestHandleMessage_BatchAllRegistered(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	// Accumulate 5 messages
	for i := 0; i < 5; i++ {
		txid := fmt.Sprintf("tx%d", i)
		err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid)))
		if err != nil {
			t.Fatalf("message %d: expected no error, got: %v", i, err)
		}
	}

	// Flush the batch
	if err := p.flushBatch(context.Background()); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if log.count("register:") != 5 {
		t.Errorf("expected 5 register events, got %d", log.count("register:"))
	}
	// Single batch POST to teranode /txs
	if log.count("broadcast-batch") != 1 {
		t.Errorf("expected 1 batch broadcast call, got %d", log.count("broadcast-batch"))
	}
	if ms.updateCount() != 5 {
		t.Errorf("expected 5 UpdateStatus calls, got %d", ms.updateCount())
	}
}

// Test 5: No merkle client — registration skipped, broadcast proceeds
func TestHandleMessage_NoMerkleClient_SkipsRegistration(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	// nil merkle client
	p := newPropagator("", teranodeSrv.URL, ms)

	err := handleAndFlush(t, p, makePropMsg("abc123"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if log.count("register:") != 0 {
		t.Error("merkle server should not have received any requests")
	}
	if log.count("broadcast") != 1 {
		t.Error("teranode should have received exactly 1 broadcast request")
	}
	if log.count("broadcast-batch") != 0 {
		t.Error("single tx should not use batch endpoint")
	}
}

// Test 6: No callback URL — registration skipped, broadcast proceeds
func TestHandleMessage_NoCallbackURL_SkipsRegistration(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	cfg := &config.Config{
		CallbackURL: "", // empty
	}
	cfg.Propagation.MerkleConcurrency = 10
	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, mc)

	err := handleAndFlush(t, p, makePropMsg("abc123"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if log.count("register:") != 0 {
		t.Error("merkle server should not have received any requests")
	}
	if log.count("broadcast") != 1 {
		t.Error("teranode should have received exactly 1 broadcast request")
	}
	if log.count("broadcast-batch") != 0 {
		t.Error("single tx should not use batch endpoint")
	}
}

// Test 7: Batch of 100 — all registered then broadcast in single call
func TestProcessBatch_100Transactions(t *testing.T) {
	var registerCount atomic.Int32
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registerCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	var batchBroadcastCount atomic.Int32
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		batchBroadcastCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	// Accumulate 100 messages
	for i := 0; i < 100; i++ {
		txid := fmt.Sprintf("tx%03d", i)
		err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid)))
		if err != nil {
			t.Fatalf("message %d: expected no error, got: %v", i, err)
		}
	}

	// Flush
	if err := p.flushBatch(context.Background()); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if registerCount.Load() != 100 {
		t.Errorf("expected 100 merkle registrations, got %d", registerCount.Load())
	}
	if batchBroadcastCount.Load() != 1 {
		t.Errorf("expected 1 batch broadcast call, got %d", batchBroadcastCount.Load())
	}
	if ms.updateCount() != 100 {
		t.Errorf("expected 100 UpdateStatus calls, got %d", ms.updateCount())
	}
}

// Oversized batches are chunked to teranode_max_batch_size so a 1.5k Kafka
// flush can't trigger "too many transactions" → per-tx storm on Teranode.
func TestProcessBatch_ChunksOversizedBatch(t *testing.T) {
	var batchBroadcastCount atomic.Int32
	var batchSizes []int
	var sizesMu sync.Mutex
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		batchBroadcastCount.Add(1)
		// Count transactions in the body as a cheap proxy for chunk size — we
		// don't parse the binary payload, we just record the byte length.
		// What we actually care about here is the *count* of POST calls.
		sizesMu.Lock()
		batchSizes = append(batchSizes, int(r.ContentLength))
		sizesMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.TeranodeMaxBatchSize = 10 // small cap so 25 txs → 3 chunks
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

	for i := 0; i < 25; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%03d", i))))
	}
	if err := p.flushBatch(context.Background()); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if got := batchBroadcastCount.Load(); got != 3 {
		t.Errorf("expected 25 txs / cap=10 → 3 /txs calls, got %d", got)
	}
	if ms.updateCount() != 25 {
		t.Errorf("expected 25 status updates, got %d", ms.updateCount())
	}
}

// Test 8: Merkle failure aborts entire batch — no broadcast
func TestProcessBatch_MerkleFailure_AbortsBatch(t *testing.T) {
	var broadcastCount atomic.Int32
	merkleSrv := newMerkleServer(&eventLog{}, http.StatusInternalServerError)
	defer merkleSrv.Close()

	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcastCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	for i := 0; i < 5; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i))))
	}

	err := p.flushBatch(context.Background())
	if err == nil {
		t.Fatal("expected error from merkle failure")
	}

	if broadcastCount.Load() != 0 {
		t.Errorf("expected 0 broadcast calls, got %d", broadcastCount.Load())
	}
	if ms.updateCount() != 0 {
		t.Errorf("expected 0 UpdateStatus calls, got %d", ms.updateCount())
	}
}

// Test 9: Nil merkle client skips registration for batch
func TestProcessBatch_NilMerkleClient_SkipsRegistration(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	// nil merkle client
	p := newPropagator("", teranodeSrv.URL, ms)

	for i := 0; i < 5; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i))))
	}

	if err := p.flushBatch(context.Background()); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if log.count("register:") != 0 {
		t.Error("merkle server should not have been called")
	}
	if ms.updateCount() != 5 {
		t.Errorf("expected 5 UpdateStatus calls, got %d", ms.updateCount())
	}
}

// Test 10: Single transaction uses /tx endpoint, not /txs
func TestSingleTransaction_UsesTxEndpoint(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	err := handleAndFlush(t, p, makePropMsg("single-tx"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if log.count("broadcast") != 1 {
		t.Errorf("expected 1 broadcast event, got %d", log.count("broadcast"))
	}
	if log.count("broadcast-batch") != 0 {
		t.Error("single tx should hit /tx, not /txs")
	}
}

// Test 11: Batch transactions use /txs endpoint, not /tx
func TestBatchTransactions_UsesTxsEndpoint(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	for i := 0; i < 3; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i))))
	}

	if err := p.flushBatch(context.Background()); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if log.count("broadcast-batch") != 1 {
		t.Errorf("expected 1 batch broadcast, got %d", log.count("broadcast-batch"))
	}
	// Verify no single-tx broadcasts occurred
	events := log.all()
	for _, ev := range events {
		if ev == "broadcast" {
			t.Error("batch should not hit /tx single endpoint")
		}
	}
}

// Test 12: Single transaction 200 → AcceptedByNetwork
func TestSingleTransaction_Status200_AcceptedByNetwork(t *testing.T) {
	ms := newMockStore()

	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	err := handleAndFlush(t, p, makePropMsg("tx-200"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if ms.updateCount() != 1 {
		t.Fatalf("expected 1 UpdateStatus call, got %d", ms.updateCount())
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.updates[0].Status != models.StatusAcceptedByNetwork {
		t.Errorf("expected AcceptedByNetwork, got %s", ms.updates[0].Status)
	}
}

// Test 13: Single transaction 202 → no status update (matching original behavior)
func TestSingleTransaction_Status202_NoStatusUpdate(t *testing.T) {
	ms := newMockStore()

	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	err := handleAndFlush(t, p, makePropMsg("tx-202"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if ms.updateCount() != 0 {
		t.Errorf("expected 0 UpdateStatus calls for 202 response, got %d", ms.updateCount())
	}
}

// Test 14: Batch — any endpoint success → AcceptedByNetwork for all
func TestBatchTransactions_AnySuccess_AcceptedByNetwork(t *testing.T) {
	ms := newMockStore()

	// First endpoint fails, second succeeds
	callCount := atomic.Int32{}
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	// Two endpoints pointing to the same server (simulates multi-endpoint)
	tc := teranode.NewClient([]string{teranodeSrv.URL, teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

	for i := 0; i < 3; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i))))
	}

	if err := p.flushBatch(context.Background()); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if ms.updateCount() != 3 {
		t.Fatalf("expected 3 UpdateStatus calls, got %d", ms.updateCount())
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for i, u := range ms.updates {
		if u.Status != models.StatusAcceptedByNetwork {
			t.Errorf("tx %d: expected AcceptedByNetwork, got %s", i, u.Status)
		}
	}
}

// Test 15: Batch — all endpoints fail → Rejected for all
func TestBatchTransactions_AllFail_Rejected(t *testing.T) {
	ms := newMockStore()

	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	for i := 0; i < 3; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i))))
	}

	if err := p.flushBatch(context.Background()); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if ms.updateCount() != 3 {
		t.Fatalf("expected 3 UpdateStatus calls, got %d", ms.updateCount())
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for i, u := range ms.updates {
		if u.Status != models.StatusRejected {
			t.Errorf("tx %d: expected Rejected, got %s", i, u.Status)
		}
	}
}

// --- Retry Tests ---

// newTeranodeServerWithError returns a server that fails with a specific error message
func newTeranodeServerWithError(errMsg string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(errMsg))
	}))
}

// newTeranodeServerToggle fails N times with errMsg, then succeeds
func newTeranodeServerToggle(failCount *atomic.Int32, maxFails int32, errMsg string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := failCount.Add(1)
		if n <= maxFails {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(errMsg))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
}

// Retryable-error first broadcast writes a durable PENDING_RETRY row via the
// new store API (BumpRetryCount + SetPendingRetryFields); reaper picks it up
// and, on a successful rebroadcast, clears the retry state to ACCEPTED_BY_NETWORK.
func TestRetry_MissingInputs_ThenReaperSuccess(t *testing.T) {
	ms := newMockStore()
	failCount := &atomic.Int32{}

	// Fail enough times to exhaust the inline retry (1 + inlineRetryAttempts),
	// so the tx lands in PENDING_RETRY; the reaper's rebroadcast then succeeds.
	teranodeSrv := newTeranodeServerToggle(failCount, int32(1+inlineRetryAttempts), "missing inputs for tx")
	defer teranodeSrv.Close()

	// Speed up the inline retry delays for the test.
	origDelay := inlineRetryDelay
	inlineRetryDelay = 0
	defer func() { inlineRetryDelay = origDelay }()

	p := newPropagator("", teranodeSrv.URL, ms)

	if err := handleAndFlush(t, p, makePropMsg("tx-retry")); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if ms.pendingRetryCount() != 1 {
		t.Fatalf("expected 1 durable pending retry row, got %d", ms.pendingRetryCount())
	}
	if ms.retryCounts["tx-retry"] != 1 {
		t.Fatalf("expected retry_count=1 after first failure, got %d", ms.retryCounts["tx-retry"])
	}

	// Simulate enough time having elapsed for the reaper to consider the row ready.
	ms.forceReady()
	p.reapOnce(context.Background())

	if ms.pendingRetryCount() != 0 {
		t.Fatalf("expected pending retry row cleared after reaper success, got %d", ms.pendingRetryCount())
	}
	// Last transition should be ACCEPTED_BY_NETWORK (via ClearRetryState).
	lastUpdate := ms.lastUpdateForTxid("tx-retry")
	if lastUpdate == nil || lastUpdate.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("expected ACCEPTED_BY_NETWORK after reaper, got %+v", lastUpdate)
	}
}

// Retryable error repeated until retry_count exceeds the configured max →
// ClearRetryState(REJECTED, "broadcast retries exhausted"). Covers the
// "don't loop forever" invariant that replaced the old retry-buffer-full path.
func TestRetry_Exhausted_ClearsToRejected(t *testing.T) {
	ms := newMockStore()

	teranodeSrv := newTeranodeServerWithError("missing inputs for tx")
	defer teranodeSrv.Close()

	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.RetryMaxAttempts = 2
	cfg.Propagation.RetryBackoffMs = 1
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, ms, nil, tc, nil)

	// Initial broadcast → PENDING_RETRY at retry_count=1.
	if err := handleAndFlush(t, p, makePropMsg("tx-exhaust")); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	// Reaper fires; Teranode still failing → handleRetryableFailure bumps to 2.
	ms.forceReady()
	p.reapOnce(context.Background())

	// One more reaper tick → retry_count becomes 3, exceeds max=2, REJECT.
	ms.forceReady()
	p.reapOnce(context.Background())

	if ms.pendingRetryCount() != 0 {
		t.Fatalf("expected no pending retries after exhaustion, got %d", ms.pendingRetryCount())
	}
	lastUpdate := ms.lastUpdateForTxid("tx-exhaust")
	if lastUpdate == nil || lastUpdate.Status != models.StatusRejected {
		t.Fatalf("expected REJECTED, got %+v", lastUpdate)
	}
	if !strings.Contains(lastUpdate.ExtraInfo, "broadcast retries exhausted") {
		t.Fatalf("expected 'broadcast retries exhausted' in ExtraInfo, got %q", lastUpdate.ExtraInfo)
	}
}

// Non-retryable error on the first broadcast → immediate REJECTED via the
// existing processBatch path (no PENDING_RETRY row is ever written).
func TestRetry_PermanentError_ImmediateReject(t *testing.T) {
	ms := newMockStore()

	teranodeSrv := newTeranodeServerWithError("bad-txns-vin-empty")
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	if err := handleAndFlush(t, p, makePropMsg("tx-perm")); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if ms.pendingRetryCount() != 0 {
		t.Fatalf("expected no pending retry row for permanent error, got %d", ms.pendingRetryCount())
	}
	lastUpdate := ms.lastUpdateForTxid("tx-perm")
	if lastUpdate == nil || lastUpdate.Status != models.StatusRejected {
		t.Fatalf("expected REJECTED, got %+v", lastUpdate)
	}
}

// A reaper tick with no ready rows is a no-op — it must not call Teranode.
func TestReaper_EmptyStore_NoBroadcast(t *testing.T) {
	ms := newMockStore()

	log := &eventLog{}
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	if log.count("broadcast") != 0 || log.count("broadcast-batch") != 0 {
		t.Errorf("reaper should not broadcast when no retries ready, got events: %v", log.all())
	}
}

// The reaper uses the batch /txs endpoint when more than one row is ready.
func TestReaper_BatchSuccess_ClearsAllToAccepted(t *testing.T) {
	ms := newMockStore()

	log := &eventLog{}
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	// Seed the store with two ready PENDING_RETRY rows.
	for _, txid := range []string{"tx-a", "tx-b"} {
		ms.retryCounts[txid] = 1
		if err := ms.SetPendingRetryFields(context.Background(), txid, []byte{0xaa}, time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("seed pending retry: %v", err)
		}
	}

	p.reapOnce(context.Background())

	if log.count("broadcast-batch") != 1 {
		t.Errorf("expected exactly 1 /txs call, got %d (events=%v)", log.count("broadcast-batch"), log.all())
	}
	if ms.pendingRetryCount() != 0 {
		t.Errorf("expected pending retries cleared, got %d", ms.pendingRetryCount())
	}
	for _, txid := range []string{"tx-a", "tx-b"} {
		u := ms.lastUpdateForTxid(txid)
		if u == nil || u.Status != models.StatusAcceptedByNetwork {
			t.Errorf("expected ACCEPTED_BY_NETWORK for %s, got %+v", txid, u)
		}
	}
}

// newPropagatorWithLeaser is like newPropagator but installs a given leaser
// so leader-election scenarios can be tested.
func newPropagatorWithLeaser(teranodeSrvURL string, st store.Store, leaser store.Leaser) *Propagator {
	cfg := &config.Config{CallbackURL: "http://localhost:8080/callback"}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{teranodeSrvURL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	return New(cfg, zap.NewNop(), nil, st, leaser, tc, nil)
}

// When the leaser refuses to grant leadership, the reaper must not broadcast
// or touch the store — every tick is a no-op.
func TestReaper_NotLeader_SkipsReap(t *testing.T) {
	ms := newMockStore()

	log := &eventLog{}
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	// Seed a ready PENDING_RETRY row that WOULD be picked up if we were leader.
	ms.retryCounts["tx-follower"] = 1
	if err := ms.SetPendingRetryFields(context.Background(), "tx-follower", []byte{0xaa}, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	leaser := scriptedLeaser(leaseResponse{heldUntil: time.Time{}})
	p := newPropagatorWithLeaser(teranodeSrv.URL, ms, leaser)
	p.tryReap(context.Background())

	if log.count("broadcast") != 0 || log.count("broadcast-batch") != 0 {
		t.Errorf("non-leader must not broadcast, got events: %v", log.all())
	}
	if ms.pendingRetryCount() != 1 {
		t.Errorf("non-leader must not clear retry rows, pending count=%d", ms.pendingRetryCount())
	}
	if leaser.callCount() != 1 {
		t.Errorf("expected 1 lease check, got %d", leaser.callCount())
	}
}

// Explicit test that leader-granted ticks still run the reap logic unchanged.
func TestReaper_Leader_RunsReap(t *testing.T) {
	ms := newMockStore()

	log := &eventLog{}
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	ms.retryCounts["tx-leader"] = 1
	if err := ms.SetPendingRetryFields(context.Background(), "tx-leader", []byte{0xaa}, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := newPropagatorWithLeaser(teranodeSrv.URL, ms, alwaysLeader())
	p.tryReap(context.Background())

	// Single-row broadcast goes via /tx, not /txs.
	if log.count("broadcast") != 1 {
		t.Errorf("expected 1 broadcast when leader, got events: %v", log.all())
	}
	if ms.pendingRetryCount() != 0 {
		t.Errorf("expected retry cleared after leader reap, got %d", ms.pendingRetryCount())
	}
}

// Lease infrastructure errors are logged but must not crash the reaper or
// trigger a split-brain broadcast.
func TestReaper_LeaseError_SkipsReap(t *testing.T) {
	ms := newMockStore()

	log := &eventLog{}
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	ms.retryCounts["tx-err"] = 1
	if err := ms.SetPendingRetryFields(context.Background(), "tx-err", []byte{0xaa}, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	leaser := scriptedLeaser(leaseResponse{err: errors.New("aerospike down")})
	p := newPropagatorWithLeaser(teranodeSrv.URL, ms, leaser)
	p.tryReap(context.Background())

	if log.count("broadcast") != 0 || log.count("broadcast-batch") != 0 {
		t.Errorf("lease error must not result in broadcast, got events: %v", log.all())
	}
}

// Handover: first tick is leader and does work, second tick has lost
// leadership (simulating another pod taking over) and must become a no-op.
func TestReaper_LeaseHandover(t *testing.T) {
	ms := newMockStore()

	log := &eventLog{}
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	ms.retryCounts["tx-handover"] = 1
	if err := ms.SetPendingRetryFields(context.Background(), "tx-handover", []byte{0xaa}, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	leaser := scriptedLeaser(
		leaseResponse{heldUntil: time.Now().Add(90 * time.Second)}, // tick 1: leader
		leaseResponse{heldUntil: time.Time{}},                      // tick 2: lost
	)
	p := newPropagatorWithLeaser(teranodeSrv.URL, ms, leaser)

	// Tick 1: leader → reap runs, clears the row, broadcasts once.
	p.tryReap(context.Background())
	if log.count("broadcast") != 1 {
		t.Fatalf("tick 1 (leader) expected 1 broadcast, got %v", log.all())
	}

	// Re-seed another ready row to verify tick 2 does NOT pick it up.
	ms.retryCounts["tx-handover-2"] = 1
	if err := ms.SetPendingRetryFields(context.Background(), "tx-handover-2", []byte{0xaa}, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("seed 2: %v", err)
	}

	// Tick 2: lost leadership → no more broadcasts, row stays pending.
	p.tryReap(context.Background())
	if log.count("broadcast") != 1 {
		t.Errorf("tick 2 (follower) must not broadcast, got events: %v", log.all())
	}
	if ms.pendingRetryCount() != 1 {
		t.Errorf("tick 2 must leave row pending, got %d", ms.pendingRetryCount())
	}
}
