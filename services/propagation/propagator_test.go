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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// recordingPublisher captures Publish and PublishBulk calls so tests can
// assert that a batch flush emits one bulk event per terminal status rather
// than N per-tx events.
type recordingPublisher struct {
	mu           sync.Mutex
	publishCalls []*models.TransactionStatus
	bulkCalls    []*models.TransactionStatus
}

func (p *recordingPublisher) Publish(_ context.Context, status *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishCalls = append(p.publishCalls, status)
	return nil
}

func (p *recordingPublisher) PublishBulk(_ context.Context, template *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bulkCalls = append(p.bulkCalls, template)
	return nil
}

func (p *recordingPublisher) Subscribe(_ context.Context, _ string) (<-chan *models.TransactionStatus, error) {
	// Tests in this file never exercise Subscribe; a closed empty channel
	// satisfies the contract (Subscribers see no events, ctx cancellation
	// terminates them) without forcing every test to plumb a real one.
	ch := make(chan *models.TransactionStatus)
	close(ch)
	return ch, nil
}

func (p *recordingPublisher) Close() error { return nil }

func (p *recordingPublisher) publishCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.publishCalls)
}

func (p *recordingPublisher) bulkSnapshot() []*models.TransactionStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*models.TransactionStatus, len(p.bulkCalls))
	copy(out, p.bulkCalls)
	return out
}

var _ events.Publisher = (*recordingPublisher)(nil)

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
	// replayRows drives IterateStatusesSince for merkle-replay tests.
	replayRows []*models.TransactionStatus
	// merkleMarks records every MarkMerkleRegisteredByTxIDs call as one
	// slice per call. Lets tests assert how many flushes happened and
	// which txids landed in each.
	merkleMarks [][]string
	// markErr forces MarkMerkleRegisteredByTxIDs to return this error.
	// Used by tests that verify a mark failure doesn't block broadcast.
	markErr error
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

// BatchUpdateStatusReturning mirrors UpdateStatus into m.updates for each row
// so existing tests that count `updateCount()` continue to observe the same
// invariant they did before propagator.processBatch switched to the batched
// store API. Every row returns a synthetic previous-status with a RECEIVED
// status and a recent timestamp so the propagator's transition-age metric
// observation and lattice no-op detection both behave naturally: prev.Status
// (RECEIVED) ≠ new.Status (ACCEPTED_BY_NETWORK or REJECTED), so every row is
// emitted as a transition.
func (m *mockStore) BatchUpdateStatusReturning(_ context.Context, statuses []*models.TransactionStatus) ([]*models.TransactionStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prevs := make([]*models.TransactionStatus, len(statuses))
	for i, s := range statuses {
		m.updates = append(m.updates, s)
		prevs[i] = &models.TransactionStatus{
			TxID:      s.TxID,
			Status:    models.StatusReceived,
			Timestamp: time.Now(),
		}
	}
	return prevs, nil
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

func (m *mockStore) MarkMerkleRegisteredByTxIDs(_ context.Context, txids []string, ts time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markErr != nil {
		return m.markErr
	}
	cp := append([]string(nil), txids...)
	m.merkleMarks = append(m.merkleMarks, cp)
	// Also stamp the replayRows so successive IterateStatusesSince calls
	// observe the marker — lets replay tests verify the round-trip.
	marked := make(map[string]struct{}, len(txids))
	for _, t := range txids {
		marked[t] = struct{}{}
	}
	for _, r := range m.replayRows {
		if _, ok := marked[r.TxID]; ok {
			r.MerkleRegisteredAt = ts
		}
	}
	return nil
}

func (m *mockStore) markCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.merkleMarks)
}

func (m *mockStore) lastMark() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.merkleMarks) == 0 {
		return nil
	}
	return m.merkleMarks[len(m.merkleMarks)-1]
}

func (m *mockStore) allMarks() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, batch := range m.merkleMarks {
		out = append(out, batch...)
	}
	return out
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

func (m *mockStore) IterateStatusesSince(_ context.Context, since time.Time, fn func(*models.TransactionStatus) error) error {
	m.mu.Lock()
	rows := append([]*models.TransactionStatus(nil), m.replayRows...)
	m.mu.Unlock()
	for _, r := range rows {
		// Honor the lookback filter so replay tests can pin behavior that
		// depends on it. Rows with a zero Timestamp are always returned —
		// matches existing tests that don't bother setting one.
		if !r.Timestamp.IsZero() && r.Timestamp.Before(since) {
			continue
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
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
	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
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

func newPropagator(merkleSrvURL, teranodeSrvURL string, st store.Store) *Propagator {
	cfg := &config.Config{
		CallbackURL: "http://localhost:8080/callback",
	}
	cfg.Propagation.MerkleConcurrency = 10

	var mc *merkleservice.Client
	if merkleSrvURL != "" {
		mc = merkleservice.NewClient(merkleSrvURL, "", 5*time.Second)
	}

	tc := teranode.NewClient([]string{teranodeSrvURL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	return New(cfg, zap.NewNop(), nil, nil, st, nil, tc, mc)
}

// handleAndFlush is a helper that adds a message and flushes (simulating consumer behavior)
func handleAndFlush(t *testing.T, p *Propagator, payload []byte) error {
	t.Helper()
	if err := p.handleMessage(context.Background(), consumerMsg(payload)); err != nil {
		return err
	}
	if err := flushSync(t, p); err != nil {
		return err
	}
	p.WaitForBatches()
	return nil
}

// flushSync drains pendingMsgs and synchronously waits for the resulting
// processBatch goroutine to finish. Mirrors the pre-pipelining semantics
// that existing tests rely on (assert state right after flushBatch returns).
func flushSync(t *testing.T, p *Propagator) error {
	t.Helper()
	if err := p.flushBatch(context.Background()); err != nil {
		return err
	}
	p.WaitForBatches()
	return nil
}

// TestHandleMessage_ForwardsCallbackToken pins the propagator → merkle-service
// half of the F-018 callback-auth loop: the token configured at the arcade
// side (cfg.CallbackToken) must reach merkle-service via the /watch payload,
// so merkle-service can attach it as Authorization on outbound delivery. If
// this test fails, callbacks will 401 even if the inbound receiver and
// merkle-service forwarder are both correct.
func TestHandleMessage_ForwardsCallbackToken(t *testing.T) {
	var gotToken string
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID          string `json:"txid"`
			CallbackToken string `json:"callbackToken"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		gotToken = req.CallbackToken
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	cfg := &config.Config{
		CallbackURL:   "http://localhost:8080/callback",
		CallbackToken: "secret-arcade-token",
	}
	cfg.Propagation.MerkleConcurrency = 10
	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, newMockStore(), nil, tc, mc)

	if err := handleAndFlush(t, p, makePropMsg("abc123")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotToken != "secret-arcade-token" {
		t.Errorf("expected merkle-service to receive callbackToken=secret-arcade-token, got %q", gotToken)
	}
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

// Test 2: Merkle failure routes the tx to durable PENDING_RETRY and prevents
// broadcast. The reaper picks the row up on its next tick (which will re-call
// registerBatch and re-broadcast); registration failures no longer abort the
// Kafka consumer's claim — that path is reserved for catastrophic decoding
// failures, not transient merkle-service unavailability.
func TestHandleMessage_MerkleFailure_NoBroadcast(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	merkleSrv := newMerkleServer(log, http.StatusInternalServerError)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	// handleMessage + flushBatch must both succeed: the failed registration is
	// handled internally by routing the tx to handleRetryableFailure.
	if err := handleAndFlush(t, p, makePropMsg("abc123")); err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	if log.count("broadcast") != 0 {
		t.Error("teranode should not have received any requests when register fails")
	}
	// Dep-aware design: a merkle-service registration failure is
	// terminal REJECTED, not PENDING_RETRY.
	if ms.pendingRetryCount() != 0 {
		t.Errorf("expected 0 PENDING_RETRY rows in the dep-aware design, got %d", ms.pendingRetryCount())
	}
	if u := ms.lastUpdateForTxid("abc123"); u == nil || u.Status != models.StatusRejected {
		t.Errorf("expected REJECTED status update, got %+v", u)
	}
}

// Test 3: Merkle timeout sends the tx to durable PENDING_RETRY instead of
// aborting the consumer claim. The reaper retries after backoff.
func TestHandleMessage_MerkleTimeout_NoBroadcast(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()

	done := make(chan struct{})
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
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
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, mc)

	if err := handleAndFlush(t, p, makePropMsg("abc123")); err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	if log.count("broadcast") != 0 {
		t.Error("teranode should not have received any requests when register times out")
	}
	u := ms.lastUpdateForTxid("abc123")
	if u == nil || u.Status != models.StatusRejected {
		t.Errorf("expected REJECTED status update after register timeout, got %+v", u)
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
	if err := flushSync(t, p); err != nil {
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
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, mc)

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

// TestRunMerkleReplay_RegistersOnlyNonTerminal verifies the startup replay
// path: every in-flight tx in the store gets POSTed to merkle-service
// /watch, but rows already MINED/IMMUTABLE/REJECTED/DOUBLE_SPEND are
// skipped because re-registering terminal txs is wasted work.
func TestRunMerkleReplay_RegistersOnlyNonTerminal(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-recvd", Status: models.StatusReceived},
		{TxID: "tx-seen", Status: models.StatusSeenOnNetwork},
		{TxID: "tx-multi", Status: models.StatusSeenMultipleNodes},
		{TxID: "tx-retry", Status: models.StatusPendingRetry},
		{TxID: "tx-mined", Status: models.StatusMined},                // terminal, skip
		{TxID: "tx-immut", Status: models.StatusImmutable},            // terminal, skip
		{TxID: "tx-rejct", Status: models.StatusRejected},             // terminal, skip
		{TxID: "tx-dspnd", Status: models.StatusDoubleSpendAttempted}, // terminal, skip
		{TxID: "", Status: models.StatusReceived},                     // empty txid, skip
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	cfg := &config.Config{CallbackURL: "http://arcade/cb", CallbackToken: "tok"}
	cfg.Propagation.MerkleConcurrency = 4
	cfg.Propagation.RegisterReplayLookbackHours = 24
	enabled := true
	cfg.Propagation.RegisterReplayOnStart = &enabled

	mc := merkleservice.NewClient(merkleSrv.URL, "auth", 5*time.Second)
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, mc)

	p.runMerkleReplay(context.Background())

	got := log.count("register:")
	if got != 4 {
		t.Errorf("registered=%d want 4 (the four non-terminal rows)", got)
	}
	// Spot-check that terminal txids aren't in the event log.
	for _, ev := range log.all() {
		for _, skip := range []string{"tx-mined", "tx-immut", "tx-rejct", "tx-dspnd"} {
			if strings.Contains(ev, skip) {
				t.Errorf("event %q should have been filtered (terminal status)", ev)
			}
		}
	}
}

// TestRunMerkleReplay_DisabledByConfig confirms that operators can opt out
// of replay (e.g. a deployment that uses an alternative resync path) and
// the replay function exits without calling merkle-service.
func TestRunMerkleReplay_DisabledByConfig(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-recvd", Status: models.StatusReceived},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	cfg := &config.Config{CallbackURL: "http://arcade/cb", CallbackToken: "tok"}
	disabled := false
	cfg.Propagation.RegisterReplayOnStart = &disabled

	mc := merkleservice.NewClient(merkleSrv.URL, "auth", 5*time.Second)
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, mc)

	p.runMerkleReplay(context.Background())

	if log.count("register:") != 0 {
		t.Errorf("registered=%d want 0 (replay disabled)", log.count("register:"))
	}
}

// replayPropagator builds a Propagator wired to the supplied merkle server,
// with all the knobs replay tests care about pre-populated. Keeps the
// per-test setup boilerplate small.
func replayPropagator(t *testing.T, ms *mockStore, merkleURL string, configure func(*config.Config)) *Propagator {
	t.Helper()
	cfg := &config.Config{CallbackURL: "http://arcade/cb", CallbackToken: "tok"}
	cfg.Propagation.MerkleConcurrency = 4
	cfg.Propagation.RegisterReplayLookbackHours = 24
	enabled := true
	cfg.Propagation.RegisterReplayOnStart = &enabled
	if configure != nil {
		configure(cfg)
	}
	mc := merkleservice.NewClient(merkleURL, "auth", 5*time.Second)
	return New(cfg, zap.NewNop(), nil, nil, ms, nil, nil, mc)
}

// TestRunMerkleReplay_SkipsRecentlyRegistered pins the issue #145 fix:
// rows whose MerkleRegisteredAt is within MerkleReplaySkipRecentMinutes
// don't need re-registration (merkle-service still has them, and POST
// /watch wouldn't refresh expires_at anyway).
func TestRunMerkleReplay_SkipsRecentlyRegistered(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	now := time.Now()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-stale-1", Status: models.StatusReceived, MerkleRegisteredAt: now.Add(-2 * time.Hour)},
		{TxID: "tx-recent-1", Status: models.StatusReceived, MerkleRegisteredAt: now.Add(-5 * time.Minute)},
		{TxID: "tx-stale-2", Status: models.StatusSeenOnNetwork, MerkleRegisteredAt: now.Add(-2 * time.Hour)},
		{TxID: "tx-recent-2", Status: models.StatusSeenOnNetwork, MerkleRegisteredAt: now.Add(-5 * time.Minute)},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	p := replayPropagator(t, ms, merkleSrv.URL, func(cfg *config.Config) {
		cfg.Propagation.MerkleReplaySkipRecentMinutes = 30
	})
	p.runMerkleReplay(context.Background())

	if got := log.count("register:"); got != 2 {
		t.Errorf("registered=%d want 2 (only stale rows)", got)
	}
	for _, skip := range []string{"tx-recent-1", "tx-recent-2"} {
		for _, ev := range log.all() {
			if strings.Contains(ev, skip) {
				t.Errorf("event %q should have been skipped (recently registered)", ev)
			}
		}
	}
}

// TestRunMerkleReplay_SkipDisabled verifies that
// MerkleReplaySkipRecentMinutes=0 forces a full re-register regardless
// of recency — the escape hatch operators need after a known
// merkle-service wipe.
func TestRunMerkleReplay_SkipDisabled(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	now := time.Now()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-1", Status: models.StatusReceived, MerkleRegisteredAt: now.Add(-1 * time.Minute)},
		{TxID: "tx-2", Status: models.StatusReceived, MerkleRegisteredAt: now.Add(-30 * time.Second)},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	p := replayPropagator(t, ms, merkleSrv.URL, func(cfg *config.Config) {
		cfg.Propagation.MerkleReplaySkipRecentMinutes = 0
	})
	p.runMerkleReplay(context.Background())

	if got := log.count("register:"); got != 2 {
		t.Errorf("registered=%d want 2 (skip disabled — every row re-registers)", got)
	}
}

// TestRunMerkleReplay_LookbackDefault24h pins the lookback default
// change. Rows older than 24h are filtered out by IterateStatusesSince;
// only the recent rows make it into the replay scan.
func TestRunMerkleReplay_LookbackDefault24h(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	now := time.Now()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-recent", Status: models.StatusReceived, Timestamp: now.Add(-12 * time.Hour)},
		{TxID: "tx-old", Status: models.StatusReceived, Timestamp: now.Add(-5 * 24 * time.Hour)},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	p := replayPropagator(t, ms, merkleSrv.URL, func(cfg *config.Config) {
		cfg.Propagation.RegisterReplayLookbackHours = 0 // fall back to defaultReplayLookback (24h)
		cfg.Propagation.MerkleReplaySkipRecentMinutes = 0
	})
	p.runMerkleReplay(context.Background())

	if got := log.count("register:"); got != 1 {
		t.Errorf("registered=%d want 1 (only the 12h-old row is in lookback)", got)
	}
	for _, ev := range log.all() {
		if strings.Contains(ev, "tx-old") {
			t.Errorf("event %q: tx-old is 5 days old, must be excluded by 24h default lookback", ev)
		}
	}
}

// TestRunMerkleReplay_RateLimit verifies the throttle: with RPS=10 and
// batch size 1000, a 30-row replay falls into one batch and pays
// ~3s of inter-batch sleep before flushing. (The first flush is also
// throttled in the current implementation since we sleep before each
// non-empty flush.) Wall-time floor with generous CI-slack.
func TestRunMerkleReplay_RateLimit(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	rows := make([]*models.TransactionStatus, 30)
	for i := range rows {
		rows[i] = &models.TransactionStatus{TxID: fmt.Sprintf("tx-%d", i), Status: models.StatusReceived}
	}
	ms.replayRows = rows

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	p := replayPropagator(t, ms, merkleSrv.URL, func(cfg *config.Config) {
		cfg.Propagation.MerkleReplayRPS = 10
		cfg.Propagation.MerkleReplaySkipRecentMinutes = 0
	})

	start := time.Now()
	p.runMerkleReplay(context.Background())
	elapsed := time.Since(start)

	if got := log.count("register:"); got != 30 {
		t.Errorf("registered=%d want 30", got)
	}
	// 30 rows / 10 rps = 3s nominal. Accept ≥ 2.5s to absorb scheduling jitter.
	if elapsed < 2500*time.Millisecond {
		t.Errorf("elapsed=%v want ≥ 2.5s with RPS=10 over 30 rows", elapsed)
	}
}

// TestRunMerkleReplay_MarksSuccessfulFlush pins the round-trip:
// replay's successful flush() must stamp merkle_registered_at on the
// rows it sent, so the NEXT replay skips them.
func TestRunMerkleReplay_MarksSuccessfulFlush(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "tx-1", Status: models.StatusReceived},
		{TxID: "tx-2", Status: models.StatusReceived},
		{TxID: "tx-3", Status: models.StatusReceived},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()

	p := replayPropagator(t, ms, merkleSrv.URL, func(cfg *config.Config) {
		cfg.Propagation.MerkleReplaySkipRecentMinutes = 0 // not relevant here, just keep behavior explicit
		cfg.Propagation.MerkleReplayRPS = 0               // no throttle so the test stays fast
	})
	p.runMerkleReplay(context.Background())

	if ms.markCount() != 1 {
		t.Errorf("expected 1 mark batch (one successful flush), got %d", ms.markCount())
	}
	got := map[string]bool{}
	for _, txid := range ms.lastMark() {
		got[txid] = true
	}
	for _, want := range []string{"tx-1", "tx-2", "tx-3"} {
		if !got[want] {
			t.Errorf("expected %s in last mark, got %v", want, ms.lastMark())
		}
	}
}

// Test 7: Batch of 100 — all registered then broadcast in single call
func TestProcessBatch_100Transactions(t *testing.T) {
	var registerCount atomic.Int32
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		registerCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	var batchBroadcastCount atomic.Int32
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	if err := flushSync(t, p); err != nil {
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

// TestProcessBatch_BulkPublish_OneEventPerStatus pins the optimization that
// drops the propagator's per-tx Publish count from N to ≤2 per flush. For a
// 50-tx batch that all teranode accepts, exactly one PublishBulk event
// (Status=ACCEPTED_BY_NETWORK, TxIDs=[50]) should be emitted and zero per-tx
// Publish calls. Mirrors the SEEN_ON_NETWORK callback-handler regression
// (TestHandleSeenOnNetwork_BulkPath_OnePublishPerCallback) on the
// propagation side.
func TestProcessBatch_BulkPublish_OneEventPerStatus(t *testing.T) {
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	ms := newMockStore()
	cfg := &config.Config{CallbackURL: "http://localhost:8080/callback"}
	cfg.Propagation.MerkleConcurrency = 10

	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	pub := &recordingPublisher{}
	p := New(cfg, zap.NewNop(), nil, pub, ms, nil, tc, mc)

	const batchSize = 50
	for i := 0; i < batchSize; i++ {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%03d", i)))); err != nil {
			t.Fatalf("handleMessage %d: %v", i, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	if pub.publishCount() != 0 {
		t.Errorf("expected 0 per-tx Publish calls, got %d", pub.publishCount())
	}
	bulks := pub.bulkSnapshot()
	if len(bulks) != 1 {
		t.Fatalf("expected exactly 1 PublishBulk call, got %d", len(bulks))
	}
	if bulks[0].Status != models.StatusAcceptedByNetwork {
		t.Errorf("expected bulk Status=ACCEPTED_BY_NETWORK, got %q", bulks[0].Status)
	}
	if got := len(bulks[0].TxIDs); got != batchSize {
		t.Errorf("expected bulk to carry %d txids, got %d", batchSize, got)
	}
}

// TestBroadcastInChunks_ParallelismHonorsConfig pins the optimization where
// p.maxParallelChunks (from cfg.Propagation.MaxParallelChunks, default 4)
// lets a single batch's chunks broadcast concurrently rather than serially.
// With teranode_max_batch_size=25 a 100-tx batch produces 4 chunks; this
// test asserts they actually run in parallel — the dominant component of
// RECEIVED→ACCEPTED_BY_NETWORK latency at 100 TPS is the per-chunk wall
// time, so serializing them would erase the gain from smaller chunks.
func TestBroadcastInChunks_ParallelismHonorsConfig(t *testing.T) {
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	// teranode mock: each /txs call increments an inflight gauge, sleeps
	// long enough that all parallel chunks overlap, then decrements.
	// maxInflight captures the peak observed concurrency.
	const sleep = 200 * time.Millisecond
	var inflight atomic.Int32
	var maxInflight atomic.Int32
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := inflight.Add(1)
		for {
			peak := maxInflight.Load()
			if cur <= peak || maxInflight.CompareAndSwap(peak, cur) {
				break
			}
		}
		time.Sleep(sleep)
		inflight.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	ms := newMockStore()
	cfg := &config.Config{CallbackURL: "http://localhost:8080/callback"}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.TeranodeMaxBatchSize = 25
	cfg.Propagation.MaxParallelChunks = 4
	cfg.Propagation.BroadcastWorkers = 256

	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	pub := &recordingPublisher{}
	p := New(cfg, zap.NewNop(), nil, pub, ms, nil, tc, mc)
	if p.maxParallelChunks != 4 {
		t.Fatalf("propagator picked up maxParallelChunks=%d, want 4", p.maxParallelChunks)
	}
	if p.broadcastWorkers != 256 {
		t.Fatalf("propagator picked up broadcastWorkers=%d, want 256", p.broadcastWorkers)
	}

	const batchSize = 100 // 4 chunks of 25 each
	for i := 0; i < batchSize; i++ {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%03d", i)))); err != nil {
			t.Fatalf("handleMessage %d: %v", i, err)
		}
	}
	start := time.Now()
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}
	elapsed := time.Since(start)

	if got, want := int(maxInflight.Load()), 4; got != want {
		t.Errorf("max concurrent chunks at teranode = %d, want %d (chunks serialized — parallelism gain lost)", got, want)
	}
	// Serial would be 4×200ms = 800ms; parallel should be ≈ 200ms. Pick a
	// loose upper bound to avoid flakes on slow CI: anything < 500ms proves
	// at least 2 chunks overlapped.
	if elapsed >= 500*time.Millisecond {
		t.Errorf("broadcast took %v with maxParallelChunks=4; expected <500ms (chunks not running in parallel)", elapsed)
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
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	for i := 0; i < 25; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%03d", i))))
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if got := batchBroadcastCount.Load(); got != 3 {
		t.Errorf("expected 25 txs / cap=10 → 3 /txs calls, got %d", got)
	}
	if ms.updateCount() != 25 {
		t.Errorf("expected 25 status updates, got %d", ms.updateCount())
	}
}

// Test 8: When every tx in a batch fails registration, none is broadcast and
// each one is routed to durable PENDING_RETRY. F-024 invariant: a broadcast is
// only attempted on the registered subset; failed-register txs return to the
// reaper-owned retry path.
func TestProcessBatch_MerkleFailure_AbortsBatch(t *testing.T) {
	var broadcastCount atomic.Int32
	merkleSrv := newMerkleServer(&eventLog{}, http.StatusInternalServerError)
	defer merkleSrv.Close()

	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		broadcastCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	for i := 0; i < 5; i++ {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i)))); err != nil {
			t.Fatalf("tx%d: expected handleMessage to succeed (failure deferred to flush), got: %v", i, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch returned: %v", err)
	}

	if broadcastCount.Load() != 0 {
		t.Errorf("expected 0 broadcast calls, got %d", broadcastCount.Load())
	}
	if ms.pendingRetryCount() != 0 {
		t.Errorf("expected 0 PENDING_RETRY rows in the dep-aware design, got %d", ms.pendingRetryCount())
	}
	// All 5 txs should have been terminally rejected.
	rejected := 0
	for _, u := range ms.updates {
		if u.Status == models.StatusRejected {
			rejected++
		}
	}
	if rejected != 5 {
		t.Errorf("expected 5 REJECTED status writes, got %d", rejected)
	}
}

// F-024 regression: when registration fails for one message inside a batch,
// the already-registered messages are broadcast and the failed one is routed
// to durable PENDING_RETRY. The reaper handles re-registration + rebroadcast.
// No tx is ever broadcast without a successful register first.
func TestHandleMessage_PartialMerkleFailure_OnlyFailedMessageIsAborted(t *testing.T) {
	// Merkle server returns 500 for txid "tx-bad", 200 for everything else.
	var registerLog eventLog
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID string `json:"txid"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		registerLog.add("register:" + req.TxID)
		if req.TxID == "tx-bad" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	var broadcastBodies []string
	var bodyMu sync.Mutex
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyMu.Lock()
		defer bodyMu.Unlock()
		broadcastBodies = append(broadcastBodies, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	// Three messages: two succeed, one (tx-bad) fails registration. All
	// queue successfully — failure is deferred to flush time.
	goodA := makePropMsg("tx-good-a")
	bad := makePropMsg("tx-bad")
	goodB := makePropMsg("tx-good-b")

	if err := p.handleMessage(context.Background(), consumerMsg(goodA)); err != nil {
		t.Fatalf("tx-good-a: expected nil, got %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(bad)); err != nil {
		t.Fatalf("tx-bad: handleMessage must succeed (failure deferred), got %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(goodB)); err != nil {
		t.Fatalf("tx-good-b: expected nil, got %v", err)
	}

	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch returned: %v", err)
	}

	// All three were attempted at the merkle layer; the failed one was the
	// "bad" txid only.
	if got := registerLog.count("register:"); got != 3 {
		t.Errorf("expected 3 merkle register attempts, got %d", got)
	}

	// Only the two surviving txids made it into the broadcast batch — that
	// is, exactly two good ones broadcast. /txs is used because batch>1.
	bodyMu.Lock()
	defer bodyMu.Unlock()
	if len(broadcastBodies) != 1 {
		t.Errorf("expected 1 broadcast call (the /txs batch of the 2 good txs), got %d: %v", len(broadcastBodies), broadcastBodies)
	}
	if len(broadcastBodies) > 0 && broadcastBodies[0] != "/txs" {
		t.Errorf("expected /txs batch endpoint, got %s", broadcastBodies[0])
	}

	if ms.pendingRetryCount() != 0 {
		t.Errorf("expected 0 PENDING_RETRY rows in the dep-aware design, got %d", ms.pendingRetryCount())
	}
	if u := ms.lastUpdateForTxid("tx-bad"); u == nil || u.Status != models.StatusRejected {
		t.Errorf("tx-bad: expected REJECTED status update, got %+v", u)
	}
	if u := ms.lastUpdateForTxid("tx-good-a"); u == nil || u.Status != models.StatusAcceptedByNetwork {
		t.Errorf("tx-good-a: expected ACCEPTED_BY_NETWORK status update, got %+v", u)
	}
	if u := ms.lastUpdateForTxid("tx-good-b"); u == nil || u.Status != models.StatusAcceptedByNetwork {
		t.Errorf("tx-good-b: expected ACCEPTED_BY_NETWORK status update, got %+v", u)
	}
}

// batchOutcomeSnapshot captures the three label counters atomically. Counters
// are process-global so we assert deltas rather than absolute values — other
// tests in this package legitimately increment them too.
func batchOutcomeSnapshot() (fullyOK, partial, allFailed float64) {
	return testutil.ToFloat64(metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("fully_ok")),
		testutil.ToFloat64(metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("partial")),
		testutil.ToFloat64(metrics.PropagationMerkleRegisterBatchOutcomeTotal.WithLabelValues("all_failed"))
}

// TestRegisterBatch_Metric_FullyOK verifies the fully_ok label increments
// exactly once per flushBatch when every tx registers cleanly.
func TestRegisterBatch_Metric_FullyOK(t *testing.T) {
	merkleSrv := newMerkleServer(&eventLog{}, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, newMockStore())

	okBefore, partialBefore, failBefore := batchOutcomeSnapshot()

	for i := 0; i < 3; i++ {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i)))); err != nil {
			t.Fatalf("handleMessage: %v", err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	okAfter, partialAfter, failAfter := batchOutcomeSnapshot()
	if delta := okAfter - okBefore; delta != 1 {
		t.Errorf("fully_ok delta=%v want 1", delta)
	}
	if delta := partialAfter - partialBefore; delta != 0 {
		t.Errorf("partial delta=%v want 0", delta)
	}
	if delta := failAfter - failBefore; delta != 0 {
		t.Errorf("all_failed delta=%v want 0", delta)
	}
}

// TestRegisterBatch_Metric_Partial verifies the partial label increments
// when some txs register and some fail — the canonical "dashboard should
// see this" signal that per-tx failure counters alone obscure.
func TestRegisterBatch_Metric_Partial(t *testing.T) {
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID string `json:"txid"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if strings.HasPrefix(req.TxID, "bad") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, newMockStore())

	okBefore, partialBefore, failBefore := batchOutcomeSnapshot()

	for _, txid := range []string{"good-a", "bad-1", "good-b"} {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
			t.Fatalf("handleMessage %s: %v", txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	okAfter, partialAfter, failAfter := batchOutcomeSnapshot()
	if delta := partialAfter - partialBefore; delta != 1 {
		t.Errorf("partial delta=%v want 1", delta)
	}
	if delta := okAfter - okBefore; delta != 0 {
		t.Errorf("fully_ok delta=%v want 0", delta)
	}
	if delta := failAfter - failBefore; delta != 0 {
		t.Errorf("all_failed delta=%v want 0", delta)
	}
}

// TestRegisterBatch_Metric_AllFailed verifies the all_failed label increments
// when every tx in the batch fails registration — the strongest signal of a
// merkle-service outage.
func TestRegisterBatch_Metric_AllFailed(t *testing.T) {
	merkleSrv := newMerkleServer(&eventLog{}, http.StatusInternalServerError)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, newMockStore())

	okBefore, partialBefore, failBefore := batchOutcomeSnapshot()

	for i := 0; i < 2; i++ {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i)))); err != nil {
			t.Fatalf("handleMessage: %v", err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	okAfter, partialAfter, failAfter := batchOutcomeSnapshot()
	if delta := failAfter - failBefore; delta != 1 {
		t.Errorf("all_failed delta=%v want 1", delta)
	}
	if delta := okAfter - okBefore; delta != 0 {
		t.Errorf("fully_ok delta=%v want 0", delta)
	}
	if delta := partialAfter - partialBefore; delta != 0 {
		t.Errorf("partial delta=%v want 0", delta)
	}
}

// TestRegisterBatch_MarksSuccessesOnly is the issue #145 hook: every txid
// that successfully /watch-registers must get a merkle_registered_at stamp
// on its row so the next startup replay can skip it. Failed txids must NOT
// be marked — their row is PENDING_RETRY and the reaper will re-register.
func TestRegisterBatch_MarksSuccessesOnly(t *testing.T) {
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID string `json:"txid"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.TxID == "tx-bad" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	for _, txid := range []string{"tx-good-a", "tx-bad", "tx-good-b"} {
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg(txid))); err != nil {
			t.Fatalf("handleMessage %s: %v", txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	marks := ms.allMarks()
	if len(marks) != 2 {
		t.Fatalf("expected 2 marks (only good txs), got %d: %v", len(marks), marks)
	}
	got := map[string]bool{}
	for _, m := range marks {
		got[m] = true
	}
	if !got["tx-good-a"] || !got["tx-good-b"] {
		t.Errorf("expected tx-good-a and tx-good-b marked, got %v", got)
	}
	if got["tx-bad"] {
		t.Errorf("tx-bad must NOT be marked — it failed registration")
	}
}

// TestRegisterBatch_MarkStoreFailure_DoesNotBlockBroadcast pins the
// "best-effort" contract on the mark hook: the marker is a replay-skip
// hint, not part of F-024. If MarkMerkleRegisteredByTxIDs returns an
// error, broadcast must still happen — worst case the next replay
// re-registers one extra time.
func TestRegisterBatch_MarkStoreFailure_DoesNotBlockBroadcast(t *testing.T) {
	merkleSrv := newMerkleServer(&eventLog{}, http.StatusOK)
	defer merkleSrv.Close()

	broadcastLog := &eventLog{}
	teranodeSrv := newTeranodeServer(broadcastLog, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	ms.markErr = errors.New("store down")
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	if err := handleAndFlush(t, p, makePropMsg("tx-1")); err != nil {
		t.Fatalf("handleAndFlush: %v", err)
	}

	if broadcastLog.count("broadcast") != 1 {
		t.Errorf("broadcast should still fire on mark failure, got %d", broadcastLog.count("broadcast"))
	}
	if u := ms.lastUpdateForTxid("tx-1"); u == nil || u.Status != models.StatusAcceptedByNetwork {
		t.Errorf("tx-1: expected ACCEPTED_BY_NETWORK, got %+v", u)
	}
}

// In the dep-aware design there's no PENDING_RETRY queue and no
// reaper: a merkle-service registration failure is terminal REJECTED.
// The wallet sees a REJECTED row with the merkle-service error in
// ExtraInfo and can resubmit if it chooses.
func TestHandleMessage_MerkleFailure_WritesTerminalRejected(t *testing.T) {
	merkleSrv := newMerkleServer(&eventLog{}, http.StatusInternalServerError)
	defer merkleSrv.Close()

	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	if err := handleAndFlush(t, p, makePropMsg("tx-reg-fail")); err != nil {
		t.Fatalf("handleAndFlush: %v", err)
	}

	if ms.pendingRetryCount() != 0 {
		t.Errorf("expected 0 PENDING_RETRY rows in the dep-aware design, got %d", ms.pendingRetryCount())
	}
	u := ms.lastUpdateForTxid("tx-reg-fail")
	if u == nil || u.Status != models.StatusRejected {
		t.Errorf("expected REJECTED status update, got %+v", u)
	}
	if u != nil && !strings.Contains(u.ExtraInfo, "merkle-service registration failed") {
		t.Errorf("ExtraInfo should mention merkle-service failure, got %q", u.ExtraInfo)
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

	if err := flushSync(t, p); err != nil {
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

	if err := flushSync(t, p); err != nil {
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

	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	if ms.pendingRetryCount() != 0 {
		t.Errorf("202 ack must NOT route to PENDING_RETRY (tx is in flight); got %d", ms.pendingRetryCount())
	}
}

// TestNoVerdict_NoHealthyEndpoints_TerminalRejected is the regression
// guard for the "tx stuck in RECEIVED" scenario. In the dep-aware
// design, no retry queue: when no peer is reachable, the tx writes a
// terminal REJECTED row and the wallet can resubmit.
func TestNoVerdict_NoHealthyEndpoints_TerminalRejected(t *testing.T) {
	ms := newMockStore()

	// teranode client with no endpoints → GetHealthyEndpoints returns empty
	// → broadcastSingleOnce returns broadcastResult{} → no_verdict.
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	if err := handleAndFlush(t, p, makePropMsg("tx-stuck")); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if ms.pendingRetryCount() != 0 {
		t.Errorf("expected 0 PENDING_RETRY rows in the dep-aware design, got %d", ms.pendingRetryCount())
	}
	u := ms.lastUpdateForTxid("tx-stuck")
	if u == nil || u.Status != models.StatusRejected {
		t.Errorf("expected terminal REJECTED, got %+v", u)
	}
}

// Test 14: Batch — any endpoint success → AcceptedByNetwork for all
func TestBatchTransactions_AnySuccess_AcceptedByNetwork(t *testing.T) {
	ms := newMockStore()

	// First endpoint fails, second succeeds
	callCount := atomic.Int32{}
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)

	for i := 0; i < 3; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i))))
	}

	if err := flushSync(t, p); err != nil {
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

	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, ms)

	for i := 0; i < 3; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsg(fmt.Sprintf("tx%d", i))))
	}

	if err := flushSync(t, p); err != nil {
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
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(errMsg))
	}))
}

// Reaper-based retry tests previously covered the legacy
// PENDING_RETRY → reaper rebroadcast flow. That flow is disabled in
// the dep-aware design (failures are terminal REJECTED, no queue, no
// reaper). The reaper code itself is preserved in-tree as inert
// tooling; if it's ever re-enabled, the tests can be restored from
// git history.

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
	return New(cfg, zap.NewNop(), nil, nil, st, leaser, tc, nil)
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
