package api_server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// mockStore implements store.Store for testing callback handlers.
//
// InsertStump records calls under a composite "blockHash:subtreeIndex" key so
// tests can verify the full round-trip of a STUMP payload (including hex
// decoding of models.HexBytes). A mutex protects the stumps map because the
// end-to-end STUMP test fires deliveries concurrently to mirror
// merkle-service's 64-worker delivery pool.
type mockStore struct {
	mu                  sync.Mutex
	updateStatusCalls   []*models.TransactionStatus
	stumps              map[string]*models.Stump
	insertStumpErr      error
	insertedSubmissions []*models.Submission
	// insertStumpFn, if set, runs before the default record step and may
	// return an error to simulate per-key failures (Aerospike RECORD_TOO_BIG,
	// DEVICE_OVERLOAD, HOT_KEY, etc.). Returning non-nil skips the record.
	insertStumpFn func(stump *models.Stump) error
}

func (m *mockStore) UpdateStatus(_ context.Context, status *models.TransactionStatus) error {
	m.updateStatusCalls = append(m.updateStatusCalls, status)
	return nil
}

func (m *mockStore) GetOrInsertStatus(context.Context, *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	return nil, false, nil
}

func (m *mockStore) BatchGetOrInsertStatus(context.Context, []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return nil, nil
}

func (m *mockStore) BatchUpdateStatus(context.Context, []*models.TransactionStatus) error {
	return nil
}

func (m *mockStore) GetStatus(context.Context, string) (*models.TransactionStatus, error) {
	return nil, nil
}

func (m *mockStore) GetStatusesSince(context.Context, time.Time) ([]*models.TransactionStatus, error) {
	return nil, nil
}

func (m *mockStore) IterateStatusesSince(context.Context, time.Time, func(*models.TransactionStatus) error) error {
	return nil
}

func (m *mockStore) SetStatusByBlockHash(context.Context, string, models.Status) ([]string, error) {
	return nil, nil
}
func (m *mockStore) InsertBUMP(context.Context, string, uint64, []byte) error { return nil }
func (m *mockStore) GetBUMP(context.Context, string) (uint64, []byte, error)  { return 0, nil, nil }
func (m *mockStore) SetMinedByTxIDs(context.Context, string, []string) ([]*models.TransactionStatus, error) {
	return nil, nil
}

func (m *mockStore) InsertSubmission(_ context.Context, sub *models.Submission) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertedSubmissions = append(m.insertedSubmissions, sub)
	return nil
}

func (m *mockStore) GetSubmissionsByTxID(context.Context, string) ([]*models.Submission, error) {
	return nil, nil
}

func (m *mockStore) GetSubmissionsByToken(context.Context, string) ([]*models.Submission, error) {
	return nil, nil
}

func (m *mockStore) UpdateDeliveryStatus(context.Context, string, models.Status, int, *time.Time) error {
	return nil
}

func (m *mockStore) InsertStump(_ context.Context, stump *models.Stump) error {
	if m.insertStumpErr != nil {
		return m.insertStumpErr
	}
	if m.insertStumpFn != nil {
		if err := m.insertStumpFn(stump); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stumps == nil {
		m.stumps = make(map[string]*models.Stump)
	}
	// Copy the payload so later caller mutation can't race with assertions.
	dataCopy := append([]byte(nil), stump.StumpData...)
	m.stumps[fmt.Sprintf("%s:%d", stump.BlockHash, stump.SubtreeIndex)] = &models.Stump{
		BlockHash:    stump.BlockHash,
		SubtreeIndex: stump.SubtreeIndex,
		StumpData:    dataCopy,
	}
	return nil
}

func (m *mockStore) GetStumpsByBlockHash(context.Context, string) ([]*models.Stump, error) {
	return nil, nil
}
func (m *mockStore) DeleteStumpsByBlockHash(context.Context, string) error { return nil }
func (m *mockStore) BumpRetryCount(context.Context, string) (int, error)   { return 0, nil }
func (m *mockStore) SetPendingRetryFields(context.Context, string, []byte, time.Time) error {
	return nil
}

func (m *mockStore) GetReadyRetries(context.Context, time.Time, int) ([]*store.PendingRetry, error) {
	return nil, nil
}

func (m *mockStore) ClearRetryState(context.Context, string, models.Status, string) error {
	return nil
}
func (m *mockStore) EnsureIndexes() error { return nil }
func (m *mockStore) UpsertDatahubEndpoint(context.Context, store.DatahubEndpoint) error {
	return nil
}

func (m *mockStore) ListDatahubEndpoints(context.Context, string) ([]store.DatahubEndpoint, error) {
	return nil, nil
}
func (m *mockStore) Close() error { return nil }

func makeMinimalTx() []byte {
	tx := sdkTx.NewTransaction()
	return tx.Bytes()
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

// testCallbackToken is the bearer secret every callback test installs into the
// Server.cfg so handleCallback's mandatory auth check passes. Real deployments
// supply a high-entropy value via config.callback_token; tests just need a
// stable non-empty string.
const testCallbackToken = "test-callback-token"

func setupServerWithStore(broker *kafka.RecordingBroker, ms *mockStore) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	producer := kafka.NewProducer(broker)
	srv := &Server{
		cfg:      &config.Config{CallbackToken: testCallbackToken},
		logger:   zap.NewNop(),
		producer: producer,
		store:    ms,
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

func setupServer(broker *kafka.RecordingBroker) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	producer := kafka.NewProducer(broker)
	srv := &Server{
		cfg:      &config.Config{CallbackToken: testCallbackToken},
		logger:   zap.NewNop(),
		producer: producer,
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

// authedCallbackRequest builds a callback POST with the canonical bearer
// header so the mandatory auth check inside handleCallback accepts it. Tests
// that exercise the auth check itself construct their own requests instead.
func authedCallbackRequest(t *testing.T, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testCallbackToken)
	return req
}

// totalMessages returns the combined count of single-message Sends and
// batched entries — matching the old Sarama mock's flat-message semantics.
func totalMessages(broker *kafka.RecordingBroker) int {
	broker.Lock()
	defer broker.Unlock()
	return len(broker.Sends) + func() int {
		n := 0
		for _, b := range broker.Batches {
			n += len(b)
		}
		return n
	}()
}

func TestHandleSubmitTransactions_BatchPublish(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker)

	// Concatenate 3 minimal transactions
	txBytes := makeMinimalTx()
	body := bytes.Repeat(txBytes, 3)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["submitted"].(float64)) != 3 {
		t.Errorf("expected submitted=3, got %v", resp["submitted"])
	}

	if broker.BatchCalls != 1 {
		t.Errorf("expected 1 batch call, got %d", broker.BatchCalls)
	}
	if got := broker.MessageCount(); got != 3 {
		t.Errorf("expected 3 messages, got %d", got)
	}
}

func TestHandleSubmitTransactions_ParseFailure_NoPublish(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker)

	// Valid tx followed by garbage
	body := append(makeMinimalTx(), 0xff, 0xfe, 0xfd)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	if broker.BatchCalls != 0 {
		t.Errorf("expected 0 batch calls on parse failure, got %d", broker.BatchCalls)
	}
}

func TestHandleSubmitTransactions_100Txs_SingleBatchCall(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	_, router := setupServer(broker)

	// Concatenate 100 minimal transactions
	txBytes := makeMinimalTx()
	body := bytes.Repeat(txBytes, 100)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["submitted"].(float64)) != 100 {
		t.Errorf("expected submitted=100, got %v", resp["submitted"])
	}

	if broker.BatchCalls != 1 {
		t.Errorf("expected exactly 1 batch call for 100 txs, got %d", broker.BatchCalls)
	}
	if got := broker.MessageCount(); got != 100 {
		t.Errorf("expected 100 messages in batch, got %d", got)
	}
}

func TestHandleSubmitTransactions_KafkaFailure_Returns500(t *testing.T) {
	broker := &kafka.RecordingBroker{BatchErr: errors.New("broker down")}
	_, router := setupServer(broker)

	body := makeMinimalTx()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleCallback_SeenMultipleNodes_UpdatesStatus(t *testing.T) {
	ms := &mockStore{}
	_, router := setupServerWithStore(&kafka.RecordingBroker{}, ms)

	payload := models.CallbackMessage{
		Type:  models.CallbackSeenMultipleNodes,
		TxIDs: []string{"tx1", "tx2"},
	}
	body := mustMarshalJSON(t, payload)

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if len(ms.updateStatusCalls) != 2 {
		t.Fatalf("expected 2 UpdateStatus calls, got %d", len(ms.updateStatusCalls))
	}
	for i, call := range ms.updateStatusCalls {
		if call.Status != models.StatusSeenMultipleNodes {
			t.Errorf("call %d: expected status %s, got %s", i, models.StatusSeenMultipleNodes, call.Status)
		}
	}
	if ms.updateStatusCalls[0].TxID != "tx1" {
		t.Errorf("expected first txid=tx1, got %s", ms.updateStatusCalls[0].TxID)
	}
	if ms.updateStatusCalls[1].TxID != "tx2" {
		t.Errorf("expected second txid=tx2, got %s", ms.updateStatusCalls[1].TxID)
	}
}

func TestHandleCallback_Stump_StorageError_Returns500(t *testing.T) {
	// When STUMP storage fails, we MUST return 5xx so merkle-service retries.
	// Swallowing the error with a 200 breaks the bump_builder's invariant that
	// every STUMP in a BLOCK_PROCESSED block is durably stored in Aerospike.
	broker := &kafka.RecordingBroker{}
	ms := &mockStore{insertStumpErr: errors.New("SERVER_MEM_ERROR")}
	_, router := setupServerWithStore(broker, ms)

	payload := models.CallbackMessage{
		Type:      models.CallbackStump,
		BlockHash: "abc123",
		Stump:     []byte{0x01, 0x02},
	}
	body := mustMarshalJSON(t, payload)

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
	if got := totalMessages(broker); got != 0 {
		t.Errorf("expected 0 Kafka messages after storage failure, got %d", got)
	}
}

func TestHandleCallback_SeenMultipleNodes_EmptyTxIDs(t *testing.T) {
	ms := &mockStore{}
	_, router := setupServerWithStore(&kafka.RecordingBroker{}, ms)

	payload := models.CallbackMessage{
		Type:  models.CallbackSeenMultipleNodes,
		TxIDs: []string{},
	}
	body := mustMarshalJSON(t, payload)

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if len(ms.updateStatusCalls) != 0 {
		t.Errorf("expected 0 UpdateStatus calls, got %d", len(ms.updateStatusCalls))
	}
}

// TestHandleCallback_FullBlockFlow_20Subtrees simulates the production delivery
// pattern that merkle-service executes for a 20,000-tx block split across 20
// subtrees:
//
//  1. Twenty STUMP callbacks (one per subtreeIndex 0..19) POSTed to
//     /api/v1/merkle-service/callback, each carrying a realistic ~8 KB payload
//     hex-encoded via models.HexBytes. merkle-service fires these in parallel
//     from a 64-worker delivery pool (merkle-service/internal/callback/delivery.go),
//     so we dispatch them concurrently here.
//  2. A single BLOCK_PROCESSED callback with just the block hash, which
//     merkle-service's stumpGate only releases AFTER every STUMP has returned
//     2xx (merkle-service/internal/callback/delivery.go stumpGate.Wait).
//
// The test asserts end-to-end correctness of the code path that production is
// returning 500s from:
//
//   - every STUMP returns 200
//   - all 20 STUMPs land in the store with the correct composite key and
//     byte-identical payload (hex round-trip through models.HexBytes)
//   - Kafka is not touched for STUMPs (they go to the store only)
//   - BLOCK_PROCESSED produces exactly one Kafka message on
//     arcade.block_processed, keyed by block hash, with the full
//     CallbackMessage JSON as the value
//   - retry semantics: a duplicated STUMP delivery still returns 200 and
//     overwrites cleanly, because merkle-service retries on any non-2xx
func TestHandleCallback_FullBlockFlow_20Subtrees(t *testing.T) {
	const (
		numSubtrees = 20
		stumpSize   = 8 * 1024 // ~8 KB — realistic for a subtree covering ~1000 txs (BRC-0074 BUMP format)
	)
	blockHash := "000000000000000000001234567890abcdef1234567890abcdef1234567890ab"

	// Deterministic per-subtree payload so byte-level assertions are stable.
	stumpPayloads := make([][]byte, numSubtrees)
	for i := range stumpPayloads {
		buf := make([]byte, stumpSize)
		for j := range buf {
			buf[j] = byte((i*31 + j) & 0xFF)
		}
		stumpPayloads[i] = buf
	}

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	_, router := setupServerWithStore(broker, ms)

	// Phase 1: fire all 20 STUMPs in parallel.
	var wg sync.WaitGroup
	errCh := make(chan error, numSubtrees)
	for i := 0; i < numSubtrees; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := models.CallbackMessage{
				Type:         models.CallbackStump,
				BlockHash:    blockHash,
				SubtreeIndex: i,
				Stump:        stumpPayloads[i],
			}
			body, err := json.Marshal(payload)
			if err != nil {
				errCh <- fmt.Errorf("marshal subtree %d: %w", i, err)
				return
			}
			req := authedCallbackRequest(t, body)
			// Match merkle-service's delivery headers exactly.
			req.Header.Set("X-Idempotency-Key", fmt.Sprintf("%s:%d:STUMP", blockHash, i))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				errCh <- fmt.Errorf("STUMP subtree %d: expected 200, got %d: %s", i, w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	// All 20 STUMPs must be in the store, bit-identical to what was sent.
	ms.mu.Lock()
	stored := len(ms.stumps)
	ms.mu.Unlock()
	if stored != numSubtrees {
		t.Fatalf("expected %d STUMPs stored, got %d", numSubtrees, stored)
	}
	for i := 0; i < numSubtrees; i++ {
		key := fmt.Sprintf("%s:%d", blockHash, i)
		ms.mu.Lock()
		stump, ok := ms.stumps[key]
		ms.mu.Unlock()
		if !ok {
			t.Errorf("missing stump for subtree %d (key=%q)", i, key)
			continue
		}
		if stump.BlockHash != blockHash {
			t.Errorf("subtree %d: blockHash = %q, want %q", i, stump.BlockHash, blockHash)
		}
		if stump.SubtreeIndex != i {
			t.Errorf("subtree %d: SubtreeIndex = %d, want %d", i, stump.SubtreeIndex, i)
		}
		if !bytes.Equal(stump.StumpData, stumpPayloads[i]) {
			t.Errorf("subtree %d: stump bytes differ after hex round-trip (got %d bytes, want %d)",
				i, len(stump.StumpData), len(stumpPayloads[i]))
		}
	}

	// STUMPs must not produce Kafka traffic — the bump_builder consumes
	// arcade.block_processed only, and STUMPs are writes to the store.
	if got := totalMessages(broker); got != 0 {
		t.Fatalf("expected 0 Kafka messages after STUMP phase, got %d", got)
	}

	// Phase 2: BLOCK_PROCESSED. Carries only the block hash; merkle-service
	// does not resend the stump bytes here.
	blockMsg := models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: blockHash,
	}
	body, err := json.Marshal(blockMsg)
	if err != nil {
		t.Fatalf("marshal BLOCK_PROCESSED: %v", err)
	}
	req := authedCallbackRequest(t, body)
	req.Header.Set("X-Idempotency-Key", blockHash+":BLOCK_PROCESSED")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("BLOCK_PROCESSED: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Exactly one Kafka message on arcade.block_processed, keyed by block
	// hash, with the full CallbackMessage JSON as the value — this is what
	// the bump_builder consumer expects (services/bump_builder/builder.go).
	if got := totalMessages(broker); got != 1 {
		t.Fatalf("expected 1 Kafka message after BLOCK_PROCESSED, got %d", got)
	}
	broker.Lock()
	if len(broker.Sends) != 1 {
		broker.Unlock()
		t.Fatalf("expected 1 Send call, got %d", len(broker.Sends))
	}
	sent := broker.Sends[0]
	broker.Unlock()
	if sent.Topic != kafka.TopicBlockProcessed {
		t.Errorf("Kafka topic = %q, want %q", sent.Topic, kafka.TopicBlockProcessed)
	}
	if sent.Key != blockHash {
		t.Errorf("Kafka key = %q, want %q", sent.Key, blockHash)
	}
	var decoded models.CallbackMessage
	if err := json.Unmarshal(sent.Value, &decoded); err != nil {
		t.Fatalf("unmarshal kafka value: %v", err)
	}
	if decoded.Type != models.CallbackBlockProcessed {
		t.Errorf("kafka value Type = %q, want %q", decoded.Type, models.CallbackBlockProcessed)
	}
	if decoded.BlockHash != blockHash {
		t.Errorf("kafka value BlockHash = %q, want %q", decoded.BlockHash, blockHash)
	}

	// Phase 3: idempotency. merkle-service retries on any non-2xx with
	// linear backoff, so a second delivery of subtree 0 must still return
	// 200. Our store is upsert-on-(blockHash,subtreeIndex) so the count
	// stays at 20.
	retryPayload := models.CallbackMessage{
		Type:         models.CallbackStump,
		BlockHash:    blockHash,
		SubtreeIndex: 0,
		Stump:        stumpPayloads[0],
	}
	retryBody := mustMarshalJSON(t, retryPayload)
	retryReq := authedCallbackRequest(t, retryBody)
	retryW := httptest.NewRecorder()
	router.ServeHTTP(retryW, retryReq)
	if retryW.Code != http.StatusOK {
		t.Fatalf("duplicate STUMP: expected 200, got %d: %s", retryW.Code, retryW.Body.String())
	}
	ms.mu.Lock()
	finalCount := len(ms.stumps)
	ms.mu.Unlock()
	if finalCount != numSubtrees {
		t.Errorf("expected stump count to stay at %d after duplicate, got %d", numSubtrees, finalCount)
	}
}

// TestHandleCallback_FullBlockFlow_PartialStumpFailure reproduces the
// production failure surface: during delivery of 20 STUMPs, one subtree's
// Put() fails at the store layer (simulating Aerospike's RECORD_TOO_BIG when
// a busy subtree's BUMP proof exceeds the namespace's write-block-size, or a
// transient DEVICE_OVERLOAD / HOT_KEY on a single composite key) while the
// other 19 succeed.
//
// The test locks down the observable behavior that the bump_builder and
// merkle-service both depend on:
//
//   - the failing STUMP responds 500 so merkle-service's retry loop
//     re-queues it (merkle-service/internal/callback/delivery.go dispatch →
//     retry path)
//   - the succeeding STUMPs respond 200 and land in the store
//   - NO BLOCK_PROCESSED-like Kafka publish happens during the STUMP phase,
//     so bump_builder never sees a block with missing STUMPs
//   - sending BLOCK_PROCESSED while one STUMP is still missing DOES still
//     publish to Kafka — arcade does not validate STUMP completeness here.
//     That is intentional: merkle-service's stumpGate is what gates
//     BLOCK_PROCESSED on upstream 2xx, and if the retry exhausts it falls
//     into merkle-service's DLQ rather than calling BLOCK_PROCESSED.
//     This assertion documents where responsibility sits.
func TestHandleCallback_FullBlockFlow_PartialStumpFailure(t *testing.T) {
	const (
		numSubtrees    = 20
		stumpSize      = 8 * 1024
		failingSubtree = 7
	)
	blockHash := "000000000000000000001234567890abcdef1234567890abcdef1234567890ab"

	stumpPayloads := make([][]byte, numSubtrees)
	for i := range stumpPayloads {
		buf := make([]byte, stumpSize)
		for j := range buf {
			buf[j] = byte((i*31 + j) & 0xFF)
		}
		stumpPayloads[i] = buf
	}

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{
		insertStumpFn: func(stump *models.Stump) error {
			if stump.SubtreeIndex == failingSubtree {
				// Shape matches an Aerospike-style error string so a log
				// consumer correlating with store-layer errors can find it.
				return errors.New("Put failed: ResultCode: SERVER_MEM_ERROR")
			}
			return nil
		},
	}
	_, router := setupServerWithStore(broker, ms)

	type result struct {
		idx    int
		status int
	}
	resCh := make(chan result, numSubtrees)

	var wg sync.WaitGroup
	for i := 0; i < numSubtrees; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := models.CallbackMessage{
				Type:         models.CallbackStump,
				BlockHash:    blockHash,
				SubtreeIndex: i,
				Stump:        stumpPayloads[i],
			}
			body := mustMarshalJSON(t, payload)
			req := authedCallbackRequest(t, body)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			resCh <- result{idx: i, status: w.Code}
		}()
	}
	wg.Wait()
	close(resCh)

	statuses := make(map[int]int, numSubtrees)
	for r := range resCh {
		statuses[r.idx] = r.status
	}
	for i := 0; i < numSubtrees; i++ {
		want := http.StatusOK
		if i == failingSubtree {
			want = http.StatusInternalServerError
		}
		if statuses[i] != want {
			t.Errorf("subtree %d: status = %d, want %d", i, statuses[i], want)
		}
	}

	ms.mu.Lock()
	stored := len(ms.stumps)
	_, failingStored := ms.stumps[fmt.Sprintf("%s:%d", blockHash, failingSubtree)]
	ms.mu.Unlock()
	if stored != numSubtrees-1 {
		t.Errorf("expected %d STUMPs in store after partial failure, got %d", numSubtrees-1, stored)
	}
	if failingStored {
		t.Errorf("failing subtree %d must not be in the store", failingSubtree)
	}

	// Kafka must be untouched during STUMP phase, even with a mid-flight 500.
	if got := totalMessages(broker); got != 0 {
		t.Fatalf("expected 0 Kafka messages during STUMP phase, got %d", got)
	}

	// BLOCK_PROCESSED is still accepted and published. arcade does not check
	// STUMP completeness — that contract lives in merkle-service's stumpGate.
	// The bump_builder handles missing STUMPs downstream via its grace window
	// + GetStumpsByBlockHash retrieval, so this call must not be rejected.
	blockMsg := models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: blockHash,
	}
	body := mustMarshalJSON(t, blockMsg)
	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("BLOCK_PROCESSED: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := totalMessages(broker); got != 1 {
		t.Errorf("expected 1 Kafka message after BLOCK_PROCESSED, got %d", got)
	}
}

// makeRealTx returns a transaction with one funded input and one output so
// that tx.EF() produces a different byte sequence than tx.Bytes(). Without
// real source data, EF() returns ErrEmptyPreviousTx and the EF/legacy hashes
// would collide — defeating the purpose of the EF-vs-legacy regression tests.
func makeRealTx(t *testing.T) *sdkTx.Transaction {
	t.Helper()
	tx := sdkTx.NewTransaction()
	if err := tx.AddInputFrom(
		"0000000000000000000000000000000000000000000000000000000000000001",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		1000,
		nil,
	); err != nil {
		t.Fatalf("AddInputFrom: %v", err)
	}
	opReturn, err := script.NewFromHex("6a")
	if err != nil {
		t.Fatalf("script.NewFromHex: %v", err)
	}
	tx.AddOutput(&sdkTx.TransactionOutput{Satoshis: 900, LockingScript: opReturn})
	return tx
}

// TestHandleSubmitTransaction_TxID_IsCanonical verifies /tx records the
// canonical Bitcoin txid (tx.TxID()) — not a hash of the wire bytes — for
// every accepted content type and for both legacy and Extended Format
// submissions. Regression test for the EF / canonical txid mismatch that
// caused submissions.txid to never match transactions.txid, which broke SSE
// status fan-out for any client posting EF.
func TestHandleSubmitTransaction_TxID_IsCanonical(t *testing.T) {
	tx := makeRealTx(t)
	legacy := tx.Bytes()
	ef, err := tx.EF()
	if err != nil {
		t.Fatalf("tx.EF: %v", err)
	}
	canonical := tx.TxID().String()

	if bytes.Equal(legacy, ef) {
		t.Fatalf("EF and legacy bytes are identical — test would be trivial")
	}

	cases := []struct {
		name        string
		contentType string
		body        []byte
	}{
		{"octet-stream legacy", "application/octet-stream", legacy},
		{"octet-stream EF", "application/octet-stream", ef},
		{"text/plain hex legacy", "text/plain", []byte(hex.EncodeToString(legacy))},
		{"text/plain hex EF", "text/plain", []byte(hex.EncodeToString(ef))},
		{"json legacy", "application/json", mustMarshalJSON(t, map[string]string{"rawTx": hex.EncodeToString(legacy)})},
		{"json EF", "application/json", mustMarshalJSON(t, map[string]string{"rawTx": hex.EncodeToString(ef)})},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			broker := &kafka.RecordingBroker{}
			ms := &mockStore{}
			_, router := setupServerWithStore(broker, ms)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(c.body))
			req.Header.Set("Content-Type", c.contentType)
			req.Header.Set("X-CallbackToken", "test-token")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusAccepted {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}

			if len(broker.Sends) != 1 {
				t.Fatalf("expected 1 Send, got %d", len(broker.Sends))
			}
			if broker.Sends[0].Key != canonical {
				t.Errorf("kafka key: want %s, got %s", canonical, broker.Sends[0].Key)
			}

			if len(ms.insertedSubmissions) != 1 {
				t.Fatalf("expected 1 submission, got %d", len(ms.insertedSubmissions))
			}
			if ms.insertedSubmissions[0].TxID != canonical {
				t.Errorf("submission txid: want %s, got %s", canonical, ms.insertedSubmissions[0].TxID)
			}
		})
	}
}

// TestHandleSubmitTransactions_TxID_IsCanonical is the bulk-endpoint
// counterpart. Mixing legacy and EF in a single batch also confirms the
// parser advances bytesUsed correctly across format changes.
func TestHandleSubmitTransactions_TxID_IsCanonical(t *testing.T) {
	txA := makeRealTx(t)
	txB := makeRealTx(t)
	txB.Outputs[0].Satoshis = 800 // make B distinct so canonicals differ

	legacyA := txA.Bytes()
	efB, err := txB.EF()
	if err != nil {
		t.Fatalf("EF: %v", err)
	}
	canonA := txA.TxID().String()
	canonB := txB.TxID().String()
	if canonA == canonB {
		t.Fatalf("test setup: txA and txB hashed equal")
	}

	body := append([]byte{}, legacyA...)
	body = append(body, efB...)

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	_, router := setupServerWithStore(broker, ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-CallbackToken", "test-token")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	if len(broker.Batches) != 1 || len(broker.Batches[0]) != 2 {
		t.Fatalf("expected 1 batch of 2, got Batches=%v", broker.Batches)
	}
	if got := broker.Batches[0][0].Key; got != canonA {
		t.Errorf("batch[0]: want %s, got %s", canonA, got)
	}
	if got := broker.Batches[0][1].Key; got != canonB {
		t.Errorf("batch[1]: want %s, got %s", canonB, got)
	}

	if len(ms.insertedSubmissions) != 2 {
		t.Fatalf("expected 2 submissions, got %d", len(ms.insertedSubmissions))
	}
	got := []string{ms.insertedSubmissions[0].TxID, ms.insertedSubmissions[1].TxID}
	want := []string{canonA, canonB}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("submissions: want %v, got %v", want, got)
	}
}

// TestHandleCallback_RejectsUnauthenticated locks down the F-018 fix: the
// /api/v1/merkle-service/callback receiver MUST refuse any request missing
// or presenting the wrong bearer token, and MUST refuse all requests when
// the configured token is empty (fail-closed). Pre-fix, the handler skipped
// the entire bearer check when CallbackToken == "", letting any unauthenticated
// caller submit forged Merkle status updates.
//
// This test exercises the runtime check directly. The "config rejects empty
// token when Merkle is enabled" half of the fix is covered in
// config/config_test.go (TestValidate_RequiresCallbackTokenWhenMerkleEnabled).
func TestHandleCallback_RejectsUnauthenticated(t *testing.T) {
	payload := mustMarshalJSON(t, models.CallbackMessage{
		Type:  models.CallbackSeenMultipleNodes,
		TxIDs: []string{"tx1"},
	})

	cases := []struct {
		name    string
		token   string // configured CallbackToken on the Server.
		header  string // Authorization header sent on the request, "" = none.
		wantOK  bool
		wantErr int
	}{
		{
			name:    "no auth header is rejected",
			token:   testCallbackToken,
			header:  "",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			name:    "wrong bearer is rejected",
			token:   testCallbackToken,
			header:  "Bearer not-the-real-token",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			name:    "non-bearer scheme is rejected",
			token:   testCallbackToken,
			header:  "Basic " + testCallbackToken,
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			// Defense-in-depth. Config validation now refuses to start with an
			// empty CallbackToken when Merkle is enabled, but if a misconfigured
			// process somehow reaches the handler with cfg.CallbackToken == ""
			// every request — including one presenting an empty bearer — must
			// still be refused.
			name:    "empty configured token rejects all callers",
			token:   "",
			header:  "Bearer ",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			// Same scenario as above with no auth header — must also be 401.
			name:    "empty configured token rejects unauthenticated caller",
			token:   "",
			header:  "",
			wantOK:  false,
			wantErr: http.StatusUnauthorized,
		},
		{
			name:   "correct bearer is accepted",
			token:  testCallbackToken,
			header: "Bearer " + testCallbackToken,
			wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := &mockStore{}
			gin.SetMode(gin.TestMode)
			producer := kafka.NewProducer(&kafka.RecordingBroker{})
			srv := &Server{
				cfg:      &config.Config{CallbackToken: tc.token},
				logger:   zap.NewNop(),
				producer: producer,
				store:    ms,
			}
			router := gin.New()
			srv.registerRoutes(router)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if tc.wantOK {
				if w.Code != http.StatusOK {
					t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
				}
				if len(ms.updateStatusCalls) != 1 {
					t.Errorf("expected store update on accepted callback, got %d", len(ms.updateStatusCalls))
				}
				return
			}
			if w.Code != tc.wantErr {
				t.Fatalf("expected %d, got %d: %s", tc.wantErr, w.Code, w.Body.String())
			}
			// Rejected requests must not reach the dispatch path.
			if len(ms.updateStatusCalls) != 0 {
				t.Errorf("rejected callback must not write to the store, got %d UpdateStatus calls", len(ms.updateStatusCalls))
			}
		})
	}
}
