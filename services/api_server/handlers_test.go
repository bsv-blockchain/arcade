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
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// mockStore implements store.Store for testing callback handlers.
//
// InsertStump records calls under a composite "blockHash:subtreeIndex" key so
// tests can verify the full round-trip of a STUMP payload (including hex
// decoding of models.HexBytes). A mutex protects the stumps map because the
// end-to-end STUMP test fires deliveries concurrently to mirror
// merkle-service's 64-worker delivery pool.
type mockStore struct {
	mu                sync.Mutex
	updateStatusCalls []*models.TransactionStatus
	stumps            map[string]*models.Stump
	insertStumpErr    error
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
func (m *mockStore) SetStatusByBlockHash(context.Context, string, models.Status) ([]string, error) {
	return nil, nil
}
func (m *mockStore) InsertBUMP(context.Context, string, uint64, []byte) error { return nil }
func (m *mockStore) GetBUMP(context.Context, string) (uint64, []byte, error)  { return 0, nil, nil }
func (m *mockStore) SetMinedByTxIDs(context.Context, string, []string) ([]*models.TransactionStatus, error) {
	return nil, nil
}
func (m *mockStore) InsertSubmission(context.Context, *models.Submission) error { return nil }
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
func (m *mockStore) ListDatahubEndpoints(context.Context) ([]store.DatahubEndpoint, error) {
	return nil, nil
}
func (m *mockStore) Close() error         { return nil }

func makeMinimalTx() []byte {
	tx := sdkTx.NewTransaction()
	return tx.Bytes()
}

func setupServerWithStore(broker *kafka.RecordingBroker, ms *mockStore) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	producer := kafka.NewProducer(broker)
	srv := &Server{
		cfg:      &config.Config{},
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
		cfg:      &config.Config{},
		logger:   zap.NewNop(),
		producer: producer,
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
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

	req := httptest.NewRequest(http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
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

	req := httptest.NewRequest(http.MethodPost, "/txs", bytes.NewReader(body))
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

	req := httptest.NewRequest(http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
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

	req := httptest.NewRequest(http.MethodPost, "/txs", bytes.NewReader(body))
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
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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
		i := i
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
			req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
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
	req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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
	retryBody, _ := json.Marshal(retryPayload)
	retryReq := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(retryBody))
	retryReq.Header.Set("Content-Type", "application/json")
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
// The test locks down the observable behaviour that the bump_builder and
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
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := models.CallbackMessage{
				Type:         models.CallbackStump,
				BlockHash:    blockHash,
				SubtreeIndex: i,
				Stump:        stumpPayloads[i],
			}
			body, _ := json.Marshal(payload)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
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
	body, _ := json.Marshal(blockMsg)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/merkle-service/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("BLOCK_PROCESSED: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := totalMessages(broker); got != 1 {
		t.Errorf("expected 1 Kafka message after BLOCK_PROCESSED, got %d", got)
	}
}
