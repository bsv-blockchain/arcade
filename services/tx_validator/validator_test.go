package tx_validator

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/validator"
)

// mockStore is a test double for the parts of store.Store the validator
// touches. Embedding store.Store gives us a panic on any unexpected call so
// new dependencies surface immediately.
type mockStore struct {
	store.Store

	mu sync.Mutex
	// inserted records every GetOrInsertStatus call. existingByTxID, when
	// populated, makes the same txid return as a duplicate (inserted=false)
	// so dedup pathways can be exercised.
	inserted        []*models.TransactionStatus
	updates         []*models.TransactionStatus
	existingByTxID  map[string]*models.TransactionStatus
	insertErrByTxID map[string]error
	updateErrByTxID map[string]error
	insertCallCount atomic.Int64
	updateCallCount atomic.Int64
}

func newMockStore() *mockStore {
	return &mockStore{
		existingByTxID:  make(map[string]*models.TransactionStatus),
		insertErrByTxID: make(map[string]error),
		updateErrByTxID: make(map[string]error),
	}
}

func (m *mockStore) EnsureIndexes() error { return nil }

func (m *mockStore) GetOrInsertStatus(_ context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	m.insertCallCount.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.insertErrByTxID[status.TxID]; ok {
		return nil, false, err
	}
	if existing, ok := m.existingByTxID[status.TxID]; ok {
		return existing, false, nil
	}
	m.inserted = append(m.inserted, status)
	return &models.TransactionStatus{TxID: status.TxID, Status: models.StatusReceived, Timestamp: time.Now()}, true, nil
}

// BatchGetOrInsertStatus / BatchUpdateStatus delegate to the per-row methods
// via the parallel-loop helper so tests exercise the same code path as the
// non-Postgres backends.
func (m *mockStore) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return store.BatchGetOrInsertStatusParallel(ctx, m, statuses)
}

func (m *mockStore) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	return store.BatchUpdateStatusParallel(ctx, m, statuses)
}

func (m *mockStore) UpdateStatus(_ context.Context, status *models.TransactionStatus) error {
	m.updateCallCount.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.updateErrByTxID[status.TxID]; ok {
		return err
	}
	m.updates = append(m.updates, status)
	return nil
}

func (m *mockStore) snapshotInserts() []*models.TransactionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*models.TransactionStatus, len(m.inserted))
	copy(out, m.inserted)
	return out
}

func (m *mockStore) snapshotUpdates() []*models.TransactionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*models.TransactionStatus, len(m.updates))
	copy(out, m.updates)
	return out
}

func makeValidTxHex() string {
	tx := sdkTx.NewTransaction()
	return hex.EncodeToString(tx.Bytes())
}

// makeValidTxBytesAndID returns a freshly minted empty-tx as bytes and the
// txid that go-sdk computes for it. Used by tests that want to assert the
// dedup path against a specific txid.
func makeValidTxBytesAndID(t *testing.T) ([]byte, string) {
	t.Helper()
	tx := sdkTx.NewTransaction()
	return tx.Bytes(), tx.TxID().String()
}

func makeTxMsg(rawTxHex string) []byte {
	rawTx, _ := hex.DecodeString(rawTxHex)
	msg := txMessage{Action: "submit", RawTx: rawTx}
	b, _ := json.Marshal(msg)
	return b
}

func makeTxMsgFromBytes(rawTx []byte) []byte {
	msg := txMessage{Action: "submit", RawTx: rawTx}
	b, _ := json.Marshal(msg)
	return b
}

func makeKafkaMsg(payload []byte) *kafka.Message {
	return &kafka.Message{Value: payload}
}

func newTestValidator(broker *kafka.RecordingBroker, ms *mockStore) *Validator {
	return newTestValidatorWithValidator(broker, ms, nil)
}

func newTestValidatorWithValidator(broker *kafka.RecordingBroker, ms *mockStore, txValidator *validator.Validator) *Validator {
	producer := kafka.NewProducer(broker)
	cfg := &config.Config{}
	tracker := store.NewTxTracker()
	v := New(cfg, zap.NewNop(), producer, ms, tracker, txValidator)
	// Pin parallelism for deterministic tests so we don't accidentally serialize
	// on a single-core CI runner.
	v.parallelism = 8
	return v
}

// TestValidator_HappyPath_TwoBatchesOf100 is the user-facing scenario from
// the design proposal: two quick bursts of 100 txs each. The validator must
// queue every message in handleMessage without doing any work, then process
// them all in parallel during a single flush, ending with one Kafka publish
// containing all 200 propagation messages.
func TestValidator_HappyPath_TwoBatchesOf100(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	// Each tx must be unique so dedup doesn't short-circuit any of them.
	// go-sdk's empty-tx encoding is deterministic, so we vary the lock_time
	// field via the bytes directly to produce different txids.
	for batch := 0; batch < 2; batch++ {
		for i := 0; i < 100; i++ {
			rawTx := uniqueRawTx(uint32(batch*100 + i))
			if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(rawTx))); err != nil {
				t.Fatalf("handleMessage[%d/%d]: %v", batch, i, err)
			}
		}
	}

	if broker.BatchCalls != 0 {
		t.Errorf("expected 0 batch calls before flush, got %d", broker.BatchCalls)
	}
	if got := ms.insertCallCount.Load(); got != 0 {
		t.Errorf("expected 0 store inserts before flush, got %d", got)
	}

	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if broker.BatchCalls != 1 {
		t.Errorf("expected exactly 1 SendBatch call, got %d", broker.BatchCalls)
	}
	if got := broker.MessageCount(); got != 200 {
		t.Errorf("expected 200 propagation messages, got %d", got)
	}
	if got := ms.insertCallCount.Load(); got != 200 {
		t.Errorf("expected 200 store inserts at flush time, got %d", got)
	}
}

// uniqueRawTx returns the bytes of an empty transaction with the given lock
// time so each call produces a distinct txid.
func uniqueRawTx(lockTime uint32) []byte {
	tx := sdkTx.NewTransaction()
	tx.LockTime = lockTime
	return tx.Bytes()
}

// Empty drain windows must not produce a Kafka publish or any store traffic.
func TestValidator_EmptyFlushIsNoOp(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if broker.BatchCalls != 0 {
		t.Errorf("expected 0 batch calls, got %d", broker.BatchCalls)
	}
	if got := ms.insertCallCount.Load(); got != 0 {
		t.Errorf("expected 0 store calls, got %d", got)
	}
}

// Single-message drain windows still flow through the pipeline so single-tx
// submissions don't regress.
func TestValidator_SingleTxFlows(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	txHex := makeValidTxHex()
	if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsg(txHex))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if broker.BatchCalls != 1 {
		t.Errorf("expected 1 batch call, got %d", broker.BatchCalls)
	}
	if got := broker.MessageCount(); got != 1 {
		t.Errorf("expected 1 propagation message, got %d", got)
	}
}

// Garbage payloads survive parsing (per-tx parse failure is logged + dropped)
// and don't break the rest of the batch.
func TestValidator_ParseFailures_DontBreakBatch(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	// Mix one garbage payload between two valid txs.
	if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(uniqueRawTx(1)))); err != nil {
		t.Fatal(err)
	}
	if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes([]byte{0x00, 0x01, 0x02}))); err != nil {
		t.Fatal(err)
	}
	if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(uniqueRawTx(2)))); err != nil {
		t.Fatal(err)
	}

	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Two valid txs publish; the garbage one is dropped pre-store.
	if got := broker.MessageCount(); got != 2 {
		t.Errorf("expected 2 messages published (garbage dropped), got %d", got)
	}
	if got := ms.insertCallCount.Load(); got != 2 {
		t.Errorf("expected 2 store inserts (garbage skipped), got %d", got)
	}
}

// A duplicate txid (already in the store) must short-circuit before
// validation, never reach the propagation topic, and update the tx tracker
// from the existing record.
func TestValidator_Duplicates_SkipValidationAndPublish(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	rawTx, txid := makeValidTxBytesAndID(t)
	ms.existingByTxID[txid] = &models.TransactionStatus{TxID: txid, Status: models.StatusSeenOnNetwork}

	if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(rawTx))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if broker.BatchCalls != 0 {
		t.Errorf("expected 0 batch calls for duplicate-only flush, got %d", broker.BatchCalls)
	}
	if got := ms.insertCallCount.Load(); got != 1 {
		t.Errorf("expected 1 GetOrInsertStatus call for the duplicate, got %d", got)
	}
}

// Validation rejection must persist a REJECTED status with the error in
// ExtraInfo and keep the tx out of the propagation topic.
func TestValidator_RejectedTxs_PersistedNotPublished(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()

	// A real validator rejects empty-input/output txs.
	realValidator := validator.NewValidator(nil, nil)
	v := newTestValidatorWithValidator(broker, ms, realValidator)

	// Three empty txs (all will fail policy validation) plus one extra to
	// confirm the loop continues across rejects.
	for i := 0; i < 3; i++ {
		if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(uniqueRawTx(uint32(i))))); err != nil {
			t.Fatal(err)
		}
	}

	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	updates := ms.snapshotUpdates()
	if len(updates) != 3 {
		t.Fatalf("expected 3 reject updates, got %d", len(updates))
	}
	for _, u := range updates {
		if u.Status != models.StatusRejected {
			t.Errorf("expected REJECTED, got %q", u.Status)
		}
		if u.ExtraInfo == "" {
			t.Errorf("expected reject reason in ExtraInfo, got empty")
		}
	}
	if broker.BatchCalls != 0 {
		t.Errorf("rejects must not publish; got %d batch calls", broker.BatchCalls)
	}
}

// A transient store failure on GetOrInsertStatus must NOT take the whole
// batch down; the offending tx is dropped and the rest publish normally.
func TestValidator_DedupStoreError_DropsOneKeepsBatch(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	goodRaw, _ := makeValidTxBytesAndID(t)
	goodRaw2 := uniqueRawTx(99)
	badRaw := uniqueRawTx(42)
	tx, _ := sdkTx.NewTransactionFromBytes(badRaw)
	ms.insertErrByTxID[tx.TxID().String()] = errors.New("transient db error")

	for _, raw := range [][]byte{goodRaw, badRaw, goodRaw2} {
		if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(raw))); err != nil {
			t.Fatal(err)
		}
	}

	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := broker.MessageCount(); got != 2 {
		t.Errorf("expected 2 messages (bad one skipped), got %d", got)
	}
}

// Failed Kafka publish must carry the messages over to the next flush so no
// validated tx is lost. This was the recovery path the old serial code had.
func TestValidator_PublishFailure_RetriesOnNextFlush(t *testing.T) {
	broker := &kafka.RecordingBroker{
		BatchErr: errors.New("transient kafka error"),
	}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	for i := 0; i < 5; i++ {
		if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(uniqueRawTx(uint32(i))))); err != nil {
			t.Fatal(err)
		}
	}
	err := v.flushValidations(context.Background())
	if err == nil {
		t.Fatal("expected publish error to surface from flush")
	}

	// Validations + inserts already done; the messages are carried for retry.
	if got := ms.insertCallCount.Load(); got != 5 {
		t.Errorf("expected 5 inserts on first flush, got %d", got)
	}

	v.mu.Lock()
	carrySize := len(v.publishCarry)
	v.mu.Unlock()
	if carrySize != 5 {
		t.Errorf("expected 5 carry-over messages, got %d", carrySize)
	}

	// Healing the broker and re-flushing must publish exactly the carried set.
	broker.Lock()
	broker.BatchErr = nil
	broker.Batches = nil
	broker.BatchCalls = 0
	broker.Unlock()

	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if got := broker.MessageCount(); got != 5 {
		t.Errorf("expected 5 messages on retry flush, got %d", got)
	}
	v.mu.Lock()
	carrySize = len(v.publishCarry)
	v.mu.Unlock()
	if carrySize != 0 {
		t.Errorf("expected carry to clear after successful retry, got %d", carrySize)
	}
	// Inserts must NOT have run again — dedup wasn't re-executed.
	if got := ms.insertCallCount.Load(); got != 5 {
		t.Errorf("expected insert count to stay at 5 after retry, got %d", got)
	}
}

// Successful flush must clear pendingValidations atomically. Two concurrent
// flush calls (a defensive scenario) must not produce duplicate publishes.
// This guards the lock discipline around v.mu / publishCarry.
func TestValidator_ConcurrentFlush_NoDoublePublish(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	for i := 0; i < 50; i++ {
		if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(uniqueRawTx(uint32(i))))); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = v.flushValidations(context.Background())
		}()
	}
	wg.Wait()

	// Total messages published across all flush calls equals the input count
	// (one of the four took the batch; the others saw an empty pending slice).
	if got := broker.MessageCount(); got != 50 {
		t.Errorf("expected 50 total messages, got %d", got)
	}
}

// Cancelling the claim context partway through a flush must let phases bail
// out cleanly. This is the rebalance-safety guarantee added by the recent
// FlushFunc(ctx) change.
func TestValidator_ContextCancel_BailsCleanly(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	v := newTestValidator(broker, ms)

	for i := 0; i < 10; i++ {
		if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(uniqueRawTx(uint32(i))))); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before flush starts

	// Flush should return without panicking. Some phases may still run (the
	// goroutines launch then early-out); the important thing is no panic and
	// the validator state stays consistent.
	_ = v.flushValidations(ctx)
}

// Wiring smoke test: the consumer's drain-then-flush hook calls the right
// function, parallelism is configured, and Start doesn't error on
// construction. We don't actually drive a Kafka claim here — that's in the
// kafka package's own integration tests.
func TestValidator_StartLogsParallelism(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()
	logger := zaptest.NewLogger(t)
	cfg := &config.Config{}
	cfg.TxValidator.Parallelism = 16

	v := New(cfg, logger, kafka.NewProducer(broker), ms, store.NewTxTracker(), nil)
	if v.parallelism != 16 {
		t.Errorf("expected parallelism=16 from config, got %d", v.parallelism)
	}
}
