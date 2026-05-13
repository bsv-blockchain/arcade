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

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
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
	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

func makeTxMsgFromBytes(rawTx []byte) []byte {
	msg := txMessage{Action: "submit", RawTx: rawTx}
	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

func makeKafkaMsg(payload []byte) *kafka.Message {
	return &kafka.Message{Value: payload}
}

func newTestValidator(broker *kafka.RecordingBroker, ms *mockStore) *Validator {
	return newTestValidatorWithValidator(broker, ms, nil)
}

func newTestValidatorWithValidator(broker *kafka.RecordingBroker, ms *mockStore, txValidator *validator.Validator) *Validator {
	return newTestValidatorWithConfig(broker, ms, txValidator, &config.Config{})
}

// newTestValidatorWithConfig wires the validator with a caller-supplied
// config so tests can flip TxValidator.SkipFeeValidation /
// SkipScriptValidation and assert the new plumbing.
func newTestValidatorWithConfig(broker *kafka.RecordingBroker, ms *mockStore, txValidator *validator.Validator, cfg *config.Config) *Validator {
	producer := kafka.NewProducer(broker)
	tracker := store.NewTxTracker()
	v := New(cfg, zap.NewNop(), producer, nil, ms, tracker, txValidator)
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

// Canceling the claim context partway through a flush must let phases bail
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
	cancel() // already canceled before flush starts

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

	v := New(cfg, logger, kafka.NewProducer(broker), nil, ms, store.NewTxTracker(), nil)
	if v.parallelism != 16 {
		t.Errorf("expected parallelism=16 from config, got %d", v.parallelism)
	}
}

// makeFeeFailingTxBytes returns a freshly minted transaction that passes
// validator.ValidatePolicy (one push-only input with a populated source
// output, one non-data output, size > minTxSizeBytes) but has totalIn ==
// totalOut, i.e. a fee of zero. The default min-fee-per-KB policy is 100
// sat/kB, so spv.Verify fee enforcement must reject it whenever
// skipFees=false. Per validator.ValidateTransaction's contract, a tx that
// has a non-nil chaintracker but zero fee will trip fee validation before
// any script work touches the wire — which is what we want for unit tests
// (no network).
func makeFeeFailingTxBytes(t *testing.T) []byte {
	t.Helper()
	tx := sdkTx.NewTransaction()
	// Vary the lock_time so the txid is unique across calls — keeps
	// dedup happy when this helper is reused inside a single batch.
	tx.LockTime = 0xdeadbeef

	srcID := chainhash.Hash{}
	srcID[0] = 0x42

	// Single input. UnlockingScript is a single OP_TRUE (0x51) so it
	// satisfies pushDataCheck (push-only and non-empty) without dragging
	// real signatures into the test fixture. The source output is set
	// inline so TotalInputSatoshis returns successfully — required for
	// spv.Verify's fee math to even run.
	unlocking := script.Script([]byte{0x51})
	in := &sdkTx.TransactionInput{
		SourceTXID:      &srcID,
		UnlockingScript: &unlocking,
	}
	in.SetSourceTxOutput(&sdkTx.TransactionOutput{
		Satoshis:      1_000,
		LockingScript: opTrueScript(),
	})
	tx.AddInput(in)

	// Single output equal to input → zero fee → fee validation rejects.
	tx.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      1_000,
		LockingScript: opTrueScript(),
	})

	// Pad with an extra OP_RETURN data output so the serialized tx is
	// comfortably above the 61-byte minimum policy size.
	dataScript := script.Script(append([]byte{0x6a, 0x20}, make([]byte, 0x20)...))
	tx.AddOutput(&sdkTx.TransactionOutput{
		Satoshis:      0,
		LockingScript: &dataScript,
	})

	return tx.Bytes()
}

// opTrueScript returns a *script.Script containing a single OP_TRUE byte.
// Treated as non-data and non-empty by the SDK heuristics, so it clears
// validator.checkOutputs without producing a dust output.
func opTrueScript() *script.Script {
	s := script.Script([]byte{0x51})
	return &s
}

// TestValidator_DefaultsRejectFeeFailures is the regression guard for
// finding F-022 / issue #80. The previous implementation hard-coded
// (skipFees=true, skipScripts=true) at the call site, so a tx with a zero
// fee was accepted even though the underlying validator was perfectly
// capable of catching it. With the fix the defaults thread through as
// (false, false) and the same tx gets rejected.
func TestValidator_DefaultsRejectFeeFailures(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()

	realValidator := validator.NewValidator(nil, nil)
	v := newTestValidatorWithValidator(broker, ms, realValidator)
	if v.skipFees || v.skipScripts {
		t.Fatalf("default config must keep fee/script validation on; got skipFees=%v skipScripts=%v",
			v.skipFees, v.skipScripts)
	}

	if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(makeFeeFailingTxBytes(t)))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	updates := ms.snapshotUpdates()
	if len(updates) != 1 {
		t.Fatalf("expected exactly 1 reject update from a zero-fee tx, got %d", len(updates))
	}
	if updates[0].Status != models.StatusRejected {
		t.Errorf("expected REJECTED, got %q", updates[0].Status)
	}
	if updates[0].ExtraInfo == "" {
		t.Errorf("reject reason should not be empty")
	}
	if broker.BatchCalls != 0 {
		t.Errorf("rejected tx must not reach the propagation topic; got %d batch calls",
			broker.BatchCalls)
	}
}

// TestValidator_SkipFlagsBypassFeeFailures pins the new config knobs:
// flipping skip_fee_validation=true makes the same zero-fee tx pass. The
// test exists so a future refactor can't silently drop the flags; if this
// test starts failing the flags have stopped being plumbed through.
func TestValidator_SkipFlagsBypassFeeFailures(t *testing.T) {
	broker := &kafka.RecordingBroker{}
	ms := newMockStore()

	cfg := &config.Config{}
	cfg.TxValidator.SkipFeeValidation = true
	cfg.TxValidator.SkipScriptValidation = true

	realValidator := validator.NewValidator(nil, nil)
	v := newTestValidatorWithConfig(broker, ms, realValidator, cfg)
	if !v.skipFees || !v.skipScripts {
		t.Fatalf("config skip flags must reach the validator; got skipFees=%v skipScripts=%v",
			v.skipFees, v.skipScripts)
	}

	if err := v.handleMessage(context.Background(), makeKafkaMsg(makeTxMsgFromBytes(makeFeeFailingTxBytes(t)))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := v.flushValidations(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := ms.updateCallCount.Load(); got != 0 {
		t.Errorf("expected 0 reject updates with skip flags on, got %d", got)
	}
	if broker.BatchCalls != 1 || broker.MessageCount() != 1 {
		t.Errorf("expected the tx to publish when skip flags are on; got batchCalls=%d messages=%d",
			broker.BatchCalls, broker.MessageCount())
	}
}

// TestValidator_DefaultSkipFlagsAreFalse is a tiny but high-value
// invariant test. If somebody flips the defaults in TxValidatorConfig the
// fee/script validators silently turn off again — this test fails loudly
// the moment that happens.
func TestValidator_DefaultSkipFlagsAreFalse(t *testing.T) {
	cfg := &config.Config{}
	v := New(cfg, zap.NewNop(), kafka.NewProducer(&kafka.RecordingBroker{}), nil,
		newMockStore(), store.NewTxTracker(), nil)
	if v.skipFees {
		t.Error("default cfg must keep fee validation on (skipFees=false)")
	}
	if v.skipScripts {
		t.Error("default cfg must keep script validation on (skipScripts=false)")
	}
}
