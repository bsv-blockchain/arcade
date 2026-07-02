package api_server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// stringField reads a zap observer ContextMap field expected to be a plain
// string.
func stringField(t *testing.T, fields map[string]interface{}, key string) string {
	t.Helper()
	v, ok := fields[key].(string)
	if !ok {
		t.Fatalf("field %q missing or not a string: %#v", key, fields[key])
	}
	return v
}

// int64Field reads a zap observer ContextMap field expected to be an
// integer (zap.Int/zap.Int64 both surface as int64 through ContextMap).
func int64Field(t *testing.T, fields map[string]interface{}, key string) int64 {
	t.Helper()
	v, ok := fields[key].(int64)
	if !ok {
		t.Fatalf("field %q missing or not an int64: %#v", key, fields[key])
	}
	return v
}

// TestHandleSubmitTransaction_LogsReceived closes the RECEIVED log gap for
// the single-tx submit path: a successful POST /tx must emit an Info
// "transaction received" line carrying the canonical txid and status
// fields, so a Coralogix search for the txid finds the start of its
// lifecycle.
func TestHandleSubmitTransaction_LogsReceived(t *testing.T) {
	rawTx := makeMinimalTx()
	parsed, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		t.Fatalf("parsing test tx: %v", err)
	}
	wantTxID := parsed.TxID().String()

	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	ms := &mockStore{
		getOrInsertFn: func(_ *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return nil, true, nil
		},
	}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         logger,
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          ms,
		txTracker:      store.NewTxTracker(),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	entries := recorded.FilterMessage("transaction received").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'transaction received' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got := stringField(t, fields, "txid"); got != wantTxID {
		t.Errorf("txid = %q, want %q", got, wantTxID)
	}
	if got := stringField(t, fields, "status"); got != string(models.StatusReceived) {
		t.Errorf("status = %q, want %q", got, models.StatusReceived)
	}
}

// TestHandleSubmitTransactions_LogsReceivedBatch closes the RECEIVED log gap
// for the batch submit path: a successful POST /txs must emit at least one
// Info "transactions received" line whose txid_count reflects the TRUE
// total published, using ForEachTxIDChunk so a batch bigger than one chunk
// would still surface every txid across multiple lines.
func TestHandleSubmitTransactions_LogsReceivedBatch(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         logger,
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	// Concatenate 3 minimal transactions — store is nil so every parsed tx
	// flows straight to toPublish (mirrors TestHandleSubmitTransactions_BatchPublish).
	body := bytes.Repeat(makeMinimalTx(), 3)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}

	entries := recorded.FilterMessage("transactions received").All()
	if len(entries) == 0 {
		t.Fatal("expected at least 1 'transactions received' log line, got 0")
	}
	fields := entries[0].ContextMap()
	if got := int64Field(t, fields, "txid_count"); got != 3 {
		t.Errorf("txid_count = %d, want 3 (true total, not chunk size)", got)
	}
	if got := stringField(t, fields, "status"); got != string(models.StatusReceived) {
		t.Errorf("status = %q, want %q", got, models.StatusReceived)
	}
}

// TestApplySeenCallback_LogsTransactionsSeen closes the SEEN_ON_NETWORK /
// SEEN_MULTIPLE_NODES log gap: a callback that transitions a batch of
// txids must emit an Info "transactions seen" line carrying the target
// status and the batch's txid count/list.
func TestApplySeenCallback_LogsTransactionsSeen(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	ms := &mockStore{}
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         logger,
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          ms,
		publisher:      pub,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	const n = 10
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("seen-tx-%02d", i)
	}
	payload := models.CallbackMessage{Type: models.CallbackSeenOnNetwork, TxIDs: txids}
	req := authedCallbackRequest(t, mustMarshalJSON(t, payload))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	entries := recorded.FilterMessage("transactions seen").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'transactions seen' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got := stringField(t, fields, "status"); got != string(models.StatusSeenOnNetwork) {
		t.Errorf("status = %q, want %q", got, models.StatusSeenOnNetwork)
	}
	if got := int64Field(t, fields, "txid_count"); got != n {
		t.Errorf("txid_count = %d, want %d", got, n)
	}
}

// TestHandleStump_LogsStumpStored and TestHandleBlockProcessed_LogsEnqueued
// close the per-block STUMP/BLOCK_PROCESSED log gaps: cheap, per-block Info
// lines carrying block_hash so a block_hash search surfaces STUMP storage
// and BLOCK_PROCESSED enqueue events alongside the rest of the lifecycle.
func TestHandleStump_LogsStumpStored(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	ms := &mockStore{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         logger,
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          ms,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	payload := models.CallbackMessage{
		Type:         models.CallbackStump,
		BlockHash:    "blockhash-stump-test",
		SubtreeIndex: 3,
		Stump:        []byte{0x01, 0x02},
	}
	req := authedCallbackRequest(t, mustMarshalJSON(t, payload))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	entries := recorded.FilterMessage("stump stored").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'stump stored' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got := stringField(t, fields, "block_hash"); got != "blockhash-stump-test" {
		t.Errorf("block_hash = %q, want %q", got, "blockhash-stump-test")
	}
	if got := int64Field(t, fields, "subtree_index"); got != 3 {
		t.Errorf("subtree_index = %d, want 3", got)
	}
}

func TestHandleBlockProcessed_LogsEnqueued(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         logger,
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	payload := models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: "blockhash-processed-test",
	}
	req := authedCallbackRequest(t, mustMarshalJSON(t, payload))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	entries := recorded.FilterMessage("block_processed enqueued").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'block_processed enqueued' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got := stringField(t, fields, "block_hash"); got != "blockhash-processed-test" {
		t.Errorf("block_hash = %q, want %q", got, "blockhash-processed-test")
	}
}
