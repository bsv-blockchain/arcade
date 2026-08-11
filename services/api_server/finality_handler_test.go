package api_server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/finality"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

// fixedChainReader serves a static 11-block chain whose BIP113
// median-time-past is testChainMTP at tip height testChainTip.
type fixedChainReader struct{}

const (
	testChainTip = uint32(100)
	testChainMTP = uint32(1_600_000_005)
)

func (fixedChainReader) GetTip(context.Context) *chaintracks.BlockHeader {
	h := &chaintracks.BlockHeader{Header: &block.Header{Timestamp: 1_600_000_010}, Height: testChainTip}
	h.Hash = chainhash.HashH([]byte{1})
	return h
}

func (fixedChainReader) GetHeaders(_ context.Context, start, count uint32) ([]*chaintracks.BlockHeader, error) {
	headers := make([]*chaintracks.BlockHeader, 0, count)
	for i := uint32(0); i < count; i++ {
		headers = append(headers, &chaintracks.BlockHeader{
			Header: &block.Header{Timestamp: 1_600_000_000 + start + i},
			Height: start + i,
		})
	}
	return headers, nil
}

// makeLockTimeTx builds a parseable single-input tx with the given locktime
// and input sequence number.
func makeLockTimeTx(t *testing.T, lockTime, sequence uint32) []byte {
	t.Helper()
	tx := sdkTx.NewTransaction()
	tx.LockTime = lockTime
	sourceTxID := chainhash.HashH([]byte("parent"))
	tx.Inputs = append(tx.Inputs, &sdkTx.TransactionInput{
		SourceTXID:       &sourceTxID,
		SourceTxOutIndex: 0,
		UnlockingScript:  &script.Script{},
		SequenceNumber:   sequence,
	})
	return tx.Bytes()
}

func newFinalityTestServer(broker *kafka.RecordingBroker, ms *mockStore, pub *recordingCallbackPub) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		publisher:      pub,
		finality:       finality.NewChecker(fixedChainReader{}, zap.NewNop()),
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

// TestHandleSubmitTransaction_NonFinal_NoVerdictPersisted pins the intake
// finality gate's zero-state contract (issues #245/#254): a non-final tx is
// answered with an actionable 400 carrying the ARC 476 code, but NOTHING is
// persisted or published — no REJECTED row (GET /tx keeps returning 404,
// "no verdict", so resubmission policies keyed on no-verdict work), no
// status event, no Kafka publish. A non-final verdict reflects arcade's
// current chain view, not the transaction bytes, so it must never become a
// durable verdict that could stick after the tx mines elsewhere — nor
// clobber an existing in-flight row on a resubmit under a stale tip.
func TestHandleSubmitTransaction_NonFinal_NoVerdictPersisted(t *testing.T) {
	ms := &mockStore{}
	ms.getOrInsertFn = func(st *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		t.Errorf("GetOrInsertStatus must not be called for a non-final tx, got %+v", st)
		return nil, false, nil
	}
	pub := &recordingCallbackPub{}
	broker := &kafka.RecordingBroker{}
	_, router := newFinalityTestServer(broker, ms, pub)

	rawTx := makeLockTimeTx(t, 1_700_000_000, 0xfffffffe) // locktime > MTP
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "transaction is not final") {
		t.Errorf("response %q missing finality reason", w.Body.String())
	}
	// The ARC taxonomy rides alongside the legacy reason (additive).
	if !strings.Contains(w.Body.String(), `"status":476`) {
		t.Errorf("response %q missing ARC 476 status code", w.Body.String())
	}

	// Nothing may reach Kafka — the tx must not be broadcast.
	if got := totalMessages(broker); got != 0 {
		t.Errorf("expected 0 published Kafka messages for non-final tx, got %d", got)
	}

	// Zero persistence: no status row writes of any kind...
	ms.mu.Lock()
	updates := append([]*models.TransactionStatus(nil), ms.updateStatusCalls...)
	ms.mu.Unlock()
	if len(updates) != 0 {
		t.Errorf("expected no status writes for non-final tx, got %v", updates)
	}
	// ...and no status event for live subscribers: with no durable row a
	// published REJECTED would be a verdict GET could never corroborate.
	pub.mu.Lock()
	publishes := append([]*models.TransactionStatus(nil), pub.publishes...)
	pub.mu.Unlock()
	if len(publishes) != 0 {
		t.Fatalf("expected no publishes for non-final tx, got %v", publishes)
	}
}

// TestHandleSubmitTransaction_FinalLockTime_Proceeds proves the gate does not
// disturb final transactions: a locktime already below the chain MTP passes
// through to the normal accept/publish path.
func TestHandleSubmitTransaction_FinalLockTime_Proceeds(t *testing.T) {
	ms := &mockStore{}
	pub := &recordingCallbackPub{}
	broker := &kafka.RecordingBroker{}
	_, router := newFinalityTestServer(broker, ms, pub)

	rawTx := makeLockTimeTx(t, 1_500_000_000, 0xfffffffe) // locktime < MTP
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	if got := totalMessages(broker); got != 1 {
		t.Errorf("expected 1 published Kafka message, got %d", got)
	}
}

// TestHandleSubmitTransaction_NoFinalityChecker_Proceeds pins nil-safety:
// deployments without a chain source skip the gate entirely.
func TestHandleSubmitTransaction_NoFinalityChecker_Proceeds(t *testing.T) {
	ms := &mockStore{}
	broker := &kafka.RecordingBroker{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          ms,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	rawTx := makeLockTimeTx(t, 1_700_000_000, 0xfffffffe)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(rawTx))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 with nil finality checker, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandleSubmitTransactions_BatchNonFinal_AbortsWithTxid mirrors the
// batch contract for validation failures: the offending txid is named and
// the whole batch aborts before any publish — and, like the single-tx path,
// a non-final verdict persists and publishes nothing (zero-state contract).
func TestHandleSubmitTransactions_BatchNonFinal_AbortsWithTxid(t *testing.T) {
	ms := &mockStore{}
	ms.getOrInsertFn = func(st *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
		t.Errorf("GetOrInsertStatus must not be called for a non-final batch, got %+v", st)
		return nil, false, nil
	}
	pub := &recordingCallbackPub{}
	broker := &kafka.RecordingBroker{}
	_, router := newFinalityTestServer(broker, ms, pub)

	finalRaw := makeLockTimeTx(t, 0, 0xffffffff)
	nonFinalRaw := makeLockTimeTx(t, 1_700_000_000, 0xfffffffe)
	nonFinalTx, _, err := sdkTx.NewTransactionFromStream(nonFinalRaw)
	if err != nil {
		t.Fatalf("parse non-final tx: %v", err)
	}

	body := append(append([]byte(nil), finalRaw...), nonFinalRaw...)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), nonFinalTx.TxID().String()) {
		t.Errorf("response %q missing offending txid %s", w.Body.String(), nonFinalTx.TxID().String())
	}
	if !strings.Contains(w.Body.String(), "transaction is not final") {
		t.Errorf("response %q missing finality reason", w.Body.String())
	}
	if got := totalMessages(broker); got != 0 {
		t.Errorf("expected 0 published Kafka messages for aborted batch, got %d", got)
	}
	ms.mu.Lock()
	updates := len(ms.updateStatusCalls)
	ms.mu.Unlock()
	if updates != 0 {
		t.Errorf("expected no status writes for non-final batch, got %d", updates)
	}
	pub.mu.Lock()
	publishes := len(pub.publishes)
	pub.mu.Unlock()
	if publishes != 0 {
		t.Errorf("expected no publishes for non-final batch, got %d", publishes)
	}
}
