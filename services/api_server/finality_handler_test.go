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

// TestHandleSubmitTransaction_NonFinal_RejectedWithReason pins the intake
// finality gate (issue #245): a tx whose timestamp locktime is ahead of the
// chain MTP with a non-final input sequence must be rejected synchronously
// with an actionable reason, persist a REJECTED row with that reason, and
// never reach the propagation topic.
func TestHandleSubmitTransaction_NonFinal_RejectedWithReason(t *testing.T) {
	ms := &mockStore{}
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

	// Nothing may reach Kafka — the tx must not be broadcast.
	if got := totalMessages(broker); got != 0 {
		t.Errorf("expected 0 published Kafka messages for non-final tx, got %d", got)
	}

	// A terminal REJECTED row with the finality reason must be persisted.
	ms.mu.Lock()
	calls := append([]*models.TransactionStatus(nil), ms.updateStatusCalls...)
	ms.mu.Unlock()
	var sawReject bool
	for _, c := range calls {
		if c.Status == models.StatusRejected && strings.Contains(c.ExtraInfo, "transaction is not final") {
			sawReject = true
			break
		}
	}
	if !sawReject {
		t.Errorf("expected REJECTED row carrying the finality reason; UpdateStatus calls=%v", calls)
	}

	// Live subscribers must see the terminal outcome with the reason.
	pub.mu.Lock()
	publishes := append([]*models.TransactionStatus(nil), pub.publishes...)
	pub.mu.Unlock()
	if len(publishes) != 1 || publishes[0].Status != models.StatusRejected {
		t.Fatalf("expected exactly 1 REJECTED publish, got %v", publishes)
	}
	if !strings.Contains(publishes[0].ExtraInfo, "transaction is not final") {
		t.Errorf("publish ExtraInfo %q missing finality reason", publishes[0].ExtraInfo)
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
// the whole batch aborts before any publish.
func TestHandleSubmitTransactions_BatchNonFinal_AbortsWithTxid(t *testing.T) {
	ms := &mockStore{}
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
}
