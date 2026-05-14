package api_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

// TestHandleSubmitTransaction_PublishesToDispatchTopic verifies that the
// new intake handler publishes to kafka.TopicDispatch (not the legacy
// TopicTransaction) and that the envelope carries the input_txids
// extracted from the parsed transaction.
func TestHandleSubmitTransaction_PublishesToDispatchTopic(t *testing.T) {
	tx := makeRealTx(t)
	legacy := tx.Bytes()
	canonical := tx.TxID().String()

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	srv, router := setupServerWithStore(broker, ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(legacy))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	drainSubmissions(t, srv)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	if len(broker.Sends) != 1 {
		t.Fatalf("expected 1 Send, got %d", len(broker.Sends))
	}
	send := broker.Sends[0]

	if send.Topic != kafka.TopicDispatch {
		t.Errorf("topic: want %s, got %s", kafka.TopicDispatch, send.Topic)
	}
	if send.Key != canonical {
		t.Errorf("kafka key: want %s, got %s", canonical, send.Key)
	}

	// The published value is a JSON-encoded envelope with txid, raw_tx,
	// and input_txids. Decode it back and confirm shape.
	var envelope struct {
		TXID       string   `json:"txid"`
		RawTx      []byte   `json:"raw_tx"`
		InputTXIDs []string `json:"input_txids"`
	}
	if err := json.Unmarshal(send.Value, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v (value=%s)", err, send.Value)
	}
	if envelope.TXID != canonical {
		t.Errorf("envelope txid: want %s, got %s", canonical, envelope.TXID)
	}
	if !bytes.Equal(envelope.RawTx, legacy) {
		t.Errorf("envelope raw_tx differs from request body")
	}
	if len(envelope.InputTXIDs) != 1 {
		t.Fatalf("expected 1 input_txid, got %d: %+v", len(envelope.InputTXIDs), envelope.InputTXIDs)
	}
	// The test tx in makeRealTx funds from "0000...0001".
	if envelope.InputTXIDs[0] != "0000000000000000000000000000000000000000000000000000000000000001" {
		t.Errorf("input_txid: got %s", envelope.InputTXIDs[0])
	}
}

// TestHandleSubmitTransaction_DedupExistingReturnsIdempotent verifies
// that when GetOrInsertStatus reports an existing row, the handler
// returns 202 with the row's current status and does NOT publish to
// Kafka.
func TestHandleSubmitTransaction_DedupExistingReturnsIdempotent(t *testing.T) {
	tx := makeRealTx(t)
	legacy := tx.Bytes()
	canonical := tx.TxID().String()

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{
		getOrInsertStatusFn: func(in *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return &models.TransactionStatus{
				TxID:   in.TxID,
				Status: models.StatusSentToNetwork,
			}, false, nil
		},
	}
	_, router := setupServerWithStore(broker, ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(legacy))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 idempotent, got %d: %s", w.Code, w.Body.String())
	}

	var body struct {
		Status string `json:"status"`
		TxID   string `json:"txid"`
		State  string `json:"state"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "already submitted" {
		t.Errorf("status field: want 'already submitted', got %q", body.Status)
	}
	if body.TxID != canonical {
		t.Errorf("txid: want %s, got %s", canonical, body.TxID)
	}
	if body.State != string(models.StatusSentToNetwork) {
		t.Errorf("state: want %s, got %s", models.StatusSentToNetwork, body.State)
	}

	if len(broker.Sends) != 0 {
		t.Errorf("dedup hit should not publish to Kafka, got %d sends", len(broker.Sends))
	}
}

// TestHandleSubmitTransaction_StoreErrorContinues verifies that a
// transient store error on the dedup CAS doesn't block the publish —
// the handler logs and proceeds, leaving the dispatcher to catch
// duplicates via its in-flight set.
func TestHandleSubmitTransaction_StoreErrorContinues(t *testing.T) {
	tx := makeRealTx(t)
	legacy := tx.Bytes()

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{
		getOrInsertStatusFn: func(*models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return nil, false, errors.New("transient store error")
		},
	}
	srv, router := setupServerWithStore(broker, ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(legacy))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	drainSubmissions(t, srv)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 despite store error, got %d: %s", w.Code, w.Body.String())
	}
	if len(broker.Sends) != 1 {
		t.Errorf("expected 1 Send despite store error, got %d", len(broker.Sends))
	}
}

// TestHandleSubmitTransaction_NotInsertedNilExisting_ProceedsToPublish
// covers the pathological case where the store reports inserted=false
// with a nil existing row. The handler logs and proceeds — without
// this defensive branch, the dereference would crash.
func TestHandleSubmitTransaction_NotInsertedNilExisting_ProceedsToPublish(t *testing.T) {
	tx := makeRealTx(t)
	legacy := tx.Bytes()

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{
		getOrInsertStatusFn: func(*models.TransactionStatus) (*models.TransactionStatus, bool, error) {
			return nil, false, nil
		},
	}
	srv, router := setupServerWithStore(broker, ms)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(legacy))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	drainSubmissions(t, srv)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on pathological dedup result, got %d: %s", w.Code, w.Body.String())
	}
	if len(broker.Sends) != 1 {
		t.Errorf("expected publish to proceed, got %d sends", len(broker.Sends))
	}
}
