package api_server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store/pebble"
)

// TestHandleCallback_SeenStoreFailure_ReturnsRetriable500 is the regression
// for a SEEN callback whose store write fails. Merkle treats HTTP 2xx as
// delivered and does not retry. A persistence failure must therefore be a
// retriable 5xx, matching the STUMP storage-error contract, not a 200.
func TestHandleCallback_SeenStoreFailure_ReturnsRetriable500(t *testing.T) {
	cases := []models.CallbackType{
		models.CallbackSeenOnNetwork,
		models.CallbackSeenMultipleNodes,
	}
	for _, cbType := range cases {
		t.Run(string(cbType), func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			ms := &mockStore{batchUpdateReturningErr: errors.New("pebble: write stall")}
			gin.SetMode(gin.TestMode)
			srv := &Server{
				cfg:            &config.Config{CallbackToken: testCallbackToken},
				logger:         zap.New(core),
				producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
				store:          ms,
				submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
				submissionStop: make(chan struct{}),
			}
			router := gin.New()
			srv.registerRoutes(router)

			before := histogramSampleCount(t, metrics.CallbackHandlerDuration.WithLabelValues(string(cbType), "error"))

			payload := models.CallbackMessage{
				Type:  cbType,
				TxIDs: []string{"tx-persist-fail"},
			}
			req := authedCallbackRequest(t, mustMarshalJSON(t, payload))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusInternalServerError {
				t.Fatalf("store failure must be a retriable 500 so Merkle retries, got %d: %s", w.Code, w.Body.String())
			}
			if got := w.Body.String(); !strings.Contains(got, "failed to store seen status") {
				t.Fatalf("body = %q, want failed to store seen status", got)
			}
			after := histogramSampleCount(t, metrics.CallbackHandlerDuration.WithLabelValues(string(cbType), "error"))
			if after != before+1 {
				t.Fatalf("CallbackHandlerDuration{type=%s,outcome=error} count = %d, want %d", cbType, after, before+1)
			}
			if logs.FilterMessage("batch update seen status failed").Len() != 1 {
				t.Fatalf("expected one error log for the failed store write, got %d", logs.Len())
			}
			if len(ms.updateStatusCalls) != 0 {
				t.Fatalf("a failed batch write must not record a successful status update, got %d", len(ms.updateStatusCalls))
			}
		})
	}
}

func histogramSampleCount(t *testing.T, obs prometheus.Observer) uint64 {
	t.Helper()
	h, ok := obs.(prometheus.Histogram)
	if !ok {
		t.Fatalf("observer is %T, want histogram", obs)
	}
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("histogram write: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestHandleCallback_SeenTwice_IsIdempotent delivers the same SEEN callback
// twice through the real handler and a Pebble store. The second delivery
// must stay 200, leave the stored status where the first write put it, and
// publish the transition only once. Merkle retries after a 500, and a
// duplicate of an already-applied callback must not regress the row or fan
// the transition out again.
func TestHandleCallback_SeenTwice_IsIdempotent(t *testing.T) {
	cases := []struct {
		cbType models.CallbackType
		want   models.Status
	}{
		{models.CallbackSeenOnNetwork, models.StatusSeenOnNetwork},
		{models.CallbackSeenMultipleNodes, models.StatusSeenMultipleNodes},
	}
	for _, tc := range cases {
		t.Run(string(tc.cbType), func(t *testing.T) {
			st, err := pebble.New(config.Pebble{Path: t.TempDir()})
			if err != nil {
				t.Fatalf("pebble.New: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })

			const txid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			ctx := context.Background()
			if _, _, err := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
				TxID:      txid,
				Status:    models.StatusAcceptedByNetwork,
				Timestamp: time.Now(),
			}); err != nil {
				t.Fatalf("GetOrInsertStatus: %v", err)
			}

			pub := &recordingCallbackPub{}
			gin.SetMode(gin.TestMode)
			srv := &Server{
				cfg:            &config.Config{CallbackToken: testCallbackToken},
				logger:         zap.NewNop(),
				producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
				store:          st,
				publisher:      pub,
				submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
				submissionStop: make(chan struct{}),
			}
			router := gin.New()
			srv.registerRoutes(router)

			payload := mustMarshalJSON(t, models.CallbackMessage{
				Type:  tc.cbType,
				TxIDs: []string{txid},
			})
			for delivery := 1; delivery <= 2; delivery++ {
				req := authedCallbackRequest(t, payload)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != http.StatusOK {
					t.Fatalf("delivery %d: status %d, want 200: %s", delivery, w.Code, w.Body.String())
				}
			}

			got, err := st.GetStatus(ctx, txid)
			if err != nil {
				t.Fatalf("GetStatus: %v", err)
			}
			if got == nil || got.Status != tc.want {
				t.Fatalf("stored status = %v, want %s", got, tc.want)
			}

			pub.mu.Lock()
			defer pub.mu.Unlock()
			if len(pub.publishes) != 0 {
				t.Fatalf("expected no per-tx Publish, got %d", len(pub.publishes))
			}
			if len(pub.bulkPublishes) != 1 {
				t.Fatalf("expected exactly one PublishBulk for two deliveries, got %d", len(pub.bulkPublishes))
			}
			if pub.bulkPublishes[0].Status != tc.want || len(pub.bulkPublishes[0].TxIDs) != 1 || pub.bulkPublishes[0].TxIDs[0] != txid {
				t.Fatalf("bulk publish = %+v", pub.bulkPublishes[0])
			}
		})
	}
}

// TestHandleCallback_UnknownType_Acknowledged pins the decision to keep 200
// for a callback type this build does not implement. Merkle retries non-2xx,
// so a 5xx would retry forever, and a 4xx can permanently reject the
// delivery. Nothing is written.
func TestHandleCallback_UnknownType_Acknowledged(t *testing.T) {
	ms := &mockStore{}
	_, router := setupServerWithStore(&kafka.RecordingBroker{}, ms)

	payload := models.CallbackMessage{Type: "FUTURE_CALLBACK", TxIDs: []string{"tx-unknown-type"}}
	req := authedCallbackRequest(t, mustMarshalJSON(t, payload))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("unknown callback type must stay 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(ms.updateStatusCalls) != 0 || len(ms.batchUpdateReturningCalls) != 0 {
		t.Fatalf("unknown type must not touch the store, updates=%d batches=%d", len(ms.updateStatusCalls), len(ms.batchUpdateReturningCalls))
	}
}
