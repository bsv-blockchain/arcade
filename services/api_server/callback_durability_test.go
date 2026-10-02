package api_server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	"github.com/bsv-blockchain/arcade/store"
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

// fallbackSeenStore is the Postgres/Aerospike batch path: GetStatus then
// UpdateStatus inside BatchUpdateStatusReturningFallback. failTxIDs makes
// UpdateStatus fail without changing the row.
type fallbackSeenStore struct {
	mockStore
	mu          sync.Mutex
	rows        map[string]*models.TransactionStatus
	failTxIDs   map[string]bool
	updateCalls map[string]int
	// loseRaceTo, when set for a txid, makes UpdateStatus leave the row at
	// that status instead of applying the request. It models another writer
	// landing MINED between GetStatus and UpdateStatus.
	loseRaceTo map[string]models.Status
	tracker    *store.TxTracker
}

func (f *fallbackSeenStore) GetStatus(_ context.Context, txid string) (*models.TransactionStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row := f.rows[txid]
	if row == nil {
		return nil, store.ErrNotFound
	}
	cp := *row
	return &cp, nil
}

func (f *fallbackSeenStore) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	_, err := f.UpdateStatusReturning(ctx, status)
	return err
}

func (f *fallbackSeenStore) UpdateStatusReturning(_ context.Context, status *models.TransactionStatus) (*models.TransactionStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls[status.TxID]++
	if f.failTxIDs[status.TxID] {
		return nil, errors.New("fallback store write failed")
	}
	row := f.rows[status.TxID]
	if row == nil {
		return nil, store.ErrNotFound
	}
	prev := *row
	if landed, ok := f.loseRaceTo[status.TxID]; ok {
		cp := *row
		cp.Status = landed
		cp.Timestamp = time.Unix(0, 1)
		f.rows[status.TxID] = &cp
		if f.tracker != nil {
			f.tracker.UpdateStatus(status.TxID, landed)
		}
		return nil, nil
	}
	if status.Status != "" && !status.Status.CanTransitionFrom(row.Status) {
		return nil, nil
	}
	cp := *row
	cp.Status = status.Status
	cp.Timestamp = status.Timestamp
	f.rows[status.TxID] = &cp
	return &prev, nil
}

func (f *fallbackSeenStore) BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]*models.TransactionStatus, error) {
	return store.BatchUpdateStatusReturningFallback(ctx, f, statuses)
}

func (f *fallbackSeenStore) calls(txid string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateCalls[txid]
}

func (f *fallbackSeenStore) status(txid string) models.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	row := f.rows[txid]
	if row == nil {
		return ""
	}
	return row.Status
}

// TestHandleCallback_FallbackStoreFailure_DoesNotAdvanceTracker is the
// Postgres/Aerospike durability regression. A failed UpdateStatus must not
// look like a persisted transition: HTTP 500, tracker stays put, and the
// retry reaches the store again. The successful retry persists and publishes
// once.
func TestHandleCallback_FallbackStoreFailure_DoesNotAdvanceTracker(t *testing.T) {
	const txid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	st := &fallbackSeenStore{
		rows: map[string]*models.TransactionStatus{
			txid: {TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
		},
		failTxIDs:   map[string]bool{txid: true},
		updateCalls: map[string]int{},
	}
	tracker := store.NewTxTracker()
	tracker.Add(txid, models.StatusAcceptedByNetwork)
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          st,
		publisher:      pub,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)
	body := mustMarshalJSON(t, models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxIDs: []string{txid},
	})

	req := authedCallbackRequest(t, body)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("fallback store failure must be HTTP 500, got %d: %s", w.Code, w.Body.String())
	}
	if got, ok := tracker.GetStatus(txid); !ok || got != models.StatusAcceptedByNetwork {
		t.Fatalf("tracker after failed write = %s ok=%v, want ACCEPTED_BY_NETWORK", got, ok)
	}
	if st.status(txid) != models.StatusAcceptedByNetwork {
		t.Fatalf("store status after failed write = %s, want ACCEPTED_BY_NETWORK", st.status(txid))
	}
	if st.calls(txid) != 1 {
		t.Fatalf("UpdateStatus calls after failure = %d, want 1", st.calls(txid))
	}
	pub.mu.Lock()
	if len(pub.bulkPublishes) != 0 {
		t.Fatalf("failed write must not publish, got %d", len(pub.bulkPublishes))
	}
	pub.mu.Unlock()

	st.mu.Lock()
	st.failTxIDs[txid] = false
	st.mu.Unlock()
	req = authedCallbackRequest(t, body)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("retry must succeed, got %d: %s", w.Code, w.Body.String())
	}
	if st.calls(txid) != 2 {
		t.Fatalf("retry must reach the store again, UpdateStatus calls = %d, want 2", st.calls(txid))
	}
	if st.status(txid) != models.StatusSeenOnNetwork {
		t.Fatalf("store status after retry = %s, want SEEN_ON_NETWORK", st.status(txid))
	}
	if got, ok := tracker.GetStatus(txid); !ok || got != models.StatusSeenOnNetwork {
		t.Fatalf("tracker after retry = %s ok=%v, want SEEN_ON_NETWORK", got, ok)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.bulkPublishes) != 1 || len(pub.bulkPublishes[0].TxIDs) != 1 || pub.bulkPublishes[0].TxIDs[0] != txid {
		t.Fatalf("successful retry must publish once, got %+v", pub.bulkPublishes)
	}
}

// TestHandleCallback_FallbackPartialBatch_PublishesOnlyPersistedRows proves a
// mixed batch still publishes the row whose UpdateStatus succeeded, and does
// not advance the tracker for the row whose write failed.
func TestHandleCallback_FallbackPartialBatch_PublishesOnlyPersistedRows(t *testing.T) {
	const (
		okTx   = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		failTx = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	)
	now := time.Now()
	st := &fallbackSeenStore{
		rows: map[string]*models.TransactionStatus{
			okTx:   {TxID: okTx, Status: models.StatusAcceptedByNetwork, Timestamp: now},
			failTx: {TxID: failTx, Status: models.StatusAcceptedByNetwork, Timestamp: now},
		},
		failTxIDs:   map[string]bool{failTx: true},
		updateCalls: map[string]int{},
	}
	tracker := store.NewTxTracker()
	tracker.Add(okTx, models.StatusAcceptedByNetwork)
	tracker.Add(failTx, models.StatusAcceptedByNetwork)
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          st,
		publisher:      pub,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := authedCallbackRequest(t, mustMarshalJSON(t, models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxIDs: []string{okTx, failTx},
	}))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("partial fallback failure must be HTTP 500, got %d: %s", w.Code, w.Body.String())
	}
	if st.status(okTx) != models.StatusSeenOnNetwork {
		t.Fatalf("persisted row = %s, want SEEN_ON_NETWORK", st.status(okTx))
	}
	if st.status(failTx) != models.StatusAcceptedByNetwork {
		t.Fatalf("failed row = %s, want ACCEPTED_BY_NETWORK", st.status(failTx))
	}
	if got, _ := tracker.GetStatus(okTx); got != models.StatusSeenOnNetwork {
		t.Fatalf("tracker for persisted row = %s, want SEEN_ON_NETWORK", got)
	}
	if got, _ := tracker.GetStatus(failTx); got != models.StatusAcceptedByNetwork {
		t.Fatalf("tracker for failed row = %s, want ACCEPTED_BY_NETWORK", got)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.bulkPublishes) != 1 || len(pub.bulkPublishes[0].TxIDs) != 1 || pub.bulkPublishes[0].TxIDs[0] != okTx {
		t.Fatalf("publish must contain only the persisted tx, got %+v", pub.bulkPublishes)
	}
}

// TestRouteDocs_SeenMultipleNodesEnumMatchesCallbackType pins the wire enum.
// The callback type and the txStatus value are SEEN_MULTIPLE_NODES.
// SEEN_ON_MULTIPLE_NODES is not accepted.
func TestRouteDocs_SeenMultipleNodesEnumMatchesCallbackType(t *testing.T) {
	var docs strings.Builder
	var callbackDescription string
	for _, route := range routeDocs {
		docs.WriteString(route.Description)
		docs.WriteString(route.Notes)
		docs.WriteString(route.ResponseBody)
		for _, h := range route.Headers {
			docs.WriteString(h.Description)
		}
		for _, body := range route.RequestBodies {
			docs.WriteString(body.Description)
			docs.WriteString(body.Example)
		}
		if route.Path == "/api/v1/merkle-service/callback" {
			callbackDescription = route.Description + route.Notes
		}
	}
	if strings.Contains(docs.String(), "SEEN_ON_MULTIPLE_NODES") {
		t.Fatal("route docs contain SEEN_ON_MULTIPLE_NODES; the accepted enum is SEEN_MULTIPLE_NODES")
	}
	if !strings.Contains(callbackDescription, "SEEN_MULTIPLE_NODES") {
		t.Fatal("callback route docs must name SEEN_MULTIPLE_NODES")
	}
	if models.CallbackSeenMultipleNodes != "SEEN_MULTIPLE_NODES" || models.StatusSeenMultipleNodes != "SEEN_MULTIPLE_NODES" {
		t.Fatalf("enum drift: callback=%s status=%s", models.CallbackSeenMultipleNodes, models.StatusSeenMultipleNodes)
	}
}

// TestHandleCallback_StalePreimageRace_DoesNotRegressTracker is the
// ACCEPTED_BY_NETWORK read that loses to a concurrent MINED write. The SEEN
// update is skipped, nothing is published, and txTracker stays at MINED.
func TestHandleCallback_StalePreimageRace_DoesNotRegressTracker(t *testing.T) {
	const txid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	tracker := store.NewTxTracker()
	tracker.Add(txid, models.StatusAcceptedByNetwork)
	st := &fallbackSeenStore{
		rows: map[string]*models.TransactionStatus{
			txid: {TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Unix(0, 2)},
		},
		failTxIDs:   map[string]bool{},
		updateCalls: map[string]int{},
		loseRaceTo:  map[string]models.Status{txid: models.StatusMined},
		tracker:     tracker,
	}
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          st,
		publisher:      pub,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)
	req := authedCallbackRequest(t, mustMarshalJSON(t, models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxIDs: []string{txid},
	}))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("a lattice skip is not a store failure, got %d: %s", w.Code, w.Body.String())
	}
	if st.status(txid) != models.StatusMined {
		t.Fatalf("store status = %s, want MINED", st.status(txid))
	}
	got, ok := tracker.GetStatus(txid)
	if !ok || (got != models.StatusMined && got != models.StatusImmutable) {
		t.Fatalf("tracker = %s ok=%v, want MINED or higher", got, ok)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.bulkPublishes) != 0 || len(pub.publishes) != 0 {
		t.Fatalf("lost SEEN race must not publish, bulk=%d per-tx=%d", len(pub.bulkPublishes), len(pub.publishes))
	}
}

// TestHandleCallback_ConcurrentDuplicate_PublishesOnce proves two SEEN
// callbacks for the same tx cannot both fan out the ACCEPTED→SEEN
// transition. Pebble serializes the applied-result, so the second call
// observes a previous row already at SEEN.
func TestHandleCallback_ConcurrentDuplicate_PublishesOnce(t *testing.T) {
	st, err := pebble.New(config.Pebble{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	const txid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	ctx := context.Background()
	if _, _, err := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID: txid, Status: models.StatusAcceptedByNetwork, Timestamp: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	tracker := store.NewTxTracker()
	tracker.Add(txid, models.StatusAcceptedByNetwork)
	pub := &recordingCallbackPub{}
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:            &config.Config{CallbackToken: testCallbackToken},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(&kafka.RecordingBroker{}),
		store:          st,
		publisher:      pub,
		txTracker:      tracker,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	router := gin.New()
	srv.registerRoutes(router)
	body := mustMarshalJSON(t, models.CallbackMessage{
		Type:  models.CallbackSeenOnNetwork,
		TxIDs: []string{txid},
	})

	var wg sync.WaitGroup
	codes := make([]int, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req := authedCallbackRequest(t, body)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			codes[i] = w.Code
		}(i)
	}
	close(start)
	wg.Wait()
	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("callback %d: status %d, want 200", i, code)
		}
	}
	got, err := st.GetStatus(ctx, txid)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Status != models.StatusSeenOnNetwork {
		t.Fatalf("stored status = %+v, want SEEN_ON_NETWORK", got)
	}
	tracked, ok := tracker.GetStatus(txid)
	if !ok || (tracked != models.StatusSeenOnNetwork && tracked != models.StatusSeenMultipleNodes && tracked != models.StatusMined && tracked != models.StatusImmutable) {
		t.Fatalf("tracker = %s ok=%v, want SEEN_ON_NETWORK or higher", tracked, ok)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.bulkPublishes) != 1 {
		t.Fatalf("expected one PublishBulk, got %d", len(pub.bulkPublishes))
	}
}
