package api_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/pebble"
)

// With merkle-services B, C and D all watching the same tx, each SEEN state
// arrives three times. The first SEEN_ON_NETWORK transitions and publishes;
// the other two are no-ops. The first SEEN_MULTIPLE_NODES (from whichever
// service gets there first) transitions and publishes; the other two are
// no-ops. A late SEEN_ON_NETWORK after that neither regresses the row nor
// publishes.
func TestHandleCallback_MultiSourceSeen_PublishesEachTransitionOnce(t *testing.T) {
	st, err := pebble.New(config.Pebble{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	const txid = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
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

	deliver := func(cb models.CallbackType) {
		t.Helper()
		req := authedCallbackRequest(t, mustMarshalJSON(t, models.CallbackMessage{Type: cb, TxIDs: []string{txid}}))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status %d, want 200: %s", cb, w.Code, w.Body.String())
		}
	}
	bulkCount := func() int {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return len(pub.bulkPublishes)
	}
	status := func() models.Status {
		t.Helper()
		got, err := st.GetStatus(ctx, txid)
		if err != nil || got == nil {
			t.Fatalf("GetStatus: %v %v", got, err)
		}
		return got.Status
	}

	// B, C, D: SEEN_ON_NETWORK.
	for i := 0; i < 3; i++ {
		deliver(models.CallbackSeenOnNetwork)
	}
	if got := bulkCount(); got != 1 {
		t.Fatalf("after 3x SEEN_ON_NETWORK publishes=%d want 1", got)
	}
	if got := status(); got != models.StatusSeenOnNetwork {
		t.Fatalf("status=%s want SEEN_ON_NETWORK", got)
	}

	// C first, then D and B: SEEN_MULTIPLE_NODES.
	for i := 0; i < 3; i++ {
		deliver(models.CallbackSeenMultipleNodes)
	}
	if got := bulkCount(); got != 2 {
		t.Fatalf("after 3x SEEN_MULTIPLE_NODES publishes=%d want 2", got)
	}
	if got := status(); got != models.StatusSeenMultipleNodes {
		t.Fatalf("status=%s want SEEN_MULTIPLE_NODES", got)
	}
	pub.mu.Lock()
	if pub.bulkPublishes[0].Status != models.StatusSeenOnNetwork || pub.bulkPublishes[1].Status != models.StatusSeenMultipleNodes {
		t.Fatalf("publish order = %s, %s", pub.bulkPublishes[0].Status, pub.bulkPublishes[1].Status)
	}
	pub.mu.Unlock()

	// A straggling SEEN_ON_NETWORK from a slow service.
	deliver(models.CallbackSeenOnNetwork)
	if got := bulkCount(); got != 2 {
		t.Fatalf("late SEEN_ON_NETWORK must not publish, publishes=%d", got)
	}
	if got := status(); got != models.StatusSeenMultipleNodes {
		t.Fatalf("late SEEN_ON_NETWORK regressed the row to %s", got)
	}
	if tracked, ok := tracker.GetStatus(txid); !ok || tracked != models.StatusSeenMultipleNodes {
		t.Fatalf("tracker = %s ok=%v, want SEEN_MULTIPLE_NODES", tracked, ok)
	}
}

// /health lists every configured merkle-service endpoint with its breaker
// state, and omits the key entirely when the integration is disabled.
func TestHandleHealth_MerkleEndpoints(t *testing.T) {
	pool := merkleservice.NewPool([]string{"http://merkle-a.example", "http://merkle-b.example"}, "", time.Second)
	srv := &Server{
		cfg:          &config.Config{},
		logger:       zap.NewNop(),
		merkleClient: pool,
	}
	code, _, raw := doHealth(t, srv)
	if code != http.StatusOK {
		t.Fatalf("status=%d", code)
	}
	var body struct {
		MerkleEndpoints []merkleservice.EndpointStatus `json:"merkle_endpoints"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.MerkleEndpoints) != 2 {
		t.Fatalf("merkle_endpoints = %+v, want 2 entries", body.MerkleEndpoints)
	}
	for _, ep := range body.MerkleEndpoints {
		if !ep.Healthy {
			t.Fatalf("fresh pool endpoint must be healthy: %+v", ep)
		}
	}

	var keys map[string]json.RawMessage
	_, _, raw = doHealth(t, &Server{cfg: &config.Config{}, logger: zap.NewNop()})
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatal(err)
	}
	if _, present := keys["merkle_endpoints"]; present {
		t.Fatal("merkle_endpoints must be omitted when merkle-service is not configured")
	}
}
