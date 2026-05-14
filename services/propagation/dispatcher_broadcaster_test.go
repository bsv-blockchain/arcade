package propagation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// broadcastTestStore captures BatchUpdateStatus calls so tests can
// assert the broadcaster wrote the expected terminal rows. Embeds the
// existing mockStore (defined in propagator_test.go) so the full Store
// interface is satisfied; we override just the methods the broadcaster
// actually exercises.
type broadcastTestStore struct {
	*mockStore

	mu      sync.Mutex
	updates []*models.TransactionStatus
}

func newBroadcastTestStore() *broadcastTestStore {
	return &broadcastTestStore{mockStore: newMockStore()}
}

func (s *broadcastTestStore) BatchUpdateStatus(_ context.Context, statuses []*models.TransactionStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range statuses {
		// Copy so the caller mutating the slice element later (it doesn't,
		// but defense-in-depth) can't corrupt the recorded value.
		cp := *st
		s.updates = append(s.updates, &cp)
	}
	return nil
}

func (s *broadcastTestStore) snapshot() []*models.TransactionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*models.TransactionStatus, len(s.updates))
	copy(out, s.updates)
	return out
}

// TestDispatcherBroadcaster_BatchAccepted verifies the happy path: a
// multi-tx batch goes to /txs, returns 200, every tx gets an
// ACCEPTED_BY_NETWORK status flip and a store write.
func TestDispatcherBroadcaster_BatchAccepted(t *testing.T) {
	var txsHits, txHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/txs":
			atomic.AddInt32(&txsHits, 1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		case "/tx":
			atomic.AddInt32(&txHits, 1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	store := newBroadcastTestStore()
	outBatch := make(chan []*inFlightEntry, 1)
	flips := make(chan *models.TransactionStatus, 4)

	b, err := newDispatcherBroadcaster(dispatcherBroadcasterConfig{
		TeranodeClient: tc,
		Store:          store,
		Incoming:       outBatch,
		Flips:          flips,
		Logger:         zap.NewNop(),
		SubmitTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("newDispatcherBroadcaster: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)

	outBatch <- []*inFlightEntry{
		{txid: "tx1", rawTx: []byte{0xaa}},
		{txid: "tx2", rawTx: []byte{0xbb}},
	}

	seen := map[string]models.Status{}
	for i := 0; i < 2; i++ {
		select {
		case f := <-flips:
			seen[f.TxID] = f.Status
		case <-time.After(2 * time.Second):
			t.Fatalf("only saw %d/2 flips: %+v", len(seen), seen)
		}
	}
	if seen["tx1"] != models.StatusAcceptedByNetwork || seen["tx2"] != models.StatusAcceptedByNetwork {
		t.Errorf("expected both tx1/tx2 ACCEPTED, got %+v", seen)
	}
	if atomic.LoadInt32(&txsHits) != 1 {
		t.Errorf("expected one /txs call, got %d", txsHits)
	}
	if atomic.LoadInt32(&txHits) != 0 {
		t.Errorf("expected zero /tx fallback calls, got %d", txHits)
	}

	updates := store.snapshot()
	if len(updates) != 2 {
		t.Errorf("expected 2 store updates, got %d: %+v", len(updates), updates)
	}
	for _, u := range updates {
		if u.Status != models.StatusAcceptedByNetwork {
			t.Errorf("store update for %s has status %s, want ACCEPTED_BY_NETWORK", u.TxID, u.Status)
		}
	}
}

// TestDispatcherBroadcaster_BatchRejected_FallsBackPerTx verifies the
// fallback path: /txs returns 500, the broadcaster fans out to /tx for
// each tx so per-tx status codes are recoverable.
func TestDispatcherBroadcaster_BatchRejected_FallsBackPerTx(t *testing.T) {
	var txsHits, txHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/txs":
			atomic.AddInt32(&txsHits, 1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("batch rejected"))
		case "/tx":
			atomic.AddInt32(&txHits, 1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("invalid tx"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	store := newBroadcastTestStore()
	outBatch := make(chan []*inFlightEntry, 1)
	flips := make(chan *models.TransactionStatus, 4)

	b, err := newDispatcherBroadcaster(dispatcherBroadcasterConfig{
		TeranodeClient: tc,
		Store:          store,
		Incoming:       outBatch,
		Flips:          flips,
		Logger:         zap.NewNop(),
		SubmitTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("newDispatcherBroadcaster: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)

	outBatch <- []*inFlightEntry{
		{txid: "txA", rawTx: []byte{0xaa}},
		{txid: "txB", rawTx: []byte{0xbb}},
	}

	rejectedTxs := map[string]int{}
	for i := 0; i < 2; i++ {
		select {
		case f := <-flips:
			if f.Status != models.StatusRejected {
				t.Errorf("expected REJECTED for %s, got %s", f.TxID, f.Status)
			}
			rejectedTxs[f.TxID] = f.StatusCode
		case <-time.After(2 * time.Second):
			t.Fatalf("only saw %d/2 flips", len(rejectedTxs))
		}
	}
	if rejectedTxs["txA"] != http.StatusBadRequest || rejectedTxs["txB"] != http.StatusBadRequest {
		t.Errorf("expected both rejected at 400, got %+v", rejectedTxs)
	}
	if atomic.LoadInt32(&txsHits) != 1 {
		t.Errorf("expected one /txs attempt, got %d", txsHits)
	}
	if atomic.LoadInt32(&txHits) != 2 {
		t.Errorf("expected per-tx fallback to call /tx twice, got %d", txHits)
	}
}

// TestDispatcherBroadcaster_SingleTx verifies the single-tx batch
// path goes directly to /tx without trying /txs.
func TestDispatcherBroadcaster_SingleTx(t *testing.T) {
	var txsHits, txHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/txs":
			atomic.AddInt32(&txsHits, 1)
			w.WriteHeader(http.StatusOK)
		case "/tx":
			atomic.AddInt32(&txHits, 1)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	store := newBroadcastTestStore()
	outBatch := make(chan []*inFlightEntry, 1)
	flips := make(chan *models.TransactionStatus, 1)

	b, err := newDispatcherBroadcaster(dispatcherBroadcasterConfig{
		TeranodeClient: tc,
		Store:          store,
		Incoming:       outBatch,
		Flips:          flips,
		Logger:         zap.NewNop(),
		SubmitTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("newDispatcherBroadcaster: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)

	outBatch <- []*inFlightEntry{{txid: "soloTx", rawTx: []byte{0xcd}}}

	select {
	case f := <-flips:
		if f.TxID != "soloTx" || f.Status != models.StatusAcceptedByNetwork {
			t.Errorf("expected soloTx ACCEPTED, got %+v", f)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no flip received for single-tx batch")
	}
	if atomic.LoadInt32(&txsHits) != 0 {
		t.Errorf("single-tx batch should not call /txs, got %d hits", txsHits)
	}
	if atomic.LoadInt32(&txHits) != 1 {
		t.Errorf("expected one /tx call, got %d", txHits)
	}
}

// TestDispatcherBroadcaster_NoHealthyEndpoints verifies that if no
// healthy endpoints are available the broadcaster emits a REJECTED flip
// with statusCode 0 — which the dispatcher classifies as retryable.
func TestDispatcherBroadcaster_NoHealthyEndpoints(t *testing.T) {
	// Use a teranode client with no endpoints so GetHealthyEndpoints
	// returns empty.
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1})

	store := newBroadcastTestStore()
	outBatch := make(chan []*inFlightEntry, 1)
	flips := make(chan *models.TransactionStatus, 1)

	b, err := newDispatcherBroadcaster(dispatcherBroadcasterConfig{
		TeranodeClient: tc,
		Store:          store,
		Incoming:       outBatch,
		Flips:          flips,
		Logger:         zap.NewNop(),
		SubmitTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("newDispatcherBroadcaster: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)

	outBatch <- []*inFlightEntry{{txid: "orphan", rawTx: []byte{0x00}}}

	select {
	case f := <-flips:
		if f.Status != models.StatusRejected {
			t.Errorf("expected REJECTED, got %s", f.Status)
		}
		if f.StatusCode != 0 {
			t.Errorf("expected statusCode=0 (retryable signal), got %d", f.StatusCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no flip received")
	}
}

// TestDispatcherBroadcaster_MerkleRegisterFailure_RetryFlip verifies
// that a merkle-service registration failure produces a retryable
// statusFlip (statusCode=0) and does NOT broadcast the failing tx to
// Teranode. The successful tx in the same batch still broadcasts.
func TestDispatcherBroadcaster_MerkleRegisterFailure_RetryFlip(t *testing.T) {
	// Teranode emulator — count how many times it was hit so we can
	// confirm the failed-registration tx never reached it.
	var teranodeHits int32
	teranodeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&teranodeHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(teranodeSrv.Close)
	tc := teranode.NewClient([]string{teranodeSrv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})

	// Merkle-service emulator — returns 500 for txid containing "fail",
	// 200 otherwise. The /watch endpoint receives a JSON payload with
	// the txid inside.
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 256)
		n, _ := r.Body.Read(body)
		if strings.Contains(string(body[:n]), "fail") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(merkleSrv.Close)
	mc := merkleservice.NewClient(merkleSrv.URL, "", 2*time.Second)

	store := newBroadcastTestStore()
	outBatch := make(chan []*inFlightEntry, 1)
	flips := make(chan *models.TransactionStatus, 4)

	b, err := newDispatcherBroadcaster(dispatcherBroadcasterConfig{
		TeranodeClient:    tc,
		Store:             store,
		Incoming:          outBatch,
		Flips:             flips,
		Logger:            zap.NewNop(),
		SubmitTimeout:     time.Second,
		MerkleClient:      mc,
		MerkleConcurrency: 4,
		MerkleCallbackURL: "http://test/callback",
	})
	if err != nil {
		t.Fatalf("newDispatcherBroadcaster: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)

	outBatch <- []*inFlightEntry{
		{txid: "okTx", rawTx: []byte{0xaa}},
		{txid: "failTx", rawTx: []byte{0xbb}},
	}

	// Two flips expected: ACCEPTED for okTx (broadcast), retryable
	// REJECTED for failTx (merkle register failed). Order is not
	// guaranteed.
	flipsByTxID := map[string]*models.TransactionStatus{}
	for i := 0; i < 2; i++ {
		select {
		case f := <-flips:
			flipsByTxID[f.TxID] = f
		case <-time.After(3 * time.Second):
			t.Fatalf("only saw %d/2 flips: %+v", len(flipsByTxID), flipsByTxID)
		}
	}

	okFlip, ok := flipsByTxID["okTx"]
	if !ok || okFlip.Status != models.StatusAcceptedByNetwork {
		t.Errorf("okTx flip: want ACCEPTED, got %+v", okFlip)
	}

	failFlip, ok := flipsByTxID["failTx"]
	if !ok || failFlip.Status != models.StatusRejected {
		t.Errorf("failTx flip: want REJECTED, got %+v", failFlip)
	}
	if failFlip.StatusCode != 0 {
		t.Errorf("failTx statusCode: want 0 (retryable), got %d", failFlip.StatusCode)
	}
	if !strings.Contains(failFlip.ExtraInfo, "merkle register failed") {
		t.Errorf("failTx errorMsg should mention merkle register failure, got %q", failFlip.ExtraInfo)
	}

	// Only the successful tx should have been broadcast — /tx (single)
	// because after registration filtering the batch is size 1.
	if got := atomic.LoadInt32(&teranodeHits); got != 1 {
		t.Errorf("expected exactly 1 Teranode submission (okTx only), got %d", got)
	}
}
