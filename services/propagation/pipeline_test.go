package propagation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// pipelineTestStore is the smallest Store impl that satisfies the
// pipeline's needs: capture writes from the broadcaster's terminal
// status path. Other methods inherit from the package's mockStore so
// the full Store interface is implemented.
type pipelineTestStore struct {
	*mockStore

	mu      sync.Mutex
	updates []*models.TransactionStatus
}

func newPipelineTestStore() *pipelineTestStore {
	return &pipelineTestStore{mockStore: newMockStore()}
}

func (s *pipelineTestStore) BatchUpdateStatus(_ context.Context, statuses []*models.TransactionStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range statuses {
		cp := *st
		s.updates = append(s.updates, &cp)
	}
	return nil
}

func (s *pipelineTestStore) byTxID() map[string]models.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]models.Status, len(s.updates))
	for _, u := range s.updates {
		out[u.TxID] = u.Status
	}
	return out
}

// TestPipeline_EndToEnd_AcceptedBatch starts the full pipeline against
// an in-memory broker and an HTTP server that mimics Teranode's
// happy-path /txs response. A producer publishes two txs to
// TopicDispatch; both should land in the broadcaster's batch, hit
// Teranode's /txs once, and produce ACCEPTED_BY_NETWORK status rows
// in the store.
func TestPipeline_EndToEnd_AcceptedBatch(t *testing.T) {
	var txsHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/txs" {
			atomic.AddInt32(&txsHits, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	store := newPipelineTestStore()
	broker := kafka.NewMemoryBroker(0)
	t.Cleanup(func() { _ = broker.Close() })
	producer := kafka.NewProducer(broker)

	cfg := &config.Config{}
	cfg.Kafka.ConsumerGroup = "test-pipeline-accept"
	cfg.Propagation.TeranodeMaxBatchSize = 4 // small so we don't have to send many
	cfg.Propagation.RetryMaxAttempts = 3
	cfg.Propagation.RetryBackoffMs = 50

	p, err := NewPipeline(cfg, zap.NewNop(), producer, store, tc, nil)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startErr := make(chan error, 1)
	go func() { startErr <- p.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = p.Stop()
		<-startErr
	})

	// Give the consumer a moment to subscribe before producing.
	time.Sleep(100 * time.Millisecond)

	for _, txid := range []string{"tx1", "tx2"} {
		envelope := propagationMsg{TXID: txid, RawTx: []byte{0xaa}, InputTXIDs: nil}
		if err := producer.Send(kafka.TopicDispatch, txid, envelope); err != nil {
			t.Fatalf("producer.Send: %v", err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.byTxID()) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	got := store.byTxID()
	if got["tx1"] != models.StatusAcceptedByNetwork || got["tx2"] != models.StatusAcceptedByNetwork {
		t.Errorf("expected both tx1/tx2 ACCEPTED, got %+v", got)
	}
	if atomic.LoadInt32(&txsHits) < 1 {
		t.Errorf("expected at least one /txs hit, got %d", txsHits)
	}
}

// TestPipeline_DepAware_ChildHeldUntilParentAccepted is the actual
// integration verification: when parent and child arrive on the
// dispatch topic, the child must NOT be broadcast until the parent's
// broadcast has succeeded. Without dep awareness the child could race
// the parent on a concurrent /txs to Teranode.
func TestPipeline_DepAware_ChildHeldUntilParentAccepted(t *testing.T) {
	// Teranode emulator: slow parent, fast child. The point of the
	// test is to ensure the child doesn't even ATTEMPT a broadcast
	// until the parent's broadcast has completed. We track the order
	// of /txs and /tx submissions by the rawTx byte.
	var (
		submitMu sync.Mutex
		submits  []byte
	)
	record := func(b byte) {
		submitMu.Lock()
		defer submitMu.Unlock()
		submits = append(submits, b)
	}

	parentReleased := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 1)
		_, _ = r.Body.Read(body)
		record(body[0])
		// Parent is 0xa1 — block until release.
		if body[0] == 0xa1 {
			<-parentReleased
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	store := newPipelineTestStore()
	broker := kafka.NewMemoryBroker(0)
	t.Cleanup(func() { _ = broker.Close() })
	producer := kafka.NewProducer(broker)

	cfg := &config.Config{}
	cfg.Kafka.ConsumerGroup = "test-pipeline-depaware"
	// batchMaxSize=1 so parent and child don't end up in the same
	// /txs batch — the dispatcher's dep-awareness is what we want to
	// observe, not Teranode's intra-batch handling.
	cfg.Propagation.TeranodeMaxBatchSize = 1
	cfg.Propagation.RetryMaxAttempts = 3
	cfg.Propagation.RetryBackoffMs = 50

	p, err := NewPipeline(cfg, zap.NewNop(), producer, store, tc, nil)
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startErr := make(chan error, 1)
	go func() { startErr <- p.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = p.Stop()
		<-startErr
	})

	time.Sleep(100 * time.Millisecond)

	// Publish parent then child.
	parentMsg := propagationMsg{TXID: "parentTx", RawTx: []byte{0xa1}}
	childMsg := propagationMsg{TXID: "childTx", RawTx: []byte{0xc1}, InputTXIDs: []string{"parentTx"}}
	if err := producer.Send(kafka.TopicDispatch, parentMsg.TXID, parentMsg); err != nil {
		t.Fatalf("send parent: %v", err)
	}
	if err := producer.Send(kafka.TopicDispatch, childMsg.TXID, childMsg); err != nil {
		t.Fatalf("send child: %v", err)
	}

	// Wait long enough that if the child were going to broadcast
	// without waiting, it would already have hit the server.
	time.Sleep(300 * time.Millisecond)

	submitMu.Lock()
	submitsCopy := append([]byte(nil), submits...)
	submitMu.Unlock()

	// Parent should have been submitted exactly once; child should
	// NOT have been submitted yet (it's blocked in the broadcaster
	// waiting on the parent's /tx to return — actually, no — the
	// dispatcher is holding it, so it never even reached the
	// broadcaster). Either way: only the parent should be visible.
	if len(submitsCopy) != 1 || submitsCopy[0] != 0xa1 {
		t.Errorf("before parent release, expected only parent submitted, got %x", submitsCopy)
	}

	// Release the parent. Its /tx returns 200, broadcaster emits
	// ACCEPTED_BY_NETWORK flip, dispatcher releases the child waiter.
	close(parentReleased)

	// Wait for child to flow through.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := store.byTxID(); got["childTx"] == models.StatusAcceptedByNetwork {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	got := store.byTxID()
	if got["parentTx"] != models.StatusAcceptedByNetwork {
		t.Errorf("expected parent ACCEPTED, got %s", got["parentTx"])
	}
	if got["childTx"] != models.StatusAcceptedByNetwork {
		t.Errorf("expected child ACCEPTED after parent release, got %s", got["childTx"])
	}

	// Order check: parent's 0xa1 must precede child's 0xc1 in submits.
	submitMu.Lock()
	defer submitMu.Unlock()
	parentIdx, childIdx := -1, -1
	for i, b := range submits {
		if b == 0xa1 && parentIdx == -1 {
			parentIdx = i
		}
		if b == 0xc1 && childIdx == -1 {
			childIdx = i
		}
	}
	if parentIdx == -1 || childIdx == -1 {
		t.Fatalf("missing submission: parent=%d, child=%d, all=%x", parentIdx, childIdx, submits)
	}
	if parentIdx >= childIdx {
		t.Errorf("parent should submit before child, but parent at %d, child at %d (all=%x)",
			parentIdx, childIdx, submits)
	}
}

// drainBatches is unused here but referenced via the package's other
// test files; this var keeps gofmt/lint quiet if/when those files are
// the only users.
var _ = json.Marshal
