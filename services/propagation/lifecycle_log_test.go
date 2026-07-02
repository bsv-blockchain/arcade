package propagation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/teranode"
)

// TestApplyTerminalStatuses_LogsAcceptedByNetwork closes the
// ACCEPTED_BY_NETWORK log gap: a batch that broadcasts successfully must
// emit an Info "transactions accepted by network" line carrying the
// canonical stage and txid batch fields, so a txid search surfaces the
// RECEIVED -> ACCEPTED_BY_NETWORK transition.
func TestApplyTerminalStatuses_LogsAcceptedByNetwork(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, logger, nil, nil, ms, nil, tc, nil)

	if err := handleAndFlush(t, p, makePropMsg("accepted-tx-1")); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	entries := recorded.FilterMessage("transactions accepted by network").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'transactions accepted by network' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["stage"].(string); got != "network" {
		t.Errorf("stage = %q, want %q", got, "network")
	}
	if got, _ := fields["txid_count"].(int64); got != 1 {
		t.Errorf("txid_count = %d, want 1", got)
	}
	// chunk_total pins the full-coverage (ForEachTxIDChunk) path: this async
	// line must carry every accepted txid across chunks, so a revert to a
	// bounded TxIDBatch line (which has no chunk_total field) fails here.
	if got, ok := fields["chunk_total"].(int64); !ok || got != 1 {
		t.Errorf("chunk_total = %v (ok=%v), want 1 — expected the chunked full-coverage path", fields["chunk_total"], ok)
	}
}

// TestRequeueAfterDelay_LogsRequeueForRetry closes the PENDING_RETRY log
// gap: scheduling a requeue must emit an Info "transactions requeued for
// retry" line synchronously (before the delay goroutine sleeps), carrying
// the true txid count and the "network" stage. Fires once per
// requeueAfterDelay call — not once per message in the batch.
func TestRequeueAfterDelay_LogsRequeueForRetry(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)
	cfg := &config.Config{}
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, logger, nil, nil, newMockStore(), nil, tc, nil)
	defer func() {
		if p.dispatcherCancel != nil {
			p.dispatcherCancel()
			if p.dispatcherDone != nil {
				<-p.dispatcherDone
			}
		}
	}()

	// Cancel immediately after the synchronous log assertion so the
	// delayed-requeue goroutine bails via ctx.Done() instead of sleeping out
	// the full requeueDelay in this test.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs := []propagationMsg{{TXID: "retry-tx-1"}, {TXID: "retry-tx-2"}}
	p.requeueAfterDelay(ctx, msgs)

	entries := recorded.FilterMessage("transactions requeued for retry").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'transactions requeued for retry' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["stage"].(string); got != "network" {
		t.Errorf("stage = %q, want %q", got, "network")
	}
	if got, _ := fields["txid_count"].(int64); got != 2 {
		t.Errorf("txid_count = %d, want 2", got)
	}
}

// TestRegisterBatch_LogsMerkleRegistrationAtDebug closes the merkle
// registration observability gap: a successful /watch registration is
// logged at Debug (not Info, to keep lifecycle Info volume flat — it isn't
// a status-lattice transition) with the batch's txid fields.
func TestRegisterBatch_LogsMerkleRegistrationAtDebug(t *testing.T) {
	merkleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer merkleSrv.Close()

	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	cfg := &config.Config{
		CallbackURL:   "http://localhost:8080/callback",
		CallbackToken: "secret",
	}
	cfg.Propagation.MerkleConcurrency = 10
	mc := merkleservice.NewClient(merkleSrv.URL, "", 5*time.Second)
	tc := teranode.NewClient([]string{"http://unused.invalid"}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, logger, nil, nil, newMockStore(), nil, tc, mc)

	batch := []propagationMsg{{TXID: "merkle-tx-1"}, {TXID: "merkle-tx-2"}}
	registered, failed := p.registerBatch(context.Background(), batch)
	if len(failed) != 0 {
		t.Fatalf("expected no registration failures, got %d", len(failed))
	}
	if len(registered) != 2 {
		t.Fatalf("expected 2 registered txs, got %d", len(registered))
	}

	entries := recorded.FilterMessage("registered with merkle-service").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'registered with merkle-service' log line, got %d", len(entries))
	}
	if entries[0].Level != zapcore.DebugLevel {
		t.Errorf("expected Debug level, got %v", entries[0].Level)
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["txid_count"].(int64); got != 2 {
		t.Errorf("txid_count = %d, want 2", got)
	}
}
