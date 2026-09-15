package propagation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/teranode"
)

// makePropMsgWithRaw is makePropMsg with a caller-chosen RawTx. The
// propagation path treats RawTx as opaque bytes, so any content works; the
// LENGTH is what the byte-aware chunker reads (issue #271).
func makePropMsgWithRaw(txid string, raw []byte) []byte {
	b, err := json.Marshal(propagationMsg{TXID: txid, RawTx: raw})
	if err != nil {
		panic(err)
	}
	return b
}

// contentLengths records the Content-Length of every POST /txs a fake
// Teranode receives. Chunks broadcast in parallel, so callers compare the
// sorted multiset rather than arrival order.
type contentLengths struct {
	mu    sync.Mutex
	sizes []int
}

func (c *contentLengths) add(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sizes = append(c.sizes, n)
}

func (c *contentLengths) sorted() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := slices.Clone(c.sizes)
	slices.Sort(out)
	return out
}

func newContentLengthServer(rec *contentLengths) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(int(r.ContentLength))
		w.WriteHeader(http.StatusOK)
	}))
}

// newByteCapPropagator builds a propagator against one fake Teranode with a
// generous count cap so only teranode_max_batch_bytes decides chunk edges.
func newByteCapPropagator(teranodeURL string, maxBytes int, logger *zap.Logger) (*Propagator, *mockStore) {
	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.TeranodeMaxBatchSize = 1000
	cfg.Propagation.TeranodeMaxBatchBytes = maxBytes
	tc := teranode.NewClient([]string{teranodeURL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	return New(cfg, logger, nil, nil, ms, nil, tc, nil), ms
}

// TestProcessBatch_ChunksByBytes — ten 1000-byte txs under a 2500-byte cap
// must leave as five POST /txs of exactly 2000 bytes each, and every tx must
// still get its status update.
func TestProcessBatch_ChunksByBytes(t *testing.T) {
	rec := &contentLengths{}
	srv := newContentLengthServer(rec)
	defer srv.Close()

	p, ms := newByteCapPropagator(srv.URL, 2500, zap.NewNop())
	for i := 0; i < 10; i++ {
		_ = p.handleMessage(context.Background(), consumerMsg(makePropMsgWithRaw(fmt.Sprintf("tx%03d", i), make([]byte, 1000))))
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	want := []int{2000, 2000, 2000, 2000, 2000}
	if got := rec.sorted(); !slices.Equal(got, want) {
		t.Errorf("POST /txs content lengths = %v, want %v", got, want)
	}
	if ms.updateCount() != 10 {
		t.Errorf("expected 10 status updates, got %d", ms.updateCount())
	}
}

// TestProcessBatch_OversizeTxBroadcastAlone — a tx larger than the byte cap
// is still broadcast, alone in its own chunk, and announced once at Warn with
// its size so an operator can see why the cap did not bound that POST.
func TestProcessBatch_OversizeTxBroadcastAlone(t *testing.T) {
	rec := &contentLengths{}
	srv := newContentLengthServer(rec)
	defer srv.Close()

	core, recorded := observer.New(zapcore.WarnLevel)
	p, ms := newByteCapPropagator(srv.URL, 1000, zap.New(core))
	_ = p.handleMessage(context.Background(), consumerMsg(makePropMsgWithRaw("small-a", make([]byte, 200))))
	_ = p.handleMessage(context.Background(), consumerMsg(makePropMsgWithRaw("big", make([]byte, 5000))))
	_ = p.handleMessage(context.Background(), consumerMsg(makePropMsgWithRaw("small-b", make([]byte, 200))))
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	want := []int{200, 200, 5000}
	if got := rec.sorted(); !slices.Equal(got, want) {
		t.Errorf("POST /txs content lengths = %v, want %v", got, want)
	}
	if ms.updateCount() != 3 {
		t.Errorf("expected 3 status updates, got %d", ms.updateCount())
	}

	warns := recorded.FilterMessage("transaction exceeds teranode_max_batch_bytes; broadcasting it alone").All()
	if len(warns) != 1 {
		t.Fatalf("expected exactly one oversize warn, got %d: %+v", len(warns), warns)
	}
	fields := warns[0].ContextMap()
	if fields["txid"] != "big" {
		t.Errorf("warn txid = %v, want big", fields["txid"])
	}
	if fields["tx_bytes"] != int64(5000) {
		t.Errorf("warn tx_bytes = %v, want 5000", fields["tx_bytes"])
	}
	if fields["max_batch_bytes"] != int64(1000) {
		t.Errorf("warn max_batch_bytes = %v, want 1000", fields["max_batch_bytes"])
	}
}

// TestNew_BatchCapDefaults — unset or non-positive caps fall back to the
// shipped config defaults (one source of truth), explicit values are honored.
func TestNew_BatchCapDefaults(t *testing.T) {
	tc := teranode.NewClient([]string{"http://127.0.0.1:1"}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	build := func(size, bytes int) *Propagator {
		cfg := &config.Config{}
		cfg.Propagation.TeranodeMaxBatchSize = size
		cfg.Propagation.TeranodeMaxBatchBytes = bytes
		return New(cfg, zap.NewNop(), nil, nil, newMockStore(), nil, tc, nil)
	}

	p := build(0, 0)
	if p.teranodeBatchCap != config.DefaultTeranodeMaxBatchSize {
		t.Errorf("unset count cap = %d, want %d", p.teranodeBatchCap, config.DefaultTeranodeMaxBatchSize)
	}
	if p.teranodeBatchBytesCap != config.DefaultTeranodeMaxBatchBytes {
		t.Errorf("unset byte cap = %d, want %d", p.teranodeBatchBytesCap, config.DefaultTeranodeMaxBatchBytes)
	}

	p = build(-1, -1)
	if p.teranodeBatchCap != config.DefaultTeranodeMaxBatchSize || p.teranodeBatchBytesCap != config.DefaultTeranodeMaxBatchBytes {
		t.Errorf("negative caps = (%d, %d), want defaults", p.teranodeBatchCap, p.teranodeBatchBytesCap)
	}

	p = build(25, 123)
	if p.teranodeBatchCap != 25 || p.teranodeBatchBytesCap != 123 {
		t.Errorf("explicit caps = (%d, %d), want (25, 123)", p.teranodeBatchCap, p.teranodeBatchBytesCap)
	}
}

// TestNew_WarnsWhenCapsReachTeranodeLimits — Teranode refuses a chunk that
// REACHES its per-request limits (1024 txs, 32 MiB), so a cap set to either
// value is a misconfiguration the operator should hear about at boot. The
// value is not clamped: the reactive narrowing path still copes.
func TestNew_WarnsWhenCapsReachTeranodeLimits(t *testing.T) {
	tc := teranode.NewClient([]string{"http://127.0.0.1:1"}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	build := func(size, bytes int) *observer.ObservedLogs {
		core, recorded := observer.New(zapcore.WarnLevel)
		cfg := &config.Config{}
		cfg.Propagation.TeranodeMaxBatchSize = size
		cfg.Propagation.TeranodeMaxBatchBytes = bytes
		New(cfg, zap.New(core), nil, nil, newMockStore(), nil, tc, nil)
		return recorded
	}

	if got := build(1000, 16<<20).Len(); got != 0 {
		t.Errorf("defaults produced %d warns, want 0", got)
	}

	recorded := build(1024, 32<<20)
	sizeWarns := recorded.FilterMessage("teranode_max_batch_size reaches teranode's per-request limit; a full chunk will be refused").All()
	if len(sizeWarns) != 1 {
		t.Fatalf("count-cap warn: got %d, want 1 (all: %+v)", len(sizeWarns), recorded.All())
	}
	if f := sizeWarns[0].ContextMap(); f["teranode_max_batch_size"] != int64(1024) || f["teranode_limit"] != int64(1024) {
		t.Errorf("count-cap warn fields = %v", f)
	}
	bytesWarns := recorded.FilterMessage("teranode_max_batch_bytes reaches teranode's per-request limit; a full chunk will be refused").All()
	if len(bytesWarns) != 1 {
		t.Fatalf("byte-cap warn: got %d, want 1 (all: %+v)", len(bytesWarns), recorded.All())
	}
	if f := bytesWarns[0].ContextMap(); f["teranode_max_batch_bytes"] != int64(32<<20) || f["teranode_limit"] != int64(32<<20) {
		t.Errorf("byte-cap warn fields = %v", f)
	}
}
