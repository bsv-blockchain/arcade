package propagation

// Batch-shape rejections (issue #271): Teranode's /txs handler refuses a
// request that reaches maxTransactionsPerRequest (1024) or maxDataPerRequest
// (32 MiB) with a bare 400 text body, and an Echo/proxy body limit answers
// 413. Neither carries a per-tx line, and Teranode has already dispatched
// every tx it read before the trip. These tests pin arcade's reaction: narrow
// the chunk (never requeue it blind), never condemn a tx on that basis, keep
// a sibling's 200 sticky, and leave peer health untouched.
//
// These rows do not fit the boundary matrix in
// teranode_reason_contract_test.go: its server always writes the
// "Failed to process transactions:" header and its cases are error CHAINS
// rendered into failure lines. See docs/teranode-error-surfacing.md, table E.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// shapeRejection is one way a peer can refuse a POST /txs by shape.
type shapeRejection struct {
	name   string
	status int
	body   string
}

func shapeRejections() []shapeRejection {
	return []shapeRejection{
		{name: "400 too much data", status: http.StatusBadRequest, body: "Invalid request body: too much data"},
		{name: "400 too many transactions", status: http.StatusBadRequest, body: "Invalid request body: too many transactions"},
		{name: "400 too many submissions", status: http.StatusBadRequest, body: "Invalid request body: too many submissions"},
		{name: "413 body limit", status: http.StatusRequestEntityTooLarge, body: `{"message":"Request Entity Too Large"}`},
	}
}

// shapeLimitServer answers 200 to any body of at most maxBytes and the given
// shape rejection otherwise, counting every request.
func shapeLimitServer(t *testing.T, maxBytes int, rej shapeRejection, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		if len(body) > maxBytes {
			w.WriteHeader(rej.status)
			_, _ = w.Write([]byte(rej.body))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newShapeTestPropagator sets arcade's own byte cap far ABOVE the fake peer's
// limit so the reactive path — not planChunks — is what gets exercised.
func newShapeTestPropagator(tc *teranode.Client, ms *mockStore, logger *zap.Logger) *Propagator {
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.TeranodeMaxBatchBytes = 1 << 20
	return New(cfg, logger, nil, nil, ms, nil, tc, nil)
}

func admitSized(t *testing.T, p *Propagator, n, size int) []string {
	t.Helper()
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("shape-tx-%03d", i)
		if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithRaw(txids[i], make([]byte, size)))); err != nil {
			t.Fatalf("handleMessage(%s): %v", txids[i], err)
		}
	}
	return txids
}

// TestNarrowChunk_SizeRejection_AllAcceptedWithinBound — sixteen 100-byte txs
// against a peer that refuses any body over 1200 bytes. The 1600-byte chunk
// is refused once, halves to two 800-byte chunks that both clear, and every
// tx lands ACCEPTED_BY_NETWORK in exactly three round trips.
func TestNarrowChunk_SizeRejection_AllAcceptedWithinBound(t *testing.T) {
	for _, rej := range shapeRejections() {
		t.Run(rej.name, func(t *testing.T) {
			var hits atomic.Int32
			srv := shapeLimitServer(t, 1200, rej, &hits)
			tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
			ms := newMockStore()
			p := newShapeTestPropagator(tc, ms, zap.NewNop())

			before := testutil.ToFloat64(metrics.PropagationChunkTotal.WithLabelValues("size_rejected"))
			txids := admitSized(t, p, 16, 100)
			if err := flushSync(t, p); err != nil {
				t.Fatalf("flushSync: %v", err)
			}

			for _, txid := range txids {
				st := ms.lastUpdateForTxid(txid)
				if st == nil || st.Status != models.StatusAcceptedByNetwork {
					t.Errorf("%s: status = %+v, want ACCEPTED_BY_NETWORK", txid, st)
				}
			}
			if got := hits.Load(); got != 3 {
				t.Errorf("POST /txs count = %d, want 3 (one refusal + two clean halves)", got)
			}
			if delta := testutil.ToFloat64(metrics.PropagationChunkTotal.WithLabelValues("size_rejected")) - before; delta != 1 {
				t.Errorf("chunk_total{size_rejected} delta = %v, want 1", delta)
			}
			if got := len(tc.GetHealthyEndpoints()); got != 1 {
				t.Errorf("healthy endpoints = %d, want 1 (a shape rejection is not a peer fault)", got)
			}
		})
	}
}

// TestBroadcast_SizeRejection_ChunkOfOne_RequeuesWithReasonNeverRejects — a
// single tx a peer refuses by shape cannot be narrowed further. It must ride
// the ordinary no-verdict path to PENDING_RETRY, carrying the peer's own
// words as the reason, and must never be laundered into REJECTED — nor
// described as a missing-parent condition, which it is not.
func TestBroadcast_SizeRejection_ChunkOfOne_RequeuesWithReasonNeverRejects(t *testing.T) {
	var hits atomic.Int32
	srv := shapeLimitServer(t, 10, shapeRejections()[0], &hits) // 100 B tx always refused
	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 2, time.Millisecond)

	const txid = "lone-oversize"
	admit(t, p, txid, make([]byte, 100))
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushSync: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := ms.lastUpdateForTxid(txid); st != nil && st.Status == models.StatusPendingRetry {
			break
		}
		_ = flushSync(t, p)
		time.Sleep(2 * time.Millisecond)
	}

	st := ms.lastUpdateForTxid(txid)
	if st == nil {
		t.Fatal("no status written")
	}
	if st.Status == models.StatusRejected {
		t.Fatalf("a shape rejection was laundered into REJECTED %q", st.ExtraInfo)
	}
	if st.Status != models.StatusPendingRetry {
		t.Fatalf("status = %s, want PENDING_RETRY; extra=%q", st.Status, st.ExtraInfo)
	}
	if !strings.Contains(st.ExtraInfo, "too much data") {
		t.Errorf("park reason %q does not quote the peer's refusal", st.ExtraInfo)
	}
	if strings.Contains(st.ExtraInfo, "parent not yet accepted") {
		t.Errorf("park reason %q invents a missing-parent story", st.ExtraInfo)
	}
}

// TestBroadcast_SizeRejection_LosesToSiblingAcceptance — one peer refuses the
// chunk by shape while its sibling accepts it. Acceptance is sticky: every tx
// is ACCEPTED, nothing is narrowed (each peer sees exactly one request), and
// the refusing peer is not charged toward the slow-track breaker.
func TestBroadcast_SizeRejection_LosesToSiblingAcceptance(t *testing.T) {
	var rejectHits, acceptHits atomic.Int32
	rejectSrv := shapeLimitServer(t, 0, shapeRejections()[0], &rejectHits)
	acceptSrv := shapeLimitServer(t, 1<<20, shapeRejections()[0], &acceptHits)
	tc := teranode.NewClient([]string{rejectSrv.URL, acceptSrv.URL}, "",
		teranode.HealthConfig{FailureThreshold: 1 << 20, BroadcastFailureThreshold: 1})
	ms := newMockStore()
	p := newShapeTestPropagator(tc, ms, zap.NewNop())

	txids := admitSized(t, p, 4, 100)
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushSync: %v", err)
	}

	for _, txid := range txids {
		st := ms.lastUpdateForTxid(txid)
		if st == nil || st.Status != models.StatusAcceptedByNetwork {
			t.Errorf("%s: status = %+v, want ACCEPTED_BY_NETWORK", txid, st)
		}
	}
	if got := rejectHits.Load(); got != 1 {
		t.Errorf("refusing peer saw %d requests, want 1 (no narrowing when a sibling accepted)", got)
	}
	if got := acceptHits.Load(); got != 1 {
		t.Errorf("accepting peer saw %d requests, want 1", got)
	}
	if got := len(tc.GetHealthyEndpoints()); got != 2 {
		t.Errorf("healthy endpoints = %d, want 2 (shape rejection must not charge the breaker)", got)
	}
}

// TestRecordBroadcastOutcomes_ShapeRejectIsNeutral — unit-level pin of the
// breaker accounting: a shape rejection neither charges nor resets a peer,
// while the existing rules for real failures are unchanged.
func TestRecordBroadcastOutcomes_ShapeRejectIsNeutral(t *testing.T) {
	const a, b = "http://a.invalid", "http://b.invalid"
	tc := teranode.NewClient([]string{a, b}, "", teranode.HealthConfig{FailureThreshold: 1 << 20, BroadcastFailureThreshold: 1})
	healthy := func() int { return len(tc.GetHealthyEndpoints()) }

	recordBroadcastOutcomes(tc, []endpointOutcome{{endpoint: a, statusCode: 400, shapeRejected: true}, {endpoint: b, statusCode: 200}})
	if healthy() != 2 {
		t.Fatalf("shape reject beside a sibling 200 sidelined a peer: healthy = %d", healthy())
	}
	recordBroadcastOutcomes(tc, []endpointOutcome{{endpoint: a, statusCode: 400, shapeRejected: true}})
	if healthy() != 2 {
		t.Fatalf("lone shape reject sidelined a peer: healthy = %d", healthy())
	}
	recordBroadcastOutcomes(tc, []endpointOutcome{{endpoint: a, statusCode: 500}})
	if healthy() != 2 {
		t.Fatalf("unanimous reject must not charge the breaker: healthy = %d", healthy())
	}
	recordBroadcastOutcomes(tc, []endpointOutcome{{endpoint: a, statusCode: 500}, {endpoint: b, statusCode: 200}})
	if healthy() != 1 {
		t.Fatalf("a real failure beside a sibling 200 must still charge the breaker: healthy = %d", healthy())
	}
}

// TestBatchShapeRejectedLog_HasSizeFields — the operator-facing Warn names
// the status, the chunk's count and bytes, and whether narrowing can help.
func TestBatchShapeRejectedLog_HasSizeFields(t *testing.T) {
	var hits atomic.Int32
	srv := shapeLimitServer(t, 0, shapeRejections()[0], &hits) // refuses everything
	tc := teranode.NewClient([]string{srv.URL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	core, recorded := observer.New(zapcore.WarnLevel)
	ms := newMockStore()
	p := newShapeTestPropagator(tc, ms, zap.New(core))

	admitSized(t, p, 2, 100)
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushSync: %v", err)
	}

	warns := recorded.FilterMessage("teranode rejected chunk by size or count; no per-tx verdict").All()
	if len(warns) == 0 {
		t.Fatalf("no shape-rejection warn; all warns: %+v", recorded.All())
	}
	first := warns[0].ContextMap()
	if first["status_code"] != int64(400) || first["chunk_size"] != int64(2) || first["chunk_bytes"] != int64(200) || first["narrowable"] != true {
		t.Errorf("first warn fields = %v, want status_code=400 chunk_size=2 chunk_bytes=200 narrowable=true", first)
	}
	if !strings.Contains(fmt.Sprint(first["error"]), "too much data") {
		t.Errorf("first warn error = %v, want the peer body quoted", first["error"])
	}
}
