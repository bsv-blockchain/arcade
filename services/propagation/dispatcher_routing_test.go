package propagation

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

// waitForInflight polls the propagator's inflight-depth mirror until it
// reaches want, or fails after ~3s.
func waitForInflight(t *testing.T, p *Propagator, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.inflightDepth.Load() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("inflight depth never reached %d (now %d)", want, p.inflightDepth.Load())
}

// startDispatcherWithIO runs a production dispatcher loop against its own
// fakeClaim and its own dispatcherIO — the multi-partition shape: Sarama
// gives each partition claim its own goroutine, and #295 gives each its
// own channel set so events can only reach the dispatcher whose inFlight
// map owns the tx. Callers must have stopped the New()-spawned test-mode
// dispatcher first (see runDispatcherWithClaim for why).
func startDispatcherWithIO(t *testing.T, p *Propagator, io *dispatcherIO) (*fakeClaim, func()) {
	t.Helper()
	claimCtx, cancel := context.WithCancel(context.Background())
	claim := newFakeClaim(claimCtx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = p.runDispatcher(claimCtx, claim, dispatcherConfig{maxPending: 1000}, io)
	}()
	return claim, func() {
		cancel()
		<-done
		p.WaitForBatches()
	}
}

// TestRunDispatcher_TwoClaims_RouteTerminalsIndependently pins the #295
// multi-partition routing contract: with two partition claims live on one
// pod, a terminal event for a tx admitted on claim A must reach ONLY A's
// dispatcher. With the pre-#295 shared terminalCh, either loop could
// consume the other's event; the wrong dispatcher would no-op the
// unknown txid, the owning tracker would never Done the offset, and that
// partition's commit watermark would freeze — the head-of-line wedge
// class from the 2026-08-11 incident, silently reintroduced.
func TestRunDispatcher_TwoClaims_RouteTerminalsIndependently(t *testing.T) {
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, newMockStore())
	// Stop the New()-spawned test-mode dispatcher: these production loops
	// replace it (same contract as runDispatcherWithClaim).
	p.dispatcherCancel()
	<-p.dispatcherDone
	p.dispatcherCancel = nil

	claimA, stopA := startDispatcherWithIO(t, p, p.defaultIO)
	defer stopA()
	claimB, stopB := startDispatcherWithIO(t, p, newDispatcherIO())
	defer stopB()

	claimA.ch <- &kafka.Message{Offset: 11, Value: makePropMsg("tx-on-claim-a")}
	claimB.ch <- &kafka.Message{Offset: 22, Value: makePropMsg("tx-on-claim-b")}

	waitForMark(t, claimA, 11, "claim A's tx must commit on claim A")
	waitForMark(t, claimB, 22, "claim B's tx must commit on claim B")

	if claimA.isMarked(22) {
		t.Error("claim A marked claim B's offset — events crossed dispatchers")
	}
	if claimB.isMarked(11) {
		t.Error("claim B marked claim A's offset — events crossed dispatchers")
	}
}

// TestApplyTerminalStatuses_NilIO_FansOutToAllDispatchers pins the reaper
// path: the reaper terminalizes txs off the Kafka path with no claim of
// its own, so its dispatcher notification (io == nil) must fan out to
// every registered dispatcher — whichever one holds the tx in flight
// releases the offset; the rest no-op. This preserves the pre-#295
// behavior where a reaper verdict could release a tx wedged in the live
// dispatcher.
func TestApplyTerminalStatuses_NilIO_FansOutToAllDispatchers(t *testing.T) {
	// Teranode returns 503 with no verdict body → the tx stays in flight
	// (requeue path), pinning its offset; only the fan-out notify can
	// release it.
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusServiceUnavailable)
	defer teranodeSrv.Close()

	ms := newMockStore()
	p := newPropagator("", teranodeSrv.URL, ms)
	p.dispatcherCancel()
	<-p.dispatcherDone
	p.dispatcherCancel = nil

	claim, stop := startDispatcherWithIO(t, p, p.defaultIO)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 31, Value: makePropMsg("reaper-owned-tx")}

	// Wait until the dispatcher actually owns the tx (it admitted and
	// broadcast it; the 503 keeps it in flight).
	waitForInflight(t, p, 1)

	// Reaper-style terminalization: no io. The fan-out must reach the
	// claim's dispatcher and release offset 31.
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{{
		TxID:      "reaper-owned-tx",
		Status:    models.StatusAcceptedByNetwork,
		Timestamp: time.Now(),
	}}, 1, 0, nil)

	waitForMark(t, claim, 31, "a nil-io (reaper) terminal must fan out to the owning dispatcher")
}
