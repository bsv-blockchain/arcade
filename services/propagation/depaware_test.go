package propagation

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// makePropMsgWithParents builds a propagationMsg envelope with explicit
// InputTXIDs. The legacy makePropMsg helper builds one without
// InputTXIDs for the original tests; this variant is used by the
// dep-aware tests below.
func makePropMsgWithParents(txid string, parents []string) []byte {
	msg := propagationMsg{
		TXID:       txid,
		RawTx:      []byte{0xde, 0xad, 0xbe, 0xef},
		InputTXIDs: parents,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

// newPropagatorForDepTest constructs a Propagator wired with a
// minimal mock store, kicks off its dispatcher goroutine via New, and
// returns a cleanup that cancels the dispatcher when the test ends.
func newPropagatorForDepTest(t *testing.T, ms *mockStore) (*Propagator, func()) {
	t.Helper()
	cfg := &config.Config{}
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)
	return p, func() {
		if p.dispatcherCancel != nil {
			p.dispatcherCancel()
		}
	}
}

// drainSet snapshots the dispatcher's pendingMsgs as a set of txids,
// CLEARING the dispatcher's state in the process. Tests that drain
// must then re-think downstream assertions accordingly.
func drainSet(p *Propagator) map[string]bool {
	batch := p.drainPending()
	out := make(map[string]bool, len(batch))
	for _, m := range batch {
		out[m.TXID] = true
	}
	return out
}

// TestHandleMessage_HoldsChildWhenParentInFlight verifies the
// dep-aware admission path: a tx whose declared input is currently in
// flight does NOT enter the pending broadcast batch; only the parent
// is observable on drain.
func TestHandleMessage_HoldsChildWhenParentInFlight(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}

	pending := drainSet(p)
	if !pending["parent"] {
		t.Errorf("parent should be in pending batch, got %v", pending)
	}
	if pending["child"] {
		t.Errorf("child should be HELD as waiter, not in pending batch; got %v", pending)
	}
}

// TestApplyTerminalStatuses_ReleasesWaitersOnAccepted verifies that
// when a parent terminalizes ACCEPTED, the child waiter is released
// back into the pending batch.
func TestApplyTerminalStatuses_ReleasesWaitersOnAccepted(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	// Drain the parent (simulating flushBatch consuming it) so the
	// subsequent drain only shows what gets ADDED after ACCEPTED.
	_ = drainSet(p)

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0)

	pending := drainSet(p)
	if !pending["child"] {
		t.Errorf("child should be released into pending batch after parent ACCEPTED; got %v", pending)
	}
}

// TestApplyTerminalStatuses_CascadesRejectedChildren verifies the
// recursive cascade: parent REJECTED → child + grandchild get REJECTED
// rows written and never enter the broadcast batch.
func TestApplyTerminalStatuses_CascadesRejectedChildren(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("grandchild", []string{"child"}))); err != nil {
		t.Fatalf("grandchild admit: %v", err)
	}
	_ = drainSet(p) // discard parent (only thing in pending batch — children are held)

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusRejected, Timestamp: time.Now(), ExtraInfo: "bad parent"},
	}, 0, 1)

	// Cascaded descendants do NOT re-enter pending batch.
	pending := drainSet(p)
	if pending["child"] || pending["grandchild"] {
		t.Errorf("cascaded descendants should NOT enter pending batch; got %v", pending)
	}

	// Cascaded descendants DO get terminal REJECTED rows written with
	// the original ancestor's rejection reason threaded through.
	ms.mu.Lock()
	rejected := map[string]string{}
	for _, st := range ms.updates {
		if st.Status == models.StatusRejected && (st.TxID == "child" || st.TxID == "grandchild") {
			rejected[st.TxID] = st.ExtraInfo
		}
	}
	ms.mu.Unlock()
	if len(rejected) != 2 {
		t.Errorf("expected 2 cascade-rejection rows (child + grandchild), got %d: %v", len(rejected), rejected)
	}
	if rejected["child"] != "bad parent" {
		t.Errorf("child rejection reason should be threaded from parent (\"bad parent\"), got %q", rejected["child"])
	}
	if rejected["grandchild"] != "bad parent" {
		t.Errorf("grandchild rejection reason should be threaded from parent (\"bad parent\"), got %q", rejected["grandchild"])
	}
}

// TestHandleMessage_NoParents_AdmitsNormally sanity-checks the legacy
// admission path: a tx with no InputTXIDs lands in the pending batch
// as before.
func TestHandleMessage_NoParents_AdmitsNormally(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("lone"))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	pending := drainSet(p)
	if !pending["lone"] {
		t.Errorf("lone tx should be in pending batch; got %v", pending)
	}
}

// TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly verifies
// that a child's InputTXIDs referencing a tx NOT currently in flight
// is admitted directly — only in-flight overlap creates a wait.
func TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"someParentNotInFlight"}))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	pending := drainSet(p)
	if !pending["child"] {
		t.Errorf("child should be admitted directly when parent is not in flight; got %v", pending)
	}
}

// TestHandleMessage_MaxPendingFull_ReturnsErrorWithoutLeakingInFlight
// verifies item 17 from the audit: when pendingMsgs is at maxPending,
// admission returns an error AND the txid is NOT leaked into
// in-flight state (which would otherwise prevent retry).
func TestHandleMessage_MaxPendingFull_ReturnsErrorWithoutLeakingInFlight(t *testing.T) {
	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MaxPending = 1 // saturate after a single admit
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)
	defer p.dispatcherCancel()

	// First admit lands.
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("tx1"))); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	// Second admit rejected because pending is full.
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("tx2"))); err == nil {
		t.Errorf("second admit should fail with full-queue error")
	}

	// Drain to clear pending.
	pending := drainSet(p)
	if pending["tx2"] {
		t.Errorf("tx2 should not have entered pending batch; got %v", pending)
	}
	if !pending["tx1"] {
		t.Errorf("tx1 should be in pending batch; got %v", pending)
	}

	// Now that pending is empty, tx2 should be admittable again on
	// retry — confirms tx2 didn't leak into in-flight state on the
	// first attempt (which would have made it look like a known tx
	// that's already being handled).
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("tx2"))); err != nil {
		t.Errorf("re-admit of tx2 after drain should succeed; got %v", err)
	}
	pending = drainSet(p)
	if !pending["tx2"] {
		t.Errorf("tx2 should be in pending batch after re-admit; got %v", pending)
	}
}
