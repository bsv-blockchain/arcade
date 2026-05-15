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

// newPropagatorForDepTest constructs a Propagator with the minimum
// wiring needed for the dep-aware admission and terminal-status tests.
// New() already starts the dispatcher goroutine; the returned cancel
// stops it via Stop()-equivalent semantics for the test's lifetime.
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

// pendingTXIDs snapshots the current pendingMsgs contents as a set of
// txids. Used to assert which txs are queued for the next flushBatch.
func pendingTXIDs(p *Propagator) map[string]bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]bool, len(p.pendingMsgs))
	for _, m := range p.pendingMsgs {
		out[m.TXID] = true
	}
	return out
}

// TestHandleMessage_HoldsChildWhenParentInFlight verifies the
// dep-aware admission path: a tx whose declared input is currently in
// flight does NOT enter pendingMsgs. The legacy parent-not-in-flight
// path still admits normally — only the in-flight overlap holds.
func TestHandleMessage_HoldsChildWhenParentInFlight(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}

	pending := pendingTXIDs(p)
	if !pending["parent"] {
		t.Errorf("parent should be in pendingMsgs, got %v", pending)
	}

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}

	pending = pendingTXIDs(p)
	if pending["child"] {
		t.Errorf("child should be HELD (waiting on parent), not in pendingMsgs; got %v", pending)
	}
	if !pending["parent"] {
		t.Errorf("parent should still be in pendingMsgs; got %v", pending)
	}
}

// TestApplyTerminalStatuses_ReleasesWaitersOnAccepted verifies the
// happy-path release: when a parent terminalizes ACCEPTED, the child
// waiter is released and re-enters pendingMsgs.
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
	// Drain pendingMsgs to simulate flushBatch consuming the parent's
	// row before its terminal status arrives. The test only cares about
	// what gets ADDED to pendingMsgs after ACCEPTED.
	p.mu.Lock()
	p.pendingMsgs = nil
	p.mu.Unlock()

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0)

	pending := pendingTXIDs(p)
	if !pending["child"] {
		t.Errorf("child should be released into pendingMsgs after parent ACCEPTED; got %v", pending)
	}
}

// TestApplyTerminalStatuses_CascadesRejectedChildren verifies that a
// parent's terminal REJECTED cascades through every descendant: child
// and grandchild get terminal REJECTED rows written, and neither
// enters pendingMsgs.
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
	// Drain pendingMsgs to keep the assertion focused on what flows
	// into it from cascade processing.
	p.mu.Lock()
	p.pendingMsgs = nil
	p.mu.Unlock()

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusRejected, Timestamp: time.Now(), ExtraInfo: "bad parent"},
	}, 0, 1)

	// Cascaded descendants do NOT re-enter pendingMsgs.
	pending := pendingTXIDs(p)
	if pending["child"] || pending["grandchild"] {
		t.Errorf("cascaded descendants should NOT enter pendingMsgs; got %v", pending)
	}

	// Cascaded descendants DO get terminal REJECTED rows written.
	ms.mu.Lock()
	rejected := 0
	for _, st := range ms.updates {
		if st.Status == models.StatusRejected && (st.TxID == "child" || st.TxID == "grandchild") {
			rejected++
		}
	}
	ms.mu.Unlock()
	if rejected != 2 {
		t.Errorf("expected 2 cascade-rejection rows (child + grandchild), got %d", rejected)
	}
}

// TestHandleMessage_NoParents_AdmitsNormally sanity-checks the legacy
// admission path: a tx with no InputTXIDs lands in pendingMsgs as
// before, no waiter bookkeeping involved.
func TestHandleMessage_NoParents_AdmitsNormally(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("lone"))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	pending := pendingTXIDs(p)
	if !pending["lone"] {
		t.Errorf("lone tx should be in pendingMsgs; got %v", pending)
	}
}

// TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly verifies
// that a child's InputTXIDs referencing a tx that is NOT currently in
// flight is admitted directly — only in-flight overlap creates a wait.
func TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"someParentNotInFlight"}))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	pending := pendingTXIDs(p)
	if !pending["child"] {
		t.Errorf("child should be admitted directly when parent is not in flight; got %v", pending)
	}
}
