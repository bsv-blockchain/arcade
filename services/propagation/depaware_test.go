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
// wiring needed for the dep-aware admission and terminal-status tests:
// no merkle client, no teranode endpoints, just a store. These tests
// exercise the in-memory dep index, not the broadcast pipeline.
func newPropagatorForDepTest(_ *testing.T, ms *mockStore) *Propagator {
	cfg := &config.Config{}
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	return New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)
}

// TestHandleMessage_HoldsChildWhenParentInFlight verifies the
// dep-aware admission path: a tx whose declared input is currently in
// flight does NOT enter pendingMsgs; it lands in heldMsgs and the
// dep index.
func TestHandleMessage_HoldsChildWhenParentInFlight(t *testing.T) {
	ms := newMockStore()
	p := newPropagatorForDepTest(t, ms)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}

	p.mu.Lock()
	parentPending := len(p.pendingMsgs)
	_, parentInFlight := p.inFlight["parent"]
	p.mu.Unlock()
	if parentPending != 1 {
		t.Errorf("parent should be in pendingMsgs (got %d entries)", parentPending)
	}
	if !parentInFlight {
		t.Errorf("parent should be in inFlight set")
	}

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}

	p.mu.Lock()
	pendingAfterChild := len(p.pendingMsgs)
	_, childInFlight := p.inFlight["child"]
	childWaiters, hasWaiter := p.waiters["parent"]
	_, childPendingParents := p.pendingParents["child"]
	_, childHeld := p.heldMsgs["child"]
	p.mu.Unlock()

	if pendingAfterChild != 1 {
		t.Errorf("child should NOT have entered pendingMsgs; expected 1 (parent only), got %d", pendingAfterChild)
	}
	if !childInFlight {
		t.Errorf("child SHOULD be in inFlight even when held — descendants need to see it as a parent to wait on")
	}
	if !hasWaiter || len(childWaiters) != 1 {
		t.Errorf("parent should have child registered as waiter; got %v", childWaiters)
	}
	if !childPendingParents {
		t.Errorf("child should have pendingParents entry")
	}
	if !childHeld {
		t.Errorf("child's message should be in heldMsgs")
	}
}

// TestApplyTerminalStatuses_ReleasesWaitersOnAccepted verifies the
// happy-path release: when a parent terminalizes ACCEPTED, its
// waiters' pendingParents sets are pruned. A child whose parent set
// goes empty is pushed back into pendingMsgs and inFlight.
func TestApplyTerminalStatuses_ReleasesWaitersOnAccepted(t *testing.T) {
	ms := newMockStore()
	p := newPropagatorForDepTest(t, ms)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}

	now := time.Now()
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusAcceptedByNetwork, Timestamp: now},
	}, 1, 0)

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, stillInFlight := p.inFlight["parent"]; stillInFlight {
		t.Errorf("parent should be removed from inFlight after ACCEPTED")
	}
	if _, childIn := p.inFlight["child"]; !childIn {
		t.Errorf("child should now be in inFlight (released waiter)")
	}
	if _, stillWaiter := p.waiters["parent"]; stillWaiter {
		t.Errorf("waiters['parent'] should be gone after release")
	}
	if _, stillPending := p.pendingParents["child"]; stillPending {
		t.Errorf("pendingParents['child'] should be gone after release")
	}
	if _, stillHeld := p.heldMsgs["child"]; stillHeld {
		t.Errorf("heldMsgs['child'] should be gone after release")
	}
	foundChild := false
	for _, m := range p.pendingMsgs {
		if m.TXID == "child" {
			foundChild = true
			break
		}
	}
	if !foundChild {
		t.Errorf("child should be re-entered into pendingMsgs after release")
	}
}

// TestApplyTerminalStatuses_CascadesRejectedChildren verifies that a
// parent's terminal REJECTED cascades through every descendant: a
// child held on a rejected parent is itself rejected (with a row
// written to the store), and a grandchild waiting on that child
// recurses correctly.
func TestApplyTerminalStatuses_CascadesRejectedChildren(t *testing.T) {
	ms := newMockStore()
	p := newPropagatorForDepTest(t, ms)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("grandchild", []string{"child"}))); err != nil {
		t.Fatalf("grandchild admit: %v", err)
	}

	now := time.Now()
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusRejected, Timestamp: now, ExtraInfo: "bad parent"},
	}, 0, 1)

	p.mu.Lock()
	_, parentIn := p.inFlight["parent"]
	_, childIn := p.inFlight["child"]
	_, grandchildIn := p.inFlight["grandchild"]
	_, childHeld := p.heldMsgs["child"]
	_, grandchildHeld := p.heldMsgs["grandchild"]
	p.mu.Unlock()

	if parentIn || childIn || grandchildIn {
		t.Errorf("none of parent/child/grandchild should remain in inFlight; got %v/%v/%v",
			parentIn, childIn, grandchildIn)
	}
	if childHeld || grandchildHeld {
		t.Errorf("cascaded children should be removed from heldMsgs")
	}

	// Cascade-rejection store writes: one row per cascaded descendant.
	// The parent's own REJECTED row is the responsibility of the
	// caller (processBatch in real flow); this test only feeds the
	// parent's status in to applyTerminalStatuses to trigger the
	// cascade.
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

// TestHandleMessage_NoParents_AdmitsNormally is a sanity check on the
// legacy admission path under the new code: a tx with no InputTXIDs
// (or an empty list) flows through to pendingMsgs and inFlight as
// before, with no waiter bookkeeping.
func TestHandleMessage_NoParents_AdmitsNormally(t *testing.T) {
	ms := newMockStore()
	p := newPropagatorForDepTest(t, ms)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("lone"))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pendingMsgs) != 1 || p.pendingMsgs[0].TXID != "lone" {
		t.Errorf("expected lone tx in pendingMsgs, got %+v", p.pendingMsgs)
	}
	if _, ok := p.inFlight["lone"]; !ok {
		t.Errorf("expected lone tx in inFlight set")
	}
	if len(p.waiters) != 0 || len(p.pendingParents) != 0 || len(p.heldMsgs) != 0 {
		t.Errorf("no waiter state should exist; got waiters=%v pendingParents=%v heldMsgs=%v",
			p.waiters, p.pendingParents, p.heldMsgs)
	}
}

// TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly verifies
// that a child's InputTXIDs referencing a tx that is NOT currently in
// flight (mined long ago, never seen by Arcade, etc.) is admitted
// directly — only in-flight overlap creates a wait.
func TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly(t *testing.T) {
	ms := newMockStore()
	p := newPropagatorForDepTest(t, ms)

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"someParentNotInFlight"}))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pendingMsgs) != 1 {
		t.Errorf("child should be admitted directly when parent is not in flight; got %d in pendingMsgs", len(p.pendingMsgs))
	}
	if _, ok := p.inFlight["child"]; !ok {
		t.Errorf("child should be in inFlight set")
	}
	if len(p.waiters) != 0 || len(p.heldMsgs) != 0 {
		t.Errorf("no waiter state expected; got waiters=%v heldMsgs=%v", p.waiters, p.heldMsgs)
	}
}
