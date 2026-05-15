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
// InputTXIDs.
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

// newPropagatorForDepTest constructs a Propagator with a minimal mock
// store. Its dispatcher goroutine starts inside New; the returned
// cleanup cancels it when the test ends.
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

// drainSet snapshots the dispatcher's pendingMsgs as a set of txids
// AND clears the dispatcher's pending state (moving the txids from
// "queued for next batch" to "broadcasting"). Tests calling this
// must then re-think subsequent admission decisions accordingly.
func drainSet(p *Propagator) map[string]bool {
	batch := p.drainPending()
	out := make(map[string]bool, len(batch))
	for _, m := range batch {
		out[m.TXID] = true
	}
	return out
}

// TestHandleMessage_SameBatchAdmission verifies the dep-aware
// optimization: a parent and its child both arrive before the next
// flush. Both end up in the same broadcast batch — Teranode handles
// the dependency ordering inside its /txs POST.
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
		t.Errorf("child should be HELD while parent is in-flight (Teranode parallel processing forbids same-batch parent+child); got %v", pending)
	}
}

// TestHandleMessage_HoldsChildWhenParentInDifferentBatch verifies the
// "different in-flight batch → hold" rule. A child arriving after its
// parent has already been drained (and is now broadcasting) must be
// held; Teranode can't coordinate across separate batches.
func TestHandleMessage_HoldsChildWhenParentInDifferentBatch(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	// Drain — parent moves from inPending to inFlight-but-not-inPending
	// (broadcasting from this point until terminal).
	if got := drainSet(p); !got["parent"] {
		t.Fatalf("parent should drain, got %v", got)
	}

	// Now child arrives. Parent is in inFlight but not in inPending →
	// child must hold.
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}

	if got := drainSet(p); got["child"] {
		t.Errorf("child should be held (parent in different batch), not in pending; got %v", got)
	}
}

// TestApplyTerminalStatuses_ReleasesWaitersOnAccepted verifies the
// release path: child held on a broadcasting parent gets released to
// the pending batch when the parent terminalizes ACCEPTED.
func TestApplyTerminalStatuses_ReleasesWaitersOnAccepted(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	_ = drainSet(p) // parent now broadcasting

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	// Confirm child is held, not in pending.
	if got := drainSet(p); got["child"] {
		t.Fatalf("child should be held before parent ACCEPTED; got %v", got)
	}

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0)

	if got := drainSet(p); !got["child"] {
		t.Errorf("child should be released into pending batch after parent ACCEPTED; got %v", got)
	}
}

// TestApplyTerminalStatuses_CascadesRejectedChildren verifies that
// when a broadcasting parent terminalizes REJECTED, every held
// descendant gets a terminal REJECTED row written and is removed
// from in-flight state without ever broadcasting.
func TestApplyTerminalStatuses_CascadesRejectedChildren(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("parent"))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	_ = drainSet(p) // parent broadcasting; subsequent children of it must hold

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("grandchild", []string{"child"}))); err != nil {
		t.Fatalf("grandchild admit: %v", err)
	}
	if got := drainSet(p); len(got) != 0 {
		t.Fatalf("child and grandchild should both be held; got %v", got)
	}

	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusRejected, Timestamp: time.Now(), ExtraInfo: "bad parent"},
	}, 0, 1)

	if got := drainSet(p); got["child"] || got["grandchild"] {
		t.Errorf("cascaded descendants should NOT enter pending batch; got %v", got)
	}

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
	for txid, reason := range rejected {
		if reason != "parent rejected" {
			t.Errorf("%s ExtraInfo should be \"parent rejected\", got %q", txid, reason)
		}
	}
}

// TestSequentialReleaseDeepChain verifies that a held chain releases
// one link at a time. Under Teranode's parallel bulk processing,
// parent and child can't share a batch — so when grandparent
// ACCEPTED, only parent (one level down) releases. Child stays held
// on parent until parent ACCEPTED in its own batch.
func TestSequentialReleaseDeepChain(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("grandparent"))); err != nil {
		t.Fatalf("grandparent admit: %v", err)
	}
	_ = drainSet(p) // grandparent broadcasting

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("parent", []string{"grandparent"}))); err != nil {
		t.Fatalf("parent admit: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"parent"}))); err != nil {
		t.Fatalf("child admit: %v", err)
	}
	if got := drainSet(p); len(got) != 0 {
		t.Fatalf("parent and child should both be held; got %v", got)
	}

	// grandparent ACCEPTED — releases parent only. Child still held
	// because parent is still in-flight (just queued for broadcast).
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "grandparent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0)

	got := drainSet(p)
	if !got["parent"] {
		t.Errorf("parent should release after grandparent ACCEPTED; got %v", got)
	}
	if got["child"] {
		t.Errorf("child should NOT release yet — parent still in-flight; got %v", got)
	}

	// parent ACCEPTED in its own batch — now child can release.
	p.applyTerminalStatuses(context.Background(), []*models.TransactionStatus{
		{TxID: "parent", Status: models.StatusAcceptedByNetwork, Timestamp: time.Now()},
	}, 1, 0)

	got = drainSet(p)
	if !got["child"] {
		t.Errorf("child should release after parent ACCEPTED; got %v", got)
	}
}

// TestHandleMessage_NoParents_AdmitsNormally is the trivial path: a
// tx with no InputTXIDs lands in the pending batch.
func TestHandleMessage_NoParents_AdmitsNormally(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("lone"))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	if got := drainSet(p); !got["lone"] {
		t.Errorf("lone tx should be in pending batch; got %v", got)
	}
}

// TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly verifies
// that a child whose declared parent isn't tracked by Arcade at all
// (mined long ago, never seen, whatever) is admitted normally — only
// IN-FLIGHT parents block.
func TestHandleMessage_ParentNotInFlight_AdmitsChildDirectly(t *testing.T) {
	ms := newMockStore()
	p, cancel := newPropagatorForDepTest(t, ms)
	defer cancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsgWithParents("child", []string{"someParentNotInFlight"}))); err != nil {
		t.Fatalf("admit: %v", err)
	}

	if got := drainSet(p); !got["child"] {
		t.Errorf("child should be admitted directly when parent is not in flight; got %v", got)
	}
}

// TestHandleMessage_MaxPendingFull_BlocksUntilDrained verifies the
// pending-cap backpressure: handleMessage BLOCKS when pending is at
// its configured cap, then unblocks after a drain frees capacity.
// No DLQ, no error to the consumer wrapper — just natural
// backpressure flowing back to the broker.
func TestHandleMessage_MaxPendingFull_BlocksUntilDrained(t *testing.T) {
	ms := newMockStore()
	cfg := &config.Config{}
	cfg.Propagation.MaxPending = 1
	tc := teranode.NewClient(nil, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, ms, nil, tc, nil)
	defer p.dispatcherCancel()

	if err := p.handleMessage(context.Background(), consumerMsg(makePropMsg("tx1"))); err != nil {
		t.Fatalf("first admit: %v", err)
	}

	tx2Done := make(chan error, 1)
	go func() {
		tx2Done <- p.handleMessage(context.Background(), consumerMsg(makePropMsg("tx2")))
	}()

	select {
	case err := <-tx2Done:
		t.Fatalf("tx2 admit should have blocked while pending was full; returned err=%v", err)
	case <-time.After(100 * time.Millisecond):
		// expected: still blocked
	}

	pending := drainSet(p)
	if !pending["tx1"] {
		t.Errorf("tx1 should be in pending batch; got %v", pending)
	}
	if pending["tx2"] {
		t.Errorf("tx2 should not have been in the drained batch yet; got %v", pending)
	}

	select {
	case err := <-tx2Done:
		if err != nil {
			t.Errorf("tx2 admit should complete after drain; got err=%v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("tx2 admit didn't unblock after pending drain")
	}

	if got := drainSet(p); !got["tx2"] {
		t.Errorf("tx2 should be in pending batch after unblocking; got %v", got)
	}
}
