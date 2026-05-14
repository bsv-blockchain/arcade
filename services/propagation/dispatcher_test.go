package propagation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// runDispatcherForTest spins up a Dispatcher with the given channels and
// returns a teardown function that cancels its context and waits for it
// to exit. Tests use this so they don't have to manage the goroutine
// lifecycle inline.
func runDispatcherForTest(t *testing.T, d *Dispatcher) (cancel func()) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(done)
	}()
	return func() {
		cancelFn()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("dispatcher did not exit within timeout")
		}
	}
}

// drainBatches reads all available batches from the outgoing channel
// until it sees `expected` messages worth of entries or the timeout
// elapses. Returns the entries in arrival order.
func drainBatches(t *testing.T, out <-chan []*inFlightEntry, expected int, timeout time.Duration) []*inFlightEntry {
	t.Helper()
	var got []*inFlightEntry
	deadline := time.After(timeout)
	for len(got) < expected {
		select {
		case batch := <-out:
			got = append(got, batch...)
		case <-deadline:
			return got
		}
	}
	return got
}

// TestDispatcher_NoParents_AdmitsImmediately verifies the baseline case:
// a tx with no in-flight parents flushes to the broadcast workers
// without delay.
func TestDispatcher_NoParents_AdmitsImmediately(t *testing.T) {
	in := make(chan propagationMsg, 1)
	out := make(chan []*inFlightEntry, 1)
	flips := make(chan *models.TransactionStatus)

	d := NewDispatcher(nil, in, out, flips, newOffsetTracker(),
		100, 1 /*batchMaxSize=1 → flush every tx*/, 3, 100, 50*time.Millisecond)
	cancel := runDispatcherForTest(t, d)
	defer cancel()

	in <- propagationMsg{TXID: "tx1", RawTx: []byte{0x01}, KafkaOffset: 1}

	got := drainBatches(t, out, 1, 500*time.Millisecond)
	if len(got) != 1 || got[0].txid != "tx1" {
		t.Fatalf("expected tx1 in batch, got %+v", got)
	}
}

// TestDispatcher_ParentInFlight_HoldsChild verifies that a child whose
// parent is currently in flight is held as a waiter and does NOT appear
// in the outgoing batch until the parent terminalizes.
func TestDispatcher_ParentInFlight_HoldsChild(t *testing.T) {
	in := make(chan propagationMsg, 2)
	out := make(chan []*inFlightEntry, 2)
	flips := make(chan *models.TransactionStatus, 1)

	d := NewDispatcher(nil, in, out, flips, newOffsetTracker(),
		100, 1, 3, 100, 50*time.Millisecond)
	cancel := runDispatcherForTest(t, d)
	defer cancel()

	// Parent first — flushes immediately because no parents.
	in <- propagationMsg{TXID: "parent", RawTx: []byte{0xaa}, KafkaOffset: 1}
	got := drainBatches(t, out, 1, 500*time.Millisecond)
	if len(got) != 1 || got[0].txid != "parent" {
		t.Fatalf("expected parent in batch, got %+v", got)
	}

	// Child arrives. Parent is still in flight (no ACCEPTED yet), so
	// child must be held as a waiter, not admitted.
	in <- propagationMsg{
		TXID:        "child",
		RawTx:       []byte{0xbb},
		InputTXIDs:  []string{"parent"},
		KafkaOffset: 2,
	}

	// Brief wait — if child were going to be admitted, it would happen
	// inside the dispatcher's incoming-handler synchronously.
	select {
	case batch := <-out:
		t.Fatalf("child should be held as waiter, but got batch %+v", batch)
	case <-time.After(100 * time.Millisecond):
	}

	// Now flip parent to ACCEPTED_BY_NETWORK; child should be released.
	flips <- &models.TransactionStatus{TxID: "parent", Status: models.StatusAcceptedByNetwork}

	got = drainBatches(t, out, 1, 500*time.Millisecond)
	if len(got) != 1 || got[0].txid != "child" {
		t.Fatalf("expected child released after parent ACCEPTED, got %+v", got)
	}
}

// TestDispatcher_ParentRejected_CascadesChildren verifies recursive
// rejection: parent's REJECTED status causes the child to be
// terminally rejected without ever being broadcast, and the rejected
// sink is invoked with a parent-rejected reason.
func TestDispatcher_ParentRejected_CascadesChildren(t *testing.T) {
	in := make(chan propagationMsg, 3)
	out := make(chan []*inFlightEntry, 3)
	flips := make(chan *models.TransactionStatus, 2)

	d := NewDispatcher(nil, in, out, flips, newOffsetTracker(),
		100, 1, 3, 100, 50*time.Millisecond)

	var (
		mu       sync.Mutex
		rejected []struct{ txid, reason string }
	)
	d.SetRejectedSink(func(txid, reason string) {
		mu.Lock()
		defer mu.Unlock()
		rejected = append(rejected, struct{ txid, reason string }{txid, reason})
	})

	cancel := runDispatcherForTest(t, d)
	defer cancel()

	in <- propagationMsg{TXID: "parent", RawTx: []byte{0xaa}, KafkaOffset: 1}
	drainBatches(t, out, 1, 500*time.Millisecond) // consume parent's batch

	in <- propagationMsg{
		TXID: "child", RawTx: []byte{0xbb}, InputTXIDs: []string{"parent"}, KafkaOffset: 2,
	}
	in <- propagationMsg{
		TXID: "grandchild", RawTx: []byte{0xcc}, InputTXIDs: []string{"child"}, KafkaOffset: 3,
	}

	// Give the dispatcher a moment to register both as waiters.
	time.Sleep(50 * time.Millisecond)

	// 400 maps to terminal REJECTED per isRetryableStatusCode.
	flips <- &models.TransactionStatus{TxID: "parent", Status: models.StatusRejected, StatusCode: 400, ExtraInfo: "bad parent"}

	// Wait for the cascade to complete.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(rejected)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(rejected) != 2 {
		t.Fatalf("expected 2 cascade rejections, got %d: %+v", len(rejected), rejected)
	}
	gotTxs := map[string]bool{}
	for _, r := range rejected {
		gotTxs[r.txid] = true
		if r.reason == "" {
			t.Errorf("rejection for %s has empty reason", r.txid)
		}
	}
	if !gotTxs["child"] || !gotTxs["grandchild"] {
		t.Errorf("expected child and grandchild rejected, got %v", gotTxs)
	}

	// No batch should have been emitted for child or grandchild.
	select {
	case batch := <-out:
		t.Fatalf("cascaded children should not be broadcast, got %+v", batch)
	default:
	}
}

// TestDispatcher_MultipleParents_HoldsUntilAllAccepted verifies the
// multi-parent case: a child with two in-flight parents stays held
// until BOTH have terminalized, not just one.
func TestDispatcher_MultipleParents_HoldsUntilAllAccepted(t *testing.T) {
	in := make(chan propagationMsg, 3)
	out := make(chan []*inFlightEntry, 3)
	flips := make(chan *models.TransactionStatus, 2)

	d := NewDispatcher(nil, in, out, flips, newOffsetTracker(),
		100, 1, 3, 100, 50*time.Millisecond)
	cancel := runDispatcherForTest(t, d)
	defer cancel()

	in <- propagationMsg{TXID: "p1", RawTx: []byte{0xa1}, KafkaOffset: 1}
	in <- propagationMsg{TXID: "p2", RawTx: []byte{0xa2}, KafkaOffset: 2}
	drainBatches(t, out, 2, 500*time.Millisecond) // consume parents

	in <- propagationMsg{
		TXID: "child", RawTx: []byte{0xcc}, InputTXIDs: []string{"p1", "p2"}, KafkaOffset: 3,
	}
	time.Sleep(50 * time.Millisecond)

	// First parent ACCEPTED — child should still be held (p2 unmet).
	flips <- &models.TransactionStatus{TxID: "p1", Status: models.StatusAcceptedByNetwork}
	select {
	case batch := <-out:
		t.Fatalf("child released after only one parent ACCEPTED: %+v", batch)
	case <-time.After(100 * time.Millisecond):
	}

	// Second parent ACCEPTED — child should now be released.
	flips <- &models.TransactionStatus{TxID: "p2", Status: models.StatusAcceptedByNetwork}
	got := drainBatches(t, out, 1, 500*time.Millisecond)
	if len(got) != 1 || got[0].txid != "child" {
		t.Fatalf("expected child released after both parents ACCEPTED, got %+v", got)
	}
}

// TestDispatcher_RetryableFailure_Requeues verifies that a retryable
// status (422 ErrTxMissingParent) puts the tx back in the queue with a
// short backoff and re-admits it to a batch when the backoff elapses.
func TestDispatcher_RetryableFailure_Requeues(t *testing.T) {
	in := make(chan propagationMsg, 1)
	out := make(chan []*inFlightEntry, 2)
	flips := make(chan *models.TransactionStatus, 1)

	// retryBackoffMs=10 so the test doesn't have to wait long.
	d := NewDispatcher(nil, in, out, flips, newOffsetTracker(),
		100, 1, 3, 10, 50*time.Millisecond)
	cancel := runDispatcherForTest(t, d)
	defer cancel()

	in <- propagationMsg{TXID: "tx1", RawTx: []byte{0x01}, KafkaOffset: 1}
	got := drainBatches(t, out, 1, 500*time.Millisecond)
	if len(got) != 1 || got[0].txid != "tx1" {
		t.Fatalf("expected initial broadcast of tx1, got %+v", got)
	}

	// 422 → retryable.
	flips <- &models.TransactionStatus{TxID: "tx1", Status: models.StatusRejected, StatusCode: 422}

	// Retry should fire after the backoff elapses.
	got = drainBatches(t, out, 1, 500*time.Millisecond)
	if len(got) != 1 || got[0].txid != "tx1" {
		t.Fatalf("expected tx1 retried, got %+v", got)
	}
	if got[0].retryAttempts != 1 {
		t.Errorf("expected retryAttempts=1, got %d", got[0].retryAttempts)
	}
}

// TestDispatcher_RetryExhausted_Terminates verifies that once
// retryMaxAttempts is exceeded, the tx is terminally rejected rather
// than re-queued indefinitely.
func TestDispatcher_RetryExhausted_Terminates(t *testing.T) {
	in := make(chan propagationMsg, 1)
	out := make(chan []*inFlightEntry, 5)
	flips := make(chan *models.TransactionStatus, 5)

	// retryMaxAttempts=2 + small backoff.
	d := NewDispatcher(nil, in, out, flips, newOffsetTracker(),
		100, 1, 2, 5, 50*time.Millisecond)

	var (
		mu       sync.Mutex
		rejected []struct{ txid, reason string }
	)
	d.SetRejectedSink(func(txid, reason string) {
		mu.Lock()
		defer mu.Unlock()
		rejected = append(rejected, struct{ txid, reason string }{txid, reason})
	})

	cancel := runDispatcherForTest(t, d)
	defer cancel()

	in <- propagationMsg{TXID: "tx1", RawTx: []byte{0x01}, KafkaOffset: 1}

	// Flip 422 repeatedly until exhaustion. We drain each retry's batch
	// before flipping again so the dispatcher actually picks up the
	// re-admission before the next flip arrives.
	for attempt := 1; attempt <= 3; attempt++ {
		got := drainBatches(t, out, 1, 500*time.Millisecond)
		if len(got) != 1 {
			t.Fatalf("attempt %d: expected one batch entry, got %+v", attempt, got)
		}
		flips <- &models.TransactionStatus{TxID: "tx1", Status: models.StatusRejected, StatusCode: 422}
	}

	// Allow the terminal rejection to land.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(rejected)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	if len(rejected) != 0 {
		t.Errorf("did not expect direct rejection through sink, got %+v", rejected)
	}
	mu.Unlock()

	// Behavioral check: a fourth flip arrives for the same txid. After
	// exhaustion the dispatcher has removed tx1 from the in-flight set
	// and should treat this flip as unknown — meaning no further batch
	// is admitted and no retry is scheduled. We wait past one full
	// backoff window to be sure.
	flips <- &models.TransactionStatus{TxID: "tx1", Status: models.StatusRejected, StatusCode: 422}
	select {
	case batch := <-out:
		t.Fatalf("expected no further broadcast after exhaustion, got %+v", batch)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestDispatcher_BatchFlushDeadline_FiresOnTimeout verifies that a
// pending batch is flushed when the flush-timeout elapses, even if the
// batch size threshold hasn't been reached.
func TestDispatcher_BatchFlushDeadline_FiresOnTimeout(t *testing.T) {
	in := make(chan propagationMsg, 3)
	out := make(chan []*inFlightEntry, 1)
	flips := make(chan *models.TransactionStatus)

	// batchMaxSize=100 → never flushes by size; batchFlushTimeout=20ms
	// → flushes by deadline.
	d := NewDispatcher(nil, in, out, flips, newOffsetTracker(),
		100, 100, 3, 100, 20*time.Millisecond)
	cancel := runDispatcherForTest(t, d)
	defer cancel()

	in <- propagationMsg{TXID: "tx1", RawTx: []byte{0x01}, KafkaOffset: 1}
	in <- propagationMsg{TXID: "tx2", RawTx: []byte{0x02}, KafkaOffset: 2}

	got := drainBatches(t, out, 2, 500*time.Millisecond)
	if len(got) != 2 {
		t.Fatalf("expected batch of 2 after deadline, got %d entries: %+v", len(got), got)
	}
}

// TestOffsetTracker_LowestUnfinished verifies the lazy-delete heap
// returns the smallest non-done offset even when done entries are
// interleaved among the heap's interior.
func TestOffsetTracker_LowestUnfinished(t *testing.T) {
	tr := newOffsetTracker()

	for _, off := range []int64{5, 1, 3, 4, 2} {
		tr.Add(off)
	}

	lowest, ok := tr.LowestUnfinished()
	if !ok || lowest != 1 {
		t.Fatalf("expected lowest=1, got (%d, %v)", lowest, ok)
	}

	tr.Done(1)
	lowest, ok = tr.LowestUnfinished()
	if !ok || lowest != 2 {
		t.Fatalf("after Done(1), expected lowest=2, got (%d, %v)", lowest, ok)
	}

	tr.Done(3) // interior, not top
	lowest, ok = tr.LowestUnfinished()
	if !ok || lowest != 2 {
		t.Fatalf("Done on interior should not affect top, expected 2, got %d", lowest)
	}

	tr.Done(2) // top — now top should advance past the previously-done 3
	lowest, ok = tr.LowestUnfinished()
	if !ok || lowest != 4 {
		t.Fatalf("after Done(2), expected lowest=4 (skipping done 3), got %d", lowest)
	}

	tr.Done(4)
	tr.Done(5)
	if _, ok := tr.LowestUnfinished(); ok {
		t.Errorf("expected empty tracker after all done")
	}
	if !tr.Empty() {
		t.Errorf("expected Empty() true after all done")
	}
}
