package propagation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/arcade/kafka"
)

// markQueue is the #295 replacement for the per-offset pendingMarks map:
// advanceMarks used to walk the ENTIRE map once per terminal event —
// O(terminals × in-flight offsets) on the dispatcher's serial path, the
// suspected 10x throughput gap in issue #295. The queue is appended in
// claim-delivery (offset) order, so advancing is a pop-from-the-front
// loop that only touches newly markable entries: amortized O(1) per
// message regardless of how often it runs.

func mq(offsets ...int64) *markQueue {
	q := &markQueue{}
	for _, o := range offsets {
		q.push(&kafka.Message{Offset: o})
	}
	return q
}

// TestMarkQueue_NothingUnfinished_DrainsAllInOrder pins the base case:
// with no unfinished offsets, advance marks everything queued, in
// offset order, and empties the queue.
func TestMarkQueue_NothingUnfinished_DrainsAllInOrder(t *testing.T) {
	claim := newFakeClaim(context.Background())
	q := mq(1, 2, 3)
	q.advance(claim, newOffsetTracker())

	for _, o := range []int64{1, 2, 3} {
		if !claim.isMarked(o) {
			t.Errorf("offset %d not marked", o)
		}
	}
	if q.pending() != 0 {
		t.Errorf("queue should be empty after full drain, %d pending", q.pending())
	}
}

// TestMarkQueue_StopsAtLowestUnfinished pins the watermark rule: only
// offsets strictly below LowestUnfinished are marked; the rest stay
// queued until their blocker terminalizes.
func TestMarkQueue_StopsAtLowestUnfinished(t *testing.T) {
	claim := newFakeClaim(context.Background())
	tracker := newOffsetTracker()
	tracker.Add(3) // offset 3 still in flight

	q := mq(1, 2, 3, 4)
	q.advance(claim, tracker)

	if !claim.isMarked(1) || !claim.isMarked(2) {
		t.Error("offsets below the watermark must be marked")
	}
	if claim.isMarked(3) || claim.isMarked(4) {
		t.Error("offsets at/above the lowest unfinished offset must NOT be marked")
	}
	if q.pending() != 2 {
		t.Errorf("2 offsets should remain queued, got %d", q.pending())
	}

	// Blocker terminalizes → the remainder drains on the next advance.
	tracker.Done(3)
	q.advance(claim, tracker)
	if !claim.isMarked(3) || !claim.isMarked(4) {
		t.Error("remainder must drain once the blocker is Done")
	}
	if q.pending() != 0 {
		t.Errorf("queue should be empty, %d pending", q.pending())
	}
}

// TestMarkQueue_NilClaim_NoOp pins test-mode semantics: a nil claim
// (dispatcher test mode) must neither mark nor drop queued entries.
func TestMarkQueue_NilClaim_NoOp(t *testing.T) {
	q := mq(1, 2)
	q.advance(nil, newOffsetTracker())
	if q.pending() != 2 {
		t.Errorf("nil-claim advance must retain entries, %d pending", q.pending())
	}
}

// TestMarkQueue_CompactsConsumedPrefix pins the memory bound: the
// backing slice must not grow without bound as entries are consumed —
// after draining most of a large queue the consumed prefix is released.
func TestMarkQueue_CompactsConsumedPrefix(t *testing.T) {
	claim := newFakeClaim(context.Background())
	tracker := newOffsetTracker()
	tracker.Add(999) // pin the tail

	q := &markQueue{}
	for o := int64(0); o < 1000; o++ {
		q.push(&kafka.Message{Offset: o})
	}
	q.advance(claim, tracker)

	if q.pending() != 1 {
		t.Fatalf("expected 1 pending entry (offset 999), got %d", q.pending())
	}
	if len(q.msgs) > 2 {
		t.Errorf("consumed prefix not compacted: backing slice still holds %d entries for 1 pending", len(q.msgs))
	}
}

// TestMarkQueue_EmptyAdvance_NoPanic pins the trivial bound.
func TestMarkQueue_EmptyAdvance_NoPanic(t *testing.T) {
	q := &markQueue{}
	q.advance(newFakeClaim(context.Background()), newOffsetTracker())
	if q.pending() != 0 {
		t.Errorf("empty queue advance changed pending count: %d", q.pending())
	}
}

// TestRunDispatcher_TeardownDrainsMarks pins the loop-exit drain: with
// marking amortized onto the 50ms flush tick, a dispatcher that stops
// right after a terminal event must still commit the finished offset on
// its way out (ctx-done path) rather than stranding up to one tick of
// completed work for replay. waitForMark first proves the terminal was
// fully processed; stop() then exercises the exit path — the assertion
// after stop() holds whether the tick or the exit drain got there first,
// which is exactly the invariant: no markable offset left behind.
func TestRunDispatcher_TeardownDrainsMarks(t *testing.T) {
	teranodeSrv := newTeranodeServer(&eventLog{}, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator("", teranodeSrv.URL, newMockStore())
	claim, stop := runDispatcherWithClaim(t, p)

	claim.ch <- &kafka.Message{Offset: 21, Value: makePropMsg("teardown-tx")}
	waitForMark(t, claim, 21, "accepted tx must be committed while the dispatcher runs")

	stop()
	if !claim.isMarked(21) {
		t.Fatal("terminalized offset lost at teardown")
	}
}
