package propagation

import "container/heap"

// offsetTracker tracks in-flight Kafka offsets for the dispatcher's
// deferred-commit mechanism. Add records an offset on admission to the
// dispatcher's in-flight set; Done marks an offset as terminalized.
// LowestUnfinished returns the smallest offset that is still in-flight,
// or (0, false) when nothing is in-flight. The propagator's watermark
// ticker reads LowestUnfinished and tells the Kafka consumer to mark
// every pending message at or below that offset.
//
// Backed by a min-heap with lazy deletion: Done flags the offset done
// without removing it from the heap, and the next LowestUnfinished call
// purges any done entries off the top before reading. This keeps Add and
// Done O(log n) and O(1) respectively, and amortizes the cost of
// removing terminalized offsets across subsequent LowestUnfinished calls.
//
// All methods are intended to be called from a single goroutine (the
// dispatcher's runDispatcher loop owns this state). No internal locking.
type offsetTracker struct {
	heap *offsetMinHeap
	done map[int64]struct{}
}

// newOffsetTracker constructs an empty offsetTracker.
func newOffsetTracker() *offsetTracker {
	return &offsetTracker{
		heap: &offsetMinHeap{},
		done: make(map[int64]struct{}),
	}
}

// Add records an offset as in-flight. The dispatcher calls Add when it
// admits a Kafka message into pendingMsgs or heldMsgs.
func (t *offsetTracker) Add(offset int64) {
	heap.Push(t.heap, offset)
}

// Done marks an in-flight offset as terminalized. Idempotent: marking the
// same offset twice is a no-op the second time. The dispatcher calls Done
// after a tx reaches a terminal status (ACCEPTED_BY_NETWORK, REJECTED) or
// is cascade-rejected as a child of a rejected parent.
func (t *offsetTracker) Done(offset int64) {
	t.done[offset] = struct{}{}
}

// LowestUnfinished returns the smallest offset still in-flight, or (0,
// false) when every offset has been marked done (or none have been
// added). The Kafka commit watermark must not advance past this value:
// if it did, a crash would leave the in-flight tx unreplayed.
//
// Cleans done entries off the heap top as a side effect so the heap top
// is always an unfinished offset after a successful call returns.
func (t *offsetTracker) LowestUnfinished() (int64, bool) {
	t.cleanTop()
	if t.heap.Len() == 0 {
		return 0, false
	}
	return (*t.heap)[0], true
}

// Empty reports whether every added offset has been marked done. Used by
// tests; production code uses LowestUnfinished.
func (t *offsetTracker) Empty() bool {
	t.cleanTop()
	return t.heap.Len() == 0
}

// cleanTop pops done entries off the heap top until either the heap is
// empty or the top is an unfinished offset.
func (t *offsetTracker) cleanTop() {
	for t.heap.Len() > 0 {
		top := (*t.heap)[0]
		if _, isDone := t.done[top]; !isDone {
			return
		}
		heap.Pop(t.heap)
		delete(t.done, top)
	}
}

// offsetMinHeap is container/heap.Interface over a slice of int64
// offsets, min-heap ordering. Pointer-receiver methods because Push and
// Pop need to mutate the slice header.
type offsetMinHeap []int64

func (h *offsetMinHeap) Len() int           { return len(*h) }
func (h *offsetMinHeap) Less(i, j int) bool { return (*h)[i] < (*h)[j] }
func (h *offsetMinHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *offsetMinHeap) Push(x any) {
	*h = append(*h, x.(int64))
}

func (h *offsetMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
