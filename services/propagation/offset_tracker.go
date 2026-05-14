package propagation

import "container/heap"

// minHeapOffsetTracker is a min-heap of in-flight Kafka offsets backed
// by a "done" set for lazy deletion. Add is O(log n); Done is O(1);
// LowestUnfinished is amortized O(log n) across the lifetime of an
// offset because each Done entry is cleaned up exactly once during a
// subsequent LowestUnfinished or Empty call.
//
// The lazy-delete approach avoids paying the O(n) cost of removing an
// arbitrary entry from a heap. Done flags simply mark offsets as
// terminalized; the heap's top is purged of done entries the next time
// the caller asks for the lowest unfinished or Empty status.
type minHeapOffsetTracker struct {
	heap *offsetMinHeap
	done map[int64]struct{}
}

func newOffsetTracker() *minHeapOffsetTracker {
	return &minHeapOffsetTracker{
		heap: &offsetMinHeap{},
		done: make(map[int64]struct{}),
	}
}

func (t *minHeapOffsetTracker) Add(offset int64) {
	heap.Push(t.heap, offset)
}

func (t *minHeapOffsetTracker) Done(offset int64) {
	t.done[offset] = struct{}{}
}

func (t *minHeapOffsetTracker) LowestUnfinished() (int64, bool) {
	t.cleanTop()
	if t.heap.Len() == 0 {
		return 0, false
	}
	return (*t.heap)[0], true
}

func (t *minHeapOffsetTracker) Empty() bool {
	t.cleanTop()
	return t.heap.Len() == 0
}

// cleanTop pops done entries from the heap's top until either the heap
// is empty or its top is an unfinished offset. Called by every query
// method so the heap top is always accurate after the call returns.
func (t *minHeapOffsetTracker) cleanTop() {
	for t.heap.Len() > 0 {
		top := (*t.heap)[0]
		if _, isDone := t.done[top]; !isDone {
			return
		}
		heap.Pop(t.heap)
		delete(t.done, top)
	}
}

// offsetMinHeap is the standard library heap.Interface implementation
// for int64 offsets. All methods are pointer-receiver — Push/Pop need
// it to mutate the slice header, and consistency on the read methods
// keeps the linter happy and removes any ambiguity about which receiver
// to use at call sites.
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
