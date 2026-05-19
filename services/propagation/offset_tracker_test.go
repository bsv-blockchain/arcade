package propagation

import "testing"

// The offsetTracker is correctness-critical: it gates how far the Kafka
// commit watermark can advance, which determines what work gets
// replayed after a crash. The dispatcher's depaware tests cover it
// indirectly; these focused tests pin the heap/lazy-deletion behavior
// at the contract level so a future refactor can't silently regress
// the LowestUnfinished/Empty semantics.

func TestOffsetTracker_EmptyOnInit(t *testing.T) {
	tr := newOffsetTracker()
	if !tr.Empty() {
		t.Fatal("new tracker should be Empty")
	}
	if got, ok := tr.LowestUnfinished(); ok || got != 0 {
		t.Fatalf("LowestUnfinished on empty = (%d, %v), want (0, false)", got, ok)
	}
}

func TestOffsetTracker_AddDoneOrdered(t *testing.T) {
	tr := newOffsetTracker()
	tr.Add(10)
	tr.Add(20)
	tr.Add(30)

	for _, want := range []int64{10, 20, 30} {
		got, ok := tr.LowestUnfinished()
		if !ok {
			t.Fatalf("LowestUnfinished want (%d, true), got (%d, false)", want, got)
		}
		if got != want {
			t.Fatalf("LowestUnfinished = %d, want %d", got, want)
		}
		tr.Done(got)
	}
	if !tr.Empty() {
		t.Fatal("tracker should be Empty after all offsets done")
	}
}

// Out-of-order Done is the case the dispatcher hits constantly: txs
// terminate in broadcast order, not Kafka offset order. The watermark
// must stay pinned at the lowest unfinished offset until the
// earlier-offset tx itself terminates.
func TestOffsetTracker_DoneOutOfOrder_PinsWatermark(t *testing.T) {
	tr := newOffsetTracker()
	tr.Add(1)
	tr.Add(2)
	tr.Add(3)

	tr.Done(2) // middle finishes first
	if got, ok := tr.LowestUnfinished(); !ok || got != 1 {
		t.Fatalf("after Done(2), LowestUnfinished = (%d, %v), want (1, true) — watermark must not jump past offset 1", got, ok)
	}

	tr.Done(3) // tail finishes
	if got, ok := tr.LowestUnfinished(); !ok || got != 1 {
		t.Fatalf("after Done(3), LowestUnfinished = (%d, %v), want (1, true) — offset 1 still in-flight", got, ok)
	}

	tr.Done(1) // head finally finishes; lazy deletion sweeps 2 and 3 off
	if !tr.Empty() {
		t.Fatal("after Done(1), tracker should be Empty — lazy deletion should sweep 2 and 3")
	}
}

// Duplicate Done is the offset-redelivery path: handleAdmit Adds the
// new offset then Dones it immediately on a duplicate-txid admission.
// If the original tx later terminates, calling Done on its already-done
// offset must remain a no-op.
func TestOffsetTracker_DoneIdempotent(t *testing.T) {
	tr := newOffsetTracker()
	tr.Add(100)
	tr.Done(100)
	tr.Done(100) // second Done — must not panic, must not corrupt state
	if !tr.Empty() {
		t.Fatal("tracker should be Empty after Done(100); idempotent Done must not resurrect the offset")
	}
}

// Done called on an offset that was never Added is a no-op — keeps
// the API safe against callers that pre-mark or defensively mark.
func TestOffsetTracker_DoneUnknownOffset_NoOp(t *testing.T) {
	tr := newOffsetTracker()
	tr.Done(999) // never added
	if !tr.Empty() {
		t.Fatal("Done on unknown offset must not allocate an in-flight entry — tracker should remain Empty")
	}
	// Adding it afterwards: behavior is "previously marked done, so
	// the Add lands on a heap entry the first cleanTop will sweep".
	// This case mirrors the redelivery race; we just want to confirm
	// it doesn't leave a permanently stuck watermark.
	tr.Add(999)
	if !tr.Empty() {
		t.Fatal("Add(999) after Done(999) should still see the offset as done — watermark must not pin")
	}
}

// Interleaved Add / Done with later-added smaller offsets — checks
// that the heap invariant survives a Kafka redelivery that lands an
// earlier offset after newer ones have been admitted.
func TestOffsetTracker_LowerOffsetAddedLater(t *testing.T) {
	tr := newOffsetTracker()
	tr.Add(50)
	tr.Add(60)
	tr.Done(50)
	if got, ok := tr.LowestUnfinished(); !ok || got != 60 {
		t.Fatalf("after Done(50), LowestUnfinished = (%d, %v), want (60, true)", got, ok)
	}

	// A delayed-replay style Add of an earlier offset must drop the
	// LowestUnfinished back to that earlier offset.
	tr.Add(40)
	if got, ok := tr.LowestUnfinished(); !ok || got != 40 {
		t.Fatalf("after Add(40), LowestUnfinished = (%d, %v), want (40, true) — heap should re-pin watermark", got, ok)
	}
	tr.Done(40)
	if got, ok := tr.LowestUnfinished(); !ok || got != 60 {
		t.Fatalf("after Done(40), LowestUnfinished = (%d, %v), want (60, true)", got, ok)
	}
	tr.Done(60)
	if !tr.Empty() {
		t.Fatal("tracker should be Empty after all offsets done")
	}
}

// LowestUnfinished must be repeatable: calling it back-to-back without
// state mutation in between returns the same value. Guards against a
// cleanTop bug that mutates state on read past what the contract allows.
func TestOffsetTracker_LowestUnfinished_Repeatable(t *testing.T) {
	tr := newOffsetTracker()
	tr.Add(7)
	tr.Add(8)

	got1, ok1 := tr.LowestUnfinished()
	got2, ok2 := tr.LowestUnfinished()
	if got1 != got2 || ok1 != ok2 {
		t.Fatalf("LowestUnfinished not repeatable: first (%d, %v), second (%d, %v)", got1, ok1, got2, ok2)
	}
}

// Stress-style mix: Add a batch, then interleave Dones in a non-trivial
// pattern. Locks in the behavior under the kind of load the dispatcher
// sees in production.
func TestOffsetTracker_BatchMix(t *testing.T) {
	tr := newOffsetTracker()
	for i := int64(1); i <= 10; i++ {
		tr.Add(i)
	}
	// Done in reverse order. LowestUnfinished must hold at 1 the whole way.
	for i := int64(10); i > 1; i-- {
		tr.Done(i)
		if got, ok := tr.LowestUnfinished(); !ok || got != 1 {
			t.Fatalf("after Done(%d), LowestUnfinished = (%d, %v), want (1, true) — watermark must hold at the unfinished head", i, got, ok)
		}
	}
	tr.Done(1)
	if !tr.Empty() {
		t.Fatal("tracker should be Empty after Done(1); lazy deletion must have swept the rest")
	}
}
