package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// awaitWithin must return ctx's error once the deadline passes even though fn
// is still blocked — that is the whole point, since the SRV lookup it wraps
// cannot be cancelled — and must return fn's value promptly when fn is fast.
func TestAwaitWithin(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := awaitWithin(ctx, func() int { <-release; return 1 })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked fn: err = %v, want context.DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("blocked fn: waited %s, the deadline must bound the caller", waited)
	}

	v, err := awaitWithin(context.Background(), func() string { return "ok" })
	if err != nil || v != "ok" {
		t.Fatalf("fast fn: (%q, %v), want (\"ok\", nil)", v, err)
	}
}

// forEach must stop dispatching after the first failure and must not start
// work under a context that is already done. Both were false before: the loop
// only ever consulted ctx inside a select that picks at random when a slot is
// free too, and a row error never stopped the remaining rows from starting —
// so with the server gone, a million-row block paid op_timeout_ms per row.
func TestForEach_StopsDispatchingOnFailureAndDoneContext(t *testing.T) {
	const n = 10_000
	var started atomic.Int64
	const conc = 16
	err := forEach(context.Background(), n, conc, func(i int) error {
		started.Add(1)
		if i == 0 {
			return errors.New("row 0 failed")
		}
		return nil
	})
	if err == nil || err.Error() != "row 0 failed" {
		t.Fatalf("err = %v, want the first failure", err)
	}
	if got := started.Load(); got > int64(conc*4) {
		t.Fatalf("started %d of %d calls after the first failure; must stop dispatching", got, n)
	}

	started.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = forEach(ctx, n, conc, func(int) error { started.Add(1); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := started.Load(); got != 0 {
		t.Fatalf("started %d calls under a cancelled context, want 0", got)
	}
}

// dropDuplicateTail must remove tail entries already reported in the prefix,
// collapse repeats within the tail, keep order, and — the property the
// comment promises — never allocate for the prefix: its set is sized by the
// tail.
func TestDropDuplicateTail(t *testing.T) {
	got := dropDuplicateTail([]string{"a", "b", "c", "b", "d", "d", "e", "a"}, 3)
	want := []string{"a", "b", "c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if got := dropDuplicateTail([]string{"a", "b"}, 2); len(got) != 2 {
		t.Fatalf("empty tail must be a no-op, got %v", got)
	}
	// A large prefix with a one-entry tail: the map must be tail-sized. The
	// allocation is measured because that is the whole point of the design.
	in := make([]string, 0, 200_001)
	for i := 0; i < 200_000; i++ {
		in = append(in, fmt.Sprintf("tx-%06d", i))
	}
	in = append(in, "straggler")
	prefix := in[:200_000]
	allocs := testing.AllocsPerRun(3, func() { _ = dropDuplicateTail(in, len(prefix)) })
	if allocs > 8 {
		t.Fatalf("dropDuplicateTail allocated %.0f times for a one-entry tail; its set must be sized by the tail, not the prefix", allocs)
	}
}
