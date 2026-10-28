package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/broadcaster/config"
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
	// One slot, so the next row can only be dispatched after row 0's worker
	// has recorded its error and released the slot: the recheck after
	// acquiring a slot must see the failure. With more slots the count
	// depended on when row 0's goroutine happened to run, and the test failed
	// intermittently under load even though forEach was right.
	const conc = 1
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
	if got := started.Load(); got != 1 {
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

// lateCancelCtx reports no error for the first `quiet` calls to Err() and
// context.Canceled thereafter, and never closes Done().
//
// It exists because the branch under test is otherwise unreachable on demand:
// it runs only when the semaphore send wins a random select against a ctx
// that is already done, and closing Done() makes the select take the other
// branch essentially every time. Holding Done() open forces the send branch,
// and the Err() counter puts the cancellation exactly where the dispatcher
// looks after acquiring its slot. A real context never behaves this way — the
// double is isolating one observation, not modelling a context.
// The four methods are spelled out rather than embedding a context.Context:
// embedding one in a struct is what containedctx flags, and the lint is right
// about the usual case even though this is the exception — a type whose whole
// purpose is to BE a context.
type lateCancelCtx struct {
	done  chan struct{}
	calls atomic.Int64
	quiet int64
}

func (c *lateCancelCtx) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *lateCancelCtx) Done() <-chan struct{} { return c.done }

func (c *lateCancelCtx) Value(any) any { return nil }

func (c *lateCancelCtx) Err() error {
	if c.calls.Add(1) <= c.quiet {
		return nil
	}
	return context.Canceled
}

// TestForEach_ReportsCancellationSeenAfterTheSlot: a cancellation the
// dispatcher first observes AFTER acquiring its semaphore slot must be
// recorded, not just used to break out of the loop.
//
// Callers read a nil error as "every row was written". SetMinedByTxIDs hands
// this error straight to the bump-builder, which withholds the block's
// processed_at stamp on failure and takes it on success — so a forEach that
// skipped rows and returned nil retires a block with rows unwritten, which is
// exactly the trap that stamp ordering exists to close.
func TestForEach_ReportsCancellationSeenAfterTheSlot(t *testing.T) {
	// Three quiet Err() calls — the dispatcher consults ctx twice per item,
	// before the send and after the slot: i=0 pre-send, i=0 post-slot, i=1
	// pre-send. The fourth is i=1's post-slot check, which is the branch under
	// test. Getting this count wrong lands the cancellation on a pre-send
	// check instead, which has always recorded, so the assertion below on the
	// number of fn calls is what keeps the test honest about which branch ran.
	ctx := &lateCancelCtx{done: make(chan struct{}), quiet: 3}
	var started atomic.Int64

	err := forEach(ctx, 50, 1, func(int) error {
		started.Add(1)
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled — a loop truncated by cancellation must not report success", err)
	}
	// Exactly one call means the loop really did stop at the post-slot check
	// for i=1, rather than the test passing via some earlier branch.
	if got := started.Load(); got != 1 {
		t.Fatalf("fn called %d times, want 1 — the cancellation was expected at i=1's post-slot check", got)
	}
}

// poolSettings must keep the idle floor small and independent of the pool
// ceiling. The driver holds MinPoolSize open to EVERY replica-set member, per
// process, forever; a floor that tracks max_pool_size (the old
// min(maxConnecting, max_pool_size)) pinned each pod at its ceiling on all
// three nodes and took a shared M10 to ~1450 of its 1500 connections.
//
// Precedence for each bound is the dedicated setting, then the URI option,
// then this package's default: a deployment that sizes its pool in the URI
// and leaves max_pool_size at 0 must keep the URI's ceiling, not silently
// get defaultMaxPoolSize.
func TestPoolSettings(t *testing.T) {
	tests := []struct {
		name     string
		cfg      config.Mongo
		uri      string
		wantMax  uint64
		wantMin  uint64
		wantIdle time.Duration
	}{
		{"defaults", config.Mongo{}, "", defaultMaxPoolSize, defaultMinPoolSize, defaultMaxConnIdle},
		{"floor stays small under a small ceiling", config.Mongo{MaxPoolSize: 12}, "", 12, defaultMinPoolSize, defaultMaxConnIdle},
		{"explicit floor", config.Mongo{MaxPoolSize: 64, MinPoolSize: 6}, "", 64, 6, defaultMaxConnIdle},
		{"floor clamped to ceiling", config.Mongo{MaxPoolSize: 4, MinPoolSize: 10}, "", 4, 4, defaultMaxConnIdle},
		{"explicit idle", config.Mongo{MaxConnIdleMs: 60000}, "", defaultMaxPoolSize, defaultMinPoolSize, time.Minute},
		{"uri ceiling kept when setting is 0", config.Mongo{}, "?maxPoolSize=20", 20, defaultMinPoolSize, defaultMaxConnIdle},
		{"floor clamped to uri ceiling", config.Mongo{MinPoolSize: 30}, "?maxPoolSize=20", 20, 20, defaultMaxConnIdle},
		{"uri unlimited ceiling kept, floor unclamped", config.Mongo{MinPoolSize: 30}, "?maxPoolSize=0", 0, 30, defaultMaxConnIdle},
		{"uri floor and idle kept when settings are 0", config.Mongo{}, "?minPoolSize=5&maxIdleTimeMS=90000", defaultMaxPoolSize, 5, 90 * time.Second},
		{"settings win over uri", config.Mongo{MaxPoolSize: 12, MinPoolSize: 3, MaxConnIdleMs: 1000}, "?maxPoolSize=50&minPoolSize=10&maxIdleTimeMS=90000", 12, 3, time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri := options.Client().ApplyURI("mongodb://127.0.0.1:27017/" + tt.uri)
			gotMax, gotMin, gotIdle := poolSettings(tt.cfg, uri)
			if gotMax != tt.wantMax || gotMin != tt.wantMin || gotIdle != tt.wantIdle {
				t.Fatalf("poolSettings = (%d, %d, %v), want (%d, %d, %v)",
					gotMax, gotMin, gotIdle, tt.wantMax, tt.wantMin, tt.wantIdle)
			}
		})
	}
}

// rowConcurrencyForPool must follow the effective driver ceiling, including a
// URI-sized pool and the URI's maxPoolSize=0 (unlimited).
func TestRowConcurrencyForPool(t *testing.T) {
	for _, tt := range []struct {
		maxPool uint64
		want    int
	}{
		{0, rowConcurrencyCeiling},
		{20, 20 - poolHeadroom},
		{4, 1},
		{rowConcurrencyCeiling + poolHeadroom, rowConcurrencyCeiling},
		{1 << 40, rowConcurrencyCeiling},
	} {
		if got := rowConcurrencyForPool(tt.maxPool); got != tt.want {
			t.Errorf("rowConcurrencyForPool(%d) = %d, want %d", tt.maxPool, got, tt.want)
		}
	}
}
