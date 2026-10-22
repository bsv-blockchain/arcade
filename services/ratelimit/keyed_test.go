package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/broadcaster/store"
)

// fakeQuota is an exact in-memory QuotaStore that records every request.
type fakeQuota struct {
	mu    sync.Mutex
	used  map[string]int64
	wants []int64
	err   error
}

func newFakeQuota() *fakeQuota { return &fakeQuota{used: map[string]int64{}} }

func (f *fakeQuota) ReserveQuota(_ context.Context, keyID string, w time.Time, want, limit int64) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wants = append(f.wants, want)
	if f.err != nil {
		return 0, 0, f.err
	}
	k := keyID + w.String()
	used := f.used[k]
	next := store.NextQuotaUsed(used, want, limit)
	f.used[k] = next
	return next - used, next, nil
}

func (f *fakeQuota) calls() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.wants) }

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTestKeyed(qs store.QuotaStore, fraction, shrink float64, clk *clock) *keyedLimiter {
	l := newKeyedLimiter(qs, fraction, shrink, clk.now)
	l.spawn = func(f func()) { f() } // prefetch synchronously
	return l
}

var noon = time.Date(2026, 10, 21, 12, 0, 15, 0, time.UTC)

func TestKeyedFreeTierExact(t *testing.T) {
	qs := newFakeQuota()
	l := newTestKeyed(qs, 0.1, 0.8, &clock{noon})
	for i := range 5 {
		d, err := l.take(context.Background(), "key_free", 50, 10)
		require.NoError(t, err)
		require.True(t, d.Allowed, "submit %d", i+1)
	}
	d, err := l.take(context.Background(), "key_free", 50, 10)
	require.NoError(t, err)
	require.False(t, d.Allowed)
	require.Equal(t, int64(0), d.Remaining)
	require.Equal(t, 45*time.Second, d.Reset, "reset is the end of the UTC minute")
	require.Equal(t, 5, qs.calls(), "free tier: the lease equals the cost, one reservation per request until the window is exhausted")
}

func TestKeyedLeaseAmortizesStoreCalls(t *testing.T) {
	qs := newFakeQuota()
	l := newTestKeyed(qs, 0.1, 0.8, &clock{noon})
	for range 100 {
		d, err := l.take(context.Background(), "key_pro", 1000, 1)
		require.NoError(t, err)
		require.True(t, d.Allowed)
	}
	require.Equal(t, int64(100), qs.wants[0], "first lease is lease_fraction × limit")
	require.Equal(t, 2, qs.calls(), "100 requests cost one lease plus one prefetch")
}

func TestKeyedShrinksNearLimit(t *testing.T) {
	qs := newFakeQuota()
	clk := &clock{noon}
	a := newTestKeyed(qs, 0.4, 0.8, clk)
	b := newTestKeyed(qs, 0.4, 0.8, clk)
	a.spawn = func(func()) {}
	b.spawn = func(func()) {}

	_, err := a.take(context.Background(), "key_x", 100, 1) // A leases 40 → shared 40
	require.NoError(t, err)
	var d Decision
	for range 40 { // B leases 40 → shared 80 (shrinking), spends all 40
		d, err = b.take(context.Background(), "key_x", 100, 1)
		require.NoError(t, err)
		require.True(t, d.Allowed)
	}
	d, err = b.take(context.Background(), "key_x", 100, 1)
	require.NoError(t, err)
	require.True(t, d.Allowed)
	require.Equal(t, int64(1), qs.wants[len(qs.wants)-1], "past the shrink threshold a replica leases only the cost")
}

func TestKeyedWindowRollover(t *testing.T) {
	qs := newFakeQuota()
	clk := &clock{noon}
	l := newTestKeyed(qs, 0.1, 0.8, clk)
	for range 5 {
		_, err := l.take(context.Background(), "key_free", 50, 10)
		require.NoError(t, err)
	}
	d, _ := l.take(context.Background(), "key_free", 50, 10)
	require.False(t, d.Allowed)
	clk.t = clk.t.Add(time.Minute)
	d, err := l.take(context.Background(), "key_free", 50, 10)
	require.NoError(t, err)
	require.True(t, d.Allowed, "a new minute is a new window")
}

func TestKeyedCostAboveLimitAndErrors(t *testing.T) {
	qs := newFakeQuota()
	l := newTestKeyed(qs, 0.1, 0.8, &clock{noon})
	d, err := l.take(context.Background(), "key_free", 50, 60)
	require.NoError(t, err)
	require.False(t, d.Allowed)
	require.Equal(t, 0, qs.calls(), "a cost above the limit never reaches the store")

	qs.err = errors.New("store down")
	_, err = l.take(context.Background(), "key_free", 50, 10)
	require.Error(t, err, "store errors surface so the caller can fail open")
}

func TestKeyedPartialGrantIsKept(t *testing.T) {
	qs := newFakeQuota()
	l := newTestKeyed(qs, 0.1, 0.8, &clock{noon})
	l.spawn = func(func()) {}
	_, err := l.take(context.Background(), "key_b", 25, 1) // lease 3 (ceil 2.5), spend 1 → 2 local
	require.NoError(t, err)
	d, err := l.take(context.Background(), "key_b", 25, 30)
	require.NoError(t, err)
	require.False(t, d.Allowed, "cost above limit")
	d, err = l.take(context.Background(), "key_b", 25, 22) // needs 20 more, shared has 22 left
	require.NoError(t, err)
	require.True(t, d.Allowed)
	d, err = l.take(context.Background(), "key_b", 25, 3) // 0 local, shared has 2 left
	require.NoError(t, err)
	require.False(t, d.Allowed)
	d, err = l.take(context.Background(), "key_b", 25, 2) // the denied request's partial grant stayed local
	require.NoError(t, err)
	require.True(t, d.Allowed, "units granted to a denied request are not lost")
}

func TestKeyedExhaustedWindowMakesNoFurtherReservations(t *testing.T) {
	qs := newFakeQuota()
	l := newTestKeyed(qs, 0.1, 0.8, &clock{noon})
	for i := range 5 {
		d, err := l.take(context.Background(), "key_free", 50, 10)
		require.NoError(t, err)
		require.True(t, d.Allowed, "submit %d", i+1)
	}
	calls := qs.calls()
	require.Equal(t, 5, calls)
	for range 10 {
		d, err := l.take(context.Background(), "key_free", 50, 10)
		require.NoError(t, err)
		require.False(t, d.Allowed)
	}
	require.Equal(t, calls, qs.calls(), "once the shared window is exhausted, denials make no further store calls")
}

// TestKeyedPrefetchErrorTripsBreakerThenRecovers drives a bucket's local
// remaining down to the prefetch threshold, lets a synchronous (spawn is
// overridden to run inline) prefetch fail, and checks the breaker it trips:
// refilling clears, the next miss fails open with no store call, and once
// storeBackoff elapses the bucket reserves again.
func TestKeyedPrefetchErrorTripsBreakerThenRecovers(t *testing.T) {
	qs := newFakeQuota()
	clk := &clock{noon}
	l := newTestKeyed(qs, 0.5, 0.8, clk) // baseLease = ceil(40*0.5) = 20
	const limit = 40

	// The first take leases 20 and spends 1, leaving 19. Draining one more
	// unit per call (all served locally: no store call) brings remaining to
	// 5 after 15 calls and 4 after 16 — crossing the base/4 = 5 prefetch
	// threshold on the 16th.
	for i := range 15 {
		d, err := l.take(context.Background(), "key_p", limit, 1)
		require.NoError(t, err)
		require.True(t, d.Allowed, "call %d", i+1)
	}
	require.Equal(t, 1, qs.calls(), "15 calls after the first lease: all served locally")

	qs.err = errors.New("store down")
	d, err := l.take(context.Background(), "key_p", limit, 1) // the 16th call: still served locally...
	require.NoError(t, err)
	require.True(t, d.Allowed)
	require.Equal(t, 2, qs.calls(), "...but it also triggers a prefetch, which reaches the store and fails")

	b := l.buckets["key_p"]
	require.False(t, b.refilling, "a prefetch error clears refilling")
	require.Equal(t, noon.Add(storeBackoff), b.failUntil, "a prefetch error trips the breaker")

	// While the breaker is open, every take fails open immediately, with no
	// further store call — regardless of what the request would have cost.
	_, err = l.take(context.Background(), "key_p", limit, 10)
	require.ErrorIs(t, err, errStoreBackoff)
	require.Equal(t, 2, qs.calls(), "the breaker short-circuits before any store call")

	// Once storeBackoff elapses (and the store recovers), take reserves again.
	clk.t = clk.t.Add(storeBackoff + time.Second)
	qs.err = nil
	d, err = l.take(context.Background(), "key_p", limit, 10) // remaining(4) < 10: forces a reserve
	require.NoError(t, err)
	require.True(t, d.Allowed)
	require.Equal(t, 3, qs.calls(), "after the backoff elapses, take reserves again")
	require.True(t, b.failUntil.IsZero(), "a successful reservation clears the breaker")
}

// TestKeyedConcurrentReplicasRespectGlobalLimit simulates M replicas (one
// keyedLimiter each) sharing one store and one key, hammered by real
// goroutines with the real (non-synchronous) spawn. However units are
// divided among replicas by amortized leases, the store's clamp
// (NextQuotaUsed) guarantees no replica is ever over-granted — and with
// enough demand per replica (many multiples of the largest possible lease),
// every granted unit is eventually spent, so the total allowed converges
// exactly on the shared limit, never above it.
func TestKeyedConcurrentReplicasRespectGlobalLimit(t *testing.T) {
	qs := newFakeQuota()
	const (
		limit                = int64(100)
		cost                 = int64(1)
		replicas             = 4
		goroutinesPerReplica = 8
		attemptsPerGoroutine = 1000 // >> the largest possible lease (10): no residual grant can go unspent
	)
	clk := &clock{noon} // fixed: a window rollover mid-test would let more than limit units be granted in total
	limiters := make([]*keyedLimiter, replicas)
	var prefetches sync.WaitGroup
	for i := range limiters {
		l := newKeyedLimiter(qs, 0.1, 0.8, clk.now) // fixed clock, real goroutine spawn
		l.spawn = func(f func()) {
			prefetches.Add(1)
			go func() { defer prefetches.Done(); f() }()
		}
		limiters[i] = l
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for _, l := range limiters {
		for range goroutinesPerReplica {
			wg.Add(1)
			go func(l *keyedLimiter) {
				defer wg.Done()
				for range attemptsPerGoroutine {
					d, err := l.take(context.Background(), "key_shared", limit, cost)
					if err == nil && d.Allowed {
						allowed.Add(1)
					}
				}
			}(l)
		}
	}
	wg.Wait()
	prefetches.Wait() // every in-flight prefetch applies its grant (or strands it) before we mop up

	// No concurrency remains past this point. A prefetch that landed after
	// its replica's goroutines already spent their fixed attempt budget
	// would otherwise strand units the store had already granted (clamped
	// to the true remaining capacity) but nobody spent — so first make
	// every further prefetch synchronous, then drain each bucket's local
	// remainder deterministically.
	for _, l := range limiters {
		l.spawn = func(f func()) { f() }
	}
	for _, l := range limiters {
		for {
			d, err := l.take(context.Background(), "key_shared", limit, cost)
			if err != nil || !d.Allowed {
				break
			}
			allowed.Add(1)
		}
	}

	total := allowed.Load()
	require.LessOrEqual(t, total, limit, "global usage must never exceed the shared limit")
	require.Equal(t, limit, total, "with enough demand across replicas, usage exhausts the limit exactly")
}

// blockingQuota blocks each ReserveQuota call until release is closed, or
// its context is done — standing in for a store connection that has
// wedged. It signals on started the instant a call begins (best effort: the
// buffer-1 send never blocks the store call itself), so a test can wait
// for a reservation to be genuinely in flight before checking that
// something else doesn't wait on it.
type blockingQuota struct {
	started chan struct{}
	release chan struct{}
}

func newBlockingQuota() *blockingQuota {
	return &blockingQuota{started: make(chan struct{}, 1), release: make(chan struct{})}
}

func (q *blockingQuota) ReserveQuota(ctx context.Context, _ string, _ time.Time, _, _ int64) (int64, int64, error) {
	select {
	case q.started <- struct{}{}:
	default:
	}
	select {
	case <-q.release:
		return 0, 0, errors.New("store still down")
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

// TestKeyedHungStoreOpensForFullBackoffThenProbesExclusively checks three
// things about the breaker: (1) its open period is measured from when the
// failing reserve completes, not from when it started — the bug the review
// caught, where reusing the pre-call "now" left only ~reserveTimeout's
// worth of backoff instead of a full storeBackoff; (2) while fully open,
// every request fails open instantly, with no store call; (3) once
// storeBackoff elapses, exactly one request probes the store — with the
// bucket's lock released around that call — while concurrent requests
// alongside it fail open immediately instead of queueing behind it.
func TestKeyedHungStoreOpensForFullBackoffThenProbesExclusively(t *testing.T) {
	clk := &clock{noon}
	qb := newBlockingQuota()
	l := newKeyedLimiter(qb, 0.1, 0.8, clk.now)

	// The first reserve hangs until reserveTimeout — real wall time, since
	// that's what bounds it — while the fixed logical clock stays at noon.
	start := time.Now()
	_, err := l.take(context.Background(), "key_hung", 50, 10)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, reserveTimeout+time.Second, "a hung store must not block past reserveTimeout")
	require.GreaterOrEqual(t, elapsed, reserveTimeout-100*time.Millisecond)

	// The regression check: failUntil is measured from completion (noon,
	// since the logical clock never moved during that real-time wait), so
	// it's a full storeBackoff away — not storeBackoff minus the
	// ~reserveTimeout just spent inside the failing reserve.
	b := l.buckets["key_hung"]
	require.Equal(t, noon.Add(storeBackoff), b.failUntil)

	fastDenials := func() {
		const n = 8
		done := make(chan time.Duration, n)
		for range n {
			go func() {
				s := time.Now()
				_, _ = l.take(context.Background(), "key_hung", 50, 10)
				done <- time.Since(s)
			}()
		}
		for range n {
			require.Less(t, <-done, 200*time.Millisecond, "an open or half-open breaker must fail open without waiting on the store")
		}
	}

	// Fully open: concurrent requests return immediately.
	fastDenials()

	// Just under storeBackoff: still fully open.
	clk.t = noon.Add(storeBackoff - time.Second)
	_, err = l.take(context.Background(), "key_hung", 50, 10)
	require.ErrorIs(t, err, errStoreBackoff)

	// Past storeBackoff: half-open. Drain the stale "started" signal from
	// the first call above before using it to synchronize with the probe.
	select {
	case <-qb.started:
	default:
	}
	clk.t = noon.Add(storeBackoff + time.Second)
	probeDone := make(chan struct{})
	go func() {
		_, _ = l.take(context.Background(), "key_hung", 50, 10)
		close(probeDone)
	}()
	<-qb.started // the probe is now inside the store call, with the lock released

	fastDenials() // concurrent requests must not queue behind the in-flight probe

	close(qb.release) // let the probe finish — it still fails, so the breaker reopens
	<-probeDone
	require.True(t, clk.now().Before(b.failUntil), "the failed probe re-opens the breaker")
}

// cancelAwareQuota reports the context's own error instead of reserving,
// standing in for a store call that notices the caller hung up. It
// delegates to an underlying store when the context is still live.
type cancelAwareQuota struct{ store.QuotaStore }

func (q cancelAwareQuota) ReserveQuota(ctx context.Context, keyID string, w time.Time, want, limit int64) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	return q.QuotaStore.ReserveQuota(ctx, keyID, w, want, limit)
}

func TestKeyedCallerCancelDoesNotTripBreaker(t *testing.T) {
	qs := newFakeQuota()
	l := newTestKeyed(cancelAwareQuota{qs}, 0.1, 0.8, &clock{noon})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := l.take(ctx, "key_cancel", 50, 10)
	require.ErrorIs(t, err, context.Canceled)

	b := l.buckets["key_cancel"]
	require.True(t, b.failUntil.IsZero(), "a caller hanging up must not trip the breaker")

	_, err = l.take(context.Background(), "key_cancel", 50, 10) // a fresh, live request right after
	require.NoError(t, err)
	require.Equal(t, 1, qs.calls(), "only the live request reached the store")
}

// panicQuota panics on ReserveQuota while armed, standing in for a store
// client that panics (a nil pointer, a bad type assertion) instead of
// returning an error. Disarmed, it delegates to an underlying store.
type panicQuota struct {
	store.QuotaStore

	armed bool
}

func (q *panicQuota) ReserveQuota(ctx context.Context, keyID string, w time.Time, want, limit int64) (int64, int64, error) {
	if q.armed {
		panic("store exploded")
	}
	return q.QuotaStore.ReserveQuota(ctx, keyID, w, want, limit)
}

// TestKeyedStorePanicIsRecoveredAndTripsBreaker checks that a panicking
// store call can't wedge a bucket on either path that calls reserve: the
// normal, lock-held path (where an unrecovered panic would skip the
// explicit b.mu.Unlock and lock the bucket forever) and the half-open
// probe path (where it would leave probing stuck true, so the breaker
// could never close again even once the store recovers).
func TestKeyedStorePanicIsRecoveredAndTripsBreaker(t *testing.T) {
	qp := &panicQuota{armed: true, QuotaStore: newFakeQuota()}
	clk := &clock{noon}
	l := newTestKeyed(qp, 0.1, 0.8, clk)

	// Normal path: a panicking reserve comes back as an error, not a
	// propagating panic, and leaves the bucket usable.
	_, err := l.take(context.Background(), "key_panic", 50, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "quota store panicked")

	b := l.buckets["key_panic"]
	require.True(t, b.mu.TryLock(), "a panicking reserve must not leave the bucket's mutex locked")
	b.mu.Unlock()
	require.False(t, b.probing)

	// The breaker is open.
	_, err = l.take(context.Background(), "key_panic", 50, 10)
	require.ErrorIs(t, err, errStoreBackoff)

	// Half-open: the probe itself panics too. The same guarantees must hold.
	clk.t = clk.t.Add(storeBackoff + time.Second)
	_, err = l.take(context.Background(), "key_panic", 50, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "quota store panicked")
	require.True(t, b.mu.TryLock(), "a panicking probe must not leave the bucket's mutex locked")
	b.mu.Unlock()
	require.False(t, b.probing, "a panicking probe must not leave probing stuck")

	// Once the store recovers, a later take (past the new backoff) works.
	clk.t = clk.t.Add(storeBackoff + time.Second)
	qp.armed = false
	d, err := l.take(context.Background(), "key_panic", 50, 10)
	require.NoError(t, err)
	require.True(t, d.Allowed)
}

// quotaFunc adapts a plain function to store.QuotaStore.
type quotaFunc func(ctx context.Context, keyID string, w time.Time, want, limit int64) (int64, int64, error)

func (f quotaFunc) ReserveQuota(ctx context.Context, keyID string, w time.Time, want, limit int64) (int64, int64, error) {
	return f(ctx, keyID, w, want, limit)
}

// TestKeyedProbeSizedIndependentlyOfStaleRemaining reproduces the bug a
// probe sized by leaseSize (which nets out b.remaining) would hit: the
// probe releases the bucket's lock around the store call, so a request
// served in that window can spend whatever remaining the probe saw before
// unlocking. If the probe's own want had been sized against that now-stale
// remaining, its own request could come back short once its grant lands.
func TestKeyedProbeSizedIndependentlyOfStaleRemaining(t *testing.T) {
	const limit, cost = int64(50), int64(10) // baseLease = ceil(50*0.1) = 5
	qs := newFakeQuota()
	clk := &clock{noon}
	l := newTestKeyed(qs, 0.1, 0.8, clk)

	// Trip the breaker so the next request that needs a reservation
	// becomes the half-open probe.
	qs.err = errors.New("store down")
	_, err := l.take(context.Background(), "key_probe", limit, cost)
	require.Error(t, err)
	qs.err = nil

	b := l.buckets["key_probe"]
	b.remaining = 4 // a small lease left over from before the trip
	clk.t = clk.t.Add(storeBackoff + time.Second)

	stolen := false
	l.store = quotaFunc(func(ctx context.Context, keyID string, w time.Time, want, limit int64) (int64, int64, error) {
		if !stolen {
			stolen = true
			// A request served while the probe's lock is released spends
			// the stale "remaining" the probe saw before unlocking.
			b.mu.Lock()
			b.remaining -= 4
			b.mu.Unlock()
		}
		return qs.ReserveQuota(ctx, keyID, w, want, limit)
	})

	d, err := l.take(context.Background(), "key_probe", limit, cost)
	require.NoError(t, err)
	require.True(t, d.Allowed, "the probe must not be shortchanged by remaining that was spent while its lock was released")
}
