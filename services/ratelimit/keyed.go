package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/bsv-blockchain/broadcaster/metrics"
	"github.com/bsv-blockchain/broadcaster/store"
)

const (
	prefetchTimeout = 5 * time.Second
	// reserveTimeout bounds the synchronous reserve a request waits on.
	// Without it a wedged store holds the bucket's mutex (and the request)
	// indefinitely: every other request for the key queues up behind it and
	// the convoy surfaces as a wall of 429s instead of a clean fail-open.
	reserveTimeout = 2 * time.Second
	// storeBackoff is how long a bucket trips its breaker after a reserve
	// (synchronous or prefetched) errors for a reason other than the
	// caller's own context. While it's open, take fails open instantly
	// without another store round trip. Measured from completion (not from
	// when the failing reserve started), so it's kept well above
	// reserveTimeout: a reserve that uses its full timeout still leaves a
	// real backoff behind it, not a sliver.
	storeBackoff = 10 * time.Second
)

// errStoreBackoff is returned by take while a bucket's breaker is open. It
// carries no information beyond "ask the local fail-open bucket instead."
var errStoreBackoff = errors.New("ratelimit: quota store circuit open for this key")

// keyedLimiter enforces a global per-key quota. Each replica leases units
// from the shared window counter (store.QuotaStore) and spends them from
// memory, so most requests make no store call, and global usage never
// exceeds the limit because every unit comes from the shared counter. The
// cost is under-delivery: units leased by a replica that goes idle are
// stranded until the window ends. Past shrinkThreshold of the limit, leases
// shrink to exactly the cost, bounding that loss.
type keyedLimiter struct {
	store           store.QuotaStore
	leaseFraction   float64
	shrinkThreshold float64
	now             func() time.Time
	spawn           func(func()) // runs prefetches; tests make it synchronous

	mu      sync.Mutex
	buckets map[string]*bucket
	sweptAt time.Time
}

type bucket struct {
	mu        sync.Mutex // held across the store call on a miss: per-key misses on a replica queue behind one reservation
	window    time.Time
	remaining int64 // units leased to this replica and not yet spent
	used      int64 // the window's shared total as of our last reservation
	refilling bool
	// limit is the tier limit remaining was leased under. Units leased
	// under a higher limit aren't valid against a lower one: on a
	// downgrade they're discarded (see take), and so is any grant that
	// lands from a reserve issued before the downgrade.
	limit int64
	// failUntil is the breaker. Zero: never tripped (or reset by a new
	// window) — normal operation, a miss reserves with the lock held, as
	// above. Non-zero and in the future: open — take fails open with no
	// store call. Non-zero and in the past: half-open — one probe (guarded
	// by probing) reserves with the lock released around the store call;
	// everyone else fails open without waiting on it.
	failUntil time.Time
	probing   bool
}

func newKeyedLimiter(qs store.QuotaStore, leaseFraction, shrinkThreshold float64, now func() time.Time) *keyedLimiter {
	return &keyedLimiter{
		store: qs, leaseFraction: leaseFraction, shrinkThreshold: shrinkThreshold, now: now,
		spawn:   func(f func()) { go f() },
		buckets: map[string]*bucket{},
	}
}

func (l *keyedLimiter) bucketFor(keyID string, now time.Time) *bucket {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.sweptAt) >= time.Minute {
		for id, b := range l.buckets {
			if b.mu.TryLock() { // a busy bucket is in use, not stale
				stale := now.Sub(b.window) > 2*time.Minute
				b.mu.Unlock()
				if stale {
					delete(l.buckets, id)
				}
			}
		}
		l.sweptAt = now
	}
	b, ok := l.buckets[keyID]
	if !ok {
		// Initialized to the current window, not the zero Time: an
		// un-initialized window reads as enormously stale, and a sweep
		// racing the bucket's own first take (which sets the window right
		// after this call, under b.mu) could otherwise evict it unseen.
		b = &bucket{window: now.Truncate(time.Minute)}
		l.buckets[keyID] = b
	}
	return b
}

// take charges cost units against keyID's limit for the current UTC minute.
// A store error (including errStoreBackoff) is returned as-is so the caller
// can fail open.
func (l *keyedLimiter) take(ctx context.Context, keyID string, limit, cost int64) (Decision, error) {
	now := l.now().UTC()
	window := now.Truncate(time.Minute)
	reset := window.Add(time.Minute).Sub(now)
	b := l.bucketFor(keyID, now)

	b.mu.Lock()
	if !b.window.Equal(window) {
		b.window, b.remaining, b.used, b.failUntil, b.probing = window, 0, 0, time.Time{}, false
	}
	if limit < b.limit {
		// A tier downgrade (e.g. 30,000 -> 50) mid-window. The local lease
		// was sized and granted under the old limit, so spending it would
		// keep honoring that limit for the rest of the window. The shared
		// counter only records leases, not spend, so there's no way to tell
		// how much of the window's usage is actually spent: drop the lease
		// and re-reserve against the new limit, which the store clamps.
		b.remaining = 0
	}
	b.limit = limit
	if now.Before(b.failUntil) {
		// Open: don't even consult local state. A single bad reservation
		// would otherwise still serve from whatever was locally remaining,
		// delaying the fail-open by one request.
		b.mu.Unlock()
		return Decision{}, errStoreBackoff
	}
	// used >= limit: the shared window is exhausted and can only stay that
	// way (used never decreases within a window), so every further denial
	// in this window is free — no reservation can grant anything more.
	needsReserve := b.remaining < cost && cost <= limit && b.used < limit
	halfOpen := !b.failUntil.IsZero() // past, but was tripped: unconfirmed recovery

	switch {
	case needsReserve && halfOpen && b.probing:
		// Someone else is already the probe; don't pile reservations onto
		// a store that might still be down, and don't wait on one either.
		b.mu.Unlock()
		return Decision{}, errStoreBackoff

	case needsReserve && halfOpen:
		// Exactly one probe, with the lock released around the store call:
		// everyone else's take (above) returns instantly instead of
		// queueing behind a reserve that might hang for reserveTimeout.
		b.probing = true
		want := l.probeLeaseSize(limit, cost, b)
		b.mu.Unlock()

		rctx, cancel := context.WithTimeout(ctx, reserveTimeout)
		granted, usedAfter, err := l.reserve(rctx, keyID, window, want, limit)
		cancel()

		b.mu.Lock()
		b.probing = false
		if !b.window.Equal(window) {
			// The window moved on while we were unlocked: whatever we
			// learned (grant or error) no longer applies to this bucket.
			b.mu.Unlock()
			return Decision{}, errStoreBackoff
		}
		if limit > b.limit {
			granted = 0 // reserved under a limit since downgraded (see bucket.limit)
		}
		if applyErr := l.recordReserve(ctx, b, granted, usedAfter, err); applyErr != nil {
			b.mu.Unlock()
			return Decision{}, applyErr
		}

	case needsReserve:
		// Normal operation: hold the lock across the reserve, so per-key
		// misses on a replica queue behind one reservation.
		rctx, cancel := context.WithTimeout(ctx, reserveTimeout)
		granted, usedAfter, err := l.reserve(rctx, keyID, window, l.leaseSize(limit, cost, b), limit)
		cancel()
		if applyErr := l.recordReserve(ctx, b, granted, usedAfter, err); applyErr != nil {
			b.mu.Unlock()
			return Decision{}, applyErr
		}
	}
	allowed := b.remaining >= cost
	if allowed {
		b.remaining -= cost
	}
	d := Decision{Allowed: allowed, Remaining: max(limit-b.used+b.remaining, 0), Reset: reset}
	prefetch := allowed && l.wantsPrefetch(limit, cost, b)
	if prefetch {
		b.refilling = true
	}
	b.mu.Unlock()

	if prefetch {
		// Detached: the refill outlives the request that triggered it, but
		// keeps its trace context.
		pctx := context.WithoutCancel(ctx)
		l.spawn(func() { l.prefetch(pctx, keyID, window, limit, b) })
	}
	return d, nil
}

// recordReserve applies a reservation's outcome to b, which the caller
// holds locked. A genuine store error (ctx.Err() is nil: not the caller
// hanging up) trips the breaker for storeBackoff measured from now — after
// the reserve returned, not from when it started, so a reserve that used
// its full reserveTimeout still leaves a real backoff behind it.
func (l *keyedLimiter) recordReserve(ctx context.Context, b *bucket, granted, usedAfter int64, err error) error {
	if err != nil {
		if ctx.Err() == nil {
			b.failUntil = l.now().UTC().Add(storeBackoff)
		}
		return err
	}
	b.failUntil = time.Time{}
	b.remaining += granted
	b.used = max(b.used, usedAfter)
	return nil
}

func (l *keyedLimiter) baseLease(limit int64) int64 {
	return max(int64(math.Ceil(float64(limit)*l.leaseFraction)), 1)
}

func (l *keyedLimiter) shrinking(limit int64, b *bucket) bool {
	return float64(b.used) >= l.shrinkThreshold*float64(limit)
}

func (l *keyedLimiter) leaseSize(limit, cost int64, b *bucket) int64 {
	need := cost - b.remaining
	if l.shrinking(limit, b) {
		return need
	}
	return max(need, l.baseLease(limit))
}

// probeLeaseSize sizes the half-open probe's own reservation. Unlike
// leaseSize, it doesn't net out b.remaining: the probe releases the lock
// around the store call, so another request can spend that remaining
// before the probe returns. Sizing against a lease it might no longer have
// would leave the probe's own request short once its grant lands. Asking
// for the full cost (or more, below the shrink threshold) guarantees the
// probe's own request succeeds whenever the store has the capacity for it,
// regardless of what else happened to the bucket while unlocked.
func (l *keyedLimiter) probeLeaseSize(limit, cost int64, b *bucket) int64 {
	if l.shrinking(limit, b) {
		return cost
	}
	return max(cost, l.baseLease(limit))
}

// wantsPrefetch refills in the background once local units fall below a
// quarter lease — only when a lease covers several requests of this cost,
// so steady traffic rarely waits on the store and tiny tiers never strand
// units in prefetches.
func (l *keyedLimiter) wantsPrefetch(limit, cost int64, b *bucket) bool {
	base := l.baseLease(limit)
	return !b.refilling && !l.shrinking(limit, b) && base >= 4*cost && b.remaining < base/4
}

func (l *keyedLimiter) prefetch(ctx context.Context, keyID string, window time.Time, limit int64, b *bucket) {
	ctx, cancel := context.WithTimeout(ctx, prefetchTimeout)
	defer cancel()
	granted, used, err := l.reserve(ctx, keyID, window, l.baseLease(limit), limit)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refilling = false
	if !b.window.Equal(window) {
		return // the window moved on: the grant (or error) is stale, as designed
	}
	if err != nil {
		// Detached from the request that triggered it (see take), so this
		// is never the caller hanging up: always a genuine store problem.
		b.failUntil = l.now().UTC().Add(storeBackoff)
		return
	}
	if limit > b.limit {
		granted = 0 // reserved under a limit since downgraded (see bucket.limit)
	}
	b.remaining += granted
	b.used = max(b.used, used)
}

func (l *keyedLimiter) reserve(ctx context.Context, keyID string, window time.Time, want, limit int64) (int64, int64, error) {
	start := time.Now()
	granted, used, err := l.reserveFromStore(ctx, keyID, window, want, limit)
	metrics.RateLimitReservationDuration.Observe(time.Since(start).Seconds())
	result := "granted"
	switch {
	case errors.Is(err, context.Canceled):
		result = "canceled"
	case err != nil:
		result = "error"
	case granted == 0:
		result = "exhausted"
	case granted < want:
		result = "partial"
	}
	metrics.RateLimitReservationsTotal.WithLabelValues(result).Inc()
	return granted, used, err
}

// reserveFromStore calls the store and recovers a panic from it, converting
// it to an error. Without this, a panicking store client would skip every
// b.mu.Unlock() between here and the handler: on the normal (lock-held)
// path the bucket's mutex wedges forever, and on the probe path, probing is
// left stuck true, so the breaker can never close again even once the
// store recovers.
func (l *keyedLimiter) reserveFromStore(ctx context.Context, keyID string, window time.Time, want, limit int64) (granted, usedAfter int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			granted, usedAfter, err = 0, 0, fmt.Errorf("quota store panicked: %v", r)
		}
	}()
	return l.store.ReserveQuota(ctx, keyID, window, want, limit)
}
