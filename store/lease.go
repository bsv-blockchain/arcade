package store

import (
	"context"
	"time"
)

// Leaser is a small coordination primitive for picking a single writer among
// multiple replicas. It's intentionally separate from Store: a backend can
// implement one, both, or neither. The shape is chosen so any reasonable
// backing store can implement it — Aerospike via gen-match CAS + native TTL,
// Redis via SET NX PX, Postgres via INSERT ... ON CONFLICT, an in-memory mock
// via sync.Mutex.
//
// Fencing tokens are deliberately NOT provided. Callers must only use a lease
// to gate actions that are safe to execute concurrently by more than one
// holder during a failover window — e.g. idempotent HTTP broadcasts. If two
// holders believe they hold the lease at the same instant (clock skew, GC
// pause), the worst outcome is redundant work, not corruption.
type Leaser interface {
	// TryAcquireOrRenew atomically acquires the named lease, renews it if we
	// already hold it, or returns a zero heldUntil if another holder currently
	// owns it. Errors are reserved for infrastructure failures — normal lease
	// contention is signalled by (time.Time{}, nil).
	//
	// holder is an identifier unique to this process for the lifetime of its
	// interest in the lease (e.g. hostname + random suffix). Callers should
	// pass the same holder across ticks so renewals succeed.
	//
	// ttl is how long the lease remains valid from the moment of this call.
	// Callers are expected to renew well before expiration (typically every
	// ttl/3).
	TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (heldUntil time.Time, err error)

	// Release best-effort releases a lease held by the caller. Safe to call
	// when we do not hold the lease (returns nil). TTL expiration is the
	// authoritative fallback if Release fails, so callers should log the
	// error but proceed with shutdown.
	Release(ctx context.Context, name, holder string) error
}
