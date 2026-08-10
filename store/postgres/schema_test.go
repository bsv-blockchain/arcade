//go:build postgres

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bsv-blockchain/arcade/store/bumpcache"
)

// isolatedDBSeq numbers throwaway databases. Tests in this package run
// sequentially (no t.Parallel), so a plain counter is race-free.
var isolatedDBSeq int

// newIsolatedStore creates a throwaway database on the shared postgres
// instance and returns a Store pointed at it. The schema tests hold table
// locks, drop indexes, and corrupt schema_info — none of which can share the
// TRUNCATE-isolated store the rest of the package uses.
func newIsolatedStore(t *testing.T) *Store {
	t.Helper()
	if sharedStore == nil {
		t.Skipf("postgres unavailable, skipping: %v", sharedStoreErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	isolatedDBSeq++
	name := fmt.Sprintf("arcade_schema_test_%d", isolatedDBSeq)
	if _, err := sharedStore.pool.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Skipf("cannot create isolated database (external DSN without CREATEDB?): %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_, _ = sharedStore.pool.Exec(dropCtx, "DROP DATABASE "+name+" WITH (FORCE)")
	})

	poolCfg := sharedStore.pool.Config().Copy()
	poolCfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("connect isolated database: %v", err)
	}
	st := &Store{pool: pool, bumpCache: bumpcache.New(), schemaApplyTimeout: 30 * time.Second}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func mustApplySchema(t *testing.T, st *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := st.EnsureIndexes(ctx); err != nil {
		t.Fatalf("initial EnsureIndexes: %v", err)
	}
}

func storedChecksum(t *testing.T, st *Store) (checksum string, appliedAt time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := st.pool.QueryRow(ctx, `SELECT checksum, applied_at FROM schema_info`).Scan(&checksum, &appliedAt); err != nil {
		t.Fatalf("read schema_info: %v", err)
	}
	return checksum, appliedAt
}

func indexExists(t *testing.T, st *Store, name string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := st.pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE indexname = $1`, name).Scan(&n); err != nil {
		t.Fatalf("query pg_indexes: %v", err)
	}
	return n == 1
}

func TestEnsureIndexes_FreshDatabaseAppliesSchemaAndChecksum(t *testing.T) {
	st := newIsolatedStore(t)
	mustApplySchema(t, st)

	got, _ := storedChecksum(t, st)
	if got != schemaChecksum {
		t.Errorf("stored checksum = %q, want compiled-in %q", got, schemaChecksum)
	}
	if !indexExists(t, st, "idx_tx_status") {
		t.Error("idx_tx_status missing after fresh apply")
	}
}

func TestEnsureIndexes_SecondCallIsFastPathNoop(t *testing.T) {
	st := newIsolatedStore(t)
	mustApplySchema(t, st)
	_, first := storedChecksum(t, st)

	mustApplySchema(t, st)
	_, second := storedChecksum(t, st)
	if !second.Equal(first) {
		t.Errorf("applied_at changed %v -> %v: second call re-applied the schema instead of fast-path skipping", first, second)
	}
}

// The issue #278 regression test: with the schema already current, boot-time
// EnsureIndexes must succeed quickly even while another session holds an
// ACCESS EXCLUSIVE lock on an arcade table — the fast path takes no DDL
// locks at all. Today's implementation queues on the lock and times out.
func TestEnsureIndexes_FastPathSucceedsUnderAccessExclusiveLock(t *testing.T) {
	st := newIsolatedStore(t)
	mustApplySchema(t, st)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	defer func() { _ = blocker.Rollback(ctx) }()
	if _, err := blocker.Exec(ctx, `LOCK TABLE transactions IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("acquire blocking lock: %v", err)
	}

	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	defer callCancel()
	start := time.Now()
	if err := st.EnsureIndexes(callCtx); err != nil {
		t.Fatalf("EnsureIndexes under ACCESS EXCLUSIVE lock: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("fast path took %v; expected well under a second (did it issue DDL?)", elapsed)
	}
}

func TestEnsureIndexes_StaleChecksumRetriesUntilLockReleased(t *testing.T) {
	st := newIsolatedStore(t)
	mustApplySchema(t, st)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := st.pool.Exec(ctx, `UPDATE schema_info SET checksum = 'stale'`); err != nil {
		t.Fatalf("corrupt checksum: %v", err)
	}

	// Compress the retry timings so one held lock guarantees at least one
	// 55P03 rollback-and-retry cycle inside a short test.
	origLockTimeout, origBackoff := schemaLockTimeout, schemaRetryBackoff
	schemaLockTimeout, schemaRetryBackoff = 250*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { schemaLockTimeout, schemaRetryBackoff = origLockTimeout, origBackoff })

	blocker, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	if _, err := blocker.Exec(ctx, `LOCK TABLE transactions IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("acquire blocking lock: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		callCtx, callCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer callCancel()
		done <- st.EnsureIndexes(callCtx)
	}()

	// Hold the lock long enough for at least one lock_timeout abort, then
	// release and let the retry succeed.
	time.Sleep(1 * time.Second)
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("release blocking lock: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("EnsureIndexes should succeed once the lock is released: %v", err)
	}
	if got, _ := storedChecksum(t, st); got != schemaChecksum {
		t.Errorf("checksum = %q after reapply, want %q", got, schemaChecksum)
	}
}

func TestEnsureIndexes_ConcurrentFreshDatabase(t *testing.T) {
	st := newIsolatedStore(t)

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			errs <- st.EnsureIndexes(ctx)
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent EnsureIndexes: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var rows int
	if err := st.pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_info`).Scan(&rows); err != nil {
		t.Fatalf("count schema_info: %v", err)
	}
	if rows != 1 {
		t.Errorf("schema_info has %d rows, want 1", rows)
	}
}

// The documented escape hatch for manual DDL drift: deleting the schema_info
// row forces a full idempotent reapply on the next start.
func TestEnsureIndexes_DeleteChecksumRowForcesReapply(t *testing.T) {
	st := newIsolatedStore(t)
	mustApplySchema(t, st)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := st.pool.Exec(ctx, `DROP INDEX idx_tx_status`); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `DELETE FROM schema_info`); err != nil {
		t.Fatalf("delete checksum row: %v", err)
	}

	mustApplySchema(t, st)
	if !indexExists(t, st, "idx_tx_status") {
		t.Error("idx_tx_status not recreated after escape-hatch reapply")
	}
	if got, _ := storedChecksum(t, st); got != schemaChecksum {
		t.Errorf("checksum = %q after reapply, want %q", got, schemaChecksum)
	}
}

// A replica cancelled (or timed out) while queued on the advisory lock must
// not poison the pool: pgxpool does not reset session state on Release, so
// the implementation has to destroy the session instead of returning it.
func TestEnsureIndexes_CancelledWhileWaitingForAdvisoryLock(t *testing.T) {
	st := newIsolatedStore(t)
	mustApplySchema(t, st)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := st.pool.Exec(ctx, `UPDATE schema_info SET checksum = 'stale'`); err != nil {
		t.Fatalf("corrupt checksum: %v", err)
	}

	holder, err := st.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire holder conn: %v", err)
	}
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, schemaLockKey); err != nil {
		t.Fatalf("hold advisory lock: %v", err)
	}
	defer func() {
		// Destroying the session releases its advisory lock.
		_ = holder.Conn().Close(ctx)
		holder.Release()
	}()

	callCtx, callCancel := context.WithTimeout(ctx, 1*time.Second)
	defer callCancel()
	if err := st.EnsureIndexes(callCtx); err == nil {
		t.Fatal("EnsureIndexes should fail when the advisory lock never frees within its deadline")
	}

	// The pool must still be healthy afterwards — the waiter's session may
	// not come back holding half-acquired state.
	var one int
	if err := st.pool.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("pool unhealthy after cancelled schema wait: %v", err)
	}
}
