package postgres

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// schemaChecksum is the compiled-in identity of schema.sql. It is stored in
// schema_info after a successful apply; a match on startup means this exact
// script has already run, so EnsureIndexes can skip every DDL statement —
// and, with them, the ACCESS EXCLUSIVE lock requests that crash-looped boots
// under load (issue #278).
var schemaChecksum = func() string {
	sum := sha256.Sum256([]byte(schemaSQL))
	return hex.EncodeToString(sum[:])
}()

// schemaLockKey serializes schema application across replicas via
// pg_advisory_lock. Advisory locks are per-database, so the constant only has
// to be unique among arcade's own advisory locks. "arcade" in ASCII hex;
// changing it would let two versions apply concurrently — never change it.
const schemaLockKey = int64(0x617263616465)

// defaultSchemaApplyTimeout bounds the slow path when
// store.postgres.schema_apply_timeout_ms is unset. The apply is one-shot at
// boot and idempotent, so generous beats fragile.
const defaultSchemaApplyTimeout = 5 * time.Minute

// Vars rather than consts only so schema_test.go can compress timings.
var (
	// schemaLockTimeout caps how long any single DDL statement may sit in a
	// lock queue (SET LOCAL lock_timeout). While an ALTER TABLE waits for
	// ACCESS EXCLUSIVE, every later query on that table queues behind it —
	// a bounded wait plus retry keeps a busy database responsive while a
	// schema change is pending.
	schemaLockTimeout = 10 * time.Second
	// schemaRetryBackoff is the delay before re-attempting after a lock
	// timeout or deadlock; doubles per attempt up to schemaRetryBackoffMax.
	schemaRetryBackoff    = time.Second
	schemaRetryBackoffMax = 15 * time.Second
	// schemaFastPathTimeout bounds the checksum probe so a hung read can't
	// eat the whole apply budget.
	schemaFastPathTimeout = 10 * time.Second
)

const (
	sqlstateLockNotAvailable = "55P03"
	sqlstateDeadlockDetected = "40P01"
)

func hasSQLState(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}

const upsertSchemaInfoSQL = `
INSERT INTO schema_info (id, checksum, applied_at) VALUES (TRUE, $1, now())
ON CONFLICT (id) DO UPDATE SET checksum = EXCLUDED.checksum, applied_at = EXCLUDED.applied_at`

// EnsureIndexes brings the database schema up to date. The common case —
// schema already current — is a single-row checksum read that takes no DDL
// locks, so restarts and rollouts boot instantly no matter how contended the
// database is. When the schema does need applying (fresh database or version
// change), replicas serialize on an advisory lock and each attempt bounds its
// lock waits, retrying until the deadline (store.postgres.
// schema_apply_timeout_ms, default 5m) expires. See issue #278.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	timeout := s.schemaApplyTimeout
	if timeout <= 0 {
		timeout = defaultSchemaApplyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if schemaCurrent(ctx, s.pool) {
		return nil
	}
	if err := s.applySchemaSerialized(ctx); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// rowQuerier is satisfied by both *pgxpool.Pool and *pgxpool.Conn, letting
// schemaCurrent serve the fast path (pool) and the post-lock re-check
// (pinned conn) alike.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// schemaCurrent reports whether schema_info's stored checksum matches the
// compiled-in one. Every failure mode — no row, table missing (42P01),
// connectivity trouble — returns false: the slow path re-checks under the
// advisory lock and surfaces real errors with proper context.
func schemaCurrent(ctx context.Context, q rowQuerier) bool {
	ctx, cancel := context.WithTimeout(ctx, schemaFastPathTimeout)
	defer cancel()
	var stored string
	if err := q.QueryRow(ctx, `SELECT checksum FROM schema_info`).Scan(&stored); err != nil {
		return false
	}
	return stored == schemaChecksum
}

// applySchemaSerialized pins one connection, serializes on the advisory lock
// so only one replica runs DDL at a time, and retries bounded-lock-wait
// apply attempts until ctx expires.
func (s *Store) applySchemaSerialized(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	locked := false
	defer func() { //nolint:contextcheck // cleanup must run even after ctx is cancelled — that's the point
		// Advisory locks are session state, and pgxpool does NOT reset
		// sessions on Release — a session still holding the lock must never
		// go back into the pool. Unlock explicitly; destroy the session when
		// that fails, or when we can't tell whether the lock was granted
		// (a ctx cancel can race the grant).
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if locked {
			if _, uerr := conn.Exec(cleanupCtx, `SELECT pg_advisory_unlock($1)`, schemaLockKey); uerr != nil {
				_ = conn.Conn().Close(cleanupCtx)
			}
		} else {
			_ = conn.Conn().Close(cleanupCtx)
		}
		conn.Release()
	}()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, schemaLockKey); err != nil {
		return fmt.Errorf("waiting for schema lock: %w", err)
	}
	locked = true

	// Another replica may have applied while we queued on the lock.
	if schemaCurrent(ctx, conn) {
		return nil
	}

	backoff := schemaRetryBackoff
	for attempt := 1; ; attempt++ {
		err := applySchemaAttempt(ctx, conn)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("timed out after %d attempts waiting for table locks (another session holds a conflicting lock on an arcade table): %w", attempt, ctxErr)
		}
		if !hasSQLState(err, sqlstateLockNotAvailable) && !hasSQLState(err, sqlstateDeadlockDetected) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %d attempts waiting for table locks (another session holds a conflicting lock on an arcade table): %w", attempt, ctx.Err())
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, schemaRetryBackoffMax)
	}
}

// applySchemaAttempt is one all-or-nothing transactional apply. SET LOCAL
// scopes lock_timeout to this transaction, so an abort can't leak the
// setting back into the pool, and a lock-timeout abort rolls the whole
// script back cleanly — the retry re-runs it from scratch (every statement
// is IF NOT EXISTS, so that's idempotent by construction).
func applySchemaAttempt(ctx context.Context, conn *pgxpool.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { //nolint:contextcheck // rollback must run even after ctx is cancelled
		// Rollback on a background ctx so a cancelled apply still leaves the
		// session clean; no-op after a successful Commit.
		rbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rbCtx)
	}()

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", schemaLockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("set lock_timeout: %w", err)
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, upsertSchemaInfoSQL, schemaChecksum); err != nil {
		return fmt.Errorf("record schema checksum: %w", err)
	}
	return tx.Commit(ctx)
}
