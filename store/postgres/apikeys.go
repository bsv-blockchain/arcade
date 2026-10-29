package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/bsv-blockchain/broadcaster/store"
)

var (
	_ store.APIKeyStore = (*Store)(nil)
	_ store.QuotaStore  = (*Store)(nil)
)

const apiKeyColumns = `id, hash, hint, name, tier, status, expires_at, created_at, updated_at, revoked_at` //nolint:gosec // column list string, not a credential

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func scanAPIKey(row pgx.Row) (*store.APIKey, error) {
	var k store.APIKey
	var status string
	var expires, revoked *time.Time
	if err := row.Scan(&k.ID, &k.Hash, &k.Hint, &k.Name, &k.Tier, &status, &expires, &k.CreatedAt, &k.UpdatedAt, &revoked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrAPIKeyNotFound
		}
		return nil, err
	}
	k.Status = store.APIKeyStatus(status)
	k.CreatedAt = k.CreatedAt.UTC()
	k.UpdatedAt = k.UpdatedAt.UTC()
	k.ExpiresAt = utcPtr(expires)
	k.RevokedAt = utcPtr(revoked)
	return &k, nil
}

func (s *Store) CreateAPIKey(ctx context.Context, k *store.APIKey) error {
	const q = `INSERT INTO api_keys (` + apiKeyColumns + `) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
	if _, err := s.pool.Exec(ctx, q, k.ID, k.Hash, k.Hint, k.Name, k.Tier, string(k.Status),
		k.ExpiresAt, k.CreatedAt, k.UpdatedAt, k.RevokedAt); err != nil {
		return fmt.Errorf("create api key %s: %w", k.ID, err)
	}
	return nil
}

func (s *Store) GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error) {
	k, err := scanAPIKey(s.pool.QueryRow(ctx, `SELECT `+apiKeyColumns+` FROM api_keys WHERE hash=$1`, hash))
	if err != nil && !errors.Is(err, store.ErrAPIKeyNotFound) {
		return nil, fmt.Errorf("get api key by hash: %w", err)
	}
	return k, err
}

func (s *Store) GetAPIKey(ctx context.Context, id string) (*store.APIKey, error) {
	k, err := scanAPIKey(s.pool.QueryRow(ctx, `SELECT `+apiKeyColumns+` FROM api_keys WHERE id=$1`, id))
	if err != nil && !errors.Is(err, store.ErrAPIKeyNotFound) {
		return nil, fmt.Errorf("get api key %s: %w", id, err)
	}
	return k, err
}

func (s *Store) ListAPIKeys(ctx context.Context) ([]*store.APIKey, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+apiKeyColumns+` FROM api_keys ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	defer rows.Close()
	var out []*store.APIKey
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, fmt.Errorf("list api keys: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *Store) UpdateAPIKeyTier(ctx context.Context, id, tier string, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET tier=$2, updated_at=$3 WHERE id=$1`, id, tier, at)
	if err != nil {
		return fmt.Errorf("update api key tier %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrAPIKeyNotFound
	}
	return nil
}

func (s *Store) RevokeAPIKey(ctx context.Context, id string, at time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET status='revoked', revoked_at=$2, updated_at=$2 WHERE id=$1 AND status <> 'revoked'`, id, at)
	if err != nil {
		return fmt.Errorf("revoke api key %s: %w", id, err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	// Nothing changed: either already revoked (a no-op) or no such key.
	var one int
	err = s.pool.QueryRow(ctx, `SELECT 1 FROM api_keys WHERE id=$1`, id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrAPIKeyNotFound
	}
	if err != nil {
		return fmt.Errorf("revoke api key %s: %w", id, err)
	}
	return nil
}

// ReserveQuota is one upsert. In ON CONFLICT DO UPDATE every right-hand side
// reads the pre-update row, so last_grant is computed from the old used in
// the same atomic statement that advances it. The insert arm is the first
// reservation of the window: used = last_grant = min(want, limit).
func (s *Store) ReserveQuota(ctx context.Context, keyID string, windowStart time.Time, want, limit int64) (int64, int64, error) {
	const q = `
INSERT INTO api_quota AS q (key_id, window_start, used, last_grant)
VALUES ($1, $2, LEAST($3::BIGINT, $4::BIGINT), LEAST($3::BIGINT, $4::BIGINT))
ON CONFLICT (key_id, window_start) DO UPDATE SET
    used       = GREATEST(q.used, LEAST(q.used + $3::BIGINT, $4::BIGINT)),
    last_grant = GREATEST(q.used, LEAST(q.used + $3::BIGINT, $4::BIGINT)) - q.used
RETURNING last_grant, used`
	var granted, used int64
	if err := s.pool.QueryRow(ctx, q, keyID, windowStart, want, limit).Scan(&granted, &used); err != nil {
		return 0, 0, fmt.Errorf("reserve quota %s: %w", keyID, err)
	}
	s.maybeSweepQuota(ctx, time.Now())
	return granted, used, nil
}

// maybeSweepQuota deletes expired windows at most once a minute per process.
// Best-effort: a failed sweep only leaves rows for the next one. The sweep is
// detached from ctx's cancellation because it outlives the reservation.
func (s *Store) maybeSweepQuota(ctx context.Context, now time.Time) {
	last := s.quotaSweptAt.Load()
	if now.UnixNano()-last < int64(time.Minute) || !s.quotaSweptAt.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	sctx := context.WithoutCancel(ctx)
	go func() {
		ctx, cancel := context.WithTimeout(sctx, 10*time.Second)
		defer cancel()
		_, _ = s.pool.Exec(ctx, `DELETE FROM api_quota WHERE window_start < $1`, now.Add(-store.QuotaRetention))
	}()
}
