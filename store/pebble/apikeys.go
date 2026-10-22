package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/bsv-blockchain/arcade/store"
)

var (
	_ store.APIKeyStore = (*Store)(nil)
	_ store.QuotaStore  = (*Store)(nil)
)

// storedAPIKey is the persistence shape for API keys; times are unix ms so
// they round-trip at the millisecond precision every backend shares. A zero
// ms value encodes a nil time.
type storedAPIKey struct {
	ID          string `json:"id"`
	Hash        string `json:"hash"`
	Hint        string `json:"hint"`
	Name        string `json:"name"`
	Tier        string `json:"tier"`
	Status      string `json:"status"`
	ExpiresAtMs int64  `json:"expires_at_ms,omitempty"`
	CreatedAtMs int64  `json:"created_at_ms"`
	UpdatedAtMs int64  `json:"updated_at_ms"`
	RevokedAtMs int64  `json:"revoked_at_ms,omitempty"`
}

func apiKeyMs(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}

func apiKeyTime(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

func toStoredAPIKey(k *store.APIKey) storedAPIKey {
	return storedAPIKey{
		ID: k.ID, Hash: k.Hash, Hint: k.Hint, Name: k.Name, Tier: k.Tier, Status: string(k.Status),
		ExpiresAtMs: apiKeyMs(k.ExpiresAt), CreatedAtMs: k.CreatedAt.UnixMilli(),
		UpdatedAtMs: k.UpdatedAt.UnixMilli(), RevokedAtMs: apiKeyMs(k.RevokedAt),
	}
}

func (r storedAPIKey) toAPIKey() *store.APIKey {
	return &store.APIKey{
		ID: r.ID, Hash: r.Hash, Hint: r.Hint, Name: r.Name, Tier: r.Tier, Status: store.APIKeyStatus(r.Status),
		ExpiresAt: apiKeyTime(r.ExpiresAtMs), CreatedAt: time.UnixMilli(r.CreatedAtMs).UTC(),
		UpdatedAt: time.UnixMilli(r.UpdatedAtMs).UTC(), RevokedAt: apiKeyTime(r.RevokedAtMs),
	}
}

// CreateAPIKey writes the primary row and the hash index in one batch, after
// checking neither exists. apiKeyMu makes check-then-write atomic.
func (s *Store) CreateAPIKey(ctx context.Context, k *store.APIKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.apiKeyMu.Lock()
	defer s.apiKeyMu.Unlock()
	for _, key := range [][]byte{apiKeyKey(k.ID), apiKeyHashKey(k.Hash)} {
		_, closer, err := s.db.Get(key)
		if err == nil {
			_ = closer.Close()
			return fmt.Errorf("create api key %s: id or hash already exists", k.ID)
		}
		if !errors.Is(err, pebbledb.ErrNotFound) {
			return fmt.Errorf("create api key %s: %w", k.ID, err)
		}
	}
	payload, err := json.Marshal(toStoredAPIKey(k))
	if err != nil {
		return err
	}
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(apiKeyKey(k.ID), payload, nil); err != nil {
		return err
	}
	if err := b.Set(apiKeyHashKey(k.Hash), []byte(k.ID), nil); err != nil {
		return err
	}
	return b.Commit(s.writeOpts)
}

func (s *Store) getAPIKeyByID(id string) (*store.APIKey, error) {
	v, closer, err := s.db.Get(apiKeyKey(id))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, store.ErrAPIKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get api key %s: %w", id, err)
	}
	defer func() { _ = closer.Close() }()
	var row storedAPIKey
	if err := json.Unmarshal(v, &row); err != nil {
		return nil, fmt.Errorf("decode api key %s: %w", id, err)
	}
	return row.toAPIKey(), nil
}

func (s *Store) GetAPIKey(ctx context.Context, id string) (*store.APIKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.getAPIKeyByID(id)
}

func (s *Store) GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, closer, err := s.db.Get(apiKeyHashKey(hash))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil, store.ErrAPIKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get api key by hash: %w", err)
	}
	id := string(v) // string() copies before the closer releases v
	_ = closer.Close()
	return s.getAPIKeyByID(id)
}

func (s *Store) ListAPIKeys(ctx context.Context) ([]*store.APIKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := apiKeyPrefix()
	iter, err := s.db.NewIter(&pebbledb.IterOptions{LowerBound: prefix, UpperBound: endOfPrefix(prefix)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	var out []*store.APIKey
	for iter.First(); iter.Valid(); iter.Next() {
		var row storedAPIKey
		if err := json.Unmarshal(iter.Value(), &row); err != nil {
			return nil, fmt.Errorf("decode api key %s: %w", iter.Key(), err)
		}
		out = append(out, row.toAPIKey())
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (s *Store) UpdateAPIKeyTier(ctx context.Context, id, tier string, at time.Time) error {
	return s.mutateAPIKey(ctx, id, func(k *store.APIKey) bool {
		k.Tier = tier
		k.UpdatedAt = at
		return true
	})
}

func (s *Store) RevokeAPIKey(ctx context.Context, id string, at time.Time) error {
	return s.mutateAPIKey(ctx, id, func(k *store.APIKey) bool {
		if k.Status == store.APIKeyRevoked {
			return false
		}
		k.Status = store.APIKeyRevoked
		k.RevokedAt = &at
		k.UpdatedAt = at
		return true
	})
}

// mutateAPIKey applies fn under apiKeyMu and writes the row back when fn
// reports a change.
func (s *Store) mutateAPIKey(ctx context.Context, id string, fn func(*store.APIKey) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.apiKeyMu.Lock()
	defer s.apiKeyMu.Unlock()
	k, err := s.getAPIKeyByID(id)
	if err != nil {
		return err
	}
	if !fn(k) {
		return nil
	}
	payload, err := json.Marshal(toStoredAPIKey(k))
	if err != nil {
		return err
	}
	return s.db.Set(apiKeyKey(id), payload, s.writeOpts)
}

// quotaKey identifies one rate-limit window's counter.
type quotaKey struct {
	keyID    string
	windowMs int64
}

// ReserveQuota keeps counters in memory: Pebble serves a single process, so
// a mutex makes the reservation exact and nothing else needs to see it.
func (s *Store) ReserveQuota(ctx context.Context, keyID string, windowStart time.Time, want, limit int64) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	s.quotaMu.Lock()
	defer s.quotaMu.Unlock()
	if s.quota == nil {
		s.quota = make(map[quotaKey]int64)
	}
	now := time.Now()
	if now.Sub(s.quotaSweptAt) >= time.Minute {
		cutoff := now.Add(-store.QuotaRetention).UnixMilli()
		for k := range s.quota {
			if k.windowMs < cutoff {
				delete(s.quota, k)
			}
		}
		s.quotaSweptAt = now
	}
	k := quotaKey{keyID: keyID, windowMs: windowStart.UnixMilli()}
	used := s.quota[k]
	next := store.NextQuotaUsed(used, want, limit)
	s.quota[k] = next
	return next - used, next, nil
}
