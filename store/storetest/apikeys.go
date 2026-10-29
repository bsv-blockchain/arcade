package storetest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/broadcaster/store"
)

// RunAPIKeySuite asserts the APIKeyStore contract. Every row carries a
// per-run suffix and the suite reads back only its own rows, so it is safe
// in a namespace shared with other integration tests.
func RunAPIKeySuite(t *testing.T, newBackend func(t *testing.T) store.APIKeyStore) {
	t.Helper()
	b := newBackend(t)
	ctx := context.Background()
	run := fmt.Sprintf("%x", time.Now().UnixNano())
	now := time.Now().UTC().Truncate(time.Millisecond)
	exp := now.Add(24 * time.Hour)

	k := &store.APIKey{
		ID: "key_c_" + run, Hash: "hash_" + run, Hint: "bsvb_Ab3x…9Qz", Name: "acme",
		Tier: "pro", Status: store.APIKeyActive, ExpiresAt: &exp, CreatedAt: now, UpdatedAt: now,
	}
	require.NoError(t, b.CreateAPIKey(ctx, k))

	got, err := b.GetAPIKeyByHash(ctx, k.Hash)
	require.NoError(t, err)
	requireSameKey(t, k, got)

	got, err = b.GetAPIKey(ctx, k.ID)
	require.NoError(t, err)
	requireSameKey(t, k, got)

	dupHash := *k
	dupHash.ID = "key_duphash_" + run
	require.Error(t, b.CreateAPIKey(ctx, &dupHash), "a duplicate hash must be rejected")
	_, err = b.GetAPIKey(ctx, dupHash.ID)
	require.ErrorIs(t, err, store.ErrAPIKeyNotFound, "a rejected create must leave no partial row")
	got, err = b.GetAPIKey(ctx, k.ID)
	require.NoError(t, err)
	requireSameKey(t, k, got)
	got, err = b.GetAPIKeyByHash(ctx, k.Hash)
	require.NoError(t, err)
	requireSameKey(t, k, got)

	dupID := *k
	dupID.Hash = "hash_dupid_" + run
	require.Error(t, b.CreateAPIKey(ctx, &dupID), "a duplicate id must be rejected")
	_, err = b.GetAPIKeyByHash(ctx, dupID.Hash)
	require.ErrorIs(t, err, store.ErrAPIKeyNotFound, "a rejected create must leave no partial row")
	got, err = b.GetAPIKey(ctx, k.ID)
	require.NoError(t, err)
	requireSameKey(t, k, got)
	got, err = b.GetAPIKeyByHash(ctx, k.Hash)
	require.NoError(t, err)
	requireSameKey(t, k, got)

	_, err = b.GetAPIKeyByHash(ctx, "hash_missing_"+run)
	require.ErrorIs(t, err, store.ErrAPIKeyNotFound)
	_, err = b.GetAPIKey(ctx, "key_missing_"+run)
	require.ErrorIs(t, err, store.ErrAPIKeyNotFound)

	// second and third are created after k but carry lexically smaller ids,
	// so the ordering assertion below is discriminating: a backend that
	// sorted by id (or by insertion order) instead of created_at would
	// produce the wrong order.
	second := &store.APIKey{
		ID: "key_b_" + run, Hash: "hash_second_" + run, Hint: "bsvb_Zz9a…0aa",
		Tier: "free", Status: store.APIKeyActive,
		CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(time.Millisecond),
	}
	require.NoError(t, b.CreateAPIKey(ctx, second))

	got, err = b.GetAPIKey(ctx, second.ID)
	require.NoError(t, err)
	requireSameKey(t, second, got)

	// third shares second's CreatedAt but sorts before it by id, so the
	// expected order below also pins down the tie-break.
	third := &store.APIKey{
		ID: "key_a_" + run, Hash: "hash_third_" + run, Hint: "bsvb_Qq1w…2ee",
		Tier: "free", Status: store.APIKeyActive,
		CreatedAt: now.Add(time.Millisecond), UpdatedAt: now.Add(time.Millisecond),
	}
	require.NoError(t, b.CreateAPIKey(ctx, third))

	all, err := b.ListAPIKeys(ctx)
	require.NoError(t, err)
	var ours []string
	for _, x := range all {
		if strings.HasSuffix(x.ID, run) {
			ours = append(ours, x.ID)
		}
	}
	require.Equal(t, []string{k.ID, third.ID, second.ID}, ours, "ListAPIKeys orders by created_at, then id")

	later := now.Add(time.Minute)
	require.NoError(t, b.UpdateAPIKeyTier(ctx, k.ID, "enterprise", later))
	got, err = b.GetAPIKeyByHash(ctx, k.Hash)
	require.NoError(t, err)
	require.Equal(t, "enterprise", got.Tier)
	require.True(t, got.UpdatedAt.Equal(later), "updated_at: want %v got %v", later, got.UpdatedAt)

	require.NoError(t, b.RevokeAPIKey(ctx, k.ID, later))
	require.NoError(t, b.RevokeAPIKey(ctx, k.ID, later.Add(time.Hour)), "revoking twice is a no-op")
	got, err = b.GetAPIKey(ctx, k.ID)
	require.NoError(t, err)
	require.Equal(t, store.APIKeyRevoked, got.Status)
	require.NotNil(t, got.RevokedAt)
	require.True(t, got.RevokedAt.Equal(later), "the second revoke must keep the first revoked_at")
	require.True(t, got.UpdatedAt.Equal(later), "the second revoke must not touch updated_at: want %v got %v", later, got.UpdatedAt)
	require.False(t, got.Usable(later))

	// UpdateAPIKeyTier is allowed on a revoked key: the tier is recorded but
	// the key stays revoked.
	tierAt := later.Add(2 * time.Hour)
	require.NoError(t, b.UpdateAPIKeyTier(ctx, k.ID, "pro", tierAt))
	got, err = b.GetAPIKey(ctx, k.ID)
	require.NoError(t, err)
	require.Equal(t, "pro", got.Tier)
	require.True(t, got.UpdatedAt.Equal(tierAt), "updated_at: want %v got %v", tierAt, got.UpdatedAt)
	require.Equal(t, store.APIKeyRevoked, got.Status, "UpdateAPIKeyTier must not reinstate a revoked key")
	require.NotNil(t, got.RevokedAt)
	require.True(t, got.RevokedAt.Equal(later), "revoked_at must survive a tier update")

	require.ErrorIs(t, b.UpdateAPIKeyTier(ctx, "key_missing_"+run, "pro", later), store.ErrAPIKeyNotFound)
	require.ErrorIs(t, b.RevokeAPIKey(ctx, "key_missing_"+run, later), store.ErrAPIKeyNotFound)
}

func requireSameKey(t *testing.T, want, got *store.APIKey) {
	t.Helper()
	require.Equal(t, want.ID, got.ID)
	require.Equal(t, want.Hash, got.Hash)
	require.Equal(t, want.Hint, got.Hint)
	require.Equal(t, want.Name, got.Name)
	require.Equal(t, want.Tier, got.Tier)
	require.Equal(t, want.Status, got.Status)
	require.True(t, want.CreatedAt.Equal(got.CreatedAt), "created_at: want %v got %v", want.CreatedAt, got.CreatedAt)
	require.True(t, want.UpdatedAt.Equal(got.UpdatedAt), "updated_at: want %v got %v", want.UpdatedAt, got.UpdatedAt)
	requireSameTimePtr(t, "expires_at", want.ExpiresAt, got.ExpiresAt)
	requireSameTimePtr(t, "revoked_at", want.RevokedAt, got.RevokedAt)
}

func requireSameTimePtr(t *testing.T, field string, want, got *time.Time) {
	t.Helper()
	if want == nil {
		require.Nil(t, got, field)
		return
	}
	require.NotNil(t, got, field)
	require.True(t, want.Equal(*got), "%s: want %v got %v", field, *want, *got)
}

// RunQuotaSuite asserts the QuotaStore contract: partial grants, exhaustion,
// limit changes mid-window, window and key isolation, and that concurrent
// reservers never over-grant.
func RunQuotaSuite(t *testing.T, newBackend func(t *testing.T) store.QuotaStore) {
	t.Helper()
	b := newBackend(t)
	ctx := context.Background()
	key := fmt.Sprintf("key_quota_%x", time.Now().UnixNano())
	w := time.Now().UTC().Truncate(time.Minute)

	reserve := func(k string, win time.Time, want, limit, wantGranted, wantUsed int64) {
		t.Helper()
		g, u, err := b.ReserveQuota(ctx, k, win, want, limit)
		require.NoError(t, err)
		require.Equal(t, wantGranted, g, "granted for want=%d limit=%d", want, limit)
		require.Equal(t, wantUsed, u, "usedAfter for want=%d limit=%d", want, limit)
	}
	reserve(key, w, 30, 100, 30, 30)
	reserve(key, w, 80, 100, 70, 100) // partial grant
	reserve(key, w, 1, 100, 0, 100)   // exhausted
	reserve(key, w, 10, 120, 10, 110) // limit raised mid-window
	reserve(key, w, 5, 50, 0, 110)    // limit lowered below used: no grant, used kept
	reserve(key, w.Add(time.Minute), 10, 100, 10, 10)
	reserve(key+"_other", w, 5, 100, 5, 5)
	reserve(key+"_big", w, 150, 100, 100, 100) // first reservation clamps to the limit

	race := key + "_race"
	var wg sync.WaitGroup
	var total atomic.Int64
	errs := make(chan error, 50)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, _, err := b.ReserveQuota(ctx, race, w, 3, 100)
			if err != nil {
				errs <- err
				return
			}
			if g < 0 || g > 3 {
				errs <- fmt.Errorf("granted %d outside [0,3]", g)
				return
			}
			total.Add(g)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(100), total.Load(), "50 racers × 3 against a limit of 100 must grant exactly 100")
	reserve(race, w, 1, 100, 0, 100) // already exhausted by the race
}
