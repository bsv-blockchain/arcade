//go:build mongodb

package mongodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/store"
)

func requireMongo(t *testing.T) {
	t.Helper()
	if sharedClient == nil {
		if os.Getenv("BSV_BROADCASTER_MONGODB_REQUIRED") != "" {
			t.Fatalf("mongodb required but unavailable at BSV_BROADCASTER_MONGODB_URI: %v", sharedErr)
		}
		t.Skipf("mongodb unavailable (set BSV_BROADCASTER_MONGODB_URI), skipping: %v", sharedErr)
	}
}

func freshDBName(t *testing.T, prefix string) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := prefix + hex.EncodeToString(b[:])
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = sharedClient.Database(name).RunCommand(ctx, bson.D{{Key: "dropAllRolesFromDatabase", Value: 1}}).Err()
		_ = sharedClient.Database(name).RunCommand(ctx, bson.D{{Key: "dropAllUsersFromDatabase", Value: 1}}).Err()
		_ = sharedClient.Database(name).Drop(ctx)
	})
	return name
}

// exerciseAuthStore drives every APIKeyStore/QuotaStore method once.
func exerciseAuthStore(t *testing.T, a *AuthStore) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Millisecond)
	k := &store.APIKey{ID: "key_a", Hash: "hash_a", Hint: "a", Name: "a", Tier: "free", Status: store.APIKeyActive, CreatedAt: now, UpdatedAt: now}
	if err := a.CreateAPIKey(ctx, k); err != nil {
		t.Fatalf("create key: %v", err)
	}
	if _, err := a.GetAPIKeyByHash(ctx, "hash_a"); err != nil {
		t.Fatalf("get by hash: %v", err)
	}
	if _, err := a.GetAPIKey(ctx, "key_a"); err != nil {
		t.Fatalf("get: %v", err)
	}
	if keys, err := a.ListAPIKeys(ctx); err != nil || len(keys) != 1 {
		t.Fatalf("list: %v (%d keys)", err, len(keys))
	}
	if err := a.UpdateAPIKeyTier(ctx, "key_a", "pro", now); err != nil {
		t.Fatalf("set tier: %v", err)
	}
	if err := a.RevokeAPIKey(ctx, "key_a", now); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := a.RevokeAPIKey(ctx, "key_missing", now); !errors.Is(err, store.ErrAPIKeyNotFound) {
		t.Fatalf("revoke missing = %v, want ErrAPIKeyNotFound", err)
	}
	granted, used, err := a.ReserveQuota(ctx, "key_a", now.Truncate(time.Minute), 3, 10)
	if err != nil || granted != 3 || used != 3 {
		t.Fatalf("reserve quota = %d/%d, %v", granted, used, err)
	}
}

// A dedicated auth database gets the api_keys and api_quota indexes and no
// other collection, and EnsureIndexes is idempotent.
func TestAuthStore_OnlyAuthCollections(t *testing.T) {
	requireMongo(t)
	dbName := freshDBName(t, "bsv_broadcaster_auth_test_")
	a := newAuthWithClient(sharedClient, dbName, config.Mongo{OpTimeoutMs: 5000})
	for i := range 2 {
		if err := a.EnsureIndexes(t.Context()); err != nil {
			t.Fatalf("EnsureIndexes #%d: %v", i+1, err)
		}
	}
	exerciseAuthStore(t, a)

	names, err := sharedClient.Database(dbName).ListCollectionNames(t.Context(), bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(names)
	if want := []string{collAPIKeys, collAPIQuota}; !slices.Equal(names, want) {
		t.Errorf("auth database collections = %v, want %v", names, want)
	}
	if got := indexNames(t, a.s.db.Collection(collAPIKeys)); len(got) != 3 || !got[idxAPIKeyHash] || !got[idxAPIKeyCreated] {
		t.Errorf("api_keys indexes = %v", got)
	}
	if got := indexNames(t, a.s.db.Collection(collAPIQuota)); len(got) != 2 || !got[idxQuotaExpires] {
		t.Errorf("api_quota indexes = %v", got)
	}
}

// The auth database may be another deployment's main database: the auth store's
// indexes must be identical to the ones Store.EnsureIndexes already created,
// so provisioning either way round succeeds.
func TestAuthStore_SharesDatabaseWithMainStore(t *testing.T) {
	requireMongo(t)
	dbName := freshDBName(t, "bsv_broadcaster_auth_shared_test_")
	mainStore := newWithClient(sharedClient, dbName, config.Mongo{OpTimeoutMs: 5000})
	if err := mainStore.EnsureIndexes(t.Context()); err != nil {
		t.Fatalf("main EnsureIndexes: %v", err)
	}
	a := newAuthWithClient(sharedClient, dbName, config.Mongo{OpTimeoutMs: 5000})
	if err := a.EnsureIndexes(t.Context()); err != nil {
		t.Fatalf("auth EnsureIndexes over a main-store database: %v", err)
	}
	if err := mainStore.EnsureIndexes(t.Context()); err != nil {
		t.Fatalf("main EnsureIndexes after auth: %v", err)
	}
	exerciseAuthStore(t, a)
	if keys, err := mainStore.ListAPIKeys(t.Context()); err != nil || len(keys) != 1 {
		t.Fatalf("main store sees %d keys, %v; want the auth store's one key", len(keys), err)
	}
}

// The auth store must work under a role scoped to only <db>.api_keys and
// <db>.api_quota with no database-level privileges — the Atlas custom role
// documented in docs/api-keys.md. Needs a mongod started with --auth and an
// BSV_BROADCASTER_MONGODB_URI user that can create roles and users; skips otherwise.
func TestAuthStore_CollectionScopedRole(t *testing.T) {
	requireMongo(t)
	dbName := freshDBName(t, "bsv_broadcaster_auth_role_test_")
	ctx := t.Context()
	db := sharedClient.Database(dbName)

	// A neighboring collection, as when the auth database is also a main
	// store's database; the restricted user must not be able to read it.
	if _, err := db.Collection(collTransactions).InsertOne(ctx, bson.D{{Key: "_id", Value: "tx"}}); err != nil {
		t.Fatal(err)
	}

	actions := bson.A{"find", "insert", "update", "remove", "createIndex", "listIndexes", "createCollection"}
	privs := make(bson.A, 0, 2)
	for _, c := range []string{collAPIKeys, collAPIQuota} {
		privs = append(privs, bson.D{
			{Key: "resource", Value: bson.D{{Key: "db", Value: dbName}, {Key: "collection", Value: c}}},
			{Key: "actions", Value: actions},
		})
	}
	if err := db.RunCommand(ctx, bson.D{
		{Key: "createRole", Value: "bsvBroadcasterAuthStore"}, {Key: "privileges", Value: privs}, {Key: "roles", Value: bson.A{}},
	}).Err(); err != nil {
		t.Skipf("cannot create roles with BSV_BROADCASTER_MONGODB_URI (need a --auth mongod and a userAdmin): %v", err)
	}
	if err := db.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: "bsv_broadcaster_auth"},
		{Key: "pwd", Value: "bsv_broadcaster_auth_pw"},
		{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "bsvBroadcasterAuthStore"}, {Key: "db", Value: dbName}}}},
	}).Err(); err != nil {
		t.Skipf("cannot create users with BSV_BROADCASTER_MONGODB_URI: %v", err)
	}

	u, err := url.Parse(sharedURI)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("bsv_broadcaster_auth", "bsv_broadcaster_auth_pw")
	if u.Path == "" {
		u.Path = "/"
	}
	q := u.Query()
	q.Set("authSource", dbName)
	u.RawQuery = q.Encode()

	a, err := NewAuthStore(ctx, config.Mongo{URI: u.String(), Database: dbName, OpTimeoutMs: 5000, ConnectTimeoutMs: 5000}, nil)
	if err != nil {
		t.Fatalf("NewAuthStore as the scoped user: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	// Prove the server enforces privileges; otherwise the assertions below
	// pass vacuously.
	var unauthorized mongo.ServerError
	err = a.s.client.Database(dbName).Collection(collTransactions).FindOne(ctx, bson.D{}).Err()
	if err == nil {
		t.Skip("mongod does not enforce authorization (start it with --auth); nothing to prove")
	}
	if !errors.As(err, &unauthorized) || !unauthorized.HasErrorCode(13) {
		t.Fatalf("reading a neighboring collection: %v, want Unauthorized (13)", err)
	}

	for i := range 2 {
		if err := a.EnsureIndexes(ctx); err != nil {
			t.Fatalf("EnsureIndexes #%d under the scoped role: %v", i+1, err)
		}
	}
	exerciseAuthStore(t, a)
}
