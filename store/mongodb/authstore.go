package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
)

var (
	_ store.APIKeyStore = (*AuthStore)(nil)
	_ store.QuotaStore  = (*AuthStore)(nil)
)

// authMinPoolFloor is the auth store's idle-connection floor. Its traffic is
// one point read per key-cache miss and one findAndModify per quota lease,
// and every api-server of every network shares the one auth deployment, so
// the main store's warm floor (maxConnecting) would only cost connections.
const authMinPoolFloor = 1

// AuthStore is a MongoDB-backed APIKeyStore and QuotaStore bound to a
// dedicated database (auth.store.mongodb), so several arcade deployments —
// one per network, each with its own main store — share one key set and one
// quota window per key.
//
// It is deliberately narrower than Store: it holds handles to the api_keys
// and api_quota collections only, and never runs a database-level command.
// That lets it run under a role scoped to just those two collections (find,
// insert, update, remove, createIndex, listIndexes, createCollection), and
// lets the auth database be one that another arcade's main store also uses:
// the indexes it creates are exactly the ones Store.EnsureIndexes creates on
// those collections, under the same names.
type AuthStore struct {
	s *Store
}

// NewAuthStore connects to cfg.URI, pings the primary (on the admin
// database, which needs no privileges) and binds to cfg.Database. Like New
// it creates no indexes; call EnsureIndexes.
func NewAuthStore(ctx context.Context, cfg config.Mongo, logger *zap.Logger) (*AuthStore, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	client, err := connect(ctx, cfg, logger, "auth.store.mongodb.uri", authMinPoolFloor)
	if err != nil {
		return nil, err
	}
	s := newAuthWithClient(client, cfg.Database, cfg)
	s.s.logger = logger
	s.s.ownsClient = true
	return s, nil
}

// newAuthWithClient is the slim counterpart of newWithClient: a Store with
// only the key and quota collections set. Every other collection handle is
// nil, so a code path that strayed outside them would fail loudly instead of
// touching another collection.
func newAuthWithClient(client *mongo.Client, database string, cfg config.Mongo) *AuthStore {
	db := client.Database(database)
	return &AuthStore{s: &Store{
		client:       client,
		db:           db,
		apiKeys:      db.Collection(collAPIKeys),
		apiQuota:     db.Collection(collAPIQuota),
		opTimeout:    msOrDefault(cfg.OpTimeoutMs, defaultOpTimeout),
		queryTimeout: msOrDefault(cfg.QueryTimeoutMs, defaultQueryTimeout),
		indexTimeout: msOrDefault(cfg.IndexTimeoutMs, defaultIndexTimeout),
		logger:       zap.NewNop(),
	}}
}

// EnsureIndexes provisions the api_keys and api_quota indexes and nothing
// else (createIndexes implicitly creates the collection). The api_quota TTL
// repair path in createIndexes would need collMod, but it only runs if an
// existing TTL differs from this build's, and Store.EnsureIndexes declares
// the same value.
func (a *AuthStore) EnsureIndexes(ctx context.Context) error {
	return a.s.provisionIndexes(ctx, a.s.authIndexSpecs(), map[string][]ttlIndex{
		collAPIQuota: {{idxQuotaExpires, 0}},
	})
}

// Close disconnects the client NewAuthStore created.
func (a *AuthStore) Close() error { return a.s.Close() }

func (a *AuthStore) CreateAPIKey(ctx context.Context, k *store.APIKey) error {
	return a.s.CreateAPIKey(ctx, k)
}

func (a *AuthStore) GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error) {
	return a.s.GetAPIKeyByHash(ctx, hash)
}

func (a *AuthStore) GetAPIKey(ctx context.Context, id string) (*store.APIKey, error) {
	return a.s.GetAPIKey(ctx, id)
}

func (a *AuthStore) ListAPIKeys(ctx context.Context) ([]*store.APIKey, error) {
	return a.s.ListAPIKeys(ctx)
}

func (a *AuthStore) UpdateAPIKeyTier(ctx context.Context, id, tier string, at time.Time) error {
	return a.s.UpdateAPIKeyTier(ctx, id, tier, at)
}

func (a *AuthStore) RevokeAPIKey(ctx context.Context, id string, at time.Time) error {
	return a.s.RevokeAPIKey(ctx, id, at)
}

func (a *AuthStore) ReserveQuota(ctx context.Context, keyID string, windowStart time.Time, want, limit int64) (int64, int64, error) {
	return a.s.ReserveQuota(ctx, keyID, windowStart, want, limit)
}
