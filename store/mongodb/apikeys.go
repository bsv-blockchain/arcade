package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/broadcaster/store"
)

var (
	_ store.APIKeyStore = (*Store)(nil)
	_ store.QuotaStore  = (*Store)(nil)
)

type apiKeyDoc struct {
	ID        string     `bson:"_id"`
	Hash      string     `bson:"hash"`
	Hint      string     `bson:"hint"`
	Name      string     `bson:"name"`
	Tier      string     `bson:"tier"`
	Status    string     `bson:"status"`
	ExpiresAt *time.Time `bson:"expires_at,omitempty"`
	CreatedAt time.Time  `bson:"created_at"`
	UpdatedAt time.Time  `bson:"updated_at"`
	RevokedAt *time.Time `bson:"revoked_at,omitempty"`
}

type quotaDoc struct {
	Used      int64 `bson:"used"`
	LastGrant int64 `bson:"last_grant"`
}

func toAPIKeyDoc(k *store.APIKey) apiKeyDoc {
	return apiKeyDoc{
		ID: k.ID, Hash: k.Hash, Hint: k.Hint, Name: k.Name, Tier: k.Tier, Status: string(k.Status),
		ExpiresAt: msTruncPtr(k.ExpiresAt), CreatedAt: msTrunc(k.CreatedAt),
		UpdatedAt: msTrunc(k.UpdatedAt), RevokedAt: msTruncPtr(k.RevokedAt),
	}
}

func (d apiKeyDoc) toAPIKey() *store.APIKey {
	return &store.APIKey{
		ID: d.ID, Hash: d.Hash, Hint: d.Hint, Name: d.Name, Tier: d.Tier, Status: store.APIKeyStatus(d.Status),
		ExpiresAt: msTruncPtr(d.ExpiresAt), CreatedAt: msTrunc(d.CreatedAt),
		UpdatedAt: msTrunc(d.UpdatedAt), RevokedAt: msTruncPtr(d.RevokedAt),
	}
}

// CreateAPIKey relies on the _id primary key and the unique hash index: a
// duplicate of either fails the single insert, leaving nothing behind.
func (s *Store) CreateAPIKey(ctx context.Context, k *store.APIKey) error {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.apiKeys.InsertOne(octx, toAPIKeyDoc(k)); err != nil {
		return fmt.Errorf("create api key %s: %w", k.ID, err)
	}
	return nil
}

func (s *Store) findAPIKey(ctx context.Context, filter bson.D) (*store.APIKey, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var d apiKeyDoc
	err := s.apiKeys.FindOne(octx, filter).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, store.ErrAPIKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get api key: %w", err)
	}
	return d.toAPIKey(), nil
}

func (s *Store) GetAPIKeyByHash(ctx context.Context, hash string) (*store.APIKey, error) {
	return s.findAPIKey(ctx, doc(kv(fAPIKeyHash, hash)))
}

func (s *Store) GetAPIKey(ctx context.Context, id string) (*store.APIKey, error) {
	return s.findAPIKey(ctx, idFilter(id))
}

func (s *Store) ListAPIKeys(ctx context.Context) ([]*store.APIKey, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.apiKeys.Find(qctx, doc(), options.Find().SetSort(doc(kv(fCreatedAt, 1), kv(fID, 1))))
	if err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	var docs []apiKeyDoc
	if err := cur.All(qctx, &docs); err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	out := make([]*store.APIKey, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.toAPIKey())
	}
	return out, nil
}

func (s *Store) UpdateAPIKeyTier(ctx context.Context, id, tier string, at time.Time) error {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	res, err := s.apiKeys.UpdateOne(octx, idFilter(id),
		doc(kv(opSet, doc(kv(fAPIKeyTier, tier), kv(fUpdatedAt, msTrunc(at))))))
	if err != nil {
		return fmt.Errorf("update api key tier %s: %w", id, err)
	}
	if res.MatchedCount == 0 {
		return store.ErrAPIKeyNotFound
	}
	return nil
}

func (s *Store) RevokeAPIKey(ctx context.Context, id string, at time.Time) error {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	ts := msTrunc(at)
	res, err := s.apiKeys.UpdateOne(octx,
		doc(kv(fID, id), kv(fStatus, doc(kv(opNe, string(store.APIKeyRevoked))))),
		doc(kv(opSet, doc(kv(fStatus, string(store.APIKeyRevoked)), kv(fAPIKeyRevokedAt, ts), kv(fUpdatedAt, ts)))))
	if err != nil {
		return fmt.Errorf("revoke api key %s: %w", id, err)
	}
	if res.MatchedCount > 0 {
		return nil
	}
	err = s.apiKeys.FindOne(octx, idFilter(id), options.FindOne().SetProjection(doc(kv(fID, 1)))).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return store.ErrAPIKeyNotFound
	}
	if err != nil {
		return fmt.Errorf("revoke api key %s: %w", id, err)
	}
	return nil // already revoked
}

func quotaID(keyID string, windowStart time.Time) string {
	return keyID + "|" + strconv.FormatInt(windowStart.UnixMilli(), 10)
}

// ReserveQuota is one upserting findAndModify with a pipeline update. Every
// expression in a $set stage reads the stage's input document, so last_grant
// is computed from the pre-update used in the same atomic write that advances
// it. The loser of an insert race for a brand-new window retries
// (withDupKeyRetry) and takes the update path.
func (s *Store) ReserveQuota(ctx context.Context, keyID string, windowStart time.Time, want, limit int64) (int64, int64, error) {
	cur := doc(kv(opIfNull, bson.A{"$" + fQuotaUsed, int64(0)}))
	next := doc(kv(opMax, bson.A{cur, doc(kv(opMin, bson.A{doc(kv(opAdd, bson.A{cur, want})), limit}))}))
	update := mongo.Pipeline{doc(kv(opSet, doc(
		kv(fQuotaLastGrant, doc(kv(opSubtract, bson.A{next, cur}))),
		kv(fQuotaUsed, next),
		kv(fExpiresAt, msTrunc(windowStart.Add(store.QuotaRetention))),
	)))}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	var d quotaDoc
	err := s.withDupKeyRetry(ctx, func(octx context.Context) error {
		d = quotaDoc{}
		return s.apiQuota.FindOneAndUpdate(octx, idFilter(quotaID(keyID, windowStart)), update, opts).Decode(&d)
	})
	if err != nil {
		return 0, 0, fmt.Errorf("reserve quota %s: %w", keyID, err)
	}
	return d.LastGrant, d.Used, nil
}
