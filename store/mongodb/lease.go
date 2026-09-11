package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/store"
)

// Compile-time proof that *Store satisfies both interfaces the factory hands
// out. Every other backend carries the same pair.
var (
	_ store.Store  = (*Store)(nil)
	_ store.Leaser = (*Store)(nil)
)

// TryAcquireOrRenew implements store.Leaser as one upserting findAndModify:
// the filter admits the row when we hold it or it has expired, and the
// upsert inserts it when it does not exist. When another holder owns an
// unexpired lease the filter misses, the upsert collides on _id, and the
// duplicate-key error is the "lost" signal — reported as (zero, nil), never
// an error. Wall-clock expiry uses the client clock like Aerospike/Pebble.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	now := msNow()
	expires := msTrunc(now.Add(ttl))
	filter := doc(
		kv(fID, name),
		kv(opOr, bson.A{doc(kv(fHolder, holder)), doc(kv(fExpiresAt, doc(kv(opLte, now))))}),
	)
	update := doc(kv(opSet, doc(kv(fHolder, holder), kv(fExpiresAt, expires))))

	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var d leaseDoc
	err := s.leases.FindOneAndUpdate(octx, filter, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&d)
	switch {
	case err == nil:
		return d.ExpiresAt, nil
	case mongo.IsDuplicateKeyError(err), errors.Is(err, mongo.ErrNoDocuments):
		return time.Time{}, nil
	default:
		return time.Time{}, fmt.Errorf("acquire lease %s: %w", name, err)
	}
}

// Release implements store.Leaser; releasing a lease we do not hold is a
// no-op.
func (s *Store) Release(ctx context.Context, name, holder string) error {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.leases.DeleteOne(octx, doc(kv(fID, name), kv(fHolder, holder))); err != nil {
		return fmt.Errorf("release lease %s: %w", name, err)
	}
	return nil
}
