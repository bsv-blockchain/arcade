// Package mongodb implements store.Store and store.Leaser on MongoDB.
//
// Design notes, in the order they matter:
//
//   - No multi-document transactions. Every method is correct against a
//     standalone mongod: correctness comes from single-document atomicity,
//     update filters that carry the guard (status lattice, "still anchored to
//     this block"), an optimistic `version` counter on transaction documents
//     for the block-scoped rewrites, and write ordering for the blobs.
//   - Large payloads (compound BUMPs, STUMPs) live in GridFS — a BUMP for a
//     block on a scaling network exceeds the 16 MB document cap — behind a
//     small manifest document keyed by block (bump_manifests) or by
//     block+subtree (stump_manifests). The manifest swap is the linearization
//     point: a writer uploads the file first, atomically points the manifest
//     at it, and deletes only the file the swap replaced, so concurrent
//     rebuilds of one block can never delete each other's upload and a reader
//     always finds either the old or the new file, never neither.
//   - Absent is the only encoding of "no value". Optional fields carry
//     omitempty, writers clear with $unset, and readers treat absent/null/0
//     alike. Partial indexes and {field: null} predicates depend on this.
//   - Timestamps are BSON datetimes (millisecond precision). Writers truncate
//     to the millisecond and write the truncated value back into the caller's
//     struct, so a value the caller holds equals what a later read returns.
//
// Timeouts: OpTimeout bounds point operations and bounded lists, QueryTimeout
// bounds aggregates. The unbounded iterators (IterateStatusesSince,
// IterateTrackerRows, IterateStatusesByToken) run under the caller's context
// only — a client-wide Timeout would cap a cursor's whole lifetime and kill a
// legitimate multi-million-row scan.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store/bumpcache"
)

const (
	defaultConnectTimeout = 10 * time.Second
	defaultOpTimeout      = 3 * time.Second
	defaultQueryTimeout   = 8 * time.Second
	defaultBatchSize      = 500
	closeTimeout          = 10 * time.Second

	// gridfsChunkSize is the per-chunk payload for BUMP/STUMP uploads. The
	// driver default (255 KiB) would spread a 100 MB compound over ~400
	// documents; 1 MiB keeps chunk fan-out modest while staying far below the
	// 16 MB document cap.
	gridfsChunkSize = 1 << 20

	// maxTokenReplayScan bounds IterateStatusesByToken by the number of
	// submissions registered under a token, matching the Aerospike and Pebble
	// backends. Past it the method refuses with store.ErrReplayUnavailable
	// rather than materializing the txid set — see the interface contract.
	maxTokenReplayScan = 250_000

	// inChunk bounds the $in list size for txid-keyed batch reads.
	inChunk = 1000
)

// Store is the MongoDB backend. Construct with New.
type Store struct {
	client     *mongo.Client
	ownsClient bool
	db         *mongo.Database

	tx       *mongo.Collection
	subs     *mongo.Collection
	blocks   *mongo.Collection
	leases   *mongo.Collection
	datahubs *mongo.Collection
	peers    *mongo.Collection
	// bumps / stumps are the GridFS buckets with the manifest collections
	// that point at the current file; see blobs.go.
	bumps  blobBucket
	stumps blobBucket

	bumpCache *bumpcache.Cache

	opTimeout    time.Duration
	queryTimeout time.Duration
	batchSize    int

	// tokenReplayLimit is maxTokenReplayScan, a field so tests can lower it.
	tokenReplayLimit int64
}

// New connects to MongoDB, verifies the deployment answers a ping, and
// returns a Store bound to cfg.Database. It does not create indexes: the
// process bootstrap calls EnsureIndexes explicitly, like the other backends.
func New(ctx context.Context, cfg config.Mongo) (*Store, error) {
	if cfg.URI == "" {
		return nil, errors.New("mongodb: uri is required")
	}
	if cfg.Database == "" {
		return nil, errors.New("mongodb: database is required")
	}
	connectTimeout := msOrDefault(cfg.ConnectTimeoutMs, defaultConnectTimeout)

	opts := options.Client().ApplyURI(cfg.URI).SetAppName("arcade").
		SetConnectTimeout(connectTimeout).
		SetServerSelectionTimeout(connectTimeout)
	if cfg.MaxPoolSize > 0 {
		opts.SetMaxPoolSize(uint64(cfg.MaxPoolSize))
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		// Never echo the URI: it may carry credentials.
		return nil, fmt.Errorf("mongodb connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		disconnectCtx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer dcancel()
		_ = client.Disconnect(disconnectCtx)
		return nil, fmt.Errorf("mongodb ping: %w", err)
	}

	s := newWithClient(client, cfg.Database, cfg)
	s.ownsClient = true
	return s, nil
}

// newWithClient binds a Store to an existing client. It is the seam New and
// the test harness share; a Store built this way does not own the client and
// Close leaves it connected.
func newWithClient(client *mongo.Client, database string, cfg config.Mongo) *Store {
	db := client.Database(database)
	return &Store{
		client:   client,
		db:       db,
		tx:       db.Collection(collTransactions),
		subs:     db.Collection(collSubmissions),
		blocks:   db.Collection(collBlockProcessing),
		leases:   db.Collection(collLeases),
		datahubs: db.Collection(collDatahubEndpoints),
		peers:    db.Collection(collPeerPolicies),
		bumps: blobBucket{
			bucket:    db.GridFSBucket(options.GridFSBucket().SetName(bucketBumps).SetChunkSizeBytes(gridfsChunkSize)),
			manifests: db.Collection(collBumpManifests),
		},
		stumps: blobBucket{
			bucket:    db.GridFSBucket(options.GridFSBucket().SetName(bucketStumps).SetChunkSizeBytes(gridfsChunkSize)),
			manifests: db.Collection(collStumpManifests),
		},
		bumpCache:        bumpcache.New(),
		opTimeout:        msOrDefault(cfg.OpTimeoutMs, defaultOpTimeout),
		queryTimeout:     msOrDefault(cfg.QueryTimeoutMs, defaultQueryTimeout),
		batchSize:        intOrDefault(cfg.BatchSize, defaultBatchSize),
		tokenReplayLimit: maxTokenReplayScan,
	}
}

// Close disconnects the client this Store created. A Store sharing a client
// (newWithClient) leaves it to its owner.
func (s *Store) Close() error {
	if !s.ownsClient {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	return s.client.Disconnect(ctx)
}

// opCtx bounds a point operation or bounded list query by the configured op
// timeout. context.WithTimeout keeps the earlier of the two deadlines, so a
// caller with a tighter budget is honored, and an already-cancelled ctx
// fails fast without a round-trip.
func (s *Store) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opTimeout)
}

// queryCtx bounds an aggregate or heavier bounded query.
func (s *Store) queryCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.queryTimeout)
}

func msOrDefault(ms int, def time.Duration) time.Duration {
	if ms <= 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

func intOrDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}
