// Package mongodb implements store.Store and store.Leaser on MongoDB.
//
// Design notes, in the order they matter:
//
//   - No multi-document transactions. Every method is correct against a
//     standalone mongod (5.0 or newer — pipeline updates and $unset stages
//     need 4.2, but the reconcile queue's $lookup sub-pipeline is only
//     index-served from 5.0; CI runs 7):
//     correctness comes from single-document atomicity, update filters that
//     carry the guard (status lattice, "still anchored to this block"),
//     aggregation-pipeline updates that evaluate bookkeeping against the row
//     as it stands at write time, and write ordering for the blobs.
//   - Large payloads (compound BUMPs, STUMPs) live in GridFS — a BUMP for a
//     block on a scaling network exceeds the 16 MB document cap — behind a
//     small manifest document keyed by block (bump_manifests) or by
//     block+subtree (stump_manifests). The manifest swap is the linearization
//     point: a writer uploads the file first, atomically points the manifest
//     at it, and deletes only the file the swap replaced, so concurrent
//     rebuilds of one block can never delete each other's upload and a reader
//     always finds either the old or the new file, never neither. Like every
//     guarantee here this assumes reads reach a primary; New warns when the
//     configured read preference says otherwise.
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
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store/bumpcache"
)

const (
	defaultConnectTimeout = 10 * time.Second
	defaultOpTimeout      = 3 * time.Second
	defaultQueryTimeout   = 8 * time.Second
	defaultIndexTimeout   = 5 * time.Minute
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

	// defaultMaxPoolSize mirrors the driver's own default. It is named here
	// because rowConcurrency is derived from the effective pool size and must
	// not guess when the operator left max_pool_size unset.
	defaultMaxPoolSize = 100

	// maxConnecting caps how many connections the pool establishes in
	// parallel. The driver's default is 2, which is sized for a steady
	// trickle, not for this package: the per-row rewrite loops open dozens of
	// concurrent operations the moment a block lands, and against a cold or
	// idle-reaped pool each waits behind a two-at-a-time handshake queue —
	// billed to op_timeout_ms, which is 3 s.
	maxConnecting = 16

	// rowConcurrencyCeiling caps the per-row rewrite parallelism regardless of
	// pool size. Past roughly this point the pool's own wait queue, not the
	// server, becomes the limit: measured on mongod 7 over loopback, the
	// guarded findAndModify loop ran 2,865 ops/s at 16 in flight, 5,114 at
	// 128, and fell back to 2,424 at 256.
	rowConcurrencyCeiling = 128

	// poolHeadroom leaves connections for the point reads, blob round trips
	// and heartbeats that run alongside a rewrite, so a block landing cannot
	// starve the rest of the process of its own pool.
	poolHeadroom = 8
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
	indexTimeout time.Duration
	batchSize    int

	// rowConcurrency bounds the per-row block rewrites. It is deliberately
	// NOT store.BatchConcurrency(), which the other backends' batch loops use
	// and which defaults to runtime.NumCPU(): that is the right shape for a
	// CPU-bound loop, and these loops are neither CPU-bound nor even
	// I/O-bound in the local sense — each row is one command's round trip to
	// mongod, so the useful parallelism is set by the connection pool, not by
	// the core count. See rowConcurrencyFor.
	rowConcurrency int

	// logger carries the warnings this package can only raise at runtime —
	// best-effort blob cleanup that keeps failing, mostly. Never nil.
	logger *zap.Logger

	// tokenReplayLimit is maxTokenReplayScan, a field so tests can lower it.
	tokenReplayLimit int64
}

// New connects to MongoDB, verifies the deployment answers a ping, and
// returns a Store bound to cfg.Database. It does not create indexes: the
// process bootstrap calls EnsureIndexes explicitly, like the other backends.
// A nil logger is accepted and discards the configuration warnings below.
func New(ctx context.Context, cfg config.Mongo, logger *zap.Logger) (*Store, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.URI == "" {
		return nil, errors.New("mongodb: uri is required")
	}
	if cfg.Database == "" {
		return nil, errors.New("mongodb: database is required")
	}
	connectTimeout := msOrDefault(cfg.ConnectTimeoutMs, defaultConnectTimeout)

	// ApplyURI is where a mongodb+srv:// seed list is resolved, and it does
	// so synchronously through the net package's default resolver, which
	// takes no context. Left alone it can outlive every deadline this package
	// or the factory sets and wedge boot against a black-holed resolver, so
	// it runs under ctx here; see awaitWithin for what that does and does not
	// bound.
	opts, err := awaitWithin(ctx, func() *options.ClientOptions {
		return options.Client().ApplyURI(cfg.URI)
	})
	if err != nil {
		return nil, fmt.Errorf("mongodb: parsing uri: %w", err)
	}
	opts.SetAppName("arcade").
		SetConnectTimeout(connectTimeout).
		SetServerSelectionTimeout(connectTimeout)
	// Every guard in this package is a read-then-write or a read-back of
	// something this process just wrote: the status lattice and the "still
	// anchored to this block" predicate ride in update filters, and a blob
	// read pairs a manifest against the file it names. A secondary read can
	// be arbitrarily stale, which turns those into coin flips — a manifest
	// read from a lagging node can report a file as still current after a
	// newer writer replaced it, and the caller then deletes the newer file.
	//
	// Warn rather than refuse: an operator who has deliberately set this may
	// have a reason, and a backend that will not start is worse than one that
	// says loudly what it cannot promise. The warning names the consequence
	// so it is recognizable later from the symptom.
	if rp := opts.ReadPreference; rp != nil && rp.Mode() != readpref.PrimaryMode {
		logger.Warn(
			"store.mongodb.uri sets a non-primary read preference; this backend's write guards "+
				"(status lattice, still-anchored-to-this-block, manifest-versus-file) are all "+
				"read-then-write, and a stale secondary read can let them pass wrongly — expect "+
				"lost status transitions and deleted blobs under concurrency",
			zap.String("read_preference", rp.Mode().String()),
		)
	}
	poolSize := defaultMaxPoolSize
	if cfg.MaxPoolSize > 0 {
		poolSize = cfg.MaxPoolSize
		opts.SetMaxPoolSize(uint64(cfg.MaxPoolSize))
	}
	// Establish connections more than two at a time (see maxConnecting), and
	// keep a small warm floor so the first block after an idle period does not
	// start from zero.
	//
	// MinPoolSize is deliberately NOT the rewrite width: these are idle
	// sockets held open per process, and a deployment runs many arcade pods
	// against one deployment, so a floor of ~90 each would cost mongod
	// thousands of connections to save a ramp that maxConnecting already
	// shortens from ~46 waves to ~6.
	opts.SetMaxConnecting(maxConnecting)
	opts.SetMinPoolSize(uint64(min(maxConnecting, poolSize)))
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
	s.logger = logger
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
		indexTimeout:     msOrDefault(cfg.IndexTimeoutMs, defaultIndexTimeout),
		batchSize:        intOrDefault(cfg.BatchSize, defaultBatchSize),
		rowConcurrency:   rowConcurrencyFor(intOrDefault(cfg.MaxPoolSize, defaultMaxPoolSize)),
		logger:           zap.NewNop(),
		tokenReplayLimit: maxTokenReplayScan,
	}
}

// rowConcurrencyFor derives the per-row rewrite parallelism from the pool the
// client was given.
//
// The loops it bounds spend essentially all of their time waiting on one
// command each, so their useful width is "how many commands can be in flight",
// which is the pool size less the headroom the rest of the process needs — not
// runtime.NumCPU(), and not something an operator should have to discover as a
// third knob. Clamped to rowConcurrencyCeiling because past that the pool's
// wait queue becomes the bottleneck and throughput falls, and floored at 1 so
// a deliberately tiny pool still makes progress.
func rowConcurrencyFor(poolSize int) int {
	return max(1, min(poolSize-poolHeadroom, rowConcurrencyCeiling))
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
