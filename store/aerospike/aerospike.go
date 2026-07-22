package aerospike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/bumpcache"
)

func isKeyNotFound(err error) bool {
	return errors.Is(err, aero.ErrKeyNotFound)
}

// isGenerationErr is true when an Aerospike write fails because the record was
// modified between our read and our CAS write (EXPECT_GEN_EQUAL mismatch).
func isGenerationErr(err error) bool {
	var aerr aero.Error
	if errors.As(err, &aerr) {
		return aerr.Matches(types.GENERATION_ERROR)
	}
	return false
}

const (
	setTransactions     = "arcade_transactions"
	setBumps            = "arcade_bumps"
	setStumps           = "arcade_stumps"
	setSubmissions      = "arcade_submissions"
	setBlockProcessing  = "arcade_block_processing"
	setLeases           = "arcade_leases"
	setDatahubEndpoints = "arcade_datahub_endpoints"
)

// BUMP chunking — large compound BUMPs (scaling networks with millions of
// txs/block produce tens of MiB of merkle path data) cannot fit in a single
// Aerospike record because record size is bounded by the namespace's
// write-block-size (default 1 MiB, max 8 MiB). InsertBUMP/GetBUMP split the
// payload across N+1 records: a manifest at <blockHash> with chunk_count, and
// chunk records at <blockHash>:c:<index>.
const (
	bumpFormatVersion         = 1
	bumpDefaultChunkSizeBytes = 768 * 1024
	bumpChunkKeyFormat        = "%s:c:%08d"
)

// STUMP chunking mirrors BUMP chunking above — a single subtree's STUMP on a
// busy block also overruns Aerospike's per-record ceiling, and an unchunked
// InsertStump then fails with RECORD_TOO_BIG, losing the block's STUMPs so the
// bump builder never marks its txs MINED. See the STUMP Operations section for
// the manifest/chunk layout. The chunk size is shared with BUMP (bumpChunkSize)
// since the same namespace write-block-size bounds both record types.
const (
	stumpFormatVersion  = 1
	stumpChunkKeyFormat = "%s:%d:c:%08d"
)

// Ensure Store implements the store interfaces.
var (
	_ store.Store  = (*Store)(nil)
	_ store.Leaser = (*Store)(nil)
)

// Store is the Aerospike-backed implementation of store.Store and store.Leaser.
type Store struct {
	client        *aero.Client
	namespace     string
	batchSize     int
	queryTimeout  time.Duration
	opTimeout     time.Duration
	socketTimeout time.Duration
	bumpChunkSize int
	// bumpCache holds parsed+indexed compound BUMPs for merkle-path
	// enrichment — sizing, budget, and singleflight live in bumpcache.
	// Especially valuable here: a miss reassembles the chunked BUMP from
	// Aerospike before parsing.
	bumpCache *bumpcache.Cache
}

// New creates an Aerospike-backed Store connected to the configured cluster.
func New(cfg config.Aero) (*Store, error) {
	hosts := make([]*aero.Host, 0, len(cfg.Hosts))
	for _, h := range cfg.Hosts {
		hostname, portStr, err := net.SplitHostPort(h)
		if err != nil {
			// No port specified, use default
			hosts = append(hosts, aero.NewHost(h, 3000))
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port in host %q: %w", h, err)
		}
		hosts = append(hosts, aero.NewHost(hostname, port))
	}

	policy := aero.NewClientPolicy()
	policy.ConnectionQueueSize = cfg.PoolSize
	// IdleTimeout reaps idle pooled connections client-side. Required when the
	// Aerospike server runs with proto-fd-idle-ms=0 (no server-side idle
	// reap) — without this, connections pile up forever and starve the pool
	// (we observed 35k zombie conns on a single node before adding this).
	// Should be set a few seconds below the server's proto-fd-idle-ms when
	// nonzero. cfg.IdleTimeoutSec=0 falls back to the safe default.
	idleSec := cfg.IdleTimeoutSec
	if idleSec <= 0 {
		idleSec = 55
	}
	policy.IdleTimeout = time.Duration(idleSec) * time.Second

	// Aggressive node-level circuit breaker. In a multi-tenant Aerospike
	// cluster a single slow node — squeezed by another namespace's traffic
	// — will otherwise drain the per-node pool because every retry just
	// piles another timeout on the unhealthy peer. The default
	// MaxErrorRate=100 is too lenient; we trip the breaker much sooner so
	// the client returns MAX_ERROR_RATE for affected partitions instead of
	// holding pool slots open while waiting for timeouts.
	if cfg.MaxErrorRate > 0 {
		policy.MaxErrorRate = cfg.MaxErrorRate
	} else {
		policy.MaxErrorRate = 5
	}
	if cfg.ErrorRateWindow > 0 {
		policy.ErrorRateWindow = cfg.ErrorRateWindow
	} else {
		policy.ErrorRateWindow = 1
	}

	client, err := aero.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to aerospike: %w", err)
	}

	bumpChunkSize := cfg.BumpChunkSizeBytes
	if bumpChunkSize <= 0 {
		bumpChunkSize = bumpDefaultChunkSizeBytes
	}

	s := &Store{
		client:        client,
		namespace:     cfg.Namespace,
		batchSize:     cfg.BatchSize,
		queryTimeout:  time.Duration(cfg.QueryTimeoutMs) * time.Millisecond,
		opTimeout:     time.Duration(cfg.OpTimeoutMs) * time.Millisecond,
		socketTimeout: time.Duration(cfg.SocketTimeoutMs) * time.Millisecond,
		bumpChunkSize: bumpChunkSize,
		bumpCache:     bumpcache.New(),
	}

	if err := s.EnsureIndexes(); err != nil {
		client.Close()
		return nil, fmt.Errorf("creating indexes: %w", err)
	}

	return s, nil
}

func (s *Store) Healthy() bool {
	return s.client.IsConnected()
}

func (s *Store) Close() error {
	s.client.Close()
	return nil
}

func (s *Store) EnsureIndexes() error {
	indexes := []struct {
		set, bin, name string
		indexType      aero.IndexType
	}{
		{setStumps, "block_hash", "arcade_idx_stumps_block_hash", aero.STRING},
		{setTransactions, "block_hash", "arcade_idx_tx_block_hash", aero.STRING},
		{setSubmissions, "txid", "arcade_idx_sub_txid", aero.STRING},
		{setSubmissions, "callback_token", "arcade_idx_sub_callback_token", aero.STRING},
		{setBlockProcessing, "block_height", "arcade_idx_bp_block_height", aero.NUMERIC},
		{setBlockProcessing, "status", "arcade_idx_bp_status", aero.STRING},
		{setTransactions, "status", "arcade_idx_tx_status", aero.STRING},
		{setDatahubEndpoints, "last_seen", "arcade_idx_dh_last_seen", aero.NUMERIC},
	}
	// CreateIndex returns an IndexTask that must be polled — the server call
	// is asynchronous and the index won't be queryable on every node until
	// the task is done. Skipping this wait was the proximate cause of the
	// INDEX_NOTFOUND storm we saw in production: the service started
	// consuming Kafka messages before propagation finished, every query
	// returned an error, the client retried, and the connection pool drained.
	for _, idx := range indexes {
		task, err := s.client.CreateIndex(nil, s.namespace, idx.set, idx.name, idx.bin, idx.indexType)
		if err != nil {
			if strings.Contains(err.Error(), "Index already exists") {
				continue
			}
			return fmt.Errorf("creating index %s: %w", idx.name, err)
		}
		if task == nil {
			continue
		}
		select {
		case taskErr := <-task.OnComplete():
			if taskErr != nil {
				return fmt.Errorf("waiting for index %s: %w", idx.name, taskErr)
			}
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timed out waiting for index %s to build", idx.name)
		}
	}
	return nil
}

func (s *Store) key(set, pk string) (*aero.Key, error) {
	return aero.NewKey(s.namespace, set, pk)
}

// remaining returns the time budget for a single Aerospike operation under ctx.
// If ctx is already done, returns 0 so the client fails fast without borrowing
// a pool slot. If ctx has no deadline, falls back to the configured default.
// Otherwise returns the shorter of (deadline-remaining, default).
func remaining(ctx context.Context, def time.Duration) time.Duration {
	if err := ctx.Err(); err != nil {
		return 0
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return def
	}
	rem := time.Until(dl)
	if rem <= 0 {
		return 0
	}
	if def > 0 && rem > def {
		return def
	}
	return rem
}

// queryPolicy builds a QueryPolicy whose TotalTimeout tracks ctx deadline,
// capped by the configured query timeout. MaxRetries is kept low so a
// failing query can't hog a connection across many server-side retries.
func (s *Store) queryPolicy(ctx context.Context) *aero.QueryPolicy {
	p := aero.NewQueryPolicy()
	p.TotalTimeout = remaining(ctx, s.queryTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 1
	return p
}

// readPolicy builds a BasePolicy for single-record reads.
func (s *Store) readPolicy(ctx context.Context) *aero.BasePolicy {
	p := aero.NewPolicy()
	p.TotalTimeout = remaining(ctx, s.opTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 2
	return p
}

// writePolicy builds a WritePolicy for single-record writes. Callers can
// mutate RecordExistsAction, Expiration, GenerationPolicy, etc. on the
// returned struct.
func (s *Store) writePolicy(ctx context.Context) *aero.WritePolicy {
	p := aero.NewWritePolicy(0, 0)
	p.TotalTimeout = remaining(ctx, s.opTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 2
	return p
}

// batchPolicy builds a BatchPolicy for multi-record operations. Retries are
// disabled at the client layer because batch failures amplify connection
// pressure (every retry re-fans-out across every node, including the sick
// one), and Kafka-driven application retries handle real recovery.
func (s *Store) batchPolicy(ctx context.Context) *aero.BatchPolicy {
	p := aero.NewBatchPolicy()
	p.TotalTimeout = remaining(ctx, s.opTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 0
	return p
}

// --- Transaction Status Operations ---

func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	key, err := s.key(setTransactions, status.TxID)
	if err != nil {
		return nil, false, err
	}

	// Try to read existing
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil && !isKeyNotFound(err) {
		return nil, false, fmt.Errorf("get status: %w", err)
	}
	if rec != nil {
		existing := recordToStatus(rec, status.TxID)
		return existing, false, nil
	}

	// Insert new
	now := time.Now()
	if status.Timestamp.IsZero() {
		status.Timestamp = now
	}
	st := string(models.StatusReceived)
	if status.Status != "" {
		st = string(status.Status)
	}

	bins := aero.BinMap{
		"txid":       status.TxID,
		"status":     st,
		"timestamp":  status.Timestamp.UnixMilli(),
		"created_at": now.UnixMilli(),
	}
	// Rows can be born terminal (intake rejection): carry the machine-
	// readable cause and code on the insert, matching the other backends.
	if status.StatusCode != 0 {
		bins["status_code"] = status.StatusCode
	}
	if status.ExtraInfo != "" {
		bins["extra_info"] = status.ExtraInfo
	}

	policy := s.writePolicy(ctx)
	policy.RecordExistsAction = aero.CREATE_ONLY
	if err := s.client.Put(policy, key, bins); err != nil {
		// Race condition: someone else inserted — re-read
		rec, getErr := s.client.Get(s.readPolicy(ctx), key)
		if getErr != nil && !isKeyNotFound(getErr) {
			return nil, false, fmt.Errorf("re-read after conflict: %w", getErr)
		}
		if rec != nil {
			return recordToStatus(rec, status.TxID), false, nil
		}
		return nil, false, fmt.Errorf("insert status: %w", err)
	}

	status.Status = models.Status(st)
	status.CreatedAt = now
	return status, true, nil
}

// BatchGetOrInsertStatus runs GetOrInsertStatus concurrently for each row.
// Aerospike's BatchOperate API doesn't support per-record CREATE_ONLY with
// the existing-row read-back semantics this method needs, so the parallel-
// loop fallback is the simplest correct path. Each Aerospike op is already
// fast (~1-3ms) so 16-way parallelism easily saturates throughput.
func (s *Store) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return store.BatchGetOrInsertStatusParallel(ctx, s, statuses)
}

// BatchUpdateStatus runs UpdateStatus concurrently for each row.
func (s *Store) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	return store.BatchUpdateStatusParallel(ctx, s, statuses)
}

// BatchUpdateStatusReturning runs UpdateStatus concurrently and returns the
// previous row per input. Aerospike's UpdateStatus doesn't yet expose the
// pre-merge bin, so we fall back to GetStatus+UpdateStatus per row — two
// round-trips. Arcade's primary deployment uses Pebble (which fuses the
// read into the same locked region); this fallback exists to satisfy the
// store.Store contract.
func (s *Store) BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]*models.TransactionStatus, error) {
	return store.BatchUpdateStatusReturningFallback(ctx, s, statuses)
}

// UpdateStatus updates an existing transaction record. If no record exists for
// status.TxID the call returns store.ErrNotFound without writing — callers
// must use GetOrInsertStatus to create new rows. This guard closes F-033 /
// issue #91: previously a callback referencing a never-submitted txid would
// create a phantom row with no submission/validation history, turning the
// callback endpoint into a write-anywhere primitive.
func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	key, err := s.key(setTransactions, status.TxID)
	if err != nil {
		return err
	}

	bins := aero.BinMap{
		"status":    string(status.Status),
		"timestamp": status.Timestamp.UnixMilli(),
	}
	if status.StatusCode != 0 {
		bins["status_code"] = status.StatusCode
	}
	if status.BlockHash != "" {
		bins["block_hash"] = status.BlockHash
	}
	if status.BlockHeight > 0 {
		bins["block_height"] = int(status.BlockHeight) //nolint:gosec // block height fits in int on 64-bit platforms (max ~10^9 << math.MaxInt32)
	}
	if status.ExtraInfo != "" {
		bins["extra_info"] = status.ExtraInfo
	}
	if len(status.MerklePath) > 0 {
		bins["merkle_path"] = []byte(status.MerklePath)
	}
	if !status.MerkleRegisteredAt.IsZero() {
		bins["merkle_reg_at"] = status.MerkleRegisteredAt.UnixMilli()
	}

	// Enforce the status lattice: refuse to overwrite a terminal status with a
	// later, lower-priority update (e.g. a stray SEEN_ON_NETWORK callback after
	// MINED). Read-then-CAS-write using the record's generation guarantees the
	// pre-write check and the write are atomic with respect to other writers.
	// See models.Status.CanTransitionFrom and #61 / F-003.
	//
	// Also: never create a record from UpdateStatus (F-033 / #91) — if the
	// record is genuinely absent we return ErrNotFound. UPDATE_ONLY on the
	// write enforces this even if a racing writer deleted the row between
	// our read and our put.
	for {
		rec, gerr := s.client.Get(s.readPolicy(ctx), key, "status")
		if gerr != nil && !isKeyNotFound(gerr) {
			return fmt.Errorf("read status for lattice check %s: %w", status.TxID, gerr)
		}
		if rec == nil {
			return store.ErrNotFound
		}
		if status.Status != "" {
			existing := models.Status(getString(rec, "status"))
			if !status.Status.CanTransitionFrom(existing) {
				return nil
			}
		}
		policy := s.writePolicy(ctx)
		policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
		policy.Generation = rec.Generation
		policy.RecordExistsAction = aero.UPDATE_ONLY
		if err := s.client.Put(policy, key, bins); err != nil {
			// Generation mismatch means another writer landed between our read
			// and our put. Re-read and re-evaluate the lattice rather than
			// silently clobbering their write.
			if isGenerationErr(err) {
				continue
			}
			// UPDATE_ONLY on a record that was deleted between our read and put.
			if isKeyNotFound(err) {
				return store.ErrNotFound
			}
			return fmt.Errorf("update tx %s: %w", status.TxID, err)
		}
		return nil
	}
}

func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return nil, err
	}

	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil && !isKeyNotFound(err) {
		return nil, fmt.Errorf("get status %s: %w", txid, err)
	}
	if rec == nil {
		return nil, nil
	}

	status := recordToStatus(rec, txid)
	s.enrichMerklePath(ctx, status)
	return status, nil
}

func (s *Store) GetStatusesSince(ctx context.Context, _ time.Time) ([]*models.TransactionStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Scan all transactions — for TxTracker loading
	stmt := aero.NewStatement(s.namespace, setTransactions)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query statuses: %w", err)
	}

	var (
		results []*models.TransactionStatus
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			txid := getString(rec.Record, "txid")
			results = append(results, recordToStatus(rec.Record, txid))
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return results, nil
}

// IterateStatusesSince streams scan results to fn one record at a time. The
// Aerospike client already produces records over a channel — we just hand
// them off without buffering, so peak memory is O(1) plus whatever fn keeps.
func (s *Store) IterateStatusesSince(ctx context.Context, _ time.Time, fn func(*models.TransactionStatus) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return fmt.Errorf("query statuses: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec, ok := <-rs.Results():
			if !ok {
				return nil
			}
			if rec.Err != nil {
				return rec.Err
			}
			txid := getString(rec.Record, "txid")
			if err := fn(recordToStatus(rec.Record, txid)); err != nil {
				return err
			}
		}
	}
}

func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions)
	_ = stmt.SetFilter(aero.NewEqualFilter("block_hash", blockHash))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query by block hash: %w", err)
	}

	var (
		txids   []string
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			txid := getString(rec.Record, "txid")
			if txid == "" {
				continue
			}
			key, err := s.key(setTransactions, txid)
			if err != nil {
				continue
			}
			bins := aero.BinMap{"status": string(newStatus), "timestamp": time.Now().UnixMilli()}
			if err := s.client.Put(s.writePolicy(ctx), key, bins); err != nil {
				continue
			}
			txids = append(txids, txid)
		}
	}
	if loopErr != nil {
		return txids, loopErr
	}
	return txids, nil
}

// BumpRetryCount atomically increments retry_count and returns the new value.
// Bin writes for status / raw_tx / next_retry_at are handled separately by
// SetPendingRetryFields so callers can compute the correct backoff from the
// post-increment count.
func (s *Store) BumpRetryCount(ctx context.Context, txid string) (int, error) {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return 0, err
	}
	// AddOp is non-idempotent — set MaxRetries=0 so a timed-out attempt is not
	// retried and double-counted.
	wp := s.writePolicy(ctx)
	wp.MaxRetries = 0
	rec, err := s.client.Operate(wp, key, aero.AddOp(aero.NewBin("retry_count", 1)), aero.GetOp())
	if err != nil {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, err)
	}
	if v, ok := rec.Bins["retry_count"]; ok {
		if n, ok := v.(int); ok {
			return n, nil
		}
	}
	return 1, nil
}

// SetPendingRetryFields writes the durable retry bins in one atomic call. The
// caller is responsible for having already bumped retry_count and computed
// next_retry_at from that post-increment value.
func (s *Store) SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time) error {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return err
	}
	ops := []*aero.Operation{
		aero.PutOp(aero.NewBin("status", string(models.StatusPendingRetry))),
		aero.PutOp(aero.NewBin("raw_tx", rawTx)),
		aero.PutOp(aero.NewBin("next_retry_at", nextRetryAt.UnixMilli())),
		aero.PutOp(aero.NewBin("timestamp", time.Now().UnixMilli())),
	}
	if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, err)
	}
	return nil
}

// GetReadyRetries uses the existing arcade_idx_tx_status index to find PENDING_RETRY
// rows and filters by next_retry_at in code. At expected cardinality
// (thousands, not millions) this is cheap.
func (s *Store) GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions)
	_ = stmt.SetFilter(aero.NewEqualFilter("status", string(models.StatusPendingRetry)))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query pending retry txs: %w", err)
	}

	nowMs := now.UnixMilli()
	results := make([]*store.PendingRetry, 0, limit)
	var loopErr error
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			nextMs := getInt64(rec.Record, "next_retry_at")
			if nextMs > nowMs {
				continue
			}
			rawTx, _ := rec.Record.Bins["raw_tx"].([]byte)
			if len(rawTx) == 0 {
				// Legacy PENDING_RETRY rows from before durable retries — skip.
				continue
			}
			results = append(results, &store.PendingRetry{
				TxID:        getString(rec.Record, "txid"),
				RawTx:       rawTx,
				RetryCount:  getInt(rec.Record, "retry_count"),
				NextRetryAt: time.UnixMilli(nextMs),
			})
			if len(results) >= limit {
				break loop
			}
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return results, nil
}

// ClearRetryState transitions a tx out of PENDING_RETRY and deletes the retry
// bins so the row stops matching the reaper's query. retry_count is retained
// for observability.
func (s *Store) ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	key, err := s.key(setTransactions, txid)
	if err != nil {
		return err
	}
	ops := []*aero.Operation{
		aero.PutOp(aero.NewBin("status", string(finalStatus))),
		aero.PutOp(aero.NewBin("timestamp", time.Now().UnixMilli())),
		// Aerospike: writing a nil bin value deletes the bin.
		aero.PutOp(aero.NewBin("raw_tx", nil)),
		aero.PutOp(aero.NewBin("next_retry_at", nil)),
	}
	if extraInfo != "" {
		ops = append(ops, aero.PutOp(aero.NewBin("extra_info", extraInfo)))
	}
	if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
		return fmt.Errorf("clear retry state %s: %w", txid, err)
	}
	return nil
}

// SetMinedByTxIDs writes a MINED status batch keyed by blockHash + blockHeight.
// blockHeight is persisted as the block_height bin and echoed back on each
// returned TransactionStatus so SSE/webhook consumers always see the height
// alongside the hash (issue #87 / F-029).
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	now := time.Now()
	var prevs, statuses []*models.TransactionStatus

	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.UPDATE_ONLY

	for i := 0; i < len(txids); i += s.batchSize {
		if err := ctx.Err(); err != nil {
			return prevs, statuses, err
		}
		end := i + s.batchSize
		if end > len(txids) {
			end = len(txids)
		}
		batch := txids[i:end]

		// Snapshot pre-update rows so callers can observe the MINED
		// transition age. BatchGet is a second round-trip but matches the
		// access pattern Pebble does atomically via per-shard locks; for
		// the current production deployment (Pebble) this code path is
		// unused, so the extra latency is acceptable here.
		keys := make([]*aero.Key, 0, len(batch))
		keyByTxID := make(map[string]int, len(batch))
		for _, txid := range batch {
			key, err := s.key(setTransactions, txid)
			if err != nil {
				continue
			}
			keyByTxID[txid] = len(keys)
			keys = append(keys, key)
		}
		prevByTxID := make(map[string]*models.TransactionStatus, len(batch))
		if len(keys) > 0 {
			recs, err := s.client.BatchGet(s.batchPolicy(ctx), keys)
			if err != nil {
				return prevs, statuses, fmt.Errorf("batch get for set mined prev: %w", err)
			}
			for txid, idx := range keyByTxID {
				if idx >= len(recs) || recs[idx] == nil {
					continue
				}
				prevByTxID[txid] = recordToStatus(recs[idx], txid)
			}
		}

		records := make([]aero.BatchRecordIfc, len(batch))
		for j, txid := range batch {
			key, err := s.key(setTransactions, txid)
			if err != nil {
				continue
			}
			ops := []*aero.Operation{
				aero.PutOp(aero.NewBin("status", string(models.StatusMined))),
				aero.PutOp(aero.NewBin("block_hash", blockHash)),
				aero.PutOp(aero.NewBin("block_height", int(blockHeight))), //nolint:gosec // block height fits in int on 64-bit platforms
				aero.PutOp(aero.NewBin("timestamp", now.UnixMilli())),
			}
			records[j] = aero.NewBatchWrite(bwp, key, ops...)
		}

		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return prevs, statuses, fmt.Errorf("batch set mined: %w", err)
		}

		for j, txid := range batch {
			if records[j] != nil && records[j].BatchRec().Err == nil {
				if prev, ok := prevByTxID[txid]; ok {
					prevs = append(prevs, prev)
				} else {
					prevs = append(prevs, nil)
				}
				statuses = append(statuses, &models.TransactionStatus{
					TxID:        txid,
					Status:      models.StatusMined,
					BlockHash:   blockHash,
					BlockHeight: blockHeight,
					Timestamp:   now,
				})
			}
		}
	}

	return prevs, statuses, nil
}

// MarkMerkleRegisteredByTxIDs writes merkle_reg_at = ts.UnixMilli() on
// every existing transaction record in the txid list. Unknown txids are
// silently skipped via UPDATE_ONLY (matching SetMinedByTxIDs). Used by the
// startup replay loop to skip rows registered recently (issue #145).
func (s *Store) MarkMerkleRegisteredByTxIDs(ctx context.Context, txids []string, ts time.Time) error {
	if len(txids) == 0 {
		return nil
	}
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.UPDATE_ONLY

	for i := 0; i < len(txids); i += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := i + s.batchSize
		if end > len(txids) {
			end = len(txids)
		}
		batch := txids[i:end]

		records := make([]aero.BatchRecordIfc, len(batch))
		for j, txid := range batch {
			key, err := s.key(setTransactions, txid)
			if err != nil {
				continue
			}
			records[j] = aero.NewBatchWrite(
				bwp, key,
				aero.PutOp(aero.NewBin("merkle_reg_at", ts.UnixMilli())),
			)
		}

		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch mark merkle registered: %w", err)
		}
	}
	return nil
}

// --- BUMP Operations ---
//
// BUMPs are stored as a manifest record at primary key <blockHash> plus N
// chunk records at <blockHash>:c:<index>. Chunking is unconditional — every
// BUMP, however small, takes one manifest + at least one chunk record. This
// is required because compound BUMPs on scaling networks routinely exceed
// Aerospike's per-record ceiling (write-block-size, default 1 MiB, max 8 MiB).
//
// Manifest bins: block_hash, block_height, chunk_count, total_size,
// format_version. The manifest carries no bump_data — it points exclusively
// at chunk records.
//
// Chunk bins: block_hash, chunk_index, chunk_data.
//
// Atomicity: chunks are written first (BatchOperate), then the manifest. The
// manifest write is the linearization point — readers see the new BUMP only
// after it lands. A crash between chunk write and manifest write leaves
// orphan chunks but the previous manifest still references the previous
// chunks, so readers continue to see the old BUMP intact.

// InsertBUMP stores a compound BUMP as a manifest plus chunks. Chunks are
// written before the manifest so a concurrent reader observing the new
// manifest is guaranteed to find every chunk it references.
func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	if blockHash == "" {
		return fmt.Errorf("insert bump: empty block hash")
	}

	// Read the previous manifest so we know whether to clean up orphan chunks
	// from a prior larger BUMP for the same block. Failure here is non-fatal
	// for the write itself — if we can't read it, we skip cleanup; the write
	// still succeeds and orphans are unreferenced.
	oldChunkCount := 0
	if oldManifestKey, err := s.key(setBumps, blockHash); err == nil {
		if rec, err := s.client.Get(s.readPolicy(ctx), oldManifestKey, "chunk_count"); err == nil && rec != nil {
			if v, ok := rec.Bins["chunk_count"].(int); ok && v > 0 {
				oldChunkCount = v
			}
		}
	}

	newChunkCount := 1
	if len(bumpData) > s.bumpChunkSize {
		newChunkCount = (len(bumpData) + s.bumpChunkSize - 1) / s.bumpChunkSize
	}

	if err := s.writeBumpChunks(ctx, blockHash, bumpData, newChunkCount); err != nil {
		return fmt.Errorf("write bump chunks for %s: %w", blockHash, err)
	}

	manifestKey, err := s.key(setBumps, blockHash)
	if err != nil {
		return err
	}
	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = aero.REPLACE
	bins := aero.BinMap{
		"block_hash":     blockHash,
		"block_height":   int(blockHeight), //nolint:gosec // block height fits in int on 64-bit platforms
		"chunk_count":    newChunkCount,
		"total_size":     len(bumpData),
		"format_version": bumpFormatVersion,
	}
	if err := s.client.Put(wp, manifestKey, bins); err != nil {
		return fmt.Errorf("write bump manifest for %s: %w", blockHash, err)
	}

	// Best-effort cleanup of orphan chunks from a prior write that had more
	// chunks than this one. Orphans are unreferenced (the manifest now caps at
	// newChunkCount) so a failure here only wastes disk.
	if oldChunkCount > newChunkCount {
		if err := s.deleteBumpChunkRange(ctx, blockHash, newChunkCount, oldChunkCount); err != nil {
			// Don't fail the write — orphans are harmless aside from disk.
			// The next InsertBUMP for this block retries the cleanup.
			_ = err
		}
	}
	// A rebuild can overwrite the compound for an existing block (e.g. late
	// STUMP callbacks). Drop any cached parse so the next enrichment reassembles
	// and re-parses the current stored BUMP.
	s.bumpCache.Remove(blockHash)
	return nil
}

// GetBUMP fetches the manifest, then batch-reads every chunk and reassembles
// the original bumpData. Returns store.ErrNotFound if no manifest exists.
// Any chunk error or assembly mismatch returns a wrapped error and no partial
// data — callers (enrichMerklePath) treat errors as "skip enrichment", which
// is correct: a truncated BUMP would silently produce wrong per-tx proofs.
func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	manifestKey, err := s.key(setBumps, blockHash)
	if err != nil {
		return 0, nil, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), manifestKey)
	if err != nil && !isKeyNotFound(err) {
		return 0, nil, fmt.Errorf("get bump manifest %s: %w", blockHash, err)
	}
	if rec == nil {
		return 0, nil, store.ErrNotFound
	}

	chunkCount, _ := rec.Bins["chunk_count"].(int)
	totalSize, _ := rec.Bins["total_size"].(int)
	formatVersion, _ := rec.Bins["format_version"].(int)
	if chunkCount <= 0 || totalSize < 0 || formatVersion != bumpFormatVersion {
		return 0, nil, fmt.Errorf(
			"get bump %s: invalid manifest (chunk_count=%d total_size=%d format_version=%d)",
			blockHash, chunkCount, totalSize, formatVersion,
		)
	}

	var height uint64
	if v, ok := rec.Bins["block_height"].(int); ok {
		height = uint64(v) //nolint:gosec // round-trip of a value we wrote as int from a uint64 fitting block height
	}

	data, err := s.readBumpChunks(ctx, blockHash, chunkCount, totalSize)
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	return height, data, nil
}

// bumpChunkKey constructs the primary key for a chunk record. Index is
// zero-padded to a fixed width so chunk keys sort lexicographically and don't
// collide with future schema additions on the same set.
func (s *Store) bumpChunkKey(blockHash string, idx int) (*aero.Key, error) {
	return s.key(setBumps, fmt.Sprintf(bumpChunkKeyFormat, blockHash, idx))
}

// writeBumpChunks slices bumpData into chunkCount records and writes them in
// one BatchOperate. Uses RecordExistsAction=REPLACE so any stale state from a
// previously failed write at the same key is dropped.
func (s *Store) writeBumpChunks(ctx context.Context, blockHash string, bumpData []byte, chunkCount int) error {
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.REPLACE

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			off := i * s.bumpChunkSize
			tail := off + s.bumpChunkSize
			if tail > len(bumpData) {
				tail = len(bumpData)
			}
			key, err := s.bumpChunkKey(blockHash, i)
			if err != nil {
				return err
			}
			ops := []*aero.Operation{
				aero.PutOp(aero.NewBin("block_hash", blockHash)),
				aero.PutOp(aero.NewBin("chunk_index", i)),
				aero.PutOp(aero.NewBin("chunk_data", bumpData[off:tail])),
			}
			records = append(records, aero.NewBatchWrite(bwp, key, ops...))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch write bump chunks: %w", err)
		}
		for _, r := range records {
			if br := r.BatchRec(); br != nil && br.Err != nil {
				return fmt.Errorf("write bump chunk: %w", br.Err)
			}
		}
	}
	return nil
}

// readBumpChunks fetches chunkCount chunk records via BatchGet (one round
// trip per batch slice) and assembles them into a single byte slice of length
// totalSize. Validates each chunk's index matches the slot it occupies and
// rejects any missing chunk or length mismatch.
func (s *Store) readBumpChunks(ctx context.Context, blockHash string, chunkCount, totalSize int) ([]byte, error) {
	out := make([]byte, totalSize)
	written := 0

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		keys := make([]*aero.Key, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.bumpChunkKey(blockHash, i)
			if err != nil {
				return nil, err
			}
			keys = append(keys, key)
		}

		recs, err := s.client.BatchGet(s.batchPolicy(ctx), keys, "chunk_index", "chunk_data")
		if err != nil {
			return nil, fmt.Errorf("batch get bump chunks: %w", err)
		}
		if len(recs) != len(keys) {
			return nil, fmt.Errorf("batch get bump chunks: got %d records, want %d", len(recs), len(keys))
		}

		for j, r := range recs {
			idx := start + j
			if r == nil {
				return nil, fmt.Errorf("missing bump chunk %d", idx)
			}
			gotIdx, ok := r.Bins["chunk_index"].(int)
			if !ok || gotIdx != idx {
				return nil, fmt.Errorf("bump chunk %d: chunk_index mismatch (got %v)", idx, r.Bins["chunk_index"])
			}
			data, ok := r.Bins["chunk_data"].([]byte)
			if !ok {
				return nil, fmt.Errorf("bump chunk %d: chunk_data missing or wrong type", idx)
			}
			off := idx * s.bumpChunkSize
			if off+len(data) > totalSize {
				return nil, fmt.Errorf("bump chunk %d: would overflow assembled buffer (off=%d len=%d total=%d)", idx, off, len(data), totalSize)
			}
			copy(out[off:], data)
			written += len(data)
		}
	}

	if written != totalSize {
		return nil, fmt.Errorf("assembled %d bytes, manifest says %d", written, totalSize)
	}
	return out, nil
}

// deleteBumpChunkRange batch-deletes chunk records at indices [fromIdx,
// toExcl). Used by InsertBUMP to clean up orphans when a rewrite shrinks the
// chunk count for a block.
func (s *Store) deleteBumpChunkRange(ctx context.Context, blockHash string, fromIdx, toExcl int) error {
	if toExcl <= fromIdx {
		return nil
	}
	for start := fromIdx; start < toExcl; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > toExcl {
			end = toExcl
		}
		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.bumpChunkKey(blockHash, i)
			if err != nil {
				return err
			}
			records = append(records, aero.NewBatchDelete(nil, key))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch delete bump chunks: %w", err)
		}
	}
	return nil
}

// --- STUMP Operations ---
//
// A STUMP is stored exactly like a BUMP (see above): a manifest record plus N
// chunk records, because a single subtree's STUMP on a busy block routinely
// exceeds Aerospike's per-record ceiling (write-block-size, default 1 MiB, max
// 8 MiB). An unchunked write fails with RECORD_TOO_BIG.
//
// Manifest record — primary key <blockHash>:<subtreeIndex>, bins: block_hash,
// subtree_index, chunk_count, total_size, format_version. It carries no
// stump_data; it points at chunk records. Only the manifest carries the
// block_hash bin, so the arcade_idx_stumps_block_hash secondary index — and
// therefore GetStumpsByBlockHash — sees manifests only, never chunks.
//
// Chunk record — primary key <blockHash>:<subtreeIndex>:c:<index>, bins:
// chunk_index, chunk_data. Deliberately no block_hash bin (see above).
//
// Atomicity matches InsertBUMP: chunks are written first, then the manifest,
// which is the linearization point a concurrent reader keys off.

// stumpManifestKey builds the primary key for a STUMP manifest record.
func (s *Store) stumpManifestKey(blockHash string, subtreeIndex int) (*aero.Key, error) {
	return s.key(setStumps, fmt.Sprintf("%s:%d", blockHash, subtreeIndex))
}

// stumpChunkKey builds the primary key for one chunk of a STUMP. The index is
// zero-padded so chunk keys sort lexicographically and never collide with a
// manifest key (which has no ":c:" segment).
func (s *Store) stumpChunkKey(blockHash string, subtreeIndex, idx int) (*aero.Key, error) {
	return s.key(setStumps, fmt.Sprintf(stumpChunkKeyFormat, blockHash, subtreeIndex, idx))
}

// InsertStump stores a STUMP as a manifest plus chunk records. Chunks are
// written before the manifest so a concurrent reader observing the new
// manifest is guaranteed to find every chunk it references.
func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	if stump.BlockHash == "" {
		return fmt.Errorf("insert stump: empty block hash")
	}

	// Read the previous manifest's chunk count so we can clean up orphan
	// chunks if this rewrite produces fewer of them. Failure here is non-fatal
	// — without it we skip cleanup and orphans only waste disk.
	oldChunkCount := 0
	if oldKey, err := s.stumpManifestKey(stump.BlockHash, stump.SubtreeIndex); err == nil {
		if rec, err := s.client.Get(s.readPolicy(ctx), oldKey, "chunk_count"); err == nil && rec != nil {
			if v, ok := rec.Bins["chunk_count"].(int); ok && v > 0 {
				oldChunkCount = v
			}
		}
	}

	newChunkCount := 1
	if len(stump.StumpData) > s.bumpChunkSize {
		newChunkCount = (len(stump.StumpData) + s.bumpChunkSize - 1) / s.bumpChunkSize
	}

	if err := s.writeStumpChunks(ctx, stump.BlockHash, stump.SubtreeIndex, stump.StumpData, newChunkCount); err != nil {
		return fmt.Errorf("write stump chunks for %s:%d: %w", stump.BlockHash, stump.SubtreeIndex, err)
	}

	manifestKey, err := s.stumpManifestKey(stump.BlockHash, stump.SubtreeIndex)
	if err != nil {
		return err
	}
	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = aero.REPLACE
	bins := aero.BinMap{
		"block_hash":     stump.BlockHash,
		"subtree_index":  stump.SubtreeIndex,
		"chunk_count":    newChunkCount,
		"total_size":     len(stump.StumpData),
		"format_version": stumpFormatVersion,
	}
	if err := s.client.Put(wp, manifestKey, bins); err != nil {
		return fmt.Errorf("write stump manifest for %s:%d: %w", stump.BlockHash, stump.SubtreeIndex, err)
	}

	// Best-effort cleanup of orphan chunks left by a prior larger write for
	// the same (blockHash, subtreeIndex). They are unreferenced now that the
	// manifest caps at newChunkCount, so a failure here only wastes disk.
	if oldChunkCount > newChunkCount {
		if err := s.deleteStumpChunkRange(ctx, stump.BlockHash, stump.SubtreeIndex, newChunkCount, oldChunkCount); err != nil {
			_ = err
		}
	}
	return nil
}

// stumpManifest is the metadata read from a manifest record before its chunks
// are fetched. It is internal scaffolding for GetStumpsByBlockHash and
// DeleteStumpsByBlockHash.
type stumpManifest struct {
	subtreeIndex int
	chunkCount   int
	totalSize    int
}

// queryStumpManifests runs the block_hash secondary-index query and returns
// every manifest for the block. Chunk records carry no block_hash bin, so the
// index never surfaces them here.
func (s *Store) queryStumpManifests(ctx context.Context, blockHash string) ([]stumpManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setStumps)
	_ = stmt.SetFilter(aero.NewEqualFilter("block_hash", blockHash))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query stumps: %w", err)
	}

	var (
		manifests []stumpManifest
		loopErr   error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			m := stumpManifest{}
			if v, ok := rec.Record.Bins["subtree_index"].(int); ok {
				m.subtreeIndex = v
			}
			if v, ok := rec.Record.Bins["chunk_count"].(int); ok {
				m.chunkCount = v
			}
			if v, ok := rec.Record.Bins["total_size"].(int); ok {
				m.totalSize = v
			}
			fv, _ := rec.Record.Bins["format_version"].(int)
			if m.chunkCount <= 0 || m.totalSize < 0 || fv != stumpFormatVersion {
				loopErr = fmt.Errorf(
					"stumps %s: invalid manifest for subtree %d (chunk_count=%d total_size=%d format_version=%d)",
					blockHash, m.subtreeIndex, m.chunkCount, m.totalSize, fv,
				)
				break loop
			}
			manifests = append(manifests, m)
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return manifests, nil
}

// GetStumpsByBlockHash retrieves every STUMP for a block. The secondary index
// returns manifest records only; each manifest's chunks are then batch-read
// and reassembled into the original stump_data.
func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	manifests, err := s.queryStumpManifests(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	stumps := make([]*models.Stump, 0, len(manifests))
	for _, m := range manifests {
		data, err := s.readStumpChunks(ctx, blockHash, m.subtreeIndex, m.chunkCount, m.totalSize)
		if err != nil {
			return nil, fmt.Errorf("get stumps %s: subtree %d: %w", blockHash, m.subtreeIndex, err)
		}
		stumps = append(stumps, &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: m.subtreeIndex,
			StumpData:    data,
		})
	}
	return stumps, nil
}

// writeStumpChunks slices stumpData into chunkCount chunk records under the
// (blockHash, subtreeIndex) namespace and writes them with REPLACE semantics
// so stale state from a previously failed write at the same key is dropped.
func (s *Store) writeStumpChunks(ctx context.Context, blockHash string, subtreeIndex int, stumpData []byte, chunkCount int) error {
	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.REPLACE

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			off := i * s.bumpChunkSize
			tail := off + s.bumpChunkSize
			if tail > len(stumpData) {
				tail = len(stumpData)
			}
			key, err := s.stumpChunkKey(blockHash, subtreeIndex, i)
			if err != nil {
				return err
			}
			// No block_hash bin: chunk records must stay out of the
			// arcade_idx_stumps_block_hash index so manifest queries don't
			// surface them.
			ops := []*aero.Operation{
				aero.PutOp(aero.NewBin("chunk_index", i)),
				aero.PutOp(aero.NewBin("chunk_data", stumpData[off:tail])),
			}
			records = append(records, aero.NewBatchWrite(bwp, key, ops...))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch write stump chunks: %w", err)
		}
		for _, r := range records {
			if br := r.BatchRec(); br != nil && br.Err != nil {
				return fmt.Errorf("write stump chunk: %w", br.Err)
			}
		}
	}
	return nil
}

// readStumpChunks batch-reads chunkCount chunk records and assembles them into
// a single byte slice of length totalSize, rejecting any missing chunk, index
// mismatch, or length overflow.
func (s *Store) readStumpChunks(ctx context.Context, blockHash string, subtreeIndex, chunkCount, totalSize int) ([]byte, error) {
	out := make([]byte, totalSize)
	written := 0

	for start := 0; start < chunkCount; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := start + s.batchSize
		if end > chunkCount {
			end = chunkCount
		}

		keys := make([]*aero.Key, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.stumpChunkKey(blockHash, subtreeIndex, i)
			if err != nil {
				return nil, err
			}
			keys = append(keys, key)
		}

		recs, err := s.client.BatchGet(s.batchPolicy(ctx), keys, "chunk_index", "chunk_data")
		if err != nil {
			return nil, fmt.Errorf("batch get stump chunks: %w", err)
		}
		if len(recs) != len(keys) {
			return nil, fmt.Errorf("batch get stump chunks: got %d records, want %d", len(recs), len(keys))
		}

		for j, r := range recs {
			idx := start + j
			if r == nil {
				return nil, fmt.Errorf("missing stump chunk %d", idx)
			}
			gotIdx, ok := r.Bins["chunk_index"].(int)
			if !ok || gotIdx != idx {
				return nil, fmt.Errorf("stump chunk %d: chunk_index mismatch (got %v)", idx, r.Bins["chunk_index"])
			}
			data, ok := r.Bins["chunk_data"].([]byte)
			if !ok {
				return nil, fmt.Errorf("stump chunk %d: chunk_data missing or wrong type", idx)
			}
			off := idx * s.bumpChunkSize
			if off+len(data) > totalSize {
				return nil, fmt.Errorf("stump chunk %d: would overflow assembled buffer (off=%d len=%d total=%d)", idx, off, len(data), totalSize)
			}
			copy(out[off:], data)
			written += len(data)
		}
	}

	if written != totalSize {
		return nil, fmt.Errorf("assembled %d bytes, manifest says %d", written, totalSize)
	}
	return out, nil
}

// deleteStumpChunkRange batch-deletes chunk records at indices [fromIdx,
// toExcl) for one (blockHash, subtreeIndex) STUMP.
func (s *Store) deleteStumpChunkRange(ctx context.Context, blockHash string, subtreeIndex, fromIdx, toExcl int) error {
	if toExcl <= fromIdx {
		return nil
	}
	for start := fromIdx; start < toExcl; start += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.batchSize
		if end > toExcl {
			end = toExcl
		}
		records := make([]aero.BatchRecordIfc, 0, end-start)
		for i := start; i < end; i++ {
			key, err := s.stumpChunkKey(blockHash, subtreeIndex, i)
			if err != nil {
				return err
			}
			records = append(records, aero.NewBatchDelete(nil, key))
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch delete stump chunks: %w", err)
		}
	}
	return nil
}

// DeleteStumpsByBlockHash removes every STUMP (manifests and chunks) for a
// block. Used during reorg cleanup. For each STUMP the manifest is deleted
// before its chunks: a crash mid-delete then leaves only unreferenced orphan
// chunks (harmless disk waste) rather than a manifest pointing at missing
// chunks (a hard read error).
func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	manifests, err := s.queryStumpManifests(ctx, blockHash)
	if err != nil {
		return err
	}

	for _, m := range manifests {
		manifestKey, err := s.stumpManifestKey(blockHash, m.subtreeIndex)
		if err != nil {
			return err
		}
		if _, err := s.client.Delete(s.writePolicy(ctx), manifestKey); err != nil {
			return fmt.Errorf("delete stump manifest %s:%d: %w", blockHash, m.subtreeIndex, err)
		}
		if err := s.deleteStumpChunkRange(ctx, blockHash, m.subtreeIndex, 0, m.chunkCount); err != nil {
			return fmt.Errorf("delete stump chunks %s:%d: %w", blockHash, m.subtreeIndex, err)
		}
	}
	return nil
}

// --- Submission Operations ---

func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	key, err := s.key(setSubmissions, sub.SubmissionID)
	if err != nil {
		return err
	}
	// Bin names must be <=15 chars (Aerospike limit) — a longer name
	// fails the whole Put with BIN_NAME_TOO_LONG.
	bins := aero.BinMap{
		"submission_id":  sub.SubmissionID,
		"txid":           sub.TxID,
		"callback_url":   sub.CallbackURL,
		"callback_token": sub.CallbackToken,
		"full_updates":   sub.FullStatusUpdates,
		"created_at":     sub.CreatedAt.UnixMilli(),
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	_ = stmt.SetFilter(aero.NewEqualFilter("txid", txid))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, err
	}

	var (
		subs    []*models.Submission
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			subs = append(subs, recordToSubmission(rec.Record))
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return subs, nil
}

// TokenHasSubmissionForTx resolves via the txid secondary index (a txid
// has a handful of submissions) — never the token side, which can hold
// millions.
func (s *Store) TokenHasSubmissionForTx(ctx context.Context, callbackToken, txid string) (bool, error) {
	subs, err := s.GetSubmissionsByTxID(ctx, txid)
	if err != nil {
		return false, err
	}
	for _, sub := range subs {
		if sub.CallbackToken == callbackToken {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) IterateStatusesByToken(ctx context.Context, callbackToken string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error {
	subs, err := s.GetSubmissionsByToken(ctx, callbackToken)
	if err != nil {
		return err
	}
	only := make(map[models.Status]struct{}, len(onlyStatuses))
	for _, st := range onlyStatuses {
		only[st] = struct{}{}
	}
	seen := make(map[string]struct{}, len(subs))
	// Projection only — per-key Get with just the status/timestamp/block
	// bins (never raw_tx or merkle_path) and no enrichMerklePath. Filter
	// BEFORE collecting so the sort buffer holds the matching set only.
	matched := make([]*models.TransactionStatus, 0, 64)
	for _, sub := range subs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, dup := seen[sub.TxID]; dup {
			continue
		}
		seen[sub.TxID] = struct{}{}
		key, kErr := s.key(setTransactions, sub.TxID)
		if kErr != nil {
			continue
		}
		rec, gErr := s.client.Get(s.readPolicy(ctx), key, "status", "timestamp", "block_hash", "block_height")
		if gErr != nil || rec == nil {
			continue
		}
		st := recordToStatus(rec, sub.TxID)
		if st == nil {
			continue
		}
		if !since.IsZero() && !st.Timestamp.After(since) {
			continue
		}
		if len(only) > 0 {
			if _, ok := only[st.Status]; !ok {
				continue
			}
		}
		matched = append(matched, &models.TransactionStatus{
			TxID:        st.TxID,
			Status:      st.Status,
			Timestamp:   st.Timestamp,
			BlockHash:   st.BlockHash,
			BlockHeight: st.BlockHeight,
		})
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Timestamp.Before(matched[j].Timestamp) })
	for _, st := range matched {
		if err := fn(st); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetSubmissionsByToken(ctx context.Context, token string) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	_ = stmt.SetFilter(aero.NewEqualFilter("callback_token", token))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, err
	}

	var (
		subs    []*models.Submission
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			subs = append(subs, recordToSubmission(rec.Record))
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return subs, nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	key, err := s.key(setSubmissions, submissionID)
	if err != nil {
		return err
	}
	// Bin names must be <=15 chars (Aerospike limit).
	bins := aero.BinMap{
		"last_status": string(lastStatus),
		"retry_count": retryCount,
	}
	if nextRetry != nil {
		bins["next_retry_at"] = nextRetry.UnixMilli()
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

// UpdateDeliveryStatusCAS implements store.Store using a generation-match
// CAS write — the same pattern TryAcquireOrRenew uses for the lease record.
// Reading then writing with EXPECT_GEN_EQUAL guarantees we lose to any
// concurrent writer that mutated the row between our Get and Put.
func (s *Store) UpdateDeliveryStatusCAS(ctx context.Context, submissionID string, expected, next models.Status) (bool, error) {
	key, err := s.key(setSubmissions, submissionID)
	if err != nil {
		return false, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil {
		if isKeyNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read submission for cas %s: %w", submissionID, err)
	}
	if rec == nil {
		return false, nil
	}
	if models.Status(getString(rec, "last_status")) != expected {
		return false, nil
	}
	bins := aero.BinMap{
		"last_status":   string(next),
		"retry_count":   0,
		"next_retry_at": int64(0),
	}
	policy := s.writePolicy(ctx)
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.Generation = rec.Generation
	if err := s.client.Put(policy, key, bins); err != nil {
		// Generation mismatch is the canonical "another writer won" outcome
		// and is treated as a normal claim-lost — the caller silently skips
		// its POST. A non-gen error (network blip, namespace unhealthy, etc.)
		// is also behaviorally safe to surface as claim-lost (the next event
		// or webhook-reaper tick retries), but it should NOT be invisible:
		// without a counter, a flat WebhookCASLostTotal in production could
		// hide a backend that's failing every CAS write. Emit a distinct
		// metric so the two are distinguishable on dashboards.
		if !isGenerationErr(err) {
			metrics.WebhookCASErrorTotal.Inc()
		}
		return false, nil
	}
	return true, nil
}

// ListSubmissionsReadyForRetry returns up to limit submissions whose POST
// previously failed (retry_count > 0) and whose next_retry_at has elapsed.
// There is no secondary index on submission retry bins (the in-retry working
// set is small enough that the cost of an index isn't justified), so this
// scans the set and filters in code — the same approach GetReadyRetries uses
// against the transactions set.
func (s *Store) ListSubmissionsReadyForRetry(ctx context.Context, now time.Time, limit int) ([]*models.Submission, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query submissions ready for retry: %w", err)
	}

	nowMs := now.UnixMilli()
	type candidate struct {
		sub    *models.Submission
		nextMs int64
	}
	candidates := make([]candidate, 0, limit)
	var loopErr error
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			retry := getInt(rec.Record, "retry_count")
			nextMs := getInt64(rec.Record, "next_retry_at")
			if retry <= 0 || nextMs <= 0 || nextMs > nowMs {
				continue
			}
			sub := recordToSubmission(rec.Record)
			if sub == nil {
				continue
			}
			candidates = append(candidates, candidate{sub: sub, nextMs: nextMs})
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}

	// Sort by next_retry_at ASC so the oldest backlog drains first, then
	// truncate. Sorting after the scan avoids reading the whole set into a
	// heap; backlog is bounded by failed-callback cardinality.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].nextMs < candidates[j].nextMs
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	out := make([]*models.Submission, len(candidates))
	for i, c := range candidates {
		out[i] = c.sub
	}
	return out, nil
}

// --- Block processing status ---
//
// Bins:
//   block_hash      string  (also primary key)
//   block_height    int
//   header_seen_at  int64 (unix nanos; 0 = unset)
//   processed_at    int64 (unix nanos; 0 = unset)
//   bump_built_at   int64 (unix nanos; 0 = unset)
//   status          string ("active" | "orphaned")
//   orphaned_at     int64 (unix nanos; 0 = unset)
//
// Writers must use Operate with per-bin PutOps so concurrent header /
// processed / bump-built paths only touch their own bins. A full-record
// Put would clobber whichever bins the other writer recently set.

const (
	binBlockHash    = "block_hash"
	binBlockHeight  = "block_height"
	binHeaderSeenAt = "header_seen_at"
	binProcessedAt  = "processed_at"
	binBUMPBuiltAt  = "bump_built_at"
	binStatus       = "status"
	binOrphanedAt   = "orphaned_at"
)

func (s *Store) UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return err
	}
	// On insert we want every bin populated (the row may be brand new). On
	// update we must NOT clobber processed_at or bump_built_at, which is
	// what per-bin Operate gives us — bins not named here are left alone.
	// We do overwrite block_height (chaintracks is authoritative) and reset
	// status/orphaned_at so a returning orphan re-joins active.
	ops := []*aero.Operation{
		aero.PutOp(aero.NewBin(binBlockHash, blockHash)),
		aero.PutOp(aero.NewBin(binBlockHeight, int(blockHeight))), //nolint:gosec // block height fits in int on 64-bit platforms
		aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusActive))),
		aero.PutOp(aero.NewBin(binOrphanedAt, nil)),
		// header_seen_at: only set when the bin is currently absent. Aerospike
		// has no client-side conditional bin write that's race-free, so we do
		// a generation-CAS read+write loop to set it on first observation
		// only.
	}
	if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
		return fmt.Errorf("upsert block header seen %s: %w", blockHash, err)
	}
	// Set header_seen_at only if absent. Generation CAS isn't needed because
	// repeat writes are idempotent (same bin name, same value would just be
	// re-written but not harmfully).
	rec, err := s.client.Get(s.readPolicy(ctx), key, binHeaderSeenAt)
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("read header_seen_at %s: %w", blockHash, err)
	}
	if rec == nil || getInt64(rec, binHeaderSeenAt) == 0 {
		setOp := aero.PutOp(aero.NewBin(binHeaderSeenAt, seenAt.UnixNano()))
		if _, err := s.client.Operate(s.writePolicy(ctx), key, setOp); err != nil {
			return fmt.Errorf("set header_seen_at %s: %w", blockHash, err)
		}
	}
	return nil
}

func (s *Store) MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error {
	return s.markBlockMilestone(ctx, blockHash, blockHeight, processedAt, binProcessedAt)
}

func (s *Store) MarkBlockBUMPBuilt(ctx context.Context, blockHash string, blockHeight uint64, builtAt time.Time) error {
	return s.markBlockMilestone(ctx, blockHash, blockHeight, builtAt, binBUMPBuiltAt)
}

// markBlockMilestone is the shared upsert path for processed_at and
// bump_built_at. When the row exists, only the milestone bin is written
// (block_height and other bins are preserved). When it doesn't, the row
// is created with header_seen_at synthesized to the milestone time so
// observability still has a record. The synthesized header_seen_at will
// be preserved by a later UpsertBlockHeaderSeen.
func (s *Store) markBlockMilestone(ctx context.Context, blockHash string, blockHeight uint64, at time.Time, milestoneBin string) error {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key, binBlockHash)
	if err != nil && !isKeyNotFound(err) {
		return fmt.Errorf("read block_processing %s: %w", blockHash, err)
	}
	if rec == nil {
		// New row: write all bins so block_height and header_seen_at land.
		ops := []*aero.Operation{
			aero.PutOp(aero.NewBin(binBlockHash, blockHash)),
			aero.PutOp(aero.NewBin(binBlockHeight, int(blockHeight))), //nolint:gosec // block height fits in int on 64-bit platforms
			aero.PutOp(aero.NewBin(binHeaderSeenAt, at.UnixNano())),
			aero.PutOp(aero.NewBin(milestoneBin, at.UnixNano())),
			aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusActive))),
		}
		if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
			return fmt.Errorf("create block_processing %s: %w", blockHash, err)
		}
		return nil
	}
	// Existing row: touch only the milestone bin.
	op := aero.PutOp(aero.NewBin(milestoneBin, at.UnixNano()))
	if _, err := s.client.Operate(s.writePolicy(ctx), key, op); err != nil {
		return fmt.Errorf("update %s on block_processing %s: %w", milestoneBin, blockHash, err)
	}
	return nil
}

func (s *Store) MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) error {
	if len(blockHashes) == 0 {
		return nil
	}
	for _, h := range blockHashes {
		key, err := s.key(setBlockProcessing, h)
		if err != nil {
			return err
		}
		// Skip rows that don't exist — chaintracks may emit OrphanedHashes
		// for blocks observed before this service started recording.
		rec, err := s.client.Get(s.readPolicy(ctx), key, binBlockHash)
		if err != nil {
			if isKeyNotFound(err) {
				continue
			}
			return fmt.Errorf("read block_processing %s: %w", h, err)
		}
		if rec == nil {
			continue
		}
		ops := []*aero.Operation{
			aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusOrphaned))),
			aero.PutOp(aero.NewBin(binOrphanedAt, orphanedAt.UnixNano())),
		}
		if _, err := s.client.Operate(s.writePolicy(ctx), key, ops...); err != nil {
			return fmt.Errorf("mark orphaned %s: %w", h, err)
		}
	}
	return nil
}

func (s *Store) MarkBlocksParked(ctx context.Context, blockHashes []string) error {
	if len(blockHashes) == 0 {
		return nil
	}
	for _, h := range blockHashes {
		key, err := s.key(setBlockProcessing, h)
		if err != nil {
			return err
		}
		rec, err := s.client.Get(s.readPolicy(ctx), key, binStatus)
		if err != nil {
			if isKeyNotFound(err) {
				continue
			}
			return fmt.Errorf("read block_processing %s: %w", h, err)
		}
		// Only 'active' rows park: missing rows are skipped, and an orphaned
		// row is off-chain (a more specific terminal state) that must not be
		// relabeled as a missing-BUMP backlog.
		if rec == nil {
			continue
		}
		if status, _ := rec.Bins[binStatus].(string); status != string(models.BlockStatusActive) {
			continue
		}
		if _, err := s.client.Operate(s.writePolicy(ctx), key,
			aero.PutOp(aero.NewBin(binStatus, string(models.BlockStatusParked)))); err != nil {
			return fmt.Errorf("mark parked %s: %w", h, err)
		}
	}
	return nil
}

func (s *Store) GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error) {
	key, err := s.key(setBlockProcessing, blockHash)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil {
		if isKeyNotFound(err) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get block_processing %s: %w", blockHash, err)
	}
	return blockProcessingFromRecord(rec), nil
}

func (s *Store) ListBlockProcessingStatus(ctx context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	if beforeHeight > 0 {
		// Aerospike's NUMERIC range filter is inclusive on the upper bound,
		// so subtract one to express "block_height < beforeHeight".
		_ = stmt.SetFilter(aero.NewRangeFilter(binBlockHeight, 0, int64(beforeHeight-1))) //nolint:gosec // height fits in int64
	}
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query block_processing: %w", err)
	}
	var (
		all     []*models.BlockProcessingStatus
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			all = append(all, blockProcessingFromRecord(rec.Record))
		}
	}
	if loopErr != nil {
		return all, loopErr
	}
	// Aerospike secondary-index queries don't sort. Sort in memory by
	// height descending and truncate. Cardinality is ~144 rows/day, so
	// even unbounded scans here cost milliseconds.
	sortByHeightDesc(all)
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func blockProcessingFromRecord(rec *aero.Record) *models.BlockProcessingStatus {
	if rec == nil {
		return nil
	}
	bp := &models.BlockProcessingStatus{
		BlockHash:   getString(rec, binBlockHash),
		BlockHeight: uint64(getInt(rec, binBlockHeight)), //nolint:gosec // height non-negative
		Status:      models.BlockProcessingStatusValue(getString(rec, binStatus)),
	}
	if v := getInt64(rec, binHeaderSeenAt); v != 0 {
		bp.HeaderSeenAt = time.Unix(0, v).UTC()
	}
	if v := getInt64(rec, binProcessedAt); v != 0 {
		t := time.Unix(0, v).UTC()
		bp.ProcessedAt = &t
	}
	if v := getInt64(rec, binBUMPBuiltAt); v != 0 {
		t := time.Unix(0, v).UTC()
		bp.BUMPBuiltAt = &t
	}
	if v := getInt64(rec, binOrphanedAt); v != 0 {
		t := time.Unix(0, v).UTC()
		bp.OrphanedAt = &t
	}
	return bp
}

// GetActiveTipBlockHeight scans every block_processing row filtered to
// status='active' via the secondary index and returns the highest height.
// Aerospike has no MAX aggregation, so we walk the matching rows. The set
// holds ~one row per BSV block (~144/day) so even unbounded retention is
// cheap.
func (s *Store) GetActiveTipBlockHeight(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	_ = stmt.SetFilter(aero.NewEqualFilter(binStatus, string(models.BlockStatusActive)))
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return 0, fmt.Errorf("query active tip: %w", err)
	}
	var tip uint64
	for {
		select {
		case <-ctx.Done():
			return tip, ctx.Err()
		case rec, ok := <-rs.Results():
			if !ok {
				return tip, nil
			}
			if rec.Err != nil {
				continue
			}
			if h := uint64(getInt(rec.Record, binBlockHeight)); h > tip { //nolint:gosec // height non-negative
				tip = h
			}
		}
	}
}

// ListStaleBlockProcessingStatus narrows by status='active' via the secondary
// index, then in-memory filters on processed_at (absent / zero) +
// header_seen_at + block_height, sorts by header_seen_at ascending, and
// truncates to limit. Cardinality bound is the count of active rows which
// matches the recency window in the common case (the watchdog never asks
// for ancient blocks).
func (s *Store) ListStaleBlockProcessingStatus(ctx context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	threshold := olderThan.UnixNano()
	stmt := aero.NewStatement(s.namespace, setBlockProcessing)
	_ = stmt.SetFilter(aero.NewEqualFilter(binStatus, string(models.BlockStatusActive)))
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query stale block_processing: %w", err)
	}
	var (
		all     []*models.BlockProcessingStatus
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				continue
			}
			// Filter in-memory. processed_at absent or zero counts as "not yet";
			// header_seen_at < threshold; block_height >= minHeight.
			if v := getInt64(rec.Record, binProcessedAt); v != 0 {
				continue
			}
			seen := getInt64(rec.Record, binHeaderSeenAt)
			if seen == 0 || seen >= threshold {
				continue
			}
			h := uint64(getInt(rec.Record, binBlockHeight)) //nolint:gosec // height non-negative
			if h < minHeight {
				continue
			}
			all = append(all, blockProcessingFromRecord(rec.Record))
		}
	}
	if loopErr != nil {
		return all, loopErr
	}
	sortByHeaderSeenAsc(all)
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func sortByHeaderSeenAsc(rows []*models.BlockProcessingStatus) {
	for i := 1; i < len(rows); i++ {
		j := i
		for j > 0 && rows[j-1].HeaderSeenAt.After(rows[j].HeaderSeenAt) {
			rows[j-1], rows[j] = rows[j], rows[j-1]
			j--
		}
	}
}

func sortByHeightDesc(rows []*models.BlockProcessingStatus) {
	// Insertion sort is fine — the result set is at most a couple hundred rows.
	for i := 1; i < len(rows); i++ {
		j := i
		for j > 0 && rows[j-1].BlockHeight < rows[j].BlockHeight {
			rows[j-1], rows[j] = rows[j], rows[j-1]
			j--
		}
	}
}

// --- Datahub endpoint registry ---

// UpsertDatahubEndpoint writes the endpoint with the URL as primary key.
// Existing rows are overwritten so LastSeen advances and a flap from
// "discovered" to "configured" (or vice versa) is reflected.
func (s *Store) UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error {
	if ep.URL == "" {
		return fmt.Errorf("upsert datahub endpoint: empty url")
	}
	key, err := s.key(setDatahubEndpoints, ep.URL)
	if err != nil {
		return err
	}
	bins := aero.BinMap{
		"url":       ep.URL,
		"network":   ep.Network,
		"source":    ep.Source,
		"last_seen": ep.LastSeen.UnixMilli(),
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

// ListDatahubEndpoints scans every row in the registry, filtering to entries
// scoped to the given network. Cardinality is expected in the dozens to low
// hundreds — no pagination needed. Records written before the network bin
// existed return an empty Network and are filtered out.
func (s *Store) ListDatahubEndpoints(ctx context.Context, network string) ([]store.DatahubEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setDatahubEndpoints)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer func() { _ = rs.Close() }()
	}
	if err != nil {
		return nil, fmt.Errorf("query datahub endpoints: %w", err)
	}

	var (
		out     []store.DatahubEndpoint
		loopErr error
	)
loop:
	for {
		select {
		case <-ctx.Done():
			loopErr = ctx.Err()
			break loop
		case rec, ok := <-rs.Results():
			if !ok {
				break loop
			}
			if rec.Err != nil {
				loopErr = rec.Err
				break loop
			}
			ep := store.DatahubEndpoint{
				URL:     getString(rec.Record, "url"),
				Network: getString(rec.Record, "network"),
				Source:  getString(rec.Record, "source"),
			}
			if ms := getInt64(rec.Record, "last_seen"); ms > 0 {
				ep.LastSeen = time.UnixMilli(ms)
			}
			if ep.URL != "" && ep.Network == network {
				out = append(out, ep)
			}
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return out, nil
}

// --- Helpers ---

func recordToStatus(rec *aero.Record, txid string) *models.TransactionStatus {
	status := &models.TransactionStatus{TxID: txid}
	if v, ok := rec.Bins["status"]; ok {
		if s, ok := v.(string); ok {
			status.Status = models.Status(s)
		}
	}
	if v, ok := rec.Bins["status_code"]; ok {
		if n, ok := v.(int); ok {
			status.StatusCode = n
		}
	}
	if v, ok := rec.Bins["block_hash"]; ok {
		if s, ok := v.(string); ok {
			status.BlockHash = s
		}
	}
	if v, ok := rec.Bins["block_height"]; ok {
		if n, ok := v.(int); ok {
			status.BlockHeight = uint64(n) //nolint:gosec // round-trip of a value we wrote as int from a uint64 fitting block height
		}
	}
	if v, ok := rec.Bins["extra_info"]; ok {
		if s, ok := v.(string); ok {
			status.ExtraInfo = s
		}
	}
	if v, ok := rec.Bins["merkle_path"]; ok {
		if b, ok := v.([]byte); ok {
			status.MerklePath = b
		}
	}
	if v, ok := rec.Bins["competing_txs"]; ok {
		switch ct := v.(type) {
		case []byte:
			_ = json.Unmarshal(ct, &status.CompetingTxs)
		case string:
			_ = json.Unmarshal([]byte(ct), &status.CompetingTxs)
		}
	}
	if v, ok := rec.Bins["timestamp"]; ok {
		if ms, ok := v.(int); ok {
			status.Timestamp = time.UnixMilli(int64(ms))
		}
	}
	if v, ok := rec.Bins["created_at"]; ok {
		if ms, ok := v.(int); ok {
			status.CreatedAt = time.UnixMilli(int64(ms))
		}
	}
	if v, ok := rec.Bins["merkle_reg_at"]; ok {
		if ms, ok := v.(int); ok {
			status.MerkleRegisteredAt = time.UnixMilli(int64(ms))
		}
	}
	return status
}

// EnrichMerklePath implements store.Store: it populates status.MerklePath in
// place for a mined/immutable status carrying a BlockHash, extracting the tx's
// minimal path from the block's compound BUMP. Best-effort — see the interface
// doc. Delegates to the same enrichMerklePath used by GetStatus.
func (s *Store) EnrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	s.enrichMerklePath(ctx, status)
}

// enrichMerklePath fetches the compound BUMP for a mined/immutable transaction
// and extracts the per-tx minimal merkle path if not already present, reusing
// the block's cached, indexed compound.
func (s *Store) enrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	s.bumpCache.Enrich(status, func() ([]byte, error) {
		_, bumpData, err := s.GetBUMP(ctx, status.BlockHash)
		return bumpData, err
	})
}

func recordToSubmission(rec *aero.Record) *models.Submission {
	sub := &models.Submission{}
	if v, ok := rec.Bins["submission_id"]; ok {
		if s, ok := v.(string); ok {
			sub.SubmissionID = s
		}
	}
	if v, ok := rec.Bins["txid"]; ok {
		if s, ok := v.(string); ok {
			sub.TxID = s
		}
	}
	if v, ok := rec.Bins["callback_url"]; ok {
		if s, ok := v.(string); ok {
			sub.CallbackURL = s
		}
	}
	if v, ok := rec.Bins["callback_token"]; ok {
		if s, ok := v.(string); ok {
			sub.CallbackToken = s
		}
	}
	if v, ok := rec.Bins["full_updates"]; ok {
		if b, ok := v.(bool); ok {
			sub.FullStatusUpdates = b
		}
	}
	if v, ok := rec.Bins["created_at"]; ok {
		if ms, ok := v.(int); ok {
			sub.CreatedAt = time.UnixMilli(int64(ms))
		}
	}
	if v, ok := rec.Bins["retry_count"]; ok {
		if n, ok := v.(int); ok {
			sub.RetryCount = n
		}
	}
	// last_status is written by UpdateDeliveryStatus / UpdateDeliveryStatusCAS.
	// Reading it back is essential for the webhook dedup path — without it
	// the in-memory `sub.LastDeliveredStatus` would always be the zero value
	// and the CAS expected-state predicate would never match after the first
	// delivery.
	if v, ok := rec.Bins["last_status"]; ok {
		if s, ok := v.(string); ok {
			sub.LastDeliveredStatus = models.Status(s)
		}
	}
	if v, ok := rec.Bins["next_retry_at"]; ok {
		if ms, ok := v.(int); ok {
			t := time.UnixMilli(int64(ms))
			sub.NextRetryAt = &t
		}
	}
	return sub
}

// --- Lease Operations ---

// TryAcquireOrRenew implements store.Leaser. Uses Aerospike generation-match
// CAS to serialize acquire / renew across concurrent writers, with the record's
// native TTL as the authoritative expiration (no client clock dependency). The
// expires_at bin is a belt-and-braces hint for the narrow window between TTL
// lapse and next client scan.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	key, err := s.key(setLeases, name)
	if err != nil {
		return time.Time{}, err
	}
	now := time.Now()
	expiresAt := now.Add(ttl)

	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil && !isKeyNotFound(err) {
		return time.Time{}, fmt.Errorf("read lease %s: %w", name, err)
	}

	bins := aero.BinMap{
		"holder":     holder,
		"expires_at": expiresAt.UnixMilli(),
	}
	ttlSecs := uint32(ttl.Seconds())

	if rec == nil {
		// No lease record yet — try to create it. CREATE_ONLY fails if a
		// concurrent writer creates the row between our Get and Put; treat
		// that as a benign contention signal.
		policy := s.writePolicy(ctx)
		policy.Expiration = ttlSecs
		policy.RecordExistsAction = aero.CREATE_ONLY
		if err := s.client.Put(policy, key, bins); err != nil {
			return time.Time{}, nil
		}
		return expiresAt, nil
	}

	currentHolder := getString(rec, "holder")
	currentExpires := getInt64(rec, "expires_at")
	canTake := currentHolder == holder || currentExpires <= now.UnixMilli()
	if !canTake {
		return time.Time{}, nil
	}

	// CAS-write: succeeds only if the record hasn't changed since our read.
	// A gen mismatch means another pod won the race; not an error.
	policy := s.writePolicy(ctx)
	policy.Expiration = ttlSecs
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.Generation = rec.Generation
	if err := s.client.Put(policy, key, bins); err != nil {
		return time.Time{}, nil
	}
	return expiresAt, nil
}

// Release deletes a lease record if it's still held by this caller. Gen-match
// prevents us from stepping on a successor who has already acquired it after
// our TTL lapsed. Swallows benign races (not held / raced to delete) as nil.
func (s *Store) Release(ctx context.Context, name, holder string) error {
	key, err := s.key(setLeases, name)
	if err != nil {
		return err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil {
		if isKeyNotFound(err) {
			return nil
		}
		return fmt.Errorf("read lease for release %s: %w", name, err)
	}
	if getString(rec, "holder") != holder {
		return nil
	}
	policy := s.writePolicy(ctx)
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.Generation = rec.Generation
	if _, err := s.client.Delete(policy, key); err != nil {
		// Gen mismatch = successor already took over. Not an error.
		return nil
	}
	return nil
}

func getString(rec *aero.Record, bin string) string {
	if v, ok := rec.Bins[bin]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getInt(rec *aero.Record, bin string) int {
	if v, ok := rec.Bins[bin]; ok {
		if n, ok := v.(int); ok {
			return n
		}
	}
	return 0
}

// getInt64 reads a 64-bit integer bin. Aerospike returns integer bins as the
// language's native int, so we widen safely.
func getInt64(rec *aero.Record, bin string) int64 {
	if v, ok := rec.Bins[bin]; ok {
		switch n := v.(type) {
		case int:
			return int64(n)
		case int64:
			return n
		}
	}
	return 0
}
