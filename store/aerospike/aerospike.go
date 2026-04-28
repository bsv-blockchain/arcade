package aerospike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

func isKeyNotFound(err error) bool {
	return errors.Is(err, aero.ErrKeyNotFound)
}

const (
	setTransactions    = "transactions"
	setBumps           = "bumps"
	setStumps          = "stumps"
	setSubmissions     = "submissions"
	setProcessedBlocks = "processed_blocks"
	setLeases          = "leases"
	setDatahubEndpoints = "datahub_endpoints"
)

// Ensure Store implements the store interfaces.
var _ store.Store = (*Store)(nil)
var _ store.Leaser = (*Store)(nil)

// Store is the Aerospike-backed implementation of store.Store and store.Leaser.
type Store struct {
	client         *aero.Client
	namespace      string
	batchSize      int
	queryTimeout   time.Duration
	opTimeout      time.Duration
	socketTimeout  time.Duration
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

	client, err := aero.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to aerospike: %w", err)
	}

	s := &Store{
		client:        client,
		namespace:     cfg.Namespace,
		batchSize:     cfg.BatchSize,
		queryTimeout:  time.Duration(cfg.QueryTimeoutMs) * time.Millisecond,
		opTimeout:     time.Duration(cfg.OpTimeoutMs) * time.Millisecond,
		socketTimeout: time.Duration(cfg.SocketTimeoutMs) * time.Millisecond,
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
		{setStumps, "block_hash", "idx_stumps_block_hash", aero.STRING},
		{setTransactions, "block_hash", "idx_tx_block_hash", aero.STRING},
		{setSubmissions, "txid", "idx_sub_txid", aero.STRING},
		{setSubmissions, "callback_token", "idx_sub_callback_token", aero.STRING},
		{setProcessedBlocks, "block_height", "idx_pb_block_height", aero.NUMERIC},
		{setTransactions, "status", "idx_tx_status", aero.STRING},
		{setDatahubEndpoints, "last_seen", "idx_dh_last_seen", aero.NUMERIC},
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

func (s *Store) key(set string, pk string) (*aero.Key, error) {
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
// kept low because batch failures amplify connection pressure.
func (s *Store) batchPolicy(ctx context.Context) *aero.BatchPolicy {
	p := aero.NewBatchPolicy()
	p.TotalTimeout = remaining(ctx, s.opTimeout)
	p.SocketTimeout = s.socketTimeout
	p.MaxRetries = 1
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

func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	key, err := s.key(setTransactions, status.TxID)
	if err != nil {
		return err
	}

	bins := aero.BinMap{
		"status":    string(status.Status),
		"timestamp": status.Timestamp.UnixMilli(),
	}
	if status.BlockHash != "" {
		bins["block_hash"] = status.BlockHash
	}
	if status.BlockHeight > 0 {
		bins["block_height"] = int(status.BlockHeight)
	}
	if status.ExtraInfo != "" {
		bins["extra_info"] = status.ExtraInfo
	}
	if len(status.MerklePath) > 0 {
		bins["merkle_path"] = []byte(status.MerklePath)
	}

	return s.client.Put(s.writePolicy(ctx), key, bins)
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
		defer rs.Close()
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

func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setTransactions)
	stmt.SetFilter(aero.NewEqualFilter("block_hash", blockHash))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
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

// GetReadyRetries uses the existing idx_tx_status index to find PENDING_RETRY
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
	stmt.SetFilter(aero.NewEqualFilter("status", string(models.StatusPendingRetry)))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
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

func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, txids []string) ([]*models.TransactionStatus, error) {
	now := time.Now()
	var statuses []*models.TransactionStatus

	bwp := aero.NewBatchWritePolicy()
	bwp.RecordExistsAction = aero.UPDATE_ONLY

	for i := 0; i < len(txids); i += s.batchSize {
		if err := ctx.Err(); err != nil {
			return statuses, err
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
			ops := []*aero.Operation{
				aero.PutOp(aero.NewBin("status", string(models.StatusMined))),
				aero.PutOp(aero.NewBin("block_hash", blockHash)),
				aero.PutOp(aero.NewBin("timestamp", now.UnixMilli())),
			}
			records[j] = aero.NewBatchWrite(bwp, key, ops...)
		}

		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return statuses, fmt.Errorf("batch set mined: %w", err)
		}

		for j, txid := range batch {
			if records[j] != nil && records[j].BatchRec().Err == nil {
				statuses = append(statuses, &models.TransactionStatus{
					TxID:      txid,
					Status:    models.StatusMined,
					BlockHash: blockHash,
					Timestamp: now,
				})
			}
		}
	}

	return statuses, nil
}

// --- BUMP Operations ---

func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	key, err := s.key(setBumps, blockHash)
	if err != nil {
		return err
	}
	bins := aero.BinMap{
		"block_hash":   blockHash,
		"block_height": int(blockHeight),
		"bump_data":    bumpData,
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	key, err := s.key(setBumps, blockHash)
	if err != nil {
		return 0, nil, err
	}
	rec, err := s.client.Get(s.readPolicy(ctx), key)
	if err != nil && !isKeyNotFound(err) {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	if rec == nil {
		return 0, nil, store.ErrNotFound
	}

	var height uint64
	if v, ok := rec.Bins["block_height"]; ok {
		if n, ok := v.(int); ok {
			height = uint64(n)
		}
	}
	var data []byte
	if v, ok := rec.Bins["bump_data"]; ok {
		if b, ok := v.([]byte); ok {
			data = b
		}
	}
	return height, data, nil
}

// --- STUMP Operations (keyed by blockHash:subtreeIndex) ---

func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	pk := fmt.Sprintf("%s:%d", stump.BlockHash, stump.SubtreeIndex)
	key, err := s.key(setStumps, pk)
	if err != nil {
		return err
	}
	bins := aero.BinMap{
		"block_hash":    stump.BlockHash,
		"subtree_index": stump.SubtreeIndex,
		"stump_data":    stump.StumpData,
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setStumps)
	stmt.SetFilter(aero.NewEqualFilter("block_hash", blockHash))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("query stumps: %w", err)
	}

	var (
		stumps  []*models.Stump
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
			stump := &models.Stump{
				BlockHash: getString(rec.Record, "block_hash"),
			}
			if v, ok := rec.Record.Bins["subtree_index"]; ok {
				if n, ok := v.(int); ok {
					stump.SubtreeIndex = n
				}
			}
			if v, ok := rec.Record.Bins["stump_data"]; ok {
				if b, ok := v.([]byte); ok {
					stump.StumpData = b
				}
			}
			stumps = append(stumps, stump)
		}
	}
	if loopErr != nil {
		return nil, loopErr
	}
	return stumps, nil
}

func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	stumps, err := s.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		return err
	}

	for i := 0; i < len(stumps); i += s.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := i + s.batchSize
		if end > len(stumps) {
			end = len(stumps)
		}
		batch := stumps[i:end]

		keys := make([]*aero.Key, len(batch))
		for j, st := range batch {
			pk := fmt.Sprintf("%s:%d", st.BlockHash, st.SubtreeIndex)
			keys[j], _ = s.key(setStumps, pk)
		}

		records := make([]aero.BatchRecordIfc, len(keys))
		for j, key := range keys {
			records[j] = aero.NewBatchDelete(nil, key)
		}
		if err := s.client.BatchOperate(s.batchPolicy(ctx), records); err != nil {
			return fmt.Errorf("batch delete stumps: %w", err)
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
	bins := aero.BinMap{
		"submission_id":       sub.SubmissionID,
		"txid":                sub.TxID,
		"callback_url":        sub.CallbackURL,
		"callback_token":      sub.CallbackToken,
		"full_status_updates": sub.FullStatusUpdates,
		"created_at":          sub.CreatedAt.UnixMilli(),
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	stmt.SetFilter(aero.NewEqualFilter("txid", txid))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
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

func (s *Store) GetSubmissionsByToken(ctx context.Context, token string) ([]*models.Submission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setSubmissions)
	stmt.SetFilter(aero.NewEqualFilter("callback_token", token))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
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
	bins := aero.BinMap{
		"last_delivered_status": string(lastStatus),
		"retry_count":           retryCount,
	}
	if nextRetry != nil {
		bins["next_retry_at"] = nextRetry.UnixMilli()
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

// --- Block Tracking Operations ---

func (s *Store) HasAnyProcessedBlocks(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	stmt := aero.NewStatement(s.namespace, setProcessedBlocks)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
	}
	if err != nil {
		return false, err
	}

	var (
		found   bool
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
			if rec.Err == nil {
				found = true
				break loop
			}
		}
	}
	if loopErr != nil {
		return false, loopErr
	}
	return found, nil
}

func (s *Store) GetOnChainBlockAtHeight(ctx context.Context, height uint64) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	stmt := aero.NewStatement(s.namespace, setProcessedBlocks)
	stmt.SetFilter(aero.NewEqualFilter("block_height", int(height)))

	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
	}
	if err != nil {
		return "", false, err
	}

	var (
		blockHash string
		onChain   bool
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
				continue
			}
			if v, ok := rec.Record.Bins["on_chain"]; ok {
				if n, ok := v.(int); ok && n == 1 {
					blockHash = getString(rec.Record, "block_hash")
					onChain = true
					break loop
				}
			}
		}
	}
	if loopErr != nil {
		return "", false, loopErr
	}
	return blockHash, onChain, nil
}

func (s *Store) MarkBlockOffChain(ctx context.Context, blockHash string) error {
	key, err := s.key(setProcessedBlocks, blockHash)
	if err != nil {
		return err
	}
	return s.client.Put(s.writePolicy(ctx), key, aero.BinMap{"on_chain": 0})
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
		"source":    ep.Source,
		"last_seen": ep.LastSeen.UnixMilli(),
	}
	return s.client.Put(s.writePolicy(ctx), key, bins)
}

// ListDatahubEndpoints scans every row in the registry. Cardinality is
// expected in the dozens to low hundreds — no pagination needed.
func (s *Store) ListDatahubEndpoints(ctx context.Context) ([]store.DatahubEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt := aero.NewStatement(s.namespace, setDatahubEndpoints)
	rs, err := s.client.Query(s.queryPolicy(ctx), stmt)
	if rs != nil {
		defer rs.Close()
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
				URL:    getString(rec.Record, "url"),
				Source: getString(rec.Record, "source"),
			}
			if ms := getInt64(rec.Record, "last_seen"); ms > 0 {
				ep.LastSeen = time.UnixMilli(ms)
			}
			if ep.URL != "" {
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
	if v, ok := rec.Bins["block_hash"]; ok {
		if s, ok := v.(string); ok {
			status.BlockHash = s
		}
	}
	if v, ok := rec.Bins["block_height"]; ok {
		if n, ok := v.(int); ok {
			status.BlockHeight = uint64(n)
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
			json.Unmarshal(ct, &status.CompetingTxs)
		case string:
			json.Unmarshal([]byte(ct), &status.CompetingTxs)
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
	return status
}

// enrichMerklePath fetches the compound BUMP for a mined/immutable transaction
// and extracts the per-tx minimal merkle path if not already present.
func (s *Store) enrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	if status == nil || len(status.MerklePath) > 0 || status.BlockHash == "" {
		return
	}
	if status.Status != models.StatusMined && status.Status != models.StatusImmutable {
		return
	}
	_, bumpData, err := s.GetBUMP(ctx, status.BlockHash)
	if err != nil || len(bumpData) == 0 {
		return
	}
	status.MerklePath = extractMinimalPathForTx(bumpData, status.TxID)
}

// extractMinimalPathForTx extracts a per-tx minimal merkle path from a compound BUMP.
func extractMinimalPathForTx(bumpData []byte, txid string) []byte {
	compound, err := transaction.NewMerklePathFromBinary(bumpData)
	if err != nil {
		return nil
	}
	txHash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return nil
	}

	var txOffset uint64
	found := false
	if len(compound.Path) > 0 {
		for _, leaf := range compound.Path[0] {
			if leaf.Hash != nil && *leaf.Hash == *txHash {
				txOffset = leaf.Offset
				found = true
				break
			}
		}
	}
	if !found {
		return nil
	}

	mp := &transaction.MerklePath{
		BlockHeight: compound.BlockHeight,
		Path:        make([][]*transaction.PathElement, len(compound.Path)),
	}
	offset := txOffset
	for level := 0; level < len(compound.Path); level++ {
		if level == 0 {
			for _, leaf := range compound.Path[level] {
				if leaf.Offset == offset {
					mp.Path[level] = append(mp.Path[level], leaf)
					break
				}
			}
		}
		sibOffset := offset ^ 1
		for _, leaf := range compound.Path[level] {
			if leaf.Offset == sibOffset {
				mp.Path[level] = append(mp.Path[level], leaf)
				break
			}
		}
		offset = offset >> 1
	}
	return mp.Bytes()
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
	if v, ok := rec.Bins["full_status_updates"]; ok {
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
	return sub
}

// --- Lease Operations ---

// TryAcquireOrRenew implements store.Leaser. Uses Aerospike generation-match
// CAS to serialise acquire / renew across concurrent writers, with the record's
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
