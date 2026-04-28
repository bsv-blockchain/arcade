// Package postgres implements store.Store and store.Leaser against a
// Postgres database via pgx/v5. It supports two modes:
//
//  1. External Postgres: provide a DSN in config.
//  2. Embedded: fergusstrange/embedded-postgres spins up a local Postgres
//     process, data-dir on disk. Intended for single-binary standalone
//     mode alongside the in-memory Kafka broker.
//
// For arcade's sustained hot-path throughput Pebble is the recommended
// standalone backend; embedded-postgres is here primarily for users who
// want a real SQL surface for testing migrations, joins, and queries.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

//go:embed schema.sql
var schemaSQL string

var _ store.Store = (*Store)(nil)
var _ store.Leaser = (*Store)(nil)

// Store is the Postgres-backed implementation of the store interfaces.
type Store struct {
	pool    *pgxpool.Pool
	stopEmb func() error
}

// New connects to Postgres (optionally starting the embedded-postgres process
// first) and applies the schema. The caller owns the returned Store and must
// call Close during shutdown.
func New(ctx context.Context, cfg config.Postgres) (*Store, error) {
	dsn := cfg.DSN
	var stopEmbedded func() error
	if cfg.Embedded {
		d, stop, err := startEmbedded(cfg)
		if err != nil {
			return nil, err
		}
		dsn = d
		stopEmbedded = stop
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		if stopEmbedded != nil {
			_ = stopEmbedded()
		}
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		if stopEmbedded != nil {
			_ = stopEmbedded()
		}
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	return &Store{pool: pool, stopEmb: stopEmbedded}, nil
}

// EnsureIndexes applies the schema. Safe to call repeatedly — every CREATE
// statement in schema.sql is IF NOT EXISTS.
func (s *Store) EnsureIndexes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// Close drains the pool and stops the embedded Postgres process (if any).
// Errors from stopping the embedded process are reported — leaving the
// process alive blocks the next start-up because it holds the data-dir lock.
func (s *Store) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.stopEmb != nil {
		return s.stopEmb()
	}
	return nil
}

// --- Transaction Status ---

// GetOrInsertStatus uses INSERT ... ON CONFLICT DO NOTHING for a single
// round-trip CAS. If no row is inserted we fall back to a SELECT to read
// whatever the winning writer persisted.
func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	now := time.Now()
	if status.Timestamp.IsZero() {
		status.Timestamp = now
	}
	if status.Status == "" {
		status.Status = models.StatusReceived
	}
	status.CreatedAt = now

	// competing_txs is a JSONB column; pass a string so pgx encodes it as JSON
	// rather than BYTEA. nil is allowed (column is nullable).
	var competing any
	if len(status.CompetingTxs) > 0 {
		b, err := json.Marshal(status.CompetingTxs)
		if err != nil {
			return nil, false, err
		}
		competing = string(b)
	}

	const q = `
INSERT INTO transactions (txid, status, status_code, block_hash, block_height,
    merkle_path, extra_info, competing_txs, raw_tx, retry_count,
    next_retry_at, timestamp_at, created_at)
VALUES ($1,$2,NULLIF($3,0),NULLIF($4,''),NULLIF($5,0),$6,NULLIF($7,''),$8,$9,$10,$11,$12,$13)
ON CONFLICT (txid) DO NOTHING`

	var nextRetry any
	if !status.NextRetryAt.IsZero() {
		nextRetry = status.NextRetryAt
	}

	tag, err := s.pool.Exec(ctx, q,
		status.TxID, string(status.Status), status.StatusCode,
		status.BlockHash, int64(status.BlockHeight),
		[]byte(status.MerklePath), status.ExtraInfo, competing,
		[]byte(status.RawTx), status.RetryCount,
		nextRetry, status.Timestamp, status.CreatedAt,
	)
	if err != nil {
		return nil, false, fmt.Errorf("insert tx %s: %w", status.TxID, err)
	}
	if tag.RowsAffected() > 0 {
		return status, true, nil
	}

	existing, err := s.GetStatus(ctx, status.TxID)
	if err != nil {
		return nil, false, err
	}
	return existing, false, nil
}

// columnsPerInsertRow is how many placeholders one row in the multi-row
// INSERT VALUES list consumes. Matches the column list in the static SQL
// fragment built by BatchGetOrInsertStatus.
const columnsPerInsertRow = 13

// BatchGetOrInsertStatus is the multi-row form of GetOrInsertStatus. It uses
// the "xmax = 0" trick: ON CONFLICT DO UPDATE SET txid = excluded.txid is a
// no-op write that nonetheless triggers RETURNING for the existing row, and
// xmax is 0 for newly-inserted rows but non-zero for rows we re-touched. So
// one round-trip gives us per-row inserted/existing flag plus the existing
// row data when the insert lost the race.
//
// The single-row GetOrInsertStatus uses ON CONFLICT DO NOTHING + a fallback
// SELECT, which is two round-trips when the row exists. For batches of 100+
// txs the win from collapsing into one round-trip is large.
func (s *Store) BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	if len(statuses) == 0 {
		return nil, nil
	}

	// Normalise inputs the same way GetOrInsertStatus does — empty Status
	// becomes RECEIVED, missing timestamps default to now, CreatedAt is set.
	now := time.Now()
	args := make([]any, 0, len(statuses)*columnsPerInsertRow)
	for _, st := range statuses {
		if st.Timestamp.IsZero() {
			st.Timestamp = now
		}
		if st.Status == "" {
			st.Status = models.StatusReceived
		}
		st.CreatedAt = now

		var competing any
		if len(st.CompetingTxs) > 0 {
			b, err := json.Marshal(st.CompetingTxs)
			if err != nil {
				return nil, fmt.Errorf("marshal competing_txs for %s: %w", st.TxID, err)
			}
			competing = string(b)
		}
		var nextRetry any
		if !st.NextRetryAt.IsZero() {
			nextRetry = st.NextRetryAt
		}
		args = append(args,
			st.TxID, string(st.Status), st.StatusCode,
			st.BlockHash, int64(st.BlockHeight),
			[]byte(st.MerklePath), st.ExtraInfo, competing,
			[]byte(st.RawTx), st.RetryCount,
			nextRetry, st.Timestamp, st.CreatedAt,
		)
	}

	// Build the VALUES clause: one (...) tuple per row, NULLIF guards mirror
	// the single-row insert exactly.
	var values strings.Builder
	for i := 0; i < len(statuses); i++ {
		if i > 0 {
			values.WriteString(", ")
		}
		base := i * columnsPerInsertRow
		fmt.Fprintf(&values,
			"($%d,$%d,NULLIF($%d,0),NULLIF($%d,''),NULLIF($%d,0),$%d,NULLIF($%d,''),$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7,
			base+8, base+9, base+10, base+11, base+12, base+13,
		)
	}

	q := `
INSERT INTO transactions (txid, status, status_code, block_hash, block_height,
    merkle_path, extra_info, competing_txs, raw_tx, retry_count,
    next_retry_at, timestamp_at, created_at)
VALUES ` + values.String() + `
ON CONFLICT (txid) DO UPDATE SET txid = transactions.txid
RETURNING txid, status, status_code, block_hash, block_height, merkle_path,
          extra_info, competing_txs, raw_tx, retry_count, next_retry_at,
          timestamp_at, created_at, (xmax = 0) AS inserted`

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("batch insert: %w", err)
	}
	defer rows.Close()

	// Postgres doesn't guarantee RETURNING order matches input order. Build a
	// txid → result map, then assemble the output in input order.
	byTxID := make(map[string]store.BatchInsertResult, len(statuses))
	for rows.Next() {
		st, inserted, err := scanStatusWithInserted(rows)
		if err != nil {
			return nil, fmt.Errorf("scan batch result: %w", err)
		}
		if inserted {
			byTxID[st.TxID] = store.BatchInsertResult{Inserted: true}
		} else {
			byTxID[st.TxID] = store.BatchInsertResult{Existing: st, Inserted: false}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	out := make([]store.BatchInsertResult, len(statuses))
	for i, st := range statuses {
		out[i] = byTxID[st.TxID]
	}
	return out, nil
}

// BatchUpdateStatus is the multi-row form of UpdateStatus. Uses
// UPDATE … FROM (VALUES …) so all rows update in one round-trip. Same
// partial-update semantics as UpdateStatus: empty fields don't overwrite
// existing values (handled by NULLIF + COALESCE in the SET clause).
func (s *Store) BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error {
	if len(statuses) == 0 {
		return nil
	}

	const colsPerRow = 6 // txid, status, block_hash, block_height, extra_info, timestamp_at + merkle_path

	args := make([]any, 0, len(statuses)*(colsPerRow+1))
	now := time.Now()
	for _, st := range statuses {
		ts := st.Timestamp
		if ts.IsZero() {
			ts = now
		}
		var mp any
		if len(st.MerklePath) > 0 {
			mp = []byte(st.MerklePath)
		}
		args = append(args,
			st.TxID,
			string(st.Status),
			st.BlockHash,
			int64(st.BlockHeight),
			st.ExtraInfo,
			mp,
			ts,
		)
	}

	var values strings.Builder
	for i := 0; i < len(statuses); i++ {
		if i > 0 {
			values.WriteString(", ")
		}
		base := i * (colsPerRow + 1)
		// Cast text/bytea/bigint/timestamptz so Postgres can pick the right
		// types for the VALUES alias columns.
		fmt.Fprintf(&values,
			"($%d::text,$%d::text,$%d::text,$%d::bigint,$%d::text,$%d::bytea,$%d::timestamptz)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7,
		)
	}

	q := `
UPDATE transactions t SET
    status       = v.status,
    block_hash   = COALESCE(NULLIF(v.block_hash, ''),     t.block_hash),
    block_height = COALESCE(NULLIF(v.block_height, 0),    t.block_height),
    extra_info   = COALESCE(NULLIF(v.extra_info, ''),     t.extra_info),
    merkle_path  = COALESCE(v.merkle_path,                t.merkle_path),
    timestamp_at = v.timestamp_at
FROM (VALUES ` + values.String() + `) AS v(txid, status, block_hash, block_height, extra_info, merkle_path, timestamp_at)
WHERE t.txid = v.txid`

	if _, err := s.pool.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("batch update: %w", err)
	}
	return nil
}

func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	// Mirror Aerospike's BinMap semantics: empty fields are ignored, so the
	// caller can issue partial updates without clobbering unrelated columns.
	sets := []string{"status = $2", "timestamp_at = $3"}
	args := []any{status.TxID, string(status.Status), status.Timestamp}
	if status.Timestamp.IsZero() {
		args[2] = time.Now()
	}
	idx := 4
	if status.BlockHash != "" {
		sets = append(sets, fmt.Sprintf("block_hash = $%d", idx))
		args = append(args, status.BlockHash)
		idx++
	}
	if status.BlockHeight > 0 {
		sets = append(sets, fmt.Sprintf("block_height = $%d", idx))
		args = append(args, int64(status.BlockHeight))
		idx++
	}
	if status.ExtraInfo != "" {
		sets = append(sets, fmt.Sprintf("extra_info = $%d", idx))
		args = append(args, status.ExtraInfo)
		idx++
	}
	if len(status.MerklePath) > 0 {
		sets = append(sets, fmt.Sprintf("merkle_path = $%d", idx))
		args = append(args, []byte(status.MerklePath))
		idx++
	}

	q := "UPDATE transactions SET "
	for i, set := range sets {
		if i > 0 {
			q += ", "
		}
		q += set
	}
	q += " WHERE txid = $1"

	_, err := s.pool.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("update tx %s: %w", status.TxID, err)
	}
	return nil
}

func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	const q = `
SELECT txid, status, status_code, block_hash, block_height, merkle_path,
       extra_info, competing_txs, raw_tx, retry_count, next_retry_at,
       timestamp_at, created_at
FROM transactions WHERE txid = $1`
	row := s.pool.QueryRow(ctx, q, txid)
	st, err := scanStatus(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.enrichMerklePath(ctx, st)
	return st, nil
}

func (s *Store) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	const q = `
SELECT txid, status, status_code, block_hash, block_height, merkle_path,
       extra_info, competing_txs, raw_tx, retry_count, next_retry_at,
       timestamp_at, created_at
FROM transactions WHERE timestamp_at >= $1
ORDER BY timestamp_at DESC`
	rows, err := s.pool.Query(ctx, q, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*models.TransactionStatus
	for rows.Next() {
		st, err := scanStatus(rows)
		if err != nil {
			return results, err
		}
		results = append(results, st)
	}
	return results, rows.Err()
}

// SetStatusByBlockHash rewrites every row in the block. Block fields are
// cleared on SEEN_ON_NETWORK transitions (reorg path) and kept otherwise —
// matches the Aerospike / Pebble contract.
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	clearBlock := newStatus == models.StatusSeenOnNetwork

	var q string
	if clearBlock {
		q = `UPDATE transactions SET status=$2, block_hash=NULL, block_height=NULL, timestamp_at=NOW()
             WHERE block_hash=$1 RETURNING txid`
	} else {
		q = `UPDATE transactions SET status=$2, timestamp_at=NOW()
             WHERE block_hash=$1 RETURNING txid`
	}

	rows, err := s.pool.Query(ctx, q, blockHash, string(newStatus))
	if err != nil {
		return nil, fmt.Errorf("update by block hash: %w", err)
	}
	defer rows.Close()

	var txids []string
	for rows.Next() {
		var txid string
		if err := rows.Scan(&txid); err != nil {
			return txids, err
		}
		txids = append(txids, txid)
	}
	return txids, rows.Err()
}

// BumpRetryCount is a single-statement atomic increment — no client-side
// mutex needed because Postgres handles the CAS.
func (s *Store) BumpRetryCount(ctx context.Context, txid string) (int, error) {
	const q = `UPDATE transactions SET retry_count = retry_count + 1
	           WHERE txid = $1 RETURNING retry_count`
	var n int
	err := s.pool.QueryRow(ctx, q, txid).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, store.ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("bump retry count %s: %w", txid, err)
	}
	return n, nil
}

func (s *Store) SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time) error {
	const q = `
UPDATE transactions
SET status=$2, raw_tx=$3, next_retry_at=$4, timestamp_at=NOW()
WHERE txid=$1`
	_, err := s.pool.Exec(ctx, q, txid, string(models.StatusPendingRetry), rawTx, nextRetryAt)
	if err != nil {
		return fmt.Errorf("set pending retry fields %s: %w", txid, err)
	}
	return nil
}

// GetReadyRetries uses FOR UPDATE SKIP LOCKED so the reaper can run on
// multiple processes without delivering the same tx twice. Postgres holds
// the row locks until the enclosing transaction commits; for arcade's
// single-request reaper that happens as soon as this function returns.
func (s *Store) GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*store.PendingRetry, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	const q = `
SELECT txid, raw_tx, retry_count, next_retry_at
FROM transactions
WHERE status = 'PENDING_RETRY' AND next_retry_at <= $1
ORDER BY next_retry_at
LIMIT $2
FOR UPDATE SKIP LOCKED`
	rows, err := tx.Query(ctx, q, now, limit)
	if err != nil {
		return nil, err
	}
	var out []*store.PendingRetry
	for rows.Next() {
		r := &store.PendingRetry{}
		if err := rows.Scan(&r.TxID, &r.RawTx, &r.RetryCount, &r.NextRetryAt); err != nil {
			rows.Close()
			return out, err
		}
		if len(r.RawTx) == 0 {
			continue
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}

func (s *Store) ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error {
	const q = `
UPDATE transactions
SET status=$2, raw_tx=NULL, next_retry_at=NULL, timestamp_at=NOW(),
    extra_info = COALESCE(NULLIF($3,''), extra_info)
WHERE txid=$1`
	_, err := s.pool.Exec(ctx, q, txid, string(finalStatus), extraInfo)
	if err != nil {
		return fmt.Errorf("clear retry state %s: %w", txid, err)
	}
	return nil
}

// SetMinedByTxIDs updates only rows that already exist. UPDATE ... WHERE txid
// = ANY($2) is a single round-trip for the batch, and RETURNING lets us emit
// one status object per affected row without a second read.
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, txids []string) ([]*models.TransactionStatus, error) {
	if len(txids) == 0 {
		return nil, nil
	}
	now := time.Now()
	const q = `
UPDATE transactions
SET status=$1, block_hash=$2, timestamp_at=$3
WHERE txid = ANY($4)
RETURNING txid`
	rows, err := s.pool.Query(ctx, q, string(models.StatusMined), blockHash, now, txids)
	if err != nil {
		return nil, fmt.Errorf("set mined: %w", err)
	}
	defer rows.Close()
	var out []*models.TransactionStatus
	for rows.Next() {
		var txid string
		if err := rows.Scan(&txid); err != nil {
			return out, err
		}
		out = append(out, &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			BlockHash: blockHash,
			Timestamp: now,
		})
	}
	return out, rows.Err()
}

// --- BUMP / STUMP ---

func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	const q = `
INSERT INTO bumps (block_hash, block_height, bump_data) VALUES ($1,$2,$3)
ON CONFLICT (block_hash) DO UPDATE SET block_height=EXCLUDED.block_height, bump_data=EXCLUDED.bump_data`
	_, err := s.pool.Exec(ctx, q, blockHash, int64(blockHeight), bumpData)
	if err != nil {
		return fmt.Errorf("insert bump %s: %w", blockHash, err)
	}
	return nil
}

func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	const q = `SELECT block_height, bump_data FROM bumps WHERE block_hash = $1`
	var h int64
	var data []byte
	err := s.pool.QueryRow(ctx, q, blockHash).Scan(&h, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, store.ErrNotFound
	}
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	return uint64(h), data, nil
}

func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	const q = `
INSERT INTO stumps (block_hash, subtree_index, stump_data) VALUES ($1,$2,$3)
ON CONFLICT (block_hash, subtree_index) DO UPDATE SET stump_data=EXCLUDED.stump_data`
	_, err := s.pool.Exec(ctx, q, stump.BlockHash, stump.SubtreeIndex, stump.StumpData)
	if err != nil {
		return fmt.Errorf("insert stump: %w", err)
	}
	return nil
}

func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	const q = `SELECT block_hash, subtree_index, stump_data FROM stumps WHERE block_hash = $1 ORDER BY subtree_index`
	rows, err := s.pool.Query(ctx, q, blockHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*models.Stump
	for rows.Next() {
		st := &models.Stump{}
		if err := rows.Scan(&st.BlockHash, &st.SubtreeIndex, &st.StumpData); err != nil {
			return out, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM stumps WHERE block_hash = $1`, blockHash)
	return err
}

// --- Submissions ---

func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now()
	}
	const q = `
INSERT INTO submissions (submission_id, txid, callback_url, callback_token,
    full_status_updates, retry_count, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (submission_id) DO NOTHING`
	_, err := s.pool.Exec(ctx, q,
		sub.SubmissionID, sub.TxID, sub.CallbackURL, sub.CallbackToken,
		sub.FullStatusUpdates, sub.RetryCount, sub.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert submission: %w", err)
	}
	return nil
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	return s.submissions(ctx, "txid = $1", txid)
}

func (s *Store) GetSubmissionsByToken(ctx context.Context, token string) ([]*models.Submission, error) {
	return s.submissions(ctx, "callback_token = $1", token)
}

func (s *Store) submissions(ctx context.Context, where string, arg any) ([]*models.Submission, error) {
	q := `
SELECT submission_id, txid, callback_url, callback_token, full_status_updates,
       last_delivered_status, retry_count, next_retry_at, created_at
FROM submissions WHERE ` + where
	rows, err := s.pool.Query(ctx, q, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*models.Submission
	for rows.Next() {
		sub := &models.Submission{}
		var lastStatus *string
		var nextRetry *time.Time
		if err := rows.Scan(
			&sub.SubmissionID, &sub.TxID, &sub.CallbackURL, &sub.CallbackToken,
			&sub.FullStatusUpdates, &lastStatus, &sub.RetryCount, &nextRetry,
			&sub.CreatedAt,
		); err != nil {
			return out, err
		}
		if lastStatus != nil {
			sub.LastDeliveredStatus = models.Status(*lastStatus)
		}
		if nextRetry != nil {
			t := *nextRetry
			sub.NextRetryAt = &t
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	const q = `
UPDATE submissions SET last_delivered_status=$2, retry_count=$3, next_retry_at=$4
WHERE submission_id=$1`
	_, err := s.pool.Exec(ctx, q, submissionID, string(lastStatus), retryCount, nextRetry)
	if err != nil {
		return fmt.Errorf("update delivery: %w", err)
	}
	return nil
}

// --- Leaser ---

// TryAcquireOrRenew uses a single CTE to perform CAS-like acquire-or-renew:
// the INSERT ... ON CONFLICT UPDATE fires only if the caller is the current
// holder OR the existing lease has expired. Any other case leaves the row
// unchanged and returns (zero, nil) to signal contention.
func (s *Store) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	expires := time.Now().Add(ttl)
	const q = `
INSERT INTO leases (name, holder, expires_at) VALUES ($1,$2,$3)
ON CONFLICT (name) DO UPDATE
SET holder=EXCLUDED.holder, expires_at=EXCLUDED.expires_at
WHERE leases.holder=EXCLUDED.holder OR leases.expires_at < NOW()
RETURNING expires_at`
	var got time.Time
	err := s.pool.QueryRow(ctx, q, name, holder, expires).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("acquire lease %s: %w", name, err)
	}
	return got, nil
}

func (s *Store) Release(ctx context.Context, name, holder string) error {
	const q = `DELETE FROM leases WHERE name=$1 AND holder=$2`
	_, err := s.pool.Exec(ctx, q, name, holder)
	if err != nil {
		return fmt.Errorf("release lease %s: %w", name, err)
	}
	return nil
}

// --- Datahub endpoint registry ---

func (s *Store) UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error {
	if ep.URL == "" {
		return fmt.Errorf("upsert datahub endpoint: empty url")
	}
	const q = `
INSERT INTO datahub_endpoints (url, source, last_seen)
VALUES ($1, $2, $3)
ON CONFLICT (url) DO UPDATE SET
    source = EXCLUDED.source,
    last_seen = EXCLUDED.last_seen`
	if _, err := s.pool.Exec(ctx, q, ep.URL, ep.Source, ep.LastSeen); err != nil {
		return fmt.Errorf("upsert datahub endpoint %s: %w", ep.URL, err)
	}
	return nil
}

func (s *Store) ListDatahubEndpoints(ctx context.Context) ([]store.DatahubEndpoint, error) {
	const q = `SELECT url, source, last_seen FROM datahub_endpoints`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list datahub endpoints: %w", err)
	}
	defer rows.Close()
	var out []store.DatahubEndpoint
	for rows.Next() {
		var ep store.DatahubEndpoint
		if err := rows.Scan(&ep.URL, &ep.Source, &ep.LastSeen); err != nil {
			return nil, fmt.Errorf("scan datahub endpoint: %w", err)
		}
		out = append(out, ep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter datahub endpoints: %w", err)
	}
	return out, nil
}

// --- scan helpers ---

type rowScanner interface {
	Scan(dest ...any) error
}

// scanStatusWithInserted is scanStatus + the boolean `inserted` column
// returned by BatchGetOrInsertStatus. Kept separate so the row layout for
// the single-row paths stays identical to the existing scanStatus.
func scanStatusWithInserted(row rowScanner) (*models.TransactionStatus, bool, error) {
	var (
		st           models.TransactionStatus
		statusCode   *int
		blockHash    *string
		blockHeight  *int64
		merklePath   []byte
		extraInfo    *string
		competingTxs []byte
		rawTx        []byte
		nextRetry    *time.Time
		inserted     bool
	)
	if err := row.Scan(
		&st.TxID, &st.Status, &statusCode,
		&blockHash, &blockHeight, &merklePath,
		&extraInfo, &competingTxs, &rawTx,
		&st.RetryCount, &nextRetry,
		&st.Timestamp, &st.CreatedAt, &inserted,
	); err != nil {
		return nil, false, err
	}
	if statusCode != nil {
		st.StatusCode = *statusCode
	}
	if blockHash != nil {
		st.BlockHash = *blockHash
	}
	if blockHeight != nil {
		st.BlockHeight = uint64(*blockHeight)
	}
	if len(merklePath) > 0 {
		st.MerklePath = merklePath
	}
	if extraInfo != nil {
		st.ExtraInfo = *extraInfo
	}
	if len(competingTxs) > 0 {
		_ = json.Unmarshal(competingTxs, &st.CompetingTxs)
	}
	if len(rawTx) > 0 {
		st.RawTx = rawTx
	}
	if nextRetry != nil {
		st.NextRetryAt = *nextRetry
	}
	return &st, inserted, nil
}

func scanStatus(row rowScanner) (*models.TransactionStatus, error) {
	var (
		st           models.TransactionStatus
		statusCode   *int
		blockHash    *string
		blockHeight  *int64
		merklePath   []byte
		extraInfo    *string
		competingTxs []byte
		rawTx        []byte
		nextRetry    *time.Time
	)
	if err := row.Scan(
		&st.TxID, &st.Status, &statusCode,
		&blockHash, &blockHeight, &merklePath,
		&extraInfo, &competingTxs, &rawTx,
		&st.RetryCount, &nextRetry,
		&st.Timestamp, &st.CreatedAt,
	); err != nil {
		return nil, err
	}
	if statusCode != nil {
		st.StatusCode = *statusCode
	}
	if blockHash != nil {
		st.BlockHash = *blockHash
	}
	if blockHeight != nil {
		st.BlockHeight = uint64(*blockHeight)
	}
	if len(merklePath) > 0 {
		st.MerklePath = merklePath
	}
	if extraInfo != nil {
		st.ExtraInfo = *extraInfo
	}
	if len(competingTxs) > 0 {
		_ = json.Unmarshal(competingTxs, &st.CompetingTxs)
	}
	if len(rawTx) > 0 {
		st.RawTx = rawTx
	}
	if nextRetry != nil {
		st.NextRetryAt = *nextRetry
	}
	return &st, nil
}

// enrichMerklePath attaches the per-tx minimal merkle path for mined/immutable
// rows, extracting it from the compound BUMP. Matches aerospike/pebble — the
// extraction is duplicated across backends so each store package stays
// self-contained.
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
