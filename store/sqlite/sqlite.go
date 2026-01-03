package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	_ "modernc.org/sqlite"
)

const (
	schemaVersion = 1

	// SQLite pragmas for better concurrency
	sqlitePragmas = `
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
PRAGMA synchronous=NORMAL;
`

	createTransactionsTable = `
CREATE TABLE IF NOT EXISTS transactions (
	txid TEXT PRIMARY KEY,
	status TEXT NOT NULL,
	timestamp DATETIME NOT NULL,
	block_hash TEXT,
	extra_info TEXT,
	competing_txs TEXT DEFAULT '{}',
	created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_block_hash ON transactions(block_hash);
`

	createMerklePathsTable = `
CREATE TABLE IF NOT EXISTS merkle_paths (
	txid TEXT NOT NULL,
	block_hash TEXT NOT NULL,
	block_height INTEGER NOT NULL,
	merkle_path BLOB NOT NULL,
	created_at DATETIME NOT NULL,
	PRIMARY KEY (txid, block_hash)
);
CREATE INDEX IF NOT EXISTS idx_merkle_paths_block_hash ON merkle_paths(block_hash);
`

	createSubmissionsTable = `
CREATE TABLE IF NOT EXISTS submissions (
	submission_id TEXT PRIMARY KEY,
	txid TEXT NOT NULL,
	callback_url TEXT,
	callback_token TEXT,
	full_status_updates INTEGER DEFAULT 0,
	last_delivered_status TEXT,
	retry_count INTEGER DEFAULT 0,
	next_retry_at DATETIME,
	created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_submissions_txid ON submissions(txid);
CREATE INDEX IF NOT EXISTS idx_submissions_callback_token ON submissions(callback_token);
CREATE INDEX IF NOT EXISTS idx_next_retry ON submissions(next_retry_at);
`
)

// Store implements store.StatusStore and store.SubmissionStore using SQLite
type Store struct {
	db *sql.DB
}

// NewStore creates a new unified SQLite store
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if _, err := db.Exec(sqlitePragmas); err != nil {
		return nil, fmt.Errorf("failed to set pragmas: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := initializeSchema(db, createTransactionsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize transactions schema: %w", err)
	}

	if err := initializeSchema(db, createMerklePathsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize merkle_paths schema: %w", err)
	}

	if err := initializeSchema(db, createSubmissionsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize submissions schema: %w", err)
	}

	return &Store{db: db}, nil
}

// StatusStore methods

func (s *Store) InsertStatus(ctx context.Context, status *models.TransactionStatus) error {
	if status.CreatedAt.IsZero() {
		status.CreatedAt = time.Now()
	}

	query := `
INSERT INTO transactions (txid, status, timestamp, block_hash, extra_info, created_at)
VALUES (?, ?, ?, ?, ?, ?)
`
	_, err := s.db.ExecContext(ctx, query,
		status.TxID,
		models.StatusReceived,
		status.Timestamp,
		nullString(status.BlockHash),
		nullString(status.ExtraInfo),
		status.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert status: %w", err)
	}

	return nil
}

func (s *Store) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	disallowed := status.Status.DisallowedPreviousStatuses()

	var query string
	var args []interface{}

	if len(status.CompetingTxs) > 0 {
		placeholders := make([]string, len(disallowed))
		for i := range disallowed {
			placeholders[i] = "?"
		}

		query = fmt.Sprintf(`
UPDATE transactions
SET status = ?,
	timestamp = ?,
	block_hash = ?,
	extra_info = ?,
	competing_txs = json_set(competing_txs, '$.' || ?, json('true'))
WHERE txid = ?
  AND status NOT IN (%s)
`, strings.Join(placeholders, ","))

		args = []interface{}{
			status.Status,
			status.Timestamp,
			nullString(status.BlockHash),
			nullString(status.ExtraInfo),
			status.CompetingTxs[0],
			status.TxID,
		}
		for _, s := range disallowed {
			args = append(args, s)
		}
	} else {
		placeholders := make([]string, len(disallowed))
		for i := range disallowed {
			placeholders[i] = "?"
		}

		query = fmt.Sprintf(`
UPDATE transactions
SET status = ?,
	timestamp = ?,
	block_hash = ?,
	extra_info = ?
WHERE txid = ?
  AND status NOT IN (%s)
`, strings.Join(placeholders, ","))

		args = []interface{}{
			status.Status,
			status.Timestamp,
			nullString(status.BlockHash),
			nullString(status.ExtraInfo),
			status.TxID,
		}
		for _, s := range disallowed {
			args = append(args, s)
		}
	}

	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	query := `
SELECT t.txid, t.status, t.timestamp, t.block_hash, mp.block_height, mp.merkle_path, t.extra_info, t.competing_txs, t.created_at
FROM transactions t
LEFT JOIN merkle_paths mp ON t.txid = mp.txid AND t.block_hash = mp.block_hash
WHERE t.txid = ?
`
	row := s.db.QueryRowContext(ctx, query, txid)
	return scanTransactionStatus(row)
}

func (s *Store) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	query := `
SELECT t.txid, t.status, t.timestamp, t.block_hash, mp.block_height, mp.merkle_path, t.extra_info, t.competing_txs, t.created_at
FROM transactions t
LEFT JOIN merkle_paths mp ON t.txid = mp.txid AND t.block_hash = mp.block_hash
WHERE t.timestamp > ?
ORDER BY t.timestamp ASC
`
	rows, err := s.db.QueryContext(ctx, query, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query statuses since: %w", err)
	}
	defer rows.Close()

	return scanTransactionStatuses(rows)
}

func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	var query string

	// For unmined statuses, clear block fields. For IMMUTABLE, keep them.
	if newStatus == models.StatusSeenOnNetwork || newStatus == models.StatusReceived {
		query = `
UPDATE transactions
SET status = ?,
    timestamp = ?,
    block_hash = NULL
WHERE block_hash = ?
RETURNING txid
`
	} else {
		query = `
UPDATE transactions
SET status = ?,
    timestamp = ?
WHERE block_hash = ?
RETURNING txid
`
	}

	rows, err := s.db.QueryContext(ctx, query, newStatus, time.Now(), blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to set status by block hash: %w", err)
	}
	defer rows.Close()

	var txids []string
	for rows.Next() {
		var txid string
		if err := rows.Scan(&txid); err != nil {
			return nil, fmt.Errorf("failed to scan txid: %w", err)
		}
		txids = append(txids, txid)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return txids, nil
}

func (s *Store) InsertMerklePath(ctx context.Context, txid, blockHash string, blockHeight uint64, merklePath []byte) error {
	query := `
INSERT INTO merkle_paths (txid, block_hash, block_height, merkle_path, created_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (txid, block_hash) DO NOTHING
`
	_, err := s.db.ExecContext(ctx, query, txid, blockHash, blockHeight, merklePath, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert merkle path: %w", err)
	}
	return nil
}

func (s *Store) SetMinedByBlockHash(ctx context.Context, blockHash string) ([]*models.TransactionStatus, error) {
	now := time.Now()

	// Update transactions that have merkle paths for this block
	updateQuery := `
UPDATE transactions
SET status = ?,
    timestamp = ?,
    block_hash = ?
FROM merkle_paths mp
WHERE transactions.txid = mp.txid
  AND mp.block_hash = ?
`
	_, err := s.db.ExecContext(ctx, updateQuery, models.StatusMined, now, blockHash, blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to set mined by block hash: %w", err)
	}

	// Query merkle paths for this block to build the status objects
	selectQuery := `
SELECT txid, block_height, merkle_path
FROM merkle_paths
WHERE block_hash = ?
`
	rows, err := s.db.QueryContext(ctx, selectQuery, blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query merkle paths: %w", err)
	}
	defer rows.Close()

	var statuses []*models.TransactionStatus
	for rows.Next() {
		var txid string
		var blockHeight uint64
		var merklePath []byte
		if err := rows.Scan(&txid, &blockHeight, &merklePath); err != nil {
			return nil, fmt.Errorf("failed to scan merkle path: %w", err)
		}
		statuses = append(statuses, &models.TransactionStatus{
			TxID:        txid,
			Status:      models.StatusMined,
			Timestamp:   now,
			BlockHash:   blockHash,
			BlockHeight: blockHeight,
			MerklePath:  merklePath,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return statuses, nil
}

// SubmissionStore methods

func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	query := `
INSERT INTO submissions (submission_id, txid, callback_url, callback_token, full_status_updates, last_delivered_status, retry_count, next_retry_at, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`
	_, err := s.db.ExecContext(ctx, query,
		sub.SubmissionID,
		sub.TxID,
		nullString(sub.CallbackURL),
		nullString(sub.CallbackToken),
		sub.FullStatusUpdates,
		nullString(string(sub.LastDeliveredStatus)),
		sub.RetryCount,
		nullTime(sub.NextRetryAt),
		sub.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert submission: %w", err)
	}

	return nil
}

func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	query := `
SELECT submission_id, txid, callback_url, callback_token, full_status_updates, last_delivered_status, retry_count, next_retry_at, created_at
FROM submissions
WHERE txid = ?
`
	rows, err := s.db.QueryContext(ctx, query, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to query submissions: %w", err)
	}
	defer rows.Close()

	return scanSubmissions(rows)
}

func (s *Store) GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error) {
	query := `
SELECT submission_id, txid, callback_url, callback_token, full_status_updates, last_delivered_status, retry_count, next_retry_at, created_at
FROM submissions
WHERE callback_token = ?
ORDER BY created_at ASC
`
	rows, err := s.db.QueryContext(ctx, query, callbackToken)
	if err != nil {
		return nil, fmt.Errorf("failed to query submissions by token: %w", err)
	}
	defer rows.Close()

	return scanSubmissions(rows)
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	query := `
UPDATE submissions
SET last_delivered_status = ?, retry_count = ?, next_retry_at = ?
WHERE submission_id = ?
`
	_, err := s.db.ExecContext(ctx, query, string(lastStatus), retryCount, nullTime(nextRetry), submissionID)
	if err != nil {
		return fmt.Errorf("failed to update delivery status: %w", err)
	}

	return nil
}

// Block tracking methods

func (s *Store) IsBlockOnChain(ctx context.Context, blockHash string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM processed_blocks WHERE block_hash = ? AND on_chain = 1",
		blockHash).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check if block is on chain: %w", err)
	}
	return count > 0, nil
}

func (s *Store) MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, onChain bool) error {
	onChainInt := 0
	if onChain {
		onChainInt = 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO processed_blocks (block_hash, block_height, on_chain) VALUES (?, ?, ?)
		 ON CONFLICT(block_hash) DO UPDATE SET on_chain = excluded.on_chain`,
		blockHash, blockHeight, onChainInt)
	if err != nil {
		return fmt.Errorf("failed to mark block as processed: %w", err)
	}
	return nil
}

func (s *Store) HasAnyProcessedBlocks(ctx context.Context) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM processed_blocks LIMIT 1").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check for processed blocks: %w", err)
	}
	return count > 0, nil
}

func (s *Store) GetOnChainBlockAtHeight(ctx context.Context, height uint64) (string, bool, error) {
	var blockHash string
	err := s.db.QueryRowContext(ctx,
		"SELECT block_hash FROM processed_blocks WHERE block_height = ? AND on_chain = 1",
		height).Scan(&blockHash)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to get on-chain block at height: %w", err)
	}
	return blockHash, true, nil
}

func (s *Store) MarkBlockOffChain(ctx context.Context, blockHash string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE processed_blocks SET on_chain = 0 WHERE block_hash = ?",
		blockHash)
	if err != nil {
		return fmt.Errorf("failed to mark block off-chain: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Helper functions

func initializeSchema(db *sql.DB, schema string) error {
	for _, stmt := range strings.Split(schema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute schema statement: %w", err)
		}
	}
	return nil
}

func scanTransactionStatus(row *sql.Row) (*models.TransactionStatus, error) {
	var status models.TransactionStatus
	var blockHash, extraInfo, competingTxsJSON sql.NullString
	var blockHeight sql.NullInt64
	var merklePath []byte

	err := row.Scan(
		&status.TxID,
		&status.Status,
		&status.Timestamp,
		&blockHash,
		&blockHeight,
		&merklePath,
		&extraInfo,
		&competingTxsJSON,
		&status.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan transaction status: %w", err)
	}

	status.BlockHash = blockHash.String
	status.BlockHeight = uint64(blockHeight.Int64)
	status.MerklePath = merklePath
	status.ExtraInfo = extraInfo.String

	if competingTxsJSON.Valid && competingTxsJSON.String != "" {
		var competingTxsMap map[string]bool
		if err := json.Unmarshal([]byte(competingTxsJSON.String), &competingTxsMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal competing_txs: %w", err)
		}
		status.CompetingTxs = make([]string, 0, len(competingTxsMap))
		for txid := range competingTxsMap {
			status.CompetingTxs = append(status.CompetingTxs, txid)
		}
	}
	if status.CompetingTxs == nil {
		status.CompetingTxs = []string{}
	}

	return &status, nil
}

func scanTransactionStatuses(rows *sql.Rows) ([]*models.TransactionStatus, error) {
	var statuses []*models.TransactionStatus

	for rows.Next() {
		var status models.TransactionStatus
		var blockHash, extraInfo, competingTxsJSON sql.NullString
		var blockHeight sql.NullInt64
		var merklePath []byte

		err := rows.Scan(
			&status.TxID,
			&status.Status,
			&status.Timestamp,
			&blockHash,
			&blockHeight,
			&merklePath,
			&extraInfo,
			&competingTxsJSON,
			&status.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction status: %w", err)
		}

		status.BlockHash = blockHash.String
		status.BlockHeight = uint64(blockHeight.Int64)
		status.MerklePath = merklePath
		status.ExtraInfo = extraInfo.String

		if competingTxsJSON.Valid && competingTxsJSON.String != "" {
			var competingTxsMap map[string]bool
			if err := json.Unmarshal([]byte(competingTxsJSON.String), &competingTxsMap); err != nil {
				return nil, fmt.Errorf("failed to unmarshal competing_txs: %w", err)
			}
			status.CompetingTxs = make([]string, 0, len(competingTxsMap))
			for txid := range competingTxsMap {
				status.CompetingTxs = append(status.CompetingTxs, txid)
			}
		}
		if status.CompetingTxs == nil {
			status.CompetingTxs = []string{}
		}

		statuses = append(statuses, &status)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return statuses, nil
}

func scanSubmissions(rows *sql.Rows) ([]*models.Submission, error) {
	var submissions []*models.Submission

	for rows.Next() {
		var sub models.Submission
		var callbackURL, callbackToken, lastDeliveredStatus sql.NullString
		var fullStatusUpdates sql.NullBool
		var nextRetryAt sql.NullTime

		err := rows.Scan(
			&sub.SubmissionID,
			&sub.TxID,
			&callbackURL,
			&callbackToken,
			&fullStatusUpdates,
			&lastDeliveredStatus,
			&sub.RetryCount,
			&nextRetryAt,
			&sub.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan submission: %w", err)
		}

		if callbackURL.Valid {
			sub.CallbackURL = callbackURL.String
		}

		if callbackToken.Valid {
			sub.CallbackToken = callbackToken.String
		}

		if fullStatusUpdates.Valid {
			sub.FullStatusUpdates = fullStatusUpdates.Bool
		}

		if lastDeliveredStatus.Valid {
			sub.LastDeliveredStatus = models.Status(lastDeliveredStatus.String)
		}

		if nextRetryAt.Valid {
			sub.NextRetryAt = &nextRetryAt.Time
		}

		submissions = append(submissions, &sub)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return submissions, nil
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func nullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: *t, Valid: true}
}
