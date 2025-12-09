package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
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
	block_height INTEGER,
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

	createNetworkStateTable = `
CREATE TABLE IF NOT EXISTS network_state (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	current_height INTEGER NOT NULL,
	last_block_hash TEXT NOT NULL,
	last_block_time DATETIME NOT NULL
);
`
)

// StatusStore implements store.StatusStore using SQLite
type StatusStore struct {
	db *sql.DB
}

// NewStatusStore creates a new SQLite status store
func NewStatusStore(dbPath string) (store.StatusStore, error) {
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

	return &StatusStore{db: db}, nil
}

func (s *StatusStore) InsertStatus(ctx context.Context, status *models.TransactionStatus) error {
	if status.CreatedAt.IsZero() {
		status.CreatedAt = time.Now()
	}

	query := `
INSERT INTO transactions (txid, status, timestamp, block_hash, block_height, extra_info, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
`
	_, err := s.db.ExecContext(ctx, query,
		status.TxID,
		models.StatusReceived,
		status.Timestamp,
		nullString(status.BlockHash),
		nullUint64(status.BlockHeight),
		nullString(status.ExtraInfo),
		status.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert status: %w", err)
	}

	return nil
}

func (s *StatusStore) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
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
	block_height = ?,
	extra_info = ?,
	competing_txs = json_set(competing_txs, '$.' || ?, json('true'))
WHERE txid = ?
  AND status NOT IN (%s)
`, strings.Join(placeholders, ","))

		args = []interface{}{
			status.Status,
			status.Timestamp,
			nullString(status.BlockHash),
			nullUint64(status.BlockHeight),
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
	block_height = ?,
	extra_info = ?
WHERE txid = ?
  AND status NOT IN (%s)
`, strings.Join(placeholders, ","))

		args = []interface{}{
			status.Status,
			status.Timestamp,
			nullString(status.BlockHash),
			nullUint64(status.BlockHeight),
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

func (s *StatusStore) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	query := `
SELECT t.txid, t.status, t.timestamp, t.block_hash, t.block_height, mp.merkle_path, t.extra_info, t.competing_txs, t.created_at
FROM transactions t
LEFT JOIN merkle_paths mp ON t.txid = mp.txid AND t.block_hash = mp.block_hash
WHERE t.txid = ?
`
	row := s.db.QueryRowContext(ctx, query, txid)
	return scanTransactionStatus(row)
}

func (s *StatusStore) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	query := `
SELECT t.txid, t.status, t.timestamp, t.block_hash, t.block_height, mp.merkle_path, t.extra_info, t.competing_txs, t.created_at
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

func (s *StatusStore) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	var query string

	// For unmined statuses, clear block fields. For IMMUTABLE, keep them.
	if newStatus == models.StatusSeenOnNetwork || newStatus == models.StatusReceived {
		query = `
UPDATE transactions
SET status = ?,
    timestamp = ?,
    block_hash = NULL,
    block_height = NULL
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

func (s *StatusStore) InsertMerklePath(ctx context.Context, txid, blockHash string, blockHeight uint64, merklePath []byte) error {
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

func (s *StatusStore) SetMinedByBlockHash(ctx context.Context, blockHash string, blockHeight uint64) ([]string, error) {
	query := `
UPDATE transactions
SET status = ?,
    timestamp = ?,
    block_hash = ?,
    block_height = ?
FROM merkle_paths mp
WHERE transactions.txid = mp.txid
  AND mp.block_hash = ?
RETURNING transactions.txid
`
	rows, err := s.db.QueryContext(ctx, query, models.StatusMined, time.Now(), blockHash, blockHeight, blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to set mined by block hash: %w", err)
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

func (s *StatusStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// SubmissionStore implements store.SubmissionStore using SQLite
type SubmissionStore struct {
	db *sql.DB
}

// NewSubmissionStore creates a new SQLite submission store
func NewSubmissionStore(dbPath string) (store.SubmissionStore, error) {
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

	if err := initializeSchema(db, createSubmissionsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &SubmissionStore{db: db}, nil
}

func (s *SubmissionStore) InsertSubmission(ctx context.Context, sub *models.Submission) error {
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

func (s *SubmissionStore) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
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

func (s *SubmissionStore) GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error) {
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

func (s *SubmissionStore) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
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

func (s *SubmissionStore) Close() error {
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

func nullUint64(u uint64) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(u), Valid: u > 0}
}

func nullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{Valid: false}
	}
	return sql.NullTime{Time: *t, Valid: true}
}

// NetworkStateStore implements store.NetworkStateStore using SQLite
type NetworkStateStore struct {
	db *sql.DB
}

// NewNetworkStateStore creates a new SQLite network state store
func NewNetworkStateStore(dbPath string) (store.NetworkStateStore, error) {
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

	if err := initializeSchema(db, createNetworkStateTable); err != nil {
		return nil, fmt.Errorf("failed to initialize network state schema: %w", err)
	}

	return &NetworkStateStore{db: db}, nil
}

func (s *NetworkStateStore) UpdateNetworkState(ctx context.Context, state *models.NetworkState) error {
	query := `
INSERT INTO network_state (id, current_height, last_block_hash, last_block_time)
VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	current_height = excluded.current_height,
	last_block_hash = excluded.last_block_hash,
	last_block_time = excluded.last_block_time
`
	_, err := s.db.ExecContext(ctx, query, state.CurrentHeight, state.LastBlockHash, state.LastBlockTime)
	if err != nil {
		return fmt.Errorf("failed to update network state: %w", err)
	}

	return nil
}

func (s *NetworkStateStore) GetNetworkState(ctx context.Context) (*models.NetworkState, error) {
	query := `
SELECT current_height, last_block_hash, last_block_time
FROM network_state
WHERE id = 1
`
	row := s.db.QueryRowContext(ctx, query)

	var state models.NetworkState
	err := row.Scan(&state.CurrentHeight, &state.LastBlockHash, &state.LastBlockTime)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get network state: %w", err)
	}

	return &state, nil
}

func (s *NetworkStateStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
