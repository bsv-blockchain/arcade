// Package sqlite provides SQLite-based storage implementation.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	// SQLite driver for database/sql.
	_ "modernc.org/sqlite"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

const (
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

	createStumpsTable = `
CREATE TABLE IF NOT EXISTS stumps (
	block_hash TEXT NOT NULL,
	subtree_index INTEGER NOT NULL,
	stump_data TEXT NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (block_hash, subtree_index)
);
`

	createBumpsTable = `
CREATE TABLE IF NOT EXISTS bumps (
	block_hash TEXT PRIMARY KEY,
	block_height INTEGER NOT NULL,
	bump_data BLOB NOT NULL,
	created_at DATETIME NOT NULL
);
`
)

// Store implements store.Store using SQLite
type Store struct {
	db *sql.DB
}

// NewStore creates a new unified SQLite store
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if _, err := db.ExecContext(context.Background(), sqlitePragmas); err != nil {
		return nil, fmt.Errorf("failed to set pragmas: %w", err)
	}

	if err := db.PingContext(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := initializeSchema(db, createTransactionsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize transactions schema: %w", err)
	}

	if err := initializeSchema(db, createSubmissionsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize submissions schema: %w", err)
	}

	if err := initializeSchema(db, createStumpsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize stumps schema: %w", err)
	}

	if err := initializeSchema(db, createBumpsTable); err != nil {
		return nil, fmt.Errorf("failed to initialize bumps schema: %w", err)
	}

	return &Store{db: db}, nil
}

// GetOrInsertStatus inserts a new transaction status or returns the existing one if it already exists.
func (s *Store) GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
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
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "PRIMARY KEY constraint failed") {
			existing, getErr := s.GetStatus(ctx, status.TxID)
			if getErr != nil {
				return nil, false, fmt.Errorf("failed to get existing status: %w", getErr)
			}
			return existing, false, nil
		}
		return nil, false, fmt.Errorf("failed to insert status: %w", err)
	}

	status.Status = models.StatusReceived
	return status, true, nil
}

// UpdateStatus updates an existing transaction status.
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
	extra_info = ?,
	competing_txs = json_set(competing_txs, '$.' || ?, json('true'))
WHERE txid = ?
  AND status NOT IN (%s)
`, strings.Join(placeholders, ","))

		args = make([]interface{}, 0, 5+len(disallowed))
		args = append(args,
			status.Status,
			status.Timestamp,
			nullString(status.ExtraInfo),
			status.CompetingTxs[0],
			status.TxID,
		)
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
	extra_info = ?
WHERE txid = ?
  AND status NOT IN (%s)
`, strings.Join(placeholders, ","))

		args = make([]interface{}, 0, 4+len(disallowed))
		args = append(args,
			status.Status,
			status.Timestamp,
			nullString(status.ExtraInfo),
			status.TxID,
		)
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

// GetStatus retrieves a transaction status by transaction ID.
func (s *Store) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	query := `
SELECT t.txid, t.status, t.timestamp, t.block_hash, b.block_height, b.bump_data, t.extra_info, t.competing_txs, t.created_at
FROM transactions t
LEFT JOIN bumps b ON t.block_hash = b.block_hash
WHERE t.txid = ?
`
	row := s.db.QueryRowContext(ctx, query, txid)
	return scanTransactionStatus(row)
}

// GetStatusesSince retrieves all transaction statuses since a given time.
func (s *Store) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	query := `
SELECT t.txid, t.status, t.timestamp, t.block_hash, b.block_height, b.bump_data, t.extra_info, t.competing_txs, t.created_at
FROM transactions t
LEFT JOIN bumps b ON t.block_hash = b.block_hash
WHERE t.timestamp > ?
ORDER BY t.timestamp ASC
`
	rows, err := s.db.QueryContext(ctx, query, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query statuses since: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	return scanTransactionStatuses(rows)
}

// SetStatusByBlockHash sets status for all transactions with a given block hash.
func (s *Store) SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error) {
	var query string

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
	defer func() {
		_ = rows.Close()
	}()

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

// InsertBUMP stores a compound BUMP for a block.
func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	query := `
INSERT INTO bumps (block_hash, block_height, bump_data, created_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (block_hash) DO NOTHING
`
	_, err := s.db.ExecContext(ctx, query, blockHash, blockHeight, bumpData, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert bump: %w", err)
	}
	return nil
}

// GetBUMP retrieves the compound BUMP for a block.
func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	var blockHeight uint64
	var bumpData []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT block_height, bump_data FROM bumps WHERE block_hash = ?",
		blockHash).Scan(&blockHeight, &bumpData)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, store.ErrNotFound
	}
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get bump: %w", err)
	}
	return blockHeight, bumpData, nil
}

// SetMinedByTxIDs marks transactions as mined for a given block hash and tx list.
func (s *Store) SetMinedByTxIDs(ctx context.Context, blockHash string, txids []string) ([]*models.TransactionStatus, error) {
	if len(txids) == 0 {
		return nil, nil
	}

	now := time.Now()

	placeholders := make([]string, len(txids))
	args := make([]interface{}, 0, 3+len(txids))
	args = append(args, models.StatusMined, now, blockHash)
	for i, txid := range txids {
		placeholders[i] = "?"
		args = append(args, txid)
	}

	updateQuery := fmt.Sprintf(`
UPDATE transactions
SET status = ?,
    timestamp = ?,
    block_hash = ?
WHERE txid IN (%s)
`, strings.Join(placeholders, ","))

	_, err := s.db.ExecContext(ctx, updateQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to set mined by txids: %w", err)
	}

	statuses := make([]*models.TransactionStatus, 0, len(txids))
	for _, txid := range txids {
		statuses = append(statuses, &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			Timestamp: now,
			BlockHash: blockHash,
		})
	}

	return statuses, nil
}

// InsertSubmission inserts a new submission record.
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

// GetSubmissionsByTxID retrieves submissions for a given transaction ID.
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
	defer func() {
		_ = rows.Close()
	}()

	return scanSubmissions(rows)
}

// GetSubmissionsByToken retrieves submissions for a given callback token.
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
	defer func() {
		_ = rows.Close()
	}()

	return scanSubmissions(rows)
}

// UpdateDeliveryStatus updates delivery status for a submission.
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

// IsBlockOnChain checks if a block is on chain.
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

// MarkBlockProcessed marks a block as processed.
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

// HasAnyProcessedBlocks returns whether any blocks have been processed.
func (s *Store) HasAnyProcessedBlocks(ctx context.Context) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM processed_blocks LIMIT 1").Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check for processed blocks: %w", err)
	}
	return count > 0, nil
}

// GetOnChainBlockAtHeight retrieves the block hash for an on-chain block at the given height.
func (s *Store) GetOnChainBlockAtHeight(ctx context.Context, height uint64) (string, bool, error) {
	var blockHash string
	err := s.db.QueryRowContext(ctx,
		"SELECT block_hash FROM processed_blocks WHERE block_height = ? AND on_chain = 1",
		height).Scan(&blockHash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to get on-chain block at height: %w", err)
	}
	return blockHash, true, nil
}

// MarkBlockOffChain marks a block as off-chain.
func (s *Store) MarkBlockOffChain(ctx context.Context, blockHash string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE processed_blocks SET on_chain = 0 WHERE block_hash = ?",
		blockHash)
	if err != nil {
		return fmt.Errorf("failed to mark block off-chain: %w", err)
	}
	return nil
}

// STUMP operations

// InsertStump stores a STUMP for a subtree in a specific block.
func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	query := `
INSERT INTO stumps (block_hash, subtree_index, stump_data, created_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (block_hash, subtree_index) DO UPDATE SET
    stump_data = excluded.stump_data
`
	encoded := base64.StdEncoding.EncodeToString(stump.StumpData)
	_, err := s.db.ExecContext(ctx, query, stump.BlockHash, stump.SubtreeIndex, encoded, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert stump: %w", err)
	}
	return nil
}

// GetStumpsByBlockHash retrieves all STUMPs for a given block hash.
func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	query := `
SELECT block_hash, subtree_index, stump_data
FROM stumps
WHERE block_hash = ?
`
	rows, err := s.db.QueryContext(ctx, query, blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query stumps: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var stumps []*models.Stump
	for rows.Next() {
		var stump models.Stump
		var encodedData string
		if err := rows.Scan(&stump.BlockHash, &stump.SubtreeIndex, &encodedData); err != nil {
			return nil, fmt.Errorf("failed to scan stump: %w", err)
		}
		decoded, err := base64.StdEncoding.DecodeString(encodedData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode stump data: %w", err)
		}
		stump.StumpData = decoded
		stumps = append(stumps, &stump)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating stump rows: %w", err)
	}
	return stumps, nil
}

// DeleteStumpsByBlockHash removes all STUMPs for a given block hash.
func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM stumps WHERE block_hash = ?", blockHash)
	if err != nil {
		return fmt.Errorf("failed to delete stumps: %w", err)
	}
	return nil
}

// Close closes the database connection.
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
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			return fmt.Errorf("failed to execute schema statement: %w", err)
		}
	}
	return nil
}

func scanTransactionStatus(row *sql.Row) (*models.TransactionStatus, error) {
	var status models.TransactionStatus
	var blockHash, extraInfo, competingTxsJSON sql.NullString
	var blockHeight sql.NullInt64
	var bumpData []byte

	err := row.Scan(
		&status.TxID,
		&status.Status,
		&status.Timestamp,
		&blockHash,
		&blockHeight,
		&bumpData,
		&extraInfo,
		&competingTxsJSON,
		&status.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan transaction status: %w", err)
	}

	status.BlockHash = blockHash.String
	status.BlockHeight = uint64(blockHeight.Int64) //nolint:gosec // safe: database constraints ensure non-negative
	status.ExtraInfo = extraInfo.String

	if len(bumpData) > 0 {
		status.MerklePath = extractMinimalPathForTx(bumpData, status.TxID)
	}

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
		var bumpData []byte

		err := rows.Scan(
			&status.TxID,
			&status.Status,
			&status.Timestamp,
			&blockHash,
			&blockHeight,
			&bumpData,
			&extraInfo,
			&competingTxsJSON,
			&status.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction status: %w", err)
		}

		status.BlockHash = blockHash.String
		status.BlockHeight = uint64(blockHeight.Int64) //nolint:gosec // safe: database constraints ensure non-negative
		status.ExtraInfo = extraInfo.String

		if len(bumpData) > 0 {
			status.MerklePath = extractMinimalPathForTx(bumpData, status.TxID)
		}

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

	// Find the tx at level 0
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

	// Extract minimal path for this offset
	mp := &transaction.MerklePath{
		BlockHeight: compound.BlockHeight,
		Path:        make([][]*transaction.PathElement, len(compound.Path)),
	}

	offset := txOffset
	for level := 0; level < len(compound.Path); level++ {
		if level == 0 {
			if leaf := compound.FindLeafByOffset(level, offset); leaf != nil {
				mp.AddLeaf(level, leaf)
			}
		}
		if sibling := compound.FindLeafByOffset(level, offset^1); sibling != nil {
			mp.AddLeaf(level, sibling)
		}
		offset = offset >> 1
	}

	return mp.Bytes()
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
