package store

import (
	"context"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// Store handles all persistence operations for transactions and submissions
type Store interface {
	// InsertStatus inserts a new transaction status (used when client submits via REST API)
	InsertStatus(ctx context.Context, status *models.TransactionStatus) error

	// UpdateStatus updates an existing transaction status (used for P2P, blocks, etc.)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error

	// GetStatus retrieves the status for a transaction
	GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)

	// GetStatusesSince retrieves all transactions updated since a given timestamp
	GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error)

	// SetStatusByBlockHash updates all transactions with the given block hash to a new status.
	// Returns the txids that were updated. For unmined statuses (SEEN_ON_NETWORK),
	// block fields are cleared. For IMMUTABLE, block fields are preserved.
	SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error)

	// InsertMerklePath stores a merkle path for a transaction in a specific block.
	// The path is stored in binary format.
	InsertMerklePath(ctx context.Context, txid, blockHash string, blockHeight uint64, merklePath []byte) error

	// SetMinedByBlockHash joins merkle_paths to set transactions as MINED for a canonical block.
	// Returns full status objects for all affected transactions.
	SetMinedByBlockHash(ctx context.Context, blockHash string) ([]*models.TransactionStatus, error)

	// InsertSubmission creates a new submission record
	InsertSubmission(ctx context.Context, sub *models.Submission) error

	// GetSubmissionsByTxID retrieves all active subscriptions for a transaction
	GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error)

	// GetSubmissionsByToken retrieves all submissions for a callback token
	GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error)

	// UpdateDeliveryStatus updates the delivery tracking for a submission
	UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error

	// Close closes the database connection
	Close() error
}

