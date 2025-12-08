package store

import (
	"context"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// StatusStore handles transaction status persistence
type StatusStore interface {
	// InsertStatus inserts a new transaction status (used when client submits via REST API)
	InsertStatus(ctx context.Context, status *models.TransactionStatus) error

	// UpdateStatus updates an existing transaction status (used for P2P, blocks, etc.)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error

	// GetStatus retrieves the status for a transaction
	GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)

	// GetStatusesSince retrieves all transactions updated since a given timestamp
	GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error)

	// Close closes the database connection
	Close() error
}

// SubmissionStore handles client submission and subscription tracking
type SubmissionStore interface {
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

// NetworkStateStore handles network state persistence
type NetworkStateStore interface {
	// UpdateNetworkState updates the current network block height and hash
	UpdateNetworkState(ctx context.Context, state *models.NetworkState) error

	// GetNetworkState retrieves the current network state
	GetNetworkState(ctx context.Context) (*models.NetworkState, error)

	// Close closes the store
	Close() error
}
