package store

import (
	"context"
	"errors"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// Store handles all persistence operations for transactions and submissions
type Store interface {
	// GetOrInsertStatus inserts a new transaction status or returns the existing one if it already exists.
	// Returns the status, a boolean indicating if it was newly inserted (true) or already existed (false), and any error.
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (existing *models.TransactionStatus, inserted bool, err error)

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

	// InsertBUMP stores a compound BUMP for a block.
	InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error

	// GetBUMP retrieves the compound BUMP for a block.
	GetBUMP(ctx context.Context, blockHash string) (blockHeight uint64, bumpData []byte, err error)

	// SetMinedByTxIDs marks transactions as mined for a given block hash and tx list.
	// Returns full status objects for all affected transactions.
	SetMinedByTxIDs(ctx context.Context, blockHash string, txids []string) ([]*models.TransactionStatus, error)

	// InsertSubmission creates a new submission record
	InsertSubmission(ctx context.Context, sub *models.Submission) error

	// GetSubmissionsByTxID retrieves all active subscriptions for a transaction
	GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error)

	// GetSubmissionsByToken retrieves all submissions for a callback token
	GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error)

	// UpdateDeliveryStatus updates the delivery tracking for a submission
	UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error

	// Block tracking for catch-up and reorg handling

	// IsBlockOnChain checks if a block is processed AND on the canonical chain
	IsBlockOnChain(ctx context.Context, blockHash string) (bool, error)

	// MarkBlockProcessed records a block as processed with its chain status
	MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, onChain bool) error

	// HasAnyProcessedBlocks checks if there are any blocks in the processed_blocks table
	HasAnyProcessedBlocks(ctx context.Context) (bool, error)

	// GetOnChainBlockAtHeight returns the block hash at the given height that is on_chain (for reorg detection)
	GetOnChainBlockAtHeight(ctx context.Context, height uint64) (blockHash string, found bool, err error)

	// MarkBlockOffChain marks a block as off-chain (orphaned due to reorg)
	MarkBlockOffChain(ctx context.Context, blockHash string) error

	// STUMP operations for Merkle Service integration

	// InsertStump stores a STUMP for a subtree in a specific block.
	InsertStump(ctx context.Context, stump *models.Stump) error

	// GetStumpsByBlockHash retrieves all STUMPs for a given block hash.
	GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error)

	// DeleteStumpsByBlockHash removes all STUMPs for a given block hash (used during reorg cleanup).
	DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error

	// Close closes the database connection
	Close() error
}
