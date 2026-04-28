package store

import (
	"context"
	"errors"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// PendingRetry is the lightweight row shape the reaper consumes. It avoids
// pulling full TransactionStatus objects through the retry hot path.
type PendingRetry struct {
	TxID        string
	RawTx       []byte
	RetryCount  int
	NextRetryAt time.Time
}

// DatahubEndpointSourceConfigured marks endpoints seeded from static config.
const DatahubEndpointSourceConfigured = "configured"

// DatahubEndpointSourceDiscovered marks endpoints registered at runtime via
// p2p discovery.
const DatahubEndpointSourceDiscovered = "discovered"

// DatahubEndpoint is a registered datahub URL persisted to the shared store
// so propagation and bump-builder pods running as separate microservices
// converge on the same union of (configured + p2p-discovered) URLs.
type DatahubEndpoint struct {
	URL      string
	Source   string // DatahubEndpointSourceConfigured or DatahubEndpointSourceDiscovered
	LastSeen time.Time
}

// BatchInsertResult is one entry in the result slice returned by
// BatchGetOrInsertStatus. Inserted is true when the row was newly written by
// this call; false when an existing row was found and Existing carries it.
// Result ordering matches the input slice ordering.
type BatchInsertResult struct {
	Existing *models.TransactionStatus // populated only when !Inserted
	Inserted bool
}

// Store handles all persistence operations for transactions and submissions
type Store interface {
	// GetOrInsertStatus inserts a new transaction status or returns the existing one if it already exists.
	// Returns the status, a boolean indicating if it was newly inserted (true) or already existed (false), and any error.
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (existing *models.TransactionStatus, inserted bool, err error)

	// BatchGetOrInsertStatus is the multi-row form of GetOrInsertStatus. The
	// returned slice is in the same order as `statuses` — result[i].Inserted
	// reports whether statuses[i] was newly inserted, and result[i].Existing
	// carries the existing row when !Inserted (it is nil for new inserts).
	//
	// Backends with native batch support (e.g. Postgres via the xmax trick)
	// implement this as a single round-trip; backends without (Aerospike,
	// Pebble) fall back to a bounded-concurrency loop over GetOrInsertStatus.
	BatchGetOrInsertStatus(ctx context.Context, statuses []*models.TransactionStatus) ([]BatchInsertResult, error)

	// UpdateStatus updates an existing transaction status (used for P2P, blocks, etc.)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error

	// BatchUpdateStatus is the multi-row form of UpdateStatus. Same partial-
	// update semantics as UpdateStatus — empty fields are ignored, non-empty
	// fields overwrite. Returns the first error encountered if any. Postgres
	// implements this in a single round-trip via UPDATE ... FROM (VALUES …);
	// other backends fall back to a bounded-concurrency loop.
	BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error

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
	// Implementations must only update records that already exist in the store;
	// txids with no existing record should be silently skipped (not created).
	// Returns full status objects only for the transactions that were actually updated.
	SetMinedByTxIDs(ctx context.Context, blockHash string, txids []string) ([]*models.TransactionStatus, error)

	// InsertSubmission creates a new submission record
	InsertSubmission(ctx context.Context, sub *models.Submission) error

	// GetSubmissionsByTxID retrieves all active subscriptions for a transaction
	GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error)

	// GetSubmissionsByToken retrieves all submissions for a callback token
	GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error)

	// UpdateDeliveryStatus updates the delivery tracking for a submission
	UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error

	// STUMP operations for Merkle Service integration

	// InsertStump stores a STUMP for a subtree in a specific block.
	InsertStump(ctx context.Context, stump *models.Stump) error

	// GetStumpsByBlockHash retrieves all STUMPs for a given block hash.
	GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error)

	// DeleteStumpsByBlockHash removes all STUMPs for a given block hash (used during reorg cleanup).
	DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error

	// BumpRetryCount atomically increments retry_count and returns the new value.
	// Does not touch any other bins — callers combine this with
	// SetPendingRetryFields or ClearRetryState depending on the new count.
	BumpRetryCount(ctx context.Context, txid string) (retryCount int, err error)

	// SetPendingRetryFields writes the durable retry bins: status=PENDING_RETRY,
	// raw_tx, next_retry_at, timestamp. retry_count is untouched — use
	// BumpRetryCount first to get the value that feeds next_retry_at backoff.
	SetPendingRetryFields(ctx context.Context, txid string, rawTx []byte, nextRetryAt time.Time) error

	// GetReadyRetries returns up to limit PENDING_RETRY rows whose
	// next_retry_at has elapsed. Rows include raw_tx and retry_count so the
	// reaper can act without a second read per row.
	GetReadyRetries(ctx context.Context, now time.Time, limit int) ([]*PendingRetry, error)

	// ClearRetryState transitions a tx out of PENDING_RETRY (either on success
	// or final rejection) and deletes the raw_tx + next_retry_at bins so the
	// row stops showing up in ready-retry queries.
	ClearRetryState(ctx context.Context, txid string, finalStatus models.Status, extraInfo string) error

	// EnsureIndexes creates any required secondary indexes for query operations.
	EnsureIndexes() error

	// UpsertDatahubEndpoint registers (or refreshes the LastSeen of) a datahub
	// URL. Used by p2p_client to publish discovered URLs and by main to seed
	// statically configured URLs so all pods see the same registry.
	UpsertDatahubEndpoint(ctx context.Context, ep DatahubEndpoint) error

	// ListDatahubEndpoints returns every registered datahub endpoint. Each
	// pod's teranode.Client polls this on a refresh interval and merges new
	// URLs into its in-memory list.
	ListDatahubEndpoints(ctx context.Context) ([]DatahubEndpoint, error)

	// Close closes the database connection
	Close() error
}
