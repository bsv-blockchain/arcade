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
//
// Network scopes the entry to a Bitcoin network (mainnet/testnet/teratestnet/
// regtest). It exists so a store reused across network changes — or shared
// between pods on the same persistence backend — never serves a peer from one
// network to a pod configured for another. Legacy rows written before this
// field existed have an empty Network and are filtered out by every read.
type DatahubEndpoint struct {
	URL      string
	Network  string
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

	// UpdateStatus updates an existing transaction status (used for P2P, blocks, etc.).
	// If no row exists for status.TxID the call returns ErrNotFound without
	// writing — callers must use GetOrInsertStatus to create new rows. This
	// guards the callback receiver path from creating phantom rows on behalf
	// of unknown txids (F-033 / issue #91).
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error

	// BatchUpdateStatus is the multi-row form of UpdateStatus. Same partial-
	// update semantics as UpdateStatus — empty fields are ignored, non-empty
	// fields overwrite. Rows whose txid is unknown are silently skipped (the
	// per-row ErrNotFound contract from UpdateStatus is collapsed to a no-op
	// here — callers wanting per-row diagnostics use UpdateStatus directly).
	// Postgres implements this in a single round-trip via UPDATE ... FROM
	// (VALUES …); other backends fall back to a bounded-concurrency loop.
	BatchUpdateStatus(ctx context.Context, statuses []*models.TransactionStatus) error

	// BatchUpdateStatusReturning is the diagnostic-rich form of BatchUpdateStatus.
	// Returns a slice the same length as `statuses` where result[i] is the
	// previous row that was merged with (i.e. the row as it existed before
	// the update), or nil for unknown txids and per-row errors. Used by the
	// inbound callback handlers to observe transition-age metrics
	// (RECEIVED→SEEN_ON_NETWORK) without an extra round-trip.
	//
	// Backends are expected to short-circuit when the requested transition
	// is blocked by the status lattice (CanTransitionFrom) — the returned
	// `previous[i]` is still the row that existed at lookup time, but the
	// update is a no-op. Callers can detect "no transition applied" by
	// comparing previous[i].Status to the requested status[i].Status.
	BatchUpdateStatusReturning(ctx context.Context, statuses []*models.TransactionStatus) ([]*models.TransactionStatus, error)

	// GetStatus retrieves the status for a transaction
	GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)

	// GetStatusesSince retrieves all transactions updated since a given timestamp
	GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error)

	// IterateStatusesSince streams every transaction updated since the given
	// timestamp through fn, one row at a time. Implementations must avoid
	// materializing the full result set in memory — this is the bounded-memory
	// path used by TxTracker.LoadFromStore at startup, where months of history
	// would otherwise pin a large slice during pruning. fn returning a non-nil
	// error stops iteration and surfaces that error to the caller.
	IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error

	// SetStatusByBlockHash updates all transactions with the given block hash to a new status.
	// Returns the txids that were updated. For unmined statuses (SEEN_ON_NETWORK),
	// block fields are cleared. For IMMUTABLE, block fields are preserved.
	SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error)

	// InsertBUMP stores a compound BUMP for a block.
	InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error

	// GetBUMP retrieves the compound BUMP for a block.
	GetBUMP(ctx context.Context, blockHash string) (blockHeight uint64, bumpData []byte, err error)

	// --- Block processing status ---
	//
	// These methods track which blocks have reached each milestone in the
	// (header observed → BLOCK_PROCESSED → compound BUMP built) pipeline. The
	// table is observability-first: writers must not fail their primary work
	// because of a status-tracking error.

	// UpsertBlockHeaderSeen records that chaintracks observed a tip header.
	// On insert, status='active' and header_seen_at=seenAt. On conflict,
	// implementations MUST overwrite block_height (chaintracks is the
	// authoritative source) and reset status='active' / orphaned_at=NULL,
	// but MUST preserve the existing header_seen_at, processed_at, and
	// bump_built_at so a re-arrival or reorg-resurrection does not erase
	// earlier milestones.
	UpsertBlockHeaderSeen(ctx context.Context, blockHash string, blockHeight uint64, seenAt time.Time) error

	// MarkBlockProcessed records that the merkle service delivered
	// BLOCK_PROCESSED for this block. Upsert: when no row exists (callback
	// arrived before chaintracks emitted the header), insert with
	// header_seen_at = processedAt. On conflict, only processed_at is
	// updated — block_height and other milestones are left alone.
	MarkBlockProcessed(ctx context.Context, blockHash string, blockHeight uint64, processedAt time.Time) error

	// MarkBlockBUMPBuilt records that the compound BUMP was successfully
	// stored for this block. Same upsert-on-missing semantics as
	// MarkBlockProcessed.
	MarkBlockBUMPBuilt(ctx context.Context, blockHash string, blockHeight uint64, builtAt time.Time) error

	// MarkBlocksOrphaned transitions every named block to status='orphaned'
	// and stamps orphaned_at. Hashes that have no row are silently skipped
	// (chaintracks may emit OrphanedHashes for blocks observed before the
	// service started recording).
	MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) error

	// GetBlockProcessingStatus returns the row keyed by blockHash. Returns
	// ErrNotFound if no row exists.
	GetBlockProcessingStatus(ctx context.Context, blockHash string) (*models.BlockProcessingStatus, error)

	// ListBlockProcessingStatus returns up to limit rows ordered by
	// block_height DESC. When beforeHeight > 0, restricts to rows with
	// block_height < beforeHeight (the keyset cursor). limit must be > 0.
	ListBlockProcessingStatus(ctx context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error)

	// GetActiveTipBlockHeight returns the highest block_height across rows
	// with status='active'. Returns 0 when the table is empty (or every row
	// is orphaned). The bump-builder watchdog uses this to compute a
	// recency window — only blocks within N of the active tip are eligible
	// for /reprocess, so a long arcade outage that floods the table with
	// historical headers doesn't trigger thousands of reprocess calls.
	GetActiveTipBlockHeight(ctx context.Context) (uint64, error)

	// ListStaleBlockProcessingStatus returns up to limit rows where
	// processed_at IS NULL, status='active', header_seen_at < olderThan,
	// and block_height >= minHeight. Ordered by header_seen_at ASC so the
	// watchdog retries the oldest gap first. limit must be > 0.
	ListStaleBlockProcessingStatus(ctx context.Context, olderThan time.Time, minHeight uint64, limit int) ([]*models.BlockProcessingStatus, error)

	// SetMinedByTxIDs marks transactions as mined for a given block (hash + height)
	// and tx list. blockHeight is required: downstream consumers (SSE, webhooks,
	// BUMP-build dedup) rely on the height to anchor each MINED status to a
	// specific block, and a zero/missing height has historically caused dropped
	// updates and BUMP-build re-work (see issue #87 / F-029). Implementations
	// must persist both blockHash and blockHeight on each updated row, and the
	// returned `mined` TransactionStatus values MUST carry BlockHeight populated.
	// Implementations must only update records that already exist in the store;
	// txids with no existing record should be silently skipped (not created).
	//
	// Returns two parallel slices of equal length: `prevs[i]` is the row as it
	// existed immediately before the MINED write, and `mined[i]` is the row as
	// it exists after. Only txids that were actually updated appear in either
	// slice — unknown-txid skips produce no entry on either side. The prevs
	// slice exists so callers can observe the
	// arcade_status_transition_age_seconds{from=*,to=MINED} metric without an
	// extra round-trip.
	SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) (prevs, mined []*models.TransactionStatus, err error)

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

	// MarkMerkleRegisteredByTxIDs records that the given txids have been
	// successfully registered with merkle-service at ts. Unknown txids are
	// silently skipped (matching SetMinedByTxIDs semantics). Used by the
	// startup replay loop to skip rows it already registered recently — see
	// issue #145.
	MarkMerkleRegisteredByTxIDs(ctx context.Context, txids []string, ts time.Time) error

	// GetReapCandidates returns up to limit transactions the reaper should
	// rebroadcast: rows in a non-terminal SEEN_* state (SEEN_ON_NETWORK or
	// SEEN_MULTIPLE_NODES) whose timestamp is in [since, seenDeadline), that
	// carry a non-empty raw_tx, and whose last_rebroadcast_at is either unset
	// or older than rebroadcastBefore. Rows are ordered by last_rebroadcast_at
	// ascending (NULLs first) so the longest-unserved txs are returned first —
	// this is the fairness guarantee that prevents head-of-line starvation
	// under a backlog larger than limit. Each returned row carries at least
	// TxID and RawTx so the reaper can rebroadcast without a second read.
	GetReapCandidates(ctx context.Context, since, seenDeadline, rebroadcastBefore time.Time, limit int) ([]*models.TransactionStatus, error)

	// MarkRebroadcastByTxIDs stamps last_rebroadcast_at = ts for the given
	// txids. Unknown txids are silently skipped (matching
	// MarkMerkleRegisteredByTxIDs semantics). The reaper calls this for every
	// txid it attempted to rebroadcast — on attempt, not just success — so a
	// perpetually-requeueing tx cedes its slot for one rebroadcast interval.
	MarkRebroadcastByTxIDs(ctx context.Context, txids []string, ts time.Time) error

	// EnsureIndexes creates any required secondary indexes for query operations.
	EnsureIndexes() error

	// UpsertDatahubEndpoint registers (or refreshes the LastSeen of) a datahub
	// URL. Used by p2p_client to publish discovered URLs and by main to seed
	// statically configured URLs so all pods see the same registry.
	UpsertDatahubEndpoint(ctx context.Context, ep DatahubEndpoint) error

	// ListDatahubEndpoints returns every registered datahub endpoint scoped
	// to the given network. Each pod's teranode.Client polls this on a refresh
	// interval and merges new URLs into its in-memory list. Entries written
	// before the schema gained a network column have an empty Network and are
	// excluded — they will be re-registered with the correct network the next
	// time their peer announces.
	ListDatahubEndpoints(ctx context.Context, network string) ([]DatahubEndpoint, error)

	// Close closes the database connection
	Close() error
}
