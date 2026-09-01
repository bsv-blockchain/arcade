package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// ErrNotFound is returned when a requested record does not exist.
var ErrNotFound = errors.New("not found")

// ErrInvalidPeerPolicy is returned by PeerPolicy.Validate, and by every
// backend's UpsertPeerPolicy, for a policy that cannot be stored faithfully.
// Callers treat it as "drop this advertisement", never as a store fault.
var ErrInvalidPeerPolicy = errors.New("invalid peer policy")

// ErrInvalidEndpointPolicy is returned by EndpointPolicy.Validate for an
// advertised policy that cannot be stored faithfully. Callers drop the policy
// and still register the URL, so this never reaches a backend as a write error.
var ErrInvalidEndpointPolicy = errors.New("invalid endpoint policy")

// ErrReplayUnavailable is returned by IterateStatusesByToken when a backend
// cannot serve the replay within its resource bounds — typically a token whose
// submission set is too large for a backend that has no keyset index over
// (token, timestamp, txid) and would otherwise materialize and sort the whole
// thing in memory.
//
// It exists so refusing is a first-class outcome. Attempting an unbounded
// replay is not merely slow: it has OOM-killed the SSE pod and taken every
// other connected client down with it (#237/#238). The SSE service turns this
// error into an explicit `event: gap`, so the client learns it must reconcile
// over REST and the live stream continues — a bounded, declared degradation
// instead of a crash.
var ErrReplayUnavailable = errors.New("replay unavailable for this token on this backend")

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
//
// Policy is the transaction policy the node advertised in the same node_status
// announcement that carried this URL, or nil when it advertised none. See
// EndpointPolicy for why a write that carries no policy never erases one.
type DatahubEndpoint struct {
	URL      string
	Network  string
	Source   string // DatahubEndpointSourceConfigured or DatahubEndpointSourceDiscovered
	Policy   *EndpointPolicy
	LastSeen time.Time
}

// EndpointPolicy is the transaction policy a teranode advertised in the
// node_status fee_policy object alongside its datahub URL: what that specific
// endpoint will accept. Sizes are in bytes; the mining fee is
// satoshis-per-Bytes, the node_status FeePolicy.MiningFee shape, e.g.
// {Satoshis: 100, Bytes: 1000} == 100 sat/kB.
//
// It is attached to the URL registration purely so operators can see, per
// endpoint, what each node enforces — GET /health reports it. The network-wide
// values arcade itself enforces come from PeerPolicy instead, which is keyed by
// libp2p PeerID and so also covers peers advertising no URL at all. The two are
// deliberately separate: same source, different keys, different questions.
//
// A nil Policy means "this node advertised none" — a statically configured seed
// URL, or a teranode predating fee_policy — which is why it is a pointer rather
// than a zero-valued struct. Every backend treats a nil-policy write as leaving
// any previously recorded policy in place: the configured-URL seed in
// app.BuildServices re-upserts on every process start, and without that rule an
// endpoint that is both configured and announced would lose its policy on each
// restart and regain it seconds later.
type EndpointPolicy struct {
	MiningFeeSatoshis       uint64
	MiningFeeBytes          uint64
	MaxTxSizePolicy         uint64
	MaxScriptSizePolicy     uint64
	MaxTxSigopsCountsPolicy uint64
}

// Validate reports whether ep can be persisted without loss, on the same terms
// as PeerPolicy.Validate: the Postgres and Aerospike backends narrow these to
// signed 64-bit, and node_status is unauthenticated gossip, so the peer chooses
// the numbers. Callers drop an invalid policy to nil rather than failing the
// write — the URL registration is the reason the row exists, and a peer sending
// a garbage size must not be able to remove its own endpoint from the registry.
func (ep EndpointPolicy) Validate() error {
	for _, f := range []struct {
		name  string
		value uint64
	}{
		{"mining fee satoshis", ep.MiningFeeSatoshis},
		{"mining fee byte basis", ep.MiningFeeBytes},
		{"max tx size", ep.MaxTxSizePolicy},
		{"max script size", ep.MaxScriptSizePolicy},
		{"max tx sigops count", ep.MaxTxSigopsCountsPolicy},
	} {
		if f.value > maxStorablePolicyValue {
			return fmt.Errorf("%w: advertised %s %d, above the storable maximum %d",
				ErrInvalidEndpointPolicy, f.name, f.value, maxStorablePolicyValue)
		}
	}
	return nil
}

// StatusCensus is one status's aggregate in a CensusStatusesSince result:
// how many rows sit at that status inside the census window, and the minimum
// timestamp among them (zero when Count is 0).
type StatusCensus struct {
	Count  int64
	Oldest time.Time
}

// PeerPolicy is the transaction policy a peer advertised in its node_status
// gossip announcement, persisted to the shared store so the ARC-compatible
// GET /policy endpoint (served by api-server pods) can compute network-wide
// values even though only the single p2p-client pod observes node_status
// (issue #212).
//
// Keyed by PeerID (a libp2p peer runs on a single network, so PeerID is
// unique); Network is a filter attribute, not part of the key — matching the
// DatahubEndpoint pattern. LastSeen lets readers drop peers not re-heard within
// a TTL so a departed cheap node cannot pin the advertised fee low forever. The
// mining fee is stored as satoshis-per-Bytes (the node_status FeePolicy.MiningFee
// shape), e.g. {Satoshis: 100, Bytes: 1000} == 100 sat/kB.
type PeerPolicy struct {
	PeerID  string
	Network string

	// MiningFeeSatoshis is the peer's advertised rate over MiningFeeBytes.
	// Zero means "not advertised", on exactly the same terms as the size
	// limits below: teranode reports min_mining_tx_fee=0 whenever its policy
	// settings are nil, and the legacy BSV/kB conversion carries that 0
	// through verbatim. Readers skip such rows rather than reading the 0 as
	// "this peer mines for free" — a genuinely zero-fee network is configured
	// with accept_zero_fee, never inferred from gossip.
	MiningFeeSatoshis uint64
	MiningFeeBytes    uint64

	// MaxTxSizePolicy and MaxScriptSizePolicy are the peer's advertised size
	// limits in bytes, from node_status FeePolicy. Zero means "not advertised"
	// — either an older teranode with no fee_policy at all, or a value too
	// large to store — and readers skip such rows rather than treating the
	// zero as a real limit.
	MaxTxSizePolicy     uint64
	MaxScriptSizePolicy uint64

	LastSeen time.Time
}

// maxStorablePolicyValue bounds a peer policy's numeric fields. The Postgres
// and Aerospike backends store them in signed 64-bit columns, so a value above
// MaxInt64 wraps negative on write and reads back as an astronomical uint64 — a
// silently corrupted value that GET /policy would then advertise as the network
// consensus.
//
// The bound can only ever reject garbage: the entire money supply, 21 million
// BSV, is 2.1e15 satoshis — some four thousand times below MaxInt64 — and no
// transaction or script size comes remotely close. It has to be checked rather
// than assumed because node_status is unauthenticated gossip: the peer chooses
// the number.
const maxStorablePolicyValue = uint64(math.MaxInt64)

// Validate reports whether pp can be persisted without loss. Every backend
// calls it before writing, so the uint64→int64 narrowing each one performs is
// bounded by a check instead of by assumption.
//
// The size limits are allowed to be zero ("peer did not advertise"), but they
// are still validated for storable range: values above maxStorablePolicyValue
// are rejected by Validate. recordPeerPolicy calls SanitizePolicySizes to zero
// out-of-range sizes so the fee observation can still be persisted.
func (pp PeerPolicy) Validate() error {
	switch {
	case pp.PeerID == "":
		return fmt.Errorf("%w: empty peer id", ErrInvalidPeerPolicy)
	case pp.MiningFeeBytes == 0:
		// A zero byte basis makes the fee meaningless: readers divide by it to
		// normalize to sat/kB, and lowestObservedFeePerKB has to skip such rows.
		// Refusing the write keeps them out of the store in the first place.
		return fmt.Errorf("%w: peer %s has a zero mining fee byte basis", ErrInvalidPeerPolicy, pp.PeerID)
	case pp.MiningFeeSatoshis > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised %d satoshis, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MiningFeeSatoshis, maxStorablePolicyValue)
	case pp.MiningFeeBytes > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised a %d byte basis, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MiningFeeBytes, maxStorablePolicyValue)
	case pp.MaxTxSizePolicy > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised a %d byte max tx size, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MaxTxSizePolicy, maxStorablePolicyValue)
	case pp.MaxScriptSizePolicy > maxStorablePolicyValue:
		return fmt.Errorf("%w: peer %s advertised a %d byte max script size, above the storable maximum %d",
			ErrInvalidPeerPolicy, pp.PeerID, pp.MaxScriptSizePolicy, maxStorablePolicyValue)
	default:
		return nil
	}
}

// SanitizePolicySizes returns a copy of pp with any advertised size limit that
// cannot be stored without loss zeroed, plus the number of fields it dropped.
// Callers use it to keep an implausible size from failing the whole write: the
// peer's fee observation is independently useful and must survive.
func (pp PeerPolicy) SanitizePolicySizes() (PeerPolicy, int) {
	dropped := 0
	if pp.MaxTxSizePolicy > maxStorablePolicyValue {
		pp.MaxTxSizePolicy = 0
		dropped++
	}
	if pp.MaxScriptSizePolicy > maxStorablePolicyValue {
		pp.MaxScriptSizePolicy = 0
		dropped++
	}
	return pp, dropped
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

	// EnrichMerklePath populates status.MerklePath in place for a MINED/IMMUTABLE
	// status that already carries a BlockHash, extracting the transaction's
	// minimal merkle path from the block's compound BUMP. It is a no-op when the
	// status is nil, already has a MerklePath, has no BlockHash, is not
	// MINED/IMMUTABLE, or the block's BUMP cannot be retrieved/parsed — so callers
	// on push paths can invoke it unconditionally and treat the proof as
	// best-effort (never a delivery gate). Safe to call repeatedly across all the
	// txids of one block: implementations share the bounded bumpcache of parsed,
	// indexed compound BUMPs (see store/bumpcache), so per-tx enrichment is
	// O(tree depth · log level-size), not a re-parse. Unlike GetStatus this does
	// not read the full row (no RawTx), which keeps it cheap enough for the
	// SSE/webhook fan-out hot path.
	EnrichMerklePath(ctx context.Context, status *models.TransactionStatus)

	// GetStatusesSince retrieves all transactions updated since a given timestamp
	GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error)

	// IterateStatusesSince streams every transaction updated since the given
	// timestamp through fn, one row at a time. Implementations must avoid
	// materializing the full result set in memory. fn returning a non-nil
	// error stops iteration and surfaces that error to the caller.
	//
	// This returns FULL rows (raw_tx, merkle_path, competing_txs, …) and, on
	// Postgres, sorts them. It exists for consumers that genuinely need the
	// whole row — propagation's replay and reaper. Do NOT reach for it from a
	// new bulk or startup scan: reusing it for TxTracker hydration shipped 14
	// columns for ~5M rows on every api-server boot and drove both arcade and
	// its Postgres out of memory (issue #276). New bulk consumers get a
	// projected, server-side-filtered method instead — see IterateTrackerRows,
	// IterateStatusesByToken and CensusStatusesSince.
	IterateStatusesSince(ctx context.Context, since time.Time, fn func(*models.TransactionStatus) error) error

	// IterateTrackerRows streams the minimal (txid, status, block_height)
	// projection TxTracker.LoadFromStore needs, filtering server-side so a
	// multi-million-row history is never shipped to the client (issue #276).
	//
	// Contract:
	//   - MUST emit every row whose status is in TrackerStatuses(), exactly
	//     once, with Status and BlockHeight equal to the stored values.
	//   - MUST NOT emit rows in any other status. Pushing the status filter
	//     server-side is the memory contract, not an optimization: REJECTED
	//     alone is ~44% of a mature store.
	//   - MAY additionally drop MINED rows with
	//     0 < block_height < scan.PruneMinedBelow. Implementations are not
	//     required to; a backend whose index cannot express the height
	//     predicate is correct, just less selective.
	//   - MUST NOT read raw_tx, merkle_path, competing_txs or orphaned_anchors,
	//     MUST NOT enrich merkle paths, and MUST NOT impose an ordering. The
	//     ORDER BY over the full table is what pinned gigabytes.
	//   - fn returning a non-nil error stops iteration and surfaces it.
	//
	// TrackerScan.Keep is the specification; the SQL/index filter is only a
	// pushdown of it. The caller re-applies Keep to every emitted row, so a
	// backend that over-emits is a performance issue only — a backend that
	// under-emits is a correctness bug. store/storetest asserts exact
	// agreement across all three backends.
	IterateTrackerRows(ctx context.Context, scan TrackerScan, fn func(TrackerRow) error) error

	// CensusStatusesSince aggregates the stuck-transient census store-side:
	// for each requested status, the number of transaction rows with
	// timestamp >= since AND timestamp < stuckDeadline, plus the minimum
	// timestamp among them. The returned map has exactly one entry per
	// requested status — zero-valued when nothing matched — so callers can
	// publish gauges without existence checks.
	//
	// This exists so the propagation reaper's census over a multi-million-row
	// table never streams rows: Postgres answers it with one GROUP BY
	// aggregate, and other backends push the status filter into their
	// secondary indexes. Counting inside an IterateStatusesSince walk pinned
	// the reaper's effective cadence to the full-scan time (~4 minutes on a
	// ~1.6M-row store) instead of reaper_interval_ms — see issue #290.
	CensusStatusesSince(ctx context.Context, since, stuckDeadline time.Time, statuses []models.Status) (map[models.Status]StatusCensus, error)

	// SetStatusByBlockHash updates all transactions with the given block hash to a new status.
	// Returns the txids that were updated. For unmined statuses (SEEN_ON_NETWORK),
	// block fields are cleared and the cleared anchor is appended to the row's
	// orphaned-anchor history (reorg revert). For IMMUTABLE, block fields are
	// preserved. IMMUTABLE rows are never touched, and rows whose block_hash no
	// longer matches at write time are skipped — a tx concurrently re-anchored
	// to the canonical block must not be reverted by a stale index read.
	SetStatusByBlockHash(ctx context.Context, blockHash string, newStatus models.Status) ([]string, error)

	// GetTxIDsByBlockHash returns the txids of every transaction row currently
	// anchored to blockHash (MINED or IMMUTABLE). The reorg reconciler reads
	// this as the affected set of an orphaned block; rows that have already
	// been re-anchored or reverted no longer appear.
	GetTxIDsByBlockHash(ctx context.Context, blockHash string) ([]string, error)

	// InsertBUMP stores a compound BUMP for a block.
	InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error

	// GetBUMP retrieves the compound BUMP for a block.
	GetBUMP(ctx context.Context, blockHash string) (blockHeight uint64, bumpData []byte, err error)

	// DeleteBUMPByBlockHash removes the stored compound BUMP for a block and
	// invalidates any cached parse. Idempotent — deleting a missing BUMP is a
	// no-op. Operator/cleanup lever; note the reorg reconciler deliberately
	// RETAINS orphaned blocks' BUMPs so historical orphaned-anchor proofs
	// stay resolvable (issue #279).
	DeleteBUMPByBlockHash(ctx context.Context, blockHash string) error

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
	// service started recording). Orphaned rows with reconciled_at IS NULL
	// form the anchor reconciler's work queue.
	MarkBlocksOrphaned(ctx context.Context, blockHashes []string, orphanedAt time.Time) error

	// MarkBlockReconciled stamps reconciled_at on an orphaned block's row,
	// recording that tx re-anchor/revert for this orphan completed. A
	// missing row is a silent no-op.
	MarkBlockReconciled(ctx context.Context, blockHash string, at time.Time) error

	// ListOrphanedBlocksToReconcile returns up to limit rows with
	// status='orphaned' AND reconciled_at IS NULL, oldest orphaned_at
	// first — the anchor reconciler's durable work queue. limit must be > 0.
	ListOrphanedBlocksToReconcile(ctx context.Context, limit int) ([]*models.BlockProcessingStatus, error)

	// MarkBlocksParked transitions every named block from status='active' to
	// status='parked' — the watchdog's terminal state for blocks whose
	// reprocess caps (attempts/age) are exhausted. Rows that are missing or
	// not 'active' (orphaned rows stay orphaned) are silently skipped. Parked
	// rows drop out of ListStaleBlockProcessingStatus so they read as an
	// explicit triage backlog rather than perpetual stale churn; a later
	// UpsertBlockHeaderSeen for the same hash resets them to 'active'.
	MarkBlocksParked(ctx context.Context, blockHashes []string) error

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

	// TokensForTxIDs returns the DISTINCT non-empty callback tokens registered
	// against each of the supplied txids, keyed by txid. A txid with no
	// submission — or none carrying a token — is ABSENT from the result map
	// (never present with an empty slice), so an absent key means "nobody is
	// subscribed". Duplicate txids in the input are collapsed.
	//
	// This is the SSE fan-out hot path. It replaced a per-(event, client)
	// existence probe: fan-out now resolves a txid's token set ONCE and
	// answers every connected client from that set in memory, and resolves a
	// whole bulk event's txid list in one batch. Implementations MUST resolve
	// from the txid side only (a txid has a handful of submissions) and MUST
	// NOT materialize a token's submission list: a single token can hold
	// millions of submissions, and loading one per event is what OOM-killed
	// the SSE service (#237/#238). Implementations that cannot issue an
	// unbounded batch must chunk internally rather than reject the call.
	TokensForTxIDs(ctx context.Context, txids []string) (map[string][]string, error)

	// IterateStatusesByToken streams the current status of every DISTINCT
	// txid registered under callbackToken through fn, in ascending
	// status-timestamp order. Rows are PROJECTED for streaming delivery —
	// txid, status, timestamp, block hash/height only. Implementations MUST
	// NOT load raw_tx or enrich the merkle path: this is the SSE catchup hot
	// path, called for tokens with millions of submissions, and per-row
	// compound-BUMP parsing is what OOMed the SSE service. Filters: when
	// since is non-zero, only statuses with Timestamp strictly after since
	// are emitted; when onlyStatuses is non-empty, only rows in one of those
	// statuses are emitted. fn returning a non-nil error stops the iteration
	// and surfaces that error to the caller.
	//
	// An implementation that cannot serve the replay within its own resource
	// bounds MUST return ErrReplayUnavailable rather than attempt it. Refusing
	// is a supported outcome — the caller degrades to an explicit gap notice
	// and the client reconciles over REST — whereas attempting it is how this
	// path has taken the whole pod down and every connected client with it.
	IterateStatusesByToken(ctx context.Context, callbackToken string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error

	// UpdateDeliveryStatus updates the delivery tracking for a submission
	UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error

	// RecordDeliveryAttempt stamps the outcome of one webhook POST attempt on
	// the submission row: Attempts is incremented and LastAttemptAt/LastResult
	// are overwritten (result is "delivered" or the failure reason, e.g.
	// "status 403"). Deliberately orthogonal to UpdateDeliveryStatus /
	// UpdateDeliveryStatusCAS: those manage the per-transition retry state the
	// CAS resets (issue #166), while this is monotonic lifetime bookkeeping
	// surfaced to clients via GET /tx?callbackToken=… for delivery
	// self-diagnosis (issue #249).
	RecordDeliveryAttempt(ctx context.Context, submissionID string, at time.Time, result string) error

	// UpdateDeliveryStatusCAS atomically advances LastDeliveredStatus from
	// `expected` to `next` for the given submission. Returns true iff a row
	// was updated; false means another replica has already advanced this
	// submission and the caller should silently skip its POST. The retry
	// counter and next-retry timestamp are cleared on success — those are
	// only meaningful while a delivery is in retry state.
	//
	// Used by webhook delivery to coordinate exactly-once POSTs across
	// horizontally-scaled api-server pods. Each replica's events.Publisher
	// subscription gets its own Kafka consumer group, so every pod sees
	// every status update; the CAS funnels concurrent attempts down to one.
	UpdateDeliveryStatusCAS(ctx context.Context, submissionID string, expected, next models.Status) (claimed bool, err error)

	// ListSubmissionsReadyForRetry returns up to limit submissions whose
	// delivery is in retry state (retry_count > 0 and next_retry_at <= now).
	// The webhook reaper consumes this to re-fire deliveries whose POST
	// failed after CAS already advanced LastDeliveredStatus — without this
	// sweep, the retry bookkeeping written by recordFailure would never be
	// consumed and a single-attempt failure would be a permanent loss.
	// Ordered by next_retry_at ASC so the oldest backlog drains first.
	ListSubmissionsReadyForRetry(ctx context.Context, now time.Time, limit int) ([]*models.Submission, error)

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

	// EnsureIndexes provisions whatever the backend needs for query operations
	// (schema, secondary indexes). ctx bounds the whole operation; backends may
	// layer tighter internal deadlines on top.
	EnsureIndexes(ctx context.Context) error

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

	// UpsertPeerPolicy records (or refreshes) a peer's observed mining fee.
	// Called by p2p_client on each node_status announcement carrying a fee.
	UpsertPeerPolicy(ctx context.Context, pp PeerPolicy) error

	// ListPeerPolicies returns every recorded peer policy scoped to the given
	// network. The GET /policy handler filters these by LastSeen and takes the
	// minimum mining fee. Entries with an empty Network (legacy rows) are excluded.
	ListPeerPolicies(ctx context.Context, network string) ([]PeerPolicy, error)

	// Close closes the database connection
	Close() error
}
