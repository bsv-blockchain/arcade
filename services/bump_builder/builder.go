package bump_builder

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	chaintrackslib "github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// ChainHeaderReader is the narrow contract bump-builder needs from a chain-
// tracker: synchronous lookup of the canonical block header for a given hash.
// Implemented in production by go-chaintracks; mocked in tests.
//
// Returning (nil, nil) is permitted for "header not yet known" so bump-builder
// can fall back to the existing post-build merkle-root validation instead of
// failing the build outright on a transient chaintracks race.
type ChainHeaderReader interface {
	GetHeaderByHash(ctx context.Context, hash *chainhash.Hash) (*chaintrackslib.BlockHeader, error)
}

type Builder struct {
	cfg       *config.Config
	logger    *zap.Logger
	store     store.Store
	producer  *kafka.Producer
	publisher events.Publisher // nil-safe; broadcasts MINED status to SSE / webhooks
	consumer  *kafka.ConsumerGroup
	teranode  *teranode.Client
	// txTracker is nil-safe: when unset, the post-build SetMined path runs with
	// the unfiltered level-0 hash list (legacy behavior). When set, level-0
	// hashes are pre-filtered to those the local tracker knows about, shrinking
	// the SetMinedByTxIDs payload from O(all leaves in block) to O(tracked
	// txids in block).
	txTracker *store.TxTracker
	// chainHeader, when non-nil, supplies the canonical merkle root for a
	// block hash so the datahub-fetched response can be cross-checked at
	// fetch time. Lets us reject a pruned/lying peer's response before
	// BuildCompoundBUMP wastes a round of failed AssembleBUMP calls. nil is
	// the legacy / regtest path — the post-build ValidateCompoundRoot at
	// builder.go (below) is still the final safety net.
	chainHeader ChainHeaderReader
}

// New constructs a Builder. producer is the shared process-wide producer —
// the builder reuses it (for DLQ routing) rather than creating a duplicate
// connection. publisher fans MINED status updates out to subscribers.
// teranodeClient supplies the live datahub URL list (static + p2p-discovered,
// refreshed from the shared store) used for block fetches. txTracker, when
// non-nil, gates the post-build SetMined call to txids the local tracker
// knows about — a no-op for correctness (the SQL UPDATE filters by row
// existence anyway) but a meaningful payload-size cut for large blocks.
//
// The block-processing watchdog lives in services/watchdog and runs as a
// separate arcade service (mode=watchdog) — bump-builder is no longer
// responsible for stale-block recovery.
func New(
	cfg *config.Config,
	logger *zap.Logger,
	producer *kafka.Producer,
	publisher events.Publisher,
	st store.Store,
	teranodeClient *teranode.Client,
	txTracker *store.TxTracker,
	chainHeader ChainHeaderReader,
) *Builder {
	return &Builder{
		cfg:         cfg,
		logger:      logger.Named("bump-builder"),
		store:       st,
		producer:    producer,
		publisher:   publisher,
		teranode:    teranodeClient,
		txTracker:   txTracker,
		chainHeader: chainHeader,
	}
}

// combineValidators chains two validators: both must accept for the response
// to be accepted. nil inputs are no-ops, so callers can compose conditionally
// without nil guards at each call site.
func combineValidators(a, b bump.BlockDataValidator) bump.BlockDataValidator {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return func(hashes []chainhash.Hash, root *chainhash.Hash) error {
		if err := a(hashes, root); err != nil {
			return err
		}
		return b(hashes, root)
	}
}

// chainHeaderRootValidator returns a validator that compares the datahub's
// header merkle root against the canonical merkle root from chaintracks. A
// mismatch means the datahub returned a block representation that doesn't
// belong to the canonical chain (pruned peer, stale cache, malicious peer),
// so the fetch loop should fall through to the next URL.
//
// Soft-fails to "no validation" when chaintracks doesn't know the header
// yet — this is a real race in mode=all where chaintracks's P2P subscription
// is independent of the BLOCK_PROCESSED message that drives bump-builder.
// The post-build ValidateCompoundRoot at builder.go still runs and catches
// any compound that doesn't reconcile.
func (b *Builder) chainHeaderRootValidator(ctx context.Context, blockHash string, logger *zap.Logger) bump.BlockDataValidator {
	if b.chainHeader == nil {
		return nil
	}
	hashObj, err := chainhash.NewHashFromHex(blockHash)
	if err != nil {
		logger.Warn("skipping canonical-root validation: invalid block hash", zap.Error(err))
		return nil
	}
	return func(_ []chainhash.Hash, fetchedRoot *chainhash.Hash) error {
		header, lookupErr := b.chainHeader.GetHeaderByHash(ctx, hashObj)
		if lookupErr != nil || header == nil {
			// Soft-fail: log and accept the response. Post-build
			// ValidateCompoundRoot is still the final guard.
			if lookupErr != nil && !errors.Is(lookupErr, context.Canceled) {
				logger.Debug("chaintracks header lookup failed; skipping canonical-root validation",
					zap.String("block_hash", blockHash),
					zap.Error(lookupErr),
				)
			}
			return nil
		}
		if fetchedRoot == nil || !header.MerkleRoot.IsEqual(fetchedRoot) {
			canonical := "<nil>"
			if header != nil {
				canonical = header.MerkleRoot.String()
			}
			fetched := "<nil>"
			if fetchedRoot != nil {
				fetched = fetchedRoot.String()
			}
			return fmt.Errorf("merkle_root mismatch: canonical %s, datahub %s", canonical, fetched)
		}
		return nil
	}
}

// publishStatus fans a status update to the events Publisher. Non-fatal —
// SSE catchup and the durable store row recover any drops.
func (b *Builder) publishStatus(ctx context.Context, status *models.TransactionStatus) {
	if b.publisher == nil || status == nil {
		return
	}
	if err := b.publisher.Publish(ctx, status); err != nil {
		b.logger.Warn("failed to publish status update",
			zap.String("txid", status.TxID),
			zap.String("status", string(status.Status)),
			zap.Error(err),
		)
	}
}

// markMinedAndPublish moves the txids to MINED and fans the resulting status
// updates out to the events Publisher. blockHeight is required so each
// published status carries the block-height anchor that downstream SSE /
// webhook / BUMP-dedup consumers depend on (issue #87 / F-029). If a backend
// regresses and returns a status with BlockHeight == 0, the publish path
// repairs it from the compound BUMP's height before fanning out so a
// half-applied revert can never reintroduce the original bug.
func (b *Builder) markMinedAndPublish(ctx context.Context, logger *zap.Logger, blockHash string, blockHeight uint64, txids []string) {
	mined, err := b.store.SetMinedByTxIDs(ctx, blockHash, blockHeight, txids)
	if err != nil {
		logger.Error("failed to set mined status", zap.Error(err))
		return
	}
	logger.Info("set transactions to MINED",
		zap.Int("count", len(mined)),
		zap.Uint64("block_height", blockHeight),
	)
	// SetMinedByTxIDs returns full status objects only for the rows it
	// actually updated — silently skipping txids without an existing record.
	// Publish the rich rows directly so SSE clients receive blockHash /
	// blockHeight / merklePath in their status updates.
	for _, st := range mined {
		if st.BlockHeight == 0 {
			st.BlockHeight = blockHeight
		}
		b.publishStatus(ctx, st)
	}
}

func (b *Builder) Name() string { return "bump-builder" }

func (b *Builder) Start(ctx context.Context) error {
	consumer, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:     b.producer.Broker(),
		GroupID:    b.cfg.Kafka.ConsumerGroup + "-bump-builder",
		Topics:     []string{kafka.TopicBlockProcessed},
		Handler:    b.handleMessage,
		Producer:   b.producer,
		MaxRetries: b.cfg.Kafka.MaxRetries,
		Logger:     b.logger,
	})
	if err != nil {
		return fmt.Errorf("creating consumer group: %w", err)
	}
	b.consumer = consumer

	b.logger.Info("bump builder started",
		zap.Int("grace_window_ms", b.cfg.BumpBuilder.GraceWindowMs),
	)

	// One-shot startup janitor: drop orphan STUMPs left behind by blocks
	// whose BUMP build already succeeded (bump_built_at IS NOT NULL). The
	// happy-path DeleteStumpsByBlockHash at the end of handleMessage is
	// best-effort — any transient store error there leaves orphans that
	// never get cleaned up by the normal flow.
	go b.pruneOrphanStumps(ctx)

	return consumer.Run(ctx)
}

// pruneOrphanStumps walks the most recent block_processing rows and deletes
// STUMP rows for any block that already has bump_built_at set. Bounded by
// the watchdog's recency window so a long-running deployment doesn't
// repeatedly scan ancient history. Runs once at startup and exits.
//
// This catches the "happy-path delete failed transiently" orphan case. The
// "perma-failure" case (BUMP build never validates, stumps stuck forever
// after Kafka DLQ) needs a different cleanup path keyed on block age rather
// than bump_built_at — tracked separately; the metric
// arcade_bump_builder_empty_stump_blocks_total will surface accumulation if
// it becomes a real issue.
func (b *Builder) pruneOrphanStumps(ctx context.Context) {
	// Use the watchdog's RecencyDepth as the scan horizon — anything older
	// than (tip - RecencyDepth) is outside arcade's recovery window anyway.
	// Default to 144 (~24h at 10min blocks) when watchdog is disabled.
	depth := uint64(b.cfg.Watchdog.RecencyDepth)
	if depth == 0 {
		depth = 144
	}

	tip, err := b.store.GetActiveTipBlockHeight(ctx)
	if err != nil {
		b.logger.Warn("orphan-stump prune: failed to read tip", zap.Error(err))
		return
	}
	if tip == 0 {
		return // empty store / fresh deployment — nothing to prune
	}

	const pageSize = 200
	var (
		cursor  = tip + 1 // exclusive upper bound; first page starts strictly below tip+1
		scanned int
		pruned  int
	)
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		rows, err := b.store.ListBlockProcessingStatus(ctx, cursor, pageSize)
		if err != nil {
			b.logger.Warn("orphan-stump prune: list failed",
				zap.Uint64("cursor", cursor),
				zap.Error(err),
			)
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			scanned++
			if r.BUMPBuiltAt == nil {
				continue
			}
			// Idempotent: no-op when there are no stumps for the block.
			if err := b.store.DeleteStumpsByBlockHash(ctx, r.BlockHash); err != nil {
				b.logger.Warn("orphan-stump prune: delete failed",
					zap.String("block_hash", r.BlockHash),
					zap.Error(err),
				)
				continue
			}
			pruned++
		}
		// Stop when we've stepped outside the recency window.
		oldest := rows[len(rows)-1]
		if tip > depth && oldest.BlockHeight <= tip-depth {
			break
		}
		// Advance the keyset cursor strictly below the oldest row we saw.
		if oldest.BlockHeight == 0 {
			break
		}
		cursor = oldest.BlockHeight
	}
	b.logger.Info("orphan-stump prune complete",
		zap.Uint64("tip", tip),
		zap.Uint64("recency_depth", depth),
		zap.Int("scanned", scanned),
		zap.Int("pruned", pruned),
	)
}

func (b *Builder) Stop() error {
	b.logger.Info("stopping bump builder")
	if b.consumer != nil {
		return b.consumer.Close()
	}
	return nil
}

func (b *Builder) handleMessage(ctx context.Context, msg *kafka.Message) error {
	overallStart := time.Now()
	metrics.BumpBuilderBlocksProcessedTotal.Inc()
	// outcome reflects the terminal disposition of this BLOCK_PROCESSED. Set
	// before each return so the duration histogram lands in the right bucket.
	outcome := "success"
	defer func() {
		metrics.BumpBuilderBuildDuration.WithLabelValues(outcome).Observe(time.Since(overallStart).Seconds())
	}()

	var callback models.CallbackMessage
	if err := json.Unmarshal(msg.Value, &callback); err != nil {
		outcome = "parse_failed"
		return fmt.Errorf("unmarshaling block processed message: %w", err)
	}

	blockHash := callback.BlockHash
	if blockHash == "" {
		outcome = "parse_failed"
		return fmt.Errorf("empty block hash in block_processed message")
	}

	logger := b.logger.With(zap.String("block_hash", blockHash))

	// Short-circuit: if a compound BUMP already exists for this block, this is
	// a redelivery (typically from a /reprocess re-firing BLOCK_PROCESSED).
	// Skip the datahub fetch + recompute entirely; re-run SetMinedByTxIDs with
	// the level-0 hashes from the stored BUMP so any tracked tx that was
	// registered AFTER the original build still gets marked MINED. The UPDATE
	// is idempotent at the store layer, so this is safe to repeat per delivery.
	if existingHeight, bumpBytes, err := b.store.GetBUMP(ctx, blockHash); err == nil && len(bumpBytes) > 0 {
		txids, parseErr := levelZeroTxidsFromBUMP(bumpBytes)
		if parseErr != nil {
			// Stored BUMP failed to decode — fall through to the normal
			// rebuild path so an upstream corruption doesn't pin a block
			// in a broken state forever.
			logger.Warn("stored BUMP failed to parse on redelivery — rebuilding", zap.Error(parseErr))
		} else {
			metrics.BumpBuilderShortCircuitTotal.Inc()
			outcome = "short_circuited"
			tracked := b.filterTrackedTxids(txids)
			logger.Info("BUMP already built — skipping datahub fetch on redelivery",
				zap.Int("level0_count", len(txids)),
				zap.Int("tracked_count", len(tracked)),
				zap.Uint64("block_height", existingHeight),
			)
			if len(tracked) > 0 {
				b.markMinedAndPublish(ctx, logger, blockHash, existingHeight, tracked)
			}
			// STUMP rows for this block should already have been pruned at
			// the end of the original build; ensure stragglers are cleared
			// in case a STUMP arrived after pruning ran.
			if err := b.store.DeleteStumpsByBlockHash(ctx, blockHash); err != nil {
				logger.Warn("failed to clean up STUMPs on short-circuit", zap.Error(err))
			}
			return nil
		}
	}

	// Grace window: merkle-service's stumpGate only waits for the first HTTP attempt
	// of each STUMP before releasing BLOCK_PROCESSED. STUMPs that got a 5xx on the
	// first attempt retry asynchronously and may land after BLOCK_PROCESSED.
	if grace := time.Duration(b.cfg.BumpBuilder.GraceWindowMs) * time.Millisecond; grace > 0 {
		metrics.BumpBuilderGraceWaitTotal.Inc()
		logger.Debug("waiting grace window", zap.Duration("duration", grace))
		select {
		case <-ctx.Done():
			outcome = "context_canceled"
			return ctx.Err()
		case <-time.After(grace):
		}
	}

	// 1. Get all STUMPs for this block
	stumps, err := b.store.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		outcome = "store_failed"
		return fmt.Errorf("getting STUMPs for block: %w", err)
	}

	metrics.BumpBuilderStumpCount.Observe(float64(len(stumps)))
	if len(stumps) == 0 {
		outcome = "no_stumps"
		metrics.BumpBuilderEmptyStumpBlocksTotal.Inc()
		// Warn — not Info — because the legitimate case (block contains no
		// tracked txs) is indistinguishable from the silent-drop case
		// (STUMP callbacks were dedup'd / DLQ'd / lost upstream). Operators
		// need to see this rate in logs; a sustained stream while watched
		// txs are in flight is the signal that merkle-service callbacks
		// aren't landing.
		logger.Warn("BLOCK_PROCESSED arrived with zero STUMPs for this block — either no tracked txs in this block, or upstream STUMP callbacks were dropped (check merkle-service callback delivery)")
		return nil
	}

	logger.Info("building compound BUMP", zap.Int("stump_count", len(stumps)))
	logStumpInputs(logger, stumps)

	// 2. Fetch subtree hashes + coinbase BUMP + header merkle root from datahub.
	// Pull the live URL list from the shared teranode.Client so this picks up
	// p2p-discovered URLs in addition to statically configured ones. Fall back
	// to the full set if every endpoint is currently sidelined by the circuit
	// breaker — better to retry against a sidelined URL than fail with zero
	// attempts.
	endpoints := b.teranode.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		endpoints = b.teranode.GetEndpoints()
	}
	// Build a response validator from the STUMPs we already have. A datahub
	// claiming fewer subtrees than max(stump.SubtreeIndex)+1 is provably
	// wrong — without this guard, a pruned/lying peer's "200 OK" with a
	// truncated subtree list poisons every retry until the message DLQs.
	minSubtrees := 0
	for _, s := range stumps {
		if s.SubtreeIndex+1 > minSubtrees {
			minSubtrees = s.SubtreeIndex + 1
		}
	}
	validator := bump.SubtreeCountValidator(minSubtrees)
	if b.chainHeader != nil {
		validator = combineValidators(validator, b.chainHeaderRootValidator(ctx, blockHash, logger))
	}

	logger.Debug("fetching block data from datahub",
		zap.Strings("datahub_urls", endpoints),
		zap.Int("min_subtrees", minSubtrees),
	)
	fetchStart := time.Now()
	subtreeHashes, coinbaseBUMP, headerMerkleRoot, err := bump.FetchBlockDataForBUMPWithOptions(ctx, endpoints, blockHash, b.cfg.BumpBuilder.DataHubMaxBlockBytes, validator, logger)
	metrics.BumpBuilderDatahubFetchDuration.Observe(time.Since(fetchStart).Seconds())
	if err != nil {
		outcome = "fetch_failed"
		return fmt.Errorf("fetching block data: %w", err)
	}
	logger.Debug("datahub fetch succeeded",
		zap.Int("subtree_count", len(subtreeHashes)),
		zap.Bool("has_coinbase_bump", coinbaseBUMP != nil),
		zap.Bool("has_header_merkle_root", headerMerkleRoot != nil),
	)
	logBlockInputs(logger, subtreeHashes, coinbaseBUMP)

	if len(subtreeHashes) == 0 {
		logger.Warn("block has no subtrees, cannot construct BUMPs")
		return nil
	}

	// Per-STUMP assembled paths (before merge) — useful for spotting which subtree
	// contributed a wrong element to the compound BUMP.
	logPerStumpAssembly(logger, stumps, subtreeHashes, coinbaseBUMP)

	// 3. Build compound BUMP (STUMPs are sparse — only for subtrees with tracked txs)
	compound, txids, err := bump.BuildCompoundBUMP(stumps, subtreeHashes, coinbaseBUMP)
	if err != nil {
		return fmt.Errorf("building compound BUMP: %w", err)
	}

	blockHeight := uint64(compound.BlockHeight)
	bumpBytes := compound.Bytes()
	logCompoundBUMP(logger, compound, bumpBytes, txids)

	// 4. Validate: compound BUMP root must match the block header's merkle root.
	// A mismatch means the compound is malformed (missing siblings, wrong offsets,
	// stale subtree roots, …). Refuse to persist so clients never see a BUMP that
	// fails ComputeRoot, and leave txs non-MINED + STUMPs intact so a retry can
	// rebuild once the inputs are correct.
	if err := bump.ValidateCompoundRoot(compound, headerMerkleRoot); err != nil {
		dumpBUMPFailureInputs(logger, stumps, subtreeHashes, coinbaseBUMP, headerMerkleRoot, compound, bumpBytes, txids, err)
		return fmt.Errorf("compound BUMP root mismatch for block %s: %w", blockHash, err)
	}

	// 5. Store compound BUMP as binary
	if err := b.store.InsertBUMP(ctx, blockHash, blockHeight, bumpBytes); err != nil {
		return fmt.Errorf("storing BUMP: %w", err)
	}

	// Observability-only: record bump_built_at on the block-processing row.
	// Failure here must not block the MINED status updates downstream.
	if err := b.store.MarkBlockBUMPBuilt(ctx, blockHash, blockHeight, time.Now()); err != nil {
		logger.Warn("failed to record bump_built status", zap.Error(err))
	}

	// 6. Set tracked transactions to MINED.
	// blockHeight is threaded through here (and asserted on the returned
	// statuses below) because downstream SSE/webhook consumers and the
	// dedup path in BUMP-build rely on the height to anchor each MINED
	// status to a specific block — a zero/missing height triggered F-029.
	//
	// BuildCompoundBUMP returns every level-0 hash in the compound (the
	// caller's job is to filter). Pre-filter against TxTracker so the
	// SetMinedByTxIDs UPDATE only carries txids the local store could
	// possibly have. For mainnet blocks with thousands of leaves this
	// drops payload size by ~99% even though the store-level WHERE clause
	// would have filtered out the misses anyway.
	tracked := b.filterTrackedTxids(txids)
	if len(tracked) > 0 {
		b.markMinedAndPublish(ctx, logger, blockHash, blockHeight, tracked)
	}

	// 7. Prune STUMPs
	if err := b.store.DeleteStumpsByBlockHash(ctx, blockHash); err != nil {
		logger.Warn("failed to clean up STUMPs", zap.Error(err))
	}

	logger.Info("BUMP built successfully",
		zap.Int("tracked_txids", len(tracked)),
		zap.Int("level0_count", len(txids)),
		zap.Int("stumps_pruned", len(stumps)),
	)
	return nil
}

// filterTrackedTxids narrows a level-0 hash list to those the local
// TxTracker knows about. If the builder was constructed without a tracker
// (legacy wiring / tests), every txid is passed through unchanged.
func (b *Builder) filterTrackedTxids(txids []string) []string {
	if b.txTracker == nil || len(txids) == 0 {
		return txids
	}
	tracked, unknown := b.txTracker.FilterTrackedTxids(txids)
	if unknown > 0 {
		metrics.BumpBuilderUntrackedTxidsTotal.Add(float64(unknown))
	}
	return tracked
}

// levelZeroTxidsFromBUMP parses a stored compound BUMP and returns every
// level-0 hash as a lowercase hex string. Used by the short-circuit path
// to recover the candidate txid list without re-running BuildCompoundBUMP.
func levelZeroTxidsFromBUMP(bumpData []byte) ([]string, error) {
	mp, err := transaction.NewMerklePathFromBinary(bumpData)
	if err != nil {
		return nil, fmt.Errorf("parsing stored BUMP: %w", err)
	}
	if len(mp.Path) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(mp.Path[0]))
	for _, elem := range mp.Path[0] {
		if elem.Hash == nil {
			continue
		}
		out = append(out, elem.Hash.String())
	}
	return out, nil
}

// --- Debug helpers ---
//
// These emit at Debug level only (enabled via log_level=debug). They dump the
// raw inputs and intermediate artifacts of BUMP construction so a human can
// replay the math offline and compare against expected values.

// logStumpInputs logs each stored STUMP: its subtree index, the raw BRC-74
// bytes, and the level-0 hashes (candidate txids in that subtree).
func logStumpInputs(logger *zap.Logger, stumps []*models.Stump) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	for _, s := range stumps {
		leaves := bump.ExtractLevel0Hashes(s.StumpData)
		leafHex := make([]string, len(leaves))
		for i, h := range leaves {
			leafHex[i] = h.String()
		}
		logger.Debug("stump input",
			zap.Int("subtree_index", s.SubtreeIndex),
			zap.Int("stump_bytes", len(s.StumpData)),
			zap.String("stump_hex", hex.EncodeToString(s.StumpData)),
			zap.Int("level0_count", len(leaves)),
			zap.Strings("level0_hashes", leafHex),
		)
	}
}

// logBlockInputs dumps the datahub-provided subtree hashes and coinbase BUMP
// that feed into compound construction.
func logBlockInputs(logger *zap.Logger, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	subtreeHex := make([]string, len(subtreeHashes))
	for i, h := range subtreeHashes {
		subtreeHex[i] = h.String()
	}
	logger.Debug("block inputs",
		zap.Int("subtree_count", len(subtreeHashes)),
		zap.Strings("subtree_hashes", subtreeHex),
	)
	if len(coinbaseBUMP) > 0 {
		cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
		var cbTxID string
		if err == nil && len(cbPath.Path) > 0 {
			for _, e := range cbPath.Path[0] {
				if e.Offset == 0 && e.Hash != nil {
					cbTxID = e.Hash.String()
					break
				}
			}
		}
		logger.Debug("coinbase bump",
			zap.Int("bytes", len(coinbaseBUMP)),
			zap.String("hex", hex.EncodeToString(coinbaseBUMP)),
			zap.String("coinbase_txid", cbTxID),
		)
	}
}

// logPerStumpAssembly expands each STUMP into its full-block merkle path in
// isolation and logs the per-level path elements. The compound BUMP is the
// deduped union of these — a wrong element here is a wrong element there.
func logPerStumpAssembly(logger *zap.Logger, stumps []*models.Stump, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	for _, s := range stumps {
		full, _, err := bump.AssembleBUMP(s.StumpData, s.SubtreeIndex, subtreeHashes, coinbaseBUMP)
		if err != nil {
			logger.Debug("per-stump assembly failed",
				zap.Int("subtree_index", s.SubtreeIndex),
				zap.Error(err),
			)
			continue
		}
		logger.Debug("per-stump assembly",
			zap.Int("subtree_index", s.SubtreeIndex),
			zap.Uint32("block_height", full.BlockHeight),
			zap.Int("levels", len(full.Path)),
			zap.String("full_bump_hex", hex.EncodeToString(full.Bytes())),
		)
		for level, elems := range full.Path {
			logger.Debug("per-stump level",
				zap.Int("subtree_index", s.SubtreeIndex),
				zap.Int("level", level),
				zap.String("elements", formatPathElements(elems)),
			)
		}
	}
}

// logCompoundBUMP dumps the final merged BUMP: raw bytes, per-level structure,
// and all tracked txids.
func logCompoundBUMP(logger *zap.Logger, compound *transaction.MerklePath, bumpBytes []byte, txids []string) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	logger.Debug("compound bump",
		zap.Uint32("block_height", compound.BlockHeight),
		zap.Int("levels", len(compound.Path)),
		zap.Int("bytes", len(bumpBytes)),
		zap.String("hex", hex.EncodeToString(bumpBytes)),
		zap.Int("txid_count", len(txids)),
		zap.Strings("txids", txids),
	)
	for level, elems := range compound.Path {
		logger.Debug("compound bump level",
			zap.Int("level", level),
			zap.Int("element_count", len(elems)),
			zap.String("elements", formatPathElements(elems)),
		)
	}
}

// dumpBUMPFailureInputs emits an ERROR-level event with every input needed to
// replay a failed compound BUMP build offline: raw STUMP bytes (hex) per subtree,
// subtree hashes, coinbase BUMP, block-header merkle root, the final compound
// BUMP bytes, and per-level offsets of the compound. Always emits regardless of
// configured log level — this fires only when validation fails, so it doesn't
// contribute to normal-path noise.
func dumpBUMPFailureInputs(
	logger *zap.Logger,
	stumps []*models.Stump,
	subtreeHashes []chainhash.Hash,
	coinbaseBUMP []byte,
	headerMerkleRoot *chainhash.Hash,
	compound *transaction.MerklePath,
	compoundBytes []byte,
	txids []string,
	validationErr error,
) {
	stumpDumps := make([]string, len(stumps))
	for i, s := range stumps {
		stumpDumps[i] = fmt.Sprintf("subtree=%d bytes=%d hex=%s",
			s.SubtreeIndex, len(s.StumpData), hex.EncodeToString(s.StumpData))
	}
	subtreeHex := make([]string, len(subtreeHashes))
	for i, h := range subtreeHashes {
		subtreeHex[i] = h.String()
	}
	levelDumps := make([]string, 0, len(compound.Path))
	for level, elems := range compound.Path {
		levelDumps = append(levelDumps,
			fmt.Sprintf("level=%d count=%d elems=[%s]", level, len(elems), formatPathElements(elems)))
	}

	var headerRootHex string
	if headerMerkleRoot != nil {
		headerRootHex = headerMerkleRoot.String()
	}

	logger.Error("compound BUMP validation failed — refusing to persist",
		zap.Error(validationErr),
		zap.String("header_merkle_root", headerRootHex),
		zap.Int("stump_count", len(stumps)),
		zap.Strings("stumps", stumpDumps),
		zap.Int("subtree_count", len(subtreeHashes)),
		zap.Strings("subtree_hashes", subtreeHex),
		zap.Int("coinbase_bump_bytes", len(coinbaseBUMP)),
		zap.String("coinbase_bump_hex", hex.EncodeToString(coinbaseBUMP)),
		zap.Int("compound_bytes", len(compoundBytes)),
		zap.String("compound_hex", hex.EncodeToString(compoundBytes)),
		zap.Int("compound_levels", len(compound.Path)),
		zap.Strings("compound_by_level", levelDumps),
		zap.Int("txid_count", len(txids)),
		zap.Strings("txids", txids),
	)
}

// formatPathElements renders a slice of PathElements as a human-readable string
// "offset=42 hash=abc… txid=true duplicate=false; offset=43 …".
func formatPathElements(elems []*transaction.PathElement) string {
	if len(elems) == 0 {
		return ""
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		var hashStr string
		if e.Hash != nil {
			hashStr = e.Hash.String()
		}
		txid := false
		if e.Txid != nil {
			txid = *e.Txid
		}
		dup := false
		if e.Duplicate != nil {
			dup = *e.Duplicate
		}
		parts = append(parts, fmt.Sprintf("offset=%d hash=%s txid=%v duplicate=%v", e.Offset, hashStr, txid, dup))
	}
	return strings.Join(parts, "; ")
}
