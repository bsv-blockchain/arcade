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
	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/telemetry"
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
// refreshed from the shared store) used for block fetches.
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
	chainHeader ChainHeaderReader,
) *Builder {
	return &Builder{
		cfg:         cfg,
		logger:      logger.Named("bump-builder"),
		store:       st,
		producer:    producer,
		publisher:   publisher,
		teranode:    teranodeClient,
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

// headerWaitPollInterval is the pause between chaintracks header lookups
// while chainHeaderRootValidator waits out the ingestion race (see below).
// 1s keeps the wait responsive without hammering the chaintracks API.
const headerWaitPollInterval = time.Second

// chainHeaderRootValidator returns a validator that compares the datahub's
// header merkle root against the canonical merkle root from chaintracks. A
// mismatch means the datahub returned a block representation that doesn't
// belong to the canonical chain (pruned peer, stale cache, malicious peer),
// so the fetch loop should fall through to the next URL.
//
// chaintracks not knowing the header yet is a real race: its teranode P2P
// subscription is independent of the Kafka BLOCK_PROCESSED message that
// drives bump-builder, and for a block mined seconds earlier the Kafka path
// usually wins. The validator retries the lookup for up to
// bump_builder.header_wait_ms so the race resolves into a real cross-check
// instead of a silent skip; only when chaintracks still doesn't know the
// header after the wait does it soft-fail to "no validation" (post-build
// ValidateCompoundRoot still runs and catches any compound that doesn't
// reconcile — but that check uses the datahub's own root, so it cannot
// prove canonicality the way this one does).
func (b *Builder) chainHeaderRootValidator(ctx context.Context, blockHash string, logger *zap.Logger) bump.BlockDataValidator {
	if b.chainHeader == nil {
		return nil
	}
	hashObj, err := chainhash.NewHashFromHex(blockHash)
	if err != nil {
		logger.Warn("skipping canonical-root validation: invalid block hash", zap.Error(err))
		return nil
	}
	// One wall-clock budget for the whole block, shared by every per-URL
	// invocation of the validator: the race being waited out is chaintracks
	// ingesting THIS block, so the clock starts once. If chaintracks is
	// genuinely down, only the first fetch attempt pays the wait — later
	// URLs find the budget spent and degrade to a single lookup each.
	var headerWaitDeadline time.Time
	if wait := time.Duration(b.cfg.BumpBuilder.HeaderWaitMs) * time.Millisecond; wait > 0 {
		headerWaitDeadline = time.Now().Add(wait)
	}
	return func(_ []chainhash.Hash, fetchedRoot *chainhash.Hash) error {
		header, lookupErr := b.lookupHeaderWithWait(ctx, hashObj, headerWaitDeadline, logger)
		if lookupErr != nil || header == nil {
			// Soft-fail: log and accept the response. Post-build
			// ValidateCompoundRoot is still the final guard.
			if lookupErr != nil && !errors.Is(lookupErr, context.Canceled) {
				logger.Warn(
					"chaintracks header still unknown after wait; skipping canonical-root validation",
					logfields.BlockHash(blockHash),
					zap.Int("header_wait_ms", b.cfg.BumpBuilder.HeaderWaitMs),
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

// lookupHeaderWithWait fetches the block header from chaintracks, retrying
// until deadline while chaintracks catches up on a freshly-mined block (its
// P2P ingestion races the Kafka path that drives bump-builder). Returns the
// last lookup outcome once the deadline passes; a zero/past deadline
// degrades to a single lookup. Context cancellation aborts the wait
// immediately.
func (b *Builder) lookupHeaderWithWait(ctx context.Context, hashObj *chainhash.Hash, deadline time.Time, logger *zap.Logger) (*chaintrackslib.BlockHeader, error) {
	header, lookupErr := b.chainHeader.GetHeaderByHash(ctx, hashObj)
	if (lookupErr == nil && header != nil) || errors.Is(lookupErr, context.Canceled) {
		return header, lookupErr
	}
	if !time.Now().Before(deadline) {
		return header, lookupErr
	}

	logger.Debug("chaintracks header not yet known; waiting for ingestion",
		zap.Duration("budget", time.Until(deadline)))
	ticker := time.NewTicker(headerWaitPollInterval)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
		header, lookupErr = b.chainHeader.GetHeaderByHash(ctx, hashObj)
		if (lookupErr == nil && header != nil) || errors.Is(lookupErr, context.Canceled) {
			return header, lookupErr
		}
	}
	return header, lookupErr
}

// markMinedAndPublish moves the txids to MINED and fans the resulting status
// updates out to the events Publisher. blockHeight is required so each
// published status carries the block-height anchor that downstream SSE /
// webhook / BUMP-dedup consumers depend on (issue #87 / F-029). If a backend
// regresses and returns a status with BlockHeight == 0, the publish path
// repairs it from the compound BUMP's height before fanning out so a
// half-applied revert can never reintroduce the original bug.
func (b *Builder) markMinedAndPublish(ctx context.Context, logger *zap.Logger, blockHash string, blockHeight uint64, txids []string) {
	prevs, mined, err := b.store.SetMinedByTxIDs(ctx, blockHash, blockHeight, txids)
	if err != nil {
		logger.Error("failed to set mined status", zap.Error(err))
		return
	}
	logger.Info(
		"set transactions to MINED",
		zap.Int("count", len(mined)),
		logfields.BlockHeight(blockHeight),
	)
	metrics.BumpBuilderTxidsMinedTotal.Add(float64(len(mined)))
	// Observe the per-tx age of the previous status row so an operator can see
	// how long each tx sat at SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES before the
	// block landed. Pairs with arcade_bump_builder_build_duration_seconds for
	// the block-level latency (BLOCK_PROCESSED → BUMP-persisted).
	for i, prev := range prevs {
		if prev == nil || prev.Timestamp.IsZero() || i >= len(mined) {
			continue
		}
		metrics.StatusTransitionAge.
			WithLabelValues(string(prev.Status), string(models.StatusMined)).
			Observe(time.Since(prev.Timestamp).Seconds())
	}

	minedTxIDs := make([]string, len(mined))
	for i, st := range mined {
		minedTxIDs[i] = st.TxID
	}
	// Full-searchability MINED line(s): every txid appears in the log stream
	// (chunked, never capped) so a txid or block_hash search in Coralogix
	// finds the MINED transition regardless of block size. A 14k-tx block
	// produces ~14 lines at maxTxIDsPerLine=1000 — intended. Independent of
	// b.publisher wiring below, since this is the lifecycle log, not the
	// downstream fan-out.
	logfields.ForEachTxIDChunk(minedTxIDs, func(chunk []string, chunkIdx, totalChunks int) {
		logger.Info(
			"transactions mined",
			logfields.BlockHash(blockHash),
			logfields.BlockHeight(blockHeight),
			logfields.TxIDCount(len(minedTxIDs)),
			logfields.TxIDs(chunk),
			zap.Int("chunk_index", chunkIdx),
			zap.Int("chunk_total", totalChunks),
		)
	})

	if len(mined) == 0 || b.publisher == nil {
		return
	}
	// Coalesce the N-per-block MINED fan-out into bulk events. Without this, a
	// single BUMP build for a 14k-tx block produced 14k individual publish
	// calls, which overran the webhook service's 1024-cap work queue and
	// triggered ~185k drops/block. Subscribers (SSE, webhook) unfan from the
	// bulk template in their own handlers.
	//
	// Chunk the txids so one event never exceeds the Kafka producer's max
	// message size: a ~27k-tx block serialized to ~1.85 MB, over the 1 MiB
	// default Producer.MaxMessageBytes, so PublishBulk failed and the MINED
	// event was silently dropped for large blocks (the DB status was still
	// MINED, but SSE/webhook subscribers never saw it). Each txid is ~67 bytes
	// of JSON, so maxTxIDsPerBulkEvent keeps every event well under the limit.
	publishTxIDs := minedTxIDs
	for start := 0; start < len(publishTxIDs); start += maxTxIDsPerBulkEvent {
		end := start + maxTxIDsPerBulkEvent
		if end > len(publishTxIDs) {
			end = len(publishTxIDs)
		}
		template := &models.TransactionStatus{
			Status:      models.StatusMined,
			BlockHash:   blockHash,
			BlockHeight: blockHeight,
			Timestamp:   time.Now(),
			TxIDs:       publishTxIDs[start:end],
		}
		if pubErr := b.publisher.PublishBulk(ctx, template); pubErr != nil {
			logger.Warn(
				"failed to publish bulk MINED",
				logfields.BlockHash(blockHash),
				zap.Int("chunk_start", start),
				zap.Int("chunk_size", end-start),
				logfields.TxIDCount(len(publishTxIDs)),
				zap.Error(pubErr),
			)
		}
	}
}

// maxTxIDsPerBulkEvent caps how many txids ride in a single bulk MINED event.
// A txid is 64 hex chars (~67 bytes of JSON with quoting + comma); 5000 keeps a
// bulk event around 340 KB, comfortably under the 1 MiB default Kafka
// Producer.MaxMessageBytes even with the status envelope.
const maxTxIDsPerBulkEvent = 5000

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

	b.logger.Info(
		"bump builder started",
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
	rd := b.cfg.Watchdog.RecencyDepth
	if rd <= 0 {
		rd = 144
	}
	depth := uint64(rd)

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
			b.logger.Warn(
				"orphan-stump prune: list failed",
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
				b.logger.Warn(
					"orphan-stump prune: delete failed",
					logfields.BlockHash(r.BlockHash),
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
	b.logger.Info(
		"orphan-stump prune complete",
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

	logger := telemetry.LoggerWith(ctx, b.logger.With(logfields.BlockHash(blockHash)))

	// Short-circuit: if a compound BUMP already exists for this block, skip
	// the datahub fetch + recompute path entirely. See tryShortCircuit for
	// the full contract.
	if b.tryShortCircuit(ctx, logger, blockHash) {
		outcome = "short_circuited"
		return nil
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

	// Completeness gate. merkle-service (PR #162) tells us exactly which subtree
	// indices produced a STUMP for this block. If any are still missing once the
	// grace window has elapsed, leave the block un-finalized: do NOT stamp
	// processed_at, so ListStaleBlockProcessingStatus surfaces it and the
	// watchdog re-drives it via /reprocess (which re-emits the missing STUMPs and
	// BLOCK_PROCESSED). Returning nil — not an error — because a Kafka requeue
	// would just replay against the same gap; only /reprocess can fill it, and
	// the durable processed_at IS NULL is the recovery signal. An empty/absent
	// expected set (pre-#162 merkle, or a block with no tracked txs) yields no
	// missing indices and falls through to the existing behavior below.
	if missing := missingStumpIndices(callback.ExpectedSubtreeIndices, stumps); len(missing) > 0 {
		outcome = "incomplete_stumps"
		metrics.BumpBuilderIncompleteStumpsTotal.Inc()
		logger.Error(
			"BLOCK_PROCESSED is missing expected STUMPs — deferring finalization so the watchdog can recover via /reprocess",
			zap.Ints("missing_subtree_indices", missing),
			zap.Int("expected_stumps", len(callback.ExpectedSubtreeIndices)),
			zap.Int("received_stumps", len(stumps)),
		)
		return nil
	}

	if len(stumps) == 0 {
		outcome = "no_stumps"
		metrics.BumpBuilderEmptyStumpBlocksTotal.Inc()
		// Warn — not Info — because the legitimate case (block contains no
		// tracked txs) is indistinguishable from the silent-drop case
		// (STUMP callbacks were dedup'd / DLQ'd / lost upstream). Operators
		// need to see this rate in logs; a sustained stream while watched
		// txs are in flight is the signal that merkle-service callbacks
		// aren't landing.
		//
		// Reaching here means the expected set was empty (a non-empty set with
		// zero received STUMPs would have been caught by the completeness gate
		// above), so this block has no tracked txs and IS finalized — stamp
		// processed_at so the watchdog doesn't keep re-driving a complete block.
		// blockHeight is unknown on this path (no compound built); pass 0, which
		// the upsert leaves the chaintracks-supplied height intact on conflict.
		logger.Warn("BLOCK_PROCESSED arrived with zero STUMPs for this block — either no tracked txs in this block, or upstream STUMP callbacks were dropped (check merkle-service callback delivery)")
		b.markBlockProcessed(ctx, logger, blockHash, 0)
		return nil
	}

	logger.Info("building compound BUMP", zap.Int("stump_count", len(stumps)))
	logStumpInputs(logger, stumps)

	// 2. Obtain subtree hashes + coinbase BUMP + header merkle root. Prefer the
	// enrichment fields merkle-service attaches to BLOCK_PROCESSED (issue #195):
	// when present they are authoritative and let us build the compound BUMP
	// with ZERO datahub calls, closing the pruned/poisoned-datahub failure
	// class. Otherwise fall back to fetching the block from a datahub.
	//
	// minSubtrees = max(stump.SubtreeIndex)+1 is the floor any valid block
	// representation must satisfy. It validates the datahub response (a peer
	// claiming fewer subtrees is provably wrong) and likewise rejects a callback
	// whose subtree list can't index every STUMP we already hold.
	minSubtrees := 0
	for _, s := range stumps {
		if s.SubtreeIndex+1 > minSubtrees {
			minSubtrees = s.SubtreeIndex + 1
		}
	}

	subtreeHashes, coinbaseBUMP, headerMerkleRoot, ok := callbackBlockData(&callback, minSubtrees, logger)
	if ok {
		metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("callback").Inc()
		logger.Info(
			"using merkle-service callback block data; skipping datahub fetch",
			zap.Int("subtree_count", len(subtreeHashes)),
		)
	} else {
		var fetchErr error
		subtreeHashes, coinbaseBUMP, headerMerkleRoot, fetchErr = b.fetchBlockDataFromDatahub(ctx, blockHash, minSubtrees, logger)
		if fetchErr != nil {
			outcome = "fetch_failed"
			return fmt.Errorf("fetching block data: %w", fetchErr)
		}
		metrics.BumpBuilderBlockDataSourceTotal.WithLabelValues("datahub").Inc()
	}

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

	// Finalize: the BUMP is built and validated and every expected STUMP was
	// present, so stamp processed_at. This (not the HTTP handler) is now the
	// sole owner of the stamp — see handleBlockProcessed. A soft failure here is
	// self-healing: processed_at stays NULL, the watchdog re-drives the block,
	// and tryShortCircuit re-stamps it on the redelivered BLOCK_PROCESSED.
	b.markBlockProcessed(ctx, logger, blockHash, blockHeight)

	// 6. Set tracked transactions to MINED.
	// blockHeight is threaded through here (and asserted on the returned
	// statuses below) because downstream SSE/webhook consumers and the
	// dedup path in BUMP-build rely on the height to anchor each MINED
	// status to a specific block — a zero/missing height triggered F-029.
	//
	// BuildCompoundBUMP returns every level-0 hash in the compound; hand the
	// full list to SetMinedByTxIDs and let the store filter by row existence
	// (its UPDATE … WHERE txid IN (…) RETURNING semantics guarantee unknown
	// txids no-op and only actual transitions come back in `mined`). The
	// previous in-memory pre-filter against TxTracker silently dropped txs
	// submitted to api-server after bump-builder's per-pod tracker hydration
	// in microservice mode.
	if len(txids) > 0 {
		b.markMinedAndPublish(ctx, logger, blockHash, blockHeight, txids)
	}

	// 7. Prune STUMPs
	if err := b.store.DeleteStumpsByBlockHash(ctx, blockHash); err != nil {
		logger.Warn("failed to clean up STUMPs", zap.Error(err))
	}

	logger.Info(
		"BUMP built successfully",
		zap.Int("level0_count", len(txids)),
		zap.Int("stumps_pruned", len(stumps)),
	)
	return nil
}

// fetchBlockDataFromDatahub is the datahub fallback for block inputs: used when
// a BLOCK_PROCESSED callback carries no usable enrichment (older merkle-service,
// or fields that failed callbackBlockData's consistency checks). It pulls the
// live URL list from the shared teranode.Client — so p2p-discovered URLs are
// included alongside statically configured ones — falling back to the full set
// when every endpoint is currently sidelined by the circuit breaker (better to
// retry a sidelined URL than fail with zero attempts).
//
// minSubtrees = max(stump.SubtreeIndex)+1 builds a response validator that
// rejects any datahub claiming fewer subtrees than the STUMPs we already hold —
// without it, a pruned/lying peer's "200 OK" with a truncated subtree list
// poisons every retry until the message DLQs.
func (b *Builder) fetchBlockDataFromDatahub(ctx context.Context, blockHash string, minSubtrees int, logger *zap.Logger) ([]chainhash.Hash, []byte, *chainhash.Hash, error) {
	endpoints := b.teranode.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		endpoints = b.teranode.GetEndpoints()
	}
	validator := bump.SubtreeCountValidator(minSubtrees)
	if b.chainHeader != nil {
		validator = combineValidators(validator, b.chainHeaderRootValidator(ctx, blockHash, logger))
	}

	logger.Debug(
		"callback missing enrichment fields; fetching block data from datahub",
		zap.Strings("datahub_urls", endpoints),
		zap.Int("min_subtrees", minSubtrees),
	)
	fetchStart := time.Now()
	subtreeHashes, coinbaseBUMP, headerMerkleRoot, err := bump.FetchBlockDataForBUMPWithOptions(ctx, endpoints, blockHash, b.cfg.BumpBuilder.DataHubMaxBlockBytes, validator, logger)
	metrics.BumpBuilderDatahubFetchDuration.Observe(time.Since(fetchStart).Seconds())
	if err != nil {
		return nil, nil, nil, err
	}
	logger.Debug(
		"datahub fetch succeeded",
		zap.Int("subtree_count", len(subtreeHashes)),
		zap.Bool("has_coinbase_bump", coinbaseBUMP != nil),
		zap.Bool("has_header_merkle_root", headerMerkleRoot != nil),
	)
	return subtreeHashes, coinbaseBUMP, headerMerkleRoot, nil
}

// callbackBlockData extracts the datahub-independent block inputs from a
// BLOCK_PROCESSED callback enriched by merkle-service (issue #195). It returns
// ok=false — signalling the caller to fall back to the datahub fetch — when the
// enrichment is absent or fails a consistency check. It never returns an error:
// a missing or malformed enrichment must degrade to the existing datahub path,
// not fail the block.
//
// merkleRoot and subtreeHashes arrive as display-order hex (same convention as
// blockHash) and are decoded with chainhash.NewHashFromHex, which reverses
// display->internal order to match the chainhash.Hash values the datahub binary
// parser produces (it uses chainhash.NewHash on raw, already-internal bytes).
// coinbaseBump is the hex of the raw BRC-74 bytes and is used as-is — the same
// []byte BuildCompoundBUMP / NewMerklePathFromBinary expect.
//
// All three of merkleRoot, subtreeHashes and coinbaseBump are required:
// merkleRoot to validate the assembled compound against, subtreeHashes to seed
// the block-level tree, and coinbaseBump to correct subtreeHashes[0] (the
// canonical subtree-0 root is computed against the coinbase placeholder, so
// without the coinbase path the assembled root never matches the header root).
//
// minSubtrees is max(stump.SubtreeIndex)+1: a callback whose subtree list can't
// index every STUMP we already hold is provably inconsistent, so we reject it
// and let the datahub path (with its own validators) try instead.
func callbackBlockData(callback *models.CallbackMessage, minSubtrees int, logger *zap.Logger) (subtreeHashes []chainhash.Hash, coinbaseBUMP []byte, headerMerkleRoot *chainhash.Hash, ok bool) {
	if callback.MerkleRoot == "" || len(callback.SubtreeHashes) == 0 || len(callback.CoinbaseBUMP) == 0 {
		return nil, nil, nil, false
	}

	if callback.SubtreeCount != 0 && callback.SubtreeCount != len(callback.SubtreeHashes) {
		logger.Warn(
			"callback subtreeCount disagrees with subtreeHashes length; falling back to datahub",
			zap.Int("subtree_count", callback.SubtreeCount),
			zap.Int("subtree_hashes", len(callback.SubtreeHashes)),
		)
		return nil, nil, nil, false
	}

	if len(callback.SubtreeHashes) < minSubtrees {
		logger.Warn(
			"callback subtreeHashes can't index every held STUMP; falling back to datahub",
			zap.Int("subtree_hashes", len(callback.SubtreeHashes)),
			zap.Int("min_subtrees", minSubtrees),
		)
		return nil, nil, nil, false
	}

	root, err := chainhash.NewHashFromHex(callback.MerkleRoot)
	if err != nil {
		logger.Warn("callback merkleRoot is not valid hex; falling back to datahub", zap.Error(err))
		return nil, nil, nil, false
	}

	hashes := make([]chainhash.Hash, len(callback.SubtreeHashes))
	for i, h := range callback.SubtreeHashes {
		sh, hErr := chainhash.NewHashFromHex(h)
		if hErr != nil {
			logger.Warn(
				"callback subtreeHash is not valid hex; falling back to datahub",
				zap.Int("index", i), zap.Error(hErr),
			)
			return nil, nil, nil, false
		}
		hashes[i] = *sh
	}

	return hashes, []byte(callback.CoinbaseBUMP), root, true
}

// tryShortCircuit attempts the BUMP-already-exists redelivery path. Returns
// true when the short-circuit handled the message and the caller should
// treat it as done. Returns false when no usable BUMP exists and the caller
// should fall through to the normal rebuild.
//
// Errors at the store-read step (not-found, etc.) are intentionally
// swallowed — they're indistinguishable from "no BUMP" and we want the
// rebuild path to handle both. A parseErr on a corrupt stored BUMP is
// logged so operators can see the divergence; the rebuild still proceeds.
//
// Extracted from processBlockProcessed to reduce nesting depth (nestif).
// The short-circuit is exercised on /reprocess redeliveries of
// BLOCK_PROCESSED — typical case is the watchdog re-firing after a missed
// callback. The SetMinedByTxIDs UPDATE is idempotent so repeated calls are
// safe; the value is letting a tx registered after the original build still
// get marked MINED on the redelivery.
func (b *Builder) tryShortCircuit(ctx context.Context, logger *zap.Logger, blockHash string) bool {
	existingHeight, bumpBytes, getErr := b.store.GetBUMP(ctx, blockHash)
	if getErr != nil {
		// not-found / transient store error — fall through to rebuild
		return false
	}
	if len(bumpBytes) == 0 {
		return false
	}
	txids, parseErr := levelZeroTxidsFromBUMP(bumpBytes)
	if parseErr != nil {
		// Stored BUMP failed to decode — fall through to the normal rebuild
		// path so an upstream corruption doesn't pin a block in a broken
		// state forever.
		logger.Warn("stored BUMP failed to parse on redelivery — rebuilding", zap.Error(parseErr))
		return false
	}
	metrics.BumpBuilderShortCircuitTotal.Inc()
	logger.Info(
		"BUMP already built — skipping datahub fetch on redelivery",
		zap.Int("level0_count", len(txids)),
		logfields.BlockHeight(existingHeight),
	)
	if len(txids) > 0 {
		b.markMinedAndPublish(ctx, logger, blockHash, existingHeight, txids)
	}
	// Stamp processed_at on the redelivery too. The typical short-circuit
	// trigger IS the watchdog re-firing BLOCK_PROCESSED for a block whose BUMP
	// was built but whose processed_at never got stamped (e.g. a crash between
	// InsertBUMP and the stamp, or a soft stamp failure on the original build).
	// Without re-stamping here that block would loop in the watchdog forever,
	// since the stale query keys on processed_at IS NULL and a built block can
	// never be re-detected as incomplete.
	b.markBlockProcessed(ctx, logger, blockHash, existingHeight)
	// STUMP rows for this block should already have been pruned at the end
	// of the original build; ensure stragglers are cleared in case a STUMP
	// arrived after pruning ran.
	if delErr := b.store.DeleteStumpsByBlockHash(ctx, blockHash); delErr != nil {
		logger.Warn("failed to clean up STUMPs on short-circuit", zap.Error(delErr))
	}
	return true
}

// markBlockProcessed stamps processed_at on the block-processing row — the
// signal that arcade considers the block fully finalized (all expected STUMPs
// present, BUMP built and validated, or a legitimate no-tracked-tx block). It
// is the single chokepoint that moved out of the HTTP handler so that a block
// stays recoverable (processed_at NULL) until finalization genuinely completes.
//
// Failure is soft by design: leaving processed_at NULL keeps the block in the
// watchdog's stale scan, which re-drives it via /reprocess and re-stamps on the
// short-circuit path — so a transient store error self-heals rather than
// wedging the block. blockHeight may be 0 on the no-STUMP finalize path; the
// MarkBlockProcessed upsert preserves any chaintracks-supplied height on
// conflict and only writes processed_at.
func (b *Builder) markBlockProcessed(ctx context.Context, logger *zap.Logger, blockHash string, blockHeight uint64) {
	if err := b.store.MarkBlockProcessed(ctx, blockHash, blockHeight, time.Now()); err != nil {
		// Include the block identity: a sustained stream of these is the signal
		// that finalized blocks are looping through the watchdog (processed_at
		// never lands), which an aggregator can only group by block hash.
		logger.Warn(
			"failed to record block_processed status; block will re-drive via watchdog until the stamp lands",
			logfields.BlockHash(blockHash),
			logfields.BlockHeight(blockHeight),
			zap.Error(err),
		)
	}
}

// missingStumpIndices returns the subtree indices merkle-service expected to
// produce a STUMP for this block (callback.ExpectedSubtreeIndices) that are NOT
// present among the STUMPs arcade actually stored, preserving the ascending
// order merkle promises. An empty expected set — pre-#162 merkle, or a block
// with no tracked txs — yields no missing indices, so the caller finalizes
// normally. Comparison is by set membership, never count: a duplicate or extra
// STUMP must never be able to mask a genuinely missing one.
func missingStumpIndices(expected []int, stumps []*models.Stump) []int {
	if len(expected) == 0 {
		return nil
	}
	received := make(map[int]struct{}, len(stumps))
	for _, s := range stumps {
		received[s.SubtreeIndex] = struct{}{}
	}
	var missing []int
	for _, idx := range expected {
		if _, ok := received[idx]; !ok {
			missing = append(missing, idx)
		}
	}
	return missing
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
		logger.Debug(
			"stump input",
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
	logger.Debug(
		"block inputs",
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
		logger.Debug(
			"coinbase bump",
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
			logger.Debug(
				"per-stump assembly failed",
				zap.Int("subtree_index", s.SubtreeIndex),
				zap.Error(err),
			)
			continue
		}
		logger.Debug(
			"per-stump assembly",
			zap.Int("subtree_index", s.SubtreeIndex),
			zap.Uint32("block_height", full.BlockHeight),
			zap.Int("levels", len(full.Path)),
			zap.String("full_bump_hex", hex.EncodeToString(full.Bytes())),
		)
		for level, elems := range full.Path {
			logger.Debug(
				"per-stump level",
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
	logger.Debug(
		"compound bump",
		zap.Uint32("block_height", compound.BlockHeight),
		zap.Int("levels", len(compound.Path)),
		zap.Int("bytes", len(bumpBytes)),
		zap.String("hex", hex.EncodeToString(bumpBytes)),
		logfields.TxIDCount(len(txids)),
		logfields.TxIDs(txids),
	)
	for level, elems := range compound.Path {
		logger.Debug(
			"compound bump level",
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

	logger.Error(
		"compound BUMP validation failed — refusing to persist",
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
		logfields.TxIDCount(len(txids)),
		logfields.TxIDs(txids),
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
