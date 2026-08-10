package bump_builder

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// ReconcilerLeaseName coordinates a single reconciler across bump-builder
// replicas via the standard store.Leaser primitive. The work is idempotent
// (re-anchor and revert both converge), so brief dual-holding during a
// failover window is redundant work, not corruption.
const ReconcilerLeaseName = "anchor-reconciler"

// Reconciler heals the transactions of orphaned blocks (issue #279): for
// every block_processing row with status='orphaned' and no reconciled_at,
// it re-anchors the txs still MINED against the orphan to the active-chain
// block that contains them (using the canonical block's already-stored
// compound BUMP — no external calls in the common case), reverts the rest
// to SEEN_ON_NETWORK, publishes corrected bulk status events, and stamps
// reconciled_at.
//
// Detection is elsewhere: the chaintracks_server tracker marks blocks
// orphaned from ReorgEvents and its tie-scan, and bump-builder's anchor
// guard marks the blocks it refuses. The reconciler is remediation only —
// plus its startup full-scan, the deep backstop that also detects orphans
// predating this code (the height-764 incident block on the scaling
// cluster heals through exactly that path).
type Reconciler struct {
	cfg         *config.Config
	logger      *zap.Logger
	store       store.Store
	publisher   events.Publisher
	chainHeader ChainHeaderReader
	merkle      *merkleservice.Client // nil ⇒ the /reprocess defer path is disabled
	leaser      store.Leaser          // nil ⇒ single-replica mode, no lease gating
	holderID    string

	// defers counts how many ticks each orphan has waited for its height's
	// canonical BUMP. In-memory by design: a restart resets the counts and
	// the worst case is a bounded number of duplicate /reprocess calls —
	// the same trade-off the watchdog makes with its attempt state.
	defers map[string]int

	now    func() time.Time
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReconciler wires the anchor reconciler. Returns nil (service skipped)
// when disabled by config or when no chain-header source exists — without
// chaintracks there is no canonicality oracle, which is the same condition
// under which the anchor guard and tie-scan are inert, so the process
// degrades to legacy behavior as one coherent unit.
func NewReconciler(
	cfg *config.Config,
	logger *zap.Logger,
	st store.Store,
	publisher events.Publisher,
	chainHeader ChainHeaderReader,
	merkle *merkleservice.Client,
	leaser store.Leaser,
) *Reconciler {
	if !cfg.BumpBuilder.Reconciler.Enabled {
		logger.Info("anchor reconciler disabled by config")
		return nil
	}
	if chainHeader == nil {
		logger.Info("anchor reconciler skipped: no chaintracks header source (enable chaintracks_server)")
		return nil
	}
	host, _ := os.Hostname()
	return &Reconciler{
		cfg:         cfg,
		logger:      logger.Named("anchor-reconciler"),
		store:       st,
		publisher:   publisher,
		chainHeader: chainHeader,
		merkle:      merkle,
		leaser:      leaser,
		holderID:    fmt.Sprintf("%s-%d", host, os.Getpid()),
		defers:      make(map[string]int),
		now:         time.Now,
		done:        make(chan struct{}),
	}
}

// Name implements services.Service.
func (r *Reconciler) Name() string { return "anchor-reconciler" }

// Start runs the reconcile loop until ctx is canceled. The optional startup
// full-scan runs once (lease-gated) before the first tick.
func (r *Reconciler) Start(ctx context.Context) error {
	ctx, r.cancel = context.WithCancel(ctx)
	defer close(r.done)

	interval := time.Duration(r.cfg.BumpBuilder.Reconciler.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 30 * time.Second
	}

	if r.cfg.BumpBuilder.Reconciler.StartupFullScan && r.acquireLease(ctx) {
		r.fullScan(ctx)
	}
	r.tick(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if r.leaser != nil {
				releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				_ = r.leaser.Release(releaseCtx, ReconcilerLeaseName, r.holderID)
				cancel()
			}
			return nil
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// Stop implements services.Service.
func (r *Reconciler) Stop() error {
	if r.cancel != nil {
		r.cancel()
	}
	<-r.done
	return nil
}

// acquireLease reports whether this replica currently leads. A nil leaser
// means single-replica mode (always lead); lease errors skip the tick.
func (r *Reconciler) acquireLease(ctx context.Context) bool {
	if r.leaser == nil {
		return true
	}
	interval := time.Duration(r.cfg.BumpBuilder.Reconciler.IntervalMs) * time.Millisecond
	ttl := 3 * interval
	if ttl < time.Minute {
		ttl = time.Minute
	}
	heldUntil, err := r.leaser.TryAcquireOrRenew(ctx, ReconcilerLeaseName, r.holderID, ttl)
	if err != nil {
		r.logger.Warn("lease check failed, skipping tick", zap.Error(err))
		return false
	}
	return !heldUntil.IsZero()
}

// tick consumes one batch of the orphaned-block queue.
func (r *Reconciler) tick(ctx context.Context) {
	if ctx.Err() != nil || !r.acquireLease(ctx) {
		return
	}
	blocksPerTick := r.cfg.BumpBuilder.Reconciler.BlocksPerTick
	if blocksPerTick <= 0 {
		blocksPerTick = 4
	}
	rows, err := r.store.ListOrphanedBlocksToReconcile(ctx, blocksPerTick)
	if err != nil {
		r.logger.Warn("failed to list orphaned blocks", zap.Error(err))
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		start := r.now()
		outcome := r.reconcileBlock(ctx, row)
		metrics.ReconcilerBlocksTotal.WithLabelValues(outcome).Inc()
		metrics.ReconcilerBlockDuration.Observe(time.Since(start).Seconds())
	}
}

// fullScan pages the entire block_processing table and orphan-marks every
// 'active' row whose height is provably held by a different block on the
// active chain. Unbounded horizon by design: this is what picks up orphans
// that predate the detection edges (guard, tie-scan, ReorgEvents) — e.g. an
// incident block far below the current tip. Marked rows are then consumed
// by the normal tick.
func (r *Reconciler) fullScan(ctx context.Context) {
	const page = 1000
	// A set, not a slice: the boundary-safe pager may visit a row more than
	// once at page boundaries (see store.ForEachBlockProcessing).
	markedSet := make(map[string]struct{})
	err := store.ForEachBlockProcessing(ctx, r.store, 0, page, func(row *models.BlockProcessingStatus) error {
		if row.Status != models.BlockStatusActive || row.BlockHeight == 0 || row.BlockHeight > math.MaxUint32 {
			return nil
		}
		active, hdrErr := r.chainHeader.GetHeaderByHeight(ctx, uint32(row.BlockHeight))
		if hdrErr != nil || active == nil {
			return nil //nolint:nilerr // fail-open by contract: never orphan on absence of evidence
		}
		if active.Hash.String() != row.BlockHash {
			markedSet[row.BlockHash] = struct{}{}
		}
		return nil
	})
	if err != nil {
		r.logger.Warn("startup full-scan: incomplete", zap.Error(err))
		if len(markedSet) == 0 {
			return
		}
		// Whatever was found before the failure still routes into healing.
	}
	if len(markedSet) == 0 {
		r.logger.Info("startup full-scan: no stale anchors found")
		return
	}
	marked := make([]string, 0, len(markedSet))
	for hash := range markedSet {
		marked = append(marked, hash)
	}
	if err := r.store.MarkBlocksOrphaned(ctx, marked, r.now()); err != nil {
		r.logger.Warn("startup full-scan: failed to mark orphaned", zap.Error(err))
		return
	}
	r.logger.Info("startup full-scan: marked off-chain blocks orphaned",
		zap.Strings("block_hashes", marked))
}

// reconcileBlock heals one orphaned block O and returns the metric outcome.
//
// Ordering matters: re-anchor runs BEFORE revert, so txs moved to a
// canonical block have left O's index by the time SetStatusByBlockHash
// computes the remainder. Crash-resume is idempotent for the same reason —
// reconciled_at is stamped last, and a re-run finds only whatever the crash
// left anchored to O.
func (r *Reconciler) reconcileBlock(ctx context.Context, row *models.BlockProcessingStatus) string {
	orphan := row.BlockHash
	logger := r.logger.With(logfields.BlockHash(orphan))

	// Resolve the orphan's height — the row may be a height-0 placeholder
	// created by MarkBlockProcessed before any header arrived; the stored
	// BUMP knows the height.
	height := row.BlockHeight
	if height == 0 {
		if h, _, err := r.store.GetBUMP(ctx, orphan); err == nil {
			height = h
		}
	}

	// Resurrection short-circuit: the block is the active-chain block at
	// its height again (flip-flop). Reset the row to active — the upsert
	// also clears orphaned_at/reconciled_at — and leave its txs alone; the
	// COMPETITOR is now the orphan and heals through its own row.
	if height > 0 && height <= math.MaxUint32 {
		if active, err := r.chainHeader.GetHeaderByHeight(ctx, uint32(height)); err == nil && active != nil &&
			active.Hash.String() == orphan {
			logger.Warn("orphan mark is stale — block is on the active chain; resetting to active")
			if err := r.store.UpsertBlockHeaderSeen(ctx, orphan, height, r.now()); err != nil {
				logger.Warn("failed to reset resurrected block", zap.Error(err))
				return "error"
			}
			delete(r.defers, orphan)
			return "resurrected"
		}
	}

	// The canonical block at O's height: re-mine its FULL BUMP set first.
	// This simultaneously (a) re-anchors O's txs that are in it, (b) mines
	// txs the anchor guard correctly refused while the canonical block was
	// still an alternate, and (c) is a no-op for rows already anchored
	// right (onlyChanged filters their events). Missing canonical BUMP ⇒
	// defer (optionally poking merkle-service /reprocess) up to the cap.
	batchSize := r.cfg.BumpBuilder.Reconciler.BatchSize
	if batchSize <= 0 {
		batchSize = maxTxIDsPerBulkEvent
	}
	reanchored := 0
	canonicalReady := false
	var canonicalHash string
	if height > 0 && height <= math.MaxUint32 {
		if active, err := r.chainHeader.GetHeaderByHeight(ctx, uint32(height)); err == nil && active != nil {
			canonicalHash = active.Hash.String()
			if n, ok := r.remineFromStoredBUMP(ctx, logger, canonicalHash, batchSize); ok {
				canonicalReady = true
				reanchored += n
			}
		}
	}
	if !canonicalReady && canonicalHash != "" {
		if r.deferForCanonicalBUMP(ctx, logger, orphan, canonicalHash) {
			return "deferred"
		}
		logger.Warn("defer cap reached without a canonical BUMP — falling back to revert-all "+
			"(SEEN_ON_NETWORK → MINED is lattice-legal; a late canonical redelivery re-mines them)",
			zap.String("canonical_block_hash", canonicalHash))
	}

	// Deep-reorg neighborhood: txs re-binned into LATER canonical blocks.
	// Only the txs still anchored to O are candidates; membership is an
	// O(1) lookup against each neighbor's stored compound BUMP.
	affected, err := r.store.GetTxIDsByBlockHash(ctx, orphan)
	if err != nil {
		logger.Warn("failed to read affected txids", zap.Error(err))
		return "error"
	}
	reanchored += r.reanchorNeighborhood(ctx, logger, affected, height, batchSize)

	// Revert the remainder: everything still anchored to O exists in no
	// canonical block we can prove — SEEN_ON_NETWORK puts them back in
	// flight (rebroadcast/propagation owns them from here), and the store
	// appends O to their orphaned-anchor history.
	reverted, err := r.store.SetStatusByBlockHash(ctx, orphan, models.StatusSeenOnNetwork)
	if err != nil {
		logger.Warn("failed to revert remaining txs", zap.Error(err))
		return "error"
	}
	r.publishReverted(ctx, logger, reverted)

	// Cleanup: STUMPs are per-subtree intermediates, safe to drop. The
	// compound BUMP is deliberately RETAINED — it serves the historical
	// orphanedProofs on GET /tx and is the store-local re-anchor fuel if
	// this block wins a later flip-flop.
	if err := r.store.DeleteStumpsByBlockHash(ctx, orphan); err != nil {
		logger.Warn("failed to clean up orphan STUMPs", zap.Error(err))
	}
	if err := r.store.MarkBlockReconciled(ctx, orphan, r.now()); err != nil {
		logger.Warn("failed to stamp reconciled_at", zap.Error(err))
		return "error"
	}
	delete(r.defers, orphan)

	metrics.ReconcilerTxsReanchoredTotal.Add(float64(reanchored))
	metrics.ReconcilerTxsRevertedTotal.Add(float64(len(reverted)))
	logger.Info(
		"orphaned block reconciled",
		logfields.BlockHeight(height),
		zap.String("canonical_block_hash", canonicalHash),
		zap.Int("txs_reanchored", reanchored),
		zap.Int("txs_reverted", len(reverted)),
	)
	switch {
	case reanchored > 0 && len(reverted) > 0:
		return "mixed"
	case reanchored > 0:
		return "reanchored"
	case len(reverted) > 0:
		return "reverted"
	default:
		return "empty"
	}
}

// deferForCanonicalBUMP handles the canonical-BUMP-missing branch: while
// under the defer cap it bumps the orphan's counter, optionally pokes
// merkle-service /reprocess for the canonical block, and reports true
// (caller returns "deferred" without stamping reconciled_at). At the cap it
// reports false so the caller falls back to revert-all.
func (r *Reconciler) deferForCanonicalBUMP(ctx context.Context, logger *zap.Logger, orphan, canonicalHash string) bool {
	maxDefer := r.cfg.BumpBuilder.Reconciler.MaxDeferAttempts
	if maxDefer <= 0 {
		maxDefer = 10
	}
	if r.defers[orphan] >= maxDefer {
		return false
	}
	r.defers[orphan]++
	logger.Info(
		"canonical block's BUMP not stored yet — deferring",
		zap.String("canonical_block_hash", canonicalHash),
		zap.Int("defer_attempt", r.defers[orphan]),
	)
	if r.merkle != nil && r.cfg.CallbackURL != "" {
		if err := r.merkle.Reprocess(ctx, canonicalHash, r.cfg.CallbackURL, r.cfg.CallbackToken); err != nil {
			logger.Warn("merkle-service /reprocess for canonical block failed", zap.Error(err))
		}
	}
	return true
}

// reanchorNeighborhood walks the heights above the orphan looking for
// canonical blocks whose stored BUMPs contain the still-affected txs (a
// deep reorg re-bins txs into later blocks) and re-anchors what it finds.
// Returns the number of rows moved. The affected slice shrinks as txs are
// claimed; whatever remains falls to the caller's revert.
func (r *Reconciler) reanchorNeighborhood(ctx context.Context, logger *zap.Logger, affected []string, height uint64, batchSize int) int {
	if len(affected) == 0 {
		return 0
	}
	// A placeholder orphan row with no resolvable height (height==0) has no
	// meaningful neighborhood above it — walking heights 1..depth is
	// nonsensical. Skip straight to the caller's revert.
	if height == 0 {
		return 0
	}
	depth := r.cfg.BumpBuilder.Reconciler.NeighborhoodDepth
	if depth < 0 {
		depth = 0
	}
	reanchored := 0
	for h := height + 1; h <= height+uint64(depth) && len(affected) > 0; h++ {
		if h > math.MaxUint32 {
			break
		}
		active, hdrErr := r.chainHeader.GetHeaderByHeight(ctx, uint32(h))
		if hdrErr != nil || active == nil {
			break // above the tip — nothing further to check
		}
		neighbor := active.Hash.String()
		_, bumpBytes, bumpErr := r.store.GetBUMP(ctx, neighbor)
		if bumpErr != nil || len(bumpBytes) == 0 {
			continue
		}
		idx, idxErr := bump.IndexCompound(bumpBytes)
		if idxErr != nil {
			continue
		}
		contained := make([]string, 0, len(affected))
		rest := make([]string, 0, len(affected))
		for _, txid := range affected {
			if idx.Contains(txid) {
				contained = append(contained, txid)
			} else {
				rest = append(rest, txid)
			}
		}
		for start := 0; start < len(contained); start += batchSize {
			end := min(start+batchSize, len(contained))
			reanchored += setMinedAndPublish(ctx, logger, r.store, r.publisher,
				neighbor, h, contained[start:end], models.ExtraInfoReorgReanchor, true)
		}
		affected = rest
	}
	return reanchored
}

// remineFromStoredBUMP re-mines every level-0 txid of blockHash's stored
// compound BUMP against it (onlyChanged — no duplicate events for rows
// already anchored right). ok=false when no usable BUMP is stored.
func (r *Reconciler) remineFromStoredBUMP(ctx context.Context, logger *zap.Logger, blockHash string, batchSize int) (changed int, ok bool) {
	bumpHeight, bumpBytes, err := r.store.GetBUMP(ctx, blockHash)
	if err != nil || len(bumpBytes) == 0 {
		return 0, false
	}
	txids, err := levelZeroTxidsFromBUMP(bumpBytes)
	if err != nil {
		logger.Warn("stored canonical BUMP failed to parse",
			zap.String("canonical_block_hash", blockHash), zap.Error(err))
		return 0, false
	}
	for start := 0; start < len(txids); start += batchSize {
		end := min(start+batchSize, len(txids))
		changed += setMinedAndPublish(ctx, logger, r.store, r.publisher,
			blockHash, bumpHeight, txids[start:end], models.ExtraInfoReorgReanchor, true)
	}
	return changed, true
}

// publishReverted fans out the bulk SEEN_ON_NETWORK correction events for a
// reorg revert, chunked like the MINED fan-out.
func (r *Reconciler) publishReverted(ctx context.Context, logger *zap.Logger, txids []string) {
	if len(txids) == 0 || r.publisher == nil {
		return
	}
	for start := 0; start < len(txids); start += maxTxIDsPerBulkEvent {
		end := min(start+maxTxIDsPerBulkEvent, len(txids))
		template := &models.TransactionStatus{
			Status:    models.StatusSeenOnNetwork,
			Timestamp: r.now(),
			TxIDs:     txids[start:end],
			ExtraInfo: models.ExtraInfoReorgUnmined,
		}
		if err := r.publisher.PublishBulk(ctx, template); err != nil {
			logger.Warn("failed to publish bulk reorg revert",
				zap.Int("chunk_start", start),
				zap.Error(err))
		}
	}
}
