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

// defaultFullScanDepth bounds the startup full-scan when
// Reconciler.FullScanDepth is unset: only rows within this many heights of
// the active tip are considered. 144 ≈ one day of BSV blocks — the same
// recency window the watchdog uses — and stops the deep backstop from
// re-orphaning a long chain's entire history oldest-first (issue #282).
const defaultFullScanDepth = 144

// defaultFullScanChaintracksReadyTimeout bounds the initial wait for the
// chain-header source to sync up to the store's active tip before the startup
// full-scan runs, when Reconciler.FullScanChaintracksReadyTimeoutMs is unset.
// The embedded chaintracks resyncs from genesis on every deploy; two minutes
// covers a typical catch-up while never blocking startup indefinitely — a
// slower resync self-heals through the tick loop instead.
const defaultFullScanChaintracksReadyTimeout = 2 * time.Minute

// Reconciler heals the transactions of orphaned blocks (issue #279): for
// every block_processing row with status='orphaned' and no reconciled_at,
// it re-anchors the txs still MINED against the orphan to the active-chain
// block that contains them (using the canonical block's already-stored
// compound BUMP — no external calls in the common case), reverts the txs
// proven to be in no canonical block to SEEN_ON_NETWORK, publishes corrected
// bulk status events, and stamps reconciled_at. When the canonical block's
// BUMP is unavailable it PARKS the block instead of reverting (issue #282):
// the txs stay MINED against the orphan until a later canonical BUMP can
// re-anchor them — un-mining a tx that is very likely in the not-yet-fetched
// canonical block would be a downgrade, not a repair.
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

	// startupScanDone flips true only after a startup full-scan that ran while
	// the chain-header source was synced to the store's active tip. Until then
	// the tick loop keeps re-attempting it (lease-gated) so the deep backstop
	// eventually runs once even if chaintracks was mid-resync at process start
	// and the initial readiness wait timed out. In-memory by design: a restart
	// re-arms the one-shot, which is idempotent (fullScan only marks, and marks
	// are convergent).
	startupScanDone bool

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
// full-scan runs once (lease-gated), but only after the chain-header source
// has synced up to the store's active tip — see waitForChaintracksReady. If it
// is still catching up when the bounded wait elapses, the scan is deferred to a
// later tick rather than run against a header source that would fail open on
// every row (issue #279 follow-up).
func (r *Reconciler) Start(ctx context.Context) error {
	ctx, r.cancel = context.WithCancel(ctx)
	defer close(r.done)

	interval := time.Duration(r.cfg.BumpBuilder.Reconciler.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 30 * time.Second
	}

	if r.cfg.BumpBuilder.Reconciler.StartupFullScan && r.acquireLease(ctx) {
		if r.waitForChaintracksReady(ctx) {
			r.fullScan(ctx)
			r.startupScanDone = true
		} else {
			r.logger.Warn("startup full-scan: chain-header source not ready within timeout; "+
				"deferring to a later tick (self-heals once chaintracks catches up to the active tip)",
				zap.Int("timeout_ms", r.cfg.BumpBuilder.Reconciler.FullScanChaintracksReadyTimeoutMs))
		}
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
	// Self-heal the startup full-scan: if it was deferred because the embedded
	// chaintracks was still resyncing from genesis at process start (its
	// storage is ephemeral, so every deploy wipes headers), run it now that the
	// lease is held and chaintracks has caught up to the active tip. Guarded by
	// startupScanDone so it runs at most once successfully; short-circuits
	// before the readiness probe once done or when the scan is disabled, so
	// steady-state ticks pay nothing.
	if r.cfg.BumpBuilder.Reconciler.StartupFullScan && !r.startupScanDone && r.chaintracksReady(ctx) {
		r.fullScan(ctx)
		r.startupScanDone = true
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

// fullScan pages the block_processing table and re-judges every 'active'
// and 'orphaned' row within the scan bounds against the active chain:
// 'active' rows whose height is provably held by a different block are
// orphan-marked (then consumed by the normal tick), and 'orphaned' rows that
// ARE the active-chain block at their height are reset to active — including
// rows already stamped reconciled, which the tick's queue can never revisit
// (issue #339) — and re-mined from their retained compound BUMP so the heal
// does not depend on a competitor's row existing. It is the deep backstop
// that picks up transitions predating the detection edges (guard, tie-scan,
// ReorgEvents) and the operator lever for an old incident.
//
// The scan is BOUNDED (issue #282): rather than re-judging the entire
// history oldest-first (which on a long chain grinds through hundreds of
// historical competition losers, starving the actual recent incident), it
// considers only heights within FullScanDepth of the active tip — or an
// explicit FullScan{Min,Max}Height range for operator-targeted recovery.
func (r *Reconciler) fullScan(ctx context.Context) {
	const page = 1000
	minHeight, maxHeight, mode := r.fullScanBounds(ctx)
	r.logger.Info("startup full-scan: scanning",
		zap.String("bound_mode", mode),
		zap.Uint64("min_height", minHeight),
		zap.Uint64("max_height", maxHeight))
	// Sets, not slices: the boundary-safe pager may visit a row more than
	// once at page boundaries (see store.ForEachBlockProcessing); the first
	// judgement of a hash wins. Parked rows belong to the watchdog and are
	// never judged here.
	markedSet := make(map[string]struct{})
	resurrectSet := make(map[string]uint64)
	err := store.ForEachBlockProcessing(ctx, r.store, minHeight, page, func(row *models.BlockProcessingStatus) error {
		if (row.Status != models.BlockStatusActive && row.Status != models.BlockStatusOrphaned) ||
			row.BlockHeight == 0 || row.BlockHeight > math.MaxUint32 {
			return nil
		}
		if maxHeight > 0 && row.BlockHeight > maxHeight {
			return nil // above the targeted range's upper bound
		}
		if _, seen := markedSet[row.BlockHash]; seen {
			return nil
		}
		if _, seen := resurrectSet[row.BlockHash]; seen {
			return nil
		}
		active, hdrErr := r.chainHeader.GetHeaderByHeight(ctx, uint32(row.BlockHeight))
		if hdrErr != nil || active == nil {
			return nil //nolint:nilerr // fail-open by contract: never judge on absence of evidence
		}
		matches := active.Hash.String() == row.BlockHash
		switch {
		case row.Status == models.BlockStatusActive && !matches:
			markedSet[row.BlockHash] = struct{}{}
		case row.Status == models.BlockStatusOrphaned && matches:
			resurrectSet[row.BlockHash] = row.BlockHeight
		}
		return nil
	})
	if err != nil {
		r.logger.Warn("startup full-scan: incomplete", zap.Error(err))
		// Whatever was found before the failure still routes into healing.
	}
	if len(markedSet) == 0 && len(resurrectSet) == 0 {
		r.logger.Info("startup full-scan: no stale anchors found")
		return
	}
	r.fullScanMarkOrphaned(ctx, markedSet)
	r.fullScanResurrect(ctx, resurrectSet)
}

func (r *Reconciler) fullScanMarkOrphaned(ctx context.Context, set map[string]struct{}) {
	if len(set) == 0 {
		return
	}
	marked := make([]string, 0, len(set))
	for hash := range set {
		marked = append(marked, hash)
	}
	if err := r.store.MarkBlocksOrphaned(ctx, marked, r.now()); err != nil {
		r.logger.Warn("startup full-scan: failed to mark orphaned", zap.Error(err))
		return
	}
	metrics.BlockStatusTransitionsTotal.
		WithLabelValues(metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceFullScan).
		Add(float64(len(marked)))
	r.logger.Info("startup full-scan: marked off-chain blocks orphaned",
		zap.Strings("block_hashes", marked))
}

// fullScanResurrect re-mines each resurrected block's txs from its retained
// compound BUMP — the same fuel reconcileBlock burns for a canonical block,
// with onlyChanged so rows already anchored right produce no events — and
// then resets the row to active (the header-seen upsert also clears
// orphaned_at/reconciled_at and preserves the milestone timestamps). The
// re-mine runs FIRST: a failed batch leaves the row orphaned so the next
// full-scan retries the whole heal, whereas reactivating first would leave
// the txs with nothing left to revisit them.
func (r *Reconciler) fullScanResurrect(ctx context.Context, set map[string]uint64) {
	if len(set) == 0 {
		return
	}
	batchSize := r.cfg.BumpBuilder.Reconciler.BatchSize
	if batchSize <= 0 {
		batchSize = maxTxIDsPerBulkEvent
	}
	resurrected := make([]string, 0, len(set))
	for hash, height := range set {
		logger := r.logger.With(logfields.BlockHash(hash), logfields.BlockHeight(height))
		n, ok, remineErr := r.remineFromStoredBUMP(ctx, logger, hash, batchSize)
		if remineErr != nil {
			logger.Error("startup full-scan: re-mine from the resurrected block's BUMP failed; "+
				"leaving the row orphaned so the next full-scan retries", zap.Error(remineErr))
			continue
		}
		if err := r.store.UpsertBlockHeaderSeen(ctx, hash, height, r.now()); err != nil {
			logger.Warn("startup full-scan: failed to reactivate resurrected block", zap.Error(err))
			continue
		}
		delete(r.defers, hash)
		metrics.BlockStatusTransitionsTotal.
			WithLabelValues(metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceFullScan).
			Inc()
		resurrected = append(resurrected, hash)
		if !ok {
			logger.Warn("startup full-scan: resurrected block has no stored compound BUMP; " +
				"its txs heal through the competitor's orphan row or a BLOCK_PROCESSED redelivery")
			continue
		}
		metrics.ReconcilerTxsReanchoredTotal.Add(float64(n))
		if n > 0 {
			logger.Info("startup full-scan: re-mined txs against resurrected block", zap.Int("txs_reanchored", n))
		}
	}
	if len(resurrected) > 0 {
		r.logger.Warn("startup full-scan: reactivated resurrected canonical blocks that were marked orphaned",
			zap.Strings("block_hashes", resurrected))
	}
}

// fullScanBounds resolves the height window the startup full-scan considers.
// An explicit FullScan{Min,Max}Height target (either > 0) wins — operator-
// pointed recovery, e.g. a known old incident. Otherwise the scan is bounded
// to within FullScanDepth of the active tip (store.GetActiveTipBlockHeight).
// Returns (0, 0) — unbounded, the pre-#282 behavior — when the tip is unknown
// or the chain is shorter than the horizon (a short table is cheap to sweep
// whole); this keeps the fail-open contract so a missing tip never hides an
// incident. maxHeight 0 means "no upper bound".
func (r *Reconciler) fullScanBounds(ctx context.Context) (minHeight, maxHeight uint64, mode string) {
	rc := r.cfg.BumpBuilder.Reconciler
	if rc.FullScanMinHeight > 0 || rc.FullScanMaxHeight > 0 {
		return rc.FullScanMinHeight, rc.FullScanMaxHeight, "targeted"
	}
	depth := rc.FullScanDepth
	if depth <= 0 {
		depth = defaultFullScanDepth
	}
	tip, err := r.store.GetActiveTipBlockHeight(ctx)
	if err != nil {
		r.logger.Warn("startup full-scan: active-tip lookup failed; scanning unbounded", zap.Error(err))
		return 0, 0, "unbounded"
	}
	if tip == 0 || uint64(depth) >= tip {
		return 0, 0, "unbounded"
	}
	return tip - uint64(depth), 0, "horizon"
}

// chaintracksReady reports whether the chain-header source has caught up to the
// store's active tip — the precondition for a MEANINGFUL startup full-scan. In
// the bump-builder deployment r.chainHeader is an embedded chaintracks with
// ephemeral storage that resyncs from genesis on every deploy; until it reaches
// the tip, GetHeaderByHeight returns nil for every height and the scan would
// fail open on every row (marking nothing) — silently breaking reorg recovery.
//
// Readiness is TRIVIALLY satisfied when the active tip is unknown/zero (nothing
// to compare against — preserves the pre-gate behavior on an empty table) or
// when the tip lookup fails (the scan itself still fails open per-row, so a
// store hiccup never blocks it forever). Otherwise chaintracks is ready exactly
// when it can return a non-nil, non-error header at the tip height.
func (r *Reconciler) chaintracksReady(ctx context.Context) bool {
	tip, err := r.store.GetActiveTipBlockHeight(ctx)
	if err != nil {
		// A canceled/expired context is shutdown, not evidence — report
		// not-ready so a cancellation mid-wait never masquerades as "ready"
		// and triggers a scan on the way down. A genuine store error still
		// fails open (treat as ready; the scan itself fails open per-row).
		if ctx.Err() != nil {
			return false
		}
		r.logger.Warn("chaintracks readiness: active-tip lookup failed; treating as ready", zap.Error(err))
		return true
	}
	if tip == 0 || tip > math.MaxUint32 {
		return true
	}
	hdr, err := r.chainHeader.GetHeaderByHeight(ctx, uint32(tip))
	return err == nil && hdr != nil
}

// waitForChaintracksReady blocks — with bounded exponential backoff — until
// chaintracksReady reports true or the configured timeout elapses, whichever
// comes first. Returns true if chaintracks became ready, false on timeout or
// context cancellation. It NEVER blocks indefinitely: a false return is the
// caller's signal to defer the startup scan to the tick-loop self-heal rather
// than run it against a not-yet-synced header source.
func (r *Reconciler) waitForChaintracksReady(ctx context.Context) bool {
	timeout := time.Duration(r.cfg.BumpBuilder.Reconciler.FullScanChaintracksReadyTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultFullScanChaintracksReadyTimeout
	}
	const (
		initialBackoff = 250 * time.Millisecond
		maxBackoff     = 5 * time.Second
	)
	deadline := time.Now().Add(timeout)
	backoff := initialBackoff
	for {
		if ctx.Err() != nil {
			return false
		}
		if r.chaintracksReady(ctx) {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		wait := backoff
		if wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
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
	// The orphan generation this pass reconciles. The final stamp is a CAS on
	// it, so a row the block-status tracker reactivated — or that was
	// orphaned again — while this pass ran is never marked reconciled by
	// stale work (issue #339).
	var orphanedAt time.Time
	if row.OrphanedAt != nil {
		orphanedAt = *row.OrphanedAt
	}

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
	if hash, ok := r.activeHashAt(ctx, height); ok && hash == orphan {
		logger.Warn("orphan mark is stale — block is on the active chain; resetting to active")
		if err := r.store.UpsertBlockHeaderSeen(ctx, orphan, height, r.now()); err != nil {
			logger.Warn("failed to reset resurrected block", zap.Error(err))
			return "error"
		}
		delete(r.defers, orphan)
		metrics.BlockStatusTransitionsTotal.
			WithLabelValues(metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceReconciler).
			Inc()
		return "resurrected"
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
	canonicalHash, canonicalKnown := r.activeHashAt(ctx, height)
	if canonicalKnown {
		n, bumpOK, remineErr := r.remineFromStoredBUMP(ctx, logger, canonicalHash, batchSize)
		if remineErr != nil {
			// A failed batch must not fall through to the revert below — that
			// would un-mine txs that ARE in the canonical block — nor stamp
			// the row. Leave it queued; the next tick retries.
			logger.Warn("re-mine from the canonical BUMP failed; leaving the block queued for retry",
				zap.String("canonical_block_hash", canonicalHash), zap.Error(remineErr))
			return "error"
		}
		if bumpOK {
			canonicalReady = true
			reanchored += n
		}
	}
	if !canonicalReady && canonicalHash != "" {
		if r.deferForCanonicalBUMP(ctx, logger, orphan, canonicalHash) {
			return "deferred"
		}
	}

	// Deep-reorg neighborhood: txs re-binned into LATER canonical blocks.
	// Only the txs still anchored to O are candidates; membership is an
	// O(1) lookup against each neighbor's stored compound BUMP. This runs
	// regardless of the height-O canonical BUMP so we still re-anchor every
	// tx we CAN prove before deciding the remainder's fate.
	affected, err := r.store.GetTxIDsByBlockHash(ctx, orphan)
	if err != nil {
		logger.Warn("failed to read affected txids", zap.Error(err))
		return "error"
	}
	n, neighborErr := r.reanchorNeighborhood(ctx, logger, affected, height, batchSize)
	reanchored += n
	if neighborErr != nil {
		logger.Warn("neighborhood re-anchor failed; leaving the block queued for retry", zap.Error(neighborErr))
		return "error"
	}

	// Park vs revert for whatever is still anchored to O (issue #282). When
	// the canonical block's BUMP was NOT available (canonicalReady == false),
	// we have no proof of where these txs belong — the canonical block simply
	// hasn't been fetched/rebuilt yet. Reverting them to SEEN_ON_NETWORK
	// would UN-MINE txs that are almost certainly in that not-yet-fetched
	// block: a downgrade, not a repair. So by default we PARK the block
	// instead — leave the txs MINED@O and stamp reconciled_at so it drops out
	// of the queue (bounded — the defer cap already fired; no infinite retry
	// and no further /reprocess spam). A later stored/rebuilt canonical BUMP
	// re-anchors them through the normal mine path (MINED@O → MINED@canonical
	// is lattice-legal). RevertWhenUnreconcilable restores the old revert-all
	// fallback as an opt-in.
	if !canonicalReady && !r.cfg.BumpBuilder.Reconciler.RevertWhenUnreconcilable {
		return r.parkBlock(ctx, logger, orphan, orphanedAt, height, canonicalHash, reanchored)
	}

	// Revert the remainder: the canonical BUMP WAS available and these txs
	// are provably in no canonical block (or the operator opted into the
	// legacy fallback) — SEEN_ON_NETWORK puts them back in flight
	// (rebroadcast/propagation owns them from here), and the store appends O
	// to their orphaned-anchor history.
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
	if delErr := r.store.DeleteStumpsByBlockHash(ctx, orphan); delErr != nil {
		logger.Warn("failed to clean up orphan STUMPs", zap.Error(delErr))
	}
	stamped, err := r.store.MarkBlockReconciled(ctx, orphan, orphanedAt, r.now())
	if err != nil {
		logger.Warn("failed to stamp reconciled_at", zap.Error(err))
		return "error"
	}
	delete(r.defers, orphan)
	if !stamped {
		return r.staleOutcome(logger, height)
	}

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

// parkBlock is the issue-#282 fallback for an orphan whose canonical BUMP is
// unavailable at the defer cap: it takes the block off the reconcile queue
// WITHOUT downgrading any transaction. The txs still anchored to O stay
// MINED@O (not reverted to SEEN_ON_NETWORK) — a later stored or rebuilt
// canonical BUMP re-anchors them through the normal mine path. STUMPs are
// pruned and the compound BUMP retained, identical to the healed-terminal
// path; only reconciled_at is stamped so the row leaves the queue (bounded:
// no infinite retry, no repeated /reprocess). Returns the "parked" outcome.
func (r *Reconciler) parkBlock(ctx context.Context, logger *zap.Logger, orphan string, orphanedAt time.Time, height uint64, canonicalHash string, reanchored int) string {
	// Count what remains MINED@O purely for the metric/log — the neighborhood
	// pass already re-anchored everything it could prove, so this is the
	// still-unresolvable set we are deliberately leaving in place.
	parked := 0
	if remaining, err := r.store.GetTxIDsByBlockHash(ctx, orphan); err == nil {
		parked = len(remaining)
	}

	if err := r.store.DeleteStumpsByBlockHash(ctx, orphan); err != nil {
		logger.Warn("failed to clean up orphan STUMPs", zap.Error(err))
	}
	stamped, err := r.store.MarkBlockReconciled(ctx, orphan, orphanedAt, r.now())
	if err != nil {
		logger.Warn("failed to stamp reconciled_at on parked block", zap.Error(err))
		return "error"
	}
	delete(r.defers, orphan)
	if !stamped {
		return r.staleOutcome(logger, height)
	}

	metrics.ReconcilerTxsReanchoredTotal.Add(float64(reanchored))
	metrics.ReconcilerTxsParkedTotal.Add(float64(parked))
	logger.Warn(
		"orphaned block parked (unreconcilable): canonical BUMP unavailable at the defer cap — "+
			"txs left MINED against the orphan, NOT reverted; a later canonical BUMP re-anchors them (issue #282)",
		logfields.BlockHeight(height),
		zap.String("canonical_block_hash", canonicalHash),
		zap.Int("txs_reanchored", reanchored),
		zap.Int("txs_parked", parked),
	)
	return "parked"
}

// staleOutcome is returned when the reconciled_at CAS found a different
// orphan generation than the one this pass processed: the block-status
// tracker reactivated the row, or it was orphaned again, while the pass ran.
// Nothing is stamped — an active row stays clean and a newer orphan
// generation stays queued for its own pass. The tx writes this pass made
// were correct against the chain state it observed; the competitor's orphan
// row (marked by the same reorg) heals them on a later tick.
func (r *Reconciler) staleOutcome(logger *zap.Logger, height uint64) string {
	logger.Warn("stale reconcile: block row changed generation while reconciling (reactivated or re-orphaned); not stamping",
		logfields.BlockHeight(height))
	return "stale"
}

// activeHashAt resolves the active-chain block hash at height. ok=false
// means the chain-header source cannot judge that height (unknown, above
// the tip, or out of range) — callers fail open, never treating absence of
// evidence as evidence.
func (r *Reconciler) activeHashAt(ctx context.Context, height uint64) (string, bool) {
	if height == 0 || height > math.MaxUint32 {
		return "", false
	}
	active, err := r.chainHeader.GetHeaderByHeight(ctx, uint32(height))
	if err != nil || active == nil {
		return "", false
	}
	return active.Hash.String(), true
}

// deferForCanonicalBUMP handles the canonical-BUMP-missing branch: while
// under the defer cap it bumps the orphan's counter, optionally pokes
// merkle-service /reprocess for the canonical block, and reports true
// (caller returns "deferred" without stamping reconciled_at). At the cap it
// reports false so the caller parks the block (issue #282) — or reverts, if
// RevertWhenUnreconcilable is set.
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
// Returns the number of rows moved and the first store write failure, if
// any (the caller must then leave the block queued). The affected slice
// shrinks as txs are claimed; whatever remains falls to the caller's revert.
func (r *Reconciler) reanchorNeighborhood(ctx context.Context, logger *zap.Logger, affected []string, height uint64, batchSize int) (int, error) {
	if len(affected) == 0 {
		return 0, nil
	}
	// A placeholder orphan row with no resolvable height (height==0) has no
	// meaningful neighborhood above it — walking heights 1..depth is
	// nonsensical. Skip straight to the caller's revert.
	if height == 0 {
		return 0, nil
	}
	depth := r.cfg.BumpBuilder.Reconciler.NeighborhoodDepth
	if depth < 0 {
		depth = 0
	}
	reanchored := 0
	for h := height + 1; h <= height+uint64(depth) && len(affected) > 0; h++ {
		neighbor, known := r.activeHashAt(ctx, h)
		if !known {
			break // above the tip — nothing further to check
		}
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
			n, mineErr := setMinedAndPublish(ctx, logger, r.store, r.publisher,
				neighbor, h, contained[start:end], models.ExtraInfoReorgReanchor, true)
			reanchored += n
			if mineErr != nil {
				return reanchored, fmt.Errorf("re-anchor to neighbor %s: %w", neighbor, mineErr)
			}
		}
		affected = rest
	}
	return reanchored, nil
}

// remineFromStoredBUMP re-mines every level-0 txid of blockHash's stored
// compound BUMP against it (onlyChanged — no duplicate events for rows
// already anchored right). ok=false when no usable BUMP is stored. A
// non-nil error means a store write failed part-way: the caller must not
// treat the block as healed (no revert, no reconciled_at stamp) so the work
// is retried.
func (r *Reconciler) remineFromStoredBUMP(ctx context.Context, logger *zap.Logger, blockHash string, batchSize int) (int, bool, error) {
	bumpHeight, bumpBytes, bumpErr := r.store.GetBUMP(ctx, blockHash)
	if bumpErr != nil || len(bumpBytes) == 0 {
		return 0, false, nil //nolint:nilerr // no usable BUMP is the ok=false signal (defer), not a write failure
	}
	txids, parseErr := levelZeroTxidsFromBUMP(bumpBytes)
	if parseErr != nil {
		logger.Warn("stored canonical BUMP failed to parse",
			zap.String("canonical_block_hash", blockHash), zap.Error(parseErr))
		return 0, false, nil
	}
	changed := 0
	for start := 0; start < len(txids); start += batchSize {
		end := min(start+batchSize, len(txids))
		n, mineErr := setMinedAndPublish(ctx, logger, r.store, r.publisher,
			blockHash, bumpHeight, txids[start:end], models.ExtraInfoReorgReanchor, true)
		changed += n
		if mineErr != nil {
			return changed, true, fmt.Errorf("re-mine batch %d..%d against %s: %w", start, end, blockHash, mineErr)
		}
	}
	return changed, true, nil
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
