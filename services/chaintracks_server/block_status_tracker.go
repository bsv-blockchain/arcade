package chaintracks_server

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// maxReorgBranchWalk bounds how many heights below the new tip the
// ReorgEvent handler re-examines for resurrected rows. A reorg deeper than
// this is a network-level incident; the reconciler's targeted full-scan
// lever (bump_builder.reconciler.full_scan_{min,max}_height) covers the
// rest.
const maxReorgBranchWalk = 1000

// blockStatusTracker subscribes to chaintracks tip + reorg channels and
// translates each event into writes against the block-processing table:
//
//   - Tip headers → UpsertBlockHeaderSeen, then a tie-scan
//   - ReorgEvent  → MarkBlocksOrphaned(Orphans), upsert the new tip,
//     reactivate the new branch's orphaned rows, then a forced tie-scan
//
// The tie-scan (issue #279) is the detection edge chaintracks itself cannot
// provide: a same-height competition loser has EQUAL chainwork, never
// becomes tip, and therefore never appears in a ReorgEvent — yet
// bump-builder may have anchored transactions to it. On each tip update the
// scan re-verifies every block_processing row within
// chaintracks_server.tie_scan_depth of the tip against the active chain, in
// BOTH directions: 'active' rows whose height is held by a different block
// are marked orphaned (feeding the anchor reconciler's queue), and
// 'orphaned' rows that ARE the active-chain block at their height are reset
// to active. The second direction is issue #339: when the next block builds
// on the tie loser, chaintracks' ReorgEvent names only the hashes it
// orphans and the new tip — never the resurrected ancestor — and the
// reconciler has usually already stamped that row reconciled, so nothing
// else would ever return it to active.
//
// Reactivation repairs the status projection only. The resurrected block's
// transactions are healed by the anchor reconciler through the COMPETITOR's
// orphan row (recordReorg marks it in the same event), the same reliance the
// reconciler's own resurrection short-circuit has; the reconciler's
// full-scan additionally re-mines whatever it reactivates.
//
// chaintracks.Subscribe is a fan-out broadcaster, so this subscription does
// not contend with the existing SSE one in chaintracksRoutes.
//
// Drop-on-overflow: chaintracks's broadcast does a non-blocking send onto a
// buffer-1 channel. If a store write stalls long enough, a tip can be
// silently dropped. That is acceptable for the orphan detection too: the
// NEXT tip re-runs the scan over the whole window, and the anchor
// reconciler's startup full-scan is the deep backstop.
type blockStatusTracker struct {
	store  store.Store
	ct     chaintracks.Chaintracks
	logger *zap.Logger

	scanDepth       int
	scanMinInterval time.Duration
	branchWalkLimit int

	// scanMu serializes tie-scans and branch walks across the header and
	// reorg loops and guards lastScan for the debounce.
	scanMu   sync.Mutex
	lastScan time.Time
}

func newBlockStatusTracker(ctx context.Context, cfg *config.Config, cm chaintracks.Chaintracks, st store.Store, logger *zap.Logger) *blockStatusTracker {
	t := &blockStatusTracker{
		store:           st,
		ct:              cm,
		logger:          logger.Named("block-status"),
		scanDepth:       cfg.ChaintracksServer.TieScanDepth,
		scanMinInterval: time.Duration(cfg.ChaintracksServer.TieScanMinIntervalMs) * time.Millisecond,
		branchWalkLimit: maxReorgBranchWalk,
	}
	go t.runHeaderLoop(ctx, cm.Subscribe(ctx))
	go t.runReorgLoop(ctx, cm.SubscribeReorg(ctx))
	return t
}

func (t *blockStatusTracker) runHeaderLoop(ctx context.Context, ch <-chan *chaintracks.BlockHeader) {
	for {
		select {
		case <-ctx.Done():
			return
		case h, ok := <-ch:
			if !ok {
				return
			}
			if h == nil {
				continue
			}
			t.recordHeader(ctx, h)
			t.tieScan(ctx, h.Height, false)
		}
	}
}

func (t *blockStatusTracker) runReorgLoop(ctx context.Context, ch <-chan *chaintracks.ReorgEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev == nil {
				continue
			}
			t.recordReorg(ctx, ev)
			if ev.NewTip != nil {
				// A ReorgEvent is a strong signal: never let the debounce
				// drop this scan in favor of one the header loop ran a
				// moment earlier.
				t.tieScan(ctx, ev.NewTip.Height, true)
			}
		}
	}
}

func (t *blockStatusTracker) recordHeader(ctx context.Context, h *chaintracks.BlockHeader) {
	hash := h.Hash.String()
	if err := t.store.UpsertBlockHeaderSeen(ctx, hash, uint64(h.Height), time.Now()); err != nil {
		t.logger.Warn("failed to record header seen",
			logfields.BlockHash(hash),
			zap.Uint32("height", h.Height),
			zap.Error(err))
	}
}

func (t *blockStatusTracker) recordReorg(ctx context.Context, ev *chaintracks.ReorgEvent) {
	if len(ev.OrphanedHashes) > 0 {
		hashes := make([]string, 0, len(ev.OrphanedHashes))
		for _, h := range ev.OrphanedHashes {
			hashes = append(hashes, h.String())
		}
		if err := t.store.MarkBlocksOrphaned(ctx, hashes, time.Now()); err != nil {
			t.logger.Warn("failed to mark orphaned blocks",
				zap.Int("count", len(hashes)),
				zap.Error(err))
		} else {
			metrics.BlockStatusTransitionsTotal.
				WithLabelValues(metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceReorgEvent).
				Add(float64(len(hashes)))
		}
	}
	// chaintracks's tip and reorg channels are independent; the new tip may
	// not have been emitted on the tip channel yet at the moment of this
	// reorg event. UpsertBlockHeaderSeen is idempotent, so doing both is
	// safe and ensures the new tip lands in the table.
	if ev.NewTip != nil {
		t.recordHeader(ctx, ev.NewTip)
		t.reactivateBranch(ctx, ev)
	}
}

// reactivateBranch walks the heights strictly between the fork point and the
// new tip — the blocks a ReorgEvent makes canonical without naming them —
// and resets to active any row a previous detection edge marked orphaned
// (issue #339: the same-height loser that the next block builds on). It is
// reactivate-only: rows arcade never recorded are not inserted (the tip
// channel owns that), and parked rows stay parked (the watchdog's in-memory
// park flag would otherwise disagree with the table). Each height is one
// in-memory header lookup plus one row read; the walk runs nearest-first and
// is capped at branchWalkLimit so a pathological event cannot stall the
// reorg loop. Serialized with the tie-scan under scanMu so the two loops
// never interleave writes for the same heights.
func (t *blockStatusTracker) reactivateBranch(ctx context.Context, ev *chaintracks.ReorgEvent) {
	if t.ct == nil || ev.NewTip == nil {
		return
	}
	if ev.CommonAncestor == nil {
		t.logger.Info("reorg: event carries no common ancestor; leaving resurrection to the tie-scan",
			zap.Uint32("tip_height", ev.NewTip.Height))
		return
	}
	if ev.NewTip.Height <= ev.CommonAncestor.Height+1 {
		return // the new branch is the tip alone, already recorded
	}
	lo, hi := ev.CommonAncestor.Height+1, ev.NewTip.Height-1

	t.scanMu.Lock()
	defer t.scanMu.Unlock()

	limit := t.branchWalkLimit
	if limit <= 0 {
		limit = maxReorgBranchWalk
	}
	if span := uint64(hi) - uint64(lo) + 1; span > uint64(limit) {
		cutoff := hi - uint32(limit) + 1 // limit < span ≤ hi+1, so this cannot underflow
		t.logger.Warn("reorg: new branch deeper than the reactivation walk limit; deeper rows are left to the reconciler full-scan",
			zap.Uint32("branch_low_height", lo),
			zap.Uint32("walk_low_height", cutoff),
			zap.Uint32("tip_height", ev.NewTip.Height),
			zap.Int("walk_limit", limit))
		lo = cutoff
	}

	reactivated := make([]string, 0, 1)
	for h := hi; h >= lo; h-- {
		active, err := t.ct.GetHeaderByHeight(ctx, h)
		if err != nil || active == nil {
			continue // fail-open: chaintracks cannot judge this height
		}
		hash := active.Hash.String()
		row, err := t.store.GetBlockProcessingStatus(ctx, hash)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				t.logger.Warn("reorg: failed to read block row", logfields.BlockHash(hash), zap.Error(err))
			}
			continue
		}
		if row.Status != models.BlockStatusOrphaned {
			continue
		}
		if err := t.store.UpsertBlockHeaderSeen(ctx, hash, uint64(h), time.Now()); err != nil {
			t.logger.Warn("reorg: failed to reactivate resurrected block",
				logfields.BlockHash(hash),
				logfields.BlockHeight(uint64(h)),
				zap.Error(err))
			continue
		}
		metrics.BlockStatusTransitionsTotal.
			WithLabelValues(metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceReorgEvent).
			Inc()
		reactivated = append(reactivated, hash)
	}
	if len(reactivated) > 0 {
		t.logger.Warn("reorg: reactivated resurrected canonical blocks that were marked orphaned",
			zap.Strings("block_hashes", reactivated),
			zap.Uint32("tip_height", ev.NewTip.Height))
	}
}

// tieScan verifies every block_processing row within scanDepth of the tip
// against the active chain. 'active' rows whose height is held by a
// different block are marked orphaned exactly as if a ReorgEvent had named
// them — the no-event case, a same-height loser filed by chaintracks as an
// equal-work alternate (issue #279). 'orphaned' rows that ARE the
// active-chain block at their height are reset to active — the flip-flop
// case, where the earlier loser wins the next block (issue #339). Debounced
// to one scan per scanMinInterval so regtest tip bursts and catch-up syncs
// don't hammer the table; force bypasses the debounce for the reorg loop,
// whose trigger is a strong signal. Cost is otherwise ≤ a few header lookups
// against the in-memory chain index plus one small table page.
func (t *blockStatusTracker) tieScan(ctx context.Context, tipHeight uint32, force bool) {
	if t.scanDepth <= 0 || t.ct == nil {
		return
	}
	t.scanMu.Lock()
	defer t.scanMu.Unlock()
	if !force && !t.lastScan.IsZero() && time.Since(t.lastScan) < t.scanMinInterval {
		return
	}
	t.lastScan = time.Now()

	minHeight := uint64(1)
	if uint64(tipHeight) > uint64(t.scanDepth) {
		minHeight = uint64(tipHeight) - uint64(t.scanDepth)
	}
	// Walk EVERY row down to minHeight via the boundary-safe pager — a
	// fixed-size sample could miss rows when competitors or churn crowd the
	// top of the height-DESC listing, and a naive height cursor would skip
	// same-height rows straddling a page boundary (the exact rows this scan
	// exists to find). The pager may visit a boundary row twice, so
	// judgements collect into sets and the first judgement of a hash wins.
	// The height-0 placeholder rows MarkBlockProcessed creates before any
	// header arrives are excluded by the height filter; parked rows belong
	// to the watchdog and are never judged here.
	orphanedSet := make(map[string]struct{})
	reactivateSet := make(map[string]uint64)
	err := store.ForEachBlockProcessing(ctx, t.store, minHeight, t.scanDepth*4, func(row *models.BlockProcessingStatus) error {
		if (row.Status != models.BlockStatusActive && row.Status != models.BlockStatusOrphaned) ||
			row.BlockHeight < minHeight ||
			row.BlockHeight > uint64(tipHeight) ||
			row.BlockHeight > math.MaxUint32 {
			return nil
		}
		if _, seen := orphanedSet[row.BlockHash]; seen {
			return nil
		}
		if _, seen := reactivateSet[row.BlockHash]; seen {
			return nil
		}
		active, hdrErr := t.ct.GetHeaderByHeight(ctx, uint32(row.BlockHeight))
		if hdrErr != nil || active == nil {
			return nil //nolint:nilerr // fail-open by contract: never judge on absence of evidence
		}
		matches := active.Hash.String() == row.BlockHash
		switch {
		case row.Status == models.BlockStatusActive && !matches:
			orphanedSet[row.BlockHash] = struct{}{}
			t.logger.Warn("tie-scan: active row lost its height to a competitor",
				logfields.BlockHash(row.BlockHash),
				logfields.BlockHeight(row.BlockHeight),
				zap.String("active_block_hash", active.Hash.String()))
		case row.Status == models.BlockStatusOrphaned && matches:
			reactivateSet[row.BlockHash] = row.BlockHeight
			t.logger.Warn("tie-scan: orphaned row is the active-chain block at its height again",
				logfields.BlockHash(row.BlockHash),
				logfields.BlockHeight(row.BlockHeight))
		}
		return nil
	})
	if err != nil {
		t.logger.Warn("tie-scan: incomplete", zap.Error(err))
		// Fall through: whatever was found still gets written; the next tip
		// re-runs the scan and the reconciler full-scan is the backstop.
	}
	t.markOrphaned(ctx, orphanedSet, tipHeight)
	t.reactivateRows(ctx, reactivateSet, tipHeight)
}

func (t *blockStatusTracker) markOrphaned(ctx context.Context, set map[string]struct{}, tipHeight uint32) {
	if len(set) == 0 {
		return
	}
	orphaned := make([]string, 0, len(set))
	for hash := range set {
		orphaned = append(orphaned, hash)
	}
	if err := t.store.MarkBlocksOrphaned(ctx, orphaned, time.Now()); err != nil {
		t.logger.Warn("tie-scan: failed to mark blocks orphaned",
			zap.Int("count", len(orphaned)),
			zap.Error(err))
		return
	}
	metrics.BlockStatusTransitionsTotal.
		WithLabelValues(metrics.BlockTransitionOrphaned, metrics.BlockTransitionSourceTieScan).
		Add(float64(len(orphaned)))
	t.logger.Info("tie-scan: marked same-height losers orphaned",
		zap.Strings("block_hashes", orphaned),
		zap.Uint32("tip_height", tipHeight))
}

// reactivateRows resets each orphaned row to active through the same
// header-seen upsert the tip channel uses: every backend's conflict path
// resets status, clears orphaned_at/reconciled_at and preserves the
// milestone timestamps. Per-row writes are fine — a resurrection is a rare,
// one-block event.
func (t *blockStatusTracker) reactivateRows(ctx context.Context, rows map[string]uint64, tipHeight uint32) {
	if len(rows) == 0 {
		return
	}
	reactivated := make([]string, 0, len(rows))
	for hash, height := range rows {
		if err := t.store.UpsertBlockHeaderSeen(ctx, hash, height, time.Now()); err != nil {
			t.logger.Warn("tie-scan: failed to reactivate resurrected block",
				logfields.BlockHash(hash),
				logfields.BlockHeight(height),
				zap.Error(err))
			continue
		}
		metrics.BlockStatusTransitionsTotal.
			WithLabelValues(metrics.BlockTransitionReactivated, metrics.BlockTransitionSourceTieScan).
			Inc()
		reactivated = append(reactivated, hash)
	}
	if len(reactivated) > 0 {
		t.logger.Info("tie-scan: reactivated resurrected canonical blocks",
			zap.Strings("block_hashes", reactivated),
			zap.Uint32("tip_height", tipHeight))
	}
}
