package chaintracks_server

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// blockStatusTracker subscribes to chaintracks tip + reorg channels and
// translates each event into writes against the block-processing table:
//
//   - Tip headers       → UpsertBlockHeaderSeen, then a tie-scan
//   - ReorgEvent.Orphans → MarkBlocksOrphaned (then upsert the new tip)
//
// The tie-scan (issue #279) is the detection edge chaintracks itself cannot
// provide: a same-height competition loser has EQUAL chainwork, never
// becomes tip, and therefore never appears in a ReorgEvent — yet
// bump-builder may have anchored transactions to it. On each tip update the
// scan re-verifies every 'active' block_processing row within
// chaintracks_server.tie_scan_depth of the tip against the active chain and
// marks mismatches orphaned, feeding the anchor reconciler's queue.
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

	// scanMu serializes tie-scans across the header and reorg loops and
	// guards lastScan for the debounce.
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
			t.scanRecentActive(ctx, h.Height)
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
				t.scanRecentActive(ctx, ev.NewTip.Height)
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
		}
	}
	// chaintracks's tip and reorg channels are independent; the new tip may
	// not have been emitted on the tip channel yet at the moment of this
	// reorg event. UpsertBlockHeaderSeen is idempotent, so doing both is
	// safe and ensures the new tip lands in the table.
	if ev.NewTip != nil {
		t.recordHeader(ctx, ev.NewTip)
	}
}

// scanRecentActive verifies that every 'active' block_processing row within
// scanDepth of the tip is still the active-chain block at its height, and
// marks mismatches orphaned exactly as if a ReorgEvent had named them. This
// catches the no-event case: a same-height loser filed by chaintracks as an
// equal-work alternate (issue #279). Debounced to one scan per
// scanMinInterval so regtest tip bursts and catch-up syncs don't hammer the
// table; cost is otherwise ≤ a few header lookups against the in-memory
// chain index plus one small table page.
func (t *blockStatusTracker) scanRecentActive(ctx context.Context, tipHeight uint32) {
	if t.scanDepth <= 0 || t.ct == nil {
		return
	}
	t.scanMu.Lock()
	defer t.scanMu.Unlock()
	if !t.lastScan.IsZero() && time.Since(t.lastScan) < t.scanMinInterval {
		return
	}
	t.lastScan = time.Now()

	minHeight := uint64(1)
	if uint64(tipHeight) > uint64(t.scanDepth) {
		minHeight = uint64(tipHeight) - uint64(t.scanDepth)
	}
	// The list is height-DESC; a window of depth heights can hold a few
	// competitors per height, so over-fetch generously and filter. The
	// height-0 placeholder rows MarkBlockProcessed creates before any
	// header arrives are excluded by the height filter.
	rows, err := t.store.ListBlockProcessingStatus(ctx, 0, t.scanDepth*4)
	if err != nil {
		t.logger.Warn("tie-scan: failed to list block_processing rows", zap.Error(err))
		return
	}

	var orphaned []string
	for _, row := range rows {
		if row.Status != models.BlockStatusActive ||
			row.BlockHeight < minHeight ||
			row.BlockHeight > uint64(tipHeight) ||
			row.BlockHeight > math.MaxUint32 {
			continue
		}
		active, err := t.ct.GetHeaderByHeight(ctx, uint32(row.BlockHeight))
		if err != nil || active == nil {
			continue // cannot judge: never orphan on absence of evidence
		}
		if active.Hash.String() != row.BlockHash {
			orphaned = append(orphaned, row.BlockHash)
			t.logger.Warn("tie-scan: active row lost its height to a competitor",
				logfields.BlockHash(row.BlockHash),
				logfields.BlockHeight(row.BlockHeight),
				zap.String("active_block_hash", active.Hash.String()))
		}
	}
	if len(orphaned) == 0 {
		return
	}
	if err := t.store.MarkBlocksOrphaned(ctx, orphaned, time.Now()); err != nil {
		t.logger.Warn("tie-scan: failed to mark blocks orphaned",
			zap.Int("count", len(orphaned)),
			zap.Error(err))
		return
	}
	t.logger.Info("tie-scan: marked same-height losers orphaned",
		zap.Strings("block_hashes", orphaned),
		zap.Uint32("tip_height", tipHeight))
}
