package chaintracks_server

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/store"
)

// blockStatusTracker subscribes to chaintracks tip + reorg channels and
// translates each event into a write against the block-processing table:
//
//   - Tip headers       → UpsertBlockHeaderSeen
//   - ReorgEvent.Orphans → MarkBlocksOrphaned (then upsert the new tip)
//
// chaintracks.Subscribe is a fan-out broadcaster, so this subscription does
// not contend with the existing SSE one in chaintracksRoutes.
//
// Drop-on-overflow: chaintracks's broadcast does a non-blocking send onto a
// buffer-1 channel. If a store write stalls long enough, a tip can be
// silently dropped. The tracker is not load-bearing for correctness — the
// table is observability-first, and the next header arrival overwrites
// block_height — so we accept that risk in v1 and keep the loops simple.
type blockStatusTracker struct {
	store  store.Store
	logger *zap.Logger
}

func newBlockStatusTracker(ctx context.Context, cm chaintracks.Chaintracks, st store.Store, logger *zap.Logger) *blockStatusTracker {
	t := &blockStatusTracker{
		store:  st,
		logger: logger.Named("block-status"),
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
		}
	}
}

func (t *blockStatusTracker) recordHeader(ctx context.Context, h *chaintracks.BlockHeader) {
	hash := h.Hash.String()
	if err := t.store.UpsertBlockHeaderSeen(ctx, hash, uint64(h.Height), time.Now()); err != nil {
		t.logger.Warn("failed to record header seen",
			zap.String("block_hash", hash),
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
