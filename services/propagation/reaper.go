package propagation

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// Stale thresholds for the reaper rebroadcast scan.
//
// staleReceivedAge: a row at RECEIVED that's older than this hasn't seen
// the broadcast pipeline make progress on it. Pick it up and try again.
// Comfortably larger than the 15s teranode broadcast timeout so we don't
// fight an in-flight processBatch goroutine that's mid-broadcast.
//
// staleSeenOnNetworkAge: a row at SEEN_ON_NETWORK that's older than this
// is in a Teranode mempool somewhere but not advancing to MINED. Rebroadcast
// to refresh upstream state — a peer may have evicted the tx, a fee bump
// may be needed, or a callback may have been dropped. Long enough that we
// don't rebroadcast every tx that takes a few minutes to mine.
//
// staleScanLookback bounds how far back IterateStatusesSince walks. Rows
// older than this are assumed permanently stuck and outside the reaper's
// responsibility — the operator surfaces them with `arcade tools surface
// stuck` if a deeper sweep is needed.
const (
	staleReceivedAge       = 30 * time.Second
	staleSeenOnNetworkAge  = time.Hour
	staleScanLookback      = 24 * time.Hour
	reaperRebroadcastBatch = 200
)

// reaperLeaseName is the well-known key every replica uses to coordinate
// reaper ownership. One lease per propagation deployment.
const reaperLeaseName = "propagation-reaper"

// runReaper drives the rebroadcast scan loop. Ticks on p.reaperInterval and
// runs reapOnce when this replica holds the reaper lease. A best-effort lease
// release happens on shutdown so a successor doesn't have to wait for the
// TTL to expire before taking over.
func (p *Propagator) runReaper(ctx context.Context) {
	p.tryReap(ctx)

	ticker := time.NewTicker(p.reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if p.leaser != nil {
				releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				_ = p.leaser.Release(releaseCtx, reaperLeaseName, p.holderID)
				cancel()
			}
			return
		case <-ticker.C:
			p.tryReap(ctx)
		}
	}
}

// tryReap acquires or renews the reaper lease before doing scan work. A
// non-leader tick is a no-op; lease errors are logged and treated as
// "not leader" for this tick — we'll try again next time.
func (p *Propagator) tryReap(ctx context.Context) {
	if p.leaser != nil {
		heldUntil, err := p.leaser.TryAcquireOrRenew(ctx, reaperLeaseName, p.holderID, p.leaseTTL)
		if err != nil {
			metrics.PropagationReaperTickTotal.WithLabelValues("lease_error").Inc()
			metrics.PropagationReaperLease.Set(0)
			p.logger.Warn("reaper: lease check failed, skipping tick", zap.Error(err))
			return
		}
		if heldUntil.IsZero() {
			metrics.PropagationReaperTickTotal.WithLabelValues("skipped_no_leader").Inc()
			metrics.PropagationReaperLease.Set(0)
			p.logger.Debug("reaper: not leader, skipping tick")
			return
		}
		metrics.PropagationReaperLease.Set(1)
	}
	metrics.PropagationReaperTickTotal.WithLabelValues("ran").Inc()
	p.reapOnce(ctx)
}

// reapOnce scans rows updated within staleScanLookback, picks out those
// stuck at RECEIVED past staleReceivedAge or SEEN_ON_NETWORK past
// staleSeenOnNetworkAge, and rebroadcasts them through the same
// registerBatch + broadcastInChunks + applyTerminalStatuses path that
// processBatch uses. The reaper bypasses the dispatcher entirely — its
// rebroadcasts go directly to teranode, and any resulting terminal
// status update notifies the dispatcher through applyTerminalStatuses
// so the Kafka offset for the original message advances naturally.
//
// Bounded by reaperRebroadcastBatch per tick so a backlog can't pin the
// reaper into a single multi-minute call.
func (p *Propagator) reapOnce(ctx context.Context) {
	now := time.Now()
	since := now.Add(-staleScanLookback)
	receivedDeadline := now.Add(-staleReceivedAge)
	seenDeadline := now.Add(-staleSeenOnNetworkAge)

	stuck := make([]propagationMsg, 0, reaperRebroadcastBatch)
	err := p.store.IterateStatusesSince(ctx, since, func(st *models.TransactionStatus) error {
		if len(stuck) >= reaperRebroadcastBatch {
			return errReaperBatchFull
		}
		if len(st.RawTx) == 0 {
			// No body to rebroadcast. Pre-reaper-population rows
			// won't have it; just skip.
			return nil
		}
		switch st.Status {
		case models.StatusReceived:
			if !st.Timestamp.Before(receivedDeadline) {
				return nil
			}
		case models.StatusSeenOnNetwork, models.StatusSeenMultipleNodes:
			if !st.Timestamp.Before(seenDeadline) {
				return nil
			}
		default:
			// Terminal statuses, MINED, IMMUTABLE, RECEIVED-but-still-fresh,
			// etc. — not the reaper's job.
			return nil
		}
		stuck = append(stuck, propagationMsg{TXID: st.TxID, RawTx: st.RawTx})
		return nil
	})
	if err != nil && !errors.Is(err, errReaperBatchFull) && !errors.Is(err, context.Canceled) {
		p.logger.Error("reaper: scan failed", zap.Error(err))
		return
	}
	if len(stuck) == 0 {
		return
	}

	p.logger.Info("reaper: rebroadcasting stuck txs", zap.Int("count", len(stuck)))
	metrics.PropagationReaperReadyDepth.Set(float64(len(stuck)))

	// Use the same broadcast pipeline as processBatch so all the
	// per-tx classification (Accepted / Rejected / InFlight / Skip)
	// applies uniformly. applyTerminalStatuses writes terminal rows
	// AND notifies the dispatcher — txids the dispatcher doesn't know
	// about (because the original Kafka message terminated long ago)
	// get a no-op notify, which is fine.
	registered := p.registerBatch(ctx, stuck)
	if len(registered) == 0 {
		return
	}
	rawTxs := make([][]byte, len(registered))
	for i, m := range registered {
		rawTxs[i] = m.RawTx
	}
	results := p.broadcastInChunks(ctx, registered, rawTxs)

	var accepted, rejected int
	terminalStatuses := make([]*models.TransactionStatus, 0, len(results))
	for _, res := range results {
		switch res.class {
		case txResultClassAccepted:
			accepted++
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassRejected:
			rejected++
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassUnknown, txResultClassSkip:
			// Skip / Unknown: leave the row alone so the next reaper
			// tick picks it up.
		}
	}
	p.applyTerminalStatuses(ctx, terminalStatuses, accepted, rejected)
}

// errReaperBatchFull halts the IterateStatusesSince walk once we've
// accumulated reaperRebroadcastBatch stuck rows. Sentinel error so
// IterateStatusesSince surfaces it cleanly without being mistaken for
// a backend error.
var errReaperBatchFull = errors.New("reaper batch full")
