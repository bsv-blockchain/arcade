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
// staleSeenOnNetworkAge: a row at SEEN_ON_NETWORK that's older than this
// is in a Teranode mempool somewhere but not advancing to MINED. Rebroadcast
// to refresh upstream state — a peer may have evicted the tx, a fee bump
// may be needed, or a callback may have been dropped. Long enough that we
// don't rebroadcast every tx that takes a few minutes to mine.
//
// staleScanLookback bounds how far back the candidate scan walks. Rows
// older than this are assumed permanently stuck and outside the reaper's
// responsibility — the operator surfaces them with `arcade tools surface
// stuck` if a deeper sweep is needed.
//
// The per-tick batch size is configurable (reaper_batch_size); the per-tx
// rebroadcast cadence is reaper_rebroadcast_interval_ms.
const (
	staleSeenOnNetworkAge = time.Hour
	staleScanLookback     = 24 * time.Hour
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

// reapOnce rebroadcasts rows stuck at SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES
// past staleSeenOnNetworkAge (peer mempool eviction, dropped BLOCK_PROCESSED
// callback, unconfirmed ancestor that has since confirmed). RECEIVED rows are
// intentionally not rebroadcast — the submitter got an error from intake on
// Kafka publish failure and owns the decision to retry.
//
// Candidates come from GetReapCandidates ordered oldest-rebroadcast-first, and
// each attempted txid is stamped via MarkRebroadcastByTxIDs so it isn't re-sent
// again for reaperRebroadcastInterval. That per-tx throttle is what keeps a
// backlog larger than reaperBatchSize from starving older rows: capacity
// spreads across the whole backlog over one interval instead of re-sending the
// same rows every tick.
//
// Rebroadcasts go through the same registerBatch + broadcastInChunks +
// applyTerminalStatuses pipeline as processBatch but bypass the
// dispatcher's admission — these rows are no longer in inFlight, so any
// resulting terminal status notifies the dispatcher via applyTerminalStatuses
// only as a no-op for offset bookkeeping.
//
// Bounded by reaperBatchSize per tick so a backlog can't pin the reaper into a
// single multi-minute call.
func (p *Propagator) reapOnce(ctx context.Context) {
	now := time.Now()
	since := now.Add(-staleScanLookback)
	seenDeadline := now.Add(-staleSeenOnNetworkAge)
	rebroadcastBefore := now.Add(-p.reaperRebroadcastInterval)

	candidates, err := p.store.GetReapCandidates(ctx, since, seenDeadline, rebroadcastBefore, p.reaperBatchSize)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			p.logger.Error("reaper: scan failed", zap.Error(err))
		}
		return
	}

	stuck := make([]propagationMsg, 0, len(candidates))
	for _, st := range candidates {
		stuck = append(stuck, propagationMsg{TXID: st.TxID, RawTx: st.RawTx})
	}

	// Publish the post-scan depth on every tick BEFORE the early-return
	// so the gauge reflects "what the last reaper observed" — including
	// the queue-is-clear case. Setting it only on the non-empty branch
	// leaves a stale non-zero value visible to dashboards after the
	// backlog drains, which used to make the metric misleading.
	metrics.PropagationReaperReadyDepth.Set(float64(len(stuck)))

	if len(stuck) == 0 {
		return
	}

	p.logger.Info("reaper: rebroadcasting stuck txs", zap.Int("count", len(stuck)))

	// Use the same broadcast pipeline as processBatch so the per-tx
	// classification (Accepted / Rejected / Requeue) applies uniformly.
	// applyTerminalStatuses writes terminal rows AND notifies the
	// dispatcher — txids the dispatcher doesn't know about (because the
	// original Kafka message terminated long ago) get a no-op notify,
	// which is fine.
	registered, failed := p.registerBatch(ctx, stuck)
	if len(registered) == 0 {
		// Nothing registered: either a global merkle /watch outage or this
		// whole batch is persistently failing registration. Don't broadcast
		// and don't stamp — a global outage must not push every stuck tx past
		// its rebroadcast window, and a fully-failing batch has no healthy
		// rows behind it to starve. The next tick retries the same set.
		return
	}
	rawTxs := make([][]byte, len(registered))
	for i, m := range registered {
		rawTxs[i] = m.RawTx
	}
	results := p.broadcastInChunks(ctx, registered, rawTxs)

	// Stamp last_rebroadcast_at so each attempted tx cedes its slot, at one of
	// two cadences by outcome class:
	//   - accepted rows stay SEEN_* (the lattice forbids the SEEN→ACCEPTED
	//     downgrade, so applyTerminalStatuses is a no-op for them) and only
	//     need a periodic refresh → full reaperRebroadcastInterval.
	//   - transient failures — a Teranode requeue/unknown verdict, plus rows
	//     whose merkle /watch registration failed (the `failed` set) — should
	//     retry soon → shorter reaperRequeueBackoff, applied by backdating the
	//     stamp so the row becomes due again after that backoff instead of the
	//     full interval. Stamping the failed subset is what stops a
	//     persistently registration-failing tx (last_rebroadcast_at == NULL)
	//     from sorting first and re-filling the front of every batch.
	// rejected rows become terminal and leave the candidate set — no stamp.
	fullIntervalTxids := make([]string, 0, len(registered))
	shortBackoffTxids := make([]string, 0, len(registered)+len(failed))
	for _, m := range failed {
		shortBackoffTxids = append(shortBackoffTxids, m.TXID)
	}

	var accepted, rejected int
	terminalStatuses := make([]*models.TransactionStatus, 0, len(results))
	for i, res := range results {
		// results is index-aligned to registered (broadcastInChunks writes
		// per-index), so registered[i] is this result's tx — needed because a
		// requeue result carries no txid of its own.
		txid := registered[i].TXID
		switch res.class {
		case txResultClassAccepted:
			accepted++
			fullIntervalTxids = append(fullIntervalTxids, txid)
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassRejected:
			rejected++
			if res.status != nil {
				terminalStatuses = append(terminalStatuses, res.status)
			}
		case txResultClassUnknown, txResultClassRequeue:
			// Transient: leave the row SEEN_* and retry after the short
			// backoff. The reaper bypasses the dispatcher, so there's no
			// inFlight entry to requeue against — the next due tick is the
			// retry.
			shortBackoffTxids = append(shortBackoffTxids, txid)
		}
	}

	p.markRebroadcast(ctx, fullIntervalTxids, now)
	if len(shortBackoffTxids) > 0 {
		// Backdate so eligibility (last_rebroadcast_at < now-interval) flips
		// true after reaperRequeueBackoff rather than the full interval.
		p.markRebroadcast(ctx, shortBackoffTxids, now.Add(-(p.reaperRebroadcastInterval - p.reaperRequeueBackoff)))
	}

	p.applyTerminalStatuses(ctx, terminalStatuses, accepted, rejected)
}

// markRebroadcast stamps last_rebroadcast_at = ts on the given txids, logging
// (but not failing) on a store error — a missed stamp only costs a redundant
// rebroadcast next tick, not correctness.
func (p *Propagator) markRebroadcast(ctx context.Context, txids []string, ts time.Time) {
	if len(txids) == 0 {
		return
	}
	if err := p.store.MarkRebroadcastByTxIDs(ctx, txids, ts); err != nil {
		p.logger.Warn("reaper: mark rebroadcast failed", zap.Error(err))
	}
}
