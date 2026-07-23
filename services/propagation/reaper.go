package propagation

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/logfields"
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
// staleReceivedAge: a row still at RECEIVED older than this never advanced to
// a network verdict — its broadcast produced no verdict (downstream timeout /
// 5xx / no per-slot body) and the in-memory requeue was lost (propagation
// restart, ctx cancellation), or an intake Kafka-publish failure stranded it
// and the submitter never retried. Rebroadcast to self-heal: the row carries
// RawTx and is a validated tx (intake validation failures land at REJECTED,
// never RECEIVED), so we accepted responsibility to propagate it. Long enough
// that the normal intake→ACCEPTED path (seconds) and the 2s in-memory requeue
// are never pre-empted by the reaper.
//
// staleAcceptedByNetworkAge: a row at ACCEPTED_BY_NETWORK older than this was
// accepted into a mempool but never advanced to SEEN — the merkle-service
// state-transfer that drives SEEN never arrived (the /watch registration was
// lost, or the tx propagated too narrowly for merkle-service's p2p to observe
// it). Rebroadcast to nudge it: re-registering gives merkle-service another
// chance to watch and detect the tx, and re-broadcasting pushes it to more
// peers so it can propagate wider. Safe to repeat every tick — the lattice
// forbids ACCEPTED_BY_NETWORK → ACCEPTED_BY_NETWORK
// (models.Status.DisallowedPreviousStatuses), so a still-accepted rebroadcast
// result is a store no-op: the stuck gauge drains only when the tx genuinely
// reaches SEEN/MINED/REJECTED, never via a spurious timestamp refresh.
//
// staleScanLookback bounds how far back IterateStatusesSince walks. Rows
// older than this are assumed permanently stuck and outside the reaper's
// responsibility.
const (
	staleSeenOnNetworkAge     = time.Hour
	staleReceivedAge          = time.Hour
	staleAcceptedByNetworkAge = time.Hour
	staleScanLookback         = 24 * time.Hour
	reaperRebroadcastBatch    = 200

	// stuckTransientAge is the threshold for the StuckTransientTxs /
	// OldestTransientTxAge gauges: a tx sitting in RECEIVED or
	// ACCEPTED_BY_NETWORK longer than this counts as stuck — the SEEN
	// state-transfer for it never arrived. Kept equal to the rebroadcast
	// stale ages so "stuck" means the same thing everywhere.
	stuckTransientAge = time.Hour
)

// stuckTransientStatuses are the transient statuses the stuck gauges track.
// Both are now nudged by the rebroadcast arm below (RECEIVED past
// staleReceivedAge, ACCEPTED_BY_NETWORK past staleAcceptedByNetworkAge); the
// gauges stay as the census of what's still stuck after those nudges.
var stuckTransientStatuses = []models.Status{
	models.StatusReceived,
	models.StatusAcceptedByNetwork,
}

// zeroStuckTransientGauges resets the leader-computed stuck gauges. Called on
// non-leader ticks so a pod that lost the lease doesn't keep exporting its
// last leader-era values (the current leader's values then win under max()).
func zeroStuckTransientGauges() {
	for _, st := range stuckTransientStatuses {
		metrics.StuckTransientTxs.WithLabelValues(string(st)).Set(0)
		metrics.OldestTransientTxAge.WithLabelValues(string(st)).Set(0)
	}
}

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
			zeroStuckTransientGauges()
			p.logger.Debug("reaper: not leader, skipping tick")
			return
		}
		metrics.PropagationReaperLease.Set(1)
	}
	metrics.PropagationReaperTickTotal.WithLabelValues("ran").Inc()
	p.reapOnce(ctx)
}

// reapOnce rebroadcasts rows stuck past their stale-age threshold:
// SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES past staleSeenOnNetworkAge (peer
// mempool eviction, dropped BLOCK_PROCESSED callback, fee bump needed),
// RECEIVED past staleReceivedAge (a no-verdict broadcast whose in-memory
// requeue was lost, or an intake Kafka-publish failure the submitter never
// retried), and ACCEPTED_BY_NETWORK past staleAcceptedByNetworkAge (accepted
// into a mempool but the SEEN state-transfer from merkle-service never
// arrived — re-register + re-broadcast to nudge it toward SEEN). All carry
// RawTx and are validated txs we accepted responsibility to propagate, so
// rebroadcasting self-heals a transient downstream outage. Recent rows (still
// in the normal intake/requeue path) and body-less rows are left alone.
//
// Rebroadcasts go through the same registerBatch + broadcastInChunks +
// applyTerminalStatuses pipeline as processBatch but bypass the
// dispatcher's admission — these rows are no longer in inFlight, so any
// resulting terminal status notifies the dispatcher via applyTerminalStatuses
// only as a no-op for offset bookkeeping.
//
// Bounded by reaperRebroadcastBatch per tick so a backlog can't pin the
// reaper into a single multi-minute call.
func (p *Propagator) reapOnce(ctx context.Context) {
	now := time.Now()
	since := now.Add(-staleScanLookback)
	seenDeadline := now.Add(-staleSeenOnNetworkAge)
	receivedDeadline := now.Add(-staleReceivedAge)
	acceptedDeadline := now.Add(-staleAcceptedByNetworkAge)

	stuckTransientDeadline := now.Add(-stuckTransientAge)
	type transientStats struct {
		count  int
		oldest time.Time
	}
	stuckTransient := make(map[models.Status]*transientStats, len(stuckTransientStatuses))
	for _, s := range stuckTransientStatuses {
		stuckTransient[s] = &transientStats{}
	}

	stuck := make([]propagationMsg, 0, reaperRebroadcastBatch)
	err := p.store.IterateStatusesSince(ctx, since, func(st *models.TransactionStatus) error {
		// Stuck-transient accounting runs on EVERY row (regardless of RawTx
		// or the rebroadcast cap): the gauges must be an uncapped census of
		// txs whose SEEN state-transfer never arrived, including
		// ACCEPTED_BY_NETWORK rows the rebroadcast arm below never touches.
		if ts, ok := stuckTransient[st.Status]; ok && st.Timestamp.Before(stuckTransientDeadline) {
			ts.count++
			if ts.oldest.IsZero() || st.Timestamp.Before(ts.oldest) {
				ts.oldest = st.Timestamp
			}
		}

		if len(stuck) >= reaperRebroadcastBatch {
			// Rebroadcast batch is full — keep walking for the census, just
			// stop collecting. (Previously the walk aborted here, which also
			// capped any counting at the batch size.)
			return nil
		}
		if len(st.RawTx) == 0 {
			// No body to rebroadcast. Pre-reaper-population rows
			// won't have it; just skip.
			return nil
		}
		switch st.Status {
		case models.StatusSeenOnNetwork, models.StatusSeenMultipleNodes:
			if !st.Timestamp.Before(seenDeadline) {
				return nil
			}
		case models.StatusReceived:
			// A RawTx-carrying row still at RECEIVED past staleReceivedAge
			// never got a network verdict — its no-verdict requeue was lost,
			// or intake's Kafka publish failed and the submitter never
			// retried. Rebroadcast to self-heal. Recent rows are still in
			// the normal intake/requeue path, so leave them alone.
			if !st.Timestamp.Before(receivedDeadline) {
				return nil
			}
		case models.StatusAcceptedByNetwork:
			// Accepted into a mempool but never advanced to SEEN past
			// staleAcceptedByNetworkAge — the merkle-service SEEN
			// state-transfer never arrived. Rebroadcast re-registers the tx
			// with merkle-service (another chance to watch + detect) and
			// re-broadcasts to more peers (wider propagation). If it stays
			// ACCEPTED the lattice makes the resulting write a no-op, so this
			// only ever helps drain the stuck gauge, never masks it. Recent
			// rows are still in the normal ACCEPTED→SEEN window, so leave
			// them alone.
			if !st.Timestamp.Before(acceptedDeadline) {
				return nil
			}
		default:
			// Terminal statuses (REJECTED, DOUBLE_SPEND_ATTEMPTED, MINED,
			// IMMUTABLE) and transient in-between states are not the
			// reaper's job to rebroadcast.
			return nil
		}
		stuck = append(stuck, propagationMsg{TXID: st.TxID, RawTx: st.RawTx})
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		p.logger.Error("reaper: scan failed", zap.Error(err))
		return
	}

	// Publish the post-scan depth on every tick BEFORE the early-return
	// so the gauge reflects "what the last reaper observed" — including
	// the queue-is-clear case. Setting it only on the non-empty branch
	// leaves a stale non-zero value visible to dashboards after the
	// backlog drains, which used to make the metric misleading.
	metrics.PropagationReaperReadyDepth.Set(float64(len(stuck)))

	// Publish the stuck-transient census the same way: every tick, zero
	// included, so the gauges always reflect the latest completed scan.
	for status, ts := range stuckTransient {
		metrics.StuckTransientTxs.WithLabelValues(string(status)).Set(float64(ts.count))
		age := 0.0
		if !ts.oldest.IsZero() {
			age = now.Sub(ts.oldest).Seconds()
		}
		metrics.OldestTransientTxAge.WithLabelValues(string(status)).Set(age)
		if ts.count > 0 {
			p.logger.Warn("reaper: transactions stuck in transient status past threshold",
				logfields.Status(string(status)),
				zap.Int("count", ts.count),
				zap.Float64("oldest_age_seconds", age))
		}
	}

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
	registered, _ := p.registerBatch(ctx, stuck)
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
		case txResultClassUnknown, txResultClassRequeue:
			// Requeue / Unknown from the reaper's rebroadcast path:
			// leave the row alone so the next reaper tick picks it up.
			// The reaper bypasses the dispatcher, so there's no inFlight
			// entry to requeue against — natural retry is just the next
			// tick.
		}
	}
	p.applyTerminalStatuses(ctx, terminalStatuses, accepted, rejected)
}
