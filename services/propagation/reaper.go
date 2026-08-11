package propagation

import (
	"context"
	"errors"
	"net/url"
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
// stalePendingRetryAge: a row parked at PENDING_RETRY by the propagation
// requeue-budget escape (parkExhaustedRequeues) got no verdict from any peer
// across its whole fast-path budget, so its dispatcher in-flight entry was
// released to free the Kafka commit watermark and the reaper inherited the
// retry. Much shorter than the other thresholds — the row is not "stuck", it
// is explicitly queued for another attempt, and the failure that parked it
// (a peer outage, a conflict line arcade could not attribute) typically
// clears in minutes. Long enough that a tx cannot bounce between the
// fast path and the reaper faster than the fast path's own budget.
//
// staleScanLookback bounds how far back IterateStatusesSince walks. Rows
// older than this are assumed permanently stuck and outside the reaper's
// responsibility.
const (
	staleSeenOnNetworkAge     = time.Hour
	staleReceivedAge          = time.Hour
	staleAcceptedByNetworkAge = time.Hour
	stalePendingRetryAge      = 5 * time.Minute
	staleScanLookback         = 24 * time.Hour

	// defaultReaperRebroadcastBatch is the per-tick rebroadcast cap applied
	// when propagation.reaper_rebroadcast_batch is unset or non-positive —
	// the source of truth for the historical hardcoded 200. See
	// config.PropagationConfig.ReaperRebroadcastBatch for the incident
	// rationale behind making it configurable.
	defaultReaperRebroadcastBatch = 200

	// stuckTransientAge is the threshold for the StuckTransientTxs /
	// OldestTransientTxAge gauges: a tx sitting in RECEIVED or
	// ACCEPTED_BY_NETWORK longer than this counts as stuck — the SEEN
	// state-transfer for it never arrived. Kept equal to the rebroadcast
	// stale ages so "stuck" means the same thing everywhere.
	stuckTransientAge = time.Hour
)

// errStopReaperWalk stops the IterateStatusesSince scan once the rebroadcast
// batch is full. It is a control signal, not a failure — reapOnce filters it
// out of the scan error check. Restoring this early stop (issue #290) is safe
// because the stuck census no longer rides on the walk; CensusStatusesSince
// answers it store-side, so the walk only needs a bounded rebroadcast batch.
var errStopReaperWalk = errors.New("propagation: rebroadcast batch full")

// stuckTransientStatuses are the transient statuses the stuck gauges track.
// Both are now nudged by the rebroadcast arm below (RECEIVED past
// staleReceivedAge, ACCEPTED_BY_NETWORK past staleAcceptedByNetworkAge); the
// gauges stay as the census of what's still stuck after those nudges.
var stuckTransientStatuses = []models.Status{
	models.StatusReceived,
	models.StatusAcceptedByNetwork,
}

// stuckAttributionCap bounds how many stuck txs one tick attributes to a
// callback host. The uncapped totals live on StuckTransientTxs; the per-host
// breakdown does one submission lookup per attributed tx, so cap it to keep
// the leader tick cheap even when the stuck set is huge (a real incident).
const stuckAttributionCap = 500

// zeroStuckTransientGauges resets the leader-computed stuck gauges. Called on
// non-leader ticks so a pod that lost the lease doesn't keep exporting its
// last leader-era values (the current leader's values then win under max()).
func zeroStuckTransientGauges() {
	for _, st := range stuckTransientStatuses {
		metrics.StuckTransientTxs.WithLabelValues(string(st)).Set(0)
		metrics.OldestTransientTxAge.WithLabelValues(string(st)).Set(0)
	}
	// The by-callback vec has dynamic host labels, so Reset (not per-label
	// Set(0)) is the only way to drop hosts this pod is no longer leader for.
	metrics.StuckTransientTxsByCallback.Reset()
}

// callbackHost extracts the hostname from a submission CallbackURL for the
// per-client stuck breakdown, or "none" when there is no usable URL.
func callbackHost(rawURL string) string {
	if rawURL == "" {
		return "none"
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return "unparsed"
	}
	return u.Hostname()
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
// arrived — re-register + re-broadcast to nudge it toward SEEN), and
// PENDING_RETRY past stalePendingRetryAge (parked by the propagation
// requeue-budget escape after getting no verdict from any peer; the reaper is
// its only remaining retry path). All carry RawTx and are validated txs we
// accepted responsibility to propagate, so rebroadcasting self-heals a
// transient downstream outage. Recent rows (still in the normal intake/requeue
// path) and body-less rows are left alone.
//
// Rebroadcasts go through the same registerBatch + broadcastInChunks +
// applyTerminalStatuses pipeline as processBatch but bypass the
// dispatcher's admission — these rows are no longer in inFlight, so any
// resulting terminal status notifies the dispatcher via applyTerminalStatuses
// only as a no-op for offset bookkeeping.
//
// Bounded by p.rebroadcastBatch (propagation.reaper_rebroadcast_batch,
// default defaultReaperRebroadcastBatch) per tick so a backlog can't pin the
// reaper into a single multi-minute call.
func (p *Propagator) reapOnce(ctx context.Context) {
	now := time.Now()
	since := now.Add(-staleScanLookback)
	seenDeadline := now.Add(-staleSeenOnNetworkAge)
	receivedDeadline := now.Add(-staleReceivedAge)
	acceptedDeadline := now.Add(-staleAcceptedByNetworkAge)
	pendingRetryDeadline := now.Add(-stalePendingRetryAge)

	stuckTransientDeadline := now.Add(-stuckTransientAge)
	// The stuck-transient census (counts + oldest age per status) is resolved
	// store-side by CensusStatusesSince after the walk, NOT by counting inside
	// it. Counting in the walk forced it to visit every row so the census
	// stayed uncapped, which pinned the reaper's cadence to the full-scan time
	// (~4 min on a ~1.6M-row store) instead of reaper_interval_ms — issue #290.
	stuckTransientSet := make(map[models.Status]struct{}, len(stuckTransientStatuses))
	for _, s := range stuckTransientStatuses {
		stuckTransientSet[s] = struct{}{}
	}
	// txids of stuck rows to attribute to a callback host after the scan
	// (bounded by stuckAttributionCap). Collected in the walk — not looked up
	// inline — so the scan itself stays free of per-row store calls. Best
	// effort: the walk stops at the rebroadcast batch, so attribution samples
	// the leading portion of the backlog.
	type stuckRef struct {
		txid   string
		status models.Status
	}
	attrib := make([]stuckRef, 0, stuckAttributionCap)

	stuck := make([]propagationMsg, 0, p.rebroadcastBatch)
	err := p.store.IterateStatusesSince(ctx, since, func(st *models.TransactionStatus) error {
		// Best-effort per-callback attribution sample (bounded by
		// stuckAttributionCap). The census COUNTS come from
		// CensusStatusesSince below, not from this walk.
		if _, ok := stuckTransientSet[st.Status]; ok &&
			st.Timestamp.Before(stuckTransientDeadline) &&
			len(attrib) < stuckAttributionCap {
			attrib = append(attrib, stuckRef{txid: st.TxID, status: st.Status})
		}

		if len(stuck) >= p.rebroadcastBatch {
			// Rebroadcast batch is full — stop the walk (issue #290). The
			// census no longer needs a full scan (CensusStatusesSince does it
			// store-side), so a large backlog drains one batch per tick at
			// reaper_interval_ms cadence instead of a multi-minute full scan.
			return errStopReaperWalk
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
		case models.StatusPendingRetry:
			// Parked by parkExhaustedRequeues: the fast path spent its whole
			// requeue budget without a verdict and released the tx's Kafka
			// offset so the consumer could keep moving. The reaper is now the
			// ONLY thing that will retry it, so this arm is what makes the
			// poison-batch escape a park rather than a drop. A successful
			// rebroadcast lifts the row straight to ACCEPTED_BY_NETWORK /
			// REJECTED (the lattice permits both from PENDING_RETRY); another
			// no-verdict result leaves it here for the next tick.
			if !st.Timestamp.Before(pendingRetryDeadline) {
				return nil
			}
		default:
			// Terminal statuses (REJECTED, DOUBLE_SPEND_ATTEMPTED, MINED,
			// IMMUTABLE) and the remaining transient in-between states are
			// not the reaper's job to rebroadcast.
			return nil
		}
		stuck = append(stuck, propagationMsg{TXID: st.TxID, RawTx: st.RawTx})
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errStopReaperWalk) {
		p.logger.Error("reaper: scan failed", zap.Error(err))
		return
	}

	// Resolve the stuck-transient census store-side (one GROUP BY aggregate on
	// Postgres) so the counts stay uncapped without the walk visiting every
	// row (issue #290). A census error must not skip the rebroadcast that
	// follows — the gauges are observability, the drain is the job — so log it
	// and publish zeroed counts.
	census, cerr := p.store.CensusStatusesSince(ctx, since, stuckTransientDeadline, stuckTransientStatuses)
	if cerr != nil {
		p.logger.Error("reaper: stuck-transient census failed", zap.Error(cerr))
		census = nil
	}

	// Publish the post-scan depth on every tick BEFORE the early-return
	// so the gauge reflects "what the last reaper observed" — including
	// the queue-is-clear case. Setting it only on the non-empty branch
	// leaves a stale non-zero value visible to dashboards after the
	// backlog drains, which used to make the metric misleading.
	metrics.PropagationReaperReadyDepth.Set(float64(len(stuck)))

	// One-line backlog snapshot per tick (~reaperInterval, leader only):
	// pending is the dispatcher's next-flush accumulator (capped at
	// max_pending by admission backpressure), inflight is its full
	// admitted-but-not-terminal census (uncapped — each entry pins an
	// uncommitted Kafka offset, so this is the log-visible proxy for the
	// replay backlog), and reaper_ready is this scan's stale-row batch.
	// During the 2026-08-10 load test a ~466k-tx uncommitted-offset backlog
	// was invisible without a DB/Kafka query; this line makes growth
	// visible in plain logs.
	p.logger.Info("reaper: propagation backlog depth",
		zap.Int64("pending_depth", p.pendingDepth.Load()),
		zap.Int64("inflight_depth", p.inflightDepth.Load()),
		zap.Int("reaper_ready_depth", len(stuck)),
	)

	// Publish the stuck-transient census every tick, zero included (iterate the
	// status list, not the result map, so a drained or errored census still
	// resets the gauges), so they always reflect the latest completed scan.
	for _, status := range stuckTransientStatuses {
		c := census[status] // zero value when census is nil or nothing matched
		metrics.StuckTransientTxs.WithLabelValues(string(status)).Set(float64(c.Count))
		age := 0.0
		if !c.Oldest.IsZero() {
			age = now.Sub(c.Oldest).Seconds()
		}
		metrics.OldestTransientTxAge.WithLabelValues(string(status)).Set(age)
		if c.Count > 0 {
			p.logger.Warn("reaper: transactions stuck in transient status past threshold",
				logfields.Status(string(status)),
				zap.Int64("count", c.Count),
				zap.Float64("oldest_age_seconds", age))
		}
	}

	// Per-client breakdown of the stuck census: attribute each collected stuck
	// tx to the callback host(s) of its submission(s), so an operator can see
	// which client owns the backlog. One submission lookup per attributed tx
	// (bounded by stuckAttributionCap). Reset the vec first so hosts that
	// drained since the last tick stop reporting (dynamic host labels).
	byCallback := make(map[models.Status]map[string]int)
	for _, ref := range attrib {
		if err := ctx.Err(); err != nil {
			break
		}
		subs, subErr := p.store.GetSubmissionsByTxID(ctx, ref.txid)
		if subErr != nil {
			continue // best-effort: the uncapped totals are already published
		}
		hosts := map[string]struct{}{}
		for _, sub := range subs {
			hosts[callbackHost(sub.CallbackURL)] = struct{}{}
		}
		if len(hosts) == 0 {
			hosts["none"] = struct{}{} // stuck tx with no registered callback
		}
		for h := range hosts {
			if byCallback[ref.status] == nil {
				byCallback[ref.status] = map[string]int{}
			}
			byCallback[ref.status][h]++
		}
	}
	metrics.StuckTransientTxsByCallback.Reset()
	for status, hosts := range byCallback {
		for host, n := range hosts {
			metrics.StuckTransientTxsByCallback.WithLabelValues(string(status), host).Set(float64(n))
		}
	}
	if len(attrib) >= stuckAttributionCap {
		p.logger.Warn("reaper: stuck-by-callback attribution capped",
			zap.Int("cap", stuckAttributionCap))
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
