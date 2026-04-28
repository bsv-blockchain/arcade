package propagation

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// runReaper is the reaper goroutine entrypoint. It ticks on the configured
// interval and calls reapOnce each tick, gated by a cluster-wide lease so
// only one replica at a time performs rebroadcast work. An initial tick fires
// immediately so a freshly-started pod with leftover PENDING_RETRY rows makes
// progress without waiting a full interval.
//
// If no leaser is configured (tests, single-process deploys that don't need
// coordination), the reaper runs unguarded.
func (p *Propagator) runReaper(ctx context.Context) {
	p.tryReap(ctx)

	ticker := time.NewTicker(p.reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Best-effort release so a successor doesn't wait for TTL.
			// Use Background since ctx is already done.
			if p.leaser != nil {
				_ = p.leaser.Release(context.Background(), reaperLeaseName, p.holderID)
			}
			return
		case <-ticker.C:
			p.tryReap(ctx)
		}
	}
}

// tryReap acquires (or renews) the reaper lease before performing work. A
// non-leader tick is a no-op. Infrastructure errors on the lease path are
// logged and treated as "not leader for this tick" — we'll try again next
// tick rather than risk a split-brain reap.
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

// reapOnce pulls up to reaperBatchSize PENDING_RETRY rows whose next_retry_at
// has elapsed, broadcasts them in teranodeBatchCap-sized chunks, and resolves
// each row based on the outcome. Batch-all-rejected falls back to per-tx
// within the failing chunk only.
func (p *Propagator) reapOnce(ctx context.Context) {
	ready, err := p.store.GetReadyRetries(ctx, time.Now(), p.reaperBatchSize)
	if err != nil {
		p.logger.Error("reaper: query ready retries failed", zap.Error(err))
		return
	}
	metrics.PropagationReaperReadyDepth.Set(float64(len(ready)))
	if len(ready) == 0 {
		return
	}

	p.logger.Info("reaper: rebroadcasting pending retries", zap.Int("count", len(ready)))

	rawTxs := make([][]byte, len(ready))
	batch := make([]propagationMsg, len(ready))
	for i, r := range ready {
		rawTxs[i] = r.RawTx
		batch[i] = propagationMsg{TXID: r.TxID, RawTx: r.RawTx}
	}

	// Reuse the same chunk-and-fallback path as processBatch — the retry
	// flow doesn't need its own broadcast machinery, just a different
	// outcome resolver.
	results := p.broadcastInChunks(ctx, batch, rawTxs)
	for i, r := range ready {
		p.resolveRetryOutcome(ctx, r, retryResult{status: results[i].status, errMsg: results[i].errMsg})
	}
}

// retryResult is the per-tx outcome of a reaper broadcast.
type retryResult struct {
	status *models.TransactionStatus
	errMsg string
}

// resolveRetryOutcome applies a broadcast outcome to a PENDING_RETRY row.
//
//   - ACCEPTED_BY_NETWORK            → ClearRetryState(ACCEPTED_BY_NETWORK)
//   - REJECTED + retryable error     → BumpRetryCount, requeue or reject if exhausted
//   - REJECTED + non-retryable error → ClearRetryState(REJECTED, errMsg)
//   - status == nil (no endpoint gave a verdict — 202, all timed out, etc.)
//     → leave PENDING_RETRY, just push next_retry_at forward by one base-
//       backoff step so the reaper doesn't hot-loop on infra blips. retry_count
//       is left alone — don't punish a tx for a transient outage.
func (p *Propagator) resolveRetryOutcome(ctx context.Context, entry *store.PendingRetry, res retryResult) {
	if res.status != nil && res.status.Status == models.StatusAcceptedByNetwork {
		if err := p.store.ClearRetryState(ctx, entry.TxID, models.StatusAcceptedByNetwork, ""); err != nil {
			p.logger.Error("reaper: clear retry state after success failed",
				zap.String("txid", entry.TxID), zap.Error(err))
		}
		return
	}

	if res.status != nil && res.status.Status == models.StatusRejected {
		if IsRetryableError(res.errMsg) {
			p.handleRetryableFailure(ctx, entry.TxID, entry.RawTx)
			return
		}
		if err := p.store.ClearRetryState(ctx, entry.TxID, models.StatusRejected, res.errMsg); err != nil {
			p.logger.Error("reaper: reject non-retryable failed",
				zap.String("txid", entry.TxID), zap.Error(err))
		}
		return
	}

	// status == nil — infra blip. Push next_retry_at forward using the same
	// attempt count so backoff stays bounded but we stop picking the row up
	// every tick. Use attempt=1 as the nudge (equivalent to 2*base, jittered).
	next := ComputeBackoff(p.retryBackoffMs, 1)
	if err := p.store.SetPendingRetryFields(ctx, entry.TxID, entry.RawTx, next); err != nil {
		p.logger.Warn("reaper: nudge next_retry_at failed",
			zap.String("txid", entry.TxID), zap.Error(err))
	}
}
