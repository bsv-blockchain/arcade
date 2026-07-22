package webhook

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// webhookReaperLeaseName is the well-known key every replica uses to
// coordinate retry-sweep ownership. One lease per arcade deployment.
const webhookReaperLeaseName = "webhook-reaper"

// webhookReaperBatch caps how many submissions a single tick re-fires. Same
// shape as propagation's reaperRebroadcastBatch — bounded so a backlog can't
// pin the reaper into a multi-minute call, and so a too-broad scan doesn't
// hold the worker pool against a sudden burst of new deliveries.
const webhookReaperBatch = 200

// runReaper drives the retry sweep ticker. tryReap runs once on entry so a
// just-elected leader doesn't have to wait a full interval before draining
// any already-due backlog. A best-effort lease release on shutdown lets a
// successor take over without waiting for TTL expiry.
func (s *Service) runReaper(ctx context.Context) {
	s.tryReap(ctx)

	ticker := time.NewTicker(s.reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if s.leaser != nil {
				releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				_ = s.leaser.Release(releaseCtx, webhookReaperLeaseName, s.holderID)
				cancel()
			}
			return
		case <-ticker.C:
			s.tryReap(ctx)
		}
	}
}

// tryReap acquires or renews the reaper lease before scanning. A non-leader
// tick is a no-op; lease errors are logged and treated as "not leader" for
// this tick. Matches services/propagation/reaper.go:tryReap.
func (s *Service) tryReap(ctx context.Context) {
	if s.leaser == nil {
		// Caller asked for the reaper to be skipped — defensive.
		return
	}
	heldUntil, err := s.leaser.TryAcquireOrRenew(ctx, webhookReaperLeaseName, s.holderID, s.leaseTTL)
	if err != nil {
		metrics.WebhookReaperTickTotal.WithLabelValues("lease_error").Inc()
		metrics.WebhookReaperLease.Set(0)
		s.logger.Warn("webhook reaper: lease check failed, skipping tick", zap.Error(err))
		return
	}
	if heldUntil.IsZero() {
		metrics.WebhookReaperTickTotal.WithLabelValues("skipped_no_leader").Inc()
		metrics.WebhookReaperLease.Set(0)
		s.logger.Debug("webhook reaper: not leader, skipping tick")
		return
	}
	metrics.WebhookReaperLease.Set(1)
	metrics.WebhookReaperTickTotal.WithLabelValues("ran").Inc()
	s.reapOnce(ctx)
}

// reapOnce drains up to webhookReaperBatch ready-for-retry submissions and
// re-fires deliver for each. The synthesized status uses sub.LastDeliveredStatus
// — the CAS advanced that value at the original claim, so a new CAS with
// expected == sub.LastDeliveredStatus matches the row's current value and the
// write proceeds as a no-op on LastDeliveredStatus while still clearing
// retry_count and next_retry_at. The captured sub.RetryCount preserves the
// backoff counter across the CAS-clear so recordFailure can bump it correctly.
//
// If a NEW event has advanced LastDeliveredStatus between the list query and
// the CAS, the CAS legitimately loses (WebhookCASLostTotal increments) and the
// reaper drops this row for the tick — acceptable, the newer transition is
// what should be delivered.
func (s *Service) reapOnce(ctx context.Context) {
	subs, err := s.store.ListSubmissionsReadyForRetry(ctx, time.Now(), webhookReaperBatch)
	if err != nil {
		s.logger.Warn("webhook reaper: list ready-for-retry failed", zap.Error(err))
		return
	}
	// Publish depth before the early-return so dashboards see "backlog
	// drained" instead of a stale non-zero gauge — same correction the
	// propagation reaper makes.
	metrics.WebhookReaperReadyDepth.Set(float64(len(subs)))

	if len(subs) == 0 {
		return
	}
	s.logger.Info("webhook reaper: retrying ready deliveries", zap.Int("count", len(subs)))

	for _, sub := range subs {
		if err := ctx.Err(); err != nil {
			return
		}
		if sub.CallbackURL == "" {
			// SSE-only subscription — shouldn't normally land in
			// ready-for-retry, but defensively skip.
			continue
		}
		status := &models.TransactionStatus{
			TxID:      sub.TxID,
			Status:    sub.LastDeliveredStatus,
			Timestamp: time.Now(),
		}
		// Redeliver the exact transition we failed on: Status stays
		// LastDeliveredStatus so deliver()'s CAS (expected=LastDeliveredStatus →
		// next=status.Status) is a no-op advance that clears retry state. For a
		// mined/immutable redelivery, graft the block context + merkle proof
		// from the stored row (GetStatus enriches merklePath) so retried
		// callbacks match the shape of a first-attempt delivery. The lean status
		// has no BlockHash, so EnrichMerklePath alone would no-op here — GetStatus
		// supplies both. Best-effort: on any error, redeliver without the proof.
		if status.Status == models.StatusMined || status.Status == models.StatusImmutable {
			if full, err := s.store.GetStatus(ctx, sub.TxID); err == nil && full != nil {
				status.BlockHash = full.BlockHash
				status.BlockHeight = full.BlockHeight
				status.MerklePath = full.MerklePath
			}
		}
		s.deliver(ctx, sub, status)
	}
}
