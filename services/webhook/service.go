// Package webhook delivers transaction status updates to client-supplied
// callback URLs. The service subscribes to events.Publisher, looks up the
// submission(s) registered for each updated txid, applies the
// FullStatusUpdates filter, and POSTs the status JSON to the registered URL.
// Failed deliveries are retried with exponential backoff; retry state is
// persisted via store.UpdateDeliveryStatus so a process restart never loses
// pending callbacks.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/callbackurl"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Service is the entry point for webhook delivery. Wire it up in main.go for
// every process that should ship callbacks (typically the api-server, but
// could be a dedicated mode if operators want to scale delivery separately).
type Service struct {
	cfg       config.WebhookConfig
	logger    *zap.Logger
	publisher events.Publisher
	store     store.Store
	client    *http.Client

	mu      sync.Mutex
	running bool
}

// New constructs a Service. publisher must be non-nil; store provides
// submission lookup and retry persistence. The HTTP client is constructed
// here so each Service has its own pool — keeps tests hermetic.
//
// callbackCfg threads the SSRF guard through to the dialer: when
// AllowPrivateIPs is false (the default), the underlying transport
// refuses to connect to loopback / link-local / RFC1918 / metadata IPs
// even if a host previously survived registration-time validation
// (catches DNS rebinding). See finding F-017 / issue #75.
func New(cfg config.WebhookConfig, callbackCfg config.CallbackConfig, logger *zap.Logger, publisher events.Publisher, st store.Store) *Service {
	timeout := time.Duration(cfg.HTTPTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Service{
		cfg:       cfg,
		logger:    logger.Named("webhook"),
		publisher: publisher,
		store:     st,
		client:    newCallbackClient(timeout, callbackCfg.AllowPrivateIPs),
	}
}

// newCallbackClient builds an http.Client whose dialer enforces the SSRF
// guard. The Dialer.Control hook fires after DNS resolution but before
// connect(), so a hostname that resolves to a banned IP fails fast with
// an error from the callbackurl package — the request never leaves the
// machine. Pulled out of New so tests can construct an equivalent client
// without instantiating the whole Service.
func newCallbackClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
		Control:   callbackurl.DialControl(allowPrivate),
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: timeout,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
			IdleConnTimeout:       90 * time.Second,
		},
	}
}

// Name satisfies the service interface used by main.go.
func (s *Service) Name() string { return "webhook" }

// Start subscribes to the event stream and spins up a goroutine that
// dispatches each update to the configured CallbackURL(s). Blocks until ctx
// is canceled.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("webhook service already running")
	}
	s.running = true
	s.mu.Unlock()

	ch, err := s.publisher.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribing to events publisher: %w", err)
	}
	s.logger.Info("webhook service started",
		zap.Int("max_retries", s.cfg.MaxRetries),
		zap.Int("http_timeout_ms", s.cfg.HTTPTimeoutMs),
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case status, ok := <-ch:
			if !ok {
				return nil
			}
			if status == nil {
				continue
			}
			s.handleUpdate(ctx, status)
		}
	}
}

// Stop is a no-op — the service shuts down via the ctx passed to Start.
// Defined to satisfy the lifecycle interface main.go uses.
func (s *Service) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	return nil
}

// handleUpdate dispatches a single status update to every submission
// registered for that txid.
func (s *Service) handleUpdate(ctx context.Context, status *models.TransactionStatus) {
	subs, err := s.store.GetSubmissionsByTxID(ctx, status.TxID)
	if err != nil {
		s.logger.Warn("submission lookup failed",
			zap.String("txid", status.TxID),
			zap.Error(err),
		)
		return
	}
	for _, sub := range subs {
		if sub.CallbackURL == "" {
			continue // SSE-only subscription; no webhook to send
		}
		if !shouldDeliver(sub, status) {
			continue
		}
		s.deliver(ctx, sub, status)
	}
}

// shouldDeliver implements the FullStatusUpdates / dedup gating rules:
//   - When FullStatusUpdates is true, every distinct status transition is
//     delivered.
//   - When FullStatusUpdates is false (the default), only terminal statuses
//     (MINED, REJECTED, MINED_IN_STALE_BLOCK / IMMUTABLE) are delivered.
//   - Same status as LastDeliveredStatus is suppressed (idempotent dedup).
func shouldDeliver(sub *models.Submission, status *models.TransactionStatus) bool {
	if sub.LastDeliveredStatus == status.Status {
		return false
	}
	if sub.FullStatusUpdates {
		return true
	}
	switch status.Status {
	case models.StatusMined, models.StatusRejected, models.StatusImmutable, models.StatusDoubleSpendAttempted:
		return true
	default:
		return false
	}
}

// deliver attempts a single POST and records the outcome. On failure, it
// schedules a retry by writing RetryCount+1 and NextRetryAt back to the
// store; the retry is picked up by a subsequent invocation rather than
// looping inline, so a slow callback target doesn't block other deliveries.
//
// On success, it writes LastDeliveredStatus and clears retry state.
func (s *Service) deliver(ctx context.Context, sub *models.Submission, status *models.TransactionStatus) {
	logger := s.logger.With(
		zap.String("txid", status.TxID),
		zap.String("callback_url", sub.CallbackURL),
		zap.String("status", string(status.Status)),
	)

	body, err := json.Marshal(status)
	if err != nil {
		logger.Warn("encoding status payload failed", zap.Error(err))
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.CallbackURL, bytes.NewReader(body))
	if err != nil {
		logger.Warn("building callback request failed", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if sub.CallbackToken != "" {
		req.Header.Set("Authorization", "Bearer "+sub.CallbackToken)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		s.recordFailure(ctx, sub, status, logger, err.Error())
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.recordFailure(ctx, sub, status, logger, fmt.Sprintf("status %d", resp.StatusCode))
		return
	}

	if err := s.store.UpdateDeliveryStatus(ctx, sub.SubmissionID, status.Status, 0, nil); err != nil {
		logger.Warn("updating delivery status after success failed", zap.Error(err))
	}
	logger.Debug("callback delivered")
}

// recordFailure increments the submission's retry counter and computes the
// next-retry timestamp using exponential backoff bounded by MaxBackoffMs.
// Once RetryCount exceeds MaxRetries the submission is marked terminal so
// the reaper / future retry sweeps skip it.
func (s *Service) recordFailure(ctx context.Context, sub *models.Submission, status *models.TransactionStatus, logger *zap.Logger, reason string) {
	maxRetries := s.cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10
	}
	nextCount := sub.RetryCount + 1
	logger.Warn("callback delivery failed", zap.Int("retry_count", nextCount), zap.String("reason", reason))

	if nextCount >= maxRetries {
		// Stop retrying — record the attempt count but don't schedule another.
		if err := s.store.UpdateDeliveryStatus(ctx, sub.SubmissionID, status.Status, nextCount, nil); err != nil {
			logger.Warn("updating delivery status on final failure failed", zap.Error(err))
		}
		return
	}

	backoff := s.computeBackoff(nextCount)
	next := time.Now().Add(backoff)
	if err := s.store.UpdateDeliveryStatus(ctx, sub.SubmissionID, sub.LastDeliveredStatus, nextCount, &next); err != nil {
		logger.Warn("scheduling retry failed", zap.Error(err))
	}
}

// computeBackoff returns the wait time for the n-th retry: initial * 2^(n-1),
// capped by MaxBackoffMs.
func (s *Service) computeBackoff(retryCount int) time.Duration {
	initial := time.Duration(s.cfg.InitialBackoffMs) * time.Millisecond
	if initial <= 0 {
		initial = 5 * time.Second
	}
	maxBackoff := time.Duration(s.cfg.MaxBackoffMs) * time.Millisecond
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Minute
	}
	d := initial
	for i := 1; i < retryCount; i++ {
		d *= 2
		if d >= maxBackoff {
			return maxBackoff
		}
	}
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
