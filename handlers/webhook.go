package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// WebhookHandler handles webhook delivery for transaction status updates
type WebhookHandler struct {
	eventPublisher  events.Publisher
	submissionStore store.SubmissionStore
	statusStore     store.StatusStore
	httpClient      *http.Client
	logger          *slog.Logger
	stopCh          chan struct{}
	pruneInterval   time.Duration
	maxAge          time.Duration
	maxRetries      int
}

// WebhookPayload represents the payload sent to webhook endpoints
type WebhookPayload struct {
	TxID         string    `json:"txid"`
	Status       string    `json:"status"`
	Timestamp    time.Time `json:"timestamp"`
	BlockHash    string    `json:"blockHash,omitempty"`
	BlockHeight  uint64    `json:"blockHeight,omitempty"`
	MerklePath   string    `json:"merklePath,omitempty"`
	ExtraInfo    string    `json:"extraInfo,omitempty"`
	CompetingTxs []string  `json:"competingTxs,omitempty"`
}

// NewWebhookHandler creates a new webhook handler
func NewWebhookHandler(
	eventPublisher events.Publisher,
	submissionStore store.SubmissionStore,
	statusStore store.StatusStore,
	logger *slog.Logger,
	pruneInterval time.Duration,
	maxAge time.Duration,
	maxRetries int,
) *WebhookHandler {
	return &WebhookHandler{
		eventPublisher:  eventPublisher,
		submissionStore: submissionStore,
		statusStore:     statusStore,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger:        logger,
		stopCh:        make(chan struct{}),
		pruneInterval: pruneInterval,
		maxAge:        maxAge,
		maxRetries:    maxRetries,
	}
}

// Start begins processing webhook deliveries
func (h *WebhookHandler) Start(ctx context.Context) error {
	h.logger.Info("Starting webhook handler")

	eventCh, err := h.eventPublisher.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe to events: %w", err)
	}

	go h.processEvents(ctx, eventCh)
	go h.pruneExpiredSubmissions(ctx)

	return nil
}

// Stop stops the webhook handler
func (h *WebhookHandler) Stop() {
	h.logger.Info("Stopping webhook handler")
	close(h.stopCh)
}

// processEvents processes incoming status update events
func (h *WebhookHandler) processEvents(ctx context.Context, eventCh <-chan models.StatusUpdate) {
	for {
		select {
		case <-ctx.Done():
			h.logger.Info("Context cancelled, stopping event processing")
			return
		case <-h.stopCh:
			h.logger.Info("Stop signal received, stopping event processing")
			return
		case event, ok := <-eventCh:
			if !ok {
				h.logger.Info("Event channel closed, stopping event processing")
				return
			}
			h.handleEvent(ctx, event)
		}
	}
}

// handleEvent handles a single status update event
func (h *WebhookHandler) handleEvent(ctx context.Context, event models.StatusUpdate) {
	submissions, err := h.submissionStore.GetSubmissionsByTxID(ctx, event.TxID)
	if err != nil {
		h.logger.Error("Failed to get submissions",
			slog.String("txid", event.TxID),
			slog.String("error", err.Error()))
		return
	}

	for _, sub := range submissions {
		if sub.CallbackURL == "" {
			continue
		}

		if sub.LastDeliveredStatus == event.Status {
			continue
		}

		go h.deliverWebhook(ctx, *sub, event)
	}
}

// pruneExpiredSubmissions periodically removes expired submissions
func (h *WebhookHandler) pruneExpiredSubmissions(ctx context.Context) {
	ticker := time.NewTicker(h.pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.performPruning(ctx)
		}
	}
}

// performPruning removes expired and failed submissions
func (h *WebhookHandler) performPruning(ctx context.Context) {
	h.logger.Info("Pruning expired submissions")
}

// deliverWebhook delivers a webhook for a specific submission
func (h *WebhookHandler) deliverWebhook(ctx context.Context, sub models.Submission, event models.StatusUpdate) {
	statusDetail, err := h.statusStore.GetStatus(ctx, event.TxID)
	if err != nil {
		h.logger.Error("Failed to get status detail",
			slog.String("txid", event.TxID),
			slog.String("error", err.Error()))
		return
	}

	payload := WebhookPayload{
		TxID:         event.TxID,
		Status:       string(event.Status),
		Timestamp:    event.Timestamp,
		BlockHash:    statusDetail.BlockHash,
		BlockHeight:  statusDetail.BlockHeight,
		MerklePath:   statusDetail.MerklePath,
		ExtraInfo:    statusDetail.ExtraInfo,
		CompetingTxs: statusDetail.CompetingTxs,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		h.logger.Error("Failed to marshal payload",
			slog.String("submission_id", sub.SubmissionID),
			slog.String("error", err.Error()))
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", sub.CallbackURL, bytes.NewReader(payloadBytes))
	if err != nil {
		h.logger.Error("Failed to create webhook request",
			slog.String("submission_id", sub.SubmissionID),
			slog.String("url", sub.CallbackURL),
			slog.String("error", err.Error()))
		return
	}

	req.Header.Set("Content-Type", "application/json")
	if sub.CallbackToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", sub.CallbackToken))
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		h.logger.Error("Failed to deliver webhook",
			slog.String("submission_id", sub.SubmissionID),
			slog.String("url", sub.CallbackURL),
			slog.String("error", err.Error()))
		h.scheduleRetry(ctx, sub)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		h.logger.Info("Webhook delivered successfully",
			slog.String("submission_id", sub.SubmissionID),
			slog.String("url", sub.CallbackURL),
			slog.String("status", string(event.Status)),
			slog.Int("http_status", resp.StatusCode))

		if err := h.submissionStore.UpdateDeliveryStatus(ctx, sub.SubmissionID, event.Status, 0, nil); err != nil {
			h.logger.Error("Failed to update submission after successful delivery",
				slog.String("submission_id", sub.SubmissionID),
				slog.String("error", err.Error()))
		}
	} else {
		h.logger.Warn("Webhook delivery failed",
			slog.String("submission_id", sub.SubmissionID),
			slog.String("url", sub.CallbackURL),
			slog.Int("http_status", resp.StatusCode))
		h.scheduleRetry(ctx, sub)
	}
}

// scheduleRetry schedules a retry for failed webhook delivery
func (h *WebhookHandler) scheduleRetry(ctx context.Context, sub models.Submission) {
	retryCount := sub.RetryCount + 1
	nextRetry := time.Now().Add(time.Duration(retryCount) * time.Minute)

	if err := h.submissionStore.UpdateDeliveryStatus(ctx, sub.SubmissionID, sub.LastDeliveredStatus, retryCount, &nextRetry); err != nil {
		h.logger.Error("Failed to schedule retry",
			slog.String("submission_id", sub.SubmissionID),
			slog.String("error", err.Error()))
	} else {
		h.logger.Info("Scheduled webhook retry",
			slog.String("submission_id", sub.SubmissionID),
			slog.Int("retry_count", retryCount),
			slog.Time("next_retry", nextRetry))
	}
}
