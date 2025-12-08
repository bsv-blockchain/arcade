package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/labstack/echo/v4"
)

type SSEHandler struct {
	eventPublisher  events.Publisher
	submissionStore store.SubmissionStore
	statusStore     store.StatusStore
	logger          *slog.Logger
}

func NewSSEHandler(
	eventPublisher events.Publisher,
	submissionStore store.SubmissionStore,
	statusStore store.StatusStore,
	logger *slog.Logger,
) *SSEHandler {
	return &SSEHandler{
		eventPublisher:  eventPublisher,
		submissionStore: submissionStore,
		statusStore:     statusStore,
		logger:          logger,
	}
}

func (h *SSEHandler) HandleSSE(c echo.Context) error {
	callbackToken := c.Param("callbackToken")

	c.Response().Header().Set("Content-Type", "text/event-stream")
	c.Response().Header().Set("Cache-Control", "no-cache")
	c.Response().Header().Set("Connection", "keep-alive")
	c.Response().WriteHeader(200)

	lastEventID := c.Request().Header.Get("Last-Event-ID")

	if lastEventID != "" {
		if err := h.sendCatchup(c, callbackToken, lastEventID); err != nil {
			h.logger.Error("catchup failed", slog.String("error", err.Error()))
			return err
		}
	}

	eventChan, err := h.eventPublisher.Subscribe(c.Request().Context())
	if err != nil {
		return err
	}

	txidSet := h.getTxidsForToken(c.Request().Context(), callbackToken)

	for {
		select {
		case <-c.Request().Context().Done():
			return nil
		case event := <-eventChan:
			if _, ok := txidSet[event.TxID]; ok {
				h.sendSSEEvent(c, event)
				c.Response().Flush()
			}
		}
	}
}

func (h *SSEHandler) sendCatchup(c echo.Context, callbackToken, lastEventID string) error {
	sinceNS, err := strconv.ParseInt(lastEventID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid Last-Event-ID: %w", err)
	}
	since := time.Unix(0, sinceNS)

	submissions, err := h.submissionStore.GetSubmissionsByToken(c.Request().Context(), callbackToken)
	if err != nil {
		return err
	}

	for _, sub := range submissions {
		status, err := h.statusStore.GetStatus(c.Request().Context(), sub.TxID)
		if err != nil || status == nil {
			continue
		}

		if status.Timestamp.After(since) {
			event := models.StatusUpdate{
				TxID:      status.TxID,
				Status:    status.Status,
				Timestamp: status.Timestamp,
			}
			h.sendSSEEvent(c, event)
		}
	}

	c.Response().Flush()
	return nil
}

func (h *SSEHandler) getTxidsForToken(ctx context.Context, callbackToken string) map[string]bool {
	submissions, err := h.submissionStore.GetSubmissionsByToken(ctx, callbackToken)
	if err != nil {
		h.logger.Error("failed to get submissions", slog.String("error", err.Error()))
		return make(map[string]bool)
	}

	txidSet := make(map[string]bool, len(submissions))
	for _, sub := range submissions {
		txidSet[sub.TxID] = true
	}
	return txidSet
}

func (h *SSEHandler) sendSSEEvent(c echo.Context, event models.StatusUpdate) {
	eventID := event.Timestamp.UnixNano()

	fmt.Fprintf(c.Response(), "id: %d\n", eventID)
	fmt.Fprintf(c.Response(), "event: status\n")
	fmt.Fprintf(c.Response(), "data: {\"txid\":\"%s\",\"status\":\"%s\",\"timestamp\":\"%s\"}\n\n",
		event.TxID,
		event.Status,
		event.Timestamp.Format(time.RFC3339))
}
