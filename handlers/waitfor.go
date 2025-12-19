package handlers

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
)

var ErrTimeout = errors.New("wait timeout exceeded")

// WaitForHandler manages per-transaction channels for synchronous status waiting
type WaitForHandler struct {
	publisher  events.Publisher
	txChannels map[string][]chan *models.TransactionStatus
	mu         sync.RWMutex
	logger     *slog.Logger
	stopCh     chan struct{}
}

// NewWaitForHandler creates a new wait-for handler
func NewWaitForHandler(publisher events.Publisher, logger *slog.Logger) *WaitForHandler {
	return &WaitForHandler{
		publisher:  publisher,
		txChannels: make(map[string][]chan *models.TransactionStatus),
		logger:     logger,
		stopCh:     make(chan struct{}),
	}
}

// Start begins routing status updates to per-transaction channels
func (h *WaitForHandler) Start(ctx context.Context) error {
	eventCh, err := h.publisher.Subscribe(ctx)
	if err != nil {
		return err
	}

	go h.processEvents(ctx, eventCh)
	return nil
}

func (h *WaitForHandler) processEvents(ctx context.Context, eventCh <-chan *models.TransactionStatus) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.stopCh:
			return
		case status, ok := <-eventCh:
			if !ok {
				return
			}
			h.routeStatus(status)
		}
	}
}

func (h *WaitForHandler) routeStatus(status *models.TransactionStatus) {
	h.mu.RLock()
	channels, exists := h.txChannels[status.TxID]
	h.mu.RUnlock()

	if !exists {
		return
	}

	for _, ch := range channels {
		select {
		case ch <- status:
		default:
			// Channel full or closed, skip
		}
	}
}

// Wait creates a channel for a specific transaction and waits for target status or timeout
func (h *WaitForHandler) Wait(ctx context.Context, txid string, targetStatus models.Status, timeout time.Duration) (models.Status, error) {
	ch := make(chan *models.TransactionStatus, 10)

	h.mu.Lock()
	h.txChannels[txid] = append(h.txChannels[txid], ch)
	h.mu.Unlock()

	defer h.removeChannel(txid, ch)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var lastStatus models.Status
	for {
		select {
		case <-ctx.Done():
			return lastStatus, ctx.Err()
		case <-timer.C:
			return lastStatus, ErrTimeout
		case status := <-ch:
			lastStatus = status.Status
			if statusReached(status.Status, targetStatus) {
				return status.Status, nil
			}
		}
	}
}

func (h *WaitForHandler) removeChannel(txid string, ch chan *models.TransactionStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()

	channels := h.txChannels[txid]
	for i, c := range channels {
		if c == ch {
			h.txChannels[txid] = append(channels[:i], channels[i+1:]...)
			break
		}
	}
	if len(h.txChannels[txid]) == 0 {
		delete(h.txChannels, txid)
	}
	close(ch)
}

// statusReached checks if the current status meets or exceeds the target status
func statusReached(current, target models.Status) bool {
	order := map[models.Status]int{
		models.StatusUnknown:              0,
		models.StatusReceived:             1,
		models.StatusSentToNetwork:        2,
		models.StatusAcceptedByNetwork:    3,
		models.StatusSeenOnNetwork:        4,
		models.StatusMined:                5,
		models.StatusRejected:             -1,
		models.StatusDoubleSpendAttempted: -1,
	}

	// Terminal negative states always count as "reached"
	if order[current] < 0 {
		return true
	}

	return order[current] >= order[target]
}

// Stop stops the wait-for handler
func (h *WaitForHandler) Stop() error {
	close(h.stopCh)

	h.mu.Lock()
	defer h.mu.Unlock()

	for txid, channels := range h.txChannels {
		for _, ch := range channels {
			close(ch)
		}
		delete(h.txChannels, txid)
	}

	return nil
}
