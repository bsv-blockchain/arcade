package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/arcade/events"
	"github.com/bitcoin-sv/arcade/models"
)

// WaitForHandler manages per-transaction channels for synchronous status waiting
type WaitForHandler struct {
	publisher  events.Publisher
	txChannels map[string][]chan models.StatusUpdate
	mu         sync.RWMutex
}

// NewWaitForHandler creates a new wait-for handler
func NewWaitForHandler(publisher events.Publisher) *WaitForHandler {
	return &WaitForHandler{
		publisher:  publisher,
		txChannels: make(map[string][]chan models.StatusUpdate),
	}
}

// Start begins routing status updates to per-transaction channels
func (h *WaitForHandler) Start(ctx context.Context) error {
	// TODO: Subscribe to global event channel
	// TODO: Route updates to per-tx channels if they exist
	return nil
}

// Wait creates a channel for a specific transaction and waits for target status or timeout
func (h *WaitForHandler) Wait(ctx context.Context, txid string, targetStatus models.Status, timeout time.Duration) (models.Status, error) {
	// TODO: Create per-tx channel
	// TODO: Register in map
	// TODO: Wait for status or timeout
	// TODO: Clean up channel when done
	return models.StatusUnknown, nil
}

// Stop stops the wait-for handler
func (h *WaitForHandler) Stop() error {
	// TODO: Close all per-tx channels
	return nil
}
