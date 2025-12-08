package events

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// Publisher broadcasts status updates to subscribers
type Publisher interface {
	// Publish sends a status update to all subscribers
	Publish(ctx context.Context, event models.StatusUpdate) error

	// Subscribe returns a channel that receives all status updates
	Subscribe(ctx context.Context) (<-chan models.StatusUpdate, error)

	// Close closes the publisher and all subscriptions
	Close() error
}
