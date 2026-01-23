// Package events provides interfaces for publishing and subscribing to transaction status updates.
package events

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// Publisher broadcasts status updates to subscribers
type Publisher interface {
	// Publish sends a status update to all subscribers
	Publish(ctx context.Context, status *models.TransactionStatus) error

	// Subscribe returns a channel that receives all status updates
	Subscribe(ctx context.Context) (<-chan *models.TransactionStatus, error)

	// Close closes the publisher and all subscriptions
	Close() error
}
