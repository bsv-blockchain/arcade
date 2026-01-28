// Package events provides interfaces for publishing and subscribing to transaction status updates.
package events

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// Submission represents a transaction submission event with raw transaction data.
// This is published when a transaction is submitted for broadcast, allowing
// subscribers to capture the transaction data before status events arrive.
type Submission struct {
	TxID  string // Transaction ID
	RawTx []byte // Raw transaction bytes (may be BEEF format)
}

// Publisher broadcasts status updates to subscribers
type Publisher interface {
	// Publish sends a status update to all subscribers
	Publish(ctx context.Context, status *models.TransactionStatus) error

	// Subscribe returns a channel that receives all status updates
	Subscribe(ctx context.Context) (<-chan *models.TransactionStatus, error)

	// PublishSubmission sends a submission event to all submission subscribers
	PublishSubmission(ctx context.Context, submission *Submission) error

	// SubscribeSubmissions returns a channel that receives all submission events
	SubscribeSubmissions(ctx context.Context) (<-chan *Submission, error)

	// Close closes the publisher and all subscriptions
	Close() error
}
