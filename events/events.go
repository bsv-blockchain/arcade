// Package events provides a publish/subscribe abstraction for transaction
// status updates. Mutations to TransactionStatus that originate in the
// validator, propagation, bump-builder, and api-server services flow through
// a Publisher so the SSE handler and webhook delivery service can fan them
// out to clients without depending on which pod the mutation came from.
//
// The default backend is Kafka — every Subscribe call mints a unique consumer
// group so each subscriber receives every event (mirroring the in-memory
// broadcast semantics of the old arcade's monolithic Publisher).
package events

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// Publisher is the contract every status-update fan-out backend implements.
// Publish must be safe for concurrent use across goroutines. Subscribe
// returns a channel the caller drains until ctx is canceled; on cancellation,
// the channel is closed and any backend resources released.
type Publisher interface {
	Publish(ctx context.Context, status *models.TransactionStatus) error
	Subscribe(ctx context.Context) (<-chan *models.TransactionStatus, error)
	Close() error
}
