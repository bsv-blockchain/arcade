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
//
// The caller string identifies the subscriber for observability — drop and
// pressure metrics are labeled with it so an operator can tell which
// subscriber (e.g. "sse", "webhook") is failing to keep up. Use a
// low-cardinality, stable identifier; treat it as a Prometheus label.
type Publisher interface {
	Publish(ctx context.Context, status *models.TransactionStatus) error
	Subscribe(ctx context.Context, caller string) (<-chan *models.TransactionStatus, error)
	Close() error
}
