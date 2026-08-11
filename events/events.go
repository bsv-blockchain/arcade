// Package events provides a publish/subscribe abstraction for transaction
// status updates. Mutations to TransactionStatus that originate in the
// validator, propagation, bump-builder, and api-server services flow through
// a Publisher so the SSE handler and webhook delivery service can fan them
// out to clients without depending on which pod the mutation came from.
//
// The default backend is Kafka — every Subscribe call mints a unique consumer
// group so each subscriber receives every event published after subscription
// (mirroring the in-memory broadcast semantics of the old arcade's monolithic
// Publisher). Groups start at the topic head: subscribers are live-only, and
// missed history recovers through store-backed paths, never Kafka replay.
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
	// PublishBulk fans a single template status across many txids. The
	// publisher sends ONE event over the wire (TxIDs populated, TxID
	// empty); subscribers unfan in their own handler. Used by bump-builder
	// to coalesce per-block MINED bursts (N=14k+) into one event so the
	// webhook service's bounded work queue can't saturate on BUMP fan-out.
	// template.TxID is ignored; template.TxIDs must be non-empty.
	PublishBulk(ctx context.Context, template *models.TransactionStatus) error
	Subscribe(ctx context.Context, caller string) (<-chan *models.TransactionStatus, error)
	Close() error
}
