package kafka

import (
	"context"
	"time"
)

// Broker is the pluggable messaging abstraction arcade services use to publish
// and consume. Two implementations ship in this package:
//   - saramaBroker: real Kafka via IBM Sarama, for horizontally scaled deployments.
//   - memoryBroker: in-process channels, for single-binary standalone mode.
//
// Services only depend on this interface (and Producer/ConsumerGroup on top of
// it), so swapping the broker at startup is purely a config decision.
type Broker interface {
	// Send synchronously publishes a single message and waits for acknowledgement.
	Send(ctx context.Context, topic, key string, value []byte) error

	// SendAsync fires a message without waiting for acknowledgement. Drops on close.
	SendAsync(ctx context.Context, topic, key string, value []byte) error

	// SendBatch publishes multiple messages to the same topic atomically where
	// the backend supports it; otherwise sequentially with best-effort batching.
	SendBatch(ctx context.Context, topic string, msgs []KeyValue) error

	// Subscribe joins a consumer group for the given topics. Each groupID
	// receives each message once across all its subscriptions (standard Kafka
	// consumer-group semantics). The returned Subscription is driven by calling
	// its Consume method.
	Subscribe(groupID string, topics []string) (Subscription, error)

	// PartitionCount reports how many partitions the given topic has. Returns
	// ErrTopicNotFound if the topic does not exist on the broker. Memory-backed
	// implementations always return 1 since standalone mode has no concept of
	// partitions. Used at startup to validate that a horizontally scaled
	// deployment actually has enough partitions for the pod count.
	PartitionCount(topic string) (int, error)

	// Close tears down broker-level resources. After Close, further Send/Subscribe
	// calls return an error.
	Close() error
}

// ErrTopicNotFound is returned by PartitionCount when the topic does not exist.
var ErrTopicNotFound = errBrokerSentinel("topic not found")

type errBrokerSentinel string

func (e errBrokerSentinel) Error() string { return string(e) }

// Subscription is a handle on an active consumer-group membership. The Consume
// method is claim-oriented to preserve the drain-then-flush correctness
// property the consumer group relies on: each Kafka rebalance ends one claim
// and starts another, and the flush hook must fire at the claim boundary so
// accumulated state doesn't leak across rebalances. The memory broker emits a
// single synthetic claim that stays open for the subscription's lifetime.
type Subscription interface {
	// Consume blocks, invoking handler once per claim. The handler drives
	// reading via Claim.Messages() and is expected to return when the claim
	// ends (its channel closes or its context is done). Consume returns when
	// ctx is cancelled or an unrecoverable error occurs.
	Consume(ctx context.Context, handler func(Claim) error) error

	// Close releases broker-side resources for this subscription.
	Close() error
}

// Claim represents a single assignment of one or more partitions to this
// subscriber. Lifetime is bounded by rebalances (Sarama) or shutdown (memory).
type Claim interface {
	// Messages yields messages from the claim. Closed when the claim ends.
	Messages() <-chan *Message

	// Context is cancelled when the claim ends (rebalance or shutdown).
	// Handlers should watch it to terminate their inner drain loop promptly.
	Context() context.Context

	// MarkMessage records the message as processed at-least-once. In Sarama
	// this enqueues an offset commit (the actual commit is batched by the
	// library). In the memory broker this is a no-op — standalone mode is
	// at-most-once on crash, which is acceptable given it's a single binary.
	MarkMessage(*Message)
}

// Message is the broker-neutral shape services consume. Mirrors the fields of
// sarama.ConsumerMessage that arcade actually reads, without leaking the
// Sarama type into service code.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
	Offset    int64
	Timestamp time.Time
	Headers   map[string][]byte
}

// MessageHandler is the service-level callback signature. It takes a neutral
// Message so services no longer depend on Sarama types.
type MessageHandler func(ctx context.Context, msg *Message) error

// KeyValue is a single entry in a batch publish.
type KeyValue struct {
	Key   string
	Value any
}
