package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
)

// ConsumerGroup is the service-facing consumer wrapper. It owns the
// drain-then-flush + retry + DLQ logic so services only supply a per-message
// handler and an optional batch-flush hook. The underlying Broker supplies
// the transport (Sarama or in-memory).
//
// When ClaimHandler is set on the ConsumerConfig, the wrapper hands the
// raw Claim to that function and lets it own the per-partition loop
// end-to-end. The drain/flush/retry/DLQ logic is bypassed entirely —
// useful for stateful services that need to own the goroutine their
// state lives on (e.g. the dep-aware propagator).
type ConsumerGroup struct {
	broker        Broker
	sub           Subscription
	topics        []string
	handler       MessageHandler
	flushFunc     FlushFunc
	claimHandler  ClaimHandler
	producer      *Producer
	maxRetries    int
	flushInterval time.Duration
	logger        *zap.Logger
	ready         chan struct{}
}

// FlushFunc is called after a drain of immediately-ready messages. The
// context belongs to the current claim — when the claim ends (shutdown or
// rebalance) the context is already canceled, so downstream work (broadcasts,
// store writes) can abort cleanly instead of running on Background.
type FlushFunc func(ctx context.Context) error

// ClaimHandler is invoked once per Sarama claim when set on a
// ConsumerConfig. The handler owns the entire per-partition loop: pull
// messages from claim.Messages(), watch claim.Context() for cancellation,
// and call claim.MarkMessage when ready to commit an offset. When the
// handler returns, the claim ends. Used by services that need to keep
// dispatcher state in the same goroutine that consumes from Kafka.
type ClaimHandler func(claim Claim) error

type ConsumerConfig struct {
	Broker     Broker
	GroupID    string
	Topics     []string
	Handler    MessageHandler
	FlushFunc  FlushFunc // called when claim channel drains or ends
	Producer   *Producer // used for DLQ routing
	MaxRetries int
	// FlushInterval, when > 0, fires the flush hook periodically even if the
	// drain loop hasn't observed a channel-empty moment. Bounds end-to-end
	// latency on bursty traffic where new messages keep arriving inside the
	// drain inner loop (which would otherwise indefinitely defer the flush).
	// Default 50ms via NewConsumerGroup; zero disables the ticker.
	FlushInterval time.Duration
	Logger        *zap.Logger
	// ClaimHandler, when set, takes precedence over Handler / FlushFunc.
	// The wrapper hands every Claim directly to this function — no
	// internal drain loop, no per-message retry, no DLQ. The service
	// owning the handler owns the goroutine running it, and can keep
	// per-partition state in local variables without any cross-goroutine
	// synchronization.
	ClaimHandler ClaimHandler
}

func NewConsumerGroup(cfg ConsumerConfig) (*ConsumerGroup, error) {
	if cfg.Broker == nil {
		return nil, fmt.Errorf("ConsumerConfig.Broker is required")
	}
	// Require exactly one of (Handler, ClaimHandler). Without this guard a
	// nil Handler in legacy mode panics inside the per-message dispatch
	// loop the first time a message arrives — failing fast at construction
	// turns that latent panic into a startup error operators can act on.
	if cfg.Handler == nil && cfg.ClaimHandler == nil {
		return nil, fmt.Errorf("ConsumerConfig: either Handler or ClaimHandler must be set")
	}
	sub, err := cfg.Broker.Subscribe(cfg.GroupID, cfg.Topics)
	if err != nil {
		return nil, fmt.Errorf("subscribing: %w", err)
	}

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	flushInterval := cfg.FlushInterval
	if flushInterval < 0 {
		flushInterval = 0
	}
	if flushInterval == 0 {
		// Default 50ms: short enough to bound tail latency under sustained
		// traffic, long enough not to add measurable overhead on idle.
		flushInterval = 50 * time.Millisecond
	}

	return &ConsumerGroup{
		broker:        cfg.Broker,
		sub:           sub,
		topics:        cfg.Topics,
		handler:       cfg.Handler,
		flushFunc:     cfg.FlushFunc,
		claimHandler:  cfg.ClaimHandler,
		producer:      cfg.Producer,
		maxRetries:    maxRetries,
		flushInterval: flushInterval,
		logger:        cfg.Logger,
		ready:         make(chan struct{}),
	}, nil
}

// Run drives the subscription. Blocks until ctx is canceled or the broker
// closes. Each call to handleClaim preserves the drain-then-flush invariant:
// all immediately-available messages are processed, then flushFunc fires,
// then the next iteration waits for new arrivals.
func (c *ConsumerGroup) Run(ctx context.Context) error {
	close(c.ready)
	return c.sub.Consume(ctx, c.handleClaim)
}

// Ready returns a channel closed once the subscription is live (for tests).
func (c *ConsumerGroup) Ready() <-chan struct{} {
	return c.ready
}

func (c *ConsumerGroup) Close() error {
	return c.sub.Close()
}

// handleClaim processes messages from a single claim with the drain-then-flush
// pattern. defer flush() runs when the claim ends (shutdown or rebalance), so
// accumulated state never leaks across a rebalance boundary. The claim's
// context is passed into the flush hook so downstream work (broadcasts, store
// writes) unwinds when the claim is revoked instead of running on
// context.Background.
//
// A flushInterval ticker fires the flush hook even when the inner drain loop
// keeps observing new messages — sustained traffic would otherwise defer the
// flush indefinitely and grow the in-memory pending slice. Bounds end-to-end
// latency without sacrificing the drain-then-flush batching efficiency.
//
// When a ClaimHandler is configured the wrapper bypasses the drain/flush
// pattern entirely and hands the raw Claim to the handler — the service
// owns the goroutine, the message-pull cadence, and the MarkMessage
// timing in that mode.
func (c *ConsumerGroup) handleClaim(claim Claim) error {
	if c.claimHandler != nil {
		return c.claimHandler(claim)
	}
	ctx := claim.Context()
	defer c.flush(ctx)

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			c.processOne(claim, msg)

			// Drain all immediately-ready messages before flushing. Messages
			// that arrived as a batch publish naturally cluster here, so batch
			// size matches producer intent without requiring a timer.
			for {
				select {
				case msg, ok := <-claim.Messages():
					if !ok {
						return nil
					}
					c.processOne(claim, msg)
				default:
					goto drainDone
				}
			}
		drainDone:
			c.flush(ctx)

		case <-ticker.C:
			// Periodic flush kicks pending work loose even if the drain
			// inner loop is hot. Cheap when there's nothing to flush — the
			// service's FlushFunc no-ops on empty pending state.
			c.flush(ctx)

		case <-ctx.Done():
			return nil
		}
	}
}

// processOne runs the configured handler against a single message, then
// marks the offset. On DLQ-publish failure the offset is left unmarked so
// Kafka redelivers on the next session — preferable to silent message
// loss.
func (c *ConsumerGroup) processOne(claim Claim, msg *Message) {
	metrics.KafkaMessagesTotal.WithLabelValues(msg.Topic, "consume").Inc()
	metrics.KafkaMessageBytes.WithLabelValues(msg.Topic, "consume").Observe(float64(len(msg.Value)))
	if err := c.processWithRetry(claim.Context(), msg); err != nil {
		// If DLQ publish also fails (e.g. transient outage on the DLQ
		// topic) we deliberately do NOT mark the offset. Leaving it
		// uncommitted causes Kafka to redeliver on the next session,
		// which is preferable to silent message loss. The next
		// rebalance / pod restart will retry from the same offset.
		if dlqErr := c.sendToDLQ(msg, err); dlqErr != nil {
			metrics.KafkaDLQPublishFailures.WithLabelValues(msg.Topic).Inc()
			c.logger.Error(
				"DLQ publish failed; leaving offset uncommitted for redelivery",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(dlqErr),
			)
			return
		}
	}
	claim.MarkMessage(msg)
}

func (c *ConsumerGroup) flush(ctx context.Context) {
	if c.flushFunc == nil {
		return
	}
	if err := c.flushFunc(ctx); err != nil {
		c.logger.Error("flush failed", zap.Error(err))
	}
}

func (c *ConsumerGroup) processWithRetry(ctx context.Context, msg *Message) error {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 100 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return lastErr
			}
		}
		if err := c.handler(ctx, msg); err != nil {
			lastErr = err
			c.logger.Warn(
				"message processing failed, retrying",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Int("attempt", attempt+1),
				zap.Error(err),
			)
			continue
		}
		return nil
	}
	return lastErr
}

// sendToDLQ publishes the failed message envelope to <topic>.dlq via the
// Broker. Routing through Broker (not the sync Sarama producer) keeps DLQ
// working in standalone mode where there's no Sarama at all.
//
// Returns an error when the DLQ publish itself failed so the caller can
// decide whether the original Kafka offset is safe to commit. If no
// producer is configured (DLQ disabled by deployment choice), this
// returns nil — the caller treats the failed message as best-effort
// dropped, which preserves the historical behavior for that mode.
// Marshal failures also return nil because retrying will not help and
// dropping is the only sane outcome.
func (c *ConsumerGroup) sendToDLQ(msg *Message, processErr error) error {
	if c.producer == nil {
		c.logger.Error(
			"no producer configured for DLQ — dropping failed message",
			zap.String("topic", msg.Topic),
			zap.Int64("offset", msg.Offset),
		)
		return nil
	}
	dlqTopic := DLQTopic(msg.Topic)
	dlqMsg := map[string]any{
		"original_topic": msg.Topic,
		"original_key":   string(msg.Key),
		"original_value": string(msg.Value),
		"error":          processErr.Error(),
		"partition":      msg.Partition,
		"offset":         msg.Offset,
	}
	data, err := json.Marshal(dlqMsg)
	if err != nil {
		c.logger.Error("failed to marshal DLQ message", zap.Error(err))
		return nil
	}
	if err := c.producer.SendRaw(dlqTopic, string(msg.Key), data); err != nil {
		c.logger.Error(
			"failed to send to DLQ",
			zap.String("dlq_topic", dlqTopic),
			zap.Error(err),
		)
		return fmt.Errorf("publishing to DLQ %q: %w", dlqTopic, err)
	}
	metrics.KafkaMessagesTotal.WithLabelValues(msg.Topic, "dlq").Inc()
	c.logger.Info(
		"message sent to DLQ",
		zap.String("dlq_topic", dlqTopic),
		zap.String("original_topic", msg.Topic),
		zap.Int32("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)
	return nil
}
