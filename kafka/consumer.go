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
type ConsumerGroup struct {
	broker     Broker
	sub        Subscription
	topics     []string
	handler    MessageHandler
	flushFunc  FlushFunc
	producer   *Producer
	maxRetries int
	logger     *zap.Logger
	ready      chan struct{}
}

// FlushFunc is called after a drain of immediately-ready messages. The
// context belongs to the current claim — when the claim ends (shutdown or
// rebalance) the context is already cancelled, so downstream work (broadcasts,
// store writes) can abort cleanly instead of running on Background.
type FlushFunc func(ctx context.Context) error

type ConsumerConfig struct {
	Broker     Broker
	GroupID    string
	Topics     []string
	Handler    MessageHandler
	FlushFunc  FlushFunc // called when claim channel drains or ends
	Producer   *Producer // used for DLQ routing
	MaxRetries int
	Logger     *zap.Logger
}

func NewConsumerGroup(cfg ConsumerConfig) (*ConsumerGroup, error) {
	if cfg.Broker == nil {
		return nil, fmt.Errorf("ConsumerConfig.Broker is required")
	}
	sub, err := cfg.Broker.Subscribe(cfg.GroupID, cfg.Topics)
	if err != nil {
		return nil, fmt.Errorf("subscribing: %w", err)
	}

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	return &ConsumerGroup{
		broker:     cfg.Broker,
		sub:        sub,
		topics:     cfg.Topics,
		handler:    cfg.Handler,
		flushFunc:  cfg.FlushFunc,
		producer:   cfg.Producer,
		maxRetries: maxRetries,
		logger:     cfg.Logger,
		ready:      make(chan struct{}),
	}, nil
}

// Run drives the subscription. Blocks until ctx is cancelled or the broker
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
func (c *ConsumerGroup) handleClaim(claim Claim) error {
	ctx := claim.Context()
	defer c.flush(ctx)
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

		case <-ctx.Done():
			return nil
		}
	}
}

func (c *ConsumerGroup) processOne(claim Claim, msg *Message) {
	metrics.KafkaMessagesTotal.WithLabelValues(msg.Topic, "consume").Inc()
	metrics.KafkaMessageBytes.WithLabelValues(msg.Topic, "consume").Observe(float64(len(msg.Value)))
	if err := c.processWithRetry(claim.Context(), msg); err != nil {
		c.sendToDLQ(msg, err)
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
			c.logger.Warn("message processing failed, retrying",
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
func (c *ConsumerGroup) sendToDLQ(msg *Message, processErr error) {
	if c.producer == nil {
		c.logger.Error("no producer configured for DLQ — dropping failed message",
			zap.String("topic", msg.Topic),
			zap.Int64("offset", msg.Offset),
		)
		return
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
		return
	}
	if err := c.producer.SendRaw(dlqTopic, string(msg.Key), data); err != nil {
		c.logger.Error("failed to send to DLQ",
			zap.String("dlq_topic", dlqTopic),
			zap.Error(err),
		)
		return
	}
	metrics.KafkaMessagesTotal.WithLabelValues(msg.Topic, "dlq").Inc()
	c.logger.Info("message sent to DLQ",
		zap.String("dlq_topic", dlqTopic),
		zap.String("original_topic", msg.Topic),
		zap.Int32("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
	)
}
