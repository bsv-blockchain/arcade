package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

// KafkaPublisher fans transaction status updates through a Kafka topic so
// services running in different processes share a view of every status
// change. Publish JSON-encodes the TransactionStatus and sends it under
// kafka.TopicStatusUpdate keyed by txid (so updates for the same tx land on
// the same partition in real Kafka). Subscribe spins up a dedicated
// consumer group with a random ID — every caller gets every message,
// regardless of how many other subscribers are running.
// DefaultSubscriberBuffer is the fallback channel capacity used when
// NewKafkaPublisher is called with a non-positive subscriberBuffer.
const DefaultSubscriberBuffer = 4096

type KafkaPublisher struct {
	producer         *kafka.Producer
	logger           *zap.Logger
	subscriberBuffer int

	mu     sync.Mutex
	closed bool
	subs   []*kafkaSubscription
}

// NewKafkaPublisher wraps a kafka.Producer. The producer's underlying broker
// is also used for Subscribe, so a single Publisher serves both publishing
// and subscribing. subscriberBuffer is the channel capacity minted for each
// Subscribe call — values <= 0 fall back to DefaultSubscriberBuffer.
func NewKafkaPublisher(producer *kafka.Producer, logger *zap.Logger, subscriberBuffer int) *KafkaPublisher {
	if subscriberBuffer <= 0 {
		subscriberBuffer = DefaultSubscriberBuffer
	}
	return &KafkaPublisher{
		producer:         producer,
		logger:           logger.Named("events.kafka"),
		subscriberBuffer: subscriberBuffer,
	}
}

// Publish serializes status to JSON and sends it on TopicStatusUpdate. Errors
// are returned to the caller; the call site decides whether to log-and-continue
// (the default for status mutations) or propagate.
//
// The kafka.Producer.Send signature does not take a context — the underlying
// broker uses an internal background context for at-most-once produce; we
// honor cancellation by short-circuiting before the call.
func (p *KafkaPublisher) Publish(ctx context.Context, status *models.TransactionStatus) error {
	if status == nil {
		return fmt.Errorf("nil status")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.producer.Send(kafka.TopicStatusUpdate, status.TxID, status) //nolint:contextcheck // kafka.Producer.Send doesn't take a context; ctx already checked above
}

// Subscribe joins a fresh consumer group on TopicStatusUpdate and returns a
// channel that yields decoded TransactionStatus values until ctx is canceled.
// The unique groupID guarantees this subscriber sees every message — useful
// when multiple subscribers (SSE manager + webhook service) coexist in the
// same process or across pods.
func (p *KafkaPublisher) Subscribe(ctx context.Context) (<-chan *models.TransactionStatus, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("publisher closed")
	}
	p.mu.Unlock()

	groupID, err := uniqueGroupID()
	if err != nil {
		return nil, fmt.Errorf("generating group id: %w", err)
	}

	out := make(chan *models.TransactionStatus, p.subscriberBuffer)

	cg, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:  p.producer.Broker(),
		GroupID: groupID,
		Topics:  []string{kafka.TopicStatusUpdate},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			var status models.TransactionStatus
			if jsonErr := json.Unmarshal(msg.Value, &status); jsonErr != nil {
				p.logger.Warn("dropping malformed status update", zap.Error(jsonErr))
				return nil
			}
			select {
			case out <- &status:
			case <-ctx.Done():
				return nil
			default:
				// Slow consumer — drop rather than block the broker. Matches the
				// old arcade's non-blocking fan-out semantics; SSE clients
				// recover via Last-Event-ID catchup on reconnect.
				p.logger.Warn("subscriber channel full, dropping update",
					zap.String("txid", status.TxID),
					zap.String("status", string(status.Status)),
				)
			}
			return nil
		},
		Producer: p.producer,
		Logger:   p.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating consumer group: %w", err)
	}

	sub := &kafkaSubscription{cg: cg, out: out}

	p.mu.Lock()
	p.subs = append(p.subs, sub)
	p.mu.Unlock()

	go func() {
		// Run blocks until ctx is canceled or the broker closes.
		if err := cg.Run(ctx); err != nil {
			p.logger.Warn("consumer group exited with error", zap.Error(err))
		}
		_ = cg.Close()
		close(out)
	}()

	return out, nil
}

// Close stops the publisher. Existing subscriptions are released through
// their context cancellation; Close does not close subscriber channels
// directly because the consumer goroutine owns the close.
func (p *KafkaPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for _, s := range p.subs {
		_ = s.cg.Close()
	}
	p.subs = nil
	return nil
}

type kafkaSubscription struct {
	cg  *kafka.ConsumerGroup
	out chan *models.TransactionStatus
}

// uniqueGroupID returns a per-call group identifier. Used so each Subscribe
// gets its own consumer group, which in turn guarantees every subscriber
// sees every message (the broker fans out across distinct groups).
func uniqueGroupID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "arcade-events-" + hex.EncodeToString(b[:]), nil
}
