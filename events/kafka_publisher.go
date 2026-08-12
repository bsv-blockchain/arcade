package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// dropLogInterval is the minimum time between "subscriber channel full"
// warn logs per (publisher, caller). The drop count is still tracked
// precisely via the Prometheus counter — this interval just throttles the
// log line so a sustained burst doesn't spam the operator with millions of
// identical warns.
const dropLogInterval = 5 * time.Second

// KafkaPublisher fans transaction status updates through a Kafka topic so
// services running in different processes share a view of every status
// change. Publish JSON-encodes the TransactionStatus and sends it under
// kafka.TopicStatusUpdate keyed by txid (so updates for the same tx land on
// the same partition in real Kafka). Subscribe spins up a dedicated
// consumer group per (process, caller) — every caller gets every message
// published after it subscribed, regardless of how many other subscribers
// are running. History is never served from Kafka: subscribers are
// live-only by contract, and missed events recover through store-backed
// paths (SSE Last-Event-ID catchup, webhook retry reaper).
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
	// groupSeq counts Subscribe calls per caller so each gets its own consumer
	// group even when one process subscribes twice under the same caller. See
	// subscriberGroupID: sharing a group would split the topic's partitions
	// between the two subscribers instead of giving each the whole stream.
	groupSeq map[string]int
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
// (the default for status mutations) or propagate. ctx is forwarded to the
// producer so an active caller span becomes the parent of the Kafka producer
// span and propagates onto the message headers.
func (p *KafkaPublisher) Publish(ctx context.Context, status *models.TransactionStatus) error {
	if status == nil {
		return fmt.Errorf("nil status")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	err := p.producer.Send(ctx, kafka.TopicStatusUpdate, status.TxID, status)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	metrics.EventsPublishDuration.WithLabelValues("single", outcome).Observe(time.Since(start).Seconds())
	return err
}

// PublishBulk sends one event carrying TxIDs[]. The kafka key is the
// BlockHash (so bulk events for the same block land on the same partition
// in real Kafka); fall back to a synthesized key if BlockHash is empty.
// See the Publisher.PublishBulk contract for rationale.
func (p *KafkaPublisher) PublishBulk(ctx context.Context, template *models.TransactionStatus) error {
	if template == nil {
		return fmt.Errorf("nil template")
	}
	if len(template.TxIDs) == 0 {
		return fmt.Errorf("PublishBulk requires non-empty TxIDs")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	key := template.BlockHash
	if key == "" {
		// Synthesize a stable key from the first txid so partitioning is
		// deterministic even before block context is known.
		key = "bulk-" + template.TxIDs[0]
	}
	start := time.Now()
	err := p.producer.Send(ctx, kafka.TopicStatusUpdate, key, template)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	metrics.EventsPublishDuration.WithLabelValues("bulk", outcome).Observe(time.Since(start).Seconds())
	return err
}

// Subscribe joins a consumer group on TopicStatusUpdate and returns a channel
// that yields decoded TransactionStatus values until ctx is canceled.
//
// The group id is unique per (process, caller) — see subscriberGroupID — which
// is what guarantees this subscriber sees EVERY message published from
// subscription time onward rather than a partition subset. That property is
// load-bearing: it is why any SSE replica can serve any client and why no
// sticky routing is needed.
//
// The group starts at the LATEST offset. A fresh random group with
// StartOldest silently replays the topic's entire retained backlog on every
// pod start (~145k messages / >1GB of JSON in a busy deployment — this
// OOM-crashlooped the 512Mi sse pods) and delivers stale duplicates to any
// client connected mid-replay. Subscribers here are live-only by contract:
// history belongs to the store (SSE Last-Event-ID catchup, webhook reaper),
// never to Kafka replay.
//
// caller is a low-cardinality identifier (e.g. "sse", "webhook") used as a
// Prometheus label on EventsSubscriberDroppedTotal so an operator can tell
// which subscriber is dropping when the channel fills.
func (p *KafkaPublisher) Subscribe(ctx context.Context, caller string) (<-chan *models.TransactionStatus, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("publisher closed")
	}
	p.mu.Unlock()

	if caller == "" {
		caller = "unknown"
	}

	groupID, err := p.subscriberGroupID(caller)
	if err != nil {
		return nil, fmt.Errorf("generating group id: %w", err)
	}
	// Logged because a duplicate group id across pods is otherwise invisible:
	// the partitions would be split between them and each pod's clients would
	// quietly receive a fraction of the stream.
	p.logger.Info("events subscriber joining consumer group",
		zap.String("caller", caller),
		zap.String("group_id", groupID))

	out := make(chan *models.TransactionStatus, p.subscriberBuffer)

	// Pre-resolve the counter so the hot path is a single atomic increment.
	dropCounter := metrics.EventsSubscriberDroppedTotal.WithLabelValues(caller)
	// Last-warn timestamp (unix nanos) for log throttling. Atomic so the
	// kafka handler goroutine can update it without a mutex.
	var lastWarnUnixNano atomic.Int64

	cg, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:  p.producer.Broker(),
		GroupID: groupID,
		Topics:  []string{kafka.TopicStatusUpdate},
		// Live-only: see the Subscribe doc comment. Never StartOldest here —
		// a random per-process group has no committed offsets, so oldest
		// means a full backlog replay on every single pod start.
		StartOffset: kafka.StartLatest,
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
				dropCounter.Inc()
				// Throttle the warn log: every drop bumps the counter (so
				// Prometheus has the precise rate), but we only emit a log
				// line once per dropLogInterval to avoid drowning the
				// operator under sustained pressure.
				now := time.Now().UnixNano()
				prev := lastWarnUnixNano.Load()
				if now-prev >= int64(dropLogInterval) && lastWarnUnixNano.CompareAndSwap(prev, now) {
					p.logger.Warn(
						"subscriber channel full, dropping update",
						zap.String("caller", caller),
						logfields.TxID(status.TxID),
						logfields.Status(string(status.Status)),
					)
				}
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

// subscriberGroupID returns this subscriber's consumer-group identifier.
//
// EVERY Subscribe call must get its own group. That is the Publisher's core
// fan-out contract: distinct groups each receive the whole topic, whereas two
// subscribers sharing a group have the partitions split between them and each
// sees only a fraction of the stream. For SSE that fraction would be silently
// missing events, and the "every replica sees everything" property — the
// reason any replica can serve any client without sticky routing — would be
// gone. Hence the per-caller sequence number: it is what keeps the contract
// intact for two same-caller subscribers in one process.
//
// Within that constraint the id is built from caller + hostname (the pod name
// under Kubernetes) rather than random bytes. A purely random id made
// redpanda_kafka_consumer_group_lag effectively unusable: the group label was
// unrecognizable and changed on every restart, so the most useful broker-side
// signal could not be matched, aggregated, or alerted on. The id now sorts and
// regex-matches by caller (`arcade-events-sse-.*`) and names the pod it
// belongs to, so lag joins back to something an operator can go and look at.
//
// This is safe with respect to #239 (the StartOldest backlog-replay OOM):
// this consumer never marks messages, so the group never commits an offset and
// Offsets.Initial (StartLatest) applies on every join. A rejoining pod still
// starts at the head, even if it reuses a name.
//
// Falls back to random bytes when the hostname is missing or non-identifying,
// where uniqueness matters more than legibility.
func (p *KafkaPublisher) subscriberGroupID(caller string) (string, error) {
	p.mu.Lock()
	if p.groupSeq == nil {
		p.groupSeq = make(map[string]int)
	}
	seq := p.groupSeq[caller]
	p.groupSeq[caller]++
	p.mu.Unlock()

	host, err := os.Hostname()
	if err != nil {
		return uniqueGroupID(caller)
	}
	sanitized := sanitizeGroupComponent(host)
	if sanitized == "" || sanitized == "localhost" {
		return uniqueGroupID(caller)
	}
	return fmt.Sprintf("arcade-events-%s-%s-%d", caller, sanitized, seq), nil
}

// uniqueGroupID returns a random per-call group identifier, used when no
// stable identity is available.
func uniqueGroupID(caller string) (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "arcade-events-" + caller + "-" + hex.EncodeToString(b[:]), nil
}

// sanitizeGroupComponent reduces a string to characters that are safe in a
// Kafka group id, dropping anything else.
func sanitizeGroupComponent(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		}
	}
	return b.String()
}
