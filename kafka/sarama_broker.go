package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// saramaBroker is the production Broker backed by IBM Sarama. It owns both a
// sync and async producer so Send/SendAsync/SendBatch can pick the appropriate
// one without the caller caring.
type saramaBroker struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	brokers       []string
	consumerGroup string
}

// NewSaramaBroker constructs a Sarama-backed Broker with sensible defaults
// (WaitForAll on sync, WaitForLocal on async, 5 retries).
func NewSaramaBroker(brokers []string, consumerGroup string) (Broker, error) {
	syncCfg := sarama.NewConfig()
	syncCfg.Producer.RequiredAcks = sarama.WaitForAll
	syncCfg.Producer.Retry.Max = 5
	syncCfg.Producer.Return.Successes = true
	syncCfg.Producer.Return.Errors = true

	syncProducer, err := sarama.NewSyncProducer(brokers, syncCfg)
	if err != nil {
		return nil, fmt.Errorf("creating sync producer: %w", err)
	}

	asyncCfg := sarama.NewConfig()
	asyncCfg.Producer.RequiredAcks = sarama.WaitForLocal
	asyncCfg.Producer.Retry.Max = 5
	asyncCfg.Producer.Return.Successes = true
	asyncCfg.Producer.Return.Errors = true

	asyncProducer, err := sarama.NewAsyncProducer(brokers, asyncCfg)
	if err != nil {
		_ = syncProducer.Close()
		return nil, fmt.Errorf("creating async producer: %w", err)
	}

	return &saramaBroker{
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
		brokers:       brokers,
		consumerGroup: consumerGroup,
	}, nil
}

func (b *saramaBroker) Send(_ context.Context, topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	if _, _, err := b.syncProducer.SendMessage(msg); err != nil {
		return fmt.Errorf("sending message to %s: %w", topic, err)
	}
	return nil
}

func (b *saramaBroker) SendAsync(_ context.Context, topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	b.asyncProducer.Input() <- msg
	return nil
}

func (b *saramaBroker) SendBatch(_ context.Context, topic string, msgs []KeyValue) error {
	if len(msgs) == 0 {
		return nil
	}
	saramaMsgs := make([]*sarama.ProducerMessage, 0, len(msgs))
	for _, m := range msgs {
		data, err := marshalValue(m.Value)
		if err != nil {
			return fmt.Errorf("marshaling batch message: %w", err)
		}
		pm := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(data),
		}
		if m.Key != "" {
			pm.Key = sarama.StringEncoder(m.Key)
		}
		saramaMsgs = append(saramaMsgs, pm)
	}
	if err := b.syncProducer.SendMessages(saramaMsgs); err != nil {
		return fmt.Errorf("sending batch to %s: %w", topic, err)
	}
	return nil
}

func (b *saramaBroker) Subscribe(groupID string, topics []string) (Subscription, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(b.brokers, groupID, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group %s: %w", groupID, err)
	}
	return &saramaSubscription{group: group, topics: topics}, nil
}

func (b *saramaBroker) PartitionCount(topic string) (int, error) {
	client, err := sarama.NewClient(b.brokers, sarama.NewConfig())
	if err != nil {
		return 0, fmt.Errorf("creating metadata client: %w", err)
	}
	defer func() { _ = client.Close() }()

	partitions, err := client.Partitions(topic)
	if err != nil {
		if errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			return 0, ErrTopicNotFound
		}
		return 0, fmt.Errorf("listing partitions for %s: %w", topic, err)
	}
	return len(partitions), nil
}

func (b *saramaBroker) Close() error {
	var errs []error
	if err := b.syncProducer.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := b.asyncProducer.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing producers: %v", errs)
	}
	return nil
}

// saramaSubscription adapts a sarama.ConsumerGroup to the neutral Subscription
// interface. Each call to Consume runs a fresh group.Consume loop that
// re-engages on rebalance, invoking handler once per claim via the adapter.
type saramaSubscription struct {
	group   sarama.ConsumerGroup
	topics  []string
	handler func(Claim) error
}

func (s *saramaSubscription) Consume(ctx context.Context, handler func(Claim) error) error {
	s.handler = handler
	adapter := &saramaGroupHandler{sub: s}
	for {
		if err := s.group.Consume(ctx, s.topics, adapter); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// transient rebalance errors surface here; the library reconnects
			// automatically on the next Consume call.
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (s *saramaSubscription) Close() error {
	return s.group.Close()
}

// saramaGroupHandler bridges sarama.ConsumerGroupHandler to the neutral Claim
// abstraction. Setup/Cleanup are no-ops; ConsumeClaim wraps the Sarama claim
// and invokes the user's handler.
type saramaGroupHandler struct {
	sub *saramaSubscription
}

func (h *saramaGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *saramaGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *saramaGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	neutral := &saramaClaim{session: session, claim: claim}
	go neutral.pump()
	return h.sub.handler(neutral)
}

// saramaClaim wraps a sarama.ConsumerGroupClaim, translating each
// sarama.ConsumerMessage into a neutral *Message on its own channel. A small
// goroutine does the translation so the neutral channel closes exactly when
// the underlying claim ends, giving handlers a clean loop-exit signal.
type saramaClaim struct {
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim
	out     chan *Message
	started bool
}

func (c *saramaClaim) pump() {
	c.out = make(chan *Message, 256)
	c.started = true
	defer close(c.out)
	for msg := range c.claim.Messages() {
		headers := make(map[string][]byte, len(msg.Headers))
		for _, h := range msg.Headers {
			headers[string(h.Key)] = h.Value
		}
		c.out <- &Message{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     msg.Value,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
			Headers:   headers,
		}
	}
}

func (c *saramaClaim) Messages() <-chan *Message {
	// pump() is started before ConsumeClaim hands us to the handler, so out is
	// non-nil by the time this is called.
	return c.out
}

func (c *saramaClaim) Context() context.Context {
	return c.session.Context()
}

func (c *saramaClaim) MarkMessage(msg *Message) {
	if msg == nil {
		return
	}
	// Rebuild a minimal sarama.ConsumerMessage — MarkMessage only uses topic,
	// partition, and offset on the Sarama side.
	c.session.MarkMessage(&sarama.ConsumerMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}, "")
}
