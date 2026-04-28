package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// memoryBroker is an in-process Broker. It's the standalone-mode counterpart
// to saramaBroker — zero external dependencies, same interface.
//
// Semantics:
//   - Each (groupID, topic) has a buffered channel. Publishing to a topic fans
//     the message out to every groupID currently subscribed to it. Within a
//     group, a single consumer drains the channel (mirrors Kafka consumer-group
//     semantics for the common case where standalone mode runs one consumer
//     per group).
//   - Offsets are monotonically assigned per topic so DLQ envelopes and logs
//     carry sensible values. Partition is always 0.
//   - MarkMessage is a no-op; at-most-once on crash is acceptable for a
//     single-binary deployment.
//   - Close closes all subscriber channels. Further Send calls return an error.
type memoryBroker struct {
	mu      sync.Mutex
	closed  bool
	groups  map[string]map[string]*memoryMailbox // [groupID][topic] = mailbox
	offsets map[string]*int64                    // per-topic monotonic offsets
	buffer  int
}

// memoryMailbox is a per-(group, topic) delivery queue. Subscriptions joined
// to the same group share it so either concurrent consumer wins the next
// message (standard consumer-group semantics).
type memoryMailbox struct {
	ch       chan *Message
	refCount int
}

// NewMemoryBroker constructs an in-process broker. buffer is the per-mailbox
// channel capacity — larger values smooth out bursts at the cost of memory.
func NewMemoryBroker(buffer int) Broker {
	if buffer <= 0 {
		buffer = 10000
	}
	return &memoryBroker{
		groups:  make(map[string]map[string]*memoryMailbox),
		offsets: make(map[string]*int64),
		buffer:  buffer,
	}
}

func (b *memoryBroker) Send(ctx context.Context, topic, key string, value []byte) error {
	return b.publish(ctx, topic, key, value, false)
}

func (b *memoryBroker) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	return b.publish(ctx, topic, key, value, true)
}

func (b *memoryBroker) SendBatch(ctx context.Context, topic string, msgs []KeyValue) error {
	for _, m := range msgs {
		data, err := marshalValue(m.Value)
		if err != nil {
			return err
		}
		if err := b.publish(ctx, topic, m.Key, data, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *memoryBroker) publish(ctx context.Context, topic, key string, value []byte, async bool) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errors.New("memory broker closed")
	}

	offsetPtr, ok := b.offsets[topic]
	if !ok {
		var o int64
		offsetPtr = &o
		b.offsets[topic] = offsetPtr
	}
	offset := atomic.AddInt64(offsetPtr, 1) - 1

	// Collect the mailboxes that currently care about this topic. Snapshot
	// under the lock, then release before sending so a blocked receiver
	// doesn't stall other publishers.
	var targets []chan *Message
	for _, topics := range b.groups {
		if mb, ok := topics[topic]; ok {
			targets = append(targets, mb.ch)
		}
	}
	b.mu.Unlock()

	msg := &Message{
		Topic:     topic,
		Key:       []byte(key),
		Value:     value,
		Partition: 0,
		Offset:    offset,
		Timestamp: time.Now(),
	}

	for _, ch := range targets {
		if async {
			select {
			case ch <- msg:
			default:
				// drop silently — async semantics
			}
			continue
		}
		select {
		case ch <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *memoryBroker) Subscribe(groupID string, topics []string) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, errors.New("memory broker closed")
	}

	topicMailboxes, ok := b.groups[groupID]
	if !ok {
		topicMailboxes = make(map[string]*memoryMailbox)
		b.groups[groupID] = topicMailboxes
	}

	// Merge all subscribed topics into a single delivery channel. Because all
	// shared a groupID, round-robin across concurrent consumers in the same
	// group happens naturally via Go channel receive semantics.
	merged := make(chan *Message, b.buffer)
	for _, t := range topics {
		mb, ok := topicMailboxes[t]
		if !ok {
			mb = &memoryMailbox{ch: make(chan *Message, b.buffer)}
			topicMailboxes[t] = mb
		}
		mb.refCount++
		go forward(mb.ch, merged)
	}

	return &memorySubscription{
		broker:  b,
		groupID: groupID,
		topics:  topics,
		out:     merged,
	}, nil
}

// forward pipes messages from a per-topic mailbox into the merged per-
// subscription channel until the mailbox is closed.
func forward(in <-chan *Message, out chan<- *Message) {
	for msg := range in {
		out <- msg
	}
}

func (b *memoryBroker) PartitionCount(_ string) (int, error) {
	// Memory broker has no concept of partitions — treat every topic as a
	// single-partition topic so callers can compute concurrency uniformly.
	return 1, nil
}

func (b *memoryBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	for _, topics := range b.groups {
		for _, mb := range topics {
			close(mb.ch)
		}
	}
	b.groups = nil
	return nil
}

// memorySubscription is a single consumer's view of the broker. Because
// standalone mode has no rebalances, Consume emits exactly one synthetic
// claim whose lifetime matches the subscription.
type memorySubscription struct {
	broker  *memoryBroker
	groupID string
	topics  []string
	out     chan *Message
	closed  atomic.Bool
}

func (s *memorySubscription) Consume(ctx context.Context, handler func(Claim) error) error {
	claimCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	claim := &memoryClaim{ctx: claimCtx, ch: s.out}
	return handler(claim)
}

func (s *memorySubscription) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// We don't close s.out here — the broker owns the underlying mailboxes
	// and will close them on broker Close(). Closing out would risk a send
	// on a closed channel from a still-running publisher.
	return nil
}

type memoryClaim struct {
	ctx context.Context
	ch  <-chan *Message
}

func (c *memoryClaim) Messages() <-chan *Message { return c.ch }
func (c *memoryClaim) Context() context.Context  { return c.ctx }
func (c *memoryClaim) MarkMessage(*Message)      {}
