package kafka

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// memoryBroker is an in-process Broker. It's the standalone-mode counterpart
// to saramaBroker — zero external dependencies, same interface.
//
// Semantics:
//   - Each (groupID, topic, partition) has a buffered channel. Publishing to
//     a topic routes the message to one partition by key (Sarama-compatible
//     FNV-1a hash, #295 — so a smoke test observes the same family placement
//     a production topic would) and fans it out to every groupID currently
//     subscribed. Within a group, whichever consumer drains the partition
//     mailbox wins the next message (mirrors Kafka consumer-group semantics).
//   - Topics default to 1 partition; NewMemoryBrokerWithPartitions widens
//     selected topics. Consume invokes the handler once per (topic,
//     partition) — the same per-partition claim shape Sarama produces — so
//     services like the dep-aware propagator get one dispatcher per
//     partition in standalone mode too.
//   - Offsets are monotonically assigned per (topic, partition) so DLQ
//     envelopes and logs carry sensible values.
//   - MarkMessage is a no-op; at-most-once on crash is acceptable for a
//     single-binary deployment.
//   - Close signals every subscriber's done channel and prevents further
//     publishes. The mailbox channels themselves are deliberately NOT closed
//     so a publisher that snapshotted a mailbox under the lock and released
//     it before sending can't panic with "send on closed channel" when Close
//     runs concurrently. Mirrors the SSE fan-out fix from #78 (F-020); see
//     F-012 for the kafka memory-broker variant.
//   - Send() has a bounded wait via sendTimeout: when the destination mailbox
//     is full the producer gets ErrBrokerBackpressure rather than hanging.
//     Callers translate this into HTTP 503 / DLQ as appropriate. Default 2s.
type memoryBroker struct {
	mu          sync.Mutex
	closed      bool
	groups      map[string]map[string][]*memoryMailbox // [groupID][topic][partition] = mailbox
	offsets     map[string]*int64                      // per (topic,partition) monotonic offsets
	partitions  map[string]int                         // per-topic partition count; absent = 1
	buffer      int
	sendTimeout time.Duration
}

// ErrBrokerBackpressure is returned by Send when a subscriber's mailbox is
// full for longer than the broker's sendTimeout. The producer should treat
// it as transient: shed load to the client (e.g. HTTP 503) and let the
// caller retry rather than hanging the calling goroutine.
var ErrBrokerBackpressure = errors.New("broker mailbox full (backpressure)")

// defaultSendTimeout is the upper bound on how long Send blocks waiting for
// a slot in a full mailbox before returning ErrBrokerBackpressure. 2s is
// long enough to ride out brief flush gaps in a healthy consumer, short
// enough that an HTTP handler still completes within a typical client
// deadline.
const defaultSendTimeout = 2 * time.Second

// memoryMailbox is a per-(group, topic, partition) delivery queue.
// Subscriptions joined to the same group share it so either concurrent
// consumer wins the next message (standard consumer-group semantics).
//
// done is closed when the mailbox is being torn down (broker Close).
// Publishers select on it so a concurrent teardown turns a would-be
// send-on-closed-channel into a clean drop. The mailbox channel itself is
// intentionally never closed — consumers exit via done instead, and the
// unreferenced channel is left to the GC. See F-012.
type memoryMailbox struct {
	ch   chan *Message
	done chan struct{}
}

// NewMemoryBroker constructs an in-process broker. buffer is the per-mailbox
// channel capacity — larger values smooth out bursts at the cost of memory.
func NewMemoryBroker(buffer int) Broker {
	return NewMemoryBrokerWithTimeout(buffer, 0)
}

// NewMemoryBrokerWithTimeout exposes the sendTimeout knob for tests and
// callers that want to deliberately tighten or loosen backpressure handling.
// sendTimeout, when > 0, bounds how long Send blocks on a full mailbox before
// returning ErrBrokerBackpressure; zero falls back to defaultSendTimeout.
func NewMemoryBrokerWithTimeout(buffer int, sendTimeout time.Duration) Broker {
	return NewMemoryBrokerWithPartitions(buffer, sendTimeout, nil)
}

// NewMemoryBrokerWithPartitions additionally widens the named topics to the
// given partition counts (absent or non-positive = 1). Keyed publishes route
// with Sarama's default hash partitioner arithmetic so standalone-mode
// placement matches what the same keys would do on a real topic (#295:
// dependency families share a key, therefore a partition, therefore a
// dispatcher).
func NewMemoryBrokerWithPartitions(buffer int, sendTimeout time.Duration, partitions map[string]int) Broker {
	if buffer <= 0 {
		buffer = 10000
	}
	if sendTimeout <= 0 {
		sendTimeout = defaultSendTimeout
	}
	return &memoryBroker{
		groups:      make(map[string]map[string][]*memoryMailbox),
		offsets:     make(map[string]*int64),
		partitions:  partitions,
		buffer:      buffer,
		sendTimeout: sendTimeout,
	}
}

// partitionsFor returns the topic's configured partition count (min 1).
func (b *memoryBroker) partitionsFor(topic string) int {
	if n, ok := b.partitions[topic]; ok && n > 0 {
		return n
	}
	return 1
}

// partitionForKey mirrors sarama.NewHashPartitioner: FNV-1a 32-bit of the
// key, cast to int32, mod partition count, absolute value. An empty key is
// routed to partition 0 (deterministic; the real Sarama randomizes keyless
// messages, but no arcade producer publishes keyless to a widened topic and
// determinism is worth more to tests than fidelity here).
func partitionForKey(key string, n int) int32 {
	if n <= 1 || key == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	p := int32(h.Sum32()) % int32(n)
	if p < 0 {
		p = -p
	}
	return p
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

	partition := partitionForKey(key, b.partitionsFor(topic))
	offsetKey := fmt.Sprintf("%s#%d", topic, partition)
	offsetPtr, ok := b.offsets[offsetKey]
	if !ok {
		var o int64
		offsetPtr = &o
		b.offsets[offsetKey] = offsetPtr
	}
	offset := atomic.AddInt64(offsetPtr, 1) - 1

	// Snapshot the partition mailboxes that currently care about this
	// topic. Snapshot under the lock, then release before sending so a
	// blocked receiver doesn't stall other publishers. Each mailbox's done
	// channel rides along so a concurrent Close that tears down the mailbox
	// between the snapshot and the send is observed as a drop rather than a
	// panic.
	type target struct {
		ch   chan *Message
		done chan struct{}
	}
	var targets []target
	for _, topics := range b.groups {
		if mbs, ok := topics[topic]; ok && int(partition) < len(mbs) {
			mb := mbs[partition]
			targets = append(targets, target{ch: mb.ch, done: mb.done})
		}
	}
	b.mu.Unlock()

	msg := &Message{
		Topic:     topic,
		Key:       []byte(key),
		Value:     value,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
		// Injects the producer's trace context (nil on the disabled/no-span
		// path — no allocation) so standalone mode round-trips traces the
		// same way the Sarama-backed broker does end-to-end.
		Headers: InjectTraceContext(ctx),
	}

	for _, t := range targets {
		// Quick out for already-torn-down mailboxes: avoids the channel
		// dance for subscribers we know are gone. The send-site re-checks
		// done so the race between this peek and the select is safe.
		select {
		case <-t.done:
			continue
		default:
		}
		if async {
			// Async semantics: drop on full buffer or torn-down mailbox.
			select {
			case t.ch <- msg:
			case <-t.done:
				// Subscriber went away — drop silently.
			default:
				// Buffer full — drop silently (async semantics).
			}
			continue
		}
		// Sync semantics: bounded wait. If the mailbox is still full when
		// sendTimeout elapses we surface ErrBrokerBackpressure so the caller
		// (HTTP handler, validator publish, propagator publish) can shed load
		// rather than pin a goroutine on a slow consumer.
		timer := time.NewTimer(b.sendTimeout)
		select {
		case t.ch <- msg:
			timer.Stop()
		case <-t.done:
			// Subscriber went away mid-publish — drop and move on. The
			// caller asked for "synchronous" but the destination is
			// gone; treating this as a successful no-op matches the
			// async drop and avoids a misleading error to the producer.
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			return ErrBrokerBackpressure
		}
	}
	return nil
}

// Subscribe ignores the StartOffset parameter: the memory broker retains
// nothing, so every subscription is inherently StartLatest — only messages
// published while a mailbox exists are ever delivered.
func (b *memoryBroker) Subscribe(groupID string, topics []string, _ StartOffset) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, errors.New("memory broker closed")
	}

	topicMailboxes, ok := b.groups[groupID]
	if !ok {
		topicMailboxes = make(map[string][]*memoryMailbox)
		b.groups[groupID] = topicMailboxes
	}

	var claims []*memoryMailbox
	for _, t := range topics {
		mbs, ok := topicMailboxes[t]
		if !ok {
			n := b.partitionsFor(t)
			mbs = make([]*memoryMailbox, n)
			for i := range mbs {
				mbs[i] = &memoryMailbox{
					ch:   make(chan *Message, b.buffer),
					done: make(chan struct{}),
				}
			}
			topicMailboxes[t] = mbs
		}
		claims = append(claims, mbs...)
	}

	return &memorySubscription{
		broker:    b,
		groupID:   groupID,
		topics:    topics,
		mailboxes: claims,
	}, nil
}

func (b *memoryBroker) PartitionCount(topic string) (int, error) {
	return b.partitionsFor(topic), nil
}

func (b *memoryBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	// Signal every mailbox via its done channel. We deliberately do NOT
	// close mb.ch: a publisher may have snapshotted the channel under the
	// lock and released the lock before sending (see publish), so closing
	// it here would race that send and panic. Closing done is enough —
	// publishers select on it to drop, and consumers select on it to
	// exit. The unreferenced channel is reclaimed by the GC. (F-012)
	for _, topics := range b.groups {
		for _, mbs := range topics {
			for _, mb := range mbs {
				close(mb.done)
			}
		}
	}
	b.groups = nil
	return nil
}

// memorySubscription is a single consumer's view of the broker. Standalone
// mode has no rebalances, so Consume emits one synthetic claim per (topic,
// partition) mailbox, each with the subscription's lifetime — the same
// shape Sarama gives a consumer that owns every partition.
type memorySubscription struct {
	broker    *memoryBroker
	groupID   string
	topics    []string
	mailboxes []*memoryMailbox
	closed    atomic.Bool
}

// Consume runs handler once per claim. A single mailbox (the pre-#295
// common case: one topic, one partition) is served inline; multiple
// mailboxes run concurrently — matching Sarama, which invokes ConsumeClaim
// on its own goroutine per assigned partition. Returns the first handler
// error, if any.
func (s *memorySubscription) Consume(ctx context.Context, handler func(Claim) error) error {
	claimCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(s.mailboxes) == 1 {
		return handler(&memoryClaim{ctx: claimCtx, ch: s.mailboxes[0].ch})
	}

	errCh := make(chan error, len(s.mailboxes))
	var wg sync.WaitGroup
	wg.Add(len(s.mailboxes))
	for _, mb := range s.mailboxes {
		go func(mb *memoryMailbox) {
			defer wg.Done()
			if err := handler(&memoryClaim{ctx: claimCtx, ch: mb.ch}); err != nil {
				errCh <- err
			}
		}(mb)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (s *memorySubscription) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// The broker owns the underlying mailboxes and their done channels, and
	// will signal them on broker Close(). Closing channels here would risk a
	// send on a closed channel from concurrent publishers.
	return nil
}

type memoryClaim struct {
	ctx context.Context
	ch  <-chan *Message
}

func (c *memoryClaim) Messages() <-chan *Message { return c.ch }
func (c *memoryClaim) Context() context.Context  { return c.ctx }
func (c *memoryClaim) MarkMessage(*Message)      {}
