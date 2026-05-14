package propagation

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/kafka"
)

// dispatcherConsumer is the dedicated Kafka consumer for the
// dependency-aware propagation topic. Unlike the standard
// kafka.ConsumerGroup wrapper — which calls Claim.MarkMessage as soon as
// a handler returns — this consumer defers MarkMessage until the
// dispatcher reports the message's offset terminalized via the shared
// offsetTracker. That way a crash can never leave a tx half-processed
// with its offset already committed; Kafka replay rebuilds the
// dispatcher's state from the lowest-unfinished offset on restart.
//
// Architecture:
//
//   - Read loop: pulls Messages from a Claim, decodes the propagation
//     envelope, records the message in pendingMessages keyed by offset,
//     and pushes a dispatcherMsg onto the dispatcher's incomingMsgs
//     channel. The Kafka offset is attached so the dispatcher can
//     register it with its offsetTracker.
//
//   - Commit loop: periodically queries offsetTracker.LowestUnfinished,
//     calls MarkMessage on every pending message whose offset is below
//     that cursor, and drops the corresponding entries from
//     pendingMessages.
//
// The two loops run as goroutines off Run; both exit when the underlying
// claim ends (shutdown or rebalance) or ctx is canceled.
type dispatcherConsumer struct {
	broker        kafka.Broker
	groupID       string
	topic         string
	dispatcher    *Dispatcher
	offsets       offsetTrackerReader
	commitTicker  time.Duration
	incomingMsgs  chan<- propagationMsg
	logger        *zap.Logger
	pending       map[int64]*kafka.Message // offset → message awaiting MarkMessage
	pendingMu     sync.Mutex
	ready         chan struct{}
	readyOnce     sync.Once
	consumerGroup string
}

// offsetTrackerReader is the read-only slice of the dispatcher's
// offsetTracker that the commit loop needs. Defined here so the
// consumer doesn't depend on the full offsetTracker mutation surface.
type offsetTrackerReader interface {
	LowestUnfinished() (offset int64, ok bool)
	Empty() bool
}

// dispatcherConsumerConfig collects the wiring needed to construct a
// dispatcherConsumer. Kept as a struct so the constructor signature
// stays stable as we add more config (DLQ, retry budget, etc.) later.
type dispatcherConsumerConfig struct {
	Broker       kafka.Broker
	GroupID      string
	Topic        string
	Dispatcher   *Dispatcher
	Offsets      offsetTrackerReader
	IncomingMsgs chan<- propagationMsg
	CommitTicker time.Duration // 0 → default 200ms
	Logger       *zap.Logger
}

func newDispatcherConsumer(cfg dispatcherConsumerConfig) (*dispatcherConsumer, error) {
	if cfg.Broker == nil {
		return nil, fmt.Errorf("dispatcherConsumer: Broker is required")
	}
	if cfg.Dispatcher == nil {
		return nil, fmt.Errorf("dispatcherConsumer: Dispatcher is required")
	}
	if cfg.Offsets == nil {
		return nil, fmt.Errorf("dispatcherConsumer: Offsets is required")
	}
	if cfg.IncomingMsgs == nil {
		return nil, fmt.Errorf("dispatcherConsumer: IncomingMsgs is required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("dispatcherConsumer: Topic is required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("dispatcherConsumer: GroupID is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	tick := cfg.CommitTicker
	if tick <= 0 {
		tick = 200 * time.Millisecond
	}
	return &dispatcherConsumer{
		broker:        cfg.Broker,
		groupID:       cfg.GroupID,
		topic:         cfg.Topic,
		dispatcher:    cfg.Dispatcher,
		offsets:       cfg.Offsets,
		commitTicker:  tick,
		incomingMsgs:  cfg.IncomingMsgs,
		logger:        logger,
		pending:       make(map[int64]*kafka.Message),
		ready:         make(chan struct{}),
		consumerGroup: cfg.GroupID,
	}, nil
}

// Ready returns a channel that's closed once the consumer's subscription
// is live and the read loop has started. Tests block on this to ensure
// the consumer is actually consuming before producing test messages.
func (c *dispatcherConsumer) Ready() <-chan struct{} {
	return c.ready
}

// Run starts the consumer. Blocks until ctx is canceled or the
// subscription closes. Returns the error from the subscription's
// Consume call, if any.
func (c *dispatcherConsumer) Run(ctx context.Context) error {
	sub, err := c.broker.Subscribe(c.groupID, []string{c.topic})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", c.topic, err)
	}
	defer func() {
		if closeErr := sub.Close(); closeErr != nil {
			c.logger.Warn("dispatcherConsumer: subscription close error", zap.Error(closeErr))
		}
	}()

	return sub.Consume(ctx, func(claim kafka.Claim) error { //nolint:contextcheck // closure signature is fixed by Subscription.Consume; we deliberately use claim.Context() inside so rebalances cancel inner work, while the outer ctx terminates the whole subscription

		c.readyOnce.Do(func() { close(c.ready) })

		commitDone := make(chan struct{})
		commitCtx, commitCancel := context.WithCancel(claim.Context())
		defer commitCancel()

		go func() {
			defer close(commitDone)
			c.runCommitLoop(commitCtx, claim)
		}()

		c.runReadLoop(claim.Context(), claim)

		commitCancel()
		<-commitDone

		// Final commit pass — pick up anything terminalized between the
		// last tick and the claim ending.
		c.commitOnce(claim)
		return nil
	})
}

// runReadLoop reads from the claim until its message channel closes or
// its context is done. Each message is decoded into a dispatcherMsg,
// recorded in pending, and sent to the dispatcher's incomingMsgs
// channel. Send is blocking — when the dispatcher is backed up, this
// applies natural backpressure: the read loop stops, claim.Messages()
// fills its buffer, and Sarama stops fetching from the broker. That's
// the chain that prevents memory blow-up under sustained backlog.
func (c *dispatcherConsumer) runReadLoop(ctx context.Context, claim kafka.Claim) {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return
			}
			c.handleMessage(ctx, msg)
		case <-ctx.Done():
			return
		}
	}
}

// handleMessage decodes a single Kafka message and submits it to the
// dispatcher. On decode failure the message is logged and skipped — the
// offset is NOT recorded in pending, so the commit loop will not be
// blocked by a malformed payload. A DLQ pathway can be added later if
// silent skip turns out to be insufficient.
func (c *dispatcherConsumer) handleMessage(ctx context.Context, msg *kafka.Message) {
	var envelope propagationMsg
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		c.logger.Warn(
			"dispatcherConsumer: decode failed, skipping",
			zap.String("topic", msg.Topic),
			zap.Int32("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
			zap.Error(err),
		)
		return
	}
	envelope.KafkaOffset = msg.Offset

	c.pendingMu.Lock()
	c.pending[msg.Offset] = msg
	c.pendingMu.Unlock()

	select {
	case c.incomingMsgs <- envelope:
	case <-ctx.Done():
	}
}

// runCommitLoop ticks at commitTicker cadence, asking the offset
// tracker for the lowest-unfinished offset and committing every pending
// message below that cursor. Exits when ctx is done.
func (c *dispatcherConsumer) runCommitLoop(ctx context.Context, claim kafka.Claim) {
	ticker := time.NewTicker(c.commitTicker)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.commitOnce(claim)
		case <-ctx.Done():
			return
		}
	}
}

// commitOnce marks every pending message with offset below the
// dispatcher's lowest-unfinished cursor as processed. If the dispatcher
// reports empty, every pending message is marked — meaning every tx
// the consumer has handed in has terminalized.
func (c *dispatcherConsumer) commitOnce(claim kafka.Claim) {
	cursor, hasCursor := c.offsets.LowestUnfinished()

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if !hasCursor {
		// No in-flight offsets — everything we've handed in has
		// terminalized. Mark every pending message and clear.
		for off, msg := range c.pending {
			claim.MarkMessage(msg)
			delete(c.pending, off)
		}
		return
	}

	for off, msg := range c.pending {
		if off >= cursor {
			continue
		}
		claim.MarkMessage(msg)
		delete(c.pending, off)
	}
}
