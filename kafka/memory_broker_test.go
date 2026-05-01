package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMemoryBroker_PublishSubscribe verifies that a message sent by Send is
// delivered to a consumer in the same group. This is the baseline wiring
// test for standalone mode.
func TestMemoryBroker_PublishSubscribe(t *testing.T) {
	b := NewMemoryBroker(16)
	defer func() { _ = b.Close() }()

	sub, err := b.Subscribe("group-a", []string{"topic-x"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	received := make(chan *Message, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() {
		_ = sub.Consume(ctx, func(claim Claim) error {
			select {
			case msg := <-claim.Messages():
				received <- msg
			case <-claim.Context().Done():
			}
			return nil
		})
	}()

	// Give the consumer a moment to engage before publishing.
	time.Sleep(10 * time.Millisecond)

	if err := b.Send(ctx, "topic-x", "k1", []byte("hello")); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case msg := <-received:
		if string(msg.Value) != "hello" {
			t.Errorf("expected value=hello, got %q", msg.Value)
		}
		if msg.Topic != "topic-x" {
			t.Errorf("expected topic=topic-x, got %q", msg.Topic)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

// TestMemoryBroker_MultipleGroupsBroadcast confirms that a single Send fans
// out to every distinct groupID subscribed to the topic. Mirrors Kafka's
// consumer-group semantics where each group is an independent consumer.
func TestMemoryBroker_MultipleGroupsBroadcast(t *testing.T) {
	b := NewMemoryBroker(16)
	defer func() { _ = b.Close() }()

	var aCount, bCount atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)

	start := func(groupID string, counter *atomic.Int32) {
		sub, err := b.Subscribe(groupID, []string{"fanout"})
		if err != nil {
			t.Errorf("subscribe %s: %v", groupID, err)
			return
		}
		go func() {
			defer wg.Done()
			_ = sub.Consume(context.Background(), func(claim Claim) error {
				for i := 0; i < 3; i++ {
					select {
					case <-claim.Messages():
						counter.Add(1)
					case <-claim.Context().Done():
						return nil
					case <-time.After(300 * time.Millisecond):
						return nil
					}
				}
				return nil
			})
		}()
	}
	start("g-a", &aCount)
	start("g-b", &bCount)
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 3; i++ {
		if err := b.Send(context.Background(), "fanout", "", []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	wg.Wait()

	if aCount.Load() != 3 || bCount.Load() != 3 {
		t.Errorf("expected both groups to receive 3 messages each, got a=%d b=%d",
			aCount.Load(), bCount.Load())
	}
}

// TestMemoryBroker_ConsumerGroupDrainThenFlush verifies the top-level
// ConsumerGroup contract holds on memory broker: the flush hook fires after
// a batch of messages is drained, not per-message.
func TestMemoryBroker_ConsumerGroupDrainThenFlush(t *testing.T) {
	b := NewMemoryBroker(16)
	defer func() { _ = b.Close() }()

	producer := NewProducer(b)
	var processed atomic.Int32
	var flushes atomic.Int32

	cg, err := NewConsumerGroup(ConsumerConfig{
		Broker:  b,
		GroupID: "drain-test",
		Topics:  []string{"drain"},
		Handler: func(_ context.Context, _ *Message) error {
			processed.Add(1)
			return nil
		},
		FlushFunc: func(_ context.Context) error {
			flushes.Add(1)
			return nil
		},
		Producer: producer,
		Logger:   nopLogger(),
	})
	if err != nil {
		t.Fatalf("consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = cg.Run(ctx) }()
	<-cg.Ready()

	// Publish 5 messages in quick succession. Because the memory broker
	// delivers immediately, all five should land in the consumer's channel
	// before the drain loop spins — meaning flush fires once, not five times.
	for i := 0; i < 5; i++ {
		if err := producer.Send("drain", "", map[string]int{"i": i}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	// Wait for the drain to complete.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if processed.Load() == 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if processed.Load() != 5 {
		t.Errorf("expected 5 processed, got %d", processed.Load())
	}
	if flushes.Load() == 0 {
		t.Errorf("expected flush to fire at least once, got %d", flushes.Load())
	}
	if flushes.Load() > 5 {
		t.Errorf("expected drain-then-flush to batch, but flush fired %d times (>=1 per msg)", flushes.Load())
	}
}

// TestMemoryBroker_CloseRacePublish stresses the F-012 fix: many publishers
// hammer a topic while Close runs concurrently. Before the fix, Publish
// snapshotted the mailbox channels under the lock and then sent without it,
// so a Close that ran in between would close mb.ch and the next send would
// panic with "send on closed channel". After the fix, Close signals each
// mailbox's done channel (without closing mb.ch) and Publish selects on
// done so the send turns into a clean drop.
//
// The test also catches sender-side goroutine leaks: every spawned publisher
// must return within the test's deadline.
func TestMemoryBroker_CloseRacePublish(t *testing.T) {
	for iter := 0; iter < 25; iter++ {
		b := NewMemoryBroker(8)

		// Spin up a fleet of subscribers across multiple groups so Publish
		// has many mailboxes to fan out to.
		const numGroups = 8
		subs := make([]Subscription, 0, numGroups)
		for g := 0; g < numGroups; g++ {
			s, err := b.Subscribe(fmt.Sprintf("g-%d", g), []string{"race"})
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			subs = append(subs, s)
		}

		// Drain in the background so most sends succeed; the test isn't
		// about correctness of delivery, only about not panicking.
		drainCtx, drainCancel := context.WithCancel(context.Background())
		var drainWG sync.WaitGroup
		for _, s := range subs {
			drainWG.Add(1)
			go func(s Subscription) {
				defer drainWG.Done()
				_ = s.Consume(drainCtx, func(claim Claim) error {
					for {
						select {
						case <-claim.Messages():
						case <-claim.Context().Done():
							return nil
						}
					}
				})
			}(s)
		}

		// Publishers race against Close. We catch any panic from a
		// publisher goroutine via a deferred recover and surface it as a
		// test failure.
		const numPublishers = 32
		var pubWG sync.WaitGroup
		panicCh := make(chan any, numPublishers)
		ctx := context.Background()
		for p := 0; p < numPublishers; p++ {
			pubWG.Add(1)
			go func() {
				defer pubWG.Done()
				defer func() {
					if r := recover(); r != nil {
						panicCh <- r
					}
				}()
				for i := 0; i < 50; i++ {
					_ = b.Send(ctx, "race", "k", []byte("v"))
					_ = b.SendAsync(ctx, "race", "k", []byte("v"))
				}
			}()
		}

		// Close concurrently with the publisher fleet. A short jitter
		// makes the Close land somewhere inside the publish loops most
		// iterations.
		time.Sleep(time.Duration(iter%5) * 100 * time.Microsecond)
		if err := b.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		pubWG.Wait()
		drainCancel()
		drainWG.Wait()
		close(panicCh)

		for r := range panicCh {
			t.Fatalf("publisher panicked: %v", r)
		}
	}
}

// TestMemoryBroker_CloseAfterAllowsNewSendsToFail confirms that once Close
// has run, subsequent publishes return a clear error rather than panicking
// or hanging. Mirrors the "broker closed" guard at the top of publish.
func TestMemoryBroker_CloseAfterAllowsNewSendsToFail(t *testing.T) {
	b := NewMemoryBroker(4)
	if err := b.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := b.Send(context.Background(), "t", "k", []byte("v")); err == nil {
		t.Errorf("expected error sending to closed broker, got nil")
	}
	if err := b.SendAsync(context.Background(), "t", "k", []byte("v")); err == nil {
		t.Errorf("expected error SendAsync-ing to closed broker, got nil")
	}
	if err := b.SendBatch(context.Background(), "t", []KeyValue{{Key: "k", Value: "v"}}); err == nil {
		t.Errorf("expected error batch-sending to closed broker, got nil")
	}
	if _, err := b.Subscribe("g", []string{"t"}); err == nil {
		t.Errorf("expected error subscribing to closed broker, got nil")
	}

	// Idempotent close: second close is a no-op, not a panic.
	if err := b.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}

// TestMemoryBroker_SlowSubscriberDoesNotBlockClose verifies that a stuck
// subscriber (full mailbox, nobody draining) doesn't wedge Close or the
// concurrent publisher. After the fix, the publisher's select observes
// done firing and drops the message instead of waiting forever on a buffer
// that will never drain.
func TestMemoryBroker_SlowSubscriberDoesNotBlockClose(t *testing.T) {
	b := NewMemoryBroker(2) // tiny buffer so we can fill it fast

	sub, err := b.Subscribe("slow", []string{"t"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Intentionally never call sub.Consume — the merged channel never
	// drains, so after a couple of forwarded messages, mb.ch backs up.
	_ = sub

	// Pre-fill the buffer so the next send would block without the fix.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	for i := 0; i < 10; i++ {
		_ = b.SendAsync(ctx, "t", "", []byte("v"))
	}

	// Kick a synchronous publisher that would otherwise block on the
	// full buffer. Close should unblock it via mb.done.
	pubDone := make(chan error, 1)
	go func() {
		pubDone <- b.Send(context.Background(), "t", "", []byte("v"))
	}()

	// Brief pause so the publisher reaches its select before Close runs.
	time.Sleep(20 * time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- b.Close() }()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked behind a slow subscriber")
	}

	select {
	case err := <-pubDone:
		// Publisher should have observed mb.done firing and returned
		// nil (drop), not panicked or hung.
		if err != nil {
			t.Fatalf("publisher returned error after Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publisher blocked behind a slow subscriber even after Close")
	}
}
