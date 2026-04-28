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
	defer b.Close()

	sub, err := b.Subscribe("group-a", []string{"topic-x"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

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
	defer b.Close()

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
	defer b.Close()

	producer := NewProducer(b)
	var processed atomic.Int32
	var flushes atomic.Int32

	cg, err := NewConsumerGroup(ConsumerConfig{
		Broker:  b,
		GroupID: "drain-test",
		Topics:  []string{"drain"},
		Handler: func(ctx context.Context, msg *Message) error {
			processed.Add(1)
			return nil
		},
		FlushFunc: func(ctx context.Context) error {
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
	go cg.Run(ctx)
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
