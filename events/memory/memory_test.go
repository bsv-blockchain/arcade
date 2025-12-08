package memory

import (
	"context"
	"testing"
	"time"

	// Note: context import kept for context.WithCancel and context.Canceled usage

	"github.com/bsv-blockchain/arcade/models"
)

func TestInMemoryPublisher_PublishSubscribe(t *testing.T) {
	pub := NewInMemoryPublisher(10)
	defer pub.Close()

	ctx := t.Context()

	ch, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	event := models.StatusUpdate{
		TxID:      "abc123",
		Status:    models.StatusReceived,
		Timestamp: time.Now(),
	}

	if err := pub.Publish(ctx, event); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case received := <-ch:
		if received.TxID != event.TxID {
			t.Errorf("Expected TxID %s, got %s", event.TxID, received.TxID)
		}
		if received.Status != event.Status {
			t.Errorf("Expected Status %s, got %s", event.Status, received.Status)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for event")
	}
}

func TestInMemoryPublisher_MultipleSubscribers(t *testing.T) {
	pub := NewInMemoryPublisher(10)
	defer pub.Close()

	ctx := t.Context()

	ch1, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe 1 failed: %v", err)
	}

	ch2, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe 2 failed: %v", err)
	}

	event := models.StatusUpdate{
		TxID:      "def456",
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	}

	if err := pub.Publish(ctx, event); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	received1 := <-ch1
	if received1.TxID != event.TxID {
		t.Errorf("Subscriber 1: Expected TxID %s, got %s", event.TxID, received1.TxID)
	}

	received2 := <-ch2
	if received2.TxID != event.TxID {
		t.Errorf("Subscriber 2: Expected TxID %s, got %s", event.TxID, received2.TxID)
	}
}

func TestInMemoryPublisher_SlowSubscriber(t *testing.T) {
	pub := NewInMemoryPublisher(2)
	defer pub.Close()

	ctx := t.Context()

	ch, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		event := models.StatusUpdate{
			TxID:      "slow123",
			Status:    models.StatusReceived,
			Timestamp: time.Now(),
		}
		if err := pub.Publish(ctx, event); err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	received := 0
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case <-ch:
			received++
		case <-timeout:
			if received < 2 {
				t.Errorf("Expected at least 2 events, got %d", received)
			}
			return
		}
	}
}

func TestInMemoryPublisher_Close(t *testing.T) {
	pub := NewInMemoryPublisher(10)

	ctx := t.Context()

	ch, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := pub.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, ok := <-ch
	if ok {
		t.Error("Expected channel to be closed")
	}
}

func TestInMemoryPublisher_ContextCancellation(t *testing.T) {
	pub := NewInMemoryPublisher(0)
	defer pub.Close()

	ctx, cancel := context.WithCancel(t.Context())

	ch, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	cancel()

	event := models.StatusUpdate{
		TxID:      "cancel123",
		Status:    models.StatusReceived,
		Timestamp: time.Now(),
	}

	err = pub.Publish(ctx, event)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}

	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
	}
}
