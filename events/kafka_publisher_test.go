package events

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

func TestKafkaPublisherRoundtrip(t *testing.T) {
	broker := kafka.NewMemoryBroker(64)
	defer func() { _ = broker.Close() }()

	pub := NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Memory broker delivers immediately, but the consumer goroutine still
	// needs a moment to wire up the claim. Publish from a goroutine and
	// drain with a timeout.
	go func() {
		// Tiny sleep is unfortunate but avoids racing the consumer setup;
		// the alternative is exposing Ready() on Subscribe, which would
		// leak Kafka details into Publisher callers.
		time.Sleep(50 * time.Millisecond)
		_ = pub.Publish(ctx, &models.TransactionStatus{
			TxID:      "abc",
			Status:    models.StatusReceived,
			Timestamp: time.Unix(0, 1234),
		})
	}()

	select {
	case got := <-ch:
		if got.TxID != "abc" {
			t.Errorf("txid = %q, want %q", got.TxID, "abc")
		}
		if got.Status != models.StatusReceived {
			t.Errorf("status = %q, want %q", got.Status, models.StatusReceived)
		}
		if got.Timestamp.UnixNano() != 1234 {
			t.Errorf("timestamp ns = %d, want %d", got.Timestamp.UnixNano(), 1234)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive status update")
	}
}

// TestKafkaPublisherFanOut verifies that two independent Subscribe calls each
// see every published message — the property that makes this Publisher safe
// to use from both the SSE manager and the webhook service in the same
// process (and from multiple pods).
func TestKafkaPublisherFanOut(t *testing.T) {
	broker := kafka.NewMemoryBroker(64)
	defer func() { _ = broker.Close() }()

	pub := NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chA, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe A: %v", err)
	}
	chB, err := pub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("subscribe B: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = pub.Publish(ctx, &models.TransactionStatus{TxID: "x", Status: models.StatusMined, Timestamp: time.Now()})
	}()

	for label, ch := range map[string]<-chan *models.TransactionStatus{"A": chA, "B": chB} {
		select {
		case got := <-ch:
			if got.TxID != "x" || got.Status != models.StatusMined {
				t.Errorf("subscriber %s got %+v", label, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %s did not receive update", label)
		}
	}
}
