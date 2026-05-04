package events

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

func TestKafkaPublisherRoundtrip(t *testing.T) {
	broker := kafka.NewMemoryBroker(64)
	defer func() { _ = broker.Close() }()

	pub := NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop(), 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := pub.Subscribe(ctx, "test")
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

	pub := NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop(), 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chA, err := pub.Subscribe(ctx, "test")
	if err != nil {
		t.Fatalf("subscribe A: %v", err)
	}
	chB, err := pub.Subscribe(ctx, "test")
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

// TestKafkaPublisherDropMetricByCaller verifies that a slow subscriber's
// dropped messages bump arcade_events_subscriber_dropped_total with the
// caller label the subscriber registered with. This is the observability
// hook an operator uses to tell which subscriber (sse vs webhook) is
// failing to keep up.
func TestKafkaPublisherDropMetricByCaller(t *testing.T) {
	broker := kafka.NewMemoryBroker(1024)
	defer func() { _ = broker.Close() }()

	// Subscriber buffer of 1 makes drops trivial to force.
	pub := NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop(), 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const caller = "test-caller-drop-metric"

	// Snapshot the counter before subscribing so the assertion is robust to
	// other tests in the package incrementing it.
	before := testutil.ToFloat64(metrics.EventsSubscriberDroppedTotal.WithLabelValues(caller))

	ch, err := pub.Subscribe(ctx, caller)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Let the consumer wire up.
	time.Sleep(50 * time.Millisecond)

	// Publish more messages than the buffer holds without reading from ch,
	// so the kafka handler hits the default arm and counts drops.
	const want = 8
	for i := 0; i < want; i++ {
		_ = pub.Publish(ctx, &models.TransactionStatus{
			TxID:      "drop-test",
			Status:    models.StatusReceived,
			Timestamp: time.Now(),
		})
	}

	// Allow the consumer to process the publishes. The kafka memory broker
	// is best-effort timing-wise; poll the counter until the increment
	// stabilizes or we hit a timeout.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := testutil.ToFloat64(metrics.EventsSubscriberDroppedTotal.WithLabelValues(caller))
		if got-before >= 1 {
			// Got at least one drop with the right label — that's the
			// invariant under test. We don't assert exact counts because
			// the memory broker's delivery timing varies.
			_ = ch
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected drop counter for caller=%q to increment, before=%v after=%v",
		caller, before, testutil.ToFloat64(metrics.EventsSubscriberDroppedTotal.WithLabelValues(caller)))
}
