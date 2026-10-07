package propagation

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/store"
)

func TestPropagator_StartSignalsReadyAfterConsumer(t *testing.T) {
	broker := kafka.NewMemoryBroker(8)
	t.Cleanup(func() { _ = broker.Close() })
	cfg := &config.Config{}
	cfg.Kafka.ConsumerGroup = "readiness"
	p := New(cfg, zap.NewNop(), kafka.NewProducer(broker), nil, nil, notLeaderLeaser{}, nil, nil)

	ready := make(chan struct{})
	var consumerMissing bool
	p.NotifyReady(func() {
		consumerMissing = p.consumer.Load() == nil
		close(ready)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- p.Start(ctx) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for startup readiness")
	}
	if consumerMissing {
		t.Fatal("signaled before the consumer group was created")
	}
	cancel()
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
}

func TestPropagator_StartConsumerErrorDoesNotSignal(t *testing.T) {
	cfg := &config.Config{}
	cfg.Kafka.ConsumerGroup = "readiness"
	p := New(cfg, zap.NewNop(), kafka.NewProducer(nil), nil, nil, nil, nil, nil)
	t.Cleanup(func() { _ = p.Stop() })

	signaled := false
	p.NotifyReady(func() { signaled = true })
	err := p.Start(context.Background())
	if err == nil {
		t.Fatal("expected consumer setup error")
	}
	if signaled {
		t.Fatal("readiness signaled after consumer setup failed")
	}
}

// notLeaderLeaser makes the reaper skip its store scan so a readiness test
// can start the propagator without a store.
type notLeaderLeaser struct{}

func (notLeaderLeaser) TryAcquireOrRenew(context.Context, string, string, time.Duration) (time.Time, error) {
	return time.Time{}, nil
}

func (notLeaderLeaser) Release(context.Context, string, string) error { return nil }

var _ store.Leaser = notLeaderLeaser{}
