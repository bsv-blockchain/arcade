package bump_builder

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/services"
	"github.com/bsv-blockchain/arcade/store"
)

func TestBuilder_StartSignalsReadyAfterConsumer(t *testing.T) {
	var _ services.ReadyNotifier = (*Builder)(nil)

	broker := kafka.NewMemoryBroker(8)
	t.Cleanup(func() { _ = broker.Close() })
	cfg := &config.Config{}
	cfg.Kafka.ConsumerGroup = "readiness"
	b := New(cfg, zap.NewNop(), kafka.NewProducer(broker), nil, tipStore{}, nil, nil)

	ready := make(chan struct{})
	var consumerMissing bool
	b.NotifyReady(func() {
		consumerMissing = b.consumer == nil
		close(ready)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- b.Start(ctx) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for startup readiness")
	}
	if consumerMissing {
		t.Fatal("signaled before the consumer group was created")
	}
	cancel()
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
}

func TestBuilder_StartConsumerErrorDoesNotSignal(t *testing.T) {
	cfg := &config.Config{}
	cfg.Kafka.ConsumerGroup = "readiness"
	b := New(cfg, zap.NewNop(), kafka.NewProducer(nil), nil, tipStore{}, nil, nil)

	signaled := false
	b.NotifyReady(func() { signaled = true })
	err := b.Start(context.Background())
	if err == nil {
		t.Fatal("expected consumer setup error")
	}
	if signaled {
		t.Fatal("readiness signaled after consumer setup failed")
	}
}

func TestReconciler_StartSignalsReadyBeforeChaintracksWait(t *testing.T) {
	var _ services.ReadyNotifier = (*Reconciler)(nil)

	st := newPebbleForTest(t)
	ctx := context.Background()
	seedMined(t, st, recOrphan, 10, recShared1)
	if err := st.UpsertBlockHeaderSeen(ctx, recOrphan, 10, time.Now()); err != nil {
		t.Fatalf("seed header: %v", err)
	}
	tip, err := st.GetActiveTipBlockHeight(ctx)
	if err != nil || tip == 0 {
		t.Fatalf("tip = %d err=%v, want a non-zero tip so the chaintracks wait actually blocks", tip, err)
	}

	stub := &stubChaintracks{}
	stub.setUnready()
	r := newTestReconciler(st, &capturePublisher{}, stub, func(c *config.ReconcilerConfig) {
		c.StartupFullScan = true
		c.FullScanChaintracksReadyTimeoutMs = 5000
	})

	ready := make(chan struct{})
	r.NotifyReady(func() { close(ready) })

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- r.Start(runCtx) }()
	select {
	case <-ready:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("readiness waited on the chaintracks startup scan")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("readiness took %s; the chaintracks wait must not gate it", elapsed)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
	if r.startupScanDone {
		t.Fatal("scan ran before readiness; the signal is supposed to precede the wait")
	}
}

// tipStore satisfies the one store call the startup janitor makes on an
// empty deployment so the builder can be started without a real database.
type tipStore struct{ store.Store }

func (tipStore) GetActiveTipBlockHeight(context.Context) (uint64, error) { return 0, nil }
