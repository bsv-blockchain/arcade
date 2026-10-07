package watchdog

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/merkleservice"
)

func TestService_StartSignalsReadyWhileRunning(t *testing.T) {
	wd := newTestWatchdog(t, &watchdogStore{}, alwaysLeader(), merkleservice.NewClient("http://127.0.0.1:1", "tok", time.Second))
	// A long interval keeps the test from spinning ticks after the first one.
	wd.cfg.Interval = time.Hour
	svc := &Service{wd: wd, logger: zap.NewNop()}

	ready := make(chan struct{})
	svc.NotifyReady(func() { close(ready) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- svc.Start(ctx) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for startup readiness")
	}
	select {
	case err := <-errCh:
		t.Fatalf("Start returned before cancel: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}
