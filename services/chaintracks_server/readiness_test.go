package chaintracks_server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/services"
)

func TestService_StartSignalsReadyAfterListen(t *testing.T) {
	var _ services.ReadyNotifier = (*Service)(nil)

	port := freeListenPort(t)
	cfg := &config.Config{}
	cfg.ChaintracksServer.Enabled = true
	cfg.ChaintracksServer.Host = "127.0.0.1"
	cfg.ChaintracksServer.Port = port
	svc := New(cfg, zap.NewNop(), nil, newFakeChaintracks())
	if svc == nil {
		t.Fatal("New returned nil")
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ready := make(chan struct{})
	var dialErr error
	svc.NotifyReady(func() {
		dialer := &net.Dialer{Timeout: time.Second}
		c, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			dialErr = err
		} else {
			_ = c.Close()
		}
		close(ready)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- svc.Start(ctx) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for startup readiness")
	}
	if dialErr != nil {
		t.Fatalf("signaled before the listener was bound: %v", dialErr)
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestService_StartListenFailureDoesNotSignal(t *testing.T) {
	cfg := &config.Config{}
	cfg.ChaintracksServer.Enabled = true
	cfg.ChaintracksServer.Host = "127.0.0.1"
	cfg.ChaintracksServer.Port = -1
	svc := New(cfg, zap.NewNop(), nil, newFakeChaintracks())

	signaled := false
	svc.NotifyReady(func() { signaled = true })
	ctx, cancel := context.WithCancel(context.Background())
	err := svc.Start(ctx)
	cancel()
	if err == nil {
		t.Fatal("expected listen failure")
	}
	if signaled {
		t.Fatal("readiness signaled after listen failed")
	}
}

func freeListenPort(t *testing.T) int {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}
