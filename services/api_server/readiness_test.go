package api_server

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

func TestServer_StartSignalsReadyAfterListen(t *testing.T) {
	var _ services.ReadyNotifier = (*Server)(nil)

	port := freeListenPort(t)
	cfg := &config.Config{}
	cfg.APIServer.Host = "127.0.0.1"
	cfg.APIServer.Port = port
	cfg.Telemetry.ServiceName = "arcade-test"
	srv := New(cfg, zap.NewNop(), nil, nil, nil, nil, nil, nil, nil, nil)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	assertSignalsAfterBind(t, addr, func(ctx context.Context) error {
		return srv.Start(ctx)
	}, func(fn func()) { srv.NotifyReady(fn) })
}

func TestServer_StartListenFailureDoesNotSignal(t *testing.T) {
	cfg := &config.Config{}
	cfg.APIServer.Host = "127.0.0.1"
	cfg.APIServer.Port = -1
	cfg.Telemetry.ServiceName = "arcade-test"
	srv := New(cfg, zap.NewNop(), nil, nil, nil, nil, nil, nil, nil, nil)
	assertListenFailureSilent(t, func(ctx context.Context) error {
		return srv.Start(ctx)
	}, func(fn func()) { srv.NotifyReady(fn) })
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

// assertSignalsAfterBind fails the test unless start signals only once addr
// accepts connections, then cancels so start can return.
func assertSignalsAfterBind(t *testing.T, addr string, start func(context.Context) error, arm func(func())) {
	t.Helper()
	ready := make(chan struct{})
	var dialErr error
	arm(func() {
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
	go func() { errCh <- start(ctx) }()

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
	case <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func assertListenFailureSilent(t *testing.T, start func(context.Context) error, arm func(func())) {
	t.Helper()
	signaled := false
	arm(func() { signaled = true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := start(ctx)
	if err == nil {
		t.Fatal("expected listen failure")
	}
	if signaled {
		t.Fatal("readiness signaled after listen failed")
	}
}
