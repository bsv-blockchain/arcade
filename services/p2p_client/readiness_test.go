package p2p_client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	p2pclient "github.com/bsv-blockchain/go-teranode-p2p-client"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/services"
)

func TestClient_DisabledDiscoveryMarksHealthReady(t *testing.T) {
	var _ services.ReadyNotifier = (*Client)(nil)

	port := freeListenPort(t)
	hs := services.NewHealthServer(port, false, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs.Start(ctx)
	waitListening(t, port)

	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = false
	c := New(cfg, zap.NewNop(), nil, nil)
	gate, err := services.PrepareReadiness(hs, []services.Service{c})
	if err != nil {
		t.Fatalf("PrepareReadiness: %v", err)
	}
	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("before start: got %d, want 503", got)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()
	waitReady(t, port, http.StatusOK)

	gate.Disable()
	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("after shutdown: got %d, want 503", got)
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

func TestClient_DiscoverySignalsAfterFactoryReturns(t *testing.T) {
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = true
	cfg.Network = config.NetworkMainnet
	c := New(cfg, zap.NewNop(), nil, &fakeEndpointWriter{})

	entered := make(chan struct{})
	release := make(chan struct{})
	fc := newFakeTeraClient("peer")
	c.clientFactory = func(context.Context, p2pclient.Config) (teraClient, error) {
		close(entered)
		<-release
		return fc, nil
	}

	signaled := make(chan struct{})
	var missingBus bool
	c.NotifyReady(func() {
		missingBus = c.bus == nil
		close(signaled)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()

	<-entered
	select {
	case <-signaled:
		t.Fatal("signaled while client initialization was still blocked")
	default:
	}
	close(release)
	select {
	case <-signaled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for readiness after init")
	}
	if missingBus {
		t.Fatal("signaled before the p2p client existed")
	}
	cancel()
	_ = c.Stop()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return")
	}
}

func TestClient_FactoryErrorDoesNotSignal(t *testing.T) {
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = true
	cfg.Network = config.NetworkMainnet
	c := New(cfg, zap.NewNop(), nil, &fakeEndpointWriter{})
	c.clientFactory = func(context.Context, p2pclient.Config) (teraClient, error) {
		return nil, errors.New("libp2p down")
	}
	signaled := false
	c.NotifyReady(func() { signaled = true })

	err := c.Start(context.Background())
	if err == nil {
		t.Fatal("expected factory error")
	}
	if signaled {
		t.Fatal("readiness signaled after p2p init failed")
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

func waitListening(t *testing.T, port int) {
	t.Helper()
	dialer := &net.Dialer{Timeout: 50 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := dialer.DialContext(t.Context(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("health server did not listen on :%d", port)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readyStatus(t *testing.T, port int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/ready", port), nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func waitReady(t *testing.T, port, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := readyStatus(t, port); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for /ready %d", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
