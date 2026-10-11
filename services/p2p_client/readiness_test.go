package p2p_client

import (
	"context"
	"errors"
	"testing"
	"time"

	p2pclient "github.com/bsv-blockchain/go-teranode-p2p-client"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
)

// TestClient_DisabledDiscoverySignalsReady: discovery being off is a
// successful start. The signal must fire while Start is still blocked on
// ctx, not as a side effect of returning.
func TestClient_DisabledDiscoverySignalsReady(t *testing.T) {
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = false
	c := New(cfg, zap.NewNop(), nil, nil)

	ready := make(chan struct{})
	c.NotifyReady(func() { close(ready) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()
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
