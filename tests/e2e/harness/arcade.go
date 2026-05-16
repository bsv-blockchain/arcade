//go:build e2e

package harness

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/bsv-blockchain/arcade/app"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/services"
)

// ArcadeRuntime is the in-process arcade instance the harness drives.
// Lifetime is tied to the *testing.T used to build it: t.Cleanup
// cancels the run context and waits for every service goroutine.
type ArcadeRuntime struct {
	Cfg     *config.Config
	Deps    *app.Deps
	Logger  *zap.Logger
	Port    int    // host port the api-server is bound to
	BaseURL string // http://127.0.0.1:<Port>; reachable from the test process
	HostURL string // http://host.docker.internal:<Port>; reachable from inside merkle-service container
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	cleanup func()
}

// ArcadeOptions carries the wiring info the harness threads into arcade
// at boot time. All fields are required for a meaningful smoke run; the
// containers + libp2p + datahub steps produce the values to fill in.
type ArcadeOptions struct {
	// MerkleServiceURL is the host-side URL of the merkle-service
	// container — what arcade hits when it POSTs to /watch.
	MerkleServiceURL string

	// DatahubURL is the host-side URL of the in-process datahub. Arcade
	// hits this when bump-builder fetches block data.
	DatahubURL string

	// LibP2PBootstrap is the multiaddr of the harness libp2p host.
	// Arcade's p2p_client subscribes to NodeStatus; without this the
	// service spins up but never sees any peers (which is fine for the
	// smoke test).
	LibP2PBootstrap string

	// MerkleAuthToken is the bearer token arcade sends on outbound
	// /watch requests. Tests pick anything non-empty.
	MerkleAuthToken string

	// CallbackToken is the bearer token arcade requires on inbound
	// callback requests. Must match what merkle-service is configured
	// to send.
	CallbackToken string
}

// StartArcade boots arcade in-process via the same code path the
// production binary uses (app.Bootstrap → app.BuildServices). The
// harness's t.Cleanup tears everything down.
func StartArcade(t *testing.T, opts ArcadeOptions) *ArcadeRuntime {
	t.Helper()

	port, err := pickFreeTCPPort()
	if err != nil {
		t.Fatalf("pick arcade port: %v", err)
	}

	cfg := buildArcadeConfig(t, port, opts)
	logger := zaptest.NewLogger(t).Named("arcade")

	ctx, cancel := context.WithCancel(context.Background())
	deps, cleanup, err := app.Bootstrap(ctx, cfg, logger)
	if err != nil {
		cancel()
		t.Fatalf("app.Bootstrap: %v", err)
	}

	svcs := app.BuildServices(deps)

	rt := &ArcadeRuntime{
		Cfg:     cfg,
		Deps:    deps,
		Logger:  logger,
		Port:    port,
		BaseURL: fmt.Sprintf("http://127.0.0.1:%d", port),
		HostURL: fmt.Sprintf("http://host.docker.internal:%d", port),
		cancel:  cancel,
		cleanup: cleanup,
	}

	for _, svc := range svcs {
		rt.wg.Add(1)
		go func(s services.Service) {
			defer rt.wg.Done()
			if err := s.Start(ctx); err != nil {
				logger.Warn("service exited with error", zap.String("service", s.Name()), zap.Error(err))
			}
		}(svc)
	}

	t.Cleanup(func() {
		rt.cancel()
		// Stop services explicitly so http.Server.Close runs before we
		// wait — Start blocks on http.ListenAndServe which only returns
		// after Stop closes the server.
		for _, svc := range svcs {
			_ = svc.Stop()
		}
		// Bound the wait — a stuck service would otherwise hang the
		// test indefinitely.
		done := make(chan struct{})
		go func() { rt.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Logf("arcade services did not stop within 15s")
		}
		rt.cleanup()
	})

	if err := rt.waitReady(15 * time.Second); err != nil {
		t.Fatalf("arcade not ready: %v", err)
	}
	return rt
}

func buildArcadeConfig(t *testing.T, port int, opts ArcadeOptions) *config.Config {
	t.Helper()
	pebbleDir := t.TempDir()
	cfg := &config.Config{
		Mode:          "all",
		LogLevel:      "warn",
		Network:       config.NetworkRegtest,
		StoragePath:   t.TempDir(),
		CallbackURL:   fmt.Sprintf("http://host.docker.internal:%d/api/v1/merkle-service/callback", port),
		CallbackToken: opts.CallbackToken,
		APIServer: config.API{
			Host: "0.0.0.0",
			Port: port,
		},
		DatahubURLs: []string{opts.DatahubURL},
		MerkleService: config.MerkleServiceConfig{
			URL:       opts.MerkleServiceURL,
			AuthToken: opts.MerkleAuthToken,
		},
		Kafka: config.Kafka{
			Backend:       "memory",
			ConsumerGroup: "arcade-e2e",
			MaxRetries:    5,
			BufferSize:    10000,
		},
		Store: config.Store{
			Backend: "pebble",
			Pebble: config.Pebble{
				Path:                  pebbleDir,
				MemTableSizeMB:        16,
				L0CompactionThreshold: 4,
				SyncWrites:            false,
			},
		},
		Health: config.HealthConfig{Port: 0},
		Webhook: config.WebhookConfig{
			MaxRetries:              3,
			ExpirationMinutes:       60,
			InitialBackoffMs:        500,
			MaxBackoffMs:            5000,
			HTTPTimeoutMs:           5000,
			MaxConcurrentDeliveries: 4,
		},
		Callback: config.CallbackConfig{
			AllowPrivateIPs: true,
			MaxBodyBytes:    config.DefaultCallbackMaxBodyBytes,
		},
		Events: config.EventsConfig{
			SubscriberBuffer: config.DefaultEventsSubscriberBuffer,
		},
		// Disable chaintracks: regtest has no embedded genesis, and the
		// smoke test doesn't need header tracking — block announcements
		// flow direct from harness libp2p host to merkle-service.
		ChaintracksServer: config.ChaintracksServerConfig{Enabled: false},
		// Propagation defaults — endpoint health probes off the harness
		// datahub. Tightened backoff so retries don't slow down tests.
		Propagation: config.PropagationConfig{
			MerkleConcurrency:    2,
			RetryMaxAttempts:     3,
			RetryBackoffMs:       100,
			ReaperIntervalMs:     5000,
			ReaperBatchSize:      100,
			TeranodeMaxBatchSize: 100,
			EndpointHealth: config.EndpointHealthConfig{
				FailureThreshold:    3,
				ProbeIntervalMs:     30000,
				ProbeTimeoutMs:      2000,
				MinHealthyEndpoints: 0,
				RefreshIntervalMs:   30000,
			},
		},
		BumpBuilder: config.BumpBuilderConfig{
			GraceWindowMs:        500,
			DataHubMaxBlockBytes: 1024 * 1024 * 16,
		},
		// Datahub discovery off: we seeded a static URL into DatahubURLs,
		// so arcade's bump-builder + propagation see it without needing
		// libp2p NodeStatus messages.
		P2P: config.P2PConfig{
			DatahubDiscovery: false,
			ListenPort:       0,
			DHTMode:          "off",
			BootstrapPeers:   []string{opts.LibP2PBootstrap},
			StoragePath:      t.TempDir(),
			AllowPrivateURLs: true,
		},
	}
	return cfg
}

// waitReady polls /health until arcade's api-server starts answering
// or the timeout elapses. Mostly cosmetic — the bump-builder and
// propagation services come up almost instantly with a memory broker,
// but gin's listener takes ~50ms to bind.
func (rt *ArcadeRuntime) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", rt.Port), 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("arcade /health not reachable within %s: %w", timeout, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
