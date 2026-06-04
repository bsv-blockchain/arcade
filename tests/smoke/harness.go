//go:build smoke

package smoke

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

// arcadeRuntime is the in-process arcade instance the smoke tests drive.
// Lifetime is tied to the *testing.T used to build it: t.Cleanup cancels
// the run context and waits for every service goroutine.
type arcadeRuntime struct {
	cfg     *config.Config
	deps    *app.Deps
	logger  *zap.Logger
	baseURL string

	wg      sync.WaitGroup
	cancel  context.CancelFunc
	cleanup func()
}

// smokeOptions threads test-specific wiring into the in-process arcade
// boot. The smoke suite never talks to a real teranode — TeranodeURL is
// always a recordingTeranode's httptest URL.
type smokeOptions struct {
	// TeranodeURL is the single fake-teranode endpoint arcade sees via
	// DatahubURLs. Required.
	TeranodeURL string
}

// startArcadeSmoke boots arcade in-process via app.Bootstrap +
// app.BuildServices with the lightest configuration that still exercises
// the api-server → kafka → propagation → teranode path: memory broker,
// Pebble in a temp dir, no merkle-service, no chaintracks, no real p2p.
// t.Cleanup tears everything down deterministically.
func startArcadeSmoke(t *testing.T, opts smokeOptions) *arcadeRuntime {
	t.Helper()
	if opts.TeranodeURL == "" {
		t.Fatal("smokeOptions.TeranodeURL must be set")
	}

	port, err := pickFreeTCPPort()
	if err != nil {
		t.Fatalf("pick arcade port: %v", err)
	}

	cfg := buildSmokeConfig(t, port, opts)
	logger := zaptest.NewLogger(t).Named("arcade-smoke")

	ctx, cancel := context.WithCancel(context.Background())
	deps, cleanup, err := app.Bootstrap(ctx, cfg, logger)
	if err != nil {
		cancel()
		t.Fatalf("app.Bootstrap: %v", err)
	}

	svcs := app.BuildServices(deps)

	rt := &arcadeRuntime{
		cfg:     cfg,
		deps:    deps,
		logger:  logger,
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", port),
		cancel:  cancel,
		cleanup: cleanup,
	}

	for _, svc := range svcs {
		rt.wg.Add(1)
		go func(s services.Service) {
			defer rt.wg.Done()
			if err := s.Start(ctx); err != nil {
				logger.Warn("service exited with error",
					zap.String("service", s.Name()), zap.Error(err))
			}
		}(svc)
	}

	t.Cleanup(func() {
		rt.cancel()
		for _, svc := range svcs {
			_ = svc.Stop()
		}
		// Bound the wait: a stuck service shouldn't hang the test.
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

// buildSmokeConfig is intentionally close to tests/e2e/harness/arcade.go's
// buildArcadeConfig: the boot path is the same code, the deltas are the
// container-touching knobs the smoke suite avoids (merkle-service URL
// empty, libp2p disabled, single in-process teranode URL).
func buildSmokeConfig(t *testing.T, port int, opts smokeOptions) *config.Config {
	t.Helper()
	pebbleDir := t.TempDir()
	cfg := &config.Config{
		Mode:        "all",
		LogLevel:    "warn",
		Network:     config.NetworkRegtest,
		StoragePath: t.TempDir(),
		// CallbackURL is required by submit handlers' SSRF guard
		// computation but never actually dialed in this suite.
		CallbackURL:   fmt.Sprintf("http://127.0.0.1:%d/api/v1/merkle-service/callback", port),
		CallbackToken: "smoke-callback-token",
		APIServer: config.API{
			Host: "127.0.0.1",
			Port: port,
		},
		DatahubURLs: []string{opts.TeranodeURL},
		MerkleService: config.MerkleServiceConfig{
			// Empty URL disables merkle registration; propagator
			// skips the /watch step and proceeds straight to
			// broadcast — which is the path the test asserts on.
			URL: "",
		},
		Kafka: config.Kafka{
			Backend:       "memory",
			ConsumerGroup: "arcade-smoke",
			MaxRetries:    5,
			BufferSize:    65536,
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
			MaxRetries:              1,
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
		ChaintracksServer: config.ChaintracksServerConfig{Enabled: false},
		Propagation: config.PropagationConfig{
			MerkleConcurrency: 2,
			RetryMaxAttempts:  1,
			RetryBackoffMs:    50,
			ReaperIntervalMs:  60000, // effectively disabled for the test window
			ReaperBatchSize:   100,
			// Leave at default-ish so the test exercises chunking
			// when total tx count > batch size.
			TeranodeMaxBatchSize: 1024,
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
		// p2p disabled at every knob: DHT off, no bootstrap peers,
		// datahub-discovery off (we seeded DatahubURLs statically).
		P2P: config.P2PConfig{
			DatahubDiscovery: false,
			ListenPort:       0,
			DHTMode:          "off",
			BootstrapPeers:   nil,
			StoragePath:      t.TempDir(),
			AllowPrivateURLs: true,
		},
	}
	return cfg
}

// waitReady polls the configured api-server port until it accepts a TCP
// connection or the deadline elapses. Same pattern the e2e harness uses;
// avoids racing the listener bind.
func (rt *arcadeRuntime) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	dialer := &net.Dialer{Timeout: 250 * time.Millisecond}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		conn, err := dialer.DialContext(ctx, "tcp", rt.baseURL[len("http://"):])
		cancel()
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("arcade api-server not reachable within %s: %w", timeout, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// pickFreeTCPPort asks the kernel for an available port by binding to
// :0, then closing the listener. Same dance the e2e harness performs;
// race-prone in principle (another process could grab the port between
// close and bind) but reliable enough for tests.
func pickFreeTCPPort() (int, error) {
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
