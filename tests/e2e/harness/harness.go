//go:build e2e

package harness

import (
	"context"
	"testing"
	"time"
)

// Harness is the top-level e2e test fixture. Tests construct one via New(t),
// drive scenarios through the harness's accessor methods, and rely on
// t.Cleanup for teardown.
//
// Sub-systems are exposed as exported fields so that tests with unusual
// needs (poking at the Postgres DSN, talking to Kafka directly) don't
// have to wait for first-class methods on Harness.
type Harness struct {
	Containers *Containers

	// Future fields — wired in subsequent steps:
	//   LibP2P  *LibP2PHost
	//   Datahub *Datahub
	//   Arcade  *ArcadeRuntime
}

// New brings up the full harness. Currently this is just the container
// layer (Postgres + Redpanda + merkle-service); the libp2p host, datahub
// fake, and in-process arcade are added in subsequent steps so each can
// be exercised in isolation while it's being built.
//
// Failure during start triggers a synchronous Close on whatever was
// already running. On success, Close runs via t.Cleanup.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.startupTimeout)
	defer cancel()

	containers, err := StartContainers(ctx, t, cfg.merkleStart)
	if err != nil {
		t.Fatalf("start containers: %v", err)
	}

	h := &Harness{Containers: containers}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer closeCancel()
		if err := containers.Close(closeCtx); err != nil {
			t.Logf("container teardown errors: %v", err)
		}
	})
	return h
}

// Option mutates harness configuration before startup.
type Option func(*harnessOptions)

type harnessOptions struct {
	startupTimeout time.Duration
	merkleStart    MerkleStartOptions
}

func defaultOptions() harnessOptions {
	return harnessOptions{
		// Cold-pull of the merkle-service image plus full container start
		// can take a while on a fresh runner. 5 minutes is generous; tests
		// hard-fail before then on warm caches.
		startupTimeout: 5 * time.Minute,
		merkleStart: MerkleStartOptions{
			P2PNetwork: "regtest",
		},
	}
}

// WithMerkleImage overrides the merkle-service image. Default is
// ghcr.io/bsv-blockchain/merkle-service:latest.
func WithMerkleImage(image string) Option {
	return func(o *harnessOptions) { o.merkleStart.Image = image }
}

// WithBootstrapPeers seeds merkle-service's libp2p bootstrap-peer list.
// In a typical test the value comes from the harness's in-process libp2p
// host once it's spun up; while that piece is still being built, tests
// can leave this empty and merkle-service will start with no peer (and
// report degraded health, which the wait strategy tolerates).
func WithBootstrapPeers(peers string) Option {
	return func(o *harnessOptions) { o.merkleStart.BootstrapPeers = peers }
}

// WithExtraMerkleEnv merges extra env vars into the merkle-service
// container's environment.
func WithExtraMerkleEnv(env map[string]string) Option {
	return func(o *harnessOptions) {
		if o.merkleStart.ExtraEnv == nil {
			o.merkleStart.ExtraEnv = make(map[string]string)
		}
		for k, v := range env {
			o.merkleStart.ExtraEnv[k] = v
		}
	}
}
