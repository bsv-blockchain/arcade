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

	// LibP2P is the in-process libp2p peer that merkle-service treats as
	// its sole bootstrap peer. Auto-built by New() using the docker
	// network's gateway IP so containers can dial back to the harness on
	// runtimes where host.docker.internal isn't routable (rootless
	// podman). Tests that want full control of the libp2p host instead
	// (e.g. to drive failure-injection scenarios) build it themselves
	// via NewLibP2PHostWith and use the lower-level pieces.
	LibP2P *LibP2PHost
}

// New brings up the full harness in the right order to dodge the
// host-from-container chicken-and-egg:
//
//  1. Create the docker bridge network.
//  2. Inspect the network to discover the host-from-container gateway IP.
//  3. Build the in-process libp2p host announcing on that gateway IP
//     (so the merkle-service container can dial it back regardless of
//     whether host.docker.internal is routable on this runtime).
//  4. Bring up Postgres + Redpanda + merkle-service on the network with
//     the libp2p host's multiaddr threaded in as P2P_BOOTSTRAP_PEERS.
//
// Failure at any step triggers a synchronous Close on whatever was
// already running. On success, Close runs via t.Cleanup.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.startupTimeout)
	defer cancel()

	// Step 1+2: network and gateway discovery, before anything else
	// needs them.
	nw, gateway, err := newNetworkWithGateway(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = nw.Remove(closeCtx)
	})

	// Step 3: libp2p host with the gateway IP baked into its announce
	// multiaddr. Skipped when the caller explicitly passed a bootstrap
	// peer string — that mode is for tests that want to wire their own
	// libp2p configuration.
	var libp2p *LibP2PHost
	if cfg.merkleStart.BootstrapPeers == "" {
		libp2p, err = NewLibP2PHostWith(t, LibP2PHostOptions{
			Network:      cfg.merkleStart.P2PNetwork,
			AnnounceHost: gateway,
		})
		if err != nil {
			t.Fatalf("build libp2p host: %v", err)
		}
		t.Cleanup(func() { _ = libp2p.Close() })
		cfg.merkleStart.BootstrapPeers = libp2p.BootstrapMultiaddr()
	}

	// Step 4: Postgres + Redpanda + merkle-service on the prepared
	// network.
	containers, err := startContainersOnNetwork(ctx, t, cfg.merkleStart, nw, gateway)
	if err != nil {
		t.Fatalf("start containers: %v", err)
	}

	h := &Harness{Containers: containers, LibP2P: libp2p}
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
