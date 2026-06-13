//go:build e2e

package harness

import (
	"context"
	"fmt"
	"io"
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

	// Datahub is non-nil only when New() was called with
	// WithReprocessReady(). Pre-allocated on the network gateway IP
	// BEFORE merkle-service starts and threaded into the container's
	// DATAHUB_FALLBACK_URLS so /reprocess can find blocks staged here
	// without needing a prior live block-fetch.
	//
	// For other scenarios (round-trip, single-tx registration, etc.)
	// tests build their own datahub via h.NewDatahub(t) after
	// New() returns.
	Datahub *Datahub
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

	// Step 3a (optional): pre-allocate a Datahub port + URL so we can
	// thread the URL into DATAHUB_FALLBACK_URLS BEFORE merkle-service
	// starts. Required for /reprocess scenarios because that endpoint
	// looks up datahubs in the operator-configured fallbacks (or the
	// internal registry); it doesn't take a URL in the request body.
	// The actual listener binds in step 5 after containers are up.
	var datahubPort int
	if cfg.reprocessReady {
		datahubPort, err = pickFreeTCPPort()
		if err != nil {
			t.Fatalf("pre-pick datahub port: %v", err)
		}
		if cfg.merkleStart.ExtraEnv == nil {
			cfg.merkleStart.ExtraEnv = make(map[string]string)
		}
		cfg.merkleStart.ExtraEnv["DATAHUB_FALLBACK_URLS"] = fmt.Sprintf("http://%s:%d", gateway, datahubPort)
	}

	// Step 4: Postgres + Redpanda + merkle-service on the prepared
	// network.
	containers, err := startContainersOnNetwork(ctx, t, cfg.merkleStart, nw, gateway)
	if err != nil {
		t.Fatalf("start containers: %v", err)
	}

	// Step 5 (optional): bind the pre-registered Datahub on the port
	// we reserved in step 3a. Has to happen AFTER container start so
	// t.Cleanup ordering matches teardown expectations (datahub closes
	// before containers, so a final container request doesn't hit a
	// dead listener).
	var datahub *Datahub
	if cfg.reprocessReady {
		datahub, err = NewDatahubOnPort(t, DatahubOptions{AnnounceHost: gateway}, datahubPort)
		if err != nil {
			t.Fatalf("bind pre-registered datahub: %v", err)
		}
	}

	h := &Harness{Containers: containers, LibP2P: libp2p, Datahub: datahub}
	t.Cleanup(func() {
		// Post-mortem: on a failed e2e test, dump the merkle-service container
		// logs before teardown. These round-trips fail upstream of arcade more
		// often than not (merkle-service didn't emit callbacks, /reprocess
		// errored, …) and the container logs are the only place that's visible.
		if t.Failed() && containers != nil && containers.Merkle != nil {
			logCtx, logCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if r, lErr := containers.Merkle.Logs(logCtx); lErr == nil {
				b, _ := io.ReadAll(r)
				_ = r.Close()
				t.Logf("=== merkle-service container logs (test failed) ===\n%s\n=== end merkle-service logs ===", b)
			}
			logCancel()
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer closeCancel()
		if err := containers.Close(closeCtx); err != nil {
			t.Logf("container teardown errors: %v", err)
		}
	})
	return h
}

// NewDatahub spins up an in-process datahub HTTP server announcing on
// the docker-network gateway IP discovered when the harness's network
// was created. The returned datahub is reachable from the merkle-service
// container regardless of whether host.docker.internal is routable on
// the runtime — same fix as the libp2p host.
//
// Lifetime is tied to t via t.Cleanup. Tests call this from within a
// scenario after harness.New(t) has run.
func (h *Harness) NewDatahub(t *testing.T) *Datahub {
	t.Helper()
	d, err := NewDatahubWith(t, DatahubOptions{AnnounceHost: h.Containers.GatewayIP})
	if err != nil {
		t.Fatalf("new datahub: %v", err)
	}
	return d
}

// Option mutates harness configuration before startup.
type Option func(*harnessOptions)

type harnessOptions struct {
	startupTimeout time.Duration
	merkleStart    MerkleStartOptions
	// reprocessReady opts into the pre-registered datahub path: the
	// harness reserves a port + advertises a datahub URL via merkle-
	// service's DATAHUB_FALLBACK_URLS env var, then binds the
	// listener after containers start. h.Datahub is non-nil only
	// when this option was set.
	reprocessReady bool
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

// WithMerkleImage overrides the merkle-service image. Default is the
// digest-pinned defaultMerkleImage (see containers.go) rather than the
// floating `:latest` tag, which has regressed the round-trip in the past.
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

// WithReprocessReady tells the harness to pre-allocate a Datahub
// bound to the docker-network gateway IP BEFORE merkle-service
// starts, and thread its URL into the container's
// DATAHUB_FALLBACK_URLS env var so merkle-service's /reprocess
// endpoint can find blocks staged on the harness datahub without
// needing a prior live block-fetch to populate the internal datahub
// registry.
//
// The pre-allocated datahub is exposed as h.Datahub. Tests stage
// fixtures via h.Datahub.StageFixture(fix) and then trigger
// /reprocess via harness.TriggerReprocess(...).
//
// Without this option, tests build their own datahub via
// h.NewDatahub(t). That works for the round-trip scenarios where
// the libp2p BlockMessage carries the DataHubURL inline (so
// merkle-service learns about the URL on first fetch), but it
// doesn't work for /reprocess because the endpoint doesn't accept
// a DataHubURL in the request body.
func WithReprocessReady() Option {
	return func(o *harnessOptions) { o.reprocessReady = true }
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
