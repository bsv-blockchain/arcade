package p2p_client

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	p2pclient "github.com/bsv-blockchain/go-teranode-p2p-client"
	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// fakeEndpointWriter records every UpsertDatahubEndpoint call so tests can
// assert that p2p_client persisted discovered URLs to the shared store.
type fakeEndpointWriter struct {
	mu    sync.Mutex
	calls []store.DatahubEndpoint
}

func (f *fakeEndpointWriter) UpsertDatahubEndpoint(_ context.Context, ep store.DatahubEndpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ep)
	return nil
}

func (f *fakeEndpointWriter) snapshot() []store.DatahubEndpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.DatahubEndpoint, len(f.calls))
	copy(out, f.calls)
	return out
}

// fakeTeraClient implements the teraClient interface so tests can push
// hand-crafted NodeStatusMessage values without standing up a libp2p host.
type fakeTeraClient struct {
	ch     chan teranodep2p.NodeStatusMessage
	closed chan struct{}
	id     string
}

func newFakeTeraClient(id string) *fakeTeraClient {
	return &fakeTeraClient{
		ch:     make(chan teranodep2p.NodeStatusMessage, 16),
		closed: make(chan struct{}),
		id:     id,
	}
}

func (f *fakeTeraClient) SubscribeNodeStatus(ctx context.Context) <-chan teranodep2p.NodeStatusMessage {
	return f.ch
}

func (f *fakeTeraClient) GetID() string { return f.id }

func (f *fakeTeraClient) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
		close(f.ch)
	}
	return nil
}

func newTestClient(t *testing.T, fc *fakeTeraClient, seedEndpoints []string) (*Client, *teranode.Client, *fakeEndpointWriter) {
	t.Helper()
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = true
	cfg.Network = config.NetworkMainnet

	tc := teranode.NewClient(seedEndpoints, "", teranode.HealthConfig{})
	w := &fakeEndpointWriter{}

	c := New(cfg, zaptest.NewLogger(t), nil, tc, w)
	c.clientFactory = func(_ context.Context, _ p2pclient.Config) (teraClient, error) { return fc, nil }
	return c, tc, w
}

// runStart starts the client in a goroutine with a cancelable context and
// returns a stop func that shuts it down cleanly. Tests that need to observe
// state after messages flow should sleep briefly before asserting — the
// consume loop runs asynchronously.
func runStart(t *testing.T, c *Client) (ctx context.Context, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		close(started)
		_ = c.Start(ctx)
	}()
	<-started
	// Give Start a moment to construct the client and launch consume.
	time.Sleep(10 * time.Millisecond)
	return ctx, func() {
		cancel()
		if err := c.Stop(); err != nil {
			t.Errorf("Stop returned: %v", err)
		}
	}
}

// waitForEndpoints polls the teranode client's endpoint list until it reaches
// `want` or the deadline fires. This keeps tests robust against the handler
// goroutine running in parallel with the assertions.
func waitForEndpoints(t *testing.T, tc *teranode.Client, want int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		eps := tc.GetEndpoints()
		if len(eps) == want {
			return eps
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d endpoints, got %d: %v", want, len(eps), eps)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// captureLibraryConfig returns a clientFactory that sends the config it sees
// through the returned channel, synchronising between the Start goroutine
// and the test goroutine so the race detector stays happy.
func captureLibraryConfig(fc teraClient) (clientFactory, <-chan p2pclient.Config) {
	ch := make(chan p2pclient.Config, 1)
	return func(_ context.Context, cfg p2pclient.Config) (teraClient, error) {
		ch <- cfg
		return fc, nil
	}, ch
}

// TestNetworkThreading_CanonicalToUpstream asserts that each canonical network
// value at the top level is translated into the upstream topic identifier and
// the matching bootstrap peer list. This is the path that was silently broken
// before the refactor: configuring stn/teratestnet subscribed to one topic
// while bootstrapping against a mismatched DNS, so no peers were ever seen.
func TestNetworkThreading_CanonicalToUpstream(t *testing.T) {
	cases := []struct {
		canonical       string
		wantTopic       string
		wantBootstrapIn string
	}{
		{config.NetworkMainnet, config.NetworkMainnet, "mainnet.bootstrap"},
		{config.NetworkTestnet, config.NetworkTestnet, "testnet.bootstrap"},
		{config.NetworkTeratestnet, config.NetworkTeratestnet, "teratestnet.bootstrap"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.canonical, func(t *testing.T) {
			fc := newFakeTeraClient("sender")
			cfg := &config.Config{}
			cfg.P2P.DatahubDiscovery = true
			cfg.Network = tc.canonical

			tcli := teranode.NewClient(nil, "", teranode.HealthConfig{})
			c := New(cfg, zaptest.NewLogger(t), nil, tcli, &fakeEndpointWriter{})
			factory, cfgCh := captureLibraryConfig(fc)
			c.clientFactory = factory

			_, stop := runStart(t, c)
			defer stop()

			select {
			case seen := <-cfgCh:
				if seen.Network != tc.wantTopic {
					t.Fatalf("topic: got %q, want %q", seen.Network, tc.wantTopic)
				}
				if len(seen.MsgBus.BootstrapPeers) == 0 {
					t.Fatalf("expected default bootstrap peers for %q", tc.canonical)
				}
				if !strings.Contains(seen.MsgBus.BootstrapPeers[0], tc.wantBootstrapIn) {
					t.Errorf("bootstrap: got %q, want substring %q",
						seen.MsgBus.BootstrapPeers[0], tc.wantBootstrapIn)
				}
			case <-time.After(time.Second):
				t.Fatal("clientFactory was not invoked within 1s")
			}
		})
	}
}

// Operator-supplied BootstrapPeers must win over the resolver defaults so
// private networks and bootstrap migrations remain possible without new
// config knobs.
func TestNetworkThreading_OperatorBootstrapWins(t *testing.T) {
	fc := newFakeTeraClient("sender")
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = true
	cfg.Network = config.NetworkTeratestnet
	cfg.P2P.BootstrapPeers = []string{"/dnsaddr/custom.bootstrap"}

	tcli := teranode.NewClient(nil, "", teranode.HealthConfig{})
	c := New(cfg, zaptest.NewLogger(t), nil, tcli, &fakeEndpointWriter{})
	factory, cfgCh := captureLibraryConfig(fc)
	c.clientFactory = factory

	_, stop := runStart(t, c)
	defer stop()

	select {
	case seen := <-cfgCh:
		if len(seen.MsgBus.BootstrapPeers) != 1 || seen.MsgBus.BootstrapPeers[0] != "/dnsaddr/custom.bootstrap" {
			t.Fatalf("operator bootstrap ignored, got %v", seen.MsgBus.BootstrapPeers)
		}
	case <-time.After(time.Second):
		t.Fatal("clientFactory was not invoked within 1s")
	}
}

func TestClient_NovelURLRegistered(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, tc, w := newTestClient(t, fc, []string{"https://static.example"})
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  "sender",
		BaseURL: "https://peer.example",
	}

	eps := waitForEndpoints(t, tc, 2)
	found := false
	for _, e := range eps {
		if e == "https://peer.example" {
			found = true
		}
	}
	if !found {
		t.Errorf("discovered URL not present: %v", eps)
	}

	// The discovery must also have been written to the shared store so that
	// other pods (propagation, bump-builder running as separate K8s
	// deployments) pick it up via teranode.Client's refresh loop. Poll
	// briefly because the upsert happens after AddEndpoints.
	deadline := time.Now().Add(time.Second)
	var calls []store.DatahubEndpoint
	for time.Now().Before(deadline) {
		calls = w.snapshot()
		if len(calls) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(calls) != 1 || calls[0].URL != "https://peer.example" {
		t.Errorf("expected one upsert for https://peer.example, got %+v", calls)
	}
	if calls[0].Source != store.DatahubEndpointSourceDiscovered {
		t.Errorf("expected source=discovered, got %q", calls[0].Source)
	}
}

func TestClient_RepeatedAnnouncementRegisteredOnce(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, tc, _ := newTestClient(t, fc, []string{"https://static.example"})
	_, stop := runStart(t, c)
	defer stop()

	for i := 0; i < 3; i++ {
		fc.ch <- teranodep2p.NodeStatusMessage{PeerID: "sender", BaseURL: "https://peer.example"}
	}

	eps := waitForEndpoints(t, tc, 2)
	if len(eps) != 2 {
		t.Errorf("expected 2 endpoints after repeated announcement, got %v", eps)
	}
}

func TestClient_EmptyURLsIgnored(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, tc, _ := newTestClient(t, fc, []string{"https://static.example"})
	_, stop := runStart(t, c)
	defer stop()

	// Peer with no advertised URLs — library pre-decoded the message so
	// we don't have a malformed-JSON path to test anymore; this exercises
	// the equivalent "peer sent nothing usable" branch.
	fc.ch <- teranodep2p.NodeStatusMessage{PeerID: "sender"}

	time.Sleep(50 * time.Millisecond)
	eps := tc.GetEndpoints()
	if len(eps) != 1 {
		t.Errorf("empty-URL announcement mutated endpoint list: %v", eps)
	}
}

func TestClient_InvalidURLRejected(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, tc, _ := newTestClient(t, fc, []string{"https://static.example"})
	_, stop := runStart(t, c)
	defer stop()

	// RFC1918 URL should be rejected with AllowPrivateURLs=false (default).
	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  "sender",
		BaseURL: "http://192.168.1.50:8080",
	}

	time.Sleep(50 * time.Millisecond)
	eps := tc.GetEndpoints()
	if len(eps) != 1 {
		t.Errorf("private URL was registered despite allow_private_urls=false: %v", eps)
	}
}

func TestClient_PropagationURLPreferred(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, tc, _ := newTestClient(t, fc, nil)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:         "sender",
		BaseURL:        "https://base.example",
		PropagationURL: "https://prop.example",
	}

	eps := waitForEndpoints(t, tc, 1)
	if eps[0] != "https://prop.example" {
		t.Errorf("expected PropagationURL to win, got %v", eps)
	}
}

func TestClient_DisabledDiscoveryOpensNoBus(t *testing.T) {
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = false
	tc := teranode.NewClient([]string{"https://static.example"}, "", teranode.HealthConfig{})

	sentinel := false
	c := New(cfg, zap.NewNop(), nil, tc, &fakeEndpointWriter{})
	c.clientFactory = func(_ context.Context, _ p2pclient.Config) (teraClient, error) {
		sentinel = true
		return newFakeTeraClient("should-not-run"), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	// Give Start long enough to have done any client construction if it
	// were going to. The disabled path should block on ctx.Done() with
	// nothing else running.
	time.Sleep(50 * time.Millisecond)
	if sentinel {
		t.Fatal("client factory invoked while discovery disabled")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Start did not return after ctx cancel")
	}
	if err := c.Stop(); err != nil {
		t.Errorf("Stop returned: %v", err)
	}
}
