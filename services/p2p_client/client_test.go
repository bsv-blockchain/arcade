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
)

// fakeEndpointWriter records every UpsertDatahubEndpoint call so tests can
// assert that p2p_client persisted discovered URLs to the shared store. The
// store is now the only sink — the in-process teranode.Client AddEndpoints
// path was removed when p2p_client was decoupled from the propagation pod.
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

func (f *fakeTeraClient) SubscribeNodeStatus(_ context.Context) <-chan teranodep2p.NodeStatusMessage {
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

func newTestClient(t *testing.T, fc *fakeTeraClient) (*Client, *fakeEndpointWriter) {
	t.Helper()
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = true
	cfg.Network = config.NetworkMainnet

	w := &fakeEndpointWriter{}
	c := New(cfg, zaptest.NewLogger(t), nil, w)
	c.clientFactory = func(_ context.Context, _ p2pclient.Config) (teraClient, error) { return fc, nil }
	return c, w
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

// waitForUpserts polls the writer's recorded calls until at least `want`
// entries have been observed or the deadline fires. The handler goroutine
// runs in parallel with the test goroutine; polling keeps the assertions
// race-free without coupling to internal timing.
func waitForUpserts(t *testing.T, w *fakeEndpointWriter, want int) []store.DatahubEndpoint {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		calls := w.snapshot()
		if len(calls) >= want {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d upserts, got %d: %+v", want, len(calls), calls)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// captureLibraryConfig returns a clientFactory that sends the config it sees
// through the returned channel, synchronizing between the Start goroutine
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
		t.Run(tc.canonical, func(t *testing.T) {
			fc := newFakeTeraClient("sender")
			cfg := &config.Config{}
			cfg.P2P.DatahubDiscovery = true
			cfg.Network = tc.canonical

			c := New(cfg, zaptest.NewLogger(t), nil, &fakeEndpointWriter{})
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

	c := New(cfg, zaptest.NewLogger(t), nil, &fakeEndpointWriter{})
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

// TestClient_NovelURLPersisted asserts that a newly announced URL reaches the
// shared DatahubEndpoint registry with the expected fields. The store is the
// only path by which other pods learn the URL, so the test guards the
// contract end-to-end as far as this service is concerned.
func TestClient_NovelURLPersisted(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  "sender",
		BaseURL: "https://peer.example",
	}

	calls := waitForUpserts(t, w, 1)
	if calls[0].URL != "https://peer.example" {
		t.Errorf("upserted wrong URL: %+v", calls[0])
	}
	if calls[0].Source != store.DatahubEndpointSourceDiscovered {
		t.Errorf("expected source=discovered, got %q", calls[0].Source)
	}
	if calls[0].Network != config.NetworkMainnet {
		t.Errorf("expected network=mainnet, got %q", calls[0].Network)
	}
}

// TestClient_RepeatedAnnouncementUpsertedEachTime confirms p2p_client
// forwards every valid announcement to the store rather than deduping
// in-process. Idempotent dedup is the store's job (UPSERT semantics); this
// service is intentionally a thin pipe so the registry's LastSeen reflects
// the most recent observation per peer.
func TestClient_RepeatedAnnouncementUpsertedEachTime(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	for i := 0; i < 3; i++ {
		fc.ch <- teranodep2p.NodeStatusMessage{PeerID: "sender", BaseURL: "https://peer.example"}
	}

	calls := waitForUpserts(t, w, 3)
	for _, c := range calls {
		if c.URL != "https://peer.example" {
			t.Errorf("unexpected URL in upsert: %+v", c)
		}
	}
}

// TestClient_EmptyURLsIgnored confirms an announcement with no usable URL
// produces no store write.
func TestClient_EmptyURLsIgnored(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{PeerID: "sender"}

	time.Sleep(50 * time.Millisecond)
	if calls := w.snapshot(); len(calls) != 0 {
		t.Errorf("empty-URL announcement triggered upsert: %+v", calls)
	}
}

// TestClient_InvalidURLRejected confirms that URLs failing validation never
// reach the store. RFC1918 with AllowPrivateURLs=false is the canonical
// rejection case.
func TestClient_InvalidURLRejected(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  "sender",
		BaseURL: "http://192.168.1.50:8080",
	}

	time.Sleep(50 * time.Millisecond)
	if calls := w.snapshot(); len(calls) != 0 {
		t.Errorf("private URL was persisted despite allow_private_urls=false: %+v", calls)
	}
}

// TestClient_PropagationURLPreferred confirms PropagationURL wins over
// BaseURL when both are present (pickDatahubURL contract).
func TestClient_PropagationURLPreferred(t *testing.T) {
	fc := newFakeTeraClient("sender")
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:         "sender",
		BaseURL:        "https://base.example",
		PropagationURL: "https://prop.example",
	}

	calls := waitForUpserts(t, w, 1)
	if calls[0].URL != "https://prop.example" {
		t.Errorf("expected PropagationURL to win, got %q", calls[0].URL)
	}
}

func TestClient_DisabledDiscoveryOpensNoBus(t *testing.T) {
	cfg := &config.Config{}
	cfg.P2P.DatahubDiscovery = false

	sentinel := false
	c := New(cfg, zap.NewNop(), nil, &fakeEndpointWriter{})
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
