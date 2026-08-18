package p2p_client

import (
	"context"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	p2pclient "github.com/bsv-blockchain/go-teranode-p2p-client"
	teranodep2p "github.com/bsv-blockchain/teranode/services/p2p"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
)

const (
	testPeerID  = "sender"
	testPeerURL = "https://peer.example"
)

// fakeEndpointWriter records every UpsertDatahubEndpoint call so tests can
// assert that p2p_client persisted discovered URLs to the shared store. The
// store is now the only sink — the in-process teranode.Client AddEndpoints
// path was removed when p2p_client was decoupled from the propagation pod.
type fakeEndpointWriter struct {
	mu       sync.Mutex
	calls    []store.DatahubEndpoint
	feeCalls []store.PeerPolicy
}

func (f *fakeEndpointWriter) UpsertDatahubEndpoint(_ context.Context, ep store.DatahubEndpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ep)
	return nil
}

func (f *fakeEndpointWriter) UpsertPeerPolicy(_ context.Context, pp store.PeerPolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.feeCalls = append(f.feeCalls, pp)
	return nil
}

func (f *fakeEndpointWriter) snapshot() []store.DatahubEndpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.DatahubEndpoint, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeEndpointWriter) feeSnapshot() []store.PeerPolicy {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.PeerPolicy, len(f.feeCalls))
	copy(out, f.feeCalls)
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
	// Existing tests announce hostnames like https://peer.example — resolve
	// everything to a public IP so validation never touches real DNS.
	c.lookupIP = func(_ context.Context, _ string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}
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

func waitForFeeUpserts(t *testing.T, w *fakeEndpointWriter, want int) []store.PeerPolicy {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		calls := w.feeSnapshot()
		if len(calls) >= want {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d peer-policy upserts, got %d: %+v", want, len(calls), calls)
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
			fc := newFakeTeraClient(testPeerID)
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
	fc := newFakeTeraClient(testPeerID)
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
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  testPeerID,
		BaseURL: testPeerURL,
	}

	calls := waitForUpserts(t, w, 1)
	if calls[0].URL != testPeerURL {
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
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	for i := 0; i < 3; i++ {
		fc.ch <- teranodep2p.NodeStatusMessage{PeerID: testPeerID, BaseURL: testPeerURL}
	}

	calls := waitForUpserts(t, w, 3)
	for _, c := range calls {
		if c.URL != testPeerURL {
			t.Errorf("unexpected URL in upsert: %+v", c)
		}
	}
}

// TestClient_EmptyURLsIgnored confirms an announcement with no usable URL
// produces no store write.
func TestClient_EmptyURLsIgnored(t *testing.T) {
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{PeerID: testPeerID}

	time.Sleep(50 * time.Millisecond)
	if calls := w.snapshot(); len(calls) != 0 {
		t.Errorf("empty-URL announcement triggered upsert: %+v", calls)
	}
}

// TestClient_InvalidURLRejected confirms that URLs failing validation never
// reach the store. RFC1918 with AllowPrivateURLs=false is the canonical
// rejection case.
func TestClient_InvalidURLRejected(t *testing.T) {
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  testPeerID,
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
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:         testPeerID,
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

// TestClient_UnresolvableURLNotPersisted is the production regression case:
// a peer announcing its own cluster-internal service name (unresolvable from
// this cluster) must never enter the shared endpoint registry.
func TestClient_UnresolvableURLNotPersisted(t *testing.T) {
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	c.lookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  testPeerID,
		BaseURL: "http://asset:8090/api/v1",
	}

	time.Sleep(50 * time.Millisecond)
	if calls := w.snapshot(); len(calls) != 0 {
		t.Errorf("unresolvable URL reached the store: %+v", calls)
	}
}

// TestClient_ValidationCacheSkipsRepeatLookups confirms the success-only TTL
// cache: three announcements of one URL cost exactly one DNS lookup, while
// the upsert-per-announcement contract (LastSeen advancing) is preserved.
func TestClient_ValidationCacheSkipsRepeatLookups(t *testing.T) {
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	var lookups atomic.Int64
	c.lookupIP = func(_ context.Context, _ string) ([]net.IP, error) {
		lookups.Add(1)
		return []net.IP{net.ParseIP("93.184.216.34")}, nil
	}
	_, stop := runStart(t, c)
	defer stop()

	for i := 0; i < 3; i++ {
		fc.ch <- teranodep2p.NodeStatusMessage{PeerID: testPeerID, BaseURL: testPeerURL}
	}

	waitForUpserts(t, w, 3)
	if got := lookups.Load(); got != 1 {
		t.Errorf("expected exactly 1 DNS lookup across 3 announcements, got %d", got)
	}
}

// TestRecordPeerPolicy_FeePolicy verifies the structured FeePolicy.MiningFee is
// persisted verbatim as the peer's observed fee.
func TestRecordPeerPolicy_FeePolicy(t *testing.T) {
	c, w := newTestClient(t, newFakeTeraClient(testPeerID))
	c.recordPeerPolicy(context.Background(), teranodep2p.NodeStatusMessage{
		PeerID:    "peer-1",
		BaseURL:   testPeerURL,
		FeePolicy: &teranodep2p.FeePolicy{MiningFee: teranodep2p.FeeAmount{Satoshis: 75, Bytes: 1000}},
	})

	fees := w.feeSnapshot()
	if len(fees) != 1 {
		t.Fatalf("expected 1 fee upsert, got %d: %+v", len(fees), fees)
	}
	if fees[0].PeerID != "peer-1" || fees[0].MiningFeeSatoshis != 75 || fees[0].MiningFeeBytes != 1000 {
		t.Errorf("fee = %+v, want {peer-1 75 1000 ...}", fees[0])
	}
	if fees[0].Network != config.NetworkMainnet {
		t.Errorf("network = %q, want %q", fees[0].Network, config.NetworkMainnet)
	}
}

// TestRecordPeerPolicy_LegacyMinMiningTxFee verifies the BSV/kB fallback is
// converted to satoshis-per-1000-bytes when no structured FeePolicy is present.
func TestRecordPeerPolicy_LegacyMinMiningTxFee(t *testing.T) {
	c, w := newTestClient(t, newFakeTeraClient(testPeerID))
	fee := 0.0000005 // BSV/kB -> 50 sat per 1000 bytes
	c.recordPeerPolicy(context.Background(), teranodep2p.NodeStatusMessage{
		PeerID:         "peer-2",
		MinMiningTxFee: &fee,
	})

	fees := w.feeSnapshot()
	if len(fees) != 1 {
		t.Fatalf("expected 1 fee upsert, got %d: %+v", len(fees), fees)
	}
	if fees[0].MiningFeeSatoshis != 50 || fees[0].MiningFeeBytes != 1000 {
		t.Errorf("fee = {%d %d}, want {50 1000}", fees[0].MiningFeeSatoshis, fees[0].MiningFeeBytes)
	}
}

// TestPeerPolicyOnlyRecordedForRegisteredURLs is the fix for a fee floor nobody
// could explain: /policy advertised 0 sat/kB while every endpoint in /health
// advertised 100.
//
// The policy feeding /policy used to be recorded ahead of the datahub-URL gate,
// so a peer with no URL — or one rejected by SSRF validation, which this
// network really does produce — set the network minimum while appearing nowhere
// in /health. Recording it after registration means the values /policy derives
// come from exactly the peers arcade can broadcast to, which is the set /health
// displays.
func TestPeerPolicyOnlyRecordedForRegisteredURLs(t *testing.T) {
	cheap := &teranodep2p.FeePolicy{MiningFee: teranodep2p.FeeAmount{Satoshis: 0, Bytes: 1000}}

	cases := []struct {
		name       string
		msg        teranodep2p.NodeStatusMessage
		wantRecord bool
	}{
		{
			name:       "peer with a registered URL counts",
			msg:        teranodep2p.NodeStatusMessage{PeerID: testPeerID, BaseURL: testPeerURL, FeePolicy: cheap},
			wantRecord: true,
		},
		{
			name: "peer advertising no URL does not count",
			msg:  teranodep2p.NodeStatusMessage{PeerID: testPeerID, FeePolicy: cheap},
		},
		{
			// A cluster-internal name that resolves nowhere reachable — the
			// "poisoned registry" case the endpoint source already filters.
			name: "peer whose URL is rejected does not count",
			msg:  teranodep2p.NodeStatusMessage{PeerID: testPeerID, BaseURL: "http://10.0.0.5:8090", FeePolicy: cheap},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeTeraClient(testPeerID)
			c, w := newTestClient(t, fc)
			_, stop := runStart(t, c)
			defer stop()

			fc.ch <- tc.msg

			if tc.wantRecord {
				fees := waitForFeeUpserts(t, w, 1)
				if fees[0].PeerID != testPeerID {
					t.Errorf("recorded the wrong peer: %+v", fees[0])
				}
				return
			}

			// Asserting an absence needs a barrier, not a sleep. Announcements
			// are consumed in order by a single goroutine, so once a later
			// message's registration is visible the earlier one has certainly
			// been handled. This barrier carries no fee policy of its own, so
			// any recorded fee could only have come from the case's message.
			fc.ch <- teranodep2p.NodeStatusMessage{PeerID: "barrier", BaseURL: "https://barrier.example"}
			waitForUpserts(t, w, 1)

			if fees := w.feeSnapshot(); len(fees) != 0 {
				t.Errorf("a peer with no registered URL must not set the network fee, got %+v", fees)
			}
		})
	}
}

// TestRecordPeerPolicy_NoFeeAdvertised verifies peers advertising no fee (old
// nodes) are skipped rather than recorded with a zero fee.
func TestRecordPeerPolicy_NoFeeAdvertised(t *testing.T) {
	c, w := newTestClient(t, newFakeTeraClient(testPeerID))
	c.recordPeerPolicy(context.Background(), teranodep2p.NodeStatusMessage{PeerID: "peer-4"})
	if fees := w.feeSnapshot(); len(fees) != 0 {
		t.Fatalf("expected no fee upsert for feeless peer, got %+v", fees)
	}
}

// TestRecordPeerPolicy_RejectsMalformedLegacyFee verifies that a malformed or
// malicious legacy MinMiningTxFee (NaN, Inf, negative, or absurdly large) is
// dropped rather than converted into a wrapped uint64 that would pollute the
// store and metrics.
func TestRecordPeerPolicy_RejectsMalformedLegacyFee(t *testing.T) {
	bad := []float64{math.NaN(), math.Inf(1), -0.0001, 1e30}
	for _, f := range bad {
		c, w := newTestClient(t, newFakeTeraClient(testPeerID))
		c.recordPeerPolicy(context.Background(), teranodep2p.NodeStatusMessage{
			PeerID:         "peer-bad",
			MinMiningTxFee: &f,
		})
		if fees := w.feeSnapshot(); len(fees) != 0 {
			t.Errorf("fee=%v: expected no upsert for malformed fee, got %+v", f, fees)
		}
	}
}

// TestRecordPeerPolicy_RecordsSizeLimits verifies the size limits teranode
// advertises alongside the fee are carried into the store, so the api-server
// can derive the network-wide maximum for GET /policy and intake.
func TestRecordPeerPolicy_RecordsSizeLimits(t *testing.T) {
	c, w := newTestClient(t, newFakeTeraClient(testPeerID))
	c.recordPeerPolicy(context.Background(), teranodep2p.NodeStatusMessage{
		PeerID:  "peer-sized",
		BaseURL: testPeerURL,
		FeePolicy: &teranodep2p.FeePolicy{
			MiningFee:           teranodep2p.FeeAmount{Satoshis: 75, Bytes: 1000},
			MaxTxSizePolicy:     100_000_000,
			MaxScriptSizePolicy: 500_000,
		},
	})

	fees := w.feeSnapshot()
	if len(fees) != 1 {
		t.Fatalf("expected 1 policy upsert, got %d: %+v", len(fees), fees)
	}
	if fees[0].MaxTxSizePolicy != 100_000_000 {
		t.Errorf("MaxTxSizePolicy = %d, want 100000000", fees[0].MaxTxSizePolicy)
	}
	if fees[0].MaxScriptSizePolicy != 500_000 {
		t.Errorf("MaxScriptSizePolicy = %d, want 500000", fees[0].MaxScriptSizePolicy)
	}
}

// TestRecordPeerPolicy_LegacyPeerLeavesSizesUnset verifies a node old enough to
// send only min_mining_tx_fee records zero size limits. Zero is the "did not
// advertise" sentinel readers skip — it must not be mistaken for a peer that
// accepts nothing, which under the network maximum would be harmless but under
// any future minimum would be catastrophic.
func TestRecordPeerPolicy_LegacyPeerLeavesSizesUnset(t *testing.T) {
	c, w := newTestClient(t, newFakeTeraClient(testPeerID))
	fee := 0.0000005
	c.recordPeerPolicy(context.Background(), teranodep2p.NodeStatusMessage{
		PeerID:         "peer-legacy",
		MinMiningTxFee: &fee,
	})

	fees := w.feeSnapshot()
	if len(fees) != 1 {
		t.Fatalf("expected 1 policy upsert, got %d: %+v", len(fees), fees)
	}
	if fees[0].MaxTxSizePolicy != 0 || fees[0].MaxScriptSizePolicy != 0 {
		t.Errorf("legacy peer must advertise no size limits, got {%d %d}",
			fees[0].MaxTxSizePolicy, fees[0].MaxScriptSizePolicy)
	}
}

// TestRecordPeerPolicy_UnstorableSizeKeepsFee pins the asymmetry: node_status
// is unauthenticated, so a peer can advertise a size too large to store. That
// must cost only the size — dropping the whole row would let a peer remove
// itself from the fee minimum just by sending garbage in another field.
func TestRecordPeerPolicy_UnstorableSizeKeepsFee(t *testing.T) {
	c, w := newTestClient(t, newFakeTeraClient(testPeerID))
	c.recordPeerPolicy(context.Background(), teranodep2p.NodeStatusMessage{
		PeerID:  "peer-hostile",
		BaseURL: testPeerURL,
		FeePolicy: &teranodep2p.FeePolicy{
			MiningFee:           teranodep2p.FeeAmount{Satoshis: 5, Bytes: 1000},
			MaxTxSizePolicy:     math.MaxUint64,
			MaxScriptSizePolicy: math.MaxUint64,
		},
	})

	fees := w.feeSnapshot()
	if len(fees) != 1 {
		t.Fatalf("the fee observation must survive an unstorable size, got %d upserts: %+v", len(fees), fees)
	}
	if fees[0].MiningFeeSatoshis != 5 || fees[0].MiningFeeBytes != 1000 {
		t.Errorf("fee = {%d %d}, want {5 1000}", fees[0].MiningFeeSatoshis, fees[0].MiningFeeBytes)
	}
	if fees[0].MaxTxSizePolicy != 0 || fees[0].MaxScriptSizePolicy != 0 {
		t.Errorf("unstorable sizes must be zeroed, got {%d %d}",
			fees[0].MaxTxSizePolicy, fees[0].MaxScriptSizePolicy)
	}
}

// TestClient_RegistrationCarriesAdvertisedPolicy is the end of the chain
// GET /health reads: the policy a node advertises in node_status travels with
// its URL into the registry, so an operator can see per endpoint what that node
// will accept.
func TestClient_RegistrationCarriesAdvertisedPolicy(t *testing.T) {
	fc := newFakeTeraClient(testPeerID)
	c, w := newTestClient(t, fc)
	_, stop := runStart(t, c)
	defer stop()

	fc.ch <- teranodep2p.NodeStatusMessage{
		PeerID:  testPeerID,
		BaseURL: testPeerURL,
		FeePolicy: &teranodep2p.FeePolicy{
			MiningFee:               teranodep2p.FeeAmount{Satoshis: 100, Bytes: 1000},
			MaxTxSizePolicy:         100_000_000,
			MaxScriptSizePolicy:     500_000,
			MaxTxSigopsCountsPolicy: 4_294_967_295,
		},
	}

	calls := waitForUpserts(t, w, 1)
	got := calls[0].Policy
	if got == nil {
		t.Fatal("registration dropped the advertised policy")
	}
	want := store.EndpointPolicy{
		MiningFeeSatoshis: 100, MiningFeeBytes: 1000,
		MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000,
		MaxTxSigopsCountsPolicy: 4_294_967_295,
	}
	if *got != want {
		t.Errorf("policy = %+v, want %+v", *got, want)
	}
}

// TestClient_RegistrationWithoutPolicyStillRegisters guards the ordering that
// matters: a node advertising no policy — or one too large to store — must
// still get its URL into the registry. The policy is a diagnostic; the URL is
// how transactions reach that node.
func TestClient_RegistrationWithoutPolicyStillRegisters(t *testing.T) {
	cases := []struct {
		name string
		msg  teranodep2p.NodeStatusMessage
	}{
		{
			name: "no policy advertised",
			msg:  teranodep2p.NodeStatusMessage{PeerID: testPeerID, BaseURL: testPeerURL},
		},
		{
			name: "policy too large to store",
			msg: teranodep2p.NodeStatusMessage{
				PeerID: testPeerID, BaseURL: testPeerURL,
				FeePolicy: &teranodep2p.FeePolicy{MaxTxSizePolicy: math.MaxUint64},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeTeraClient(testPeerID)
			c, w := newTestClient(t, fc)
			_, stop := runStart(t, c)
			defer stop()

			fc.ch <- tc.msg

			calls := waitForUpserts(t, w, 1)
			if calls[0].URL != testPeerURL {
				t.Errorf("URL = %q, want the announced URL", calls[0].URL)
			}
			if calls[0].Policy != nil {
				t.Errorf("policy = %+v, want nil", *calls[0].Policy)
			}
		})
	}
}
