package config

import (
	"strings"
	"testing"
)

// baseValidConfig returns a Config populated with the minimum fields other
// validate() branches require so each test can focus on the network branch.
//
// Sets a non-empty CallbackToken because validate() now refuses to start
// when MerkleService.URL is set without a bearer token (issue #76 / F-018).
// Tests that need to exercise that branch override CallbackToken explicitly.
func baseValidConfig() *Config {
	cfg := &Config{}
	cfg.Mode = "all"
	cfg.Kafka.Backend = "memory"
	cfg.Store.Backend = "pebble"
	cfg.Store.Pebble.Path = "/tmp/arcade-test"
	cfg.Network = NetworkMainnet
	cfg.MerkleService.URL = "http://merkle.local"
	cfg.CallbackToken = "test-callback-token"
	return cfg
}

// merkle_service.url is optional: URL-presence is the runtime toggle for the
// Merkle integration. cmd/arcade/main.go only constructs a merkleservice.Client
// when the URL is set, and propagation nil-guards every dereference. The
// documented standalone profile ships with merkle_service.url: "" — validation
// must accept that, otherwise the standalone binary refuses to start (issue #59
// / finding F-001).
func TestValidate_AllowsEmptyMerkleServiceURL(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = ""
	if err := validate(cfg); err != nil {
		t.Fatalf("empty merkle_service.url should be accepted, got: %v", err)
	}
}

// A populated merkle_service.url is the production path and must continue to
// validate cleanly so the "set the URL to enable Merkle" toggle stays symmetric
// with the empty-URL case above.
func TestValidate_AcceptsPopulatedMerkleServiceURL(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = "http://merkle.local"
	if err := validate(cfg); err != nil {
		t.Fatalf("populated merkle_service.url should be accepted, got: %v", err)
	}
}

// Issue #76 / finding F-018: when the Merkle integration is wired up, the
// inbound /api/v1/merkle-service/callback endpoint MUST be authenticated. We
// fail fast at config load when MerkleService.URL is set without a
// callback_token rather than silently exposing an unauthenticated receiver
// that any unauthenticated caller could use to submit forged status updates.
func TestValidate_RequiresCallbackTokenWhenMerkleEnabled(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = "http://merkle.local"
	cfg.CallbackToken = ""
	err := validate(cfg)
	if err == nil {
		t.Fatal("expected error when merkle_service.url is set without callback_token")
	}
	if !strings.Contains(err.Error(), "callback_token") {
		t.Errorf("error should mention callback_token, got: %v", err)
	}
}

// Standalone profiles ship with merkle_service.url: "" and frequently leave
// callback_token empty too — there is no Merkle Service issuing callbacks, so
// no token is required. The validation must pass cleanly so the standalone
// binary keeps booting (issue #59 fix from #104 must continue to hold).
func TestValidate_AllowsEmptyCallbackTokenInStandaloneMode(t *testing.T) {
	cfg := baseValidConfig()
	cfg.MerkleService.URL = ""
	cfg.CallbackToken = ""
	if err := validate(cfg); err != nil {
		t.Fatalf("standalone (no merkle, no callback_token) should validate, got: %v", err)
	}
}

// Each canonical network name must validate cleanly. The empty string is also
// accepted — validate() normalizes it to mainnet so CLI users can omit the key.
// Regtest validates without bootstrap_peers here because baseValidConfig leaves
// p2p.datahub_discovery=false; the bootstrap-peers requirement only triggers
// when discovery is on (covered by TestValidate_RegtestRequiresBootstrapPeers).
func TestValidate_AcceptsCanonicalNetworks(t *testing.T) {
	for _, net := range []string{"", NetworkMainnet, NetworkTestnet, NetworkTeratestnet, NetworkRegtest} {
		cfg := baseValidConfig()
		cfg.Network = net
		if err := validate(cfg); err != nil {
			t.Errorf("network=%q unexpected error: %v", net, err)
		}
	}
}

// Anything outside the canonical set is rejected — operators who need a
// private network should override p2p.bootstrap_peers on top of a canonical
// choice, not invent a new name.
func TestValidate_RejectsUnknownNetwork(t *testing.T) {
	for _, net := range []string{"main", "stn", "ttn", "bogus"} {
		cfg := baseValidConfig()
		cfg.Network = net
		err := validate(cfg)
		if err == nil {
			t.Fatalf("network=%q should be rejected", net)
		}
		if !strings.Contains(err.Error(), "invalid network") {
			t.Errorf("error should mention invalid network, got: %v", err)
		}
	}
}

// ResolveP2PNetwork is the bridge between arcade's canonical names and the
// values go-teranode-p2p-client actually accepts. Regressing any of these
// pairings silently breaks bootstrap or topic subscription.
func TestResolveP2PNetwork(t *testing.T) {
	cases := []struct {
		network         string
		wantTopic       string
		wantBootstrapIn string // empty => expect no default peers
	}{
		{NetworkMainnet, NetworkMainnet, "mainnet.bootstrap.teranode.bsvb.tech"},
		{NetworkTestnet, NetworkTestnet, "testnet.bootstrap.teranode.bsvb.tech"},
		{NetworkTeratestnet, NetworkTeratestnet, "teratestnet.bootstrap.teranode.bsvb.tech"},
		{"", NetworkMainnet, "mainnet.bootstrap.teranode.bsvb.tech"},
		{NetworkRegtest, NetworkRegtest, ""},
	}
	for _, tc := range cases {
		t.Run(tc.network, func(t *testing.T) {
			topic, boots := ResolveP2PNetwork(tc.network)
			if topic != tc.wantTopic {
				t.Errorf("topic: got %q, want %q", topic, tc.wantTopic)
			}
			if tc.wantBootstrapIn == "" {
				if len(boots) != 0 {
					t.Errorf("expected no default bootstrap peers for %q, got %v", tc.network, boots)
				}
				return
			}
			if len(boots) == 0 {
				t.Fatalf("expected at least one bootstrap peer for %q", tc.network)
			}
			if !strings.Contains(boots[0], tc.wantBootstrapIn) {
				t.Errorf("bootstrap: got %q, want substring %q", boots[0], tc.wantBootstrapIn)
			}
		})
	}
}

// Regtest has no canonical bootstrap DNS, so validate() requires the operator
// to supply p2p.bootstrap_peers when datahub_discovery is on. Catching this at
// config load is much friendlier than a libp2p host that boots into a vacuum.
func TestValidate_RegtestRequiresBootstrapPeers(t *testing.T) {
	t.Run("missing peers errors", func(t *testing.T) {
		cfg := baseValidConfig()
		cfg.Network = NetworkRegtest
		cfg.P2P.DatahubDiscovery = true
		err := validate(cfg)
		if err == nil {
			t.Fatal("expected error when regtest+discovery has no bootstrap_peers")
		}
		if !strings.Contains(err.Error(), "bootstrap_peers") {
			t.Errorf("error should mention bootstrap_peers, got: %v", err)
		}
	})
	t.Run("peers populated passes", func(t *testing.T) {
		cfg := baseValidConfig()
		cfg.Network = NetworkRegtest
		cfg.P2P.DatahubDiscovery = true
		cfg.P2P.BootstrapPeers = []string{"/ip4/127.0.0.1/tcp/9905/p2p/12D3KooWxyz"}
		if err := validate(cfg); err != nil {
			t.Errorf("unexpected error with bootstrap_peers set: %v", err)
		}
	})
}

// chaintracks has no regtest genesis header, so validate() force-disables the
// embedded server when network=regtest. Otherwise api-server crashes at init
// with ErrUnknownNetwork.
func TestValidate_RegtestAutoDisablesChaintracksServer(t *testing.T) {
	cfg := baseValidConfig()
	cfg.Network = NetworkRegtest
	cfg.ChaintracksServer.Enabled = true
	if err := validate(cfg); err != nil {
		t.Fatalf("unexpected validate error: %v", err)
	}
	if cfg.ChaintracksServer.Enabled {
		t.Error("expected chaintracks_server.enabled to be flipped to false for regtest")
	}
}

// ResolveChaintracksNetwork translates to go-chaintracks's stricter accepted
// set. Regressing this is the bug that crashed prod with "unknown network:
// mainnet" — chainmanager.getGenesisHeader is an exact-match switch over
// "main"/"test"/"teratest"/"teratestnet".
func TestResolveChaintracksNetwork(t *testing.T) {
	cases := []struct {
		network string
		want    string
	}{
		{NetworkMainnet, "main"},
		{NetworkTestnet, "test"},
		{NetworkTeratestnet, NetworkTeratestnet},
		{"", "main"},
	}
	for _, tc := range cases {
		t.Run(tc.network, func(t *testing.T) {
			got := ResolveChaintracksNetwork(tc.network)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
