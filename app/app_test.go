package app

import (
	"reflect"
	"testing"

	"github.com/bsv-blockchain/arcade/config"
)

func TestModeNeedsChaintracks(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{"all", true},
		{"chaintracks", true},
		{"bump-builder", true},
		{"api-server", false},
		{"sse", false},
		{"propagation", false},
		{"p2p-client", false},
		{"watchdog", false},
		{"", false},
		{"unknown-mode", false},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			if got := modeNeedsChaintracks(tc.mode); got != tc.want {
				t.Errorf("modeNeedsChaintracks(%q) = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}

func TestResolveChaintracksBootstrapPeers(t *testing.T) {
	const sharedPeer = "/dns4/shared.example/tcp/9905/p2p/12D3KooWShared"
	const ctSpecific = "/dns4/chaintracks.example/tcp/9905/p2p/12D3KooWCT"

	cases := []struct {
		name             string
		network          string
		p2pPeers         []string
		chaintracksPeers []string
		want             []string
	}{
		{
			name:    "testnet default when nothing set",
			network: config.NetworkTestnet,
			want:    []string{"/dnsaddr/testnet.bootstrap.teranode.bsvb.tech"},
		},
		{
			name:    "regtest default is nil when nothing set",
			network: config.NetworkRegtest,
			want:    nil,
		},
		{
			name:     "p2p.bootstrap_peers feeds chaintracks",
			network:  config.NetworkRegtest,
			p2pPeers: []string{sharedPeer},
			want:     []string{sharedPeer},
		},
		{
			name:             "chaintracks-specific wins over p2p",
			network:          config.NetworkRegtest,
			p2pPeers:         []string{sharedPeer},
			chaintracksPeers: []string{ctSpecific},
			want:             []string{ctSpecific},
		},
		{
			name:             "chaintracks-specific wins over network default",
			network:          config.NetworkTestnet,
			chaintracksPeers: []string{ctSpecific},
			want:             []string{ctSpecific},
		},
		{
			name:     "p2p list wins over network default",
			network:  config.NetworkMainnet,
			p2pPeers: []string{sharedPeer},
			want:     []string{sharedPeer},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{Network: tc.network}
			cfg.P2P.BootstrapPeers = tc.p2pPeers
			cfg.Chaintracks.P2P.MsgBus.BootstrapPeers = tc.chaintracksPeers

			got := resolveChaintracksBootstrapPeers(cfg)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("resolveChaintracksBootstrapPeers() = %v, want %v", got, tc.want)
			}
		})
	}
}
