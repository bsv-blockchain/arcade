//go:build e2e

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	teranodep2p "github.com/bsv-blockchain/go-teranode-p2p-client"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"
)

// LibP2PHost is the in-process libp2p peer the harness drives. It plays
// two roles:
//
//   - Sole bootstrap peer for the merkle-service container, so the
//     container's libp2p stack joins the same gossipsub mesh we publish on.
//   - Publisher for synthetic SubtreeMessage / BlockMessage announcements
//     that drive merkle-service's block-processing pipeline.
//
// The host listens on 0.0.0.0:<port> and announces itself via
// /dns4/host.docker.internal/<port>, which is what containers see when they
// dial back. Tests running directly on the host can reach the same peer
// via 127.0.0.1:<port>.
type LibP2PHost struct {
	client      msgbus.Client
	peerID      string
	listenPort  int
	network     string
	bootstrapMA string
}

// LibP2PHostOptions tunes how the in-process peer is built. Zero-value
// is the legacy "advertise via host.docker.internal" behavior; set
// AnnounceHost to a routable IP/hostname when host.docker.internal isn't
// reachable from your container runtime (notably rootless podman with
// slirp4netns, where host-gateway resolves to a non-routable link-local
// address).
type LibP2PHostOptions struct {
	// Network is the BSV-network identifier — "regtest" for the smoke
	// test. go-p2p-message-bus namespaces pubsub topics by this string.
	Network string
	// ListenPort can be 0 to let the OS pick a free TCP port; the chosen
	// port is encoded into BootstrapMultiaddr.
	ListenPort int
	// AnnounceHost overrides the hostname/IP in the libp2p announce
	// multiaddr. When empty, defaults to "host.docker.internal" (works
	// on Docker engines where host-gateway resolves to a routable
	// address). For rootless podman, pass the docker-network gateway
	// IP discovered via Containers.GatewayIP — that's the address
	// containers on the bridge use to reach the host.
	AnnounceHost string
}

// NewLibP2PHost spins up the harness's in-process libp2p peer.
//
// Equivalent to NewLibP2PHostWith(t, LibP2PHostOptions{Network: network,
// ListenPort: listenPort}). Kept for callers that don't need to override
// the announce host.
func NewLibP2PHost(t *testing.T, network string, listenPort int) (*LibP2PHost, error) {
	return NewLibP2PHostWith(t, LibP2PHostOptions{
		Network:    network,
		ListenPort: listenPort,
	})
}

// NewLibP2PHostWith is the option-bag variant of NewLibP2PHost. Use this
// when you need to override the announce host — typically with the
// docker-network gateway IP from Containers.GatewayIP.
//
// listenPort can be 0 to let the OS pick a free port; the chosen port
// is reflected in the returned BootstrapMultiaddr.
//
// On success, the returned host's BootstrapMultiaddr is what gets fed
// to merkle-service via P2P_BOOTSTRAP_PEERS. Close releases the libp2p
// host and waits for goroutines.
func NewLibP2PHostWith(t *testing.T, opts LibP2PHostOptions) (*LibP2PHost, error) {
	t.Helper()
	netName := opts.Network
	if netName == "" {
		netName = teranodep2p.NetworkRegtest
	}
	announceHost := opts.AnnounceHost
	if announceHost == "" {
		announceHost = "host.docker.internal"
	}
	listenPort := opts.ListenPort

	// Picking the port up-front (when listenPort == 0) lets us encode it
	// into the announce multiaddr before NewClient binds. There's a tiny
	// race window between the listener Close and the libp2p bind, but
	// it's <10ms and only matters in this test path.
	if listenPort == 0 {
		port, err := pickFreeTCPPort()
		if err != nil {
			return nil, fmt.Errorf("pick free port: %w", err)
		}
		listenPort = port
	}

	privKey, err := msgbus.GeneratePrivateKey()
	if err != nil {
		return nil, fmt.Errorf("generate p2p private key: %w", err)
	}

	announceAddr := announceMultiaddr(announceHost, listenPort)

	cfg := msgbus.Config{
		Name:            "arcade-e2e-harness",
		PrivateKey:      privKey,
		Port:            listenPort,
		AnnounceAddrs:   []string{announceAddr},
		DHTMode:         "off",
		EnableNAT:       false,
		EnableMDNS:      false,
		AllowPrivateIPs: true,
		// PeerCacheFile in TempDir keeps the state out of the user's home.
		PeerCacheFile: t.TempDir() + "/peer_cache.json",
	}
	client, err := msgbus.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create msgbus client: %w", err)
	}

	peerID := client.GetID()
	if peerID == "" {
		_ = client.Close()
		return nil, fmt.Errorf("msgbus client returned empty peer ID")
	}

	// Bootstrap multiaddr the merkle-service container dials. The peer
	// ID anchors the dial so libp2p verifies the remote identity.
	bootstrap := fmt.Sprintf("%s/p2p/%s", announceAddr, peerID)

	// Subscribe to the topics we'll publish on. Required for two reasons:
	//   (a) gossipsub forms mesh links only between subscribers, so a
	//       publish-only harness never builds a route to merkle-service.
	//   (b) msgbus's [CONNECTED] Topic peer log line — our peering signal
	//       in tests — fires only when both sides are subscribed to a
	//       shared topic.
	// Channels we just leak (drain isn't strictly needed for our
	// lifecycle since Close tears msgbus down). The brief sleep below
	// gives msgbus.Subscribe's internal goroutine time to complete
	// pubsub.Join before any quick test-teardown could race with it
	// (msgbus has a known close-of-closed-channel panic when Close races
	// with a still-pending Subscribe Join).
	for _, topic := range []string{
		teranodep2p.TopicName(netName, teranodep2p.TopicBlock),
		teranodep2p.TopicName(netName, teranodep2p.TopicSubtree),
	} {
		_ = client.Subscribe(topic)
	}
	time.Sleep(250 * time.Millisecond)

	return &LibP2PHost{
		client:      client,
		peerID:      peerID,
		listenPort:  listenPort,
		network:     netName,
		bootstrapMA: bootstrap,
	}, nil
}

// announceMultiaddr renders an announce multiaddr for the libp2p host.
// We use /ip4 when announceHost parses as a literal IPv4 address (the
// rootless-podman fallback path) and /dns4 otherwise (the legacy
// host.docker.internal path).
func announceMultiaddr(announceHost string, listenPort int) string {
	if ip := net.ParseIP(announceHost); ip != nil && ip.To4() != nil {
		return fmt.Sprintf("/ip4/%s/tcp/%d", announceHost, listenPort)
	}
	return fmt.Sprintf("/dns4/%s/tcp/%d", announceHost, listenPort)
}

// BootstrapMultiaddr is the /dns4/host.docker.internal/.../p2p/<id> form
// the merkle-service container should dial. Pass it as the value of the
// P2P_BOOTSTRAP_PEERS env var.
func (h *LibP2PHost) BootstrapMultiaddr() string { return h.bootstrapMA }

// PeerID returns the libp2p peer ID this host advertises.
func (h *LibP2PHost) PeerID() string { return h.peerID }

// ListenPort returns the TCP port libp2p is bound to.
func (h *LibP2PHost) ListenPort() int { return h.listenPort }

// Network returns the BSV network name the host's topics are namespaced by.
func (h *LibP2PHost) Network() string { return h.network }

// PeerCount returns the current number of peers libp2p has tracked. Useful
// for WaitForPeers convergence checks before publishing.
func (h *LibP2PHost) PeerCount() int {
	return len(h.client.GetPeers())
}

// WaitForPeers blocks until at least n peers are tracked on a topic, or
// timeout elapses. Note this is BEST-EFFORT: msgbus's peerTracker only
// counts peers we've received messages from, so a silent merkle-service
// (no publishes) will keep this counter at zero even though gossipsub
// mesh formation has succeeded. The intended use is post-publish: once
// the harness has published at least one message and any subscriber
// has echoed something back, this becomes a tight loop.
//
// For the up-front "is merkle-service ready to receive?" question,
// rely on a small sleep (gossipsub mesh forms in 1–3s after both peers
// subscribe) plus a publish-then-verify check on the merkle-service
// container's effects (callbacks delivered, etc.) instead.
func (h *LibP2PHost) WaitForPeers(ctx context.Context, n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		count := h.PeerCount()
		if count >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d peers (have %d)", n, count)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// PublishBlock encodes msg as JSON and publishes it on the network's
// block topic. Subscribers (merkle-service) receive it via gossipsub.
func (h *LibP2PHost) PublishBlock(ctx context.Context, msg teranode.BlockMessage) error {
	return h.publishJSON(ctx, teranodep2p.TopicName(h.network, teranodep2p.TopicBlock), msg)
}

// PublishSubtree encodes msg as JSON and publishes it on the network's
// subtree topic.
func (h *LibP2PHost) PublishSubtree(ctx context.Context, msg teranode.SubtreeMessage) error {
	return h.publishJSON(ctx, teranodep2p.TopicName(h.network, teranodep2p.TopicSubtree), msg)
}

func (h *LibP2PHost) publishJSON(ctx context.Context, topic string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", topic, err)
	}
	if err := h.client.Publish(ctx, topic, data); err != nil {
		return fmt.Errorf("publish %s: %w", topic, err)
	}
	return nil
}

// Close shuts down the libp2p host and frees its resources. Safe on a
// nil receiver.
func (h *LibP2PHost) Close() error {
	if h == nil || h.client == nil {
		return nil
	}
	return h.client.Close()
}

// pickFreeTCPPort asks the kernel to bind port 0 on loopback and returns
// the assigned port. The listener is closed immediately — the port is
// likely free for the brief window before the libp2p host re-binds.
func pickFreeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
