//go:build e2e

package harness

import (
	"context"
	"testing"
	"time"
)

// TestLibP2PHost_StartsAndExposesMultiaddr is a fast self-test on the
// libp2p host alone — no containers, no merkle-service. It just confirms
// the host comes up, advertises a /dns4/host.docker.internal multiaddr,
// and tears down cleanly.
func TestLibP2PHost_StartsAndExposesMultiaddr(t *testing.T) {
	h, err := NewLibP2PHost(t, "regtest", 0)
	if err != nil {
		t.Fatalf("new libp2p host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if h.PeerID() == "" {
		t.Error("peer ID is empty")
	}
	if h.ListenPort() <= 0 {
		t.Error("listen port not set")
	}
	multiaddr := h.BootstrapMultiaddr()
	if multiaddr == "" {
		t.Error("bootstrap multiaddr is empty")
	}
	t.Logf("libp2p bootstrap multiaddr: %s", multiaddr)
}

// TestLibP2PHost_MerkleServicePeersWithHost is the joint test for the
// container layer + libp2p host. We bring up the libp2p host, feed its
// bootstrap multiaddr to merkle-service via P2P_BOOTSTRAP_PEERS, and
// assert merkle-service logs a `[CONNECTED] Topic peer <harness-peer-id>`
// line within a generous timeout. This proves
//
//   - merkle-service can reach the harness via host.docker.internal
//   - both sides agree on the regtest topic namespace
//   - gossipsub mesh formation succeeded.
//
// We use the merkle-service container's log output as the peering
// signal because msgbus.GetPeers() only counts peers we've received
// messages from — merkle-service stays silent in a static deployment,
// so the harness's own peerTracker would otherwise stay empty even
// when peering succeeds.
func TestLibP2PHost_MerkleServicePeersWithHost(t *testing.T) {
	skipIfNoDocker(t)

	host, err := NewLibP2PHost(t, "regtest", 0)
	if err != nil {
		t.Fatalf("new libp2p host: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	h := New(t, WithBootstrapPeers(host.BootstrapMultiaddr()))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// merkle-service's go-p2p-message-bus emits this exact prefix when it
	// completes a libp2p connection on a topic the peer is also subscribed
	// to. Matching the peer ID anchors the assertion to OUR harness host
	// rather than any incidental peer.
	signal := "[CONNECTED] Topic peer " + host.PeerID()
	if err := h.Containers.WaitForMerkleLogLine(ctx, signal, 90*time.Second); err != nil {
		t.Fatalf("merkle-service did not log peering with harness: %v", err)
	}
	t.Logf("merkle-service peered with harness on topic mesh (peer ID %s)", host.PeerID())
}
