//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	teranode "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/tests/e2e/harness"
)

// TestSmoke_RealBlockMined_SingleSubtree drives the full
// arcade ↔ merkle-service round-trip with a real mainnet block and
// asserts each watched tx reaches MINED with a merklePath that
// reproduces the block's real header merkle root.
//
// Block: 000000000000000001bc8a601dd5f0659d36a9b077808850375dfa2d9f009396
// (height 948351, 1911 txs, 1 subtree). Picked txids + their raw
// bytes live under tests/e2e/fixtures/blocks/<hash>/. Refresh via
// `go run ./tools/fetch-block-fixture --block <hash> --out ...`.
func TestSmoke_RealBlockMined_SingleSubtree(t *testing.T) {
	skipIfNoDocker(t)
	const blockHash = "000000000000000001bc8a601dd5f0659d36a9b077808850375dfa2d9f009396"

	fix := harness.LoadBlockFixture(t, blockHash)
	t.Logf("loaded fixture: height=%d txCount=%d picked=%d subtrees=%d",
		fix.Height, fix.TxCount, len(fix.PickedTxIDs), len(fix.Subtrees))

	h := harness.New(t)
	datahub := h.NewDatahub(t)
	datahub.StageFixture(fix)

	rt := harness.StartArcade(t, harness.ArcadeOptions{
		MerkleServiceURL: h.Containers.MerkleHostURL,
		// arcade runs on the host; from here the datahub is reachable
		// via loopback. merkle-service inside its container reaches
		// the same listener via the gateway IP — that URL goes into
		// the libp2p BlockMessage / SubtreeMessage payloads below.
		DatahubURL:      datahub.LocalURL(),
		LibP2PBootstrap: h.LibP2P.BootstrapMultiaddr(),
		MerkleAuthToken: "e2e-watch-token",
		CallbackToken:   "e2e-callback-token",
	})

	// Total scenario budget: 4 minutes. Container cold-pull + libp2p
	// peering is included; the actual work is ~30s warm.
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()

	// 1. Submit each picked tx through arcade. arcade's intake handler
	//    structurally validates + propagation calls merkle-service
	//    /watch with arcade's callback URL + token.
	txids, err := harness.BroadcastRawTxs(ctx, t, rt, fix.PickedRawTxs)
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	t.Logf("broadcast %d txs", len(txids))

	// 2. Wait for /watch registrations to land in merkle-service.
	//    Without this, the next-step block announcement could race
	//    ahead of arcade's propagation flush and merkle-service
	//    would emit STUMPs for unwatched txids (i.e., none).
	for _, id := range txids {
		if err := harness.WaitForMerkleRegistration(ctx, h.Containers.MerkleHostURL, id, 60*time.Second); err != nil {
			t.Fatalf("watch %s: %v", id, err)
		}
	}
	t.Logf("all %d txids registered with merkle-service", len(txids))

	// 3. Publish each subtree and the block via libp2p with the
	//    harness datahub URL. merkle-service:
	//      a. fetches /subtree/<hash> for each subtree announcement
	//         and finds the watched txids inside (they're real
	//         mainnet hashes, so the lookup hits)
	//      b. fetches /block/<hash> for the block announcement,
	//         processes each subtree's STUMP, sends SEEN_ON_NETWORK
	//         + STUMP + BLOCK_PROCESSED callbacks to arcade
	//    arcade's bump-builder consumes those, builds compound BUMPs
	//    against the real header merkle root, marks the txs MINED.
	for _, st := range fix.Subtrees {
		if err := h.LibP2P.PublishSubtree(ctx, teranode.SubtreeMessage{
			Hash:       st.Hash.String(),
			DataHubURL: datahub.HostURL(),
		}); err != nil {
			t.Fatalf("publish subtree %s: %v", st.Hash, err)
		}
	}
	if err := h.LibP2P.PublishBlock(ctx, teranode.BlockMessage{
		Hash:       fix.BlockHash.String(),
		Height:     fix.Height,
		DataHubURL: datahub.HostURL(),
	}); err != nil {
		t.Fatalf("publish block: %v", err)
	}

	// 4. Wait for every tx to reach MINED with a non-empty merklePath.
	if err := harness.WaitForMined(ctx, t, rt, txids, 90*time.Second); err != nil {
		t.Fatalf("MINED: %v", err)
	}
	t.Logf("all %d txs reached MINED", len(txids))

	// 5. Sanity-check: the merklePath in each tx status reproduces
	//    the block header's merkle root via SDK ComputeRoot. Catches
	//    "MINED happened but the path is wrong" silently — a
	//    confused-deputy bug between merkle-service and arcade's
	//    compound BUMP that the MINED-only assertion wouldn't catch.
	harness.AssertMerklePathsMatchHeaderRoot(t, rt, fix.MerkleRoot, txids)
}
