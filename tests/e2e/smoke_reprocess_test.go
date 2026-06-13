//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/tests/e2e/harness"
)

// TestSmoke_RealBlockMined_ViaReprocess proves merkle-service's
// /reprocess endpoint is wired end-to-end: arcade registers 10
// watched txids, the block they're in is NEVER announced over
// libp2p (so merkle-service never processes it live, simulating
// "arcade missed the BLOCK_PROCESSED message"), and the test
// triggers /reprocess to recover. After the call we expect the
// same valid compound BUMP + merklePaths as the round-trip test
// produces.
//
// Distinguishing this from TestSmoke_RealBlockMined_SingleSubtree:
//
//   - Round-trip: harness publishes a libp2p BlockMessage carrying
//     the harness datahub URL; merkle-service fetches the block
//     inline, emits callbacks on the live pipeline.
//   - Reprocess: no libp2p BlockMessage. merkle-service has no
//     in-memory knowledge of the block. /reprocess hits the
//     `DATAHUB_FALLBACK_URLS` env var (set by WithReprocessReady)
//     to find a datahub serving the block, then runs the same
//     pipeline with override callback URL/token scoped to arcade.
//
// Unlike the round-trip test this path doesn't require libp2p
// mesh formation, so it passes locally on rootless podman too.
func TestSmoke_RealBlockMined_ViaReprocess(t *testing.T) {
	// QUARANTINED: merkle-service's /reprocess enqueues the block message (HTTP
	// 202) but its franz block-processor consumer never fetches it, so no
	// callbacks are emitted and the txs never reach MINED — a regression from the
	// sarama->franz migration that only affects the recovery path (the live
	// round-trip in TestSmoke_RealBlockMined_SingleSubtree, which covers the
	// datahub-free callback path, passes). Tracked in
	// bsv-blockchain/merkle-service#148; re-enable once that ships.
	t.Skip("quarantined pending bsv-blockchain/merkle-service#148 (/reprocess block message not consumed by franz block-processor)")

	skipIfNoDocker(t)
	const blockHash = "000000000000000001bc8a601dd5f0659d36a9b077808850375dfa2d9f009396"

	fix := harness.LoadBlockFixture(t, blockHash)
	t.Logf("loaded fixture: height=%d txCount=%d picked=%d subtrees=%d",
		fix.Height, fix.TxCount, len(fix.PickedTxIDs), len(fix.Subtrees))

	// WithReprocessReady pre-allocates the harness datahub on the
	// network gateway IP and stuffs its URL into merkle-service's
	// DATAHUB_FALLBACK_URLS env var at container creation time. The
	// datahub listener binds after the container is up but on the
	// pre-reserved port, so the URL we registered up front is the
	// one that actually serves the block.
	h := harness.New(t, harness.WithReprocessReady())
	h.Datahub.StageFixture(fix)

	rt := harness.StartArcade(t, harness.ArcadeOptions{
		MerkleServiceURL: h.Containers.MerkleHostURL,
		DatahubURL:       h.Datahub.LocalURL(), // arcade hits loopback; merkle-service hits gateway IP
		LibP2PBootstrap:  h.LibP2P.BootstrapMultiaddr(),
		MerkleAuthToken:  "e2e-watch-token",
		CallbackToken:    "e2e-callback-token",
	})

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()

	// 1. Register the 10 watched txids with merkle-service via
	//    arcade's normal /tx → /watch flow.
	txids, err := harness.BroadcastRawTxs(ctx, t, rt, fix.PickedRawTxs)
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	for _, id := range txids {
		if err := harness.WaitForMerkleRegistration(ctx, h.Containers.MerkleHostURL, id, 60*time.Second); err != nil {
			t.Fatalf("watch %s: %v", id, err)
		}
	}
	t.Logf("registered %d txids with merkle-service", len(txids))

	// 2. KEY: never publish a libp2p BlockMessage. merkle-service
	//    therefore never processes the block live and never emits
	//    STUMPs/BLOCK_PROCESSED for our watched txs — arcade is
	//    blind to the mining event.

	// 3. Sanity-check: wait briefly, then assert none of the txs has
	//    been MINED yet. Without /reprocess there's no code path to
	//    MINED; if one accidentally fired the test below would still
	//    pass for the wrong reason.
	time.Sleep(5 * time.Second)
	for _, id := range txids {
		st, _, _ := harness.GetTxStatus(ctx, rt, id)
		if st.TxStatus == "MINED" || st.TxStatus == "IMMUTABLE" {
			t.Fatalf("tx %s reached %s before /reprocess — scenario invalidated", id, st.TxStatus)
		}
	}
	t.Log("confirmed all 10 txs are pre-MINED before /reprocess")

	// 4. Trigger /reprocess. merkle-service probes its fallback
	//    datahubs (we registered the harness URL via env), finds
	//    the block, queues a BlockMessage with OverrideCallbackURL
	//    + OverrideCallbackToken + BypassDedup, and the normal
	//    block-processing pipeline runs scoped to arcade alone.
	callbackURL := rt.HostURL + "/api/v1/merkle-service/callback"
	resp, err := harness.TriggerReprocess(ctx, h.Containers.MerkleHostURL, blockHash, callbackURL, "e2e-callback-token")
	if err != nil {
		t.Fatalf("reprocess: %v", err)
	}
	if resp.DataHubURL == "" {
		t.Fatalf("/reprocess returned empty dataHubUrl — DATAHUB_FALLBACK_URLS wiring failed")
	}
	t.Logf("merkle-service queued reprocess from datahub %s", resp.DataHubURL)

	// 5. Subtree workers build STUMPs filtered to arcade's URL,
	//    BLOCK_PROCESSED gets posted to arcade, bump-builder
	//    constructs the compound BUMP, and SetMinedByTxIDs flips
	//    each row to MINED with merklePath set.
	if err := harness.WaitForMined(ctx, t, rt, txids, 90*time.Second); err != nil {
		t.Fatalf("MINED post-reprocess: %v", err)
	}
	t.Logf("all %d txs reached MINED post-reprocess", len(txids))

	// 6. The recovered merklePath must ComputeRoot()s to the block's
	//    real header merkle root — same proof of correctness as the
	//    round-trip test, just delivered via the recovery path.
	harness.AssertMerklePathsMatchHeaderRoot(t, rt, fix.MerkleRoot, txids)
}
