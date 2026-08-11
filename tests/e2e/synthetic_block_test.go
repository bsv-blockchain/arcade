//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	teranode "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/tests/e2e/harness"
)

// TestSmoke_SyntheticBlockMined proves the synthetic-block path end to
// end: a fully fabricated single-subtree block (teranode composition
// rules: coinbase placeholder at subtree leaf 0, coinbase-corrected
// header root, regtest-target nonce) is accepted by merkle-service,
// drives arcade's watched txs to MINED with verifiable merkle paths,
// and is tracked as the tip by arcade's embedded chaintracks.
//
// This is the go/no-go gate for the reorg scenarios in reorg_test.go,
// which fabricate two competing synthetic blocks at the same height.
// If this test breaks, fix it before reading a reorg failure as real.
func TestSmoke_SyntheticBlockMined(t *testing.T) {
	skipIfNoDocker(t)

	h := harness.New(t)

	msDatahub := h.NewDatahub(t)
	arcadeDatahub := h.NewDatahub(t)

	rt := harness.StartArcade(t, harness.ArcadeOptions{
		MerkleServiceURL:  h.Containers.MerkleHostURL,
		DatahubURL:        arcadeDatahub.LocalURL(),
		LibP2PBootstrap:   h.LibP2P.BootstrapMultiaddr(),
		LibP2PLoopback:    h.LibP2P.LoopbackMultiaddr(),
		MerkleAuthToken:   "e2e-watch-token",
		CallbackToken:     "e2e-callback-token",
		EnableChaintracks: true,
	})

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()

	// 1. Broadcast 7 validatable txs through arcade → propagation
	//    registers them with merkle-service via /watch.
	txs := harness.BuildValidatableTxs(7, 1000)
	txids := make([]string, 0, len(txs))
	for _, tx := range txs {
		id, err := harness.BroadcastTx(ctx, t, rt, tx)
		if err != nil {
			t.Fatalf("broadcast: %v", err)
		}
		txids = append(txids, id)
	}
	for _, id := range txids {
		if err := harness.WaitForMerkleRegistration(ctx, h.Containers.MerkleHostURL, id, 60*time.Second); err != nil {
			t.Fatalf("watch %s: %v", id, err)
		}
	}
	t.Logf("broadcast + registered %d txs", len(txids))

	// 2. Fabricate the block: 7 txs + coinbase = 8 leaves, height 1 on
	//    the regtest genesis.
	blk, err := harness.BuildSyntheticBlock(harness.SyntheticBlockSpec{
		PrevHash:  harness.RegtestGenesisHash(),
		Height:    1,
		Timestamp: uint32(time.Now().Unix()), //nolint:gosec // wall clock fits in uint32 until 2106
		TxIDs:     harness.TxIDsFromHex(t, txids),
	})
	if err != nil {
		t.Fatalf("build synthetic block: %v", err)
	}
	blk.Stage(msDatahub)
	blk.Stage(arcadeDatahub)
	t.Logf("synthetic block %s (subtree %s, root %s)", blk.Hash, blk.SubtreeHash, blk.MerkleRoot)

	// 3. Announce subtree then block; republish the block until arcade's
	//    embedded chaintracks reports it as tip (gossipsub mesh formation
	//    can swallow early publishes; all consumers dedup by hash).
	if err := h.LibP2P.PublishSubtree(ctx, teranode.SubtreeMessage{
		Hash:       blk.SubtreeHash.String(),
		DataHubURL: msDatahub.HostURL(),
	}); err != nil {
		t.Fatalf("publish subtree: %v", err)
	}
	blockMsg := teranode.BlockMessage{
		Hash:       blk.Hash.String(),
		Height:     blk.Height,
		DataHubURL: msDatahub.HostURL(),
		Header:     blk.HeaderHex,
		Coinbase:   blk.CoinbaseHex,
	}
	if err := harness.PublishBlockUntilTip(ctx, rt, h.LibP2P, blockMsg, 90*time.Second); err != nil {
		t.Fatalf("chaintracks never adopted the synthetic block as tip: %v", err)
	}
	t.Logf("chaintracks tip = %s", blk.Hash)

	// 4. All txs MINED against this block, with paths that fold to the
	//    synthetic header's merkle root.
	if err := harness.WaitForMinedInBlock(ctx, t, rt, txids, blk.Hash.String(), 90*time.Second); err != nil {
		t.Fatalf("MINED@%s: %v", blk.Hash, err)
	}
	harness.AssertMerklePathsMatchHeaderRoot(t, rt, blk.MerkleRoot.String(), txids)
	t.Logf("all %d txs MINED with paths matching the synthetic root", len(txids))
}
