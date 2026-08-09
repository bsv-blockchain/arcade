//go:build e2e

package e2e_test

import (
	"context"
	"encoding/hex"
	"strings"
	"sync"
	"testing"
	"time"

	sdkchainhash "github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/tests/e2e/harness"
)

// reorgStack is the shared scaffolding for the same-height reorg
// scenarios (issue #279): containers + arcade with embedded chaintracks,
// two datahubs, and three tx groups broadcast + registered:
//
//	shared — mined in BOTH competing blocks (must follow the winner)
//	aOnly  — only in block A (the eventual canonical block)
//	bOnly  — only in block B (the eventual orphan; must end unmined)
//
// Blocks: A and B compete at height 1 on the regtest genesis (7 txs +
// coinbase placeholder = 8 subtree leaves each); C is a coinbase-only
// block on top of A that resolves the competition in A's favor.
type reorgStack struct {
	h             *harness.Harness
	rt            *harness.ArcadeRuntime
	msDatahub     *harness.Datahub
	arcadeDatahub *harness.Datahub

	shared, aOnly, bOnly []string
	blkA, blkB, blkC     *harness.SyntheticBlock
}

func newReorgStack(ctx context.Context, t *testing.T) *reorgStack {
	t.Helper()

	s := &reorgStack{h: harness.New(t)}
	s.msDatahub = s.h.NewDatahub(t)
	s.arcadeDatahub = s.h.NewDatahub(t)

	s.rt = harness.StartArcade(t, harness.ArcadeOptions{
		MerkleServiceURL:  s.h.Containers.MerkleHostURL,
		DatahubURL:        s.arcadeDatahub.LocalURL(),
		LibP2PBootstrap:   s.h.LibP2P.BootstrapMultiaddr(),
		LibP2PLoopback:    s.h.LibP2P.LoopbackMultiaddr(),
		MerkleAuthToken:   "e2e-watch-token",
		CallbackToken:     "e2e-callback-token",
		EnableChaintracks: true,
	})

	// 5 shared + 2 A-only + 2 B-only = 9 broadcasts.
	txs := harness.BuildValidatableTxs(9, 2000)
	ids := make([]string, 0, len(txs))
	for _, tx := range txs {
		id, err := harness.BroadcastTx(ctx, t, s.rt, tx)
		if err != nil {
			t.Fatalf("broadcast: %v", err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		if err := harness.WaitForMerkleRegistration(ctx, s.h.Containers.MerkleHostURL, id, 60*time.Second); err != nil {
			t.Fatalf("watch %s: %v", id, err)
		}
	}
	s.shared, s.aOnly, s.bOnly = ids[:5], ids[5:7], ids[7:9]

	ts := uint32(time.Now().Unix()) //nolint:gosec // wall clock fits in uint32 until 2106
	var err error
	s.blkA, err = harness.BuildSyntheticBlock(harness.SyntheticBlockSpec{
		PrevHash:  harness.RegtestGenesisHash(),
		Height:    1,
		Timestamp: ts,
		CBExtra:   1,
		TxIDs:     harness.TxIDsFromHex(t, append(append([]string{}, s.shared...), s.aOnly...)),
	})
	if err != nil {
		t.Fatalf("build block A: %v", err)
	}
	s.blkB, err = harness.BuildSyntheticBlock(harness.SyntheticBlockSpec{
		PrevHash:  harness.RegtestGenesisHash(),
		Height:    1,
		Timestamp: ts + 1,
		CBExtra:   2,
		TxIDs:     harness.TxIDsFromHex(t, append(append([]string{}, s.shared...), s.bOnly...)),
	})
	if err != nil {
		t.Fatalf("build block B: %v", err)
	}
	s.blkC, err = harness.BuildEmptySyntheticBlock(s.blkA.Hash, 2, ts+2, 3)
	if err != nil {
		t.Fatalf("build block C: %v", err)
	}
	if s.blkA.Hash.IsEqual(&s.blkB.Hash) {
		t.Fatal("competing blocks must differ")
	}

	for _, blk := range []*harness.SyntheticBlock{s.blkA, s.blkB, s.blkC} {
		blk.Stage(s.msDatahub)
		blk.Stage(s.arcadeDatahub)
	}

	for _, blk := range []*harness.SyntheticBlock{s.blkA, s.blkB} {
		if err := s.h.LibP2P.PublishSubtree(ctx, teranode.SubtreeMessage{
			Hash:       blk.SubtreeHash.String(),
			DataHubURL: s.msDatahub.HostURL(),
		}); err != nil {
			t.Fatalf("publish subtree %s: %v", blk.SubtreeHash, err)
		}
	}

	t.Logf("stack: A=%s B=%s C=%s shared=%v aOnly=%v bOnly=%v",
		s.blkA.Hash, s.blkB.Hash, s.blkC.Hash, s.shared, s.aOnly, s.bOnly)
	return s
}

// assertMinedPathsAgainst asserts each txid is MINED against blk with a
// merklePath folding to blk's header root.
func (s *reorgStack) assertMinedPathsAgainst(ctx context.Context, t *testing.T, blk *harness.SyntheticBlock, txids []string, timeout time.Duration) {
	t.Helper()
	if err := harness.WaitForMinedInBlock(ctx, t, s.rt, txids, blk.Hash.String(), timeout); err != nil {
		t.Fatalf("MINED@%s: %v", blk.Hash, err)
	}
	harness.AssertMerklePathsMatchHeaderRoot(t, s.rt, blk.MerkleRoot.String(), txids)
}

// TestReorg_SameHeightTie_OrphanProcessedSecond replicates the
// scale-cluster incident ordering with the roles reversed at the
// chain-selection layer: the canonical block A is announced (and becomes
// tip) FIRST, then the same-height competitor B arrives. go-chaintracks
// files B as an alternate — equal chainwork, no tip change, and
// crucially NO ReorgEvent. On unfixed arcade, B's STUMP/BLOCK_PROCESSED
// pipeline still unconditionally re-anchors the shared txs to B
// (SetMinedByTxIDs is last-writer-wins) and falsely mines the B-only
// txs, with no recovery trigger ever firing — exactly the ~92,700
// orphan-anchored MINED txs of issue #279.
func TestReorg_SameHeightTie_OrphanProcessedSecond(t *testing.T) {
	skipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	s := newReorgStack(ctx, t)

	// 1. A becomes tip and mines its txs.
	if err := harness.PublishBlockUntilTip(ctx, s.rt, s.h.LibP2P, s.blkA.BlockMessage(s.msDatahub.HostURL()), 90*time.Second); err != nil {
		t.Fatalf("A → tip: %v", err)
	}
	s.assertMinedPathsAgainst(ctx, t, s.blkA, append(append([]string{}, s.shared...), s.aOnly...), 90*time.Second)
	t.Logf("phase 1: shared+aOnly MINED@A=%s", s.blkA.Hash)

	// 2. B arrives second: same height, equal chainwork — an alternate
	//    that never becomes tip and never triggers a ReorgEvent. Wait
	//    until chaintracks has the header AND arcade fully processed B's
	//    BLOCK_PROCESSED (its compound BUMP is persisted).
	if err := harness.PublishBlockUntilTracked(ctx, s.rt, s.h.LibP2P, s.blkB.BlockMessage(s.msDatahub.HostURL()), 90*time.Second); err != nil {
		t.Fatalf("B → tracked: %v", err)
	}
	if err := harness.WaitForBUMPStored(ctx, s.rt, s.blkB.Hash.String(), 90*time.Second); err != nil {
		t.Fatalf("B BUMP: %v", err)
	}
	t.Logf("phase 2: B=%s processed by the full pipeline", s.blkB.Hash)

	// 3. THE core regression assertion (fails on unfixed arcade): no tx
	//    may be anchored to B — not the shared ones (they were MINED@A
	//    and must stay), not the B-only ones (B is off the active chain;
	//    mining them against it is a false MINED).
	all := append(append(append([]string{}, s.shared...), s.aOnly...), s.bOnly...)
	harness.AssertAnchorsStable(ctx, t, s.rt, all, s.blkB.Hash.String(), 15*time.Second)
	t.Log("phase 3: no anchors moved to the same-height loser")

	// 4. C extends A: competition resolved, tip advances along A's
	//    branch. Anchors must remain stable.
	if err := harness.PublishBlockUntilTip(ctx, s.rt, s.h.LibP2P, s.blkC.BlockMessage(s.msDatahub.HostURL()), 90*time.Second); err != nil {
		t.Fatalf("C → tip: %v", err)
	}
	harness.AssertAnchorsStable(ctx, t, s.rt, all, s.blkB.Hash.String(), 10*time.Second)
	s.assertMinedPathsAgainst(ctx, t, s.blkA, append(append([]string{}, s.shared...), s.aOnly...), 60*time.Second)

	// 5. B-only txs must not be MINED at all (their only containing
	//    block lost the height).
	for _, id := range s.bOnly {
		st, ok, err := harness.GetTxStatus(ctx, s.rt, id)
		if err != nil || !ok {
			t.Fatalf("status %s: ok=%v err=%v", id, ok, err)
		}
		if st.TxStatus == string(models.StatusMined) || st.TxStatus == string(models.StatusImmutable) {
			t.Errorf("bOnly tx %s reports %s @ %s — its only block lost the same-height competition",
				id, st.TxStatus, st.BlockHash)
		}
	}
}

// TestReorg_SameHeightTie_OrphanWasTip_ReanchorsOnReorgEvent drives the
// other ordering: B is announced first and legitimately becomes tip
// (its MINED anchors are correct at that moment), then A arrives as an
// alternate, then C extends A — chaintracks emits a ReorgEvent orphaning
// B. Fixed arcade must re-anchor the shared txs to A (using A's stored
// compound BUMP), mine the A-only txs it previously (correctly) refused
// to anchor, revert the B-only txs to SEEN_ON_NETWORK, publish corrected
// events, and expose B as an orphaned proof on GET /tx. Unfixed arcade
// leaves the B-only txs MINED@B forever (recordReorg only touches the
// block_processing table).
func TestReorg_SameHeightTie_OrphanWasTip_ReanchorsOnReorgEvent(t *testing.T) {
	skipIfNoDocker(t)
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	s := newReorgStack(ctx, t)

	// Collect status events so the corrected-event contract is asserted
	// too (webhook/SSE consumers must learn about re-anchors).
	events := make([]*models.TransactionStatus, 0, 64)
	var eventsMu sync.Mutex
	evCh, err := s.rt.Deps.Publisher.Subscribe(ctx, "reorg-e2e")
	if err != nil {
		t.Fatalf("subscribe events: %v", err)
	}
	go func() {
		for ev := range evCh {
			eventsMu.Lock()
			events = append(events, ev)
			eventsMu.Unlock()
		}
	}()

	// 1. B first: legitimately tip, its txs legitimately MINED@B.
	if err := harness.PublishBlockUntilTip(ctx, s.rt, s.h.LibP2P, s.blkB.BlockMessage(s.msDatahub.HostURL()), 90*time.Second); err != nil {
		t.Fatalf("B → tip: %v", err)
	}
	s.assertMinedPathsAgainst(ctx, t, s.blkB, append(append([]string{}, s.shared...), s.bOnly...), 90*time.Second)
	t.Logf("phase 1: shared+bOnly legitimately MINED@B=%s", s.blkB.Hash)

	// 2. A second: alternate, never tip. Arcade must still fully process
	//    and persist A's compound BUMP — it is the re-anchor fuel.
	if err := harness.PublishBlockUntilTracked(ctx, s.rt, s.h.LibP2P, s.blkA.BlockMessage(s.msDatahub.HostURL()), 90*time.Second); err != nil {
		t.Fatalf("A → tracked: %v", err)
	}
	if err := harness.WaitForBUMPStored(ctx, s.rt, s.blkA.Hash.String(), 90*time.Second); err != nil {
		t.Fatalf("A's compound BUMP must be persisted even while A is an alternate: %v", err)
	}
	t.Logf("phase 2: A=%s processed, BUMP persisted", s.blkA.Hash)

	// 3. C extends A → A's branch is strictly heavier → chaintracks
	//    reorganizes and emits ReorgEvent{orphaned: [B]}. The C
	//    announcement carries two DataHubURL variants: the
	//    container-reachable one for merkle-service and the host-local
	//    one for chaintracks' backward /headers crawl (C's parent A is
	//    off chaintracks' main chain at this moment, so it must crawl).
	cVariants := []teranode.BlockMessage{
		s.blkC.BlockMessage(s.msDatahub.HostURL()),
		s.blkC.BlockMessage(s.msDatahub.LocalURL()),
	}
	if err := harness.PublishBlockVariantsUntilTip(ctx, s.rt, s.h.LibP2P, cVariants, 120*time.Second); err != nil {
		t.Fatalf("C → tip (reorg): %v", err)
	}
	t.Logf("phase 3: tip=C=%s, B orphaned", s.blkC.Hash)

	// 4. Post-reorg convergence (fails on unfixed arcade):
	//    shared re-anchored to A with valid A-paths…
	s.assertMinedPathsAgainst(ctx, t, s.blkA, s.shared, 120*time.Second)
	//    …aOnly finally mined against A (the write guard correctly
	//    refused them while B was tip)…
	s.assertMinedPathsAgainst(ctx, t, s.blkA, s.aOnly, 120*time.Second)
	//    …and bOnly reverted to SEEN_ON_NETWORK (not in the canonical
	//    block; a MINED@B report would be a false MINED).
	for _, id := range s.bOnly {
		if err := harness.WaitForStatus(ctx, s.rt, id, string(models.StatusSeenOnNetwork), 120*time.Second); err != nil {
			t.Fatalf("bOnly %s must revert to SEEN_ON_NETWORK after B is orphaned: %v", id, err)
		}
	}
	t.Log("phase 4: re-anchor + revert converged")

	// 5. Historical anchor preserved: every re-anchored tx exposes B as
	//    an orphaned proof whose merklePath still folds to B's root.
	for _, id := range s.shared {
		st, ok, err := harness.GetTxStatus(ctx, s.rt, id)
		if err != nil || !ok {
			t.Fatalf("status %s: ok=%v err=%v", id, ok, err)
		}
		proof, found := st.GetOrphanedProof(s.blkB.Hash.String())
		if !found {
			t.Errorf("tx %s: missing orphanedProofs entry for superseded anchor %s", id, s.blkB.Hash)
			continue
		}
		if proof.MerklePath == "" {
			t.Errorf("tx %s: orphaned proof for %s has no merklePath", id, s.blkB.Hash)
			continue
		}
		assertPathFoldsToRoot(t, id, proof.MerklePath, s.blkB.MerkleRoot.String())
	}
	t.Log("phase 5: orphaned proofs preserved and verifiable")

	// 6. Corrected events reached the status stream: a MINED@A event
	//    flagged as a reorg re-anchor covering the shared txs, and a
	//    revert event for the bOnly txs.
	deadline := time.Now().Add(30 * time.Second)
	for {
		eventsMu.Lock()
		gotReanchor := eventCovers(events, s.shared, string(models.StatusMined), s.blkA.Hash.String(), "reorg_reanchor")
		gotRevert := eventCovers(events, s.bOnly, string(models.StatusSeenOnNetwork), "", "reorg_unmined")
		eventsMu.Unlock()
		if gotReanchor && gotRevert {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("corrected status events missing: reanchor=%v revert=%v", gotReanchor, gotRevert)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Log("phase 6: corrected events published")
}

// eventCovers reports whether the collected events include every txid in
// want with the given status, blockHash (when non-empty), and extraInfo.
func eventCovers(events []*models.TransactionStatus, want []string, status, blockHash, extraInfo string) bool {
	covered := make(map[string]bool, len(want))
	for _, ev := range events {
		if string(ev.Status) != status || ev.ExtraInfo != extraInfo {
			continue
		}
		if blockHash != "" && ev.BlockHash != blockHash {
			continue
		}
		if ev.TxID != "" {
			covered[ev.TxID] = true
		}
		for _, id := range ev.TxIDs {
			covered[id] = true
		}
	}
	for _, id := range want {
		if !covered[id] {
			return false
		}
	}
	return true
}

// assertPathFoldsToRoot parses a BRC-74 merklePath and asserts
// ComputeRoot for txid equals wantRoot (display-order hex).
func assertPathFoldsToRoot(t *testing.T, txid, merklePathHex, wantRoot string) {
	t.Helper()
	raw, err := hex.DecodeString(merklePathHex)
	if err != nil {
		t.Errorf("tx %s: decode orphaned merklePath: %v", txid, err)
		return
	}
	mp, err := sdkTx.NewMerklePathFromBinary(raw)
	if err != nil {
		t.Errorf("tx %s: parse orphaned merklePath: %v", txid, err)
		return
	}
	th, err := sdkchainhash.NewHashFromHex(txid)
	if err != nil {
		t.Errorf("tx %s: parse txid: %v", txid, err)
		return
	}
	root, err := mp.ComputeRoot(th)
	if err != nil {
		t.Errorf("tx %s: ComputeRoot: %v", txid, err)
		return
	}
	if !strings.EqualFold(root.String(), wantRoot) {
		t.Errorf("tx %s: orphaned proof folds to %s, want %s", txid, root, wantRoot)
	}
}
