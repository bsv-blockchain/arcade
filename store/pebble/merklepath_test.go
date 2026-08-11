package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/internal/synthblock"
	"github.com/bsv-blockchain/arcade/models"
)

const mpTestBlockHash = "00000000000000000000000000000000000000000000000000000000000000ff"

// TestEnrichMerklePath_VerifiesAndCaches exercises the push-path enrichment
// primitive: every mined tx gets a merkle proof that recomputes the block root,
// repeated enrichment for one block is served from the parsed-BUMP cache, and
// InsertBUMP invalidates that cache. Guards also confirm the best-effort no-ops.
func TestEnrichMerklePath_VerifiesAndCaches(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	blk, err := synthblock.Build(8, 900001)
	if err != nil {
		t.Fatalf("synthblock.Build: %v", err)
	}
	for _, txid := range blk.Txids {
		if _, _, err := s.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("seed %s: %v", txid, err)
		}
	}
	if _, _, err := s.SetMinedByTxIDs(ctx, mpTestBlockHash, 900001, blk.Txids); err != nil {
		t.Fatalf("SetMinedByTxIDs: %v", err)
	}
	if err := s.InsertBUMP(ctx, mpTestBlockHash, 900001, blk.BumpBytes); err != nil {
		t.Fatalf("InsertBUMP: %v", err)
	}

	// Every mined tx enriches to a verifying proof.
	for _, txid := range blk.Txids {
		st := &models.TransactionStatus{TxID: txid, Status: models.StatusMined, BlockHash: mpTestBlockHash}
		s.EnrichMerklePath(ctx, st)
		if len(st.MerklePath) == 0 {
			t.Fatalf("no merklePath for %s", txid)
		}
		if err := synthblock.VerifyMerklePath(st.MerklePath, txid, blk.Root); err != nil {
			t.Fatalf("enriched merklePath does not verify for %s: %v", txid, err)
		}
	}

	// The block's parsed compound is cached after enrichment...
	if !s.bumpCache.Contains(mpTestBlockHash) {
		t.Fatal("expected block to be cached after enrichment")
	}
	// ...and re-inserting the BUMP invalidates it.
	if err := s.InsertBUMP(ctx, mpTestBlockHash, 900001, blk.BumpBytes); err != nil {
		t.Fatalf("re-InsertBUMP: %v", err)
	}
	if s.bumpCache.Contains(mpTestBlockHash) {
		t.Fatal("InsertBUMP must invalidate the cached compound")
	}

	// Guard: non-mined status is a no-op.
	seen := &models.TransactionStatus{TxID: blk.Txids[0], Status: models.StatusSeenOnNetwork, BlockHash: mpTestBlockHash}
	s.EnrichMerklePath(ctx, seen)
	if len(seen.MerklePath) != 0 {
		t.Fatal("non-mined status must not be enriched")
	}

	// Guard: unknown block is a best-effort no-op (no panic, no path).
	missing := &models.TransactionStatus{TxID: blk.Txids[0], Status: models.StatusMined, BlockHash: "00000000000000000000000000000000000000000000000000000000deadbeef"}
	s.EnrichMerklePath(ctx, missing)
	if len(missing.MerklePath) != 0 {
		t.Fatal("unknown block must yield no merklePath")
	}
}
