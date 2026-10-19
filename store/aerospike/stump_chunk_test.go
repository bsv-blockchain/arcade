//go:build integration

package aerospike

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
)

// TestInsertGetStump_Chunked exercises the manifest+chunk STUMP layout: a STUMP
// larger than a single Aerospike record (the case that previously failed with
// RECORD_TOO_BIG) must round-trip byte-identical, alongside a small one. Rows
// are content-addressed, so a second, different STUMP for the same subtree is
// a second variant with its own chunks (never a rewrite of the first), an
// identical re-insert is a no-op, and the block delete removes every variant.
func TestInsertGetStump_Chunked(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()

	const blockHash = "stumptest-chunked-block-0001"

	// Spans several chunk records — far past any per-record ceiling.
	big := make([]byte, s.bumpChunkSize*3+1234)
	if _, err := rand.Read(big); err != nil {
		t.Fatalf("rand: %v", err)
	}
	small := []byte("a single small stump payload")

	in := []*models.Stump{
		{BlockHash: blockHash, SubtreeIndex: 0, StumpData: big},
		{BlockHash: blockHash, SubtreeIndex: 1, StumpData: small},
	}
	t.Cleanup(func() { _ = s.DeleteStumpsByBlockHash(ctx, blockHash) })

	for _, st := range in {
		if err := s.InsertStump(ctx, st); err != nil {
			t.Fatalf("InsertStump subtree %d: %v", st.SubtreeIndex, err)
		}
	}

	got, err := s.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash: %v", err)
	}
	if len(got) != len(in) {
		t.Fatalf("got %d stumps, want %d", len(got), len(in))
	}
	bySubtree := map[int][]byte{}
	for _, st := range got {
		bySubtree[st.SubtreeIndex] = st.StumpData
	}
	for _, st := range in {
		if !bytes.Equal(bySubtree[st.SubtreeIndex], st.StumpData) {
			t.Fatalf("subtree %d: round-trip mismatch (got %d bytes, want %d)",
				st.SubtreeIndex, len(bySubtree[st.SubtreeIndex]), len(st.StumpData))
		}
	}

	// An identical re-insert of the large STUMP is a no-op; a different
	// STUMP for subtree 0 is a second variant. Both must read back
	// byte-identical: the big one's chunks are not disturbed by the small
	// variant's write.
	if err := s.InsertStump(ctx, &models.Stump{BlockHash: blockHash, SubtreeIndex: 0, StumpData: big}); err != nil {
		t.Fatalf("InsertStump identical re-insert: %v", err)
	}
	if err := s.InsertStump(ctx, &models.Stump{BlockHash: blockHash, SubtreeIndex: 0, StumpData: small}); err != nil {
		t.Fatalf("InsertStump variant: %v", err)
	}
	got, err = s.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash after variant insert: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("after variant insert got %d stumps, want 3 (big@0, small@0, small@1)", len(got))
	}
	variants0 := map[string]bool{}
	for _, st := range got {
		if st.ContentHash != models.StumpContentHash(st.StumpData) {
			t.Fatalf("subtree %d: content hash does not match the bytes", st.SubtreeIndex)
		}
		if st.SubtreeIndex == 0 {
			variants0[st.ContentHash] = bytes.Equal(st.StumpData, big) || bytes.Equal(st.StumpData, small)
		}
	}
	if len(variants0) != 2 || !variants0[models.StumpContentHash(big)] || !variants0[models.StumpContentHash(small)] {
		t.Fatalf("subtree 0 must hold both variants byte-identical, got %v", variants0)
	}

	if err := s.DeleteStumpsByBlockHash(ctx, blockHash); err != nil {
		t.Fatalf("DeleteStumpsByBlockHash: %v", err)
	}
	got, err = s.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash after delete: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("after delete got %d stumps, want 0", len(got))
	}
}
