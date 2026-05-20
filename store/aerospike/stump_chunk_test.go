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
// RECORD_TOO_BIG) must round-trip byte-identical, alongside a small one, and a
// shrinking rewrite must not leave stale chunk data behind.
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

	// Rewrite the large STUMP smaller — orphan chunks from the first write
	// must be cleaned up so the reassembled payload is exactly the new data.
	if err := s.InsertStump(ctx, &models.Stump{BlockHash: blockHash, SubtreeIndex: 0, StumpData: small}); err != nil {
		t.Fatalf("InsertStump rewrite: %v", err)
	}
	got, err = s.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash after rewrite: %v", err)
	}
	for _, st := range got {
		if st.SubtreeIndex == 0 && !bytes.Equal(st.StumpData, small) {
			t.Fatalf("subtree 0 after rewrite: got %d bytes, want %d", len(st.StumpData), len(small))
		}
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
