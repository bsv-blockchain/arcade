package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
)

// StumpBackend is the slice of store.Store the STUMP suite drives.
type StumpBackend interface {
	InsertStump(ctx context.Context, stump *models.Stump) error
	GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error)
	DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error
}

// RunStumpSuite asserts the content-addressed STUMP contract every backend
// shares: rows key on (block_hash, subtree_index, content_hash), so the same
// bytes inserted twice (two merkle-services delivering an identical STUMP)
// are one row, a different STUMP for the same subtree (a merkle-service that
// missed registrations during an outage) is a second row, reads come back
// ordered by subtree index with ContentHash set, and the block delete removes
// every row. newBackend is called once per subtest.
//
// Block hashes are derived from the subtest name: Aerospike runs this suite
// in a namespace shared with every other integration test and never clears
// it, so each subtest must only ever read back rows it wrote itself.
func RunStumpSuite(t *testing.T, newBackend func(t *testing.T) StumpBackend) {
	t.Helper()
	ctx := context.Background()
	variantA := []byte("stump-subtree-3-variant-a")
	variantB := []byte("stump-subtree-3-variant-b")
	subtree0 := []byte("stump-subtree-0")

	t.Run("identical bytes collapse to one row", func(t *testing.T) {
		b := newBackend(t)
		block := suiteBlockHash(t, "block")
		for i := 0; i < 3; i++ {
			if err := b.InsertStump(ctx, models.NewStump(block, 3, variantA)); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
		// A caller that left ContentHash empty still lands on the same row.
		if err := b.InsertStump(ctx, &models.Stump{BlockHash: block, SubtreeIndex: 3, StumpData: variantA}); err != nil {
			t.Fatalf("insert without hash: %v", err)
		}
		got, err := b.GetStumpsByBlockHash(ctx, block)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("rows = %d, want 1: %s", len(got), describe(got))
		}
		if got[0].SubtreeIndex != 3 || !bytes.Equal(got[0].StumpData, variantA) || got[0].ContentHash != models.StumpContentHash(variantA) {
			t.Fatalf("row = %s", describe(got))
		}
	})

	t.Run("divergent variants coexist", func(t *testing.T) {
		b := newBackend(t)
		block, other := suiteBlockHash(t, "block"), suiteBlockHash(t, "other")
		for _, st := range []*models.Stump{
			models.NewStump(block, 3, variantA),
			models.NewStump(block, 3, variantB),
			models.NewStump(block, 0, subtree0),
			models.NewStump(block, 3, variantA), // redelivery
			models.NewStump(other, 3, variantA), // another block, same bytes
		} {
			if err := b.InsertStump(ctx, st); err != nil {
				t.Fatalf("insert %d/%s: %v", st.SubtreeIndex, st.ContentHash[:8], err)
			}
		}
		got, err := b.GetStumpsByBlockHash(ctx, block)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 {
			t.Fatalf("rows = %d, want 3: %s", len(got), describe(got))
		}
		if got[0].SubtreeIndex != 0 || got[1].SubtreeIndex != 3 || got[2].SubtreeIndex != 3 {
			t.Fatalf("rows must be ordered by subtree index: %s", describe(got))
		}
		seen := map[string]bool{}
		for _, st := range got {
			if st.BlockHash != block {
				t.Fatalf("row from another block leaked: %s", describe(got))
			}
			if st.ContentHash != models.StumpContentHash(st.StumpData) {
				t.Fatalf("ContentHash must match the bytes: %s", describe(got))
			}
			seen[st.ContentHash] = true
		}
		for _, want := range [][]byte{variantA, variantB, subtree0} {
			if !seen[models.StumpContentHash(want)] {
				t.Fatalf("variant %q missing: %s", want, describe(got))
			}
		}
	})

	t.Run("delete removes every variant and nothing else", func(t *testing.T) {
		b := newBackend(t)
		block, other := suiteBlockHash(t, "block"), suiteBlockHash(t, "other")
		for _, st := range []*models.Stump{
			models.NewStump(block, 3, variantA),
			models.NewStump(block, 3, variantB),
			models.NewStump(other, 1, subtree0),
		} {
			if err := b.InsertStump(ctx, st); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.DeleteStumpsByBlockHash(ctx, block); err != nil {
			t.Fatal(err)
		}
		got, err := b.GetStumpsByBlockHash(ctx, block)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("rows after delete = %s", describe(got))
		}
		kept, err := b.GetStumpsByBlockHash(ctx, other)
		if err != nil {
			t.Fatal(err)
		}
		if len(kept) != 1 {
			t.Fatalf("other block's rows must survive, got %s", describe(kept))
		}
		// Deleting again is a no-op.
		if err := b.DeleteStumpsByBlockHash(ctx, block); err != nil {
			t.Fatalf("second delete: %v", err)
		}
	})

	t.Run("unknown block reads empty", func(t *testing.T) {
		b := newBackend(t)
		got, err := b.GetStumpsByBlockHash(ctx, suiteBlockHash(t, "never-written"))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("rows = %s", describe(got))
		}
	})
}

// suiteBlockHash derives a 64-hex block hash unique to the calling subtest.
func suiteBlockHash(t *testing.T, tag string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(t.Name() + "/" + tag))
	return hex.EncodeToString(sum[:])
}

func describe(stumps []*models.Stump) string {
	var buf bytes.Buffer
	for _, st := range stumps {
		hash := st.ContentHash
		if len(hash) > 8 {
			hash = hash[:8]
		}
		fmt.Fprintf(&buf, "[%s idx=%d hash=%s %q]", st.BlockHash[:8], st.SubtreeIndex, hash, st.StumpData)
	}
	return buf.String()
}
