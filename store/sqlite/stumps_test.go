package sqlite

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
)

func TestStore_StumpOperations(t *testing.T) {
	dbPath, cleanup := setupTestDB(t)
	defer cleanup()

	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer func() {
		_ = s.Close()
	}()

	ctx := t.Context()

	// Insert a STUMP
	stump := &models.Stump{
		BlockHash:    "block1",
		SubtreeIndex: 2,
		StumpData:    []byte{0x01, 0x02, 0x03},
	}

	if err := s.InsertStump(ctx, stump); err != nil {
		t.Fatalf("InsertStump failed: %v", err)
	}

	// Retrieve STUMPs by block hash
	stumps, err := s.GetStumpsByBlockHash(ctx, "block1")
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash failed: %v", err)
	}
	if len(stumps) != 1 {
		t.Fatalf("expected 1 stump, got %d", len(stumps))
	}
	if stumps[0].SubtreeIndex != 2 {
		t.Errorf("expected subtree index 2, got %d", stumps[0].SubtreeIndex)
	}
	if !bytes.Equal(stumps[0].StumpData, []byte{0x01, 0x02, 0x03}) {
		t.Errorf("stump data mismatch")
	}

	// Insert another STUMP for same block, different subtree
	stump2 := &models.Stump{
		BlockHash:    "block1",
		SubtreeIndex: 3,
		StumpData:    []byte{0x04, 0x05},
	}
	if err := s.InsertStump(ctx, stump2); err != nil {
		t.Fatalf("InsertStump failed: %v", err)
	}

	stumps, err = s.GetStumpsByBlockHash(ctx, "block1")
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash failed: %v", err)
	}
	if len(stumps) != 2 {
		t.Fatalf("expected 2 stumps, got %d", len(stumps))
	}

	// Upsert existing STUMP (same block_hash + subtree_index)
	stumpUpdated := &models.Stump{
		BlockHash:    "block1",
		SubtreeIndex: 2,
		StumpData:    []byte{0x0a, 0x0b},
	}
	if err := s.InsertStump(ctx, stumpUpdated); err != nil {
		t.Fatalf("InsertStump (upsert) failed: %v", err)
	}

	stumps, err = s.GetStumpsByBlockHash(ctx, "block1")
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash failed: %v", err)
	}
	if len(stumps) != 2 {
		t.Fatalf("expected 2 stumps after upsert, got %d", len(stumps))
	}

	// Verify upserted data
	for _, st := range stumps {
		if st.SubtreeIndex == 2 {
			if !bytes.Equal(st.StumpData, []byte{0x0a, 0x0b}) {
				t.Errorf("expected updated stump data")
			}
		}
	}

	// Empty block returns no STUMPs
	stumps, err = s.GetStumpsByBlockHash(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash failed: %v", err)
	}
	if len(stumps) != 0 {
		t.Fatalf("expected 0 stumps for nonexistent block, got %d", len(stumps))
	}

	// Delete STUMPs by block hash
	if err := s.DeleteStumpsByBlockHash(ctx, "block1"); err != nil {
		t.Fatalf("DeleteStumpsByBlockHash failed: %v", err)
	}

	stumps, err = s.GetStumpsByBlockHash(ctx, "block1")
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash failed: %v", err)
	}
	if len(stumps) != 0 {
		t.Fatalf("expected 0 stumps after delete, got %d", len(stumps))
	}

	// Delete nonexistent block hash (should not error)
	if err := s.DeleteStumpsByBlockHash(ctx, "nonexistent"); err != nil {
		t.Fatalf("DeleteStumpsByBlockHash for nonexistent block failed: %v", err)
	}
}
