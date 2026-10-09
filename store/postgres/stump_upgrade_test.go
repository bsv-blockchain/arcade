//go:build postgres

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// A store created before STUMP rows were content-addressed has a
// (block_hash, subtree_index) primary key and no content_hash column. The
// schema's idempotent upgrade must add the column, swap the primary key, keep
// the legacy rows readable, and from then on accept two variants for the
// same subtree.
func TestSchema_UpgradesStumpsToContentAddressedKey(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const legacy = `
DROP TABLE stumps;
CREATE TABLE stumps (
    block_hash    TEXT NOT NULL,
    subtree_index INT NOT NULL,
    stump_data    BYTEA NOT NULL,
    PRIMARY KEY (block_hash, subtree_index)
);
INSERT INTO stumps (block_hash, subtree_index, stump_data) VALUES ('legacyblock', 4, '\x0102');
DELETE FROM schema_info;`
	if _, err := s.pool.Exec(ctx, legacy); err != nil {
		t.Fatalf("seed legacy shape: %v", err)
	}
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes on legacy shape: %v", err)
	}

	var pkCols int
	if err := s.pool.QueryRow(ctx,
		`SELECT array_length(conkey, 1) FROM pg_constraint WHERE conrelid = 'stumps'::regclass AND contype = 'p'`,
	).Scan(&pkCols); err != nil {
		t.Fatalf("read primary key: %v", err)
	}
	if pkCols != 3 {
		t.Fatalf("stumps primary key has %d columns after upgrade, want 3", pkCols)
	}

	// The legacy row survives with an empty content_hash and reads back with
	// the hash computed from its bytes.
	got, err := s.GetStumpsByBlockHash(ctx, "legacyblock")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SubtreeIndex != 4 || got[0].ContentHash != models.StumpContentHash([]byte{1, 2}) {
		t.Fatalf("legacy row after upgrade = %+v", got)
	}

	// Re-running the apply is a no-op (the DO block sees the 3-column key).
	if _, err := s.pool.Exec(ctx, `DELETE FROM schema_info`); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("second EnsureIndexes: %v", err)
	}

	// Two variants for the same subtree now coexist.
	for _, data := range [][]byte{[]byte("variant-a"), []byte("variant-b"), []byte("variant-a")} {
		if err := s.InsertStump(ctx, models.NewStump("legacyblock", 4, data)); err != nil {
			t.Fatalf("insert %q: %v", data, err)
		}
	}
	got, err = s.GetStumpsByBlockHash(ctx, "legacyblock")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3 (legacy + two variants): %+v", len(got), got)
	}
}
