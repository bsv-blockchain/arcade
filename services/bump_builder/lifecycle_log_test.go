package bump_builder

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestMarkMinedAndPublish_LogsTransactionsMinedLevelSplit pins the MINED log
// split: markMinedAndPublish must emit exactly ONE Info "transactions mined"
// line per block — the txid list capped at maxTxIDsPerLine (from package
// logfields) but txid_count carrying the TRUE total, so Info-level volume
// stays flat regardless of block size — plus one Debug line per chunk so a
// block larger than one chunk still surfaces every txid somewhere in the log
// stream. Each Debug line also carries block_hash/block_height, the TRUE
// total txid_count (not the chunk size), and its position in the chunk
// sequence.
func TestMarkMinedAndPublish_LogsTransactionsMinedLevelSplit(t *testing.T) {
	ms := newMockStore()
	b := newTestBuilder(ms, "http://unused.invalid")

	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	// 2500 spans logfields.maxTxIDsPerLine (1000) twice over plus a
	// remainder, so the expected chunking is 1000 + 1000 + 500 = 3 lines.
	const n = 2500
	const wantChunks = 3
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("mined-tx-%05d", i)
	}

	b.markMinedAndPublish(context.Background(), logger, "block-hash-mined-test", 777, txids)

	// Info: exactly one bounded line, TRUE total in txid_count, list capped.
	infoEntries := recorded.FilterMessage("transactions mined").
		FilterLevelExact(zapcore.InfoLevel).All()
	if len(infoEntries) != 1 {
		t.Fatalf("expected exactly 1 Info 'transactions mined' line, got %d", len(infoEntries))
	}
	infoFields := infoEntries[0].ContextMap()
	if got, _ := infoFields["block_hash"].(string); got != "block-hash-mined-test" {
		t.Errorf("Info: block_hash = %q, want %q", got, "block-hash-mined-test")
	}
	if got, _ := infoFields["block_height"].(uint64); got != 777 {
		t.Errorf("Info: block_height = %v, want 777", infoFields["block_height"])
	}
	if got, _ := infoFields["txid_count"].(int64); got != int64(n) {
		t.Errorf("Info: txid_count = %v, want %d (true total, not the capped list length)", infoFields["txid_count"], n)
	}
	infoTxIDs, ok := infoFields["txids"].([]interface{})
	if !ok {
		t.Fatalf("Info: txids field missing or wrong type: %#v", infoFields["txids"])
	}
	if len(infoTxIDs) != 1000 {
		t.Errorf("Info: txids list length = %d, want 1000 (capped at maxTxIDsPerLine)", len(infoTxIDs))
	}

	// Debug: one line per chunk, full coverage across the union.
	debugEntries := recorded.FilterMessage("transactions mined").
		FilterLevelExact(zapcore.DebugLevel).All()
	if len(debugEntries) != wantChunks {
		t.Fatalf("expected %d Debug 'transactions mined' chunk lines, got %d", wantChunks, len(debugEntries))
	}

	seen := make(map[string]bool, n)
	for i, e := range debugEntries {
		fields := e.ContextMap()
		if got, _ := fields["block_hash"].(string); got != "block-hash-mined-test" {
			t.Errorf("entry %d: block_hash = %q, want %q", i, got, "block-hash-mined-test")
		}
		if got, _ := fields["block_height"].(uint64); got != 777 {
			t.Errorf("entry %d: block_height = %v, want 777", i, fields["block_height"])
		}
		if got, _ := fields["txid_count"].(int64); got != int64(n) {
			t.Errorf("entry %d: txid_count = %v, want %d (true total on every chunk line)", i, fields["txid_count"], n)
		}
		if got, _ := fields["chunk_index"].(int64); got != int64(i) {
			t.Errorf("entry %d: chunk_index = %v, want %d", i, fields["chunk_index"], i)
		}
		if got, _ := fields["chunk_total"].(int64); got != int64(wantChunks) {
			t.Errorf("entry %d: chunk_total = %v, want %d", i, fields["chunk_total"], wantChunks)
		}
		chunkTxIDs, ok := fields["txids"].([]interface{})
		if !ok {
			t.Fatalf("entry %d: txids field missing or wrong type: %#v", i, fields["txids"])
		}
		if len(chunkTxIDs) == 0 || len(chunkTxIDs) > 1000 {
			t.Fatalf("entry %d: chunk size %d out of bounds (1, 1000]", i, len(chunkTxIDs))
		}
		for _, v := range chunkTxIDs {
			s, _ := v.(string)
			if seen[s] {
				t.Fatalf("txid %q appeared in more than one chunk", s)
			}
			seen[s] = true
		}
	}
	if len(seen) != n {
		t.Fatalf("union of all chunks covered %d distinct txids, want %d (every txid must appear exactly once across chunks)", len(seen), n)
	}
}

// TestMarkMinedAndPublish_LogsTransactionsMinedSingleChunk pins the common
// case (a block small enough for one chunk) to exactly one Info line and one
// Debug line, so the level split doesn't accidentally fragment or duplicate
// small blocks.
func TestMarkMinedAndPublish_LogsTransactionsMinedSingleChunk(t *testing.T) {
	ms := newMockStore()
	b := newTestBuilder(ms, "http://unused.invalid")

	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	txids := []string{"tx-a", "tx-b", "tx-c"}
	b.markMinedAndPublish(context.Background(), logger, "block-hash-small", 42, txids)

	infoEntries := recorded.FilterMessage("transactions mined").
		FilterLevelExact(zapcore.InfoLevel).All()
	if len(infoEntries) != 1 {
		t.Fatalf("expected exactly 1 Info 'transactions mined' line, got %d", len(infoEntries))
	}
	fields := infoEntries[0].ContextMap()
	if got, _ := fields["txid_count"].(int64); got != 3 {
		t.Errorf("Info: txid_count = %v, want 3", fields["txid_count"])
	}
	if got, ok := fields["txids"].([]interface{}); !ok || len(got) != 3 {
		t.Errorf("Info: txids = %#v, want the full 3-element list (under the cap)", fields["txids"])
	}

	debugEntries := recorded.FilterMessage("transactions mined").
		FilterLevelExact(zapcore.DebugLevel).All()
	if len(debugEntries) != 1 {
		t.Fatalf("expected exactly 1 Debug 'transactions mined' chunk line, got %d", len(debugEntries))
	}
	if got, _ := debugEntries[0].ContextMap()["chunk_total"].(int64); got != 1 {
		t.Errorf("Debug: chunk_total = %v, want 1", debugEntries[0].ContextMap()["chunk_total"])
	}
}
