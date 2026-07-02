package bump_builder

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestMarkMinedAndPublish_LogsTransactionsMinedChunked closes the MINED log
// gap: markMinedAndPublish must emit an Info "transactions mined" line per
// maxTxIDsPerLine-sized chunk (from package logfields) so a block larger
// than one chunk still surfaces every txid somewhere in the log stream —
// each line also carries block_hash/block_height and the TRUE total
// txid_count (not the chunk size), plus its position in the chunk sequence.
func TestMarkMinedAndPublish_LogsTransactionsMinedChunked(t *testing.T) {
	ms := newMockStore()
	b := newTestBuilder(ms, "http://unused.invalid")

	core, recorded := observer.New(zapcore.InfoLevel)
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

	entries := recorded.FilterMessage("transactions mined").All()
	if len(entries) != wantChunks {
		t.Fatalf("expected %d 'transactions mined' log lines, got %d", wantChunks, len(entries))
	}

	seen := make(map[string]bool, n)
	for i, e := range entries {
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
// case (a block small enough for one chunk) to exactly one log line, so the
// chunked path doesn't accidentally fragment small blocks.
func TestMarkMinedAndPublish_LogsTransactionsMinedSingleChunk(t *testing.T) {
	ms := newMockStore()
	b := newTestBuilder(ms, "http://unused.invalid")

	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	txids := []string{"tx-a", "tx-b", "tx-c"}
	b.markMinedAndPublish(context.Background(), logger, "block-hash-small", 42, txids)

	entries := recorded.FilterMessage("transactions mined").All()
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 'transactions mined' log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got, _ := fields["txid_count"].(int64); got != 3 {
		t.Errorf("txid_count = %v, want 3", fields["txid_count"])
	}
	if got, _ := fields["chunk_total"].(int64); got != 1 {
		t.Errorf("chunk_total = %v, want 1", fields["chunk_total"])
	}
}
