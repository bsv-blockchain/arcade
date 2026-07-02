package logfields

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// wantField asserts f has the expected key and zapcore type, failing t
// with a descriptive message otherwise.
func wantField(t *testing.T, f zap.Field, wantKey string, wantType zapcore.FieldType) {
	t.Helper()
	if f.Key != wantKey {
		t.Errorf("field key = %q, want %q", f.Key, wantKey)
	}
	if f.Type != wantType {
		t.Errorf("field %q type = %v, want %v", wantKey, f.Type, wantType)
	}
}

func TestConstructors(t *testing.T) {
	if f := TxID("abc"); true {
		wantField(t, f, "txid", zapcore.StringType)
		if f.String != "abc" {
			t.Errorf("TxID value = %q, want %q", f.String, "abc")
		}
	}
	if f := TxIDs([]string{"a", "b"}); true {
		wantField(t, f, "txids", zapcore.ArrayMarshalerType)
	}
	if f := TxIDCount(3); true {
		wantField(t, f, "txid_count", zapcore.Int64Type)
		if f.Integer != 3 {
			t.Errorf("TxIDCount value = %d, want 3", f.Integer)
		}
	}
	if f := BlockHash("h"); true {
		wantField(t, f, "block_hash", zapcore.StringType)
	}
	if f := BlockHeight(42); true {
		wantField(t, f, "block_height", zapcore.Uint64Type)
		if f.Integer != 42 {
			t.Errorf("BlockHeight value = %d, want 42", f.Integer)
		}
	}
	if f := SubtreeHash("s"); true {
		wantField(t, f, "subtree_hash", zapcore.StringType)
	}
	if f := CallbackURL("u"); true {
		wantField(t, f, "callback_url", zapcore.StringType)
	}
	if f := Status("MINED"); true {
		wantField(t, f, "status", zapcore.StringType)
	}
	if f := Stage("network"); true {
		wantField(t, f, "stage", zapcore.StringType)
	}
}

// idsOfLen returns a slice of n distinct placeholder txids.
func idsOfLen(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = string(rune('a' + i%26))
	}
	return out
}

func TestTxIDBatch_CapsListButReportsTrueCount(t *testing.T) {
	tests := []struct {
		name      string
		n         int
		wantListN int
	}{
		{"empty", 0, 0},
		{"under cap", 5, 5},
		{"exactly cap", maxTxIDsPerLine, maxTxIDsPerLine},
		{"over cap", maxTxIDsPerLine + 250, maxTxIDsPerLine},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := idsOfLen(tt.n)
			fields := TxIDBatch(ids)
			if len(fields) != 2 {
				t.Fatalf("TxIDBatch returned %d fields, want 2", len(fields))
			}
			countField, listField := fields[0], fields[1]
			wantField(t, countField, "txid_count", zapcore.Int64Type)
			if int(countField.Integer) != tt.n {
				t.Errorf("txid_count = %d, want true total %d", countField.Integer, tt.n)
			}
			wantField(t, listField, "txids", zapcore.ArrayMarshalerType)
			enc := zapcore.NewMapObjectEncoder()
			listField.AddTo(enc)
			gotList, _ := enc.Fields["txids"].([]interface{})
			if len(gotList) != tt.wantListN {
				t.Errorf("txids list length = %d, want %d (capped)", len(gotList), tt.wantListN)
			}
		})
	}
}

func TestForEachTxIDChunk(t *testing.T) {
	tests := []struct {
		name       string
		n          int
		wantChunks []int // length of each chunk, in order
	}{
		{"empty", 0, nil},
		{"single", 1, []int{1}},
		{"exact multiple", maxTxIDsPerLine * 3, []int{maxTxIDsPerLine, maxTxIDsPerLine, maxTxIDsPerLine}},
		{"remainder", maxTxIDsPerLine*2 + 7, []int{maxTxIDsPerLine, maxTxIDsPerLine, 7}},
		{"under one chunk", maxTxIDsPerLine - 1, []int{maxTxIDsPerLine - 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := idsOfLen(tt.n)
			var gotLens []int
			var lastTotal int
			invocations := 0
			ForEachTxIDChunk(ids, func(chunk []string, chunkIdx, totalChunks int) {
				if chunkIdx != invocations {
					t.Errorf("chunkIdx = %d, want sequential %d", chunkIdx, invocations)
				}
				gotLens = append(gotLens, len(chunk))
				lastTotal = totalChunks
				invocations++
			})
			if len(gotLens) != len(tt.wantChunks) {
				t.Fatalf("got %d chunks %v, want %d chunks %v", len(gotLens), gotLens, len(tt.wantChunks), tt.wantChunks)
			}
			for i, want := range tt.wantChunks {
				if gotLens[i] != want {
					t.Errorf("chunk %d length = %d, want %d", i, gotLens[i], want)
				}
			}
			if invocations > 0 && lastTotal != invocations {
				t.Errorf("totalChunks reported = %d, want %d", lastTotal, invocations)
			}
			// Every id must appear exactly once across all chunks, in order.
			var reassembled []string
			ForEachTxIDChunk(ids, func(chunk []string, _, _ int) {
				reassembled = append(reassembled, chunk...)
			})
			if len(reassembled) != len(ids) {
				t.Fatalf("reassembled %d ids, want %d", len(reassembled), len(ids))
			}
			for i := range ids {
				if reassembled[i] != ids[i] {
					t.Fatalf("reassembled[%d] = %q, want %q", i, reassembled[i], ids[i])
				}
			}
		})
	}
}
