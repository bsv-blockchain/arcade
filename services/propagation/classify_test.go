package propagation

import (
	"strings"
	"testing"
)

// TestClassifyFailureLine pins the conservative Teranode-line → ARC-code map
// (issue #254 / external feedback item 2). Invariants: the verbatim Teranode
// line is always preserved in the message; only confidently-mappable code
// prefixes gain an ARC status code; and the non-final family carries an
// explicit retryable hint. Opaque PROCESSING is routed to requeue by the
// broadcast loop (lineIsOpaqueProcessing) and does not reach this helper.
func TestClassifyFailureLine(t *testing.T) {
	cases := []struct {
		name          string
		line          string
		wantCode      int
		wantRetryHint bool
	}{
		{
			name: "processing catch-all stays uncoded",
			line: "PROCESSING (4): [ProcessTransaction][ab12] failed to validate transaction",
		},
		{
			name:     "tx invalid maps to generic 467",
			line:     "TX_INVALID (31): [ProcessTransaction][ab12] tx is invalid because fee too low",
			wantCode: 467,
		},
		{
			name:          "tx lock time maps to non-final 476 with retry hint",
			line:          "TX_LOCK_TIME (35): [ProcessTransaction][ab12] Bad tx lock time",
			wantCode:      476,
			wantRetryHint: true,
		},
		{
			name:          "utxo non-final maps to non-final 476 with retry hint",
			line:          "UTXO_NON_FINAL (71): [ProcessTransaction][ab12] tx is non-final",
			wantCode:      476,
			wantRetryHint: true,
		},
		{
			name:     "tx conflicting maps to conflict 466",
			line:     "TX_CONFLICTING (36): [ProcessTransaction][ab12] tx conflicting",
			wantCode: 466,
		},
		{
			name:     "utxo spent maps to conflict 466",
			line:     "UTXO_SPENT (70): [ProcessTransaction][ab12] utxo already spent by tx cd34",
			wantCode: 466,
		},
		{
			// Exact doubled-prefix shape observed on mainnet 409 responses:
			// the code name appears twice because Teranode wraps the inner
			// error's UserMessage. strings.Cut on the first " (" still
			// recovers the code.
			name:     "doubled UTXO_SPENT prefix (live 409 shape) maps to conflict 466",
			line:     "UTXO_SPENT (70): UTXO_SPENT (70): 256f...ae43:2 utxo already spent by tx 7dfb...8489[0]",
			wantCode: 466,
		},
		{
			name:     "tx invalid double spend maps to conflict 466",
			line:     "TX_INVALID_DOUBLE_SPEND (32): [ProcessTransaction][ab12] tx invalid double spend",
			wantCode: 466,
		},
		{
			name: "unknown code name stays uncoded",
			line: "SOME_FUTURE_CODE (99): [ProcessTransaction][ab12] who knows",
		},
		{
			name: "line without code prefix stays uncoded",
			line: "malformed line with no teranode code",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, code := classifyFailureLine(tc.line)
			if code != tc.wantCode {
				t.Errorf("code = %d, want %d", code, tc.wantCode)
			}
			if !strings.Contains(msg, tc.line) {
				t.Errorf("message %q must preserve the Teranode line verbatim %q", msg, tc.line)
			}
			if got := strings.Contains(msg, "retryable"); got != tc.wantRetryHint {
				t.Errorf("retryable hint present=%v, want %v (msg=%q)", got, tc.wantRetryHint, msg)
			}
		})
	}
}

// TestPreferRejectionLine ensures multi-peer aggregation keeps the best
// wallet-facing reason rather than first-writer-wins.
func TestPreferRejectionLine(t *testing.T) {
	processing := "PROCESSING (4): [ProcessTransaction][ab] failed to validate transaction"
	utxoSpent := "UTXO_SPENT (70): [ProcessTransaction][ab] utxo already spent"
	txInvalid := "TX_INVALID (31): [ProcessTransaction][ab] bad fee"
	opaque := "malformed peer body"
	cases := []struct {
		name      string
		current   string
		candidate string
		want      string
	}{
		{name: "empty current takes candidate", current: "", candidate: processing, want: processing},
		{name: "empty candidate keeps current", current: processing, candidate: "", want: processing},
		{name: "UTXO_SPENT beats PROCESSING", current: processing, candidate: utxoSpent, want: utxoSpent},
		{name: "UTXO_SPENT beats TX_INVALID", current: txInvalid, candidate: utxoSpent, want: utxoSpent},
		{name: "PROCESSING beats opaque", current: opaque, candidate: processing, want: processing},
		{name: "TX_INVALID beats PROCESSING", current: processing, candidate: txInvalid, want: txInvalid},
		{
			// A code this build doesn't know is still a concrete verdict:
			// better than the PROCESSING catch-all, worse than a known code.
			name:      "unknown code beats PROCESSING",
			current:   processing,
			candidate: "SOME_FUTURE_CODE (99): [ProcessTransaction][ab] specific reason",
			want:      "SOME_FUTURE_CODE (99): [ProcessTransaction][ab] specific reason",
		},
		{
			name:      "TX_INVALID beats unknown code",
			current:   "SOME_FUTURE_CODE (99): [ProcessTransaction][ab] specific reason",
			candidate: txInvalid,
			want:      txInvalid,
		},
		{name: "equal score keeps current", current: processing + " a", candidate: processing + " b", want: processing + " a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := preferRejectionLine(tc.current, tc.candidate); got != tc.want {
				t.Errorf("preferRejectionLine(%q, %q) = %q, want %q", tc.current, tc.candidate, got, tc.want)
			}
		})
	}
}

// TestFailureLinesForLog pins the operator-log contract: nil for empty
// input, at most 8 lines, and most-informative-first ordering so the cap
// drops PROCESSING catch-alls before concrete validator verdicts —
// deterministic across map iteration order.
func TestFailureLinesForLog(t *testing.T) {
	if got := failureLinesForLog(nil); got != nil {
		t.Errorf("failureLinesForLog(nil) = %v, want nil", got)
	}
	if got := failureLinesForLog(map[string]string{}); got != nil {
		t.Errorf("failureLinesForLog(empty) = %v, want nil", got)
	}

	failures := make(map[string]string, 10)
	for i := 0; i < 9; i++ {
		key := strings.Repeat(string(rune('a'+i)), 64)
		failures[key] = "PROCESSING (4): [ProcessTransaction][" + key + "] failed to validate transaction"
	}
	spentKey := strings.Repeat("f", 64)
	spentLine := "UTXO_SPENT (70): [ProcessTransaction][" + spentKey + "] utxo already spent"
	failures[spentKey] = spentLine

	got := failureLinesForLog(failures)
	if len(got) != 8 {
		t.Fatalf("len=%d want 8 (cap)", len(got))
	}
	if got[0] != spentLine {
		t.Errorf("got[0]=%q — the UTXO_SPENT line must survive the cap and sort first", got[0])
	}
	// Determinism: same input, same output, regardless of map iteration.
	again := failureLinesForLog(failures)
	for i := range got {
		if got[i] != again[i] {
			t.Fatalf("non-deterministic output at index %d: %q vs %q", i, got[i], again[i])
		}
	}
}

func TestLineIsOpaqueProcessing(t *testing.T) {
	cases := []struct {
		name string
		line string
		want bool
	}{
		{
			name: "bare PROCESSING catch-all",
			line: "PROCESSING (4): [ProcessTransaction][ab12] failed to validate transaction",
			want: true,
		},
		{
			name: "repeated PROCESSING wrapper still opaque",
			line: "PROCESSING (4): PROCESSING (4): failed to validate transaction",
			want: true,
		},
		{
			name: "nested TX_INVALID is a verdict",
			line: "PROCESSING (4): [ProcessTransaction][ab12] failed: TX_INVALID (31): fee too low",
			want: false,
		},
		{
			name: "nested UTXO_SPENT is a verdict",
			line: "PROCESSING (4): UTXO_SPENT (70): abc:0 already spent",
			want: false,
		},
		{
			name: "nested TX_MISSING_PARENT is a condition, not opaque",
			line: "PROCESSING (4): TX_MISSING_PARENT (34): missing parent",
			want: false,
		},
		{
			name: "unknown nested code is still a nested verdict",
			line: "PROCESSING (4): SOME_FUTURE_CODE (99): specific reason",
			want: false,
		},
		{
			name: "bare TX_INVALID is not PROCESSING",
			line: "TX_INVALID (31): fee too low",
			want: false,
		},
		{name: "empty", line: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lineIsOpaqueProcessing(tc.line); got != tc.want {
				t.Errorf("lineIsOpaqueProcessing(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}
