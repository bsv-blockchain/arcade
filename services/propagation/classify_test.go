package propagation

import (
	"strings"
	"testing"
)

// TestClassifyFailureLine pins the conservative Teranode-line → ARC-code map
// (issue #254 / external feedback item 2). Invariants: the verbatim Teranode
// line is always preserved in the message (the caller keeps every per-tx line
// terminally rejected — the requeue signal is a body-less 5xx, decided
// elsewhere); only confidently-mappable code prefixes gain an ARC status
// code; and the non-final family carries an explicit retryable hint.
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
