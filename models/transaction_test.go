package models

import (
	"fmt"
	"testing"
	"time"
)

// TestStatus_IsTerminal pins the set of statuses that the system treats as
// terminal — i.e. statuses that callers must not silently overwrite with a
// later lower-priority update.
func TestStatus_IsTerminal(t *testing.T) {
	cases := []struct {
		s    Status
		want bool
	}{
		{StatusUnknown, false},
		{StatusReceived, false},
		{StatusSentToNetwork, false},
		{StatusAcceptedByNetwork, false},
		{StatusSeenOnNetwork, false},
		{StatusSeenMultipleNodes, false},
		{StatusPendingRetry, false},
		{StatusStumpProcessing, false},
		{StatusRejected, true},
		{StatusDoubleSpendAttempted, true},
		{StatusMined, true},
		{StatusImmutable, true},
	}
	for _, c := range cases {
		if got := c.s.IsTerminal(); got != c.want {
			t.Errorf("IsTerminal(%s) = %v, want %v", c.s, got, c.want)
		}
	}
}

// TestStatus_CanTransitionFrom_TerminalStaysTerminal is the regression for
// F-003: once a tx is MINED/IMMUTABLE/DOUBLE_SPEND_ATTEMPTED, no in-flight
// status update may regress it. REJECTED is covered separately because it
// allows forward recovery to ACCEPTED_BY_NETWORK / SEEN_ON_NETWORK /
// SEEN_MULTIPLE_NODES.
func TestStatus_CanTransitionFrom_TerminalStaysTerminal(t *testing.T) {
	terminals := []Status{
		StatusDoubleSpendAttempted,
		StatusMined,
		StatusImmutable,
	}
	regressions := []Status{
		StatusUnknown,
		StatusReceived,
		StatusSentToNetwork,
		StatusAcceptedByNetwork,
		StatusSeenOnNetwork,
		StatusSeenMultipleNodes,
		StatusPendingRetry,
		StatusStumpProcessing,
	}
	for _, prev := range terminals {
		for _, next := range regressions {
			if next.CanTransitionFrom(prev) {
				t.Errorf("regression allowed: %s → %s should be rejected", prev, next)
			}
		}
	}
}

// TestStatus_CanTransitionFrom_RejectedRecovery pins the partial-terminal
// behavior of REJECTED: forward acceptance/seen states must be reachable so
// late callbacks from a peer that did accept can correct the status, but
// regressions to pre-broadcast or retry states must still be blocked.
func TestStatus_CanTransitionFrom_RejectedRecovery(t *testing.T) {
	allowedForward := []Status{
		StatusAcceptedByNetwork,
		StatusSeenOnNetwork,
		StatusSeenMultipleNodes,
		StatusMined,
		StatusImmutable,
	}
	for _, next := range allowedForward {
		if !next.CanTransitionFrom(StatusRejected) {
			t.Errorf("forward recovery wrongly blocked: REJECTED → %s", next)
		}
	}
	blocked := []Status{
		StatusUnknown,
		StatusReceived,
		StatusSentToNetwork,
		StatusPendingRetry,
		StatusStumpProcessing,
	}
	for _, next := range blocked {
		if next.CanTransitionFrom(StatusRejected) {
			t.Errorf("regression allowed: REJECTED → %s should be rejected", next)
		}
	}
}

// TestStatus_CanTransitionFrom_Immutable verifies IMMUTABLE is a true sink:
// every other status (including MINED and the other terminals) must fail to
// overwrite it.
func TestStatus_CanTransitionFrom_Immutable(t *testing.T) {
	all := []Status{
		StatusUnknown, StatusReceived, StatusSentToNetwork,
		StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusPendingRetry, StatusStumpProcessing,
		StatusRejected, StatusDoubleSpendAttempted, StatusMined,
	}
	for _, next := range all {
		if next.CanTransitionFrom(StatusImmutable) {
			t.Errorf("IMMUTABLE → %s must be rejected", next)
		}
	}
	// IMMUTABLE → IMMUTABLE is an idempotent no-op and must be allowed.
	if !StatusImmutable.CanTransitionFrom(StatusImmutable) {
		t.Errorf("IMMUTABLE → IMMUTABLE must be allowed (idempotent)")
	}
}

// TestStatus_CanTransitionFrom_HappyPath spot-checks the forward edges that
// the propagation/api_server/tracker code paths actually rely on.
func TestStatus_CanTransitionFrom_HappyPath(t *testing.T) {
	allowed := []struct {
		prev, next Status
	}{
		{"", StatusReceived},                              // initial insert
		{StatusReceived, StatusSentToNetwork},             // propagation broadcast
		{StatusSentToNetwork, StatusAcceptedByNetwork},    // datahub ack
		{StatusAcceptedByNetwork, StatusSeenOnNetwork},    // p2p inv
		{StatusSeenOnNetwork, StatusSeenMultipleNodes},    // 2nd peer
		{StatusSeenMultipleNodes, StatusMined},            // mined notification
		{StatusMined, StatusImmutable},                    // confirmation depth
		{StatusSentToNetwork, StatusRejected},             // datahub reject
		{StatusSeenOnNetwork, StatusDoubleSpendAttempted}, // double-spend detected
		{StatusSentToNetwork, StatusPendingRetry},         // retryable failure
		{StatusPendingRetry, StatusSentToNetwork},         // reaper retry
		{StatusReceived, StatusReceived},                  // idempotent dup
		{StatusMined, StatusMined},                        // idempotent dup
		{StatusRejected, StatusAcceptedByNetwork},         // peer accepted after another rejected
		{StatusRejected, StatusSeenOnNetwork},             // SEEN callback after REJECTED
		{StatusRejected, StatusSeenMultipleNodes},         // SEEN_MULTIPLE callback after REJECTED
	}
	for _, c := range allowed {
		if !c.next.CanTransitionFrom(c.prev) {
			t.Errorf("forward transition wrongly rejected: %s → %s", c.prev, c.next)
		}
	}
}

// TestStatus_CanTransitionFrom_Regressions covers the specific scenario
// flagged by F-003 plus a few sibling cases.
func TestStatus_CanTransitionFrom_Regressions(t *testing.T) {
	regressions := []struct {
		prev, next Status
		reason     string
	}{
		{StatusMined, StatusSeenOnNetwork, "F-003: late SEEN callback after MINED"},
		{StatusMined, StatusSeenMultipleNodes, "F-003: late SEEN_MULTIPLE callback after MINED"},
		{StatusMined, StatusPendingRetry, "delayed retry attempt after MINED"},
		{StatusMined, StatusRejected, "late rejection after MINED"},
		{StatusImmutable, StatusMined, "MINED must not pull tx out of IMMUTABLE"},
		{StatusImmutable, StatusSeenOnNetwork, "late SEEN after IMMUTABLE"},
		{StatusRejected, StatusSentToNetwork, "republish after REJECTED"},
		{StatusDoubleSpendAttempted, StatusSeenOnNetwork, "late SEEN after DOUBLE_SPEND_ATTEMPTED"},
		{StatusSeenMultipleNodes, StatusSeenOnNetwork, "single-peer downgrade"},
		{StatusAcceptedByNetwork, StatusSentToNetwork, "regress to pre-ack state"},
	}
	for _, c := range regressions {
		if c.next.CanTransitionFrom(c.prev) {
			t.Errorf("%s: %s → %s should be rejected", c.reason, c.prev, c.next)
		}
	}
}

// TestAppendOrphanedAnchor pins the orphaned-anchor history helper (issue
// #279): empty block hashes are no-ops, consecutive duplicates collapse so
// idempotent replays cannot grow history, and the history caps at
// MaxOrphanedAnchors keeping the newest entries.
func TestAppendOrphanedAnchor(t *testing.T) {
	now := time.Now()

	t.Run("EmptyHashNoOp", func(t *testing.T) {
		history := []OrphanedAnchor{{BlockHash: "blk-A", BlockHeight: 1, OrphanedAt: now}}
		got := AppendOrphanedAnchor(history, OrphanedAnchor{BlockHash: "", BlockHeight: 2, OrphanedAt: now})
		if len(got) != 1 || got[0].BlockHash != "blk-A" {
			t.Fatalf("empty hash must be a no-op, got %+v", got)
		}
		// Also from a nil history.
		if got := AppendOrphanedAnchor(nil, OrphanedAnchor{}); got != nil {
			t.Fatalf("empty hash on nil history must stay nil, got %+v", got)
		}
	})

	t.Run("ConsecutiveDupCollapses", func(t *testing.T) {
		var history []OrphanedAnchor
		history = AppendOrphanedAnchor(history, OrphanedAnchor{BlockHash: "blk-A", BlockHeight: 1, OrphanedAt: now})
		history = AppendOrphanedAnchor(history, OrphanedAnchor{BlockHash: "blk-A", BlockHeight: 1, OrphanedAt: now.Add(time.Minute)})
		if len(history) != 1 {
			t.Fatalf("consecutive duplicate must collapse, got %+v", history)
		}
		// A different hash appends; the SAME hash non-consecutively appends
		// too (only the tail entry is compared).
		history = AppendOrphanedAnchor(history, OrphanedAnchor{BlockHash: "blk-B", BlockHeight: 1, OrphanedAt: now})
		history = AppendOrphanedAnchor(history, OrphanedAnchor{BlockHash: "blk-A", BlockHeight: 1, OrphanedAt: now})
		if len(history) != 3 ||
			history[0].BlockHash != "blk-A" || history[1].BlockHash != "blk-B" || history[2].BlockHash != "blk-A" {
			t.Fatalf("non-consecutive re-append broken: %+v", history)
		}
	})

	t.Run("CapKeepsNewest", func(t *testing.T) {
		var history []OrphanedAnchor
		for i := 0; i < MaxOrphanedAnchors+2; i++ {
			history = AppendOrphanedAnchor(history, OrphanedAnchor{
				BlockHash:   fmt.Sprintf("blk-%d", i),
				BlockHeight: uint64(i),
				OrphanedAt:  now.Add(time.Duration(i) * time.Minute),
			})
		}
		if len(history) != MaxOrphanedAnchors {
			t.Fatalf("history len = %d, want cap %d", len(history), MaxOrphanedAnchors)
		}
		// The two oldest entries (blk-0, blk-1) were dropped; the newest
		// (blk-6) is the tail.
		if history[0].BlockHash != "blk-2" {
			t.Errorf("oldest surviving entry = %q, want blk-2", history[0].BlockHash)
		}
		if history[len(history)-1].BlockHash != fmt.Sprintf("blk-%d", MaxOrphanedAnchors+1) {
			t.Errorf("newest entry = %q, want blk-%d", history[len(history)-1].BlockHash, MaxOrphanedAnchors+1)
		}
	})
}
