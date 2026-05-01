package models

import "testing"

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
// F-003: once a tx is MINED/IMMUTABLE/REJECTED/DOUBLE_SPEND_ATTEMPTED, no
// in-flight status update may regress it.
func TestStatus_CanTransitionFrom_TerminalStaysTerminal(t *testing.T) {
	terminals := []Status{
		StatusRejected,
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
		{StatusRejected, StatusSeenOnNetwork, "late SEEN after REJECTED"},
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
