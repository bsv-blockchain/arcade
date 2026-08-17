package store

import (
	"github.com/bsv-blockchain/arcade/models"
)

// TrackerRow is the projection TxTracker hydration needs and nothing more:
// identity, status, mined height. Passed by value — three fields, no pointer —
// so backends can emit from a stack local and hydration cannot retain rows.
//
// Deliberately not *models.TransactionStatus: a sparse 16-field struct costs a
// heap allocation per row (millions at startup) and invites a future caller to
// read a field the store never populated.
type TrackerRow struct {
	TxID        string
	Status      models.Status
	BlockHeight uint64
}

// trackerStatuses is the set of statuses the tracker keeps, derived from
// models.AllStatuses() by one rule so a status added to the models domain is
// classified here automatically and can never go missing from the store-side
// filter.
//
// Deliberately NOT models.NonTerminalStatuses(): that set omits StatusUnknown,
// which the pre-#276 hydration filter kept (StatusUnknown.IsTerminal() is
// false). Deriving from AllStatuses preserves that behavior exactly, and keeps
// hydration insulated from SSE-driven changes to NonTerminalStatuses.
var trackerStatuses = func() []models.Status {
	all := models.AllStatuses()
	out := make([]models.Status, 0, len(all))
	for _, s := range all {
		if !s.IsTerminal() || s == models.StatusMined {
			out = append(out, s)
		}
	}
	return out
}()

// trackerStatusSet is the O(1) membership form of trackerStatuses.
var trackerStatusSet = func() map[models.Status]struct{} {
	set := make(map[models.Status]struct{}, len(trackerStatuses))
	for _, s := range trackerStatuses {
		set[s] = struct{}{}
	}
	return set
}()

// TrackerStatuses returns the statuses IterateTrackerRows implementations must
// filter on, as a copy so callers cannot mutate the shared set.
//
// A row whose status string is outside models.AllStatuses() (a hand-edited row,
// or a downgrade from a future version) is NOT in this set and is therefore
// dropped. The pre-#276 filter kept such rows, because !IsTerminal() is true
// for any unrecognized string. This is a deliberate change: the positive
// status filter is what makes the query selective, and a status arcade does
// not know cannot participate in the lattice prefilter anyway — the store
// remains authoritative for anything the tracker doesn't know about.
func TrackerStatuses() []models.Status {
	out := make([]models.Status, len(trackerStatuses))
	copy(out, trackerStatuses)
	return out
}

// TracksStatus reports whether the tracker keeps rows in this status.
func TracksStatus(s models.Status) bool {
	_, ok := trackerStatusSet[s]
	return ok
}

// TrackerScan parameterizes a hydration scan. The zero value is the safe
// degraded scan: no height pruning, every MINED row kept.
type TrackerScan struct {
	// PruneMinedBelow, when > 0, is the inclusive lower bound on block_height
	// for MINED rows worth tracking: a MINED row with a nonzero block_height
	// below this is deeply confirmed and would be pruned moments later.
	//
	// Zero means the chain height is unknown, which disables MINED pruning
	// entirely. That is the degraded mode — safe, but it loads the full mined
	// history, which is what made api-server take 147s to bind its listener in
	// v0.13.0 (see NewTrackerScan).
	PruneMinedBelow uint64
}

// NewTrackerScan derives the scan from a chain tip height and, crucially,
// whether that height is KNOWN.
//
// The (value, known) pair is taken explicitly because go-chaintracks' GetHeight
// returns 0 both for genesis and for every failure mode, and callers have
// twice treated the latter as the former. Passing a bare uint64 here would
// re-open exactly that hole: a zero would silently mean "genesis", and MINED
// pruning would appear to work while doing nothing.
func NewTrackerScan(tipHeight uint64, tipKnown bool) TrackerScan {
	// tipHeight < ConfirmationsRequired cannot prune anything (no block is
	// buried deeply enough yet) and would underflow the subtraction below.
	if !tipKnown || tipHeight < ConfirmationsRequired {
		return TrackerScan{}
	}
	return TrackerScan{PruneMinedBelow: tipHeight - ConfirmationsRequired + 1}
}

// Statuses returns the status set this scan matches.
func (s TrackerScan) Statuses() []models.Status { return TrackerStatuses() }

// Keep is the single predicate defining what hydration retains. Store backends
// push as much of it down into their query or index as they can; the caller
// re-applies it to every emitted row, so a pushdown that is less selective than
// Keep is only a performance property, never a correctness one.
//
// Equivalent to the pre-#276 inline filter, which skipped terminal-but-not-
// MINED rows and skipped MINED rows where currentHeight >= blockHeight +
// ConfirmationsRequired. That inequality rearranges to blockHeight <
// PruneMinedBelow; store/trackerscan_test.go pins the equivalence across the
// full status × height × tip cross-product.
func (s TrackerScan) Keep(status models.Status, blockHeight uint64) bool {
	if !TracksStatus(status) {
		return false
	}
	// A MINED row with no recorded height cannot be judged deeply confirmed,
	// so it is always kept — matching the pre-#276 `BlockHeight > 0` guard.
	if status == models.StatusMined && blockHeight > 0 &&
		s.PruneMinedBelow > 0 && blockHeight < s.PruneMinedBelow {
		return false
	}
	return true
}
