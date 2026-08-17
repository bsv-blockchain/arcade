package store

import (
	"testing"

	"github.com/bsv-blockchain/arcade/models"
)

// legacyKeep is a literal copy of the pre-#276 hydration filter from
// loadFromStore, inverted to a keep/skip predicate. It is the oracle proving
// the TrackerScan refactor is behavior-preserving; do not "simplify" it to
// call Keep, that would make the test vacuous.
func legacyKeep(status models.Status, blockHeight, currentHeight uint64) bool {
	if status.IsTerminal() && status != models.StatusMined {
		return false
	}
	if status == models.StatusMined && blockHeight > 0 {
		if currentHeight >= blockHeight+ConfirmationsRequired {
			return false
		}
	}
	return true
}

func TestTracksStatus_DerivedFromAllStatuses(t *testing.T) {
	for _, s := range models.AllStatuses() {
		want := !s.IsTerminal() || s == models.StatusMined
		if got := TracksStatus(s); got != want {
			t.Errorf("TracksStatus(%q) = %v, want %v", s, got, want)
		}
	}

	// StatusUnknown is the trap: models.NonTerminalStatuses() omits it, but the
	// pre-#276 filter kept it. Deriving from AllStatuses must preserve that.
	if !TracksStatus(models.StatusUnknown) {
		t.Error("StatusUnknown must be tracked (pre-#276 filter kept it)")
	}
	for _, s := range []models.Status{
		models.StatusRejected, models.StatusDoubleSpendAttempted, models.StatusImmutable,
	} {
		if TracksStatus(s) {
			t.Errorf("terminal status %q must not be tracked", s)
		}
	}
	if !TracksStatus(models.StatusMined) {
		t.Error("MINED must be tracked (retained until deeply confirmed)")
	}
}

func TestTrackerStatuses_ReturnsCopy(t *testing.T) {
	first := TrackerStatuses()
	if len(first) == 0 {
		t.Fatal("TrackerStatuses returned empty set")
	}
	first[0] = models.Status("MUTATED")
	if second := TrackerStatuses(); second[0] == models.Status("MUTATED") {
		t.Fatal("TrackerStatuses leaked the shared backing array")
	}
}

func TestTrackerStatuses_ExcludesUnknownStatusStrings(t *testing.T) {
	// A status string outside the models domain is dropped — the documented
	// deliberate change from the pre-#276 filter.
	if TracksStatus(models.Status("SOME_FUTURE_STATUS")) {
		t.Error("out-of-domain status must not be tracked")
	}
}

func TestTrackerScan_MatchesLegacyPredicate(t *testing.T) {
	// Cross-product over the interesting boundaries: the ConfirmationsRequired
	// edge (99/100/101), zero, and a realistic mainnet-scale height.
	heights := []uint64{0, 1, 50, 99, 100, 101, 900_000}
	tips := []uint64{0, 1, 99, 100, 101, 900_000}

	for _, status := range models.AllStatuses() {
		for _, tip := range tips {
			// tipKnown=true is the only regime where the legacy predicate and
			// the new one are meant to agree: legacy had no way to express
			// "unknown", it just passed 0.
			scan := NewTrackerScan(tip, true)
			for _, h := range heights {
				want := legacyKeep(status, h, tip)
				got := scan.Keep(status, h)
				if got != want {
					t.Errorf("Keep(status=%s, height=%d, tip=%d) = %v, legacy = %v (PruneMinedBelow=%d)",
						status, h, tip, got, want, scan.PruneMinedBelow)
				}
			}
		}
	}
}

func TestNewTrackerScan_UnknownHeightDisablesPruning(t *testing.T) {
	for _, tip := range []uint64{0, 1, 100, 900_000} {
		scan := NewTrackerScan(tip, false)
		if scan.PruneMinedBelow != 0 {
			t.Fatalf("NewTrackerScan(%d, false).PruneMinedBelow = %d, want 0", tip, scan.PruneMinedBelow)
		}
		// Every MINED row must survive, however old.
		if !scan.Keep(models.StatusMined, 1) {
			t.Fatalf("tip=%d unknown: ancient MINED row must be kept", tip)
		}
	}
}

func TestNewTrackerScan_BelowConfirmationsDisablesPruning(t *testing.T) {
	// Guards the uint64 underflow in tipHeight - ConfirmationsRequired.
	for _, tip := range []uint64{0, 1, ConfirmationsRequired - 1} {
		scan := NewTrackerScan(tip, true)
		if scan.PruneMinedBelow != 0 {
			t.Fatalf("NewTrackerScan(%d, true).PruneMinedBelow = %d, want 0", tip, scan.PruneMinedBelow)
		}
	}
}

func TestTrackerScan_PruneBoundary(t *testing.T) {
	const tip = uint64(1000)
	scan := NewTrackerScan(tip, true) // PruneMinedBelow = 901

	if scan.PruneMinedBelow != 901 {
		t.Fatalf("PruneMinedBelow = %d, want 901", scan.PruneMinedBelow)
	}
	cases := []struct {
		height uint64
		keep   bool
	}{
		{0, true},    // no recorded height: always kept
		{1, false},   // ancient
		{900, false}, // exactly ConfirmationsRequired deep -> pruned
		{901, true},  // one short of deeply confirmed -> kept
		{1000, true}, // the tip itself
	}
	for _, tc := range cases {
		if got := scan.Keep(models.StatusMined, tc.height); got != tc.keep {
			t.Errorf("Keep(MINED, %d) with tip %d = %v, want %v", tc.height, tip, got, tc.keep)
		}
	}
}

func TestTrackerScan_MinedZeroHeightAlwaysKept(t *testing.T) {
	// A high cutoff must not sweep away MINED rows with no recorded height —
	// the easy thing to lose in a `WHERE block_height >= $2` pushdown.
	scan := NewTrackerScan(900_000, true)
	if !scan.Keep(models.StatusMined, 0) {
		t.Fatal("MINED at block_height 0 must be kept at any cutoff")
	}
}
