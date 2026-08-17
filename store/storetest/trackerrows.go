// Package storetest holds cross-backend conformance suites for store.Store.
//
// It is a non-test package so build-tagged backend test packages
// (//go:build postgres, //go:build integration) can import it, the same way
// net/http/httptest imports testing from a non-test package.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Backend is the slice of store.Store the tracker-rows suite drives.
type Backend interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	IterateTrackerRows(ctx context.Context, scan store.TrackerScan, fn func(store.TrackerRow) error) error
}

// tipHeight is the fixed chain tip the corpus is built around. Chosen large
// enough that tip - ConfirmationsRequired cannot underflow and small enough to
// stay readable in failure messages.
const tipHeight = uint64(900_000)

// seed is one corpus row plus the identity the assertions use.
type seed struct {
	txid   string
	status models.Status
	height uint64
	note   string
}

// corpus covers every status in the models domain plus the MINED height
// boundary on both sides of tip-ConfirmationsRequired. Heights are only
// meaningful for MINED rows, but a couple of non-MINED rows carry one too so a
// backend that applies the height predicate to the wrong statuses is caught.
func corpus() []seed {
	out := make([]seed, 0, len(models.AllStatuses())+8)
	for i, st := range models.AllStatuses() {
		out = append(out, seed{
			txid:   txidHex(i),
			status: st,
			note:   "one row per status",
		})
	}
	next := len(models.AllStatuses())
	minedHeights := []struct {
		h    uint64
		note string
	}{
		{0, "MINED with no recorded height: never prunable"},
		{1, "ancient MINED"},
		{tipHeight - store.ConfirmationsRequired - 1, "one below the prune boundary"},
		{tipHeight - store.ConfirmationsRequired, "exactly ConfirmationsRequired deep: pruned"},
		{tipHeight - store.ConfirmationsRequired + 1, "one short of deeply confirmed: kept"},
		{tipHeight, "the tip itself"},
	}
	for _, mh := range minedHeights {
		out = append(out, seed{txid: txidHex(next), status: models.StatusMined, height: mh.h, note: mh.note})
		next++
	}
	// Terminal rows carrying a recent height: these must never be emitted, and
	// a height-only filter would wrongly keep them.
	out = append(out,
		seed{txid: txidHex(next), status: models.StatusRejected, height: tipHeight, note: "recent REJECTED"},
		seed{txid: txidHex(next + 1), status: models.StatusImmutable, height: tipHeight, note: "recent IMMUTABLE"},
	)
	return out
}

func txidHex(i int) string { return fmt.Sprintf("%064x", i+1) }

// RunTrackerRowsSuite asserts that a backend's IterateTrackerRows honors the
// store.Store contract: it emits exactly the rows TrackerScan.Keep accepts,
// with faithful field values, and never leaks a terminal row.
//
// TrackerScan.Keep is the specification; the backend's SQL or index filter is
// only a pushdown of it. The ExactAgreement subtest is the anti-drift check —
// it is what stops the three backends and the Go predicate from diverging.
//
// newBackend must return a backend with an empty transactions set; it is called
// once per subtest so subtests cannot contaminate each other.
func RunTrackerRowsSuite(t *testing.T, newBackend func(t *testing.T) Backend) {
	t.Helper()

	t.Run("ExactAgreement", func(t *testing.T) {
		// The core assertion, run across every interesting height regime.
		regimes := []struct {
			name string
			scan store.TrackerScan
		}{
			{"KnownTip", store.NewTrackerScan(tipHeight, true)},
			{"UnknownHeight", store.NewTrackerScan(tipHeight, false)},
			{"ZeroKnown", store.NewTrackerScan(0, true)},
			{"BelowConfirmations", store.NewTrackerScan(store.ConfirmationsRequired-1, true)},
		}
		for _, r := range regimes {
			t.Run(r.name, func(t *testing.T) {
				b := seedBackend(t, newBackend)
				got := collect(t, b, r.scan)

				want := map[string]seed{}
				for _, s := range corpus() {
					if r.scan.Keep(s.status, s.height) {
						want[s.txid] = s
					}
				}
				assertSameSet(t, want, got)
			})
		}
	})

	t.Run("NoTerminalLeakage", func(t *testing.T) {
		// The memory contract from issue #276: REJECTED alone is ~44% of a
		// mature store and must never leave the database.
		b := seedBackend(t, newBackend)
		for _, scan := range []store.TrackerScan{
			store.NewTrackerScan(tipHeight, true),
			store.NewTrackerScan(tipHeight, false),
		} {
			for txid, row := range collect(t, b, scan) {
				if row.Status.IsTerminal() && row.Status != models.StatusMined {
					t.Errorf("terminal row leaked: txid=%s status=%s", txid, row.Status)
				}
				if !store.TracksStatus(row.Status) {
					t.Errorf("untracked status emitted: txid=%s status=%s", txid, row.Status)
				}
			}
		}
	})

	t.Run("Fidelity", func(t *testing.T) {
		b := seedBackend(t, newBackend)
		got := collect(t, b, store.NewTrackerScan(tipHeight, true))
		for _, s := range corpus() {
			row, ok := got[s.txid]
			if !ok {
				continue
			}
			if row.Status != s.status {
				t.Errorf("txid %s (%s): status = %s, want %s", s.txid, s.note, row.Status, s.status)
			}
			if row.BlockHeight != s.height {
				t.Errorf("txid %s (%s): block_height = %d, want %d", s.txid, s.note, row.BlockHeight, s.height)
			}
			if row.TxID != s.txid {
				t.Errorf("row.TxID = %s, want %s", row.TxID, s.txid)
			}
		}
	})

	t.Run("MinedZeroHeightKept", func(t *testing.T) {
		// A MINED row with no recorded height cannot be judged deeply confirmed.
		// The easy thing to lose in a `WHERE block_height >= $cutoff` pushdown.
		b := seedBackend(t, newBackend)
		got := collect(t, b, store.NewTrackerScan(tipHeight, true))
		for _, s := range corpus() {
			if s.status == models.StatusMined && s.height == 0 {
				if _, ok := got[s.txid]; !ok {
					t.Fatalf("MINED row at block_height 0 (txid %s) was pruned", s.txid)
				}
			}
		}
	})

	t.Run("UnknownHeightKeepsAllMined", func(t *testing.T) {
		// The v0.13.0 production condition. Every MINED row survives.
		b := seedBackend(t, newBackend)
		got := collect(t, b, store.NewTrackerScan(tipHeight, false))
		for _, s := range corpus() {
			if s.status != models.StatusMined {
				continue
			}
			if _, ok := got[s.txid]; !ok {
				t.Errorf("MINED row txid %s (%s, height %d) dropped despite unknown chain height",
					s.txid, s.note, s.height)
			}
		}
	})

	t.Run("KnownHeightPrunes", func(t *testing.T) {
		// The whole point of the fix: a real height must actually shrink the set.
		b := seedBackend(t, newBackend)
		pruned := collect(t, b, store.NewTrackerScan(tipHeight, true))
		all := collect(t, b, store.NewTrackerScan(tipHeight, false))
		if len(pruned) >= len(all) {
			t.Fatalf("a known chain height pruned nothing: %d rows with tip, %d without", len(pruned), len(all))
		}
		boundary := tipHeight - store.ConfirmationsRequired
		for _, s := range corpus() {
			if s.status != models.StatusMined || s.height == 0 {
				continue
			}
			_, kept := pruned[s.txid]
			wantKept := s.height > boundary
			if kept != wantKept {
				t.Errorf("MINED at height %d (%s): kept=%v, want %v (boundary %d)",
					s.height, s.note, kept, wantKept, boundary)
			}
		}
	})

	t.Run("NoDuplicates", func(t *testing.T) {
		b := seedBackend(t, newBackend)
		seen := map[string]int{}
		err := b.IterateTrackerRows(context.Background(), store.NewTrackerScan(tipHeight, true),
			func(row store.TrackerRow) error {
				seen[row.TxID]++
				return nil
			})
		if err != nil {
			t.Fatalf("IterateTrackerRows: %v", err)
		}
		for txid, n := range seen {
			if n != 1 {
				t.Errorf("txid %s emitted %d times, want exactly 1", txid, n)
			}
		}
	})

	t.Run("CallbackErrorStops", func(t *testing.T) {
		b := seedBackend(t, newBackend)
		sentinel := errors.New("stop here")
		calls := 0
		err := b.IterateTrackerRows(context.Background(), store.NewTrackerScan(tipHeight, false),
			func(store.TrackerRow) error {
				calls++
				return sentinel
			})
		if !errors.Is(err, sentinel) {
			t.Fatalf("expected the callback error to surface, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("iteration continued past the callback error: %d calls", calls)
		}
	})

	t.Run("CanceledContext", func(t *testing.T) {
		b := seedBackend(t, newBackend)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := b.IterateTrackerRows(ctx, store.NewTrackerScan(tipHeight, true), func(store.TrackerRow) error {
			calls++
			return nil
		})
		if err == nil {
			t.Fatal("expected an error from a canceled context")
		}
		if calls != 0 {
			t.Fatalf("emitted %d rows under a canceled context", calls)
		}
	})
}

func seedBackend(t *testing.T, newBackend func(t *testing.T) Backend) Backend {
	t.Helper()
	b := newBackend(t)
	ctx := context.Background()
	for _, s := range corpus() {
		row := &models.TransactionStatus{
			TxID:        s.txid,
			Status:      s.status,
			BlockHeight: s.height,
		}
		if _, _, err := b.GetOrInsertStatus(ctx, row); err != nil {
			t.Fatalf("seeding txid %s (%s, %s): %v", s.txid, s.status, s.note, err)
		}
	}
	return b
}

func collect(t *testing.T, b Backend, scan store.TrackerScan) map[string]store.TrackerRow {
	t.Helper()
	out := map[string]store.TrackerRow{}
	err := b.IterateTrackerRows(context.Background(), scan, func(row store.TrackerRow) error {
		out[row.TxID] = row
		return nil
	})
	if err != nil {
		t.Fatalf("IterateTrackerRows: %v", err)
	}
	return out
}

func assertSameSet(t *testing.T, want map[string]seed, got map[string]store.TrackerRow) {
	t.Helper()

	var missing, extra []string
	for txid, s := range want {
		if _, ok := got[txid]; !ok {
			missing = append(missing, fmt.Sprintf("%s (%s h=%d: %s)", txid, s.status, s.height, s.note))
		}
	}
	byTxid := map[string]seed{}
	for _, s := range corpus() {
		byTxid[s.txid] = s
	}
	for txid := range got {
		if _, ok := want[txid]; !ok {
			s := byTxid[txid]
			extra = append(extra, fmt.Sprintf("%s (%s h=%d: %s)", txid, s.status, s.height, s.note))
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)

	// Missing rows are a correctness bug: the backend under-emitted relative to
	// TrackerScan.Keep. Extra rows mean the pushdown drifted from the predicate;
	// the tracker's client-side re-check would still drop them, but the whole
	// point of #276 is that they should not cross the wire.
	if len(missing) > 0 {
		t.Errorf("backend did not emit %d row(s) Keep accepts (CORRECTNESS):\n  %v", len(missing), missing)
	}
	if len(extra) > 0 {
		t.Errorf("backend emitted %d row(s) Keep rejects (pushdown drift):\n  %v", len(extra), extra)
	}
}
