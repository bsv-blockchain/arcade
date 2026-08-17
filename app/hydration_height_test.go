package app

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/store"
)

// fakeChainReader is a finality.ChainReader whose GetTip can express
// "unavailable" by returning nil — the property that makes this fix work at
// all. GetHeaders is unused by hydrationHeight.
type fakeChainReader struct {
	tip *chaintracks.BlockHeader
}

func (f *fakeChainReader) GetTip(context.Context) *chaintracks.BlockHeader { return f.tip }

func (f *fakeChainReader) GetHeaders(context.Context, uint32, uint32) ([]*chaintracks.BlockHeader, error) {
	return nil, nil
}

// fakeHeightStore embeds store.Store so only the one method under test needs
// implementing; anything else panics rather than silently returning a zero.
type fakeHeightStore struct {
	store.Store

	height uint64
	err    error
	calls  int
}

func (f *fakeHeightStore) GetActiveTipBlockHeight(context.Context) (uint64, error) {
	f.calls++
	return f.height, f.err
}

func TestHydrationHeight(t *testing.T) {
	cases := []struct {
		name        string
		reader      *fakeChainReader
		st          *fakeHeightStore
		wantHeight  uint64
		wantKnown   bool
		wantSource  string
		wantStoreHi bool // the store fallback should have been consulted
	}{
		{
			name:       "chaintracks tip wins",
			reader:     &fakeChainReader{tip: &chaintracks.BlockHeader{Height: 915_432}},
			st:         &fakeHeightStore{height: 915_430},
			wantHeight: 915_432,
			wantKnown:  true,
			wantSource: "chaintracks",
		},
		{
			name:        "nil tip falls back to the store active tip",
			reader:      &fakeChainReader{tip: nil},
			st:          &fakeHeightStore{height: 915_430},
			wantHeight:  915_430,
			wantKnown:   true,
			wantSource:  "store",
			wantStoreHi: true,
		},
		{
			name:        "no reader configured falls back to the store",
			reader:      nil,
			st:          &fakeHeightStore{height: 42},
			wantHeight:  42,
			wantKnown:   true,
			wantSource:  "store",
			wantStoreHi: true,
		},
		{
			// The exact v0.13.0 production condition: api-server had no
			// chaintracks and nothing had populated block_processing.
			name:        "both sources unavailable is UNKNOWN, not height zero",
			reader:      &fakeChainReader{tip: nil},
			st:          &fakeHeightStore{height: 0},
			wantHeight:  0,
			wantKnown:   false,
			wantSource:  "none",
			wantStoreHi: true,
		},
		{
			name:        "store error is unknown, not zero",
			reader:      nil,
			st:          &fakeHeightStore{err: errors.New("db down")},
			wantHeight:  0,
			wantKnown:   false,
			wantSource:  "none",
			wantStoreHi: true,
		},
		{
			// A genuine genesis-height chain must still read as KNOWN when the
			// chain source answered. This is the case a bare uint64 return
			// could never distinguish.
			name:       "tip at height zero is known",
			reader:     &fakeChainReader{tip: &chaintracks.BlockHeader{Height: 0}},
			st:         &fakeHeightStore{height: 500},
			wantHeight: 0,
			wantKnown:  true,
			wantSource: "chaintracks",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A nil *fakeChainReader must be passed as a genuinely nil
			// interface, not a typed nil.
			height, known, source := func() (uint64, bool, string) {
				if tc.reader == nil {
					return hydrationHeight(context.Background(), nil, tc.st, zap.NewNop())
				}
				return hydrationHeight(context.Background(), tc.reader, tc.st, zap.NewNop())
			}()

			if height != tc.wantHeight {
				t.Errorf("height = %d, want %d", height, tc.wantHeight)
			}
			if known != tc.wantKnown {
				t.Errorf("known = %v, want %v", known, tc.wantKnown)
			}
			if source != tc.wantSource {
				t.Errorf("source = %q, want %q", source, tc.wantSource)
			}
			if got := tc.st.calls > 0; got != tc.wantStoreHi {
				t.Errorf("store consulted = %v, want %v", got, tc.wantStoreHi)
			}
		})
	}
}

func TestHydrationHeight_FeedsTrackerScan(t *testing.T) {
	// The join between the two halves of the fix: an unknown height must
	// produce a scan that prunes nothing, and a known one must produce a real
	// cutoff. Getting this backwards is precisely the v0.13.0 bug.
	unknown := &fakeChainReader{tip: nil}
	emptyStore := &fakeHeightStore{height: 0}
	h, known, _ := hydrationHeight(context.Background(), unknown, emptyStore, zap.NewNop())
	if scan := store.NewTrackerScan(h, known); scan.PruneMinedBelow != 0 {
		t.Fatalf("unknown height must disable pruning, got PruneMinedBelow=%d", scan.PruneMinedBelow)
	}

	live := &fakeChainReader{tip: &chaintracks.BlockHeader{Height: 900_000}}
	h, known, _ = hydrationHeight(context.Background(), live, emptyStore, zap.NewNop())
	scan := store.NewTrackerScan(h, known)
	if scan.PruneMinedBelow != 900_000-store.ConfirmationsRequired+1 {
		t.Fatalf("PruneMinedBelow = %d, want %d", scan.PruneMinedBelow, 900_000-store.ConfirmationsRequired+1)
	}
}
