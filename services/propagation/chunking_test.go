package propagation

import (
	"math/rand"
	"testing"
)

// sizes builds a rawTxs slice whose i-th element has exactly sizes[i] bytes;
// planChunks only ever reads len(), so the contents are irrelevant.
func sizes(ns ...int) [][]byte {
	out := make([][]byte, len(ns))
	for i, n := range ns {
		out[i] = make([]byte, n)
	}
	return out
}

func spansEqual(a, b []chunkSpan) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPlanChunks_Table is the specification for the byte-aware chunker
// (issue #271): chunks are bounded by BOTH a tx count and a byte total, the
// local caps are inclusive, a non-positive cap disables that bound, and a
// lone transaction larger than the byte cap still travels — alone.
func TestPlanChunks_Table(t *testing.T) {
	cases := []struct {
		name     string
		raw      [][]byte
		maxCount int
		maxBytes int
		want     []chunkSpan
	}{
		{name: "empty input yields no spans", raw: sizes(), maxCount: 10, maxBytes: 100, want: nil},
		{name: "single tx within both caps", raw: sizes(10), maxCount: 10, maxBytes: 100, want: []chunkSpan{{0, 1}}},
		{
			name: "exact byte fit is inclusive",
			raw:  sizes(100, 100, 100), maxCount: 10, maxBytes: 300,
			want: []chunkSpan{{0, 3}},
		},
		{
			name: "one byte over closes the span",
			raw:  sizes(100, 100, 100, 1), maxCount: 10, maxBytes: 300,
			want: []chunkSpan{{0, 3}, {3, 4}},
		},
		{
			name: "count cap first (today's 25 txs at cap 10)",
			raw:  sizes(4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4), maxCount: 10, maxBytes: 1 << 20,
			want: []chunkSpan{{0, 10}, {10, 20}, {20, 25}},
		},
		{
			name: "byte cap first",
			raw:  sizes(100, 100, 100, 100, 100, 100, 100, 100, 100, 100), maxCount: 1000, maxBytes: 250,
			want: []chunkSpan{{0, 2}, {2, 4}, {4, 6}, {6, 8}, {8, 10}},
		},
		{
			// {0,2}: a third 100 would reach 300 > 250. {2,5}: 100+100+50 = 250
			// fits exactly, then the count cap (3) closes it. {5,8}: three 50s,
			// count cap again. {8,10}: 50+200 = 250 fits exactly.
			name: "both caps bind within one batch",
			raw:  sizes(100, 100, 100, 100, 50, 50, 50, 50, 50, 200), maxCount: 3, maxBytes: 250,
			want: []chunkSpan{{0, 2}, {2, 5}, {5, 8}, {8, 10}},
		},
		{
			name: "lone oversize tx at the start travels alone",
			raw:  sizes(500, 100, 100), maxCount: 10, maxBytes: 300,
			want: []chunkSpan{{0, 1}, {1, 3}},
		},
		{
			name: "lone oversize tx in the middle travels alone",
			raw:  sizes(100, 500, 100), maxCount: 10, maxBytes: 300,
			want: []chunkSpan{{0, 1}, {1, 2}, {2, 3}},
		},
		{
			name: "lone oversize tx at the end travels alone",
			raw:  sizes(100, 100, 500), maxCount: 10, maxBytes: 300,
			want: []chunkSpan{{0, 2}, {2, 3}},
		},
		{
			name: "non-positive byte cap disables the byte bound",
			raw:  sizes(1<<20, 1<<20, 1<<20), maxCount: 2, maxBytes: 0,
			want: []chunkSpan{{0, 2}, {2, 3}},
		},
		{
			name: "non-positive count cap disables the count bound",
			raw:  sizes(1, 1, 1, 1, 1), maxCount: 0, maxBytes: 3,
			want: []chunkSpan{{0, 3}, {3, 5}},
		},
		{
			name: "both caps disabled yields one span",
			raw:  sizes(1<<20, 1<<20, 1<<20), maxCount: -1, maxBytes: -1,
			want: []chunkSpan{{0, 3}},
		},
		{
			name: "zero-length txs never produce an empty span",
			raw:  sizes(0, 0, 0), maxCount: 2, maxBytes: 1,
			want: []chunkSpan{{0, 2}, {2, 3}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planChunks(tc.raw, tc.maxCount, tc.maxBytes)
			if !spansEqual(got, tc.want) {
				t.Fatalf("planChunks(%d txs, count=%d, bytes=%d) = %v, want %v",
					len(tc.raw), tc.maxCount, tc.maxBytes, got, tc.want)
			}
		})
	}
}

// TestPlanChunks_Invariants guards the property broadcastInChunks relies on
// for its concurrent, mutex-free writes into results[c.start:c.end]: spans
// are contiguous from 0 to len(rawTxs), non-empty and disjoint. It also pins
// the two caps and the lone-oversize escape hatch over random inputs.
func TestPlanChunks_Invariants(t *testing.T) {
	rng := rand.New(rand.NewSource(271)) //nolint:gosec // deterministic test data
	for iter := 0; iter < 500; iter++ {
		n := rng.Intn(60)
		raw := make([][]byte, n)
		for i := range raw {
			raw[i] = make([]byte, rng.Intn(1200))
		}
		maxCount := rng.Intn(12) - 1 // -1..10; <=0 disables
		maxBytes := rng.Intn(3000) - 1

		spans := planChunks(raw, maxCount, maxBytes)

		if n == 0 {
			if len(spans) != 0 {
				t.Fatalf("iter %d: empty input produced spans %v", iter, spans)
			}
			continue
		}
		next := 0
		for _, s := range spans {
			if s.start != next {
				t.Fatalf("iter %d: span %v does not start where the previous ended (%d)", iter, s, next)
			}
			if s.end <= s.start {
				t.Fatalf("iter %d: empty or inverted span %v", iter, s)
			}
			count := s.end - s.start
			if maxCount > 0 && count > maxCount {
				t.Fatalf("iter %d: span %v holds %d txs, cap %d", iter, s, count, maxCount)
			}
			bytes := rawTxsBytes(raw[s.start:s.end])
			if maxBytes > 0 && bytes > maxBytes && count != 1 {
				t.Fatalf("iter %d: span %v holds %d bytes over cap %d with %d txs", iter, s, bytes, maxBytes, count)
			}
			next = s.end
		}
		if next != n {
			t.Fatalf("iter %d: spans cover [0,%d) of %d txs", iter, next, n)
		}
	}
}
