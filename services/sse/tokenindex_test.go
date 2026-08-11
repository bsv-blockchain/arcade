package sse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// tokenStoreStub is a programmable Store for the index's unit tests: it
// records every batch it was asked for and can fail on demand.
type tokenStoreStub struct {
	store.Store

	tokens map[string][]string
	err    error

	calls   int
	batches [][]string
}

// EnrichMerklePath is a no-op: these tests never exercise BUMP extraction, but
// it must exist so fan-out enrichment doesn't dereference the nil embedded
// store.Store.
func (s *tokenStoreStub) EnrichMerklePath(_ context.Context, _ *models.TransactionStatus) {}

func (s *tokenStoreStub) TokensForTxIDs(_ context.Context, txids []string) (map[string][]string, error) {
	s.calls++
	s.batches = append(s.batches, slices.Clone(txids))
	if s.err != nil {
		return nil, s.err
	}
	out := make(map[string][]string)
	for _, txid := range txids {
		if tokens, ok := s.tokens[txid]; ok {
			out[txid] = tokens
		}
	}
	return out, nil
}

func newTestIndex(t *testing.T, st store.Store, entries, bytes int, ttl time.Duration) *tokenIndex {
	t.Helper()
	return newTokenIndexWithLimits(st, zap.NewNop(), entries, bytes, ttl)
}

// TestTokenIndexCachesPositiveResults is the base case: a resolved token set
// answers every later lookup for the same txid without touching the store.
func TestTokenIndexCachesPositiveResults(t *testing.T) {
	st := &tokenStoreStub{tokens: map[string][]string{"tx-1": {tokA}}}
	idx := newTestIndex(t, st, 16, 1<<20, time.Minute)

	for range 5 {
		if got := idx.lookup(context.Background(), "tx-1"); !got.has(tokA) {
			t.Fatalf("lookup = %v, want the set to contain %s", got, tokA)
		}
	}
	if st.calls != 1 {
		t.Errorf("store calls = %d, want 1", st.calls)
	}
}

// TestTokenIndexDoesNotCacheEmptyResults is a correctness guard, not an
// optimisation. The api-server records submissions asynchronously, so a status
// event can overtake its own submission row by a few milliseconds. Caching
// "no subscriber" would then suppress every later transition of that
// transaction for the whole TTL — a silent delivery hole.
func TestTokenIndexDoesNotCacheEmptyResults(t *testing.T) {
	st := &tokenStoreStub{tokens: map[string][]string{}}
	idx := newTestIndex(t, st, 16, 1<<20, time.Minute)

	if got := idx.lookup(context.Background(), "tx-late"); len(got) != 0 {
		t.Fatalf("lookup = %v, want empty", got)
	}
	if got := idx.lookup(context.Background(), "tx-late"); len(got) != 0 {
		t.Fatalf("second lookup = %v, want empty", got)
	}
	if st.calls != 2 {
		t.Errorf("store calls = %d, want 2 (an empty answer is never cached)", st.calls)
	}
	if idx.len() != 0 {
		t.Errorf("cache holds %d entries, want 0", idx.len())
	}

	// The submission lands late; the very next event must see it.
	st.tokens["tx-late"] = []string{tokA}
	if got := idx.lookup(context.Background(), "tx-late"); !got.has(tokA) {
		t.Errorf("lookup after late submission = %v, want it to contain %s", got, tokA)
	}
}

// TestTokenIndexWithholdsOnStoreError: a failed lookup must behave like the
// probe it replaced — withhold the frame (the client recovers via catchup) and
// cache nothing, so the next event retries.
func TestTokenIndexWithholdsOnStoreError(t *testing.T) {
	st := &tokenStoreStub{err: errors.New("boom")}
	idx := newTestIndex(t, st, 16, 1<<20, time.Minute)

	if got := idx.lookup(context.Background(), "tx-err"); len(got) != 0 {
		t.Errorf("lookup on store error = %v, want empty", got)
	}
	if idx.len() != 0 {
		t.Errorf("cache holds %d entries after an error, want 0", idx.len())
	}
	_ = idx.lookup(context.Background(), "tx-err")
	if st.calls != 2 {
		t.Errorf("store calls = %d, want 2 (errors are not cached)", st.calls)
	}
}

// TestTokenIndexExpiresEntries covers the TTL: a cached set is used to
// SUPPRESS delivery, so it must not be able to suppress indefinitely if a
// second submission registers the same txid under another token.
func TestTokenIndexExpiresEntries(t *testing.T) {
	st := &tokenStoreStub{tokens: map[string][]string{"tx-ttl": {tokA}}}
	idx := newTestIndex(t, st, 16, 1<<20, time.Minute)
	now := time.Now()
	idx.now = func() time.Time { return now }

	_ = idx.lookup(context.Background(), "tx-ttl")
	now = now.Add(30 * time.Second)
	_ = idx.lookup(context.Background(), "tx-ttl")
	if st.calls != 1 {
		t.Fatalf("store calls before TTL = %d, want 1", st.calls)
	}

	st.tokens["tx-ttl"] = []string{tokA, "tok-B"}
	now = now.Add(time.Minute)
	got := idx.lookup(context.Background(), "tx-ttl")
	if st.calls != 2 {
		t.Errorf("store calls after TTL = %d, want 2", st.calls)
	}
	if !got.has("tok-B") {
		t.Errorf("post-expiry lookup = %v, want it to pick up tok-B", got)
	}
}

// TestTokenIndexEnforcesEntryCap covers the first of the two bounds.
func TestTokenIndexEnforcesEntryCap(t *testing.T) {
	const entryCap = 8
	st := &tokenStoreStub{tokens: map[string][]string{}}
	idx := newTestIndex(t, st, entryCap, 1<<20, time.Minute)
	for i := range 64 {
		txid := fmt.Sprintf("tx-%03d", i)
		st.tokens[txid] = []string{tokA}
		_ = idx.lookup(context.Background(), txid)
	}
	if got := idx.len(); got != entryCap {
		t.Errorf("cache holds %d entries, want the cap %d", got, entryCap)
	}
}

// TestTokenIndexEnforcesByteBudget covers the second bound. The entry cap
// alone says nothing about bytes — token strings are caller-supplied — and
// this pod has been OOM-killed by an unbounded resident set before
// (#237/#238), so the byte budget must bind independently.
func TestTokenIndexEnforcesByteBudget(t *testing.T) {
	const budget = 4096
	st := &tokenStoreStub{tokens: map[string][]string{}}
	idx := newTestIndex(t, st, 1024, budget, time.Minute)

	long := string(make([]byte, 512)) // a 512-byte callback token
	for i := range 64 {
		txid := fmt.Sprintf("tx-%03d", i)
		st.tokens[txid] = []string{long}
		_ = idx.lookup(context.Background(), txid)
	}
	if idx.len() >= 64 {
		t.Errorf("cache holds %d entries, want the byte budget to have evicted", idx.len())
	}
	idx.mu.Lock()
	resident := idx.bytes
	idx.mu.Unlock()
	if resident > budget {
		t.Errorf("resident bytes = %d, over the %d budget", resident, budget)
	}
}

// TestTokenIndexBatchBypassesCache pins the deliberate asymmetry: a bulk
// event's txids are a block's terminal-state burst with no locality against
// the live working set, so admitting them would evict the entries that
// actually produce hits.
func TestTokenIndexBatchBypassesCache(t *testing.T) {
	st := &tokenStoreStub{tokens: map[string][]string{"tx-a": {tokA}, "tx-b": {tokA}}}
	idx := newTestIndex(t, st, 16, 1<<20, time.Minute)

	got := idx.resolveBatch(context.Background(), []string{"tx-a", "tx-b", "tx-c"})
	if !got["tx-a"].has(tokA) || !got["tx-b"].has(tokA) {
		t.Errorf("batch = %v, want tx-a and tx-b subscribed to %s", got, tokA)
	}
	if _, ok := got["tx-c"]; ok {
		t.Errorf("unsubscribed txid present in batch result: %v", got["tx-c"])
	}
	if st.calls != 1 {
		t.Errorf("store calls = %d, want 1 for the whole batch", st.calls)
	}
	if len(st.batches[0]) != 3 {
		t.Errorf("batch carried %d txids, want 3", len(st.batches[0]))
	}
	if idx.len() != 0 {
		t.Errorf("cache holds %d entries after a bulk resolve, want 0", idx.len())
	}
}

// TestTokenIndexNilStore keeps the "no fan-out wired" construction safe.
func TestTokenIndexNilStore(t *testing.T) {
	idx := newTokenIndex(nil, zap.NewNop())
	if got := idx.lookup(context.Background(), "tx"); got != nil {
		t.Errorf("lookup with nil store = %v, want nil", got)
	}
	if got := idx.resolveBatch(context.Background(), []string{"tx"}); got != nil {
		t.Errorf("resolveBatch with nil store = %v, want nil", got)
	}
}
