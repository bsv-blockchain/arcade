// Package bumpcache is the shared parsed-BUMP cache behind every store
// backend's EnrichMerklePath. Enrichment runs on every GetStatus of a mined tx
// and once per subscribed tx during a MINED SSE/webhook fan-out; without a
// cache each call would re-fetch and re-parse the whole block BUMP — the
// dominant heap consumer under sustained load. Lookups and fan-out cluster on
// the few recently-mined blocks in flight, so a small LRU gives a high hit
// rate. The cache bounds its resident set two ways: an entry cap AND a total
// PathElement budget, because a single mega-block compound can dwarf any
// count-based limit (this service has OOM'd inside a 512Mi pod before —
// #237/#238). Concurrent misses for one block collapse onto a single
// fetch+parse via singleflight, so a MINED burst hitting the SSE fan-out,
// webhook workers, and GET /tx handlers at once pays for one parse, not three.
package bumpcache

import (
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"golang.org/x/sync/singleflight"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/models"
)

// maxEntries bounds how many blocks' parsed+indexed compound BUMPs stay
// resident at once.
const maxEntries = 16

// maxTotalLeaves bounds the summed PathElement count across every cached
// entry (~100B resident per element → a ceiling in the tens of MB). When an
// insert pushes the sum over budget, oldest entries are evicted until it
// fits — but the newest entry always stays, even alone over budget: a block's
// fan-out must be able to reuse one parse, and one resident compound is the
// floor the process pays anyway while serving it.
const maxTotalLeaves = 512 * 1024

// Cache is a bounded, singleflight-guarded cache of bump.CompoundIndex per
// block hash. The zero value is not usable; construct with New.
type Cache struct {
	group     singleflight.Group
	maxLeaves int

	// mu guards lru, totalLeaves, gen and inflight — every mutable field on
	// this struct except the singleflight group, which does its own locking.
	// The LRU's evict callback adjusts totalLeaves and runs synchronously
	// inside mutations, so it MUST NOT take mu itself — every lru call site
	// below already holds it.
	mu          sync.Mutex
	lru         *simplelru.LRU[string, *bump.CompoundIndex]
	totalLeaves int

	// gen is a per-block invalidation counter and inflight the number of fills
	// running for that block. A fill takes gen as a token when it starts and
	// may install only if the token still matches when it finishes, so any
	// invalidation in between discards it: those bytes predate the rebuild.
	//
	// A token per fill, rather than one shared flag per block, is what makes
	// this correct when two fills overlap — which Remove's Forget allows, and
	// which a shared flag gets wrong: the second fill's start would clear the
	// mark meant for the first, letting the first install pre-invalidation
	// bytes after the second had already installed fresh ones.
	//
	// Both maps are keyed only while a block has a fill running and are
	// pruned when its last one ends, so they hold at most one entry per block
	// being fetched at this instant. Tracking per block matters too: a single
	// counter for the whole cache would let a write to ANY block discard the
	// in-flight fill of every other, and during a merkle backlog drain (block
	// after block arriving, each ending in InsertBUMP) that is every fill,
	// leaving the cache permanently empty exactly when it is load-bearing.
	gen      map[string]uint64
	inflight map[string]int
}

// New constructs an empty cache with the package's production bounds.
func New() *Cache {
	return newWithLimits(maxEntries, maxTotalLeaves)
}

// newWithLimits is the constructor proper, split out so tests can exercise
// eviction without building half-million-leaf compounds.
func newWithLimits(entries, leaves int) *Cache {
	c := &Cache{
		maxLeaves: leaves,
		gen:       make(map[string]uint64),
		inflight:  make(map[string]int),
	}
	l, err := simplelru.NewLRU(entries, func(_ string, v *bump.CompoundIndex) {
		c.totalLeaves -= v.Leaves()
	})
	if err != nil {
		// Unreachable: NewLRU only fails for a non-positive size and every
		// caller passes a positive constant.
		panic(err)
	}
	c.lru = l
	return c
}

// Enrich populates status.MerklePath in place for a MINED/IMMUTABLE status
// that already carries a BlockHash, extracting the tx's minimal path from the
// block's cached compound index. It is a no-op when the status is nil, already
// has a MerklePath, has no BlockHash, is not MINED/IMMUTABLE, or the BUMP
// cannot be fetched/parsed — best-effort, never a delivery gate. fetch is only
// invoked on a cache miss and must return the block's raw compound BUMP.
func (c *Cache) Enrich(status *models.TransactionStatus, fetch func() ([]byte, error)) {
	if status == nil || len(status.MerklePath) > 0 || status.BlockHash == "" {
		return
	}
	if status.Status != models.StatusMined && status.Status != models.StatusImmutable {
		return
	}
	idx := c.index(status.BlockHash, fetch)
	if idx == nil {
		return
	}
	status.MerklePath = idx.MinimalPathBytes(status.TxID)
}

// MinimalPath returns txid's BRC-74 minimal path from blockHash's compound
// BUMP, or nil when the BUMP cannot be fetched/parsed or the txid is not a
// level-0 leaf. Unlike Enrich this is not tied to a status row's CURRENT
// anchor — it resolves a proof against any retained block, which is how
// orphaned-anchor proofs are served after a reorg re-anchor (issue #279).
// fetch is only invoked on a cache miss.
func (c *Cache) MinimalPath(blockHash, txid string, fetch func() ([]byte, error)) []byte {
	if blockHash == "" || txid == "" {
		return nil
	}
	idx := c.index(blockHash, fetch)
	if idx == nil {
		return nil
	}
	return idx.MinimalPathBytes(txid)
}

// Remove invalidates a block's cached index. Called by InsertBUMP: a rebuild
// can overwrite the stored compound for an existing block (e.g. late STUMP
// callbacks), so the next enrichment must re-fetch and re-parse.
//
// Dropping the entry is not enough on its own. A fill for the same block can
// already be in flight, holding bytes it read from the store BEFORE the
// rebuild landed; without this it would add them after this call and serve the
// superseded compound to every later reader until the next write or eviction.
// Advancing the block's generation invalidates the token held by every fill
// that has REGISTERED for this block, so none of them can install.
//
// Fills that have not registered yet need no bump, and that is the invariant
// holding this together: beginFill runs before the fetch, so a fill with no
// registration has not read the store either and its read will see
// post-rebuild bytes. Registering after the fetch instead would silently
// reintroduce the stale install this guard exists to stop — a fill could read
// old bytes, miss the bump because it was not yet counted, and install them.
// TestFill_RegistersBeforeItReadsTheStore pins the ordering.
//
// Forget releases the singleflight slot as well, so a reader that misses the
// cache after this call starts its own fetch instead of joining the in-flight
// one and being handed pre-rebuild bytes it could have missed entirely. That
// is what makes two fills for one block overlap, which is why the generation
// is taken per fill rather than kept as one flag per block.
//
// It is deliberately taken before mu, and therefore covers no reader already
// inside this call: one that looks up between the Forget and the lru.Remove
// still finds the pre-rebuild entry and is served it. That is the ordinary
// read-raced-a-write outcome. Taking Forget after the entry is dropped would
// trade it for a worse one — that reader would miss the cache and join the
// pre-rebuild flight instead, which is the case Forget is here to prevent.
func (c *Cache) Remove(blockHash string) {
	c.group.Forget(blockHash)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Remove(blockHash)
	// Only while a fill is registered: with none out there is no token to
	// invalidate (see above), and not writing keeps the map to live fills.
	if c.inflight[blockHash] > 0 {
		c.gen[blockHash]++
	}
}

// Contains reports whether a block's index is currently cached, without
// updating recency. Exposed for tests.
func (c *Cache) Contains(blockHash string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Contains(blockHash)
}

// index returns the block's compound index, loading it via fetch on a miss.
// Concurrent misses for the same block share one fetch+parse; every caller
// gets the same shared, read-only index. Returns nil when the BUMP is
// unavailable or unparseable (the error is not cached — the next call
// retries).
func (c *Cache) index(blockHash string, fetch func() ([]byte, error)) *bump.CompoundIndex {
	if idx, ok := c.lookup(blockHash); ok {
		return idx
	}
	v, err, _ := c.group.Do(blockHash, func() (any, error) {
		// Re-check under the flight: a previous leader may have populated
		// the cache between our miss and joining the group.
		if idx, ok := c.lookup(blockHash); ok {
			return idx, nil
		}
		token := c.beginFill(blockHash)
		// Deregistering is deferred so it also runs when fetch or the parse
		// panics: singleflight re-panics to the caller, and a fill left
		// registered would pin its map entries for the life of the process
		// and make every later Remove for that block bump a generation no
		// one holds. built stays nil unless there is something to install.
		var built *bump.CompoundIndex
		defer func() { c.endFill(blockHash, built, token) }()

		data, err := fetch()
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return (*bump.CompoundIndex)(nil), nil
		}
		idx, err := bump.IndexCompound(data)
		if err != nil {
			return nil, err
		}
		// Returned either way: these bytes were current when this fetch
		// began, which is all a read that raced a write can promise. Only
		// CACHING them is withheld when an invalidation landed in the
		// meantime, because a cached stale compound outlives the race it
		// came from.
		built = idx
		return idx, nil
	})
	if err != nil {
		return nil
	}
	idx, _ := v.(*bump.CompoundIndex)
	return idx
}

func (c *Cache) lookup(blockHash string) (*bump.CompoundIndex, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Get(blockHash)
}

// beginFill registers a fill and returns the block's current generation as its
// token. An invalidation that happened BEFORE this call is already reflected in
// what the fetch is about to read, which is why the token is the generation as
// it stands now rather than a reset of it.
func (c *Cache) beginFill(blockHash string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflight[blockHash]++
	return c.gen[blockHash]
}

// endFill deregisters a fill and installs idx unless the block was invalidated
// while it ran — that is, unless its token is stale. A nil idx just
// deregisters. Both maps are pruned once a block's last fill ends, which is
// safe precisely because no token for it can still be outstanding.
//
// Inserting enforces the leaf budget; replacing an existing key removes it
// first so the evict callback keeps totalLeaves exact (the LRU's in-place
// update path skips the callback).
func (c *Cache) endFill(blockHash string, idx *bump.CompoundIndex, token uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stale := c.gen[blockHash] != token
	c.inflight[blockHash]--
	if c.inflight[blockHash] <= 0 {
		delete(c.inflight, blockHash)
		delete(c.gen, blockHash)
	}
	if stale || idx == nil {
		return
	}
	c.lru.Remove(blockHash)
	c.lru.Add(blockHash, idx)
	c.totalLeaves += idx.Leaves()
	for c.totalLeaves > c.maxLeaves && c.lru.Len() > 1 {
		c.lru.RemoveOldest()
	}
}
