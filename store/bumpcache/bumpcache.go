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

	// mu guards lru and totalLeaves. The LRU's evict callback adjusts
	// totalLeaves and runs synchronously inside mutations, so it MUST NOT
	// take mu itself — every lru call site below already holds it.
	mu          sync.Mutex
	lru         *simplelru.LRU[string, *bump.CompoundIndex]
	totalLeaves int
}

// New constructs an empty cache with the package's production bounds.
func New() *Cache {
	return newWithLimits(maxEntries, maxTotalLeaves)
}

// newWithLimits is the constructor proper, split out so tests can exercise
// eviction without building half-million-leaf compounds.
func newWithLimits(entries, leaves int) *Cache {
	c := &Cache{maxLeaves: leaves}
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

// Remove invalidates a block's cached index. Called by InsertBUMP: a rebuild
// can overwrite the stored compound for an existing block (e.g. late STUMP
// callbacks), so the next enrichment must re-fetch and re-parse.
func (c *Cache) Remove(blockHash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Remove(blockHash)
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
		c.add(blockHash, idx)
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

// add inserts an entry and enforces the leaf budget. Replacing an existing
// key removes it first so the evict callback keeps totalLeaves exact (the
// LRU's in-place update path skips the callback).
func (c *Cache) add(blockHash string, idx *bump.CompoundIndex) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Remove(blockHash)
	c.lru.Add(blockHash, idx)
	c.totalLeaves += idx.Leaves()
	for c.totalLeaves > c.maxLeaves && c.lru.Len() > 1 {
		c.lru.RemoveOldest()
	}
}
