package ssrfguard

import (
	"sync"
	"time"
)

// SuccessCache is a bounded, TTL'd cache for successful URL validations.
// Only successes belong in it — rejections must always re-validate so a
// peer that fixes its DNS or config is re-accepted immediately.
//
// The bound matters because cache keys originate from untrusted p2p
// announcements: without a cap, a peer announcing many unique-but-valid
// URLs (each cached for the TTL) could grow the map without limit for the
// life of the process. At capacity, Put first sweeps expired entries; if
// the cache is still full the new entry is simply not cached — validation
// still happens, callers just lose the caching benefit under churn, which
// is the safe failure mode.
type SuccessCache struct {
	ttl time.Duration
	max int

	mu      sync.Mutex
	entries map[string]successEntry
}

type successEntry struct {
	value  string
	expiry time.Time
}

// NewSuccessCache returns a cache holding at most maxEntries values for up
// to ttl each. maxEntries must be > 0.
func NewSuccessCache(ttl time.Duration, maxEntries int) *SuccessCache {
	return &SuccessCache{
		ttl:     ttl,
		max:     maxEntries,
		entries: make(map[string]successEntry),
	}
}

// Get returns the cached value for key if present and unexpired. Expired
// entries are deleted on access.
func (c *SuccessCache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return "", false
	}
	if time.Now().After(e.expiry) {
		delete(c.entries, key)
		return "", false
	}
	return e.value, true
}

// Put caches value under key for the cache TTL. At capacity it sweeps
// expired entries first; if still full, the entry is dropped (see type doc).
func (c *SuccessCache) Put(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.max {
		now := time.Now()
		for k, e := range c.entries {
			if now.After(e.expiry) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.max {
			return
		}
	}
	c.entries[key] = successEntry{value: value, expiry: time.Now().Add(c.ttl)}
}
