package ratelimit

import "sync"

// inflight counts concurrent requests per subject on this replica.
type inflight struct {
	mu sync.Mutex
	n  map[string]int
}

func newInflight() *inflight { return &inflight{n: map[string]int{}} }

func (f *inflight) acquire(subject string, limit int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n[subject] >= limit {
		return false
	}
	f.n[subject]++
	return true
}

func (f *inflight) release(subject string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n[subject] <= 1 {
		delete(f.n, subject)
		return
	}
	f.n[subject]--
}
