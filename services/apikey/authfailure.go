package apikey

import (
	"net/netip"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/time/rate"
)

// Limits on the per-client-IP invalid-key throttle. A caller guessing or
// replaying leaked keys can otherwise hammer Resolve -- and, once its
// negative-cache entries age out, the store -- at whatever rate it likes;
// this bounds that to one bucket per source IP, independent of the
// resolver's own caches.
const (
	authFailureBucketSize     = 100_000
	authFailureLimitPerMinute = 60
	authFailureBurst          = 60
)

// authFailureLimiter is a per-client-IP token bucket counting invalid-key
// attempts (malformed, unknown, revoked or expired keys). It is unrelated to
// the quota rate-limiting in services/ratelimit: it exists only to stop a
// flood of bad credentials from one IP, not to meter traffic.
type authFailureLimiter struct {
	mu      sync.Mutex
	buckets *lru.Cache[string, *rate.Limiter]
	perMin  float64
	burst   int
}

// newAuthFailureLimiter builds a limiter with its own LRU of size entries,
// allowing perMinute failures per IP with the given burst. Tests pass a
// small size/perMinute/burst to trip the throttle in a handful of requests.
func newAuthFailureLimiter(size, perMinute, burst int) *authFailureLimiter {
	c, _ := lru.New[string, *rate.Limiter](size) // errors only for size <= 0
	return &authFailureLimiter{buckets: c, perMin: float64(perMinute), burst: burst}
}

func (a *authFailureLimiter) limiterFor(ip string) *rate.Limiter {
	key := NormalizeClientIP(ip)
	a.mu.Lock()
	defer a.mu.Unlock()
	lim, ok := a.buckets.Get(key)
	if !ok {
		lim = rate.NewLimiter(rate.Limit(a.perMin/60), a.burst)
		a.buckets.Add(key, lim)
	}
	return lim
}

// NormalizeClientIP normalizes ip for per-client accounting shared across
// this package: the invalid-key throttle's buckets here, and the anonymous
// caller's Subject in principal.go. Unmap first, so an IPv4-mapped IPv6
// address (::ffff:a.b.c.d) collapses to the same key as its plain IPv4 form
// rather than being treated as IPv6. What's left as a genuine IPv6 address
// is keyed by its /64 -- the usual ISP/edge allocation boundary -- so one
// client cycling through addresses within its own /64 (privacy extensions,
// CGNAT) shares a single bucket/quota instead of getting a fresh, full one
// per request. IPv4 and anything netip cannot parse are keyed by the
// (unmapped) address string unchanged.
func NormalizeClientIP(ip string) string {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	addr = addr.Unmap()
	if addr.Is6() {
		if prefix, err := addr.Prefix(64); err == nil {
			return prefix.String()
		}
	}
	return addr.String()
}

// check reports the tokens currently available for ip without consuming
// any, plus (when fewer than one is available) how long until one is.
//
// check and fail are not atomic together: a burst of concurrent requests
// from the same IP can all observe tokens >= 1 here before any of them
// calls fail, so the bucket can be overdrawn by up to that burst's
// concurrency. That overshoot is bounded by how many requests from one IP
// the server runs at once, not by anything in this limiter, and is judged
// an acceptable trade for not serializing every request behind one lock
// spanning both the check and the eventual outcome.
func (a *authFailureLimiter) check(ip string, now time.Time) (tokens float64, retryAfter time.Duration) {
	tokens = a.limiterFor(ip).TokensAt(now)
	if tokens >= 1 {
		return tokens, 0
	}
	wait := (1 - tokens) / (a.perMin / 60) * float64(time.Second)
	return tokens, time.Duration(wait)
}

// fail records one invalid-key attempt from ip, consuming a token.
func (a *authFailureLimiter) fail(ip string, now time.Time) {
	a.limiterFor(ip).AllowN(now, 1)
}
