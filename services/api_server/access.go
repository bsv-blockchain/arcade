package api_server

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/services/apikey"
	"github.com/bsv-blockchain/arcade/services/ratelimit"
	"github.com/bsv-blockchain/arcade/store"
)

// initAccessControl builds the key resolver and rate limiter the config
// asks for. Every backend implements APIKeyStore and QuotaStore; the
// assertions keep a test double that does not from silently disabling auth.
//
// The identify middleware is built exactly once here, not per route: it
// owns a per-client-IP invalid-key throttle (see apikey.Middleware), and a
// fresh apikey.Middleware(...) call creates a fresh throttle with its own
// budget. Calling it once per guarded route would give a caller guessing
// keys one throttle budget per route instead of one for the whole API.
func (s *Server) initAccessControl() error {
	if s.cfg.Admin.Enabled {
		if _, ok := s.store.(store.APIKeyStore); !ok {
			return fmt.Errorf("admin.enabled: store backend %q does not support api keys", s.cfg.Store.Backend)
		}
	}
	if !s.cfg.Auth.Enabled {
		return nil
	}
	keys, ok := s.store.(store.APIKeyStore)
	if !ok {
		return fmt.Errorf("auth.enabled: store backend %q does not support api keys", s.cfg.Store.Backend)
	}
	rl := s.cfg.RateLimit
	s.keyResolver = apikey.NewResolver(keys,
		time.Duration(rl.KeyCacheTTLMs)*time.Millisecond,
		time.Duration(rl.KeyCacheStaleMaxMs)*time.Millisecond)
	s.identify = apikey.Middleware(s.keyResolver)
	// Export the apikey-identification series at 0 from the first scrape
	// whenever auth is on, independent of ratelimit.enabled: identification
	// (and its {none,*} decisions and cache results) runs either way.
	metrics.PreRegisterAPIKey()
	if !rl.Enabled {
		return nil
	}
	quota, ok := s.store.(store.QuotaStore)
	if !ok {
		return fmt.Errorf("ratelimit.enabled: store backend %q does not support quotas", s.cfg.Store.Backend)
	}
	s.limiter = ratelimit.New(rl, quota, s.logger)
	return nil
}

// guarded prefixes h with the identify and rate-limit middleware the config
// enables, charging units for the route. With both off it is just h.
func (s *Server) guarded(units int64, h gin.HandlerFunc) []gin.HandlerFunc {
	var hs []gin.HandlerFunc
	if s.identify != nil {
		hs = append(hs, s.identify)
	}
	if s.limiter != nil {
		hs = append(hs, s.limiter.Middleware(units))
	}
	return append(hs, h)
}

// applyTrustedPlatform decides which header c.ClientIP() trusts. Gin's
// default trusts X-Forwarded-For from anyone, which would let a caller pick
// its own anonymous-tier bucket.
//
// api.trusted_proxies names the reverse proxies (CIDRs or bare IPs) allowed
// to set X-Forwarded-For; nil/empty (the default) trusts none, so
// c.ClientIP() is always the TCP peer regardless of any header a caller
// sends. config validation already rejects an unparseable entry, so a
// SetTrustedProxies error here means the config changed after validation —
// trusting nothing is the safe fallback.
//
// CF-Connecting-IP is a separate, narrower trust decision: it is honored
// only under cloudflare, independent of trusted_proxies, because a request
// without it (pod-to-pod) must fall back to the TCP peer, not to a header
// the caller chose.
func (s *Server) applyTrustedPlatform(router *gin.Engine) {
	if err := router.SetTrustedProxies(s.cfg.APIServer.TrustedProxies); err != nil {
		s.logger.Warn("invalid api.trusted_proxies; trusting none", zap.Error(err))
		_ = router.SetTrustedProxies(nil) // nil never errors
	}
	if s.cfg.APIServer.TrustedPlatform == config.TrustedPlatformCloudflare {
		router.TrustedPlatform = gin.PlatformCloudflare
	}
}
