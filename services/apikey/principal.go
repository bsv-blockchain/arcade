package apikey

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
)

const ctxPrincipal = "apikey.principal"

// Principal is the identified caller of a request.
type Principal struct {
	KeyID    string // "" for anonymous callers
	Tier     string // the key's tier; config.TierAnonymous for anonymous callers
	ClientIP string
}

// Anonymous reports whether the caller presented no key.
func (p Principal) Anonymous() bool { return p.KeyID == "" }

// Subject identifies the principal for per-caller accounting: the anonymous
// quota bucket, the fail-open bucket, and the in-flight concurrency map.
// Anonymous callers are keyed by NormalizeClientIP(ClientIP), not the raw
// address -- an IPv6 client rotating addresses within its own /64 (privacy
// extensions, CGNAT) must share one bucket, not get a fresh one on every
// rotation. ClientIP itself stays raw; request logs (client_ip) still
// report the exact address the request arrived from.
func (p Principal) Subject() string {
	if p.Anonymous() {
		return "ip:" + NormalizeClientIP(p.ClientIP)
	}
	return "key:" + p.KeyID
}

// WithPrincipal stores p on the request. Middleware calls it; tests use it
// to stand in for Middleware.
func WithPrincipal(c *gin.Context, p Principal) { c.Set(ctxPrincipal, p) }

// PrincipalFrom returns the request's principal. Without one (identify
// middleware not installed on this route) the caller is anonymous.
func PrincipalFrom(c *gin.Context) Principal {
	if v, ok := c.Get(ctxPrincipal); ok {
		if p, ok := v.(Principal); ok {
			return p
		}
	}
	return Principal{Tier: config.TierAnonymous, ClientIP: c.ClientIP()}
}

// LogFields returns key_id and tier for request logs when the caller
// presented a valid key, and nothing otherwise. Never the key itself.
func LogFields(c *gin.Context) []zap.Field {
	v, ok := c.Get(ctxPrincipal)
	if !ok {
		return nil
	}
	p, ok := v.(Principal)
	if !ok || p.Anonymous() {
		return nil
	}
	return []zap.Field{zap.String("key_id", p.KeyID), zap.String("tier", p.Tier)}
}
