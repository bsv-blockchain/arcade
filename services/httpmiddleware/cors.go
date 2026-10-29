// Package httpmiddleware holds HTTP middleware shared across BSV Broadcaster services.
// It exists because in microservice deployments each service (api-server,
// sse, …) runs in its own pod with its own http.Server, so per-service
// middleware copies would drift out of sync.
package httpmiddleware

import "net/http"

// WithCORS wraps an http.Handler so every response carries CORS headers and
// OPTIONS preflight requests are answered directly with 204 — without ever
// reaching gin's router. This matters because gin only registers concrete
// (method, path) pairs: a browser's `OPTIONS /tx` preflight would otherwise
// 404 when only `POST /tx` is declared, and the middleware would never run.
//
// BSV Broadcaster exposes a public broadcast/status API reached from browser-based
// wallets and explorers across many origins, so the default policy is "allow
// any origin" with no credentials. The wildcard origin is safe here because
// auth is Bearer-token based; it cannot be combined with
// Access-Control-Allow-Credentials, which we don't need.
//
// The SSE service uses the same wrapper. In microservice deployments
// (mode=sse) the SSE pod serves /events on its own HTTP listener, so the
// api-server's CORS handling isn't in the request path; wrapping the SSE
// router here is what lets EventSource clients from third-party origins
// connect at all.
func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Last-Event-ID, X-Requested-With, X-CallbackURL, X-FullStatusUpdates, X-CallbackToken")
		// Browser clients need the rate-limit fields to back off correctly.
		h.Set("Access-Control-Expose-Headers", "Content-Type, Retry-After, RateLimit, RateLimit-Policy, X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset, X-RateLimit-Reason")
		h.Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
