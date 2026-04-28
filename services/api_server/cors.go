package api_server

import "net/http"

// withCORS wraps an http.Handler so every response carries CORS headers and
// OPTIONS preflight requests are answered directly with 204 — without ever
// reaching Gin's router. This matters because Gin only registers concrete
// (method, path) pairs: a browser's `OPTIONS /tx` preflight would otherwise
// 404 when only `POST /tx` is declared, and the middleware would never run.
//
// Arcade exposes a public broadcast/status API reached from browser-based
// wallets and explorers across many origins, so the default policy is "allow
// any origin" with no credentials. The wildcard origin is safe here because
// auth is Bearer-token based; it cannot be combined with
// Access-Control-Allow-Credentials, which we don't need.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Last-Event-ID, X-Requested-With, X-CallbackURL, X-FullStatusUpdates")
		h.Set("Access-Control-Expose-Headers", "Content-Type")
		h.Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
