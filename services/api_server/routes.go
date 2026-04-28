package api_server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type RouteDoc struct {
	Method         string
	Path           string
	Description    string
	RequestFormat  string
	ResponseFormat string
}

var routeDocs = []RouteDoc{
	{
		Method:         "GET",
		Path:           "/health",
		Description:    "Health check",
		RequestFormat:  "None",
		ResponseFormat: `{"status": "ok"}`,
	},
	{
		Method:         "GET",
		Path:           "/ready",
		Description:    "Readiness check",
		RequestFormat:  "None",
		ResponseFormat: `{"status": "ready"}`,
	},
	{
		Method:         "GET",
		Path:           "/tx/:txid",
		Description:    "Get transaction status by txid",
		RequestFormat:  "Path param: txid",
		ResponseFormat: `{"txid", "txStatus", "status", "timestamp", "blockHash", "blockHeight", "merklePath", "extraInfo", "competingTxs"}`,
	},
	{
		Method:         "POST",
		Path:           "/tx",
		Description:    "Submit a transaction",
		RequestFormat:  "application/octet-stream (raw), text/plain (hex), or JSON {\"rawTx\": \"<hex>\"}",
		ResponseFormat: `{"status": "submitted"} (202 Accepted)`,
	},
	{
		Method:         "POST",
		Path:           "/txs",
		Description:    "Submit a batch of transactions",
		RequestFormat:  "application/octet-stream (concatenated raw tx bytes)",
		ResponseFormat: `{"submitted": N} (200 OK)`,
	},
	{
		Method:         "POST",
		Path:           "/api/v1/merkle-service/callback",
		Description:    "Receive callbacks from Merkle Service",
		RequestFormat:  "JSON CallbackMessage with type field. Optional Bearer token auth.",
		ResponseFormat: "200 OK",
	},
}

func (s *Server) registerRoutes(r *gin.Engine) {
	r.GET("/", s.handleDocs)

	r.GET("/health", s.handleHealth)
	r.GET("/ready", s.handleReady)
	// Prometheus scrape endpoint. Mounted on the API server (in addition to
	// the per-pod health server) so api-server-only deployments still expose
	// metrics.
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.POST("/api/v1/merkle-service/callback", s.handleCallback)
	r.GET("/tx/:txid", s.handleGetTransaction)
	r.POST("/tx", s.handleSubmitTransaction)
	r.POST("/txs", s.handleSubmitTransactions)

	s.registerChaintracksRoutes(r)
}

// registerChaintracksRoutes mounts the embedded go-chaintracks HTTP API under
// /chaintracks/v1 and /chaintracks/v2, plus a static-file handler for bulk
// header downloads at /chaintracks/<file>. Routes are only mounted when
// chaintracks was initialized (ChaintracksServer.Enabled=true).
//
// The static handler is registered LAST so the v1/v2 API prefixes win Gin's
// radix-tree lookup. Static serving is done via a catch-all under
// /chaintracks-files/ that we redirect-forward to http.ServeFile so Range
// requests work for the large header files.
func (s *Server) registerChaintracksRoutes(r *gin.Engine) {
	if s.ctRoutes == nil {
		return
	}
	v2 := r.Group("/chaintracks/v2")
	s.ctRoutes.Register(v2)
	v1 := r.Group("/chaintracks/v1")
	s.ctRoutes.RegisterLegacy(v1)

	// Static file serving for bulk header downloads. Uses http.ServeFile
	// (via Gin's File) which supports Range requests out of the box.
	storagePath := s.cfg.Chaintracks.StoragePath
	r.GET("/chaintracks/:file", func(c *gin.Context) {
		file := c.Param("file")
		// Guard against path traversal: reject names containing '/' or '..'.
		if file == "" || file == "v1" || file == "v2" || containsUnsafePathChars(file) {
			c.Status(http.StatusNotFound)
			return
		}
		c.File(storagePath + "/" + file)
	})
}

// containsUnsafePathChars rejects filenames that could escape the storage
// directory. Header files are flat names like `mainNet_0.headers`, so any
// slash or dot-dot is unsafe.
func containsUnsafePathChars(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' || s[i] == '\\' {
			return true
		}
	}
	// Reject parent-dir traversal.
	if s == ".." || len(s) >= 3 && s[:3] == "../" {
		return true
	}
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '.' && s[i+1] == '.' {
			return true
		}
	}
	return false
}
