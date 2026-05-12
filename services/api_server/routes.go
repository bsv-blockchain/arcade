package api_server

import (
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
		RequestFormat:  "JSON CallbackMessage with type field. Bearer token auth required (Authorization: Bearer <callback_token>).",
		ResponseFormat: "200 OK",
	},
	{
		Method:         "GET",
		Path:           "/api/v1/blocks/processing-status",
		Description:    "List block processing milestones (header seen, BLOCK_PROCESSED received, compound BUMP built) in descending-height order. Pages via ?limit (default 50, max 200) and ?before-height (cursor = lowest height from previous page).",
		RequestFormat:  "Optional query: limit, before-height",
		ResponseFormat: `{"blocks": [{"blockHash", "blockHeight", "headerSeenAt", "processedAt", "bumpBuiltAt", "status", "orphanedAt", "hasBlockProcessed", "hasCompoundBUMP"}], "nextCursor"}`,
	},
	{
		Method:         "GET",
		Path:           "/api/v1/blocks/processing-status/:blockHash",
		Description:    "Get block processing milestones for a single block by hash.",
		RequestFormat:  "Path param: blockHash",
		ResponseFormat: `{"blockHash", "blockHeight", "headerSeenAt", "processedAt", "bumpBuiltAt", "status", "orphanedAt", "hasBlockProcessed", "hasCompoundBUMP"}`,
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
	r.GET("/api/v1/blocks/processing-status", s.handleListBlockProcessingStatus)
	r.GET("/api/v1/blocks/processing-status/:blockHash", s.handleGetBlockProcessingStatus)
	r.GET("/tx/:txid", s.handleGetTransaction)
	r.POST("/tx", s.handleSubmitTransaction)
	r.POST("/txs", s.handleSubmitTransactions)
}
