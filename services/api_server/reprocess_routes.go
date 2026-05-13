package api_server

import (
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/merkleservice"
)

// blockHashHexLen is the length of a Bitcoin block hash in hex (32 bytes × 2).
const blockHashHexLen = 64

// validBlockHash returns true when s is exactly 64 lowercase-or-uppercase hex
// characters. Used to reject obviously malformed inputs at the API edge before
// they cost a round-trip to merkle-service.
func validBlockHash(s string) bool {
	if len(s) != blockHashHexLen {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// handleReprocessBlock asks merkle-service to re-emit STUMP + BLOCK_PROCESSED
// callbacks for the given block hash. The callbacks land back at arcade's
// /api/v1/merkle-service/callback receiver, so the configured CallbackURL +
// CallbackToken are forwarded as part of the request.
//
// Status mapping:
//   - 202 on the expected ack from merkle-service.
//   - 503 when no merkle-service client is configured (deployment without a
//     /reprocess target).
//   - 502 when merkle-service responds 5xx — transient infrastructure failure,
//     caller can retry.
//   - 422 when merkle-service responds 4xx — typically "block isn't on the
//     consensus chain"; retrying without changing input won't help.
//   - 500 for transport-level failures (network, marshaling).
func (s *Server) handleReprocessBlock(c *gin.Context) {
	hash := c.Param("blockHash")
	if hash == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "blockHash is required"})
		return
	}
	if !validBlockHash(hash) {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "blockHash must be 64 hex characters"})
		return
	}
	if s.merkleClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "merkle service not configured"})
		return
	}

	err := s.merkleClient.Reprocess(c.Request.Context(), hash, s.cfg.CallbackURL, s.cfg.CallbackToken)
	if err == nil {
		c.JSON(http.StatusAccepted, gin.H{"status": "accepted", "blockHash": hash})
		return
	}

	var fail *merkleservice.ReprocessError
	if errors.As(err, &fail) {
		s.logger.Warn("reprocess rejected by merkle-service",
			zap.String("block_hash", hash),
			zap.Int("upstream_status", fail.StatusCode),
			zap.String("upstream_body", fail.Body),
		)
		// 5xx from merkle-service → 502 (bad gateway); 4xx → 422 (we relayed
		// a request the upstream refused on its merits).
		status := http.StatusBadGateway
		if fail.StatusCode >= 400 && fail.StatusCode < 500 {
			status = http.StatusUnprocessableEntity
		}
		c.JSON(status, gin.H{
			jsonKeyError:      "merkle service rejected reprocess",
			"upstreamStatus":  fail.StatusCode,
			"upstreamMessage": fail.Body,
		})
		return
	}

	s.logger.Error("reprocess transport failure",
		zap.String("block_hash", hash),
		zap.Error(err),
	)
	c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to contact merkle service"})
}
