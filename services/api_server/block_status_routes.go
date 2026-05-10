package api_server

import (
	"errors"
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

const (
	blockStatusListDefaultLimit = 50
	blockStatusListMaxLimit     = 200
)

type blockProcessingStatusResponse struct {
	BlockHash         string                            `json:"blockHash"`
	BlockHeight       uint64                            `json:"blockHeight"`
	HeaderSeenAt      string                            `json:"headerSeenAt,omitempty"`
	ProcessedAt       string                            `json:"processedAt,omitempty"`
	BUMPBuiltAt       string                            `json:"bumpBuiltAt,omitempty"`
	Status            models.BlockProcessingStatusValue `json:"status"`
	OrphanedAt        string                            `json:"orphanedAt,omitempty"`
	HasBlockProcessed bool                              `json:"hasBlockProcessed"`
	HasCompoundBUMP   bool                              `json:"hasCompoundBUMP"`
}

type listBlockProcessingStatusResponse struct {
	Blocks     []blockProcessingStatusResponse `json:"blocks"`
	NextCursor *uint64                         `json:"nextCursor,omitempty"`
}

func toBlockProcessingResponse(bp *models.BlockProcessingStatus) blockProcessingStatusResponse {
	resp := blockProcessingStatusResponse{
		BlockHash:         bp.BlockHash,
		BlockHeight:       bp.BlockHeight,
		Status:            bp.Status,
		HasBlockProcessed: bp.ProcessedAt != nil,
		HasCompoundBUMP:   bp.BUMPBuiltAt != nil,
	}
	if !bp.HeaderSeenAt.IsZero() {
		resp.HeaderSeenAt = bp.HeaderSeenAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	}
	if bp.ProcessedAt != nil {
		resp.ProcessedAt = bp.ProcessedAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	}
	if bp.BUMPBuiltAt != nil {
		resp.BUMPBuiltAt = bp.BUMPBuiltAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	}
	if bp.OrphanedAt != nil {
		resp.OrphanedAt = bp.OrphanedAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	}
	return resp
}

// handleListBlockProcessingStatus paginates block_processing rows in
// descending-height order. Cursor is the lowest height returned on the
// previous page; pass it as ?before-height=<n> to fetch the next page.
func (s *Server) handleListBlockProcessingStatus(c *gin.Context) {
	limit := blockStatusListDefaultLimit
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "limit must be a positive integer"})
			return
		}
		limit = n
	}
	if limit > blockStatusListMaxLimit {
		limit = blockStatusListMaxLimit
	}

	var beforeHeight uint64
	if v := c.Query("before-height"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "before-height must be a non-negative integer"})
			return
		}
		if n > math.MaxInt64 {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "before-height is too large"})
			return
		}
		beforeHeight = n
	}

	// Fetch limit+1 so we can detect a next page without a second query.
	rows, err := s.store.ListBlockProcessingStatus(c.Request.Context(), beforeHeight, limit+1)
	if err != nil {
		s.logger.Error("list block_processing", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to list block processing status"})
		return
	}

	resp := listBlockProcessingStatusResponse{Blocks: make([]blockProcessingStatusResponse, 0, len(rows))}
	if len(rows) > limit {
		next := rows[limit-1].BlockHeight
		resp.NextCursor = &next
		rows = rows[:limit]
	}
	for _, r := range rows {
		resp.Blocks = append(resp.Blocks, toBlockProcessingResponse(r))
	}
	c.JSON(http.StatusOK, resp)
}

// handleGetBlockProcessingStatus returns the row for one block hash, or 404
// if no row exists.
func (s *Server) handleGetBlockProcessingStatus(c *gin.Context) {
	hash := c.Param("blockHash")
	if hash == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "blockHash is required"})
		return
	}
	bp, err := s.store.GetBlockProcessingStatus(c.Request.Context(), hash)
	if errors.Is(err, store.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{jsonKeyError: "block processing status not found"})
		return
	}
	if err != nil {
		s.logger.Error("get block_processing", zap.String("block_hash", hash), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to fetch block processing status"})
		return
	}
	c.JSON(http.StatusOK, toBlockProcessingResponse(bp))
}
