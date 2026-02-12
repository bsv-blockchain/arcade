// Package fiber provides Fiber route registration for arcade.
package fiber

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"

	"github.com/bsv-blockchain/arcade"
	arcerrors "github.com/bsv-blockchain/arcade/errors"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/service"
	"github.com/bsv-blockchain/arcade/store"
)

// Static errors for routes package
var (
	errInvalidTransactionHex = errors.New("invalid transaction hex")
	errInvalidRequestBody    = errors.New("invalid request body")
)

// TransactionRequest represents a transaction submission request.
type TransactionRequest struct {
	RawTx string `json:"rawTx" example:"0100000001..."`
}

// Config holds configuration for the routes.
type Config struct {
	Service        service.ArcadeService
	Store          store.Store      // For health checks and SSE catchup
	EventPublisher events.Publisher // For SSE
	Arcade         *arcade.Arcade   // For dashboard access
	Logger         *slog.Logger
}

// Routes handles HTTP routes for arcade.
type Routes struct {
	service        service.ArcadeService
	store          store.Store
	eventPublisher events.Publisher
	arcade         *arcade.Arcade
	logger         *slog.Logger
}

// NewRoutes creates a new Routes instance.
func NewRoutes(cfg Config) *Routes {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Routes{
		service:        cfg.Service,
		store:          cfg.Store,
		eventPublisher: cfg.EventPublisher,
		arcade:         cfg.Arcade,
		logger:         logger,
	}
}

// Register registers all arcade routes on the given router.
func (r *Routes) Register(router fiber.Router) {
	router.Get("/policy", r.handleGetPolicy)
	router.Post("/tx", r.handlePostTx)
	router.Post("/txs", r.handlePostTxs)
	router.Get("/tx/:txid", r.handleGetTx)
	router.Get("/events", r.handleTxSSE)
}

// handleGetPolicy returns the policy configuration
// @Summary Get policy
// @Description Returns the transaction policy configuration including fee rates and limits
// @Tags arcade
// @Produce json
// @Success 200 {object} models.Policy
// @Router /policy [get]
func (r *Routes) handleGetPolicy(c *fiber.Ctx) error {
	policy, err := r.service.GetPolicy(c.UserContext())
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to get policy"})
	}
	return c.JSON(policy)
}

// handlePostTx submits a single transaction
// @Summary Submit transaction
// @Description Submit a single transaction for broadcast. Accepts raw transaction bytes, hex string, or JSON with rawTx field.
// @Tags arcade
// @Accept json,application/octet-stream,text/plain
// @Produce json
// @Param transaction body TransactionRequest true "Transaction data"
// @Param X-CallbackUrl header string false "URL for status callbacks"
// @Param X-CallbackToken header string false "Token for SSE event filtering"
// @Param X-FullStatusUpdates header string false "Send all status updates (true/false)"
// @Param X-SkipFeeValidation header string false "Skip fee validation (true/false)"
// @Param X-SkipScriptValidation header string false "Skip script validation (true/false)"
// @Success 200 {object} models.TransactionStatus
// @Failure 400 {object} arcerrors.ErrorFields
// @Failure 465 {object} errors.ErrorFields "ARC validation error"
// @Failure 500 {object} map[string]string
// @Router /tx [post]
func (r *Routes) handlePostTx(c *fiber.Ctx) error {
	ctx := c.UserContext()

	// Parse raw transaction from request body
	data, err := r.parseTransactionBody(c)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(arcerrors.NewErrorFields(arcerrors.StatusBadRequest, err.Error()))
	}

	// Extract options from headers
	opts := r.extractSubmitOptions(c)

	// Delegate to service
	status, err := r.service.SubmitTransaction(ctx, data, opts)
	if err != nil {
		return r.handleSubmitError(c, err)
	}

	return c.JSON(status)
}

// handlePostTxs submits multiple transactions
// @Summary Submit multiple transactions
// @Description Submit multiple transactions for broadcast
// @Tags arcade
// @Accept json
// @Produce json
// @Param transactions body []TransactionRequest true "Array of transactions"
// @Param X-CallbackUrl header string false "URL for status callbacks"
// @Param X-CallbackToken header string false "Token for SSE event filtering"
// @Param X-FullStatusUpdates header string false "Send all status updates (true/false)"
// @Param X-SkipFeeValidation header string false "Skip fee validation (true/false)"
// @Param X-SkipScriptValidation header string false "Skip script validation (true/false)"
// @Success 200 {array} models.TransactionStatus
// @Failure 400 {object} arcerrors.ErrorFields
// @Failure 465 {object} errors.ErrorFields "ARC validation error"
// @Router /txs [post]
func (r *Routes) handlePostTxs(c *fiber.Ctx) error {
	ctx := c.UserContext()

	var reqs []TransactionRequest
	if err := c.BodyParser(&reqs); err != nil {
		return c.Status(http.StatusBadRequest).JSON(arcerrors.NewErrorFields(arcerrors.StatusBadRequest, "Invalid request body"))
	}

	// Convert to raw bytes
	rawTxs := make([][]byte, 0, len(reqs))
	for _, req := range reqs {
		data, err := hex.DecodeString(req.RawTx)
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(arcerrors.NewErrorFields(arcerrors.StatusBadRequest, "Invalid transaction hex"))
		}
		rawTxs = append(rawTxs, data)
	}

	// Extract options from headers
	opts := r.extractSubmitOptions(c)

	// Delegate to service
	statuses, err := r.service.SubmitTransactions(ctx, rawTxs, opts)
	if err != nil {
		return r.handleSubmitError(c, err)
	}

	return c.JSON(statuses)
}

// handleSubmitError returns an appropriate HTTP response for transaction submission errors.
// It checks for ARC-compatible errors and returns the corresponding status code.
func (r *Routes) handleSubmitError(c *fiber.Ctx, err error) error {
	// Check if this is an ARC-typed error
	if arcErr := arcerrors.GetArcError(err); arcErr != nil {
		return c.Status(int(arcErr.StatusCode)).JSON(arcErr.ToErrorFields())
	}

	// Fallback for untyped errors
	errStr := err.Error()
	if strings.Contains(errStr, "validation failed") || strings.Contains(errStr, "failed to parse") {
		return c.Status(http.StatusBadRequest).JSON(arcerrors.NewErrorFields(arcerrors.StatusBadRequest, errStr))
	}

	return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": errStr})
}

// handleGetTx retrieves transaction status
// @Summary Get transaction status
// @Description Get the current status of a submitted transaction
// @Tags arcade
// @Produce json
// @Param txid path string true "Transaction ID"
// @Success 200 {object} models.TransactionStatus
// @Failure 404 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /tx/{txid} [get]
func (r *Routes) handleGetTx(c *fiber.Ctx) error {
	status, err := r.service.GetStatus(c.UserContext(), c.Params("txid"))
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Transaction not found"})
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to get status"})
	}
	return c.JSON(status)
}

// handleTxSSE streams transaction status updates via SSE
// @Summary Stream transaction events
// @Description Server-Sent Events stream of transaction status updates. If callbackToken is provided, only events for that token are streamed.
// @Tags arcade
// @Produce text/event-stream
// @Param callbackToken query string false "Callback token from transaction submission"
// @Success 200 {string} string "SSE stream of transaction status updates"
// @Router /events [get]
func (r *Routes) handleTxSSE(c *fiber.Ctx) error {
	callbackToken := c.Query("callbackToken")

	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	ctx := c.UserContext()
	lastEventID := c.Get("Last-Event-ID")

	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		// Handle catchup from last event ID
		if lastEventID != "" && callbackToken != "" {
			r.sendTxSSECatchup(ctx, w, callbackToken, lastEventID)
		}

		// Subscribe to status updates via service interface
		eventChan, err := r.service.Subscribe(ctx, callbackToken)
		if err != nil {
			r.logger.Error("failed to subscribe to status updates", slog.String("error", err.Error()))
			return
		}
		defer r.service.Unsubscribe(eventChan)

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventChan:
				if !ok {
					return
				}
				_, _ = fmt.Fprintf(w, "id: %d\n", event.Timestamp.UnixNano())
				_, _ = fmt.Fprintf(w, "event: status\n")
				_, _ = fmt.Fprintf(w, "data: {\"txid\":\"%s\",\"txStatus\":\"%s\",\"timestamp\":\"%s\"}\n\n",
					event.TxID, event.Status, event.Timestamp.Format(time.RFC3339))
				_ = w.Flush()
			}
		}
	}))

	return nil
}

// HandleGetHealth returns the health status
// @Summary Health check
// @Description Returns the health status of the service
// @Tags arcade
// @Produce text/plain
// @Success 200 {string} string "OK"
// @Failure 503 {string} string "Service Unavailable"
// @Router /health [get]
func (r *Routes) HandleGetHealth(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), 2*time.Second)
	defer cancel()

	// Health check: verify database is reachable by executing a query
	// ErrNotFound is expected for non-existent txid and indicates healthy database
	// Any other error indicates database connectivity issues
	if _, err := r.store.GetStatus(ctx, "health-check-probe"); err != nil && !errors.Is(err, store.ErrNotFound) {
		return c.Status(http.StatusServiceUnavailable).SendString("Database connection failed")
	}

	return c.SendString("OK")
}

// parseTransactionBody extracts raw transaction bytes from the request body.
func (r *Routes) parseTransactionBody(c *fiber.Ctx) ([]byte, error) {
	switch c.Get("Content-Type") {
	case "application/octet-stream":
		return c.Body(), nil
	case "text/plain":
		data, err := hex.DecodeString(string(c.Body()))
		if err != nil {
			return nil, errInvalidTransactionHex
		}
		return data, nil
	default:
		var req TransactionRequest
		if err := c.BodyParser(&req); err != nil {
			return nil, errInvalidRequestBody
		}
		data, err := hex.DecodeString(req.RawTx)
		if err != nil {
			return nil, errInvalidTransactionHex
		}
		return data, nil
	}
}

// extractSubmitOptions extracts SubmitOptions from HTTP headers.
func (r *Routes) extractSubmitOptions(c *fiber.Ctx) *models.SubmitOptions {
	return &models.SubmitOptions{
		CallbackURL:          c.Get("X-CallbackUrl"),
		CallbackToken:        c.Get("X-CallbackToken"),
		FullStatusUpdates:    c.Get("X-FullStatusUpdates") == "true",
		SkipFeeValidation:    c.Get("X-SkipFeeValidation") == "true",
		SkipScriptValidation: c.Get("X-SkipScriptValidation") == "true",
	}
}

// sendTxSSECatchup sends any status updates since the last event ID.
func (r *Routes) sendTxSSECatchup(ctx context.Context, w *bufio.Writer, callbackToken, lastEventID string) {
	sinceNS, err := strconv.ParseInt(lastEventID, 10, 64)
	if err != nil {
		return
	}
	since := time.Unix(0, sinceNS)

	submissions, err := r.store.GetSubmissionsByToken(ctx, callbackToken)
	if err != nil {
		return
	}

	for _, sub := range submissions {
		status, err := r.store.GetStatus(ctx, sub.TxID)
		if err != nil || status == nil || !status.Timestamp.After(since) {
			continue
		}
		_, _ = fmt.Fprintf(w, "id: %d\n", status.Timestamp.UnixNano())
		_, _ = fmt.Fprintf(w, "event: status\n")
		_, _ = fmt.Fprintf(w, "data: {\"txid\":\"%s\",\"txStatus\":\"%s\",\"timestamp\":\"%s\"}\n\n",
			status.TxID, status.Status, status.Timestamp.Format(time.RFC3339))
	}
	_ = w.Flush()
}

// GetArcade returns the arcade instance for use by dashboard or other handlers.
func (r *Routes) GetArcade() *arcade.Arcade {
	return r.arcade
}
