// Package fiber provides Fiber route registration for arcade.
package fiber

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/bsv-blockchain/arcade"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
)

// TransactionRequest represents a transaction submission request.
type TransactionRequest struct {
	RawTx string `json:"rawTx" example:"0100000001..."`
}

// PolicyResponse represents the policy configuration.
type PolicyResponse struct {
	MaxScriptSizePolicy     uint64    `json:"maxscriptsizepolicy"`
	MaxTxSigOpsCountsPolicy uint64    `json:"maxtxsigopscountspolicy"`
	MaxTxSizePolicy         uint64    `json:"maxtxsizepolicy"`
	MiningFee               FeeAmount `json:"miningFee"`
}

// FeeAmount represents fee amount in bytes and satoshis.
type FeeAmount struct {
	Bytes    uint64 `json:"bytes"`
	Satoshis uint64 `json:"satoshis"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Healthy bool   `json:"healthy"`
	Reason  string `json:"reason,omitempty"`
	Version string `json:"version,omitempty"`
}

// Config holds configuration for the routes.
type Config struct {
	StatusStore     store.StatusStore
	SubmissionStore store.SubmissionStore
	TxTracker       *store.TxTracker
	EventPublisher  events.Publisher
	TeranodeClient  *teranode.Client
	TxValidator     *validator.Validator
	Arcade          *arcade.Arcade
	Policy          *PolicyResponse
	Logger          *slog.Logger
}

// Routes handles HTTP routes for arcade.
type Routes struct {
	statusStore     store.StatusStore
	submissionStore store.SubmissionStore
	txTracker       *store.TxTracker
	eventPublisher  events.Publisher
	teranodeClient  *teranode.Client
	txValidator     *validator.Validator
	arcade          *arcade.Arcade
	policy          *PolicyResponse
	logger          *slog.Logger
}

// NewRoutes creates a new Routes instance.
func NewRoutes(cfg Config) *Routes {
	return &Routes{
		statusStore:     cfg.StatusStore,
		submissionStore: cfg.SubmissionStore,
		txTracker:       cfg.TxTracker,
		eventPublisher:  cfg.EventPublisher,
		teranodeClient:  cfg.TeranodeClient,
		txValidator:     cfg.TxValidator,
		arcade:          cfg.Arcade,
		policy:          cfg.Policy,
		logger:          cfg.Logger,
	}
}

// Register registers all arcade routes on the given router.
func (r *Routes) Register(router fiber.Router) {
	router.Post("/tx", r.handlePostTx)
	router.Post("/txs", r.handlePostTxs)
	router.Get("/tx/:txid", r.handleGetTx)
	router.Get("/policy", r.handleGetPolicy)
	router.Get("/health", r.handleGetHealth)
	router.Get("/events/:callbackToken", r.handleTxSSE)
}

// handlePostTx submits a single transaction
// @Summary Submit transaction
// @Description Submit a single transaction for broadcast. Accepts raw transaction bytes, hex string, or JSON with rawTx field.
// @Tags transactions
// @Accept json,application/octet-stream,text/plain
// @Produce json
// @Param transaction body TransactionRequest true "Transaction data"
// @Param X-CallbackUrl header string false "URL for status callbacks"
// @Param X-CallbackToken header string false "Token for SSE event filtering"
// @Param X-FullStatusUpdates header string false "Send all status updates (true/false)"
// @Param X-SkipFeeValidation header string false "Skip fee validation (true/false)"
// @Param X-SkipScriptValidation header string false "Skip script validation (true/false)"
// @Success 200 {object} models.TransactionStatus
// @Failure 400 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /tx [post]
func (r *Routes) handlePostTx(c *fiber.Ctx) error {
	ctx := c.UserContext()

	var data []byte
	var err error
	switch c.Get("Content-Type") {
	case "application/octet-stream":
		data = c.Body()
	case "text/plain":
		data, err = hex.DecodeString(string(c.Body()))
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid transaction hex"})
		}
	default:
		var req TransactionRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
		}
		data, err = hex.DecodeString(req.RawTx)
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid transaction hex"})
		}
	}

	_, tx, _, err := sdkTx.ParseBeef(data)
	if err != nil || tx == nil {
		tx, err = sdkTx.NewTransactionFromBytes(data)
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Failed to parse transaction"})
		}
	}

	skipFees := c.Get("X-SkipFeeValidation") == "true"
	skipScripts := c.Get("X-SkipScriptValidation") == "true"
	if err := r.txValidator.ValidateTransaction(ctx, tx, skipFees, skipScripts); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Validation failed: %v", err)})
	}

	txid := tx.TxID().String()

	if err := r.statusStore.InsertStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now(),
	}); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to store status"})
	}

	r.txTracker.Add(txid, models.StatusReceived)

	callbackURL := c.Get("X-CallbackUrl")
	callbackToken := c.Get("X-CallbackToken")
	if callbackURL != "" || callbackToken != "" {
		if err := r.submissionStore.InsertSubmission(ctx, &models.Submission{
			SubmissionID:      uuid.New().String(),
			TxID:              txid,
			CallbackURL:       callbackURL,
			CallbackToken:     callbackToken,
			FullStatusUpdates: c.Get("X-FullStatusUpdates") == "true",
			CreatedAt:         time.Now(),
		}); err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to store submission"})
		}
	}

	for _, endpoint := range r.teranodeClient.GetEndpoints() {
		go r.submitToTeranode(ctx, endpoint, tx.Bytes(), txid)
	}

	status, err := r.statusStore.GetStatus(ctx, txid)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to get status"})
	}

	return c.JSON(status)
}

// handlePostTxs submits multiple transactions
// @Summary Submit multiple transactions
// @Description Submit multiple transactions for broadcast
// @Tags transactions
// @Accept json
// @Produce json
// @Param transactions body []TransactionRequest true "Array of transactions"
// @Param X-SkipFeeValidation header string false "Skip fee validation (true/false)"
// @Param X-SkipScriptValidation header string false "Skip script validation (true/false)"
// @Success 200 {array} models.TransactionStatus
// @Failure 400 {object} map[string]string
// @Router /txs [post]
func (r *Routes) handlePostTxs(c *fiber.Ctx) error {
	ctx := c.UserContext()

	var reqs []TransactionRequest
	if err := c.BodyParser(&reqs); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	skipFees := c.Get("X-SkipFeeValidation") == "true"
	skipScripts := c.Get("X-SkipScriptValidation") == "true"

	var txs []*sdkTx.Transaction
	for _, req := range reqs {
		data, err := hex.DecodeString(req.RawTx)
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid transaction hex"})
		}

		_, tx, _, err := sdkTx.ParseBeef(data)
		if err != nil || tx == nil {
			tx, err = sdkTx.NewTransactionFromBytes(data)
			if err != nil {
				return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Failed to parse transaction"})
			}
		}

		if err := r.txValidator.ValidateTransaction(ctx, tx, skipFees, skipScripts); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Validation failed: %v", err)})
		}

		txs = append(txs, tx)
		txid := tx.TxID().String()
		r.statusStore.InsertStatus(ctx, &models.TransactionStatus{
			TxID:      txid,
			Timestamp: time.Now(),
		})
		r.txTracker.Add(txid, models.StatusReceived)
	}

	for _, tx := range txs {
		txid := tx.TxID().String()
		for _, endpoint := range r.teranodeClient.GetEndpoints() {
			go r.submitToTeranode(ctx, endpoint, tx.Bytes(), txid)
		}
	}

	var responses []*models.TransactionStatus
	for _, tx := range txs {
		status, _ := r.statusStore.GetStatus(ctx, tx.TxID().String())
		responses = append(responses, status)
	}

	return c.JSON(responses)
}

// handleGetTx retrieves transaction status
// @Summary Get transaction status
// @Description Get the current status of a submitted transaction
// @Tags transactions
// @Produce json
// @Param txid path string true "Transaction ID"
// @Success 200 {object} models.TransactionStatus
// @Failure 404 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /tx/{txid} [get]
func (r *Routes) handleGetTx(c *fiber.Ctx) error {
	status, err := r.statusStore.GetStatus(c.UserContext(), c.Params("txid"))
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to get status"})
	}
	if status == nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Transaction not found"})
	}
	return c.JSON(status)
}

// handleGetPolicy returns the policy configuration
// @Summary Get policy
// @Description Returns the transaction policy configuration including fee rates and limits
// @Tags info
// @Produce json
// @Success 200 {object} PolicyResponse
// @Router /policy [get]
func (r *Routes) handleGetPolicy(c *fiber.Ctx) error {
	return c.JSON(r.policy)
}

// handleGetHealth returns the health status
// @Summary Health check
// @Description Returns the health status of the service
// @Tags info
// @Produce json
// @Success 200 {object} HealthResponse
// @Router /health [get]
func (r *Routes) handleGetHealth(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), 2*time.Second)
	defer cancel()

	healthy := true
	reason := ""
	if _, err := r.statusStore.GetStatus(ctx, "nonexistent"); err != nil {
		healthy = false
		reason = "Database connection failed"
	}

	return c.JSON(HealthResponse{Healthy: healthy, Reason: reason, Version: "0.1.0"})
}

// handleTxSSE streams transaction status updates via SSE
// @Summary Stream transaction events
// @Description Server-Sent Events stream of transaction status updates for transactions associated with the callback token
// @Tags transactions
// @Produce text/event-stream
// @Param callbackToken path string true "Callback token from transaction submission"
// @Success 200 {string} string "SSE stream of transaction status updates"
// @Router /events/{callbackToken} [get]
func (r *Routes) handleTxSSE(c *fiber.Ctx) error {
	callbackToken := c.Params("callbackToken")

	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	ctx := c.UserContext()
	lastEventID := c.Get("Last-Event-ID")

	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		if lastEventID != "" {
			r.sendTxSSECatchup(ctx, w, callbackToken, lastEventID)
		}

		eventChan, err := r.eventPublisher.Subscribe(ctx)
		if err != nil {
			return
		}

		txidSet := r.getTxidsForToken(ctx, callbackToken)

		for {
			select {
			case <-ctx.Done():
				return
			case event := <-eventChan:
				if txidSet[event.TxID] {
					fmt.Fprintf(w, "id: %d\n", event.Timestamp.UnixNano())
					fmt.Fprintf(w, "event: status\n")
					fmt.Fprintf(w, "data: {\"txid\":\"%s\",\"status\":\"%s\",\"timestamp\":\"%s\"}\n\n",
						event.TxID, event.Status, event.Timestamp.Format(time.RFC3339))
					w.Flush()
				}
			}
		}
	}))

	return nil
}

func (r *Routes) submitToTeranode(ctx context.Context, endpoint string, rawTx []byte, txid string) {
	statusCode, err := r.teranodeClient.SubmitTransaction(ctx, endpoint, rawTx)
	if err != nil {
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusRejected,
			Timestamp: time.Now(),
			ExtraInfo: err.Error(),
		}
		r.statusStore.UpdateStatus(ctx, status)
		r.eventPublisher.Publish(ctx, status)
		return
	}

	var txStatus models.Status
	switch statusCode {
	case http.StatusOK:
		txStatus = models.StatusAcceptedByNetwork
	case http.StatusNoContent:
		txStatus = models.StatusSentToNetwork
	default:
		return
	}

	status := &models.TransactionStatus{
		TxID:      txid,
		Status:    txStatus,
		Timestamp: time.Now(),
	}
	r.statusStore.UpdateStatus(ctx, status)
	r.eventPublisher.Publish(ctx, status)
}

func (r *Routes) sendTxSSECatchup(ctx context.Context, w *bufio.Writer, callbackToken, lastEventID string) {
	sinceNS, err := strconv.ParseInt(lastEventID, 10, 64)
	if err != nil {
		return
	}
	since := time.Unix(0, sinceNS)

	submissions, err := r.submissionStore.GetSubmissionsByToken(ctx, callbackToken)
	if err != nil {
		return
	}

	for _, sub := range submissions {
		status, err := r.statusStore.GetStatus(ctx, sub.TxID)
		if err != nil || status == nil || !status.Timestamp.After(since) {
			continue
		}
		fmt.Fprintf(w, "id: %d\n", status.Timestamp.UnixNano())
		fmt.Fprintf(w, "event: status\n")
		fmt.Fprintf(w, "data: {\"txid\":\"%s\",\"status\":\"%s\",\"timestamp\":\"%s\"}\n\n",
			status.TxID, status.Status, status.Timestamp.Format(time.RFC3339))
	}
	w.Flush()
}

func (r *Routes) getTxidsForToken(ctx context.Context, callbackToken string) map[string]bool {
	submissions, err := r.submissionStore.GetSubmissionsByToken(ctx, callbackToken)
	if err != nil {
		return make(map[string]bool)
	}
	txidSet := make(map[string]bool, len(submissions))
	for _, sub := range submissions {
		txidSet[sub.TxID] = true
	}
	return txidSet
}

// GetArcade returns the arcade instance for use by dashboard or other handlers.
func (r *Routes) GetArcade() *arcade.Arcade {
	return r.arcade
}
