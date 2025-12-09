package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bsv-blockchain/arcade"
	"github.com/bsv-blockchain/arcade/api"
	"github.com/bsv-blockchain/arcade/beef"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
)

// Error response
// @Description Error response
type ErrorResponse struct {
	Error string `json:"error" example:"Invalid request"`
}

// Network response
// @Description Network name response
type NetworkResponse struct {
	Network string `json:"network" example:"main"`
}

// Height response
// @Description Current blockchain height
type HeightResponse struct {
	Height uint32 `json:"height" example:"873421"`
}

// Headers response
// @Description Concatenated raw headers in hex
type HeadersResponse struct {
	Headers string `json:"headers" example:"0100000000000000..."`
}

// handlePostTx submits a transaction
// @Summary Submit transaction
// @Description Submit a raw transaction for broadcast to the network
// @Tags Transactions
// @Accept json
// @Accept application/octet-stream
// @Produce json
// @Param transaction body api.TransactionRequest true "Transaction request"
// @Param X-CallbackUrl header string false "Webhook callback URL"
// @Param X-CallbackToken header string false "Callback token for SSE/webhook auth"
// @Param X-FullStatusUpdates header boolean false "Receive all status updates"
// @Success 200 {object} models.TransactionStatus
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /tx [post]
func handlePostTx(c *fiber.Ctx) error {
	ctx := c.UserContext()

	var rawTx []byte
	var err error

	if c.Get("Content-Type") == "application/octet-stream" {
		rawTx = c.Body()
		if beef.IsBeef(rawTx) {
			tx, err := beef.ParseAndValidate(rawTx)
			if err != nil {
				return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Invalid BEEF format: %v", err)})
			}
			rawTx = tx.Bytes()
		}
	} else {
		var req api.TransactionRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
		}
		rawTx, err = hex.DecodeString(req.RawTx)
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid transaction hex"})
		}
	}

	if err := txValidator.ValidateTransaction(rawTx); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Validation failed: %v", err)})
	}

	tx, err := sdkTx.NewTransactionFromBytes(rawTx)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Failed to parse transaction"})
	}
	txid := tx.TxID().String()

	if err := statusStore.InsertStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now(),
	}); err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to store status"})
	}

	txTracker.Add(txid, models.StatusReceived)

	callbackURL := c.Get("X-CallbackUrl")
	callbackToken := c.Get("X-CallbackToken")
	if callbackURL != "" || callbackToken != "" {
		if err := submissionStore.InsertSubmission(ctx, &models.Submission{
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

	for _, endpoint := range teranodeClient.GetEndpoints() {
		go submitToTeranode(ctx, endpoint, rawTx, txid)
	}

	status, err := statusStore.GetStatus(ctx, txid)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to get status"})
	}

	return c.JSON(status)
}

// handlePostTxs submits multiple transactions
// @Summary Submit multiple transactions
// @Description Submit multiple raw transactions for broadcast
// @Tags Transactions
// @Accept json
// @Produce json
// @Param transactions body []api.TransactionRequest true "Array of transactions"
// @Success 200 {array} models.TransactionStatus
// @Failure 400 {object} ErrorResponse
// @Router /txs [post]
func handlePostTxs(c *fiber.Ctx) error {
	ctx := c.UserContext()

	var reqs []api.TransactionRequest
	if err := c.BodyParser(&reqs); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	var rawTxs [][]byte
	for _, req := range reqs {
		rawTx, err := hex.DecodeString(req.RawTx)
		if err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid transaction hex"})
		}
		if err := txValidator.ValidateTransaction(rawTx); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Validation failed: %v", err)})
		}
		rawTxs = append(rawTxs, rawTx)

		tx, _ := sdkTx.NewTransactionFromBytes(rawTx)
		txid := tx.TxID().String()
		statusStore.InsertStatus(ctx, &models.TransactionStatus{
			TxID:      txid,
			Timestamp: time.Now(),
		})
		txTracker.Add(txid, models.StatusReceived)
	}

	for _, rawTx := range rawTxs {
		tx, _ := sdkTx.NewTransactionFromBytes(rawTx)
		txid := tx.TxID().String()
		for _, endpoint := range teranodeClient.GetEndpoints() {
			go submitToTeranode(ctx, endpoint, rawTx, txid)
		}
	}

	var responses []*models.TransactionStatus
	for _, rawTx := range rawTxs {
		tx, _ := sdkTx.NewTransactionFromBytes(rawTx)
		status, _ := statusStore.GetStatus(ctx, tx.TxID().String())
		responses = append(responses, status)
	}

	return c.JSON(responses)
}

// handleGetTx retrieves transaction status
// @Summary Get transaction status
// @Description Get the current status of a submitted transaction
// @Tags Transactions
// @Produce json
// @Param txid path string true "Transaction ID"
// @Success 200 {object} models.TransactionStatus
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /tx/{txid} [get]
func handleGetTx(c *fiber.Ctx) error {
	status, err := statusStore.GetStatus(c.UserContext(), c.Params("txid"))
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to get status"})
	}
	if status == nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Transaction not found"})
	}
	return c.JSON(status)
}

// handleGetPolicy returns the server policy
// @Summary Get policy
// @Description Get transaction validation policy and fee requirements
// @Tags Info
// @Produce json
// @Success 200 {object} api.PolicyResponse
// @Router /policy [get]
func handleGetPolicy(c *fiber.Ctx) error {
	return c.JSON(policy)
}

// handleGetHealth returns server health status
// @Summary Health check
// @Description Check server health and database connectivity
// @Tags Info
// @Produce json
// @Success 200 {object} api.HealthResponse
// @Router /health [get]
func handleGetHealth(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.UserContext(), 2*time.Second)
	defer cancel()

	healthy := true
	reason := ""
	if _, err := statusStore.GetStatus(ctx, "nonexistent"); err != nil {
		healthy = false
		reason = "Database connection failed"
	}

	return c.JSON(api.HealthResponse{Healthy: healthy, Reason: reason, Version: "0.1.0"})
}

// handleTxSSE streams transaction status updates via SSE
// @Summary Transaction status stream
// @Description Subscribe to transaction status updates via Server-Sent Events
// @Tags Transactions
// @Produce text/event-stream
// @Param callbackToken path string true "Callback token from transaction submission"
// @Param Last-Event-ID header string false "Resume from event ID"
// @Success 200 {string} string "SSE stream"
// @Router /events/{callbackToken} [get]
func handleTxSSE(c *fiber.Ctx) error {
	callbackToken := c.Params("callbackToken")

	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	ctx := c.UserContext()
	lastEventID := c.Get("Last-Event-ID")

	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		if lastEventID != "" {
			sendTxSSECatchup(ctx, w, callbackToken, lastEventID)
		}

		eventChan, err := eventPublisher.Subscribe(ctx)
		if err != nil {
			return
		}

		txidSet := getTxidsForToken(ctx, callbackToken)

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

// handleGetNetwork returns the network name
// @Summary Get network
// @Description Get the BSV network name (main, test, etc.)
// @Tags Chain
// @Produce json
// @Success 200 {object} NetworkResponse
// @Failure 500 {object} ErrorResponse
// @Router /network [get]
func handleGetNetwork(c *fiber.Ctx) error {
	network, err := arcadeInstance.GetNetwork(c.UserContext())
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"network": network})
}

// handleGetHeight returns the current chain height
// @Summary Get height
// @Description Get the current blockchain height
// @Tags Chain
// @Produce json
// @Success 200 {object} HeightResponse
// @Router /height [get]
func handleGetHeight(c *fiber.Ctx) error {
	c.Set("Cache-Control", "public, max-age=60")
	return c.JSON(fiber.Map{"height": arcadeInstance.GetHeight(c.UserContext())})
}

// handleGetTip returns the chain tip header
// @Summary Get chain tip
// @Description Get the current chain tip block header
// @Tags Chain
// @Produce json
// @Success 200 {object} arcade.BlockHeader
// @Failure 404 {object} ErrorResponse
// @Router /tip [get]
func handleGetTip(c *fiber.Ctx) error {
	c.Set("Cache-Control", "no-cache")
	tip := arcadeInstance.GetTip(c.UserContext())
	if tip == nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Chain tip not found"})
	}
	return c.JSON(tip)
}

// handleTipStream streams chain tip updates via SSE
// @Summary Chain tip stream
// @Description Subscribe to chain tip updates via Server-Sent Events
// @Tags Chain
// @Produce text/event-stream
// @Success 200 {string} string "SSE stream"
// @Router /tip/stream [get]
func handleTipStream(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")

	ctx := c.UserContext()

	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		clientID := time.Now().UnixNano()

		tipClientsMu.Lock()
		tipClients[clientID] = w
		tipClientsMu.Unlock()

		defer func() {
			tipClientsMu.Lock()
			delete(tipClients, clientID)
			tipClientsMu.Unlock()
		}()

		if tip := arcadeInstance.GetTip(ctx); tip != nil {
			if data, err := json.Marshal(tip); err == nil {
				fmt.Fprintf(w, "data: %s\n\n", data)
				w.Flush()
			}
		}

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Fprintf(w, ": keepalive\n\n")
				w.Flush()
			}
		}
	}))

	return nil
}

// handleGetHeaderByHeight returns a header by height
// @Summary Get header by height
// @Description Get a block header at the specified height
// @Tags Chain
// @Produce json
// @Param height path int true "Block height"
// @Success 200 {object} arcade.BlockHeader
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /header/height/{height} [get]
func handleGetHeaderByHeight(c *fiber.Ctx) error {
	height, err := strconv.ParseUint(c.Params("height"), 10, 32)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid height parameter"})
	}

	ctx := c.UserContext()
	tip := arcadeInstance.GetHeight(ctx)
	if uint32(height) < tip-100 {
		c.Set("Cache-Control", "public, max-age=3600")
	} else {
		c.Set("Cache-Control", "no-cache")
	}

	header, err := arcadeInstance.GetHeaderByHeight(ctx, uint32(height))
	if err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Header not found"})
	}
	return c.JSON(header)
}

// handleGetHeaderByHash returns a header by hash
// @Summary Get header by hash
// @Description Get a block header with the specified hash
// @Tags Chain
// @Produce json
// @Param hash path string true "Block hash (hex)"
// @Success 200 {object} arcade.BlockHeader
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Router /header/hash/{hash} [get]
func handleGetHeaderByHash(c *fiber.Ctx) error {
	hash, err := chainhash.NewHashFromHex(c.Params("hash"))
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid hash parameter"})
	}

	ctx := c.UserContext()
	header, err := arcadeInstance.GetHeaderByHash(ctx, hash)
	if err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Header not found"})
	}

	tip := arcadeInstance.GetHeight(ctx)
	if header.Height < tip-100 {
		c.Set("Cache-Control", "public, max-age=3600")
	} else {
		c.Set("Cache-Control", "no-cache")
	}

	return c.JSON(header)
}

// handleGetHeaders returns multiple headers as concatenated hex
// @Summary Get multiple headers
// @Description Get multiple consecutive block headers as concatenated raw hex
// @Tags Chain
// @Produce json
// @Param height query int true "Starting height"
// @Param count query int true "Number of headers"
// @Success 200 {object} HeadersResponse
// @Failure 400 {object} ErrorResponse
// @Router /headers [get]
func handleGetHeaders(c *fiber.Ctx) error {
	heightStr := c.Query("height")
	countStr := c.Query("count")
	if heightStr == "" || countStr == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Missing height or count parameter"})
	}

	height, err := strconv.ParseUint(heightStr, 10, 32)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid height parameter"})
	}
	count, err := strconv.ParseUint(countStr, 10, 32)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid count parameter"})
	}

	ctx := c.UserContext()
	tip := arcadeInstance.GetHeight(ctx)
	if uint32(height) < tip-100 {
		c.Set("Cache-Control", "public, max-age=3600")
	} else {
		c.Set("Cache-Control", "no-cache")
	}

	var hexData string
	for i := uint32(0); i < uint32(count); i++ {
		header, err := arcadeInstance.GetHeaderByHeight(ctx, uint32(height)+i)
		if err != nil {
			break
		}
		hexData += hex.EncodeToString(header.Bytes())
	}

	return c.JSON(fiber.Map{"headers": hexData})
}

// Helper functions

func submitToTeranode(ctx context.Context, endpoint string, rawTx []byte, txid string) {
	statusCode, err := teranodeClient.SubmitTransaction(ctx, endpoint, rawTx)
	if err != nil {
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusRejected,
			Timestamp: time.Now(),
			ExtraInfo: err.Error(),
		}
		statusStore.UpdateStatus(ctx, status)
		eventPublisher.Publish(ctx, status)
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
	statusStore.UpdateStatus(ctx, status)
	eventPublisher.Publish(ctx, status)
}


func sendTxSSECatchup(ctx context.Context, w *bufio.Writer, callbackToken, lastEventID string) {
	sinceNS, err := strconv.ParseInt(lastEventID, 10, 64)
	if err != nil {
		return
	}
	since := time.Unix(0, sinceNS)

	submissions, err := submissionStore.GetSubmissionsByToken(ctx, callbackToken)
	if err != nil {
		return
	}

	for _, sub := range submissions {
		status, err := statusStore.GetStatus(ctx, sub.TxID)
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

func getTxidsForToken(ctx context.Context, callbackToken string) map[string]bool {
	submissions, err := submissionStore.GetSubmissionsByToken(ctx, callbackToken)
	if err != nil {
		return make(map[string]bool)
	}
	txidSet := make(map[string]bool, len(submissions))
	for _, sub := range submissions {
		txidSet[sub.TxID] = true
	}
	return txidSet
}

func broadcastTipUpdates(ctx context.Context, tipChan <-chan *arcade.BlockHeader) {
	for {
		select {
		case <-ctx.Done():
			return
		case tip := <-tipChan:
			if tip == nil {
				continue
			}
			data, err := json.Marshal(tip)
			if err != nil {
				continue
			}
			msg := fmt.Sprintf("data: %s\n\n", data)

			tipClientsMu.RLock()
			clients := make(map[int64]*bufio.Writer, len(tipClients))
			for id, w := range tipClients {
				clients[id] = w
			}
			tipClientsMu.RUnlock()

			var failed []int64
			for id, w := range clients {
				if _, err := fmt.Fprint(w, msg); err != nil {
					failed = append(failed, id)
					continue
				}
				if err := w.Flush(); err != nil {
					failed = append(failed, id)
				}
			}

			if len(failed) > 0 {
				tipClientsMu.Lock()
				for _, id := range failed {
					delete(tipClients, id)
				}
				tipClientsMu.Unlock()
			}
		}
	}
}
