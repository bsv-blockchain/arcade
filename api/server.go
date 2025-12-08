package api

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/bsv-blockchain/arcade/beef"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/handlers"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

const (
	defaultWaitFor    = "SENT_TO_NETWORK"
	defaultMaxTimeout = 5
	maxMaxTimeout     = 30
)

// Server handles HTTP API requests
type Server struct {
	statusStore     store.StatusStore
	submissionStore store.SubmissionStore
	networkStore    store.NetworkStateStore
	eventPublisher  events.Publisher
	teranodeClient  *teranode.Client
	validator       *validator.Validator
	policy          *PolicyResponse
	authToken       string
	sseHandler      *handlers.SSEHandler
	echo            *echo.Echo
}

// NewServer creates a new API server
func NewServer(
	statusStore store.StatusStore,
	submissionStore store.SubmissionStore,
	networkStore store.NetworkStateStore,
	eventPublisher events.Publisher,
	teranodeClient *teranode.Client,
	v *validator.Validator,
	policy *PolicyResponse,
	authToken string,
	logger *slog.Logger,
) *Server {
	sseHandler := handlers.NewSSEHandler(
		eventPublisher,
		submissionStore,
		statusStore,
		logger,
	)

	return &Server{
		statusStore:     statusStore,
		submissionStore: submissionStore,
		networkStore:    networkStore,
		eventPublisher:  eventPublisher,
		teranodeClient:  teranodeClient,
		validator:       v,
		policy:          policy,
		authToken:       authToken,
		sseHandler:      sseHandler,
	}
}

// Start starts the HTTP server
func (s *Server) Start(addr string) error {
	e := echo.New()
	s.echo = e

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	if s.authToken != "" {
		e.Use(s.authMiddleware)
	}

	v1 := e.Group("/v1")
	v1.POST("/tx", s.handlePostTransaction)
	v1.POST("/txs", s.handlePostTransactions)
	v1.GET("/tx/:txid", s.handleGetTransaction)
	v1.GET("/policy", s.handleGetPolicy)
	v1.GET("/health", s.handleGetHealth)
	v1.GET("/events/:callbackToken", s.sseHandler.HandleSSE)

	return e.Start(addr)
}

// authMiddleware validates bearer token authentication
func (s *Server) authMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if c.Path() == "/v1/health" || c.Path() == "/v1/policy" {
			return next(c)
		}

		auth := c.Request().Header.Get("Authorization")
		if auth == "" {
			return c.JSON(http.StatusUnauthorized, map[string]string{
				"error": "Missing authorization header",
			})
		}

		const bearerPrefix = "Bearer "
		if len(auth) < len(bearerPrefix) || auth[:len(bearerPrefix)] != bearerPrefix {
			return c.JSON(http.StatusUnauthorized, map[string]string{
				"error": "Invalid authorization header format",
			})
		}

		token := auth[len(bearerPrefix):]
		if token != s.authToken {
			return c.JSON(http.StatusUnauthorized, map[string]string{
				"error": "Invalid token",
			})
		}

		return next(c)
	}
}

// Stop gracefully stops the HTTP server
func (s *Server) Stop(ctx context.Context) error {
	if s.echo != nil {
		return s.echo.Shutdown(ctx)
	}
	return nil
}

func (s *Server) handlePostTransaction(c echo.Context) error {
	ctx := c.Request().Context()
	headers := parseHeaders(c)

	var rawTx []byte
	var err error

	contentType := c.Request().Header.Get("Content-Type")

	if contentType == "application/octet-stream" {
		rawTx, err = io.ReadAll(c.Request().Body)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Failed to read request body",
			})
		}

		if beef.IsBeef(rawTx) {
			tx, err := beef.ParseAndValidate(rawTx)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{
					"error": fmt.Sprintf("Invalid BEEF format: %v", err),
				})
			}
			rawTx = tx.Bytes()
		}
	} else {
		var req TransactionRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Invalid request body",
			})
		}

		rawTx, err = hex.DecodeString(req.RawTx)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Invalid transaction hex",
			})
		}
	}

	if err := s.validator.ValidateTransaction(rawTx); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Validation failed: %v", err),
		})
	}

	txid, err := extractTxID(rawTx)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Failed to extract txid",
		})
	}

	submissionID := uuid.New().String()

	if err := s.statusStore.InsertStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now(),
	}); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to store status",
		})
	}

	if headers.CallbackURL != "" || headers.CallbackToken != "" {
		if err := s.submissionStore.InsertSubmission(ctx, &models.Submission{
			SubmissionID:      submissionID,
			TxID:              txid,
			CallbackURL:       headers.CallbackURL,
			CallbackToken:     headers.CallbackToken,
			FullStatusUpdates: headers.FullStatusUpdates,
			CreatedAt:         time.Now(),
		}); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "Failed to store submission",
			})
		}
	}

	endpoints := s.teranodeClient.GetEndpoints()
	for _, endpoint := range endpoints {
		go func(ep string) {
			statusCode, err := s.teranodeClient.SubmitTransaction(ctx, ep, rawTx)
			if err != nil {
				s.statusStore.UpdateStatus(ctx, &models.TransactionStatus{
					TxID:      txid,
					Status:    models.StatusRejected,
					Timestamp: time.Now(),
					ExtraInfo: err.Error(),
				})
				s.eventPublisher.Publish(ctx, models.StatusUpdate{
					TxID:      txid,
					Status:    models.StatusRejected,
					Timestamp: time.Now(),
				})
				return
			}

			var status models.Status
			if statusCode == http.StatusOK {
				status = models.StatusAcceptedByNetwork
			} else if statusCode == http.StatusNoContent {
				status = models.StatusSentToNetwork
			} else {
				return
			}

			s.statusStore.UpdateStatus(ctx, &models.TransactionStatus{
				TxID:      txid,
				Status:    status,
				Timestamp: time.Now(),
			})

			s.eventPublisher.Publish(ctx, models.StatusUpdate{
				TxID:      txid,
				Status:    status,
				Timestamp: time.Now(),
			})
		}(endpoint)
	}

	status, err := s.statusStore.GetStatus(ctx, txid)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to get status",
		})
	}

	return c.JSON(http.StatusOK, toTransactionResponse(status))
}

func (s *Server) handlePostTransactions(c echo.Context) error {
	ctx := c.Request().Context()
	_ = parseHeaders(c)

	var reqs []TransactionRequest
	if err := c.Bind(&reqs); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
	}

	responses := make([]TransactionResponse, 0, len(reqs))
	rawTxs := make([][]byte, 0, len(reqs))

	for _, req := range reqs {
		rawTx, err := hex.DecodeString(req.RawTx)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Invalid transaction hex",
			})
		}

		if err := s.validator.ValidateTransaction(rawTx); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": fmt.Sprintf("Validation failed: %v", err),
			})
		}

		rawTxs = append(rawTxs, rawTx)

		txid, _ := extractTxID(rawTx)

		s.statusStore.InsertStatus(ctx, &models.TransactionStatus{
			TxID:      txid,
			Timestamp: time.Now(),
		})
	}

	endpoints := s.teranodeClient.GetEndpoints()
	for _, rawTx := range rawTxs {
		txid, _ := extractTxID(rawTx)
		for _, endpoint := range endpoints {
			go func(ep string, tx []byte, id string) {
				statusCode, err := s.teranodeClient.SubmitTransaction(ctx, ep, tx)
				if err != nil {
					s.statusStore.UpdateStatus(ctx, &models.TransactionStatus{
						TxID:      id,
						Status:    models.StatusRejected,
						Timestamp: time.Now(),
						ExtraInfo: err.Error(),
					})
					s.eventPublisher.Publish(ctx, models.StatusUpdate{
						TxID:      id,
						Status:    models.StatusRejected,
						Timestamp: time.Now(),
					})
					return
				}

				var status models.Status
				if statusCode == http.StatusOK {
					status = models.StatusAcceptedByNetwork
				} else if statusCode == http.StatusNoContent {
					status = models.StatusSentToNetwork
				} else {
					return
				}

				s.statusStore.UpdateStatus(ctx, &models.TransactionStatus{
					TxID:      id,
					Status:    status,
					Timestamp: time.Now(),
				})

				s.eventPublisher.Publish(ctx, models.StatusUpdate{
					TxID:      id,
					Status:    status,
					Timestamp: time.Now(),
				})
			}(endpoint, rawTx, txid)
		}
	}

	for _, rawTx := range rawTxs {
		txid, _ := extractTxID(rawTx)
		status, _ := s.statusStore.GetStatus(ctx, txid)
		responses = append(responses, toTransactionResponse(status))
	}

	return c.JSON(http.StatusOK, responses)
}

func (s *Server) handleGetTransaction(c echo.Context) error {
	ctx := c.Request().Context()
	txid := c.Param("txid")

	status, err := s.statusStore.GetStatus(ctx, txid)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to get status",
		})
	}

	if status == nil {
		return c.JSON(http.StatusNotFound, map[string]string{
			"error": "Transaction not found",
		})
	}

	return c.JSON(http.StatusOK, toTransactionResponse(status))
}

func (s *Server) handleGetPolicy(c echo.Context) error {
	return c.JSON(http.StatusOK, s.policy)
}

func (s *Server) handleGetHealth(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
	defer cancel()

	healthy := true
	reason := ""

	if _, err := s.statusStore.GetStatus(ctx, "nonexistent"); err != nil {
		healthy = false
		reason = "Database connection failed"
	}

	return c.JSON(http.StatusOK, HealthResponse{
		Healthy: healthy,
		Reason:  reason,
		Version: "0.1.0",
	})
}

func parseHeaders(c echo.Context) RequestHeaders {
	h := RequestHeaders{
		CallbackURL:          c.Request().Header.Get("X-CallbackUrl"),
		CallbackToken:        c.Request().Header.Get("X-CallbackToken"),
		CallbackBatch:        c.Request().Header.Get("X-CallbackBatch") == "true",
		FullStatusUpdates:    c.Request().Header.Get("X-FullStatusUpdates") == "true",
		WaitFor:              c.Request().Header.Get("X-WaitFor"),
		SkipFeeValidation:    c.Request().Header.Get("X-SkipFeeValidation") == "true",
		SkipScriptValidation: c.Request().Header.Get("X-SkipScriptValidation") == "true",
	}

	if h.WaitFor == "" {
		h.WaitFor = defaultWaitFor
	}

	timeoutStr := c.Request().Header.Get("X-MaxTimeout")
	if timeoutStr != "" {
		if timeout, err := strconv.Atoi(timeoutStr); err == nil {
			h.MaxTimeout = timeout
		}
	}

	if h.MaxTimeout == 0 {
		h.MaxTimeout = defaultMaxTimeout
	}
	if h.MaxTimeout > maxMaxTimeout {
		h.MaxTimeout = maxMaxTimeout
	}

	return h
}

func toTransactionResponse(status *models.TransactionStatus) TransactionResponse {
	if status == nil {
		return TransactionResponse{}
	}

	return TransactionResponse{
		Timestamp:    status.Timestamp,
		TxID:         status.TxID,
		TxStatus:     string(status.Status),
		BlockHash:    status.BlockHash,
		BlockHeight:  status.BlockHeight,
		MerklePath:   status.MerklePath,
		ExtraInfo:    status.ExtraInfo,
		CompetingTxs: status.CompetingTxs,
	}
}

func extractTxID(rawTx []byte) (string, error) {
	tx, err := sdkTx.NewTransactionFromBytes(rawTx)
	if err != nil {
		return "", fmt.Errorf("failed to parse transaction: %w", err)
	}
	return tx.TxID().String(), nil
}
