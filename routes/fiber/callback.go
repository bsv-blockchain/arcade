package fiber

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/gofiber/fiber/v2"

	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// BUMPConstructor is called when BLOCK_PROCESSED is received to build full BUMPs.
type BUMPConstructor interface {
	ConstructBUMPsForBlock(ctx context.Context, blockHash string) error
}

// CallbackConfig holds configuration for the Merkle Service callback handler.
type CallbackConfig struct {
	Store           store.Store
	EventPublisher  events.Publisher
	TxTracker       *store.TxTracker
	BUMPConstructor BUMPConstructor
	CallbackToken   string // Bearer token for authenticating inbound callbacks
	Logger          *slog.Logger
}

// CallbackHandler handles inbound callbacks from Merkle Service.
type CallbackHandler struct {
	store           store.Store
	eventPublisher  events.Publisher
	txTracker       *store.TxTracker
	bumpConstructor BUMPConstructor
	callbackToken   string
	logger          *slog.Logger
}

// NewCallbackHandler creates a new callback handler.
func NewCallbackHandler(cfg CallbackConfig) *CallbackHandler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &CallbackHandler{
		store:           cfg.Store,
		eventPublisher:  cfg.EventPublisher,
		txTracker:       cfg.TxTracker,
		bumpConstructor: cfg.BUMPConstructor,
		callbackToken:   cfg.CallbackToken,
		logger:          logger,
	}
}

// Register registers the callback route on the given router.
func (h *CallbackHandler) Register(router fiber.Router) {
	router.Post("/api/v1/merkle-service/callback", h.handleCallback)
}

func (h *CallbackHandler) handleCallback(c *fiber.Ctx) error {
	// Validate authentication
	if h.callbackToken != "" {
		auth := c.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || auth[len("Bearer "):] != h.callbackToken {
			return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
		}
	}

	var msg models.CallbackMessage
	if err := c.BodyParser(&msg); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx := c.UserContext()

	switch msg.Type {
	case models.CallbackSeenOnNetwork:
		h.handleSeenOnNetwork(ctx, msg)
	case models.CallbackSeenMultipleNodes:
		h.handleSeenMultipleNodes(ctx, msg)
	case models.CallbackStump:
		h.handleStump(ctx, msg)
	case models.CallbackBlockProcessed:
		h.handleBlockProcessed(ctx, msg)
	default:
		h.logger.Warn("unknown callback type", slog.String("type", string(msg.Type)))
	}

	return c.SendStatus(http.StatusOK)
}

func (h *CallbackHandler) handleSeenOnNetwork(ctx context.Context, msg models.CallbackMessage) {
	if msg.TxID == "" {
		return
	}

	status := &models.TransactionStatus{
		TxID:      msg.TxID,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	}

	if err := h.store.UpdateStatus(ctx, status); err != nil {
		h.logger.Warn("failed to update seen_on_network status",
			slog.String("txid", msg.TxID),
			slog.String("error", err.Error()))
		return
	}

	if h.txTracker != nil {
		hash, err := chainhash.NewHashFromHex(msg.TxID)
		if err == nil {
			h.txTracker.UpdateStatusHash(*hash, models.StatusSeenOnNetwork)
		}
	}

	if h.eventPublisher != nil {
		_ = h.eventPublisher.Publish(ctx, status)
	}
}

func (h *CallbackHandler) handleSeenMultipleNodes(ctx context.Context, msg models.CallbackMessage) {
	if msg.TxID == "" {
		return
	}

	// Publish an event so downstream subscribers (webhooks, SSE) are notified
	status := &models.TransactionStatus{
		TxID:      msg.TxID,
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
		ExtraInfo: "seen by multiple nodes",
	}

	if h.eventPublisher != nil {
		_ = h.eventPublisher.Publish(ctx, status)
	}
}

func (h *CallbackHandler) handleStump(ctx context.Context, msg models.CallbackMessage) {
	if msg.TxID == "" || msg.BlockHash == "" || len(msg.Stump) == 0 {
		h.logger.Warn("incomplete STUMP callback",
			slog.String("txid", msg.TxID),
			slog.String("blockHash", msg.BlockHash))
		return
	}

	stump := &models.Stump{
		TxID:         msg.TxID,
		BlockHash:    msg.BlockHash,
		SubtreeIndex: msg.SubtreeIndex,
		StumpData:    msg.Stump,
	}

	if err := h.store.InsertStump(ctx, stump); err != nil {
		h.logger.Error("failed to store STUMP",
			slog.String("txid", msg.TxID),
			slog.String("blockHash", msg.BlockHash),
			slog.String("error", err.Error()))
	}
}

func (h *CallbackHandler) handleBlockProcessed(ctx context.Context, msg models.CallbackMessage) {
	if msg.BlockHash == "" {
		return
	}

	if h.bumpConstructor == nil {
		h.logger.Warn("BLOCK_PROCESSED received but no BUMP constructor configured",
			slog.String("blockHash", msg.BlockHash))
		return
	}

	if err := h.bumpConstructor.ConstructBUMPsForBlock(ctx, msg.BlockHash); err != nil {
		h.logger.Error("failed to construct BUMPs for block",
			slog.String("blockHash", msg.BlockHash),
			slog.String("error", err.Error()))
	}
}
