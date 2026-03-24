package fiber

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
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

	h.logger.Debug("received callback", slog.String("type", string(msg.Type)), slog.String("txid", msg.TxID), slog.Int("txids", len(msg.TxIDs)), slog.String("blockHash", msg.BlockHash), slog.Int("subtreeIndex", msg.SubtreeIndex), slog.Int("stumpSize", len(msg.Stump)))

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
	txids := msg.ResolveSeenTxIDs()
	if len(txids) == 0 {
		return
	}

	now := time.Now()
	for _, txid := range txids {
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: now,
		}

		if err := h.store.UpdateStatus(ctx, status); err != nil {
			h.logger.Warn("failed to update seen_on_network status",
				slog.String("txid", txid),
				slog.String("error", err.Error()))
			continue
		}

		if h.txTracker != nil {
			hash, err := chainhash.NewHashFromHex(txid)
			if err == nil {
				h.txTracker.UpdateStatusHash(*hash, models.StatusSeenOnNetwork)
			}
		}

		if h.eventPublisher != nil {
			_ = h.eventPublisher.Publish(ctx, status)
		}
	}
}

func (h *CallbackHandler) handleSeenMultipleNodes(ctx context.Context, msg models.CallbackMessage) {
	txids := msg.ResolveSeenTxIDs()
	if len(txids) == 0 {
		return
	}

	now := time.Now()
	for _, txid := range txids {
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: now,
			ExtraInfo: "seen by multiple nodes",
		}

		if h.eventPublisher != nil {
			_ = h.eventPublisher.Publish(ctx, status)
		}
	}
}

func (h *CallbackHandler) handleStump(ctx context.Context, msg models.CallbackMessage) {
	if msg.BlockHash == "" || len(msg.Stump) == 0 {
		h.logger.Warn("incomplete STUMP callback",
			slog.String("blockHash", msg.BlockHash))
		return
	}

	// Parse STUMP binary to extract level-0 leaf hashes
	level0Hashes := extractLevel0Hashes(msg.Stump)
	if len(level0Hashes) == 0 {
		h.logger.Warn("STUMP has no level-0 hashes",
			slog.String("blockHash", msg.BlockHash),
			slog.Int("subtreeIndex", msg.SubtreeIndex))
		return
	}

	// Find which level-0 hashes are transactions we're tracking
	var trackedHashes []chainhash.Hash
	if h.txTracker != nil {
		trackedHashes = h.txTracker.FilterTrackedHashes(level0Hashes)
	}

	// Update status for each tracked transaction
	now := time.Now()
	for _, hash := range trackedHashes {
		txid := hash.String()
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			BlockHash: msg.BlockHash,
			Timestamp: now,
		}

		if err := h.store.UpdateStatus(ctx, status); err != nil {
			h.logger.Warn("failed to update mined status from STUMP",
				slog.String("txid", txid),
				slog.String("error", err.Error()))
			continue
		}

		h.txTracker.UpdateStatusHash(hash, models.StatusStumpProcessing)

		if h.eventPublisher != nil {
			_ = h.eventPublisher.Publish(ctx, status)
		}
	}

	// Store one stump per (blockHash, subtreeIndex)
	stump := &models.Stump{
		BlockHash:    msg.BlockHash,
		SubtreeIndex: msg.SubtreeIndex,
		StumpData:    msg.Stump,
	}

	if err := h.store.InsertStump(ctx, stump); err != nil {
		h.logger.Error("failed to store STUMP",
			slog.String("blockHash", msg.BlockHash),
			slog.Int("subtreeIndex", msg.SubtreeIndex),
			slog.String("error", err.Error()))
	}
}

// extractLevel0Hashes parses a BRC-74 STUMP binary and returns all level-0 hashes.
func extractLevel0Hashes(stumpData []byte) []chainhash.Hash {
	mp, err := transaction.NewMerklePathFromBinary(stumpData)
	if err != nil || len(mp.Path) == 0 {
		return nil
	}

	hashes := make([]chainhash.Hash, 0, len(mp.Path[0]))
	for _, leaf := range mp.Path[0] {
		if leaf.Hash != nil {
			hashes = append(hashes, *leaf.Hash)
		}
	}
	return hashes
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
