// Package embedded provides an in-process implementation of the ArcadeService interface.
package embedded

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/arcade"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/service"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/google/uuid"
)

// Ensure Embedded implements service.ArcadeService
var _ service.ArcadeService = (*Embedded)(nil)

// Config holds configuration for the embedded service.
type Config struct {
	Store          store.Store
	TxTracker      *store.TxTracker
	EventPublisher events.Publisher
	TeranodeClient *teranode.Client
	TxValidator    *validator.Validator
	Arcade         *arcade.Arcade
	Policy         *models.Policy
	Logger         *slog.Logger
}

// Embedded is an in-process implementation of ArcadeService.
type Embedded struct {
	store          store.Store
	txTracker      *store.TxTracker
	eventPublisher events.Publisher
	teranodeClient *teranode.Client
	txValidator    *validator.Validator
	arcade         *arcade.Arcade
	policy         *models.Policy
	logger         *slog.Logger

	// Subscription tracking
	subMu    sync.RWMutex
	subChans map[<-chan *models.TransactionStatus]context.CancelFunc
}

// New creates a new Embedded service instance.
func New(cfg Config) (*Embedded, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("Store is required")
	}
	if cfg.TxTracker == nil {
		return nil, fmt.Errorf("TxTracker is required")
	}
	if cfg.EventPublisher == nil {
		return nil, fmt.Errorf("EventPublisher is required")
	}
	if cfg.TeranodeClient == nil {
		return nil, fmt.Errorf("TeranodeClient is required")
	}
	if cfg.TxValidator == nil {
		return nil, fmt.Errorf("TxValidator is required")
	}
	if cfg.Arcade == nil {
		return nil, fmt.Errorf("Arcade is required")
	}
	if cfg.Policy == nil {
		return nil, fmt.Errorf("Policy is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Embedded{
		store:          cfg.Store,
		txTracker:      cfg.TxTracker,
		eventPublisher: cfg.EventPublisher,
		teranodeClient: cfg.TeranodeClient,
		txValidator:    cfg.TxValidator,
		arcade:         cfg.Arcade,
		policy:         cfg.Policy,
		logger:         cfg.Logger,
		subChans:       make(map[<-chan *models.TransactionStatus]context.CancelFunc),
	}, nil
}

// SubmitTransaction submits a single transaction for broadcast.
func (e *Embedded) SubmitTransaction(ctx context.Context, rawTx []byte, opts *models.SubmitOptions) (*models.TransactionStatus, error) {
	if opts == nil {
		opts = &models.SubmitOptions{}
	}

	// Parse transaction (try BEEF first, then raw bytes)
	_, tx, _, err := sdkTx.ParseBeef(rawTx)
	if err != nil || tx == nil {
		tx, err = sdkTx.NewTransactionFromBytes(rawTx)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transaction: %w", err)
		}
	}

	// Validate transaction
	if err := e.txValidator.ValidateTransaction(ctx, tx, opts.SkipFeeValidation, opts.SkipScriptValidation); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	txid := tx.TxID().String()

	// Insert initial status
	if err := e.store.InsertStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now(),
	}); err != nil {
		return nil, fmt.Errorf("failed to store status: %w", err)
	}

	// Track transaction
	e.txTracker.Add(txid, models.StatusReceived)

	// Create submission record if callback URL or token provided
	if opts.CallbackURL != "" || opts.CallbackToken != "" {
		if err := e.store.InsertSubmission(ctx, &models.Submission{
			SubmissionID:      uuid.New().String(),
			TxID:              txid,
			CallbackURL:       opts.CallbackURL,
			CallbackToken:     opts.CallbackToken,
			FullStatusUpdates: opts.FullStatusUpdates,
			CreatedAt:         time.Now(),
		}); err != nil {
			return nil, fmt.Errorf("failed to store submission: %w", err)
		}
	}

	// Submit to all teranode endpoints
	for _, endpoint := range e.teranodeClient.GetEndpoints() {
		go e.submitToTeranode(ctx, endpoint, tx.Bytes(), txid)
	}

	// Return current status
	return e.store.GetStatus(ctx, txid)
}

// SubmitTransactions submits multiple transactions for broadcast.
func (e *Embedded) SubmitTransactions(ctx context.Context, rawTxs [][]byte, opts *models.SubmitOptions) ([]*models.TransactionStatus, error) {
	if opts == nil {
		opts = &models.SubmitOptions{}
	}

	var txs []*sdkTx.Transaction
	for _, rawTx := range rawTxs {
		// Parse transaction (try BEEF first, then raw bytes)
		_, tx, _, err := sdkTx.ParseBeef(rawTx)
		if err != nil || tx == nil {
			tx, err = sdkTx.NewTransactionFromBytes(rawTx)
			if err != nil {
				return nil, fmt.Errorf("failed to parse transaction: %w", err)
			}
		}

		// Validate transaction
		if err := e.txValidator.ValidateTransaction(ctx, tx, opts.SkipFeeValidation, opts.SkipScriptValidation); err != nil {
			return nil, fmt.Errorf("validation failed: %w", err)
		}

		txs = append(txs, tx)
		txid := tx.TxID().String()

		// Insert status and track
		e.store.InsertStatus(ctx, &models.TransactionStatus{
			TxID:      txid,
			Timestamp: time.Now(),
		})
		e.txTracker.Add(txid, models.StatusReceived)

		// Create submission record if callback URL or token provided
		if opts.CallbackURL != "" || opts.CallbackToken != "" {
			e.store.InsertSubmission(ctx, &models.Submission{
				SubmissionID:      uuid.New().String(),
				TxID:              txid,
				CallbackURL:       opts.CallbackURL,
				CallbackToken:     opts.CallbackToken,
				FullStatusUpdates: opts.FullStatusUpdates,
				CreatedAt:         time.Now(),
			})
		}
	}

	// Submit all to teranode
	for _, tx := range txs {
		txid := tx.TxID().String()
		for _, endpoint := range e.teranodeClient.GetEndpoints() {
			go e.submitToTeranode(ctx, endpoint, tx.Bytes(), txid)
		}
	}

	// Collect statuses
	var responses []*models.TransactionStatus
	for _, tx := range txs {
		status, _ := e.store.GetStatus(ctx, tx.TxID().String())
		responses = append(responses, status)
	}

	return responses, nil
}

// GetStatus retrieves the current status of a transaction.
func (e *Embedded) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	status, err := e.store.GetStatus(ctx, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}
	if status == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	return status, nil
}

// Subscribe returns a channel for transaction status updates.
func (e *Embedded) Subscribe(ctx context.Context, callbackToken string) (<-chan *models.TransactionStatus, error) {
	subCtx, cancel := context.WithCancel(ctx)
	ch := e.arcade.SubscribeStatus(subCtx, callbackToken)

	e.subMu.Lock()
	e.subChans[ch] = cancel
	e.subMu.Unlock()

	// Auto-cleanup when context is done
	go func() {
		<-subCtx.Done()
		e.subMu.Lock()
		delete(e.subChans, ch)
		e.subMu.Unlock()
	}()

	return ch, nil
}

// Unsubscribe removes a subscription channel.
func (e *Embedded) Unsubscribe(ch <-chan *models.TransactionStatus) {
	e.subMu.Lock()
	defer e.subMu.Unlock()

	if cancel, ok := e.subChans[ch]; ok {
		cancel()
		delete(e.subChans, ch)
	}
}

// GetPolicy returns the transaction policy configuration.
func (e *Embedded) GetPolicy(ctx context.Context) (*models.Policy, error) {
	return e.policy, nil
}

// submitToTeranode submits a transaction to a teranode endpoint and updates status.
func (e *Embedded) submitToTeranode(ctx context.Context, endpoint string, rawTx []byte, txid string) {
	statusCode, err := e.teranodeClient.SubmitTransaction(ctx, endpoint, rawTx)
	if err != nil {
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusRejected,
			Timestamp: time.Now(),
			ExtraInfo: err.Error(),
		}
		e.store.UpdateStatus(ctx, status)
		e.eventPublisher.Publish(ctx, status)
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
	e.store.UpdateStatus(ctx, status)
	e.eventPublisher.Publish(ctx, status)
}
