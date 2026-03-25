// Package embedded provides an in-process implementation of the ArcadeService interface.
package embedded

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/google/uuid"

	"github.com/bsv-blockchain/arcade"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/service"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
)

// Ensure Embedded implements service.ArcadeService
var _ service.ArcadeService = (*Embedded)(nil)

// Static errors for embedded service.
var (
	errStoreRequired          = errors.New("store is required")
	errTxTrackerRequired      = errors.New("TxTracker is required")
	errEventPublisherRequired = errors.New("EventPublisher is required")
	errTeranodeClientRequired = errors.New("TeranodeClient is required")
	errTxValidatorRequired    = errors.New("TxValidator is required")
	errArcadeRequired         = errors.New("arcade is required")
	errPolicyRequired         = errors.New("policy is required")
	errTransactionNotFound    = errors.New("transaction not found")
)

// Config holds configuration for the embedded service.
type Config struct {
	Store                      store.Store
	TxTracker                  *store.TxTracker
	EventPublisher             events.Publisher
	TeranodeClient             *teranode.Client
	MerkleServiceClient        *merkleservice.Client // Optional: nil disables Merkle Service integration
	MerkleServiceCallbackURL   string                // Full callback URL sent to Merkle Service during registration
	TxValidator                *validator.Validator
	Arcade                     *arcade.Arcade
	Policy                     *models.Policy
	Logger                     *slog.Logger
}

// Embedded is an in-process implementation of ArcadeService.
type Embedded struct {
	store                    store.Store
	txTracker                *store.TxTracker
	eventPublisher           events.Publisher
	teranodeClient           *teranode.Client
	merkleServiceClient      *merkleservice.Client
	merkleServiceCallbackURL string
	txValidator              *validator.Validator
	arcade                   *arcade.Arcade
	policy                   *models.Policy
	logger                   *slog.Logger

	// Subscription tracking
	subMu    sync.RWMutex
	subChans map[<-chan *models.TransactionStatus]context.CancelFunc
}

// New creates a new Embedded service instance.
func New(cfg Config) (*Embedded, error) {
	if cfg.Store == nil {
		return nil, errStoreRequired
	}
	if cfg.TxTracker == nil {
		return nil, errTxTrackerRequired
	}
	if cfg.EventPublisher == nil {
		return nil, errEventPublisherRequired
	}
	if cfg.TeranodeClient == nil {
		return nil, errTeranodeClientRequired
	}
	if cfg.TxValidator == nil {
		return nil, errTxValidatorRequired
	}
	if cfg.Arcade == nil {
		return nil, errArcadeRequired
	}
	if cfg.Policy == nil {
		return nil, errPolicyRequired
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Embedded{
		store:                    cfg.Store,
		txTracker:                cfg.TxTracker,
		eventPublisher:           cfg.EventPublisher,
		teranodeClient:           cfg.TeranodeClient,
		merkleServiceClient:      cfg.MerkleServiceClient,
		merkleServiceCallbackURL: cfg.MerkleServiceCallbackURL,
		txValidator:              cfg.TxValidator,
		arcade:                   cfg.Arcade,
		policy:                   cfg.Policy,
		logger:                   cfg.Logger,
		subChans:                 make(map[<-chan *models.TransactionStatus]context.CancelFunc),
	}, nil
}

// SubmitTransaction submits a single transaction for broadcast.
//
//nolint:gocyclo // complex transaction validation logic
func (e *Embedded) SubmitTransaction(ctx context.Context, rawTx []byte, opts *models.SubmitOptions) (*models.TransactionStatus, error) {
	if opts == nil {
		opts = &models.SubmitOptions{}
	}

	// Parse transaction (try BEEF first, then raw bytes)
	_, tx, _, err := sdkTx.ParseBeef(rawTx)
	if err != nil || tx == nil {
		tx, err = sdkTx.NewTransactionFromBytes(rawTx)
		if err != nil {
			e.logger.Debug("failed to parse transaction",
				"error", err.Error(),
				"rawTxSize", len(rawTx),
			)
			return nil, fmt.Errorf("failed to parse transaction: %w", err)
		}
	}

	txid := tx.TxID().String()

	// Check for existing status before validation — duplicate submissions return existing status
	existingStatus, isNew, err := e.store.GetOrInsertStatus(ctx, &models.TransactionStatus{
		TxID:      txid,
		Timestamp: time.Now(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to store status: %w", err)
	}

	if !isNew {
		e.logger.Debug("duplicate transaction submission",
			"txid", txid,
			"existingStatus", existingStatus.Status,
		)
		e.txTracker.Add(txid, existingStatus.Status)
		return existingStatus, nil
	}

	// Validate transaction
	if valErr := e.txValidator.ValidateTransaction(ctx, tx, opts.SkipFeeValidation, opts.SkipScriptValidation); valErr != nil {
		// Calculate actual fee for logging
		var inputSats, outputSats uint64
		for _, input := range tx.Inputs {
			if input.SourceTxSatoshis() != nil {
				inputSats += *input.SourceTxSatoshis()
			}
		}
		for _, output := range tx.Outputs {
			outputSats += output.Satoshis
		}
		var feePerKB float64
		txSize := tx.Size()
		if txSize > 0 && inputSats >= outputSats {
			feePerKB = float64(inputSats-outputSats) / float64(txSize) * 1000
		}

		e.logger.Debug("transaction validation failed",
			"txid", txid,
			"error", valErr.Error(),
			"skipFeeValidation", opts.SkipFeeValidation,
			"skipScriptValidation", opts.SkipScriptValidation,
			"txSize", txSize,
			"inputCount", len(tx.Inputs),
			"outputCount", len(tx.Outputs),
			"inputSatoshis", inputSats,
			"outputSatoshis", outputSats,
			"feePerKB", feePerKB,
			"minFeePerKB", e.txValidator.MinFeePerKB(),
			"rawTxHex", tx.Hex(),
		)
		return nil, fmt.Errorf("validation failed: %w", valErr)
	}

	// Track transaction in memory
	e.txTracker.Add(txid, existingStatus.Status)

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

	// Register with Merkle Service before broadcasting (best-effort)
	e.registerWithMerkleService(ctx, txid)
	e.logger.Info("transaction validated and registered with Merkle Service",)

	// Submit to teranode endpoints synchronously with timeout
	// Wait for first success/rejection, or timeout after 15 seconds
	endpoints := e.teranodeClient.GetEndpoints()
	resultCh := make(chan *models.TransactionStatus, len(endpoints))
	submitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			status := e.submitToTeranodeSync(submitCtx, ep, tx.Bytes(), txid)
			select {
			case resultCh <- status:
			case <-submitCtx.Done():
				return
			}
		}(endpoint)
	}

	// Close channel when all goroutines complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Wait for first result or timeout
	select {
	case status := <-resultCh:
		return status, nil
	case <-submitCtx.Done():
		// Timeout - return current status (RECEIVED)
		return e.store.GetStatus(ctx, txid)
	}
}

// registerWithMerkleService registers a transaction with the Merkle Service.
// Failures are logged but do not block the broadcast.
func (e *Embedded) registerWithMerkleService(ctx context.Context, txid string) {
	e.logger.Info("registering transaction with Merkle Service",)
	if e.merkleServiceClient == nil || e.merkleServiceCallbackURL == "" {
		return
	}

	if err := e.merkleServiceClient.Register(ctx, txid, e.merkleServiceCallbackURL); err != nil {
		e.logger.Warn("failed to register transaction with Merkle Service",
			slog.String("txid", txid),
			slog.String("error", err.Error()))
	}
}

// SubmitTransactions submits multiple transactions for broadcast.
//
//nolint:gocyclo // complex batch transaction validation logic
func (e *Embedded) SubmitTransactions(ctx context.Context, rawTxs [][]byte, opts *models.SubmitOptions) ([]*models.TransactionStatus, error) {
	if opts == nil {
		opts = &models.SubmitOptions{}
	}

	// Process each transaction: get or insert status, register callbacks
	type txInfo struct {
		tx     *sdkTx.Transaction
		rawTx  []byte
		txid   string
		isNew  bool
		status *models.TransactionStatus
	}
	txInfos := make([]txInfo, 0, len(rawTxs))

	for _, rawTx := range rawTxs {
		// Parse transaction (try BEEF first, then raw bytes)
		_, tx, _, err := sdkTx.ParseBeef(rawTx)
		if err != nil || tx == nil {
			tx, err = sdkTx.NewTransactionFromBytes(rawTx)
			if err != nil {
				return nil, fmt.Errorf("failed to parse transaction: %w", err)
			}
		}

		txid := tx.TxID().String()

		// Check for existing status before validation — duplicate submissions return existing status
		existingStatus, isNew, err := e.store.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID:      txid,
			Timestamp: time.Now(),
		})
		if err != nil {
			// Log error but continue with other transactions
			continue
		}

		if !isNew {
			e.logger.Debug("duplicate transaction submission",
				"txid", txid,
				"existingStatus", existingStatus.Status,
			)
			e.txTracker.Add(txid, existingStatus.Status)
			txInfos = append(txInfos, txInfo{tx: tx, rawTx: rawTx, txid: txid, isNew: false, status: existingStatus})
			continue
		}

		// Validate transaction (only for new submissions)
		if valErr := e.txValidator.ValidateTransaction(ctx, tx, opts.SkipFeeValidation, opts.SkipScriptValidation); valErr != nil {
			// Calculate actual fee for logging
			var inputSats, outputSats uint64
			for _, input := range tx.Inputs {
				if input.SourceTxSatoshis() != nil {
					inputSats += *input.SourceTxSatoshis()
				}
			}
			for _, output := range tx.Outputs {
				outputSats += output.Satoshis
			}
			var feePerKB float64
			txSize := tx.Size()
			if txSize > 0 && inputSats >= outputSats {
				feePerKB = float64(inputSats-outputSats) / float64(txSize) * 1000
			}

			e.logger.Debug("transaction validation failed",
				"txid", txid,
				"error", valErr.Error(),
				"skipFeeValidation", opts.SkipFeeValidation,
				"skipScriptValidation", opts.SkipScriptValidation,
				"txSize", txSize,
				"inputCount", len(tx.Inputs),
				"outputCount", len(tx.Outputs),
				"inputSatoshis", inputSats,
				"outputSatoshis", outputSats,
				"feePerKB", feePerKB,
				"minFeePerKB", e.txValidator.MinFeePerKB(),
			)
			return nil, fmt.Errorf("validation failed: %w", valErr)
		}

		e.txTracker.Add(txid, existingStatus.Status)

		// Create submission record if callback URL or token provided
		// This happens regardless of whether the transaction is new
		if opts.CallbackURL != "" || opts.CallbackToken != "" {
			if err := e.store.InsertSubmission(ctx, &models.Submission{
				SubmissionID:      uuid.New().String(),
				TxID:              txid,
				CallbackURL:       opts.CallbackURL,
				CallbackToken:     opts.CallbackToken,
				FullStatusUpdates: opts.FullStatusUpdates,
				CreatedAt:         time.Now(),
			}); err != nil {
				e.logger.Error("failed to insert submission", slog.String("txID", txid), slog.String("error", err.Error()))
			}
		}

		txInfos = append(txInfos, txInfo{tx: tx, rawTx: rawTx, txid: txid, isNew: isNew, status: existingStatus})
	}

	// Separate transactions into those that need broadcasting and those that don't
	responses := make([]*models.TransactionStatus, 0, len(txInfos))
	var toBroadcast []txInfo
	for _, info := range txInfos {
		if !info.isNew {
			//nolint:exhaustive // intentionally only handling terminal states
			switch info.status.Status {
			case models.StatusSeenOnNetwork, models.StatusMined, models.StatusImmutable,
				models.StatusRejected, models.StatusDoubleSpendAttempted:
				responses = append(responses, info.status)
				continue
			default:
				// Still pending (RECEIVED, SENT_TO_NETWORK, ACCEPTED_BY_NETWORK) - rebroadcast
			}
		}
		toBroadcast = append(toBroadcast, info)
	}

	if len(toBroadcast) == 0 {
		return responses, nil
	}

	// Register all with Merkle Service before broadcasting (best-effort)
	for _, info := range toBroadcast {
		e.registerWithMerkleService(ctx, info.txid)
	}

	// Collect raw tx bytes for batch submission
	batchTxs := make([][]byte, len(toBroadcast))
	for i, info := range toBroadcast {
		batchTxs[i] = info.tx.Bytes()
	}

	// Submit batch to each teranode endpoint, use first success
	submitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	endpoints := e.teranodeClient.GetEndpoints()
	resultCh := make(chan error, len(endpoints))
	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		wg.Add(1)
		go func(ep string) {
			defer wg.Done()
			_, err := e.teranodeClient.SubmitTransactions(submitCtx, ep, batchTxs)
			select {
			case resultCh <- err:
			case <-submitCtx.Done():
			}
		}(endpoint)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Wait for first result
	var batchErr error
	select {
	case err := <-resultCh:
		batchErr = err
	case <-submitCtx.Done():
		batchErr = submitCtx.Err()
	}

	// Update statuses based on batch result
	now := time.Now()
	for _, info := range toBroadcast {
		var status *models.TransactionStatus
		if batchErr != nil {
			e.logger.Debug("transaction rejected by network (batch)",
				slog.String("txid", info.txid),
				slog.String("reason", batchErr.Error()))
			status = &models.TransactionStatus{
				TxID:      info.txid,
				Status:    models.StatusRejected,
				Timestamp: now,
				ExtraInfo: batchErr.Error(),
			}
		} else {
			status = &models.TransactionStatus{
				TxID:      info.txid,
				Status:    models.StatusAcceptedByNetwork,
				Timestamp: now,
			}
		}
		if err := e.store.UpdateStatus(ctx, status); err != nil {
			e.logger.Error("failed to update status", slog.String("txID", info.txid), slog.String("error", err.Error()))
		}
		if err := e.eventPublisher.Publish(ctx, status); err != nil {
			e.logger.Error("failed to publish status", slog.String("txID", info.txid), slog.String("error", err.Error()))
		}
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
		return nil, errTransactionNotFound
	}
	return status, nil
}

// Subscribe returns a channel for transaction status updates.
func (e *Embedded) Subscribe(ctx context.Context, callbackToken string) (<-chan *models.TransactionStatus, error) {
	subCtx, cancel := context.WithCancel(ctx) //nolint:gosec // G118: cancel stored in subChans map, called via Unsubscribe
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
func (e *Embedded) GetPolicy(_ context.Context) (*models.Policy, error) {
	return e.policy, nil
}

// submitToTeranodeSync submits a transaction to a teranode endpoint, updates status, and returns the result.
func (e *Embedded) submitToTeranodeSync(ctx context.Context, endpoint string, rawTx []byte, txid string) *models.TransactionStatus {
	statusCode, err := e.teranodeClient.SubmitTransaction(ctx, endpoint, rawTx)
	if err != nil {
		e.logger.Debug("transaction rejected by network",
			slog.String("txid", txid),
			slog.String("endpoint", endpoint),
			slog.String("reason", err.Error()))
		status := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusRejected,
			Timestamp: time.Now(),
			ExtraInfo: err.Error(),
		}
		if err := e.store.UpdateStatus(ctx, status); err != nil {
			e.logger.Error("failed to update status", slog.String("txID", txid), slog.String("error", err.Error()))
		}
		if err := e.eventPublisher.Publish(ctx, status); err != nil {
			e.logger.Error("failed to publish status", slog.String("txID", txid), slog.String("error", err.Error()))
		}
		return status
	}

	var txStatus models.Status
	switch statusCode {
	case http.StatusOK:
		txStatus = models.StatusAcceptedByNetwork
	case http.StatusNoContent:
		txStatus = models.StatusSentToNetwork
	default:
		return nil
	}

	status := &models.TransactionStatus{
		TxID:      txid,
		Status:    txStatus,
		Timestamp: time.Now(),
	}
	if err := e.store.UpdateStatus(ctx, status); err != nil {
		e.logger.Error("failed to update status", slog.String("txID", txid), slog.String("error", err.Error()))
	}
	if err := e.eventPublisher.Publish(ctx, status); err != nil {
		e.logger.Error("failed to publish status", slog.String("txID", txid), slog.String("error", err.Error()))
	}
	return status
}
