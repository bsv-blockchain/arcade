// Package service defines the core Arcade service interface.
package service

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// ArcadeService defines the interface for interacting with Arcade.
// This interface can be satisfied by:
//   - Embedded implementation: Direct access to Arcade internals (for in-process use)
//   - REST client: HTTP client for remote Arcade servers
type ArcadeService interface {
	// SubmitTransaction submits a single transaction for broadcast.
	// rawTx can be raw bytes or BEEF format.
	SubmitTransaction(ctx context.Context, rawTx []byte, opts *models.SubmitOptions) (*models.TransactionStatus, error)

	// SubmitTransactions submits multiple transactions for broadcast.
	SubmitTransactions(ctx context.Context, rawTxs [][]byte, opts *models.SubmitOptions) ([]*models.TransactionStatus, error)

	// GetStatus retrieves the current status of a transaction.
	GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error)

	// Subscribe returns a channel for transaction status updates.
	// If callbackToken is empty, all status updates are returned.
	// If callbackToken is provided, only updates for that token are returned.
	Subscribe(ctx context.Context, callbackToken string) (<-chan *models.TransactionStatus, error)

	// Unsubscribe removes a subscription channel.
	Unsubscribe(ch <-chan *models.TransactionStatus)

	// GetPolicy returns the transaction policy configuration.
	GetPolicy(ctx context.Context) (*models.Policy, error)
}
