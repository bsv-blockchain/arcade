package postgres

import (
	"context"
	"time"

	"github.com/bitcoin-sv/arcade/models"
	"github.com/bitcoin-sv/arcade/store"
)

// StatusStore implements store.StatusStore using PostgreSQL
type StatusStore struct {
	// TODO: Add database connection
}

// NewStatusStore creates a new PostgreSQL status store
func NewStatusStore(connectionString string) (store.StatusStore, error) {
	// TODO: Implement PostgreSQL connection
	return &StatusStore{}, nil
}

func (s *StatusStore) InsertStatus(ctx context.Context, status *models.TransactionStatus) error {
	// TODO: Implement
	return nil
}

func (s *StatusStore) UpdateStatus(ctx context.Context, status *models.TransactionStatus) error {
	// TODO: Implement
	return nil
}

func (s *StatusStore) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	// TODO: Implement
	return nil, nil
}

func (s *StatusStore) GetStatusesSince(ctx context.Context, since time.Time) ([]*models.TransactionStatus, error) {
	// TODO: Implement
	return nil, nil
}

func (s *StatusStore) Close() error {
	// TODO: Implement
	return nil
}

// SubmissionStore implements store.SubmissionStore using PostgreSQL
type SubmissionStore struct {
	// TODO: Add database connection
}

// NewSubmissionStore creates a new PostgreSQL submission store
func NewSubmissionStore(connectionString string) (store.SubmissionStore, error) {
	// TODO: Implement PostgreSQL connection
	return &SubmissionStore{}, nil
}

func (s *SubmissionStore) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	// TODO: Implement
	return nil
}

func (s *SubmissionStore) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	// TODO: Implement
	return nil, nil
}

func (s *SubmissionStore) GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error) {
	// TODO: Implement
	return nil, nil
}

func (s *SubmissionStore) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	// TODO: Implement
	return nil
}

func (s *SubmissionStore) Close() error {
	// TODO: Implement
	return nil
}
