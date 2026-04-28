package store

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/arcade/models"
)

// defaultBatchConcurrency caps how many parallel single-record store calls
// the helpers issue. Set to keep DB pool pressure modest on backends that
// don't have a native batch implementation.
const defaultBatchConcurrency = 16

// SingleStore is the narrow contract the parallel-loop helpers need: the
// single-record GetOrInsertStatus / UpdateStatus methods. Every Store
// satisfies this trivially. Defined as an interface so the helpers can be
// reused by any backend's BatchGetOrInsertStatus / BatchUpdateStatus
// implementation that wants the parallel-loop fallback.
type SingleStore interface {
	GetOrInsertStatus(ctx context.Context, status *models.TransactionStatus) (*models.TransactionStatus, bool, error)
	UpdateStatus(ctx context.Context, status *models.TransactionStatus) error
}

// BatchGetOrInsertStatusParallel runs GetOrInsertStatus concurrently for each
// row, bounded by defaultBatchConcurrency. Result order matches input order.
// Returns the first error encountered by any goroutine; rows whose call
// failed get a zero-value BatchInsertResult so callers can still iterate the
// slice safely.
func BatchGetOrInsertStatusParallel(ctx context.Context, s SingleStore, statuses []*models.TransactionStatus) ([]BatchInsertResult, error) {
	if len(statuses) == 0 {
		return nil, nil
	}

	results := make([]BatchInsertResult, len(statuses))
	sem := make(chan struct{}, defaultBatchConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, st := range statuses {
		i, st := i, st
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			mu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			existing, inserted, err := s.GetOrInsertStatus(ctx, st)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			if inserted {
				results[i] = BatchInsertResult{Inserted: true}
			} else {
				results[i] = BatchInsertResult{Existing: existing, Inserted: false}
			}
		}()
	}
	wg.Wait()
	return results, firstErr
}

// BatchUpdateStatusParallel runs UpdateStatus concurrently for each row,
// bounded by defaultBatchConcurrency. Returns the first error encountered.
func BatchUpdateStatusParallel(ctx context.Context, s SingleStore, statuses []*models.TransactionStatus) error {
	if len(statuses) == 0 {
		return nil
	}

	sem := make(chan struct{}, defaultBatchConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, st := range statuses {
		st := st
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			mu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.UpdateStatus(ctx, st); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}
