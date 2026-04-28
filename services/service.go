package services

import "context"

// Service is the common interface all arcade services implement.
// Follows the Teranode daemon pattern.
type Service interface {
	// Start initializes connections and begins processing.
	// The context is used for lifecycle management — cancellation signals shutdown.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the service, completing in-flight operations.
	Stop() error

	// Name returns the service identifier for logging and configuration.
	Name() string
}
