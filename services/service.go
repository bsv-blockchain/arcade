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

	// NotifyReady registers fn, which Start invokes exactly once after the
	// service's startup sequence has succeeded. Start blocks for the process
	// lifetime, so returning from it cannot mean "started"; this callback is
	// how the supervisor learns that /ready may open. The supervisor calls
	// NotifyReady before Start. Implement it by embedding ReadyHook and
	// calling SignalReady at the startup-success point.
	NotifyReady(fn func())
}
