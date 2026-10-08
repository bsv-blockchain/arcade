package merkleservice

import (
	"context"
	"time"
)

// Service is the merkle-service surface arcade's services depend on. The
// single-endpoint *Client and the multi-endpoint *Pool both satisfy it, so a
// consumer never needs to know how many merkle-services are configured: it
// registers a tx or asks for a block reprocess, and the implementation decides
// which endpoints that reaches.
type Service interface {
	// Register registers one txid for watching. See Client.Register.
	Register(ctx context.Context, txid, callbackURL, callbackToken string) error
	// RegisterBatch registers many txids and fails fast on the first error.
	// See Client.RegisterBatch.
	RegisterBatch(ctx context.Context, registrations []Registration, maxConcurrency int) error
	// RegisterBatchWithResults registers many txids and reports one error per
	// input index (nil = registered). See Client.RegisterBatchWithResults.
	RegisterBatchWithResults(ctx context.Context, registrations []Registration, maxConcurrency int) []error
	// Reprocess asks for a block's STUMP + BLOCK_PROCESSED callbacks to be
	// re-emitted. See Client.Reprocess.
	Reprocess(ctx context.Context, blockHash, callbackURL, callbackToken string) error
}

// Recoverable is the optional surface a multi-endpoint implementation exposes
// so a consumer can react when an endpoint comes back after an outage. *Pool
// implements it; *Client does not. propagation type-asserts for it to run the
// per-endpoint catch-up replay (re-registering the txs that endpoint missed
// while its breaker was open).
type Recoverable interface {
	// Endpoints lists the configured base URLs, in config order.
	Endpoints() []string
	// Endpoint returns a Service that talks to exactly one endpoint, bypassing
	// the breaker, or nil when baseURL is not part of the pool.
	Endpoint(baseURL string) Service
	// OnRecovered registers fn to run (synchronously, on the pool's probe
	// goroutine) each time an endpoint's breaker closes. openedAt is when the
	// breaker opened, i.e. the start of the window the endpoint may have
	// missed registrations for. fn must return quickly; spawn work if needed.
	OnRecovered(fn func(endpoint string, openedAt time.Time))
}

// HealthReporter is the optional surface the api-server's /health uses to
// show per-endpoint breaker state. *Pool implements it.
type HealthReporter interface {
	EndpointStatuses() []EndpointStatus
}

// EndpointStatus is one endpoint's breaker view for /health and tests.
type EndpointStatus struct {
	URL                 string     `json:"url"`
	Healthy             bool       `json:"healthy"`
	ConsecutiveFailures int        `json:"consecutiveFailures"`
	LastFailureAt       *time.Time `json:"lastFailureAt,omitempty"`
	OpenSince           *time.Time `json:"openSince,omitempty"`
}

var (
	_ Service        = (*Client)(nil)
	_ Service        = (*Pool)(nil)
	_ Recoverable    = (*Pool)(nil)
	_ HealthReporter = (*Pool)(nil)
)
