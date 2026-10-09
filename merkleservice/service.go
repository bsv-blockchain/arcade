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
// so a consumer can resync one endpoint. *Pool implements it; *Client does
// not. The pool itself re-sends every registration an endpoint missed from a
// bounded per-endpoint catch-up queue; propagation type-asserts for this
// interface to run a full lookback replay against one endpoint when that
// queue overflowed and the dropped registrations are unknown.
type Recoverable interface {
	// Endpoints lists the configured base URLs, in config order.
	Endpoints() []string
	// Endpoint returns a Service that talks to exactly one endpoint (still
	// honoring its breaker and queueing its failures), or nil when baseURL
	// is not part of the pool.
	Endpoint(baseURL string) Service
	// OnResyncNeeded registers fn to run (synchronously, on the pool's
	// background goroutine) when an endpoint's catch-up queue overflowed and
	// has since drained. fn must return quickly; spawn work if needed.
	OnResyncNeeded(fn func(endpoint string))
}

// HealthReporter is the optional surface the api-server's /health uses to
// show per-endpoint breaker state. *Pool implements it.
type HealthReporter interface {
	EndpointStatuses() []EndpointStatus
}

// EndpointStatus is one endpoint's breaker and catch-up view for /health
// and tests.
type EndpointStatus struct {
	URL                 string     `json:"url"`
	Healthy             bool       `json:"healthy"`
	ConsecutiveFailures int        `json:"consecutiveFailures"`
	LastFailureAt       *time.Time `json:"lastFailureAt,omitempty"`
	OpenSince           *time.Time `json:"openSince,omitempty"`
	// CatchupPending is how many registrations the pool still owes this
	// endpoint (accepted elsewhere, not yet acknowledged here).
	CatchupPending int `json:"catchupPending"`
}

var (
	_ Service        = (*Client)(nil)
	_ Service        = (*Pool)(nil)
	_ Recoverable    = (*Pool)(nil)
	_ HealthReporter = (*Pool)(nil)
)
