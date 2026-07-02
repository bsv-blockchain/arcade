// Package merkleservice provides a client for communicating with the Merkle Service.
package merkleservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultTimeout = 30 * time.Second

// Client handles communication with the Merkle Service
type Client struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
	logger     *zap.Logger
}

// NewClient creates a new Merkle Service client
func NewClient(baseURL, authToken string, timeout time.Duration) *Client {
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return &Client{
		baseURL:   strings.TrimSuffix(baseURL, "/"),
		authToken: authToken,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

// SetLogger sets an optional logger for debug-level HTTP tracing.
func (c *Client) SetLogger(logger *zap.Logger) {
	c.logger = logger
}

// watchRequest is the payload sent to POST /watch.
// CallbackToken (when non-empty) tells merkle-service which bearer token to
// attach as `Authorization: Bearer <token>` on outbound callback delivery to
// arcade. arcade's /api/v1/merkle-service/callback receiver requires this
// header (PR #112 / F-018), so a missing token means callbacks 401. Empty
// values are omitted from the JSON to preserve back-compat with merkle-service
// builds that don't yet know the field.
type watchRequest struct {
	TxID          string `json:"txid"`
	CallbackURL   string `json:"callbackUrl"`
	CallbackToken string `json:"callbackToken,omitempty"`
}

// Register registers a transaction with the Merkle Service for watching.
// The Merkle Service will send callbacks to callbackURL when the transaction is seen or mined.
// callbackToken is forwarded so merkle-service can authenticate itself back to
// arcade on callback delivery; empty string disables forwarding (and the JSON
// field is omitted entirely thanks to omitempty).
func (c *Client) Register(ctx context.Context, txid, callbackURL, callbackToken string) error {
	body, err := json.Marshal(watchRequest{
		TxID:          txid,
		CallbackURL:   callbackURL,
		CallbackToken: callbackToken,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal watch request: %w", err)
	}

	url := c.baseURL + "/watch"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to register with merkle service: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		err := fmt.Errorf("POST %s: merkle service returned status %d (body: %s)", url, resp.StatusCode, string(body))
		if c.logger != nil {
			c.logger.Debug(
				"merkle service registration failed",
				zap.String("url", url),
				zap.String("txid", txid),
				zap.Int("status_code", resp.StatusCode),
				zap.String("response_body", string(body)),
			)
		}
		return err
	}

	return nil
}

// ReprocessError is returned by Reprocess when merkle-service responds
// with a non-2xx status. StatusCode carries the HTTP code so callers can
// distinguish transient infrastructure errors (5xx — retry soon) from
// terminal disagreements (4xx — block isn't on the consensus chain,
// back off heavily).
type ReprocessError struct {
	StatusCode int
	Body       string
}

func (e *ReprocessError) Error() string {
	return fmt.Sprintf("merkle service /reprocess returned status %d (body: %s)", e.StatusCode, e.Body)
}

// reprocessRequest is the payload sent to POST /reprocess.
type reprocessRequest struct {
	BlockHash     string `json:"blockHash"`
	CallbackURL   string `json:"callbackUrl"`
	CallbackToken string `json:"callbackToken,omitempty"`
}

// Reprocess asks merkle-service to re-emit STUMP + BLOCK_PROCESSED callbacks
// for blockHash to the given callbackURL. Used by the bump-builder watchdog
// to recover from missed BLOCK_PROCESSED deliveries. Returns nil on the
// expected HTTP 202 ack; non-2xx responses come back as *ReprocessError so
// callers can branch on StatusCode for backoff selection.
func (c *Client) Reprocess(ctx context.Context, blockHash, callbackURL, callbackToken string) error {
	body, err := json.Marshal(reprocessRequest{
		BlockHash:     blockHash,
		CallbackURL:   callbackURL,
		CallbackToken: callbackToken,
	})
	if err != nil {
		return fmt.Errorf("marshal reprocess request: %w", err)
	}

	url := c.baseURL + "/reprocess"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build reprocess request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post reprocess: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		fail := &ReprocessError{StatusCode: resp.StatusCode, Body: string(respBody)}
		if c.logger != nil {
			c.logger.Debug(
				"merkle service /reprocess failed",
				zap.String("url", url),
				zap.String("block_hash", blockHash),
				zap.Int("status_code", resp.StatusCode),
				zap.String("response_body", fail.Body),
			)
		}
		return fail
	}

	return nil
}

// Registration represents a single txid+callbackURL pair for batch registration.
// CallbackToken is the bearer token merkle-service should use when calling
// back to arcade for this registration; empty omits the field on the wire.
type Registration struct {
	TxID          string
	CallbackURL   string
	CallbackToken string
}

// RegisterBatch registers multiple transactions concurrently with bounded parallelism.
// Returns on first error with context cancellation (fail-fast).
func (c *Client) RegisterBatch(ctx context.Context, registrations []Registration, maxConcurrency int) error {
	if len(registrations) == 0 {
		return nil
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrency)

	for _, reg := range registrations {
		g.Go(func() error {
			return c.Register(gctx, reg.TxID, reg.CallbackURL, reg.CallbackToken)
		})
	}

	return g.Wait()
}

// RegisterBatchWithResults registers each transaction in parallel and returns
// a per-index error slice (nil = success). Unlike RegisterBatch this does NOT
// fail-fast: every registration runs to completion (or hits its own context
// timeout) so callers can partition the batch into "registered" vs "failed"
// without losing successes when one element errors.
//
// Used by the propagation flush to preserve F-024 ("registration is durable
// BEFORE broadcast") at batch granularity: the propagator skips broadcast for
// any tx whose entry is non-nil and routes it through handleRetryableFailure.
//
// Output length always equals len(registrations); errors[i] corresponds to
// registrations[i] in input order.
func (c *Client) RegisterBatchWithResults(ctx context.Context, registrations []Registration, maxConcurrency int) []error {
	if len(registrations) == 0 {
		return nil
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}

	errs := make([]error, len(registrations))
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	for i, reg := range registrations {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			errs[i] = ctx.Err()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			errs[i] = c.Register(ctx, reg.TxID, reg.CallbackURL, reg.CallbackToken)
		}()
	}
	wg.Wait()
	return errs
}
