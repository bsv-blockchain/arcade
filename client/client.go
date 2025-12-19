// Package client provides a REST client for the Arcade API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/service"
)

// Ensure Client implements service.ArcadeService
var _ service.ArcadeService = (*Client)(nil)

// Client is an HTTP client for the Arcade REST API.
type Client struct {
	baseURL    string
	httpClient *http.Client

	// SSE state
	sseManager *sseManager
}

// New creates a new Arcade REST client.
func New(baseURL string, opts ...Option) *Client {
	// Remove trailing slash
	baseURL = strings.TrimSuffix(baseURL, "/")

	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Apply options
	for _, opt := range opts {
		opt(c)
	}

	// Initialize SSE manager
	c.sseManager = newSSEManager(c)

	return c
}

// SubmitTransaction submits a single transaction for broadcast.
func (c *Client) SubmitTransaction(ctx context.Context, rawTx []byte, opts *models.SubmitOptions) (*models.TransactionStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/tx", bytes.NewReader(rawTx))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	c.setSubmitHeaders(req, opts)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to submit transaction: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var status models.TransactionStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &status, nil
}

// SubmitTransactions submits multiple transactions for broadcast.
func (c *Client) SubmitTransactions(ctx context.Context, rawTxs [][]byte, opts *models.SubmitOptions) ([]*models.TransactionStatus, error) {
	// Convert to JSON format expected by server
	type txRequest struct {
		RawTx string `json:"rawTx"`
	}

	var reqs []txRequest
	for _, rawTx := range rawTxs {
		reqs = append(reqs, txRequest{RawTx: fmt.Sprintf("%x", rawTx)})
	}

	body, err := json.Marshal(reqs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/txs", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.setSubmitHeaders(req, opts)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to submit transactions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var statuses []*models.TransactionStatus
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return statuses, nil
}

// GetStatus retrieves the current status of a transaction.
func (c *Client) GetStatus(ctx context.Context, txid string) (*models.TransactionStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/tx/"+txid, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("transaction not found")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var status models.TransactionStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &status, nil
}

// Subscribe returns a channel for transaction status updates.
func (c *Client) Subscribe(ctx context.Context, callbackToken string) (<-chan *models.TransactionStatus, error) {
	return c.sseManager.subscribe(ctx, callbackToken)
}

// Unsubscribe removes a subscription channel.
func (c *Client) Unsubscribe(ch <-chan *models.TransactionStatus) {
	c.sseManager.unsubscribe(ch)
}

// GetPolicy returns the transaction policy configuration.
func (c *Client) GetPolicy(ctx context.Context) (*models.Policy, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/policy", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get policy: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseErrorResponse(resp)
	}

	var policy models.Policy
	if err := json.NewDecoder(resp.Body).Decode(&policy); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &policy, nil
}

// setSubmitHeaders sets the X-* headers from SubmitOptions.
func (c *Client) setSubmitHeaders(req *http.Request, opts *models.SubmitOptions) {
	if opts == nil {
		return
	}
	if opts.CallbackURL != "" {
		req.Header.Set("X-CallbackUrl", opts.CallbackURL)
	}
	if opts.CallbackToken != "" {
		req.Header.Set("X-CallbackToken", opts.CallbackToken)
	}
	if opts.FullStatusUpdates {
		req.Header.Set("X-FullStatusUpdates", "true")
	}
	if opts.SkipFeeValidation {
		req.Header.Set("X-SkipFeeValidation", "true")
	}
	if opts.SkipScriptValidation {
		req.Header.Set("X-SkipScriptValidation", "true")
	}
}

// parseErrorResponse extracts an error message from an HTTP response.
func (c *Client) parseErrorResponse(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)

	// Try to parse as JSON error
	var errResp struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		return fmt.Errorf("%s: %s", resp.Status, errResp.Error)
	}

	// Return raw body if not JSON
	if len(body) > 0 {
		return fmt.Errorf("%s: %s", resp.Status, string(body))
	}

	return fmt.Errorf("%s", resp.Status)
}

// Close closes the client and any active connections.
func (c *Client) Close() error {
	c.sseManager.close()
	return nil
}
