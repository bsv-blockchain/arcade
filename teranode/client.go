package teranode

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultTimeout = 30 * time.Second
)

// Client handles communication with teranode endpoints
type Client struct {
	endpoints  []string
	httpClient *http.Client
}

// NewClient creates a new teranode client
func NewClient(endpoints []string) *Client {
	return &Client{
		endpoints: endpoints,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
}

// SubmitTransaction submits a transaction to a single endpoint
// Returns the HTTP status code (200 = accepted, 202 = queued)
func (c *Client) SubmitTransaction(ctx context.Context, endpoint string, rawTx []byte) (int, error) {
	url := endpoint + "/tx"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rawTx))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to submit transaction: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	return resp.StatusCode, nil
}

// GetEndpoints returns the configured endpoints
func (c *Client) GetEndpoints() []string {
	return c.endpoints
}
