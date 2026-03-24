// Package merkleservice provides a client for communicating with the Merkle Service.
package merkleservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const defaultTimeout = 30 * time.Second

// Client handles communication with the Merkle Service
type Client struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
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
			Timeout: timeout,
		},
	}
}

// watchRequest is the payload sent to POST /watch
type watchRequest struct {
	TxID        string `json:"txid"`
	CallbackURL string `json:"callbackUrl"`
}

// Register registers a transaction with the Merkle Service for watching.
// The Merkle Service will send callbacks to callbackURL when the transaction is seen or mined.
func (c *Client) Register(ctx context.Context, txid, callbackURL string) error {
	body, err := json.Marshal(watchRequest{
		TxID:        txid,
		CallbackURL: callbackURL,
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
		return fmt.Errorf("merkle service returned status %d", resp.StatusCode)
	}

	return nil
}
