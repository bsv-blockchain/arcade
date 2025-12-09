package chaintracks

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/bsv-blockchain/arcade"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Client is an HTTP client for arcade server with SSE support.
// It implements the arcade.Chaintracks interface.
type Client struct {
	baseURL    string
	httpClient *http.Client
	currentTip *arcade.BlockHeader
	tipMu      sync.RWMutex
}

// NewClient creates a new HTTP client for arcade server
func NewClient(baseURL string) *Client {
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")

	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

// SubscribeTip returns a channel for chain tip updates via SSE.
// The SSE connection is closed when the context is cancelled.
func (c *Client) SubscribeTip(ctx context.Context) <-chan *arcade.BlockHeader {
	ch := make(chan *arcade.BlockHeader, 1)

	go func() {
		defer close(ch)

		req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/tip/stream", nil)
		if err != nil {
			return
		}

		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Connection", "keep-alive")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return
		}

		c.readTipSSE(ctx, resp.Body, ch)
	}()

	return ch
}

// readTipSSE reads tip updates from an SSE stream
func (c *Client) readTipSSE(ctx context.Context, body io.Reader, ch chan<- *arcade.BlockHeader) {
	reader := bufio.NewReader(body)
	var lastHash *chainhash.Hash

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		data := strings.TrimPrefix(line, "data: ")
		if data == "" {
			continue
		}

		var blockHeader arcade.BlockHeader
		if err := json.Unmarshal([]byte(data), &blockHeader); err != nil {
			continue
		}

		if lastHash != nil && lastHash.IsEqual(&blockHeader.Hash) {
			continue
		}

		lastHash = &blockHeader.Hash

		c.tipMu.Lock()
		c.currentTip = &blockHeader
		c.tipMu.Unlock()

		select {
		case ch <- &blockHeader:
		case <-ctx.Done():
			return
		default:
			// Channel full, skip
		}
	}
}

// SubscribeStatus returns a channel for transaction status updates via SSE.
// If token is empty, subscribes to all status updates.
// If token is provided, subscribes to updates for that callback token.
// The SSE connection is closed when the context is cancelled.
func (c *Client) SubscribeStatus(ctx context.Context, token string) <-chan *models.TransactionStatus {
	ch := make(chan *models.TransactionStatus, 100)

	go func() {
		defer close(ch)

		url := c.baseURL + "/events/" + token
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return
		}

		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Connection", "keep-alive")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return
		}

		c.readStatusSSE(ctx, resp.Body, ch)
	}()

	return ch
}

// readStatusSSE reads status updates from an SSE stream
func (c *Client) readStatusSSE(ctx context.Context, body io.Reader, ch chan<- *models.TransactionStatus) {
	reader := bufio.NewReader(body)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		data := strings.TrimPrefix(line, "data: ")
		if data == "" {
			continue
		}

		var status models.TransactionStatus
		if err := json.Unmarshal([]byte(data), &status); err != nil {
			continue
		}

		select {
		case ch <- &status:
		case <-ctx.Done():
			return
		default:
			// Channel full, skip
		}
	}
}

// GetTip returns the current chain tip (cached from last SSE update)
func (c *Client) GetTip(_ context.Context) *arcade.BlockHeader {
	c.tipMu.RLock()
	defer c.tipMu.RUnlock()
	return c.currentTip
}

// GetHeight returns the current chain height (cached from last SSE update)
func (c *Client) GetHeight(_ context.Context) uint32 {
	c.tipMu.RLock()
	defer c.tipMu.RUnlock()
	if c.currentTip == nil {
		return 0
	}
	return c.currentTip.Height
}

// GetHeaderByHeight retrieves a header by height from the server
func (c *Client) GetHeaderByHeight(ctx context.Context, height uint32) (*arcade.BlockHeader, error) {
	url := fmt.Sprintf("%s/header/height/%d", c.baseURL, height)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch header: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrServerRequestFailed, resp.StatusCode)
	}

	var header arcade.BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &header, nil
}

// GetHeaderByHash retrieves a header by hash from the server
func (c *Client) GetHeaderByHash(ctx context.Context, hash *chainhash.Hash) (*arcade.BlockHeader, error) {
	url := fmt.Sprintf("%s/header/hash/%s", c.baseURL, hash.String())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch header: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrServerRequestFailed, resp.StatusCode)
	}

	var header arcade.BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &header, nil
}

// IsValidRootForHeight implements the ChainTracker interface
func (c *Client) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	header, err := c.GetHeaderByHeight(ctx, height)
	if err != nil {
		return false, err
	}
	return header.MerkleRoot.IsEqual(root), nil
}

// CurrentHeight implements the ChainTracker interface
func (c *Client) CurrentHeight(ctx context.Context) (uint32, error) {
	return c.GetHeight(ctx), nil
}

// GetNetwork returns the network name from the server
func (c *Client) GetNetwork(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/network", nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch network: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var response struct {
		Network string `json:"network"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	return response.Network, nil
}
