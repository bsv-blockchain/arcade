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

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Client is an HTTP client for arcade server with SSE support
type Client struct {
	baseURL    string
	httpClient *http.Client
	currentTip *BlockHeader
	tipMu      sync.RWMutex
	msgChan    chan *BlockHeader
	cancelFunc context.CancelFunc
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

// Start connects to the SSE stream and returns a channel for tip updates
func (cc *Client) Start(ctx context.Context) (<-chan *BlockHeader, error) {
	cc.msgChan = make(chan *BlockHeader, 1)

	childCtx, cancel := context.WithCancel(ctx)
	cc.cancelFunc = cancel

	req, err := http.NewRequestWithContext(childCtx, "GET", cc.baseURL+"/tip/stream", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSE request: %w", err)
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := cc.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SSE stream: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: status %d", ErrSSEStreamFailed, resp.StatusCode)
	}

	go cc.readSSE(childCtx, resp.Body)

	return cc.msgChan, nil
}

// readSSE reads Server-Sent Events from the response body
//
//nolint:gocyclo // Inherent complexity of SSE parsing logic
func (cc *Client) readSSE(ctx context.Context, body io.ReadCloser) {
	defer func() { _ = body.Close() }()
	defer close(cc.msgChan)

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
			if err != io.EOF {
				return
			}
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

		var blockHeader BlockHeader
		if err := json.Unmarshal([]byte(data), &blockHeader); err != nil {
			continue
		}

		if lastHash != nil && lastHash.IsEqual(&blockHeader.Hash) {
			continue
		}

		lastHash = &blockHeader.Hash

		cc.tipMu.Lock()
		cc.currentTip = &blockHeader
		cc.tipMu.Unlock()

		select {
		case cc.msgChan <- &blockHeader:
		case <-ctx.Done():
			return
		default:
		}
	}
}

// Stop closes the SSE connection
func (cc *Client) Stop() error {
	if cc.cancelFunc != nil {
		cc.cancelFunc()
	}
	return nil
}

// GetTip returns the current chain tip
func (cc *Client) GetTip(_ context.Context) *BlockHeader {
	cc.tipMu.RLock()
	defer cc.tipMu.RUnlock()
	return cc.currentTip
}

// GetHeight returns the current chain height
func (cc *Client) GetHeight(_ context.Context) uint32 {
	cc.tipMu.RLock()
	defer cc.tipMu.RUnlock()
	if cc.currentTip == nil {
		return 0
	}
	return cc.currentTip.Height
}

// GetHeaderByHeight retrieves a header by height from the server
func (cc *Client) GetHeaderByHeight(ctx context.Context, height uint32) (*BlockHeader, error) {
	url := fmt.Sprintf("%s/header/height/%d", cc.baseURL, height)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := cc.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch header: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrServerRequestFailed, resp.StatusCode)
	}

	var header BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &header, nil
}

// GetHeaderByHash retrieves a header by hash from the server
func (cc *Client) GetHeaderByHash(ctx context.Context, hash *chainhash.Hash) (*BlockHeader, error) {
	url := fmt.Sprintf("%s/header/hash/%s", cc.baseURL, hash.String())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := cc.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch header: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrServerRequestFailed, resp.StatusCode)
	}

	var header BlockHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &header, nil
}

// IsValidRootForHeight implements the ChainTracker interface
func (cc *Client) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	header, err := cc.GetHeaderByHeight(ctx, height)
	if err != nil {
		return false, err
	}
	return header.MerkleRoot.IsEqual(root), nil
}

// CurrentHeight implements the ChainTracker interface
func (cc *Client) CurrentHeight(ctx context.Context) (uint32, error) {
	return cc.GetHeight(ctx), nil
}

// GetNetwork returns the network name from the server
func (cc *Client) GetNetwork(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", cc.baseURL+"/network", nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := cc.httpClient.Do(req)
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
