// Package teranode provides a client for communicating with Teranode P2P network.
package teranode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
)

const (
	defaultTimeout = 30 * time.Second

	defaultFailureThreshold = 3
	defaultProbeInterval    = 30 * time.Second
	defaultProbeTimeout     = 2 * time.Second
)

var errUnexpectedStatusCode = errors.New("unexpected status code")

// healthState is the circuit-breaker state for a single endpoint.
type healthState int

const (
	stateHealthy healthState = iota
	stateUnhealthy
)

// healthSource records how an endpoint entered the client's registration list.
// Exposed for diagnostic health responses — not used on any hot path.
type healthSource int

const (
	sourceConfigured healthSource = iota // from NewClient static seed
	sourceDiscovered                     // from AddEndpoints at runtime
)

// endpointHealth tracks the running health of a single endpoint URL. It is
// guarded by the enclosing Client's RWMutex — the struct itself is not
// independently thread-safe.
type endpointHealth struct {
	consecutiveFailures int
	lastFailure         time.Time
	state               healthState
	source              healthSource
}

// EndpointStatus is a diagnostic snapshot of one endpoint's registration
// origin and current circuit-breaker state. Returned by GetEndpointStatuses
// for health-surface consumers; not used on any hot path.
type EndpointStatus struct {
	URL     string `json:"url"`
	Source  string `json:"source"` // "configured" or "discovered"
	Healthy bool   `json:"healthy"`
}

// HealthConfig tunes the per-endpoint circuit-breaker. Zero or negative values
// fall back to documented defaults inside NewClient. A nil Logger is replaced
// with zap.NewNop() so callers don't have to plumb a logger when they don't
// care about transition logs.
//
// Source + RefreshInterval enable distributed endpoint discovery: when Source
// is non-nil the client polls it on RefreshInterval and merges the URLs into
// the in-memory list via AddEndpoints. This is how a bump-builder pod sees
// URLs that the p2p-client pod discovered. Leave Source nil in monolith mode
// or in tests that don't care about discovery.
type HealthConfig struct {
	FailureThreshold    int
	ProbeInterval       time.Duration
	ProbeTimeout        time.Duration
	MinHealthyEndpoints int
	RefreshInterval     time.Duration
	Source              EndpointSource
	Logger              *zap.Logger
}

// EndpointSource produces a set of datahub URLs. The expected implementation
// is a thin adapter over the shared store; the interface keeps teranode free
// of a direct store dependency.
type EndpointSource interface {
	ListEndpointURLs(ctx context.Context) ([]string, error)
}

const defaultRefreshInterval = 30 * time.Second
const refreshStartupTimeout = 2 * time.Second

// Client handles communication with teranode endpoints. The endpoint list is
// mutable at runtime (see AddEndpoints) so peer-discovery services can merge
// additional propagation targets without reconstructing the client. `seen`
// tracks the normalized form (trailing-slash-trimmed) of every registered
// URL so duplicate announcements are cheap to reject. Each endpoint also
// carries a circuit-breaker entry in `health` so repeated failures sideline
// the endpoint from broadcasts; a background goroutine (started by Start)
// periodically probes sidelined endpoints to detect recovery.
type Client struct {
	mu         sync.RWMutex
	endpoints  []string
	seen       map[string]struct{}
	health     map[string]*endpointHealth
	authToken  string
	httpClient *http.Client

	failureThreshold    int
	probeInterval       time.Duration
	probeTimeout        time.Duration
	minHealthyEndpoints int
	refreshInterval     time.Duration
	source              EndpointSource
	logger              *zap.Logger
	belowThreshold      bool

	startOnce   sync.Once
	probeCancel context.CancelFunc
	probeDone   chan struct{}
	refreshDone chan struct{}
}

// normalizeURL trims a single trailing slash. Two peers announcing the same
// URL with and without a trailing slash should be treated as the same target.
func normalizeURL(u string) string {
	return strings.TrimSuffix(u, "/")
}

// NewClient creates a new teranode client. Statically configured endpoints
// are seeded into the dedup set so a subsequent peer announcement of the
// same URL is silently ignored. Every seeded and later-added endpoint starts
// in the healthy state; the circuit-breaker trips only after
// hc.FailureThreshold consecutive failures.
func NewClient(endpoints []string, authToken string, hc HealthConfig) *Client {
	if hc.FailureThreshold <= 0 {
		hc.FailureThreshold = defaultFailureThreshold
	}
	if hc.ProbeInterval <= 0 {
		hc.ProbeInterval = defaultProbeInterval
	}
	if hc.ProbeTimeout <= 0 {
		hc.ProbeTimeout = defaultProbeTimeout
	}
	if hc.MinHealthyEndpoints < 0 {
		hc.MinHealthyEndpoints = 0
	}
	if hc.RefreshInterval <= 0 {
		hc.RefreshInterval = defaultRefreshInterval
	}
	if hc.Logger == nil {
		hc.Logger = zap.NewNop()
	}
	c := &Client{
		seen:      make(map[string]struct{}, len(endpoints)),
		health:    make(map[string]*endpointHealth, len(endpoints)),
		authToken: authToken,
		httpClient: &http.Client{
			Timeout:   defaultTimeout,
			Transport: newBroadcastTransport(),
		},
		failureThreshold:    hc.FailureThreshold,
		probeInterval:       hc.ProbeInterval,
		probeTimeout:        hc.ProbeTimeout,
		minHealthyEndpoints: hc.MinHealthyEndpoints,
		refreshInterval:     hc.RefreshInterval,
		source:              hc.Source,
		logger:              hc.Logger.Named("teranode-client"),
	}
	for _, ep := range endpoints {
		n := normalizeURL(ep)
		if n == "" {
			continue
		}
		if _, ok := c.seen[n]; ok {
			continue
		}
		c.seen[n] = struct{}{}
		c.endpoints = append(c.endpoints, n)
		c.health[n] = &endpointHealth{state: stateHealthy, source: sourceConfigured}
		metrics.TeranodeEndpointHealth.WithLabelValues(n, "configured").Set(1)
	}
	c.refreshEndpointCountMetric()
	return c
}

// refreshEndpointCountMetric sets the per-source endpoint count gauges from the
// current registration state. Caller must hold c.mu (any kind) so it sees a
// consistent view; the gauge update itself is goroutine-safe.
func (c *Client) refreshEndpointCountMetric() {
	var configured, discovered float64
	for _, h := range c.health {
		switch h.source {
		case sourceConfigured:
			configured++
		case sourceDiscovered:
			discovered++
		}
	}
	metrics.TeranodeEndpointCount.WithLabelValues("configured").Set(configured)
	metrics.TeranodeEndpointCount.WithLabelValues("discovered").Set(discovered)
}

// Start launches the background probe goroutine and (when an EndpointSource
// is configured) the endpoint refresh goroutine. It is idempotent — calling
// Start more than once is a no-op after the first call. Both goroutines run
// until either the provided context is cancelled or Close is called.
//
// When an EndpointSource is configured, Start blocks briefly on a synchronous
// first refresh so a freshly started pod converges to the current registry
// before serving traffic. The wait is capped at refreshStartupTimeout so a
// slow store doesn't gate pod readiness.
func (c *Client) Start(ctx context.Context) {
	c.startOnce.Do(func() {
		if c.source != nil {
			c.refreshOnceWithTimeout(ctx, refreshStartupTimeout)
		}

		probeCtx, cancel := context.WithCancel(ctx)
		c.probeCancel = cancel
		c.probeDone = make(chan struct{})
		go c.probeLoop(probeCtx)

		if c.source != nil {
			c.refreshDone = make(chan struct{})
			go c.refreshLoop(probeCtx)
		}
	})
}

// Close stops the background goroutines and waits for them to exit. Safe to
// call even if Start was never invoked.
func (c *Client) Close() {
	if c.probeCancel != nil {
		c.probeCancel()
	}
	if c.probeDone != nil {
		<-c.probeDone
	}
	if c.refreshDone != nil {
		<-c.refreshDone
	}
}

// refreshLoop polls the EndpointSource on the configured interval and merges
// new URLs via AddEndpoints. The merge is idempotent (AddEndpoints dedupes by
// the seen map) so the loop is safe to run alongside p2p_client's direct
// AddEndpoints calls in monolith mode.
func (c *Client) refreshLoop(ctx context.Context) {
	defer close(c.refreshDone)
	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refreshOnce(ctx)
		}
	}
}

// refreshOnce queries the source for the current URL set and merges any new
// entries. Errors are logged at warn — a transient store glitch does not
// disrupt the in-memory list, which the circuit-breaker can still curate.
func (c *Client) refreshOnce(ctx context.Context) {
	urls, err := c.source.ListEndpointURLs(ctx)
	if err != nil {
		c.logger.Warn("endpoint refresh failed", zap.Error(err))
		return
	}
	if added := c.AddEndpoints(urls); added > 0 {
		c.logger.Info("endpoint refresh added urls",
			zap.Int("added", added),
			zap.Int("total", len(c.GetEndpoints())),
		)
	}
}

// refreshOnceWithTimeout runs the synchronous first refresh under a bounded
// timeout so Start cannot block indefinitely on a slow store.
func (c *Client) refreshOnceWithTimeout(ctx context.Context, d time.Duration) {
	cctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	c.refreshOnce(cctx)
}

// AddEndpoints merges the given URLs into the runtime endpoint list,
// deduplicating against both the static seed list and prior additions. Each
// newly registered URL is seeded into the health tracker in the healthy
// state. The return value is the number of URLs newly registered (zero if
// all were duplicates). Safe for concurrent callers.
func (c *Client) AddEndpoints(urls []string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	added := 0
	for _, u := range urls {
		n := normalizeURL(u)
		if n == "" {
			continue
		}
		if _, ok := c.seen[n]; ok {
			continue
		}
		c.seen[n] = struct{}{}
		c.endpoints = append(c.endpoints, n)
		c.health[n] = &endpointHealth{state: stateHealthy, source: sourceDiscovered}
		metrics.TeranodeEndpointHealth.WithLabelValues(n, "discovered").Set(1)
		added++
	}
	if added > 0 {
		c.refreshEndpointCountMetric()
	}
	return added
}

// RecordSuccess resets an endpoint's failure counter to zero and, if the
// endpoint was previously unhealthy, transitions it back to healthy.
// Unknown URLs are silently ignored so callers don't need to pre-check.
func (c *Client) RecordSuccess(url string) {
	n := normalizeURL(url)
	c.mu.Lock()
	h, ok := c.health[n]
	if !ok {
		c.mu.Unlock()
		return
	}
	transitioned := h.state == stateUnhealthy
	h.consecutiveFailures = 0
	h.state = stateHealthy
	source := h.source
	c.recomputeBelowThresholdLocked()
	c.mu.Unlock()
	if transitioned {
		metrics.TeranodeEndpointHealth.WithLabelValues(n, sourceLabel(source)).Set(1)
		c.logger.Info("endpoint healthy",
			zap.String("endpoint", n),
			zap.String("from", "unhealthy"),
			zap.String("to", "healthy"),
		)
	}
}

// RecordFailure increments the consecutive-failure counter for an endpoint
// and transitions it to unhealthy once the counter reaches failureThreshold.
// Unknown URLs are silently ignored.
func (c *Client) RecordFailure(url string) {
	n := normalizeURL(url)
	c.mu.Lock()
	h, ok := c.health[n]
	if !ok {
		c.mu.Unlock()
		return
	}
	h.consecutiveFailures++
	h.lastFailure = time.Now()
	transitioned := false
	if h.state == stateHealthy && h.consecutiveFailures >= c.failureThreshold {
		h.state = stateUnhealthy
		transitioned = true
	}
	source := h.source
	c.recomputeBelowThresholdLocked()
	c.mu.Unlock()
	if transitioned {
		metrics.TeranodeEndpointHealth.WithLabelValues(n, sourceLabel(source)).Set(0)
		c.logger.Warn("endpoint unhealthy",
			zap.String("endpoint", n),
			zap.Int("consecutive_failures", h.consecutiveFailures),
			zap.String("from", "healthy"),
			zap.String("to", "unhealthy"),
		)
	}
}

// sourceLabel converts the internal healthSource enum to the metric label value.
func sourceLabel(s healthSource) string {
	if s == sourceDiscovered {
		return "discovered"
	}
	return "configured"
}

// GetEndpoints returns a snapshot of the current endpoint list. The returned
// slice is a defensive copy so callers may iterate without external locking
// while concurrent AddEndpoints calls continue. Includes endpoints regardless
// of health state — use GetHealthyEndpoints for the broadcast view.
func (c *Client) GetEndpoints() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.endpoints))
	copy(out, c.endpoints)
	return out
}

// GetHealthyEndpoints returns a snapshot containing only endpoints whose
// circuit-breaker is in the healthy state, in the same registration order as
// GetEndpoints. Callers performing a broadcast should use this view so bad
// peers are transparently skipped.
func (c *Client) GetHealthyEndpoints() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.endpoints))
	for _, ep := range c.endpoints {
		if h, ok := c.health[ep]; ok && h.state == stateHealthy {
			out = append(out, ep)
		}
	}
	return out
}

// GetEndpointStatuses returns a diagnostic snapshot of every registered
// endpoint, in registration order, for use in health-check responses. Each
// entry records the URL, its source (configured vs discovered), and whether
// it is currently in the healthy set.
func (c *Client) GetEndpointStatuses() []EndpointStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]EndpointStatus, 0, len(c.endpoints))
	for _, ep := range c.endpoints {
		h, ok := c.health[ep]
		if !ok {
			continue
		}
		src := "configured"
		if h.source == sourceDiscovered {
			src = "discovered"
		}
		out = append(out, EndpointStatus{
			URL:     ep,
			Source:  src,
			Healthy: h.state == stateHealthy,
		})
	}
	return out
}

// recomputeBelowThresholdLocked refreshes the min-healthy warning state.
// Must be called with c.mu held for writing. Emits a single WARN log line on
// the false→true crossing and clears the flag on the true→false crossing.
// A minHealthyEndpoints value of 0 disables the warning entirely.
func (c *Client) recomputeBelowThresholdLocked() {
	if c.minHealthyEndpoints <= 0 {
		return
	}
	healthyCount := 0
	for _, ep := range c.endpoints {
		if h, ok := c.health[ep]; ok && h.state == stateHealthy {
			healthyCount++
		}
	}
	below := healthyCount < c.minHealthyEndpoints
	if below && !c.belowThreshold {
		c.belowThreshold = true
		c.logger.Warn("healthy endpoint count below minimum",
			zap.Int("healthy", healthyCount),
			zap.Int("min_healthy_endpoints", c.minHealthyEndpoints),
		)
	} else if !below && c.belowThreshold {
		c.belowThreshold = false
	}
}

// probeLoop runs on the client's probe interval and issues a lightweight
// GET <url>/health request to every endpoint currently marked unhealthy.
// Any HTTP response received — including 4xx / 5xx — is treated as success
// because the only question this probe answers is "can we reach the peer at
// all?". Transport errors and context timeouts are recorded as failures so a
// still-broken peer stays in the unhealthy set.
func (c *Client) probeLoop(ctx context.Context) {
	defer close(c.probeDone)
	ticker := time.NewTicker(c.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probeOnce(ctx)
		}
	}
}

// probeOnce collects the current unhealthy set under an RLock, then probes
// each endpoint concurrently so one slow probe doesn't block the others.
func (c *Client) probeOnce(ctx context.Context) {
	c.mu.RLock()
	var targets []string
	for _, ep := range c.endpoints {
		if h, ok := c.health[ep]; ok && h.state == stateUnhealthy {
			targets = append(targets, ep)
		}
	}
	c.mu.RUnlock()
	if len(targets) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, ep := range targets {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			c.probeEndpoint(ctx, url)
		}(ep)
	}
	wg.Wait()
}

// probeEndpoint issues a single GET /health. A non-nil HTTP response counts
// as reachable regardless of status code; a transport error or context
// timeout counts as a failure.
func (c *Client) probeEndpoint(ctx context.Context, endpoint string) {
	start := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, c.probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, endpoint+"/health", nil)
	if err != nil {
		observeRequest("probe", 0, start)
		c.RecordFailure(endpoint)
		return
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		observeRequest("probe", 0, start)
		c.RecordFailure(endpoint)
		return
	}
	observeRequest("probe", resp.StatusCode, start)
	drainAndClose(resp.Body)
	c.RecordSuccess(endpoint)
}

// observeRequest records latency + status class for an outbound HTTP request.
// statusCode == 0 means transport error (no HTTP response).
func observeRequest(op string, statusCode int, start time.Time) {
	metrics.TeranodeRequestDuration.WithLabelValues(op, metrics.ObserveStatusClass(statusCode)).Observe(time.Since(start).Seconds())
}

// SubmitTransaction submits a transaction to a single endpoint
// Returns the HTTP status code (200 = accepted, 202 = queued)
func (c *Client) SubmitTransaction(ctx context.Context, endpoint string, rawTx []byte) (int, error) {
	start := time.Now()
	url := endpoint + "/tx"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rawTx))
	if err != nil {
		observeRequest("submit_tx", 0, start)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		observeRequest("submit_tx", 0, start)
		return 0, fmt.Errorf("failed to submit transaction: %w", err)
	}
	defer drainAndClose(resp.Body)
	defer observeRequest("submit_tx", resp.StatusCode, start)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, fmt.Errorf("%w %d: %s", errUnexpectedStatusCode, resp.StatusCode, string(body))
	}

	return resp.StatusCode, nil
}

// SubmitTransactions submits multiple transactions as a batch to a single endpoint.
// The raw transaction bytes are concatenated into a single body and POSTed to /txs.
// Returns the HTTP status code on success.
func (c *Client) SubmitTransactions(ctx context.Context, endpoint string, rawTxs [][]byte) (int, error) {
	start := time.Now()
	// Calculate total size for pre-allocation
	totalSize := 0
	for _, tx := range rawTxs {
		totalSize += len(tx)
	}

	body := make([]byte, 0, totalSize)
	for _, tx := range rawTxs {
		body = append(body, tx...)
	}

	url := endpoint + "/txs"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		observeRequest("submit_txs", 0, start)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		observeRequest("submit_txs", 0, start)
		return 0, fmt.Errorf("failed to submit transactions: %w", err)
	}
	defer drainAndClose(resp.Body)
	defer observeRequest("submit_txs", resp.StatusCode, start)

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, fmt.Errorf("%w %d: %s", errUnexpectedStatusCode, resp.StatusCode, string(respBody))
	}

	return resp.StatusCode, nil
}

// newBroadcastTransport configures an http.Transport sized for fan-out
// broadcasts to a handful of datahub endpoints at hundreds of TPS. The
// DefaultTransport's per-host idle cap of 2 is far too tight for this workload
// — under load Go would tear down and re-establish connections constantly.
// Values here are sized for ~10 datahubs and several hundred concurrent
// requests per endpoint; raise MaxConnsPerHost if the fleet grows.
func newBroadcastTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// drainAndClose ensures the response body is fully consumed before close so
// net/http can return the underlying TCP connection to the idle pool. Without
// the drain Go silently skips connection reuse — at hundreds of TPS across
// several datahubs that's thousands of extra handshakes and TIME_WAIT entries
// per second.
func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}
