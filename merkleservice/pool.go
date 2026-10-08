package merkleservice

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
)

// ErrNoHealthyEndpoints is returned when every endpoint's breaker is open,
// so nothing was sent. It is a transport-class error: the propagator
// classifies it as register_error and requeues the tx, and the watchdog treats
// it as a transient /reprocess failure.
var ErrNoHealthyEndpoints = errors.New("merkle service: no healthy endpoints")

const (
	// defaultFailureThreshold is how many consecutive transport/5xx failures
	// open an endpoint's breaker. 4xx responses never count: the service is
	// reachable and the loud auth warning in propagation must keep firing.
	defaultFailureThreshold = 3
	// defaultProbeInterval is how often open endpoints are probed with
	// GET /health. Any non-5xx HTTP response closes the breaker.
	defaultProbeInterval = 5 * time.Second
	defaultProbeTimeout  = 5 * time.Second

	opWatch     = "watch"
	opReprocess = "reprocess"
	opProbe     = "probe"

	outcomeOK          = "ok"
	outcomeAuth        = "err_auth"
	outcome4xx         = "err_4xx"
	outcome5xx         = "err_5xx"
	outcomeNetwork     = "err_network"
	outcomeCanceled    = "canceled"
	outcomeSkippedOpen = "skipped_open"
)

// endpoint is one merkle-service plus its circuit-breaker state.
type endpoint struct {
	url    string
	client *Client

	mu                  sync.Mutex
	consecutiveFailures int
	open                bool
	openedAt            time.Time
	lastFailureAt       time.Time
}

func (ep *endpoint) isOpen() bool {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	return ep.open
}

// PoolOption tunes a Pool. The defaults suit production; tests shrink the
// probe interval and pin the clock.
type PoolOption func(*Pool)

// WithFailureThreshold sets how many consecutive transport/5xx failures open
// an endpoint's breaker. Values below 1 keep the default.
func WithFailureThreshold(n int) PoolOption {
	return func(p *Pool) {
		if n > 0 {
			p.threshold = n
		}
	}
}

// WithProbeInterval sets how often open endpoints are probed.
func WithProbeInterval(d time.Duration) PoolOption {
	return func(p *Pool) {
		if d > 0 {
			p.probeInterval = d
		}
	}
}

// WithClock overrides the pool's clock (tests).
func WithClock(now func() time.Time) PoolOption {
	return func(p *Pool) {
		if now != nil {
			p.now = now
		}
	}
}

// Pool fans every merkle-service call out to all configured endpoints so a
// tx is watched by each of them and any one of them delivering callbacks is
// enough. Per call the pool reports success when at least one endpoint
// accepted; otherwise it surfaces the most retryable failure so the existing
// error classification in propagation and the watchdog keeps its meaning
// (context error > network/5xx > 401/403 > other 4xx).
//
// Each endpoint has a circuit breaker: after threshold consecutive
// transport/5xx failures it is skipped on the hot path so a black-holed
// service cannot add its full HTTP timeout to every registration batch. A
// background probe (Start) GETs /health on open endpoints and closes the
// breaker on any non-5xx answer, firing the OnRecovered hooks so propagation
// can re-register the txs that endpoint missed.
//
// A Pool with a single endpoint behaves like the Client it wraps, plus the
// breaker and per-endpoint metrics.
type Pool struct {
	endpoints     []*endpoint
	threshold     int
	probeInterval time.Duration
	probeClient   *http.Client
	now           func() time.Time
	logger        *zap.Logger

	hooksMu        sync.Mutex
	recoveredHooks []func(endpoint string, openedAt time.Time)
}

// NewPool builds one Client per base URL (all sharing authToken and the
// per-request timeout, 0 = Client default) and wraps them in a Pool. Duplicate
// and empty URLs are dropped; config.MerkleServiceConfig.Endpoints already
// normalizes the list, but NewPool is defensive so tests can pass raw slices.
func NewPool(baseURLs []string, authToken string, timeout time.Duration, opts ...PoolOption) *Pool {
	p := &Pool{
		threshold:     defaultFailureThreshold,
		probeInterval: defaultProbeInterval,
		probeClient:   &http.Client{Timeout: defaultProbeTimeout},
		now:           time.Now,
		logger:        zap.NewNop(),
	}
	seen := make(map[string]struct{}, len(baseURLs))
	for _, u := range baseURLs {
		u = strings.TrimSuffix(strings.TrimSpace(u), "/")
		if u == "" {
			continue
		}
		if _, dup := seen[u]; dup {
			continue
		}
		seen[u] = struct{}{}
		p.endpoints = append(p.endpoints, &endpoint{url: u, client: NewClient(u, authToken, timeout)})
		metrics.MerkleEndpointHealthy.WithLabelValues(u).Set(1)
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// SetLogger sets the pool's logger and gives every endpoint client a child
// logger tagged with its base URL. Mirrors Client.SetLogger.
func (p *Pool) SetLogger(logger *zap.Logger) {
	if logger == nil {
		return
	}
	p.logger = logger
	for _, ep := range p.endpoints {
		ep.client.SetLogger(logger.With(zap.String("endpoint", ep.url)))
	}
}

// Endpoints lists the configured base URLs in config order.
func (p *Pool) Endpoints() []string {
	out := make([]string, 0, len(p.endpoints))
	for _, ep := range p.endpoints {
		out = append(out, ep.url)
	}
	return out
}

// Endpoint returns a Service bound to exactly one endpoint, bypassing the
// breaker and pool bookkeeping. nil when baseURL is unknown.
func (p *Pool) Endpoint(baseURL string) Service {
	baseURL = strings.TrimSuffix(strings.TrimSpace(baseURL), "/")
	for _, ep := range p.endpoints {
		if ep.url == baseURL {
			return ep.client
		}
	}
	return nil
}

// OnRecovered registers fn to run each time an endpoint's breaker closes.
func (p *Pool) OnRecovered(fn func(endpoint string, openedAt time.Time)) {
	if fn == nil {
		return
	}
	p.hooksMu.Lock()
	defer p.hooksMu.Unlock()
	p.recoveredHooks = append(p.recoveredHooks, fn)
}

// EndpointStatuses snapshots every endpoint's breaker state.
func (p *Pool) EndpointStatuses() []EndpointStatus {
	out := make([]EndpointStatus, 0, len(p.endpoints))
	for _, ep := range p.endpoints {
		ep.mu.Lock()
		st := EndpointStatus{
			URL:                 ep.url,
			Healthy:             !ep.open,
			ConsecutiveFailures: ep.consecutiveFailures,
		}
		if !ep.lastFailureAt.IsZero() {
			t := ep.lastFailureAt
			st.LastFailureAt = &t
		}
		if ep.open {
			t := ep.openedAt
			st.OpenSince = &t
		}
		ep.mu.Unlock()
		out = append(out, st)
	}
	return out
}

// Start launches the background probe loop. It returns immediately; the loop
// exits when ctx is canceled. Without Start an open breaker only closes when
// a direct call through Endpoint succeeds, so production always calls it.
func (p *Pool) Start(ctx context.Context) {
	if len(p.endpoints) == 0 {
		return
	}
	go p.probeLoop(ctx)
}

// Register fans one /watch out to every closed endpoint.
func (p *Pool) Register(ctx context.Context, txid, callbackURL, callbackToken string) error {
	return p.fanOut(ctx, opWatch, func(ctx context.Context, c *Client) error {
		return c.Register(ctx, txid, callbackURL, callbackToken)
	})
}

// Reprocess fans one /reprocess out to every closed endpoint. nil when any
// endpoint accepted: every endpoint re-emits its own STUMP set, and the
// bump-builder is idempotent across sources.
func (p *Pool) Reprocess(ctx context.Context, blockHash, callbackURL, callbackToken string) error {
	return p.fanOut(ctx, opReprocess, func(ctx context.Context, c *Client) error {
		return c.Reprocess(ctx, blockHash, callbackURL, callbackToken)
	})
}

// RegisterBatch keeps Client.RegisterBatch's contract (one error, nil only
// when every tx registered) on top of the per-tx fan-out. It runs the whole
// batch rather than failing fast: a fail-fast across endpoints would cancel
// in-flight /watch calls on healthy endpoints because one endpoint hiccuped.
func (p *Pool) RegisterBatch(ctx context.Context, registrations []Registration, maxConcurrency int) error {
	for _, err := range p.RegisterBatchWithResults(ctx, registrations, maxConcurrency) {
		if err != nil {
			return err
		}
	}
	return nil
}

// RegisterBatchWithResults runs the batch against every closed endpoint
// concurrently (each endpoint bounded by maxConcurrency, as in
// Client.RegisterBatchWithResults) and merges per input index: nil when any
// endpoint accepted the tx, otherwise the most retryable of that tx's errors.
// Output length always equals len(registrations).
func (p *Pool) RegisterBatchWithResults(ctx context.Context, registrations []Registration, maxConcurrency int) []error {
	if len(registrations) == 0 {
		return nil
	}
	out := make([]error, len(registrations))
	targets := p.closedEndpoints(opWatch)
	if len(targets) == 0 {
		for i := range out {
			out[i] = ErrNoHealthyEndpoints
		}
		return out
	}

	perEndpoint := make([][]error, len(targets))
	var wg sync.WaitGroup
	for i, ep := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := ep.client.RegisterBatchWithResults(ctx, registrations, maxConcurrency)
			for _, err := range res {
				p.record(ctx, ep, opWatch, err)
			}
			perEndpoint[i] = res
		}()
	}
	wg.Wait()

	candidates := make([]error, len(targets))
	for j := range registrations {
		for i := range targets {
			candidates[i] = perEndpoint[i][j]
		}
		out[j] = mergeErrors(ctx, candidates)
	}
	return out
}

// fanOut runs call against every closed endpoint concurrently and merges the
// results (nil if any succeeded, else the most retryable error).
func (p *Pool) fanOut(ctx context.Context, op string, call func(ctx context.Context, c *Client) error) error {
	targets := p.closedEndpoints(op)
	if len(targets) == 0 {
		return ErrNoHealthyEndpoints
	}
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, ep := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := call(ctx, ep.client)
			p.record(ctx, ep, op, err)
			errs[i] = err
		}()
	}
	wg.Wait()
	return mergeErrors(ctx, errs)
}

// closedEndpoints returns the endpoints whose breaker is closed and counts a
// skipped_open sample for each one that is open.
func (p *Pool) closedEndpoints(op string) []*endpoint {
	out := make([]*endpoint, 0, len(p.endpoints))
	for _, ep := range p.endpoints {
		if ep.isOpen() {
			metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, op, outcomeSkippedOpen).Inc()
			continue
		}
		out = append(out, ep)
	}
	return out
}

// classify maps one call's error to its metric outcome and whether it counts
// toward the breaker. ctx is consulted because an http.Client timeout
// satisfies errors.Is(err, context.DeadlineExceeded) on recent Go versions,
// and that timeout IS the black-holed-endpoint signal the breaker exists for;
// only the caller's own cancellation is exempt.
func classify(ctx context.Context, err error) (outcome string, failure bool) {
	if err == nil {
		return outcomeOK, false
	}
	if ctx.Err() != nil {
		return outcomeCanceled, false
	}
	var regErr *RegisterError
	var repErr *ReprocessError
	var code int
	switch {
	case errors.As(err, &regErr):
		code = regErr.StatusCode
	case errors.As(err, &repErr):
		code = repErr.StatusCode
	default:
		return outcomeNetwork, true
	}
	switch {
	case code >= http.StatusInternalServerError:
		return outcome5xx, true
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return outcomeAuth, false
	default:
		return outcome4xx, false
	}
}

// errRank orders failures by how much a retry is worth: a lower rank is
// surfaced in preference to a higher one when every endpoint failed.
func errRank(ctx context.Context, err error) int {
	switch outcome, _ := classify(ctx, err); outcome {
	case outcomeCanceled:
		return 0
	case outcomeNetwork:
		return 1
	case outcome5xx:
		return 2
	case outcomeAuth:
		return 3
	default:
		return 4
	}
}

// mergeErrors reduces one call's per-endpoint results to the pool's answer.
func mergeErrors(ctx context.Context, errs []error) error {
	var best error
	bestRank := int(^uint(0) >> 1)
	for _, err := range errs {
		if err == nil {
			return nil
		}
		if r := errRank(ctx, err); r < bestRank {
			best, bestRank = err, r
		}
	}
	return best
}

// record updates the endpoint's breaker and metrics after one call.
func (p *Pool) record(ctx context.Context, ep *endpoint, op string, err error) {
	outcome, failure := classify(ctx, err)
	metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, op, outcome).Inc()
	if outcome == outcomeCanceled {
		return // the caller's context, not the endpoint's fault
	}
	if !failure {
		// Any HTTP answer below 500 proves reachability. A success while the
		// breaker is open can only come from a direct Endpoint() call, but if
		// it happens the endpoint is evidently back.
		ep.mu.Lock()
		ep.consecutiveFailures = 0
		wasOpen := ep.open
		ep.mu.Unlock()
		if wasOpen && err == nil {
			p.closeBreaker(ep, op)
		}
		return
	}
	ep.mu.Lock()
	ep.consecutiveFailures++
	ep.lastFailureAt = p.now()
	trip := !ep.open && ep.consecutiveFailures >= p.threshold
	if trip {
		ep.open = true
		ep.openedAt = ep.lastFailureAt
	}
	failures := ep.consecutiveFailures
	ep.mu.Unlock()
	if trip {
		metrics.MerkleEndpointHealthy.WithLabelValues(ep.url).Set(0)
		p.logger.Warn(
			"merkle-service endpoint unhealthy; skipping it until /health answers",
			zap.String("endpoint", ep.url),
			zap.String("op", op),
			zap.Int("consecutive_failures", failures),
			zap.Error(err),
		)
	}
}

// closeBreaker marks ep healthy and fires the OnRecovered hooks with the
// window the endpoint was open for. No-op when the breaker is already closed.
func (p *Pool) closeBreaker(ep *endpoint, via string) {
	ep.mu.Lock()
	if !ep.open {
		ep.mu.Unlock()
		return
	}
	openedAt := ep.openedAt
	ep.open = false
	ep.openedAt = time.Time{}
	ep.consecutiveFailures = 0
	ep.mu.Unlock()

	metrics.MerkleEndpointHealthy.WithLabelValues(ep.url).Set(1)
	p.logger.Info(
		"merkle-service endpoint healthy again",
		zap.String("endpoint", ep.url),
		zap.String("via", via),
		zap.Duration("open_for", p.now().Sub(openedAt)),
	)

	p.hooksMu.Lock()
	hooks := make([]func(string, time.Time), len(p.recoveredHooks))
	copy(hooks, p.recoveredHooks)
	p.hooksMu.Unlock()
	for _, fn := range hooks {
		fn(ep.url, openedAt)
	}
}

func (p *Pool) probeLoop(ctx context.Context) {
	ticker := time.NewTicker(p.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.probeOpenEndpoints(ctx)
		}
	}
}

// probeOpenEndpoints GETs /health on every open endpoint and closes the
// breaker of each one that answers with anything but a 5xx. A 4xx (e.g. 404
// from a build without /health) still proves the process is reachable; a 5xx
// means the service itself reports unhealthy, so it stays open.
func (p *Pool) probeOpenEndpoints(ctx context.Context) {
	for _, ep := range p.endpoints {
		if !ep.isOpen() {
			continue
		}
		if err := p.probe(ctx, ep); err != nil {
			metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, opProbe, outcomeNetwork).Inc()
			p.logger.Debug("merkle-service probe failed", zap.String("endpoint", ep.url), zap.Error(err))
			continue
		}
		metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, opProbe, outcomeOK).Inc()
		p.closeBreaker(ep, opProbe)
	}
}

func (p *Pool) probe(ctx context.Context, ep *endpoint) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep.url+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := p.probeClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= http.StatusInternalServerError {
		return &probeError{StatusCode: resp.StatusCode}
	}
	return nil
}

// probeError is a 5xx answer to GET /health.
type probeError struct {
	StatusCode int
}

func (e *probeError) Error() string {
	return "merkle service /health returned status " + http.StatusText(e.StatusCode)
}
