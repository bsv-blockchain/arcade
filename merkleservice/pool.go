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
	// GET /health and how often closed endpoints drain their catch-up queue.
	defaultProbeInterval = 5 * time.Second
	defaultProbeTimeout  = 5 * time.Second
	// defaultCatchupQueueSize bounds the per-endpoint catch-up queue. At
	// 100 TPS it holds roughly half an hour of registrations an endpoint
	// missed; past that the oldest entries are dropped and a full resync is
	// requested once the queue drains (see Recoverable.OnResyncNeeded).
	defaultCatchupQueueSize = 200_000
	// catchupBatchSize is how many queued registrations one drain tick sends
	// to an endpoint; with the default probe interval that is ~100/s.
	catchupBatchSize = 500
	// catchupConcurrency bounds the in-flight /watch calls of one drain.
	catchupConcurrency = 10
	// catchupMaxBackoffTicks caps how many ticks a drain waits after a batch
	// in which nothing landed; the breaker handles a fully-down endpoint.
	catchupMaxBackoffTicks = 12

	opWatch     = "watch"
	opCatchup   = "catchup"
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

// endpoint is one merkle-service plus its circuit-breaker state and the
// catch-up queue of registrations it has not acknowledged yet.
type endpoint struct {
	url    string
	client *Client

	mu                  sync.Mutex
	consecutiveFailures int
	open                bool
	openedAt            time.Time
	lastFailureAt       time.Time

	pending *catchupQueue
	// backoffTicks is how many drain ticks to skip after a drain batch in
	// which nothing landed; doubles up to catchupMaxBackoffTicks.
	backoffTicks int
	skipTicks    int
}

func (ep *endpoint) isOpen() bool {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	return ep.open
}

// catchupQueue is the bounded, deduplicated set of registrations one
// endpoint still owes. A tx the pool reported registered (some endpoint
// accepted it) but this endpoint refused, timed out on, or was skipped for
// (breaker open) lands here and is re-sent once the endpoint answers again.
// It is in-memory on purpose: the startup replay re-registers every
// in-flight tx with the whole pool, so a restart loses nothing durable.
type catchupQueue struct {
	mu      sync.Mutex
	max     int
	order   []string
	byTxID  map[string]Registration
	dropped int
	// overflowed is set when entries were dropped and cleared when the
	// queue next drains empty; that transition requests a full resync.
	overflowed bool
}

func newCatchupQueue(capacity int) *catchupQueue {
	return &catchupQueue{max: capacity, byTxID: make(map[string]Registration)}
}

// add queues reg (replacing an older entry for the same txid). When the
// queue is full the oldest entry is dropped and the overflow flag set.
func (q *catchupQueue) add(reg Registration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, dup := q.byTxID[reg.TxID]; dup {
		q.byTxID[reg.TxID] = reg
		return
	}
	for len(q.order) >= q.max && len(q.order) > 0 {
		oldest := q.order[0]
		q.order = q.order[1:]
		delete(q.byTxID, oldest)
		q.dropped++
		q.overflowed = true
	}
	q.order = append(q.order, reg.TxID)
	q.byTxID[reg.TxID] = reg
}

// take removes and returns up to n entries from the front.
func (q *catchupQueue) take(n int) []Registration {
	q.mu.Lock()
	defer q.mu.Unlock()
	if n > len(q.order) {
		n = len(q.order)
	}
	out := make([]Registration, 0, n)
	for _, txid := range q.order[:n] {
		out = append(out, q.byTxID[txid])
		delete(q.byTxID, txid)
	}
	q.order = q.order[n:]
	return out
}

func (q *catchupQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// drainedAfterOverflow reports and clears the overflow flag once the queue
// is empty, returning how many entries were dropped since the last resync.
func (q *catchupQueue) drainedAfterOverflow() (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.overflowed || len(q.order) > 0 {
		return 0, false
	}
	n := q.dropped
	q.overflowed, q.dropped = false, 0
	return n, true
}

// PoolOption tunes a Pool. The defaults suit production; tests shrink the
// probe interval and queue size.
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

// WithProbeInterval sets how often open endpoints are probed and closed
// endpoints drain their catch-up queue.
func WithProbeInterval(d time.Duration) PoolOption {
	return func(p *Pool) {
		if d > 0 {
			p.probeInterval = d
		}
	}
}

// WithCatchupQueueSize bounds each endpoint's catch-up queue.
func WithCatchupQueueSize(n int) PoolOption {
	return func(p *Pool) {
		if n > 0 {
			p.queueSize = n
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
// A tx the pool reported registered must still reach every endpoint, or the
// endpoints' watch sets diverge and their STUMPs disagree. So every
// endpoint-specific failure behind a pool-level success — a refused or timed
// out /watch, or an endpoint skipped because its breaker was open — is kept
// in that endpoint's catch-up queue and re-sent from the background loop
// once the endpoint answers again, with backoff. The queue is bounded; past
// its size the oldest entries are dropped and, once it drains, the
// OnResyncNeeded hooks fire so propagation can run a full lookback replay
// against that endpoint alone.
//
// Each endpoint has a circuit breaker: after threshold consecutive
// transport/5xx failures it is skipped on the hot path so a black-holed
// service cannot add its full HTTP timeout to every registration batch. The
// background loop (Start) GETs /health on open endpoints and closes the
// breaker on any non-5xx answer.
//
// A Pool with a single endpoint behaves like the Client it wraps, plus the
// breaker, queue and per-endpoint metrics.
type Pool struct {
	endpoints     []*endpoint
	threshold     int
	probeInterval time.Duration
	queueSize     int
	probeClient   *http.Client
	now           func() time.Time
	logger        *zap.Logger

	hooksMu     sync.Mutex
	resyncHooks []func(endpoint string)
}

// NewPool builds one Client per base URL (all sharing authToken and the
// per-request timeout, 0 = Client default) and wraps them in a Pool. Duplicate
// and empty URLs are dropped; config.MerkleServiceConfig.Endpoints already
// normalizes the list, but NewPool is defensive so tests can pass raw slices.
func NewPool(baseURLs []string, authToken string, timeout time.Duration, opts ...PoolOption) *Pool {
	p := &Pool{
		threshold:     defaultFailureThreshold,
		probeInterval: defaultProbeInterval,
		queueSize:     defaultCatchupQueueSize,
		probeClient:   &http.Client{Timeout: defaultProbeTimeout},
		now:           time.Now,
		logger:        zap.NewNop(),
	}
	for _, o := range opts {
		o(p)
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
		p.endpoints = append(p.endpoints, &endpoint{
			url:     u,
			client:  NewClient(u, authToken, timeout),
			pending: newCatchupQueue(p.queueSize),
		})
		metrics.MerkleEndpointHealthy.WithLabelValues(u).Set(1)
		metrics.MerkleEndpointCatchupPending.WithLabelValues(u).Set(0)
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

// Endpoint returns a Service bound to exactly one endpoint. Calls through it
// still honor that endpoint's breaker and feed its metrics, and a failed
// registration lands in the endpoint's catch-up queue rather than being
// lost. nil when baseURL is unknown.
func (p *Pool) Endpoint(baseURL string) Service {
	baseURL = strings.TrimSuffix(strings.TrimSpace(baseURL), "/")
	for _, ep := range p.endpoints {
		if ep.url == baseURL {
			return &endpointService{pool: p, ep: ep}
		}
	}
	return nil
}

// OnResyncNeeded registers fn to run each time an endpoint's catch-up queue
// has overflowed and then drained: the registrations it dropped are unknown,
// so the endpoint needs a full lookback replay. fn runs on the pool's
// background goroutine and must return quickly.
func (p *Pool) OnResyncNeeded(fn func(endpoint string)) {
	if fn == nil {
		return
	}
	p.hooksMu.Lock()
	defer p.hooksMu.Unlock()
	p.resyncHooks = append(p.resyncHooks, fn)
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
		st.CatchupPending = ep.pending.len()
		out = append(out, st)
	}
	return out
}

// Start launches the background loop that probes open endpoints and drains
// closed endpoints' catch-up queues. It returns immediately; the loop exits
// when ctx is canceled. Without Start an open breaker never closes and a
// queued registration is never re-sent, so production always calls it.
func (p *Pool) Start(ctx context.Context) {
	if len(p.endpoints) == 0 {
		return
	}
	go p.loop(ctx)
}

// Register fans one /watch out to every closed endpoint.
func (p *Pool) Register(ctx context.Context, txid, callbackURL, callbackToken string) error {
	errs := p.RegisterBatchWithResults(ctx, []Registration{{TxID: txid, CallbackURL: callbackURL, CallbackToken: callbackToken}}, 1)
	return errs[0]
}

// Reprocess fans one /reprocess out to every closed endpoint. nil when any
// endpoint accepted: every endpoint re-emits its own STUMP set, and the
// bump-builder is idempotent across sources.
func (p *Pool) Reprocess(ctx context.Context, blockHash, callbackURL, callbackToken string) error {
	targets := p.closedEndpoints(opReprocess)
	if len(targets) == 0 {
		return ErrNoHealthyEndpoints
	}
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, ep := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := ep.client.Reprocess(ctx, blockHash, callbackURL, callbackToken)
			p.record(ctx, ep, opReprocess, err)
			errs[i] = err
		}()
	}
	wg.Wait()
	return mergeErrors(ctx, errs)
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
// Every tx the pool reports registered that some endpoint did not accept
// (refused, timed out, or skipped for an open breaker) is queued for that
// endpoint. Output length always equals len(registrations).
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

	perEndpoint := make(map[*endpoint][]error, len(targets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ep := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := ep.client.RegisterBatchWithResults(ctx, registrations, maxConcurrency)
			for _, err := range res {
				p.record(ctx, ep, opWatch, err)
			}
			mu.Lock()
			perEndpoint[ep] = res
			mu.Unlock()
		}()
	}
	wg.Wait()

	candidates := make([]error, 0, len(targets))
	for j := range registrations {
		candidates = candidates[:0]
		for _, ep := range targets {
			candidates = append(candidates, perEndpoint[ep][j])
		}
		out[j] = mergeErrors(ctx, candidates)
	}
	// Behind every pool-level success, queue the endpoints that did not get
	// the tx: refused/timed out, or not even asked because the breaker was
	// open. Without this a single transport blip (below the breaker
	// threshold) would leave the endpoint permanently missing the txid.
	for _, ep := range p.endpoints {
		res := perEndpoint[ep] // nil for endpoints that were skipped
		for j := range registrations {
			if out[j] != nil {
				continue
			}
			if res == nil || res[j] != nil {
				ep.pending.add(registrations[j])
			}
		}
		metrics.MerkleEndpointCatchupPending.WithLabelValues(ep.url).Set(float64(ep.pending.len()))
	}
	return out
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
	var prbErr *probeError
	var code int
	switch {
	case errors.As(err, &regErr):
		code = regErr.StatusCode
	case errors.As(err, &repErr):
		code = repErr.StatusCode
	case errors.As(err, &prbErr):
		code = prbErr.StatusCode
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
		// breaker is open can only come from the probe path, but if it
		// happens the endpoint is evidently back.
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

// closeBreaker marks ep healthy. No-op when the breaker is already closed.
// The catch-up queue drains on the next loop tick, so nothing else needs to
// happen here.
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
		zap.Int("catchup_pending", ep.pending.len()),
	)
}

func (p *Pool) loop(ctx context.Context) {
	ticker := time.NewTicker(p.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.probeOpenEndpoints(ctx)
			p.drainCatchup(ctx)
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
		err := p.probe(ctx, ep)
		outcome, _ := classify(ctx, err)
		metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, opProbe, outcome).Inc()
		if err != nil {
			p.logger.Debug("merkle-service probe failed", zap.String("endpoint", ep.url), zap.String("outcome", outcome), zap.Error(err))
			continue
		}
		p.closeBreaker(ep, opProbe)
	}
}

// drainCatchup re-sends one batch of queued registrations to every closed
// endpoint that owes some. Failures go back to the queue (and count toward
// the breaker); a batch in which nothing landed backs the endpoint off for a
// growing number of ticks. When a queue that had overflowed drains empty,
// the resync hooks fire for that endpoint.
func (p *Pool) drainCatchup(ctx context.Context) {
	for _, ep := range p.endpoints {
		if ctx.Err() != nil {
			return
		}
		if ep.isOpen() {
			continue
		}
		if ep.skipTicks > 0 {
			ep.skipTicks--
			continue
		}
		batch := ep.pending.take(catchupBatchSize)
		if len(batch) > 0 {
			landed := p.sendCatchup(ctx, ep, batch)
			switch {
			case landed == 0 && ctx.Err() == nil:
				if ep.backoffTicks == 0 {
					ep.backoffTicks = 1
				} else if ep.backoffTicks < catchupMaxBackoffTicks {
					ep.backoffTicks *= 2
				}
				ep.skipTicks = ep.backoffTicks
			case landed > 0:
				ep.backoffTicks = 0
				p.logger.Info(
					"merkle-service catch-up delivered missed registrations",
					zap.String("endpoint", ep.url),
					zap.Int("landed", landed),
					zap.Int("failed", len(batch)-landed),
					zap.Int("still_pending", ep.pending.len()),
				)
			}
		}
		metrics.MerkleEndpointCatchupPending.WithLabelValues(ep.url).Set(float64(ep.pending.len()))
		if dropped, ok := ep.pending.drainedAfterOverflow(); ok {
			p.logger.Warn(
				"merkle-service catch-up queue had overflowed; requesting a full resync of this endpoint",
				zap.String("endpoint", ep.url),
				zap.Int("dropped", dropped),
			)
			p.fireResync(ep.url)
		}
	}
}

// sendCatchup registers batch with ep alone, re-queues what failed and
// returns how many landed.
func (p *Pool) sendCatchup(ctx context.Context, ep *endpoint, batch []Registration) int {
	res := ep.client.RegisterBatchWithResults(ctx, batch, catchupConcurrency)
	landed := 0
	for i, err := range res {
		p.record(ctx, ep, opCatchup, err)
		if err == nil {
			landed++
			continue
		}
		ep.pending.add(batch[i])
	}
	return landed
}

func (p *Pool) fireResync(url string) {
	p.hooksMu.Lock()
	hooks := make([]func(string), len(p.resyncHooks))
	copy(hooks, p.resyncHooks)
	p.hooksMu.Unlock()
	for _, fn := range hooks {
		fn(url)
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

// endpointService is the Service returned by Pool.Endpoint: one endpoint,
// honoring its breaker (an open endpoint refuses fast with
// ErrNoHealthyEndpoints instead of waiting out the HTTP timeout) and
// keeping every failed registration in the catch-up queue.
type endpointService struct {
	pool *Pool
	ep   *endpoint
}

func (s *endpointService) Register(ctx context.Context, txid, callbackURL, callbackToken string) error {
	return s.RegisterBatchWithResults(ctx, []Registration{{TxID: txid, CallbackURL: callbackURL, CallbackToken: callbackToken}}, 1)[0]
}

func (s *endpointService) RegisterBatch(ctx context.Context, registrations []Registration, maxConcurrency int) error {
	for _, err := range s.RegisterBatchWithResults(ctx, registrations, maxConcurrency) {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *endpointService) RegisterBatchWithResults(ctx context.Context, registrations []Registration, maxConcurrency int) []error {
	if len(registrations) == 0 {
		return nil
	}
	out := make([]error, len(registrations))
	if s.ep.isOpen() {
		metrics.MerkleEndpointRequestsTotal.WithLabelValues(s.ep.url, opWatch, outcomeSkippedOpen).Inc()
		for i := range registrations {
			out[i] = ErrNoHealthyEndpoints
			s.ep.pending.add(registrations[i])
		}
		return out
	}
	res := s.ep.client.RegisterBatchWithResults(ctx, registrations, maxConcurrency)
	for i, reg := range registrations {
		if i >= len(res) {
			break
		}
		s.pool.record(ctx, s.ep, opWatch, res[i])
		if res[i] != nil {
			s.ep.pending.add(reg)
		}
		out[i] = res[i]
	}
	metrics.MerkleEndpointCatchupPending.WithLabelValues(s.ep.url).Set(float64(s.ep.pending.len()))
	return out
}

func (s *endpointService) Reprocess(ctx context.Context, blockHash, callbackURL, callbackToken string) error {
	if s.ep.isOpen() {
		metrics.MerkleEndpointRequestsTotal.WithLabelValues(s.ep.url, opReprocess, outcomeSkippedOpen).Inc()
		return ErrNoHealthyEndpoints
	}
	err := s.ep.client.Reprocess(ctx, blockHash, callbackURL, callbackToken)
	s.pool.record(ctx, s.ep, opReprocess, err)
	return err
}

var _ Service = (*endpointService)(nil)
