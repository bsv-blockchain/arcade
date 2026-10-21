package merkleservice

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/bsv-broadcaster/metrics"
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
	// defaultStragglerGrace is how long a /watch batch keeps waiting for the
	// remaining endpoints once every tx in it has been accepted by some
	// endpoint. Past it they are canceled and owe the txs via catch-up. Same
	// bar as the trial /watch before a breaker closes.
	defaultStragglerGrace = defaultProbeTimeout
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
	// draining is set while a catch-up drain for this endpoint is running, so
	// a slow endpoint never has two drains in flight. Guarded by mu.
	draining bool
	// backoffTicks is how many drain ticks to skip after a drain batch in
	// which nothing landed; doubles up to catchupMaxBackoffTicks. Guarded by
	// mu, as is skipTicks.
	backoffTicks int
	skipTicks    int
}

func (ep *endpoint) isOpen() bool {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	return ep.open
}

// syncPendingGauge publishes the catch-up queue length. Called after every
// change to the queue so the gauge agrees with /health, including while the
// breaker is open.
func (ep *endpoint) syncPendingGauge() {
	metrics.MerkleEndpointCatchupPending.WithLabelValues(ep.url).Set(float64(ep.pending.len()))
}

// catchupQueue is the bounded, deduplicated set of registrations one
// endpoint still owes. A tx the pool reported registered (some endpoint
// accepted it) but this endpoint refused, timed out on, or was skipped for
// (breaker open) lands here and is re-sent once the endpoint answers again.
// It is in-memory on purpose: with several endpoints the startup replay
// re-registers every in-flight tx in the lookback window with the whole pool
// (the merkle_registered_at skip is off by default, since that stamp is set
// when any one endpoint accepted), so what a restart drops is re-sent then.
type catchupQueue struct {
	mu     sync.Mutex
	max    int
	order  []string
	byTxID map[string]Registration
	// inflight counts entries taken for a catch-up send and not yet settled;
	// they still count as owed until the endpoint has answered.
	inflight int
	dropped  int
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

// take removes and returns up to n entries from the front. They stay counted
// by len until settle is called for them.
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
	q.inflight += len(out)
	return out
}

// settle marks n taken entries as answered; any that failed have been added
// back by then.
func (q *catchupQueue) settle(n int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.inflight -= n
}

// len is how many registrations are owed: queued plus taken but unsettled.
func (q *catchupQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order) + q.inflight
}

// drainedAfterOverflow reports and clears the overflow flag once the queue
// is empty, returning how many entries were dropped since the last resync.
func (q *catchupQueue) drainedAfterOverflow() (int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.overflowed || len(q.order)+q.inflight > 0 {
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

// WithStragglerGrace sets how long a /watch batch waits for the remaining
// endpoints once every tx has been accepted somewhere.
func WithStragglerGrace(d time.Duration) PoolOption {
	return func(p *Pool) {
		if d > 0 {
			p.stragglerGrace = d
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
// breaker on any non-5xx answer, after one owed registration (if any) also
// went through as a trial /watch.
//
// A Pool with a single endpoint behaves like the Client it wraps, plus the
// breaker, queue and per-endpoint metrics.
type Pool struct {
	endpoints     []*endpoint
	threshold     int
	probeInterval time.Duration
	queueSize     int
	// stragglerGrace bounds the wait for slower endpoints once every tx of
	// a /watch batch is accepted (see RegisterBatchWithResults).
	stragglerGrace time.Duration
	probeClient    *http.Client
	now            func() time.Time
	logger         *zap.Logger

	hooksMu     sync.Mutex
	resyncHooks []func(endpoint string)
}

// NewPool builds one Client per base URL (all sharing authToken and the
// per-request timeout, 0 = Client default) and wraps them in a Pool. Duplicate
// and empty URLs are dropped; config.MerkleServiceConfig.Endpoints already
// normalizes the list, but NewPool is defensive so tests can pass raw slices.
func NewPool(baseURLs []string, authToken string, timeout time.Duration, opts ...PoolOption) *Pool {
	p := &Pool{
		threshold:      defaultFailureThreshold,
		probeInterval:  defaultProbeInterval,
		queueSize:      defaultCatchupQueueSize,
		stragglerGrace: defaultStragglerGrace,
		probeClient:    &http.Client{Timeout: defaultProbeTimeout},
		now:            time.Now,
		logger:         zap.NewNop(),
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

// Retain implements Recoverable.Retain.
func (p *Pool) Retain(registrations []Registration, errs []error) {
	for _, ep := range p.endpoints {
		for i, reg := range registrations {
			if i >= len(errs) || errs[i] == nil || permanentRejection(errs[i]) {
				continue
			}
			ep.pending.add(reg)
		}
		ep.syncPendingGauge()
	}
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
	targets := p.closedEndpoints(opReprocess, 1)
	if len(targets) == 0 {
		return ErrNoHealthyEndpoints
	}
	// One slot per configured endpoint: an endpoint skipped for an open
	// breaker answers ErrNoHealthyEndpoints, so a terminal answer from the
	// others (a 404, say) cannot outrank the possibility that the skipped
	// one is merely down and would have accepted.
	errs := make([]error, len(p.endpoints))
	for i := len(targets); i < len(errs); i++ {
		errs[i] = ErrNoHealthyEndpoints
	}
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
// endpoint. A batch that every tx has cleared waits at most stragglerGrace
// for the remaining endpoints (see collectEndpointBatches), so one hung
// endpoint does not hold registration — and therefore broadcast — for its
// full HTTP timeout. Output length always equals len(registrations).
func (p *Pool) RegisterBatchWithResults(ctx context.Context, registrations []Registration, maxConcurrency int) []error {
	if len(registrations) == 0 {
		return nil
	}
	out := make([]error, len(registrations))
	targets := p.closedEndpoints(opWatch, len(registrations))
	if len(targets) == 0 {
		for i := range out {
			out[i] = ErrNoHealthyEndpoints
		}
		return out
	}

	perEndpoint := p.collectEndpointBatches(ctx, targets, registrations, maxConcurrency)

	// Endpoints skipped up front for an open breaker take part in the merge
	// as ErrNoHealthyEndpoints (transport-class), as mid-batch skips already
	// do: a 401 or 4xx from the endpoints that answered must not read as the
	// pool's verdict while another endpoint is merely down.
	skipped := len(p.endpoints) - len(targets)
	candidates := make([]error, 0, len(p.endpoints))
	for j := range registrations {
		candidates = candidates[:0]
		for _, ep := range targets {
			candidates = append(candidates, perEndpoint[ep][j])
		}
		for range skipped {
			candidates = append(candidates, ErrNoHealthyEndpoints)
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
		ep.syncPendingGauge()
	}
	return out
}

// collectEndpointBatches runs the batch against every target concurrently
// and returns each endpoint's per-tx results. It returns as soon as every
// endpoint has answered, or stragglerGrace after every tx has been accepted
// by some endpoint, whichever is first: the stragglers' sends are then
// canceled. Acceptance is tracked per tx as each /watch lands, not per
// finished endpoint batch, so complementary coverage counts too: A accepting
// tx1 while hanging on tx2 and B accepting tx2 while hanging on tx1 still
// starts the grace timer. A straggler that acknowledged nothing in the batch
// has the cancellations recorded against its breaker (see runEndpointBatch),
// so a black-holed endpoint trips on its first batch instead of holding every
// batch for a full HTTP timeout; one that acknowledged something is only
// slower and keeps its health. What the
// stragglers did not acknowledge lands in their catch-up queue through the
// caller's usual path.
func (p *Pool) collectEndpointBatches(ctx context.Context, targets []*endpoint, registrations []Registration, maxConcurrency int) map[*endpoint][]error {
	sendCtx, cancelSends := context.WithCancel(ctx)
	defer cancelSends()
	type endpointResult struct {
		ep  *endpoint
		res []error
	}
	results := make(chan endpointResult, len(targets))
	// Sized for every possible acceptance so a sender never blocks, even
	// after the collector has returned.
	acks := make(chan int, len(targets)*len(registrations))
	onAccepted := func(j int) { acks <- j }
	for _, ep := range targets {
		go func() {
			results <- endpointResult{ep, p.runEndpointBatch(ctx, sendCtx, ep, opWatch, registrations, maxConcurrency, onAccepted)}
		}()
	}

	perEndpoint := make(map[*endpoint][]error, len(targets))
	accepted := make([]bool, len(registrations))
	unaccepted := len(registrations)
	accept := func(j int) {
		if !accepted[j] {
			accepted[j] = true
			unaccepted--
		}
	}
	var grace <-chan time.Time
	for len(perEndpoint) < len(targets) {
		select {
		case j := <-acks:
			accept(j)
		case r := <-results:
			perEndpoint[r.ep] = r.res
			for j, err := range r.res {
				if err == nil {
					accept(j)
				}
			}
		case <-grace:
			// Every tx is registered somewhere; stop waiting on the rest.
			// Their sends return promptly and are still collected above.
			cancelSends()
			grace = nil
			continue
		}
		if unaccepted == 0 && grace == nil && len(perEndpoint) < len(targets) && sendCtx.Err() == nil {
			t := time.NewTimer(p.stragglerGrace)
			defer t.Stop()
			grace = t.C
		}
	}
	return perEndpoint
}

// runEndpointBatch sends registrations to ep alone, at most maxConcurrency at
// a time, and records each result as it lands so the breaker can open in the
// middle of a batch. Once it is open nothing more is sent: the remaining
// entries come back as ErrNoHealthyEndpoints right away. Without that a
// black-holed endpoint would hold the caller for one HTTP timeout per
// maxConcurrency entries, even though another endpoint accepted them all
// long ago; with it the first batch waits for at most the requests already
// in flight when the breaker trips. res[i] corresponds to registrations[i].
//
// sendCtx bounds the requests; ctx is the caller's and decides whether a
// failure was the caller's own cancellation (not the endpoint's fault) when it
// is recorded. They differ only when the pool cuts a straggler short.
//
// onAccepted, when non-nil, is called with the index of each registration the
// endpoint acknowledges, as it lands; it must not block.
func (p *Pool) runEndpointBatch(ctx, sendCtx context.Context, ep *endpoint, op string, registrations []Registration, maxConcurrency int, onAccepted func(int)) []error {
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	res := make([]error, len(registrations))
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup
	skipped := 0
	// acked counts this batch's acknowledgements. A request the pool cut
	// short (sendCtx canceled after the straggler grace) is held against the
	// endpoint only when the endpoint has acknowledged nothing in the batch:
	// a black-holed endpoint acknowledges nothing and still trips on its
	// first batch, while one that is merely slower than the others keeps
	// its health and just owes the rest through catch-up.
	var acked atomic.Int64
	for i, reg := range registrations {
		select {
		case sem <- struct{}{}:
		case <-sendCtx.Done():
			res[i] = sendCtx.Err()
			continue
		}
		// Checked after taking a slot: a slot frees only when an earlier
		// request has finished and been recorded, so a trip it caused is
		// already visible here.
		if ep.isOpen() {
			<-sem
			res[i] = ErrNoHealthyEndpoints
			skipped++
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := ep.client.Register(sendCtx, reg.TxID, reg.CallbackURL, reg.CallbackToken)
			classifyCtx := ctx
			if err == nil {
				acked.Add(1)
			} else if ctx.Err() == nil && sendCtx.Err() != nil && acked.Load() > 0 {
				classifyCtx = sendCtx // recorded as canceled, not a failure
			}
			p.record(classifyCtx, ep, op, err)
			res[i] = err
			if err == nil && onAccepted != nil {
				onAccepted(i)
			}
		}()
	}
	wg.Wait()
	if skipped > 0 {
		metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, op, outcomeSkippedOpen).Add(float64(skipped))
	}
	return res
}

// closedEndpoints returns the endpoints whose breaker is closed and counts
// requests skipped_open samples for each one that is open: the number of
// calls the endpoint would otherwise have received (one /watch per
// registration, one /reprocess), so the counter matches mid-batch skips.
func (p *Pool) closedEndpoints(op string, requests int) []*endpoint {
	out := make([]*endpoint, 0, len(p.endpoints))
	for _, ep := range p.endpoints {
		if ep.isOpen() {
			metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, op, outcomeSkippedOpen).Add(float64(requests))
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

// probeOpenEndpoints probes every open endpoint concurrently, so several
// black-holed endpoints cost one probe timeout per tick rather than one each.
func (p *Pool) probeOpenEndpoints(ctx context.Context) {
	var wg sync.WaitGroup
	for _, ep := range p.endpoints {
		if !ep.isOpen() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.probeEndpoint(ctx, ep)
		}()
	}
	wg.Wait()
}

// probeEndpoint GETs /health on one open endpoint and closes its breaker when
// it answers with anything but a 5xx and a trial /watch (see trialWatch) also
// answers. A 4xx (e.g. 404 from a build without /health) still proves the
// process is reachable; a 5xx means the service itself reports unhealthy, so
// it stays open.
func (p *Pool) probeEndpoint(ctx context.Context, ep *endpoint) {
	err := p.probe(ctx, ep)
	outcome, _ := classify(ctx, err)
	metrics.MerkleEndpointRequestsTotal.WithLabelValues(ep.url, opProbe, outcome).Inc()
	if err != nil {
		p.logger.Debug("merkle-service probe failed", zap.String("endpoint", ep.url), zap.String("outcome", outcome), zap.Error(err))
		return
	}
	if !p.trialWatch(ctx, ep) {
		return
	}
	p.closeBreaker(ep, opProbe)
}

// trialWatch re-sends one registration the endpoint owes, bounded by the
// probe timeout, before its breaker closes. /health is shallow: an endpoint
// whose /health answers while /watch hangs would otherwise be closed every
// probe tick and hold the next hot-path batch for a full HTTP timeout before
// re-tripping. It reports whether /watch answered below 500. With nothing
// owed there is nothing to try, and /health alone decides.
func (p *Pool) trialWatch(ctx context.Context, ep *endpoint) bool {
	batch := ep.pending.take(1)
	if len(batch) == 0 {
		return true
	}
	reg := batch[0]
	trialCtx, cancel := context.WithTimeout(ctx, defaultProbeTimeout)
	err := ep.client.Register(trialCtx, reg.TxID, reg.CallbackURL, reg.CallbackToken)
	cancel()
	// Classified against the parent ctx: the trial's own deadline is the
	// hanging-/watch signal and must count as a failure.
	outcome, failure := classify(ctx, err)
	p.record(ctx, ep, opCatchup, err)
	if err != nil && !permanentRejection(err) {
		ep.pending.add(reg)
	}
	ep.pending.settle(1)
	ep.syncPendingGauge()
	if outcome == outcomeCanceled || failure {
		p.logger.Debug("merkle-service trial /watch failed; keeping breaker open", zap.String("endpoint", ep.url), zap.String("outcome", outcome), zap.Error(err))
		return false
	}
	return true
}

// drainCatchup starts, for every closed endpoint that owes registrations and
// is not backing off, a drain of one batch. Each endpoint drains on its own
// goroutine, at most one at a time, so a hanging endpoint cannot hold up the
// probes or another endpoint's catch-up.
func (p *Pool) drainCatchup(ctx context.Context) {
	for _, ep := range p.endpoints {
		if ctx.Err() != nil {
			return
		}
		ep.syncPendingGauge()
		ep.mu.Lock()
		if ep.open || ep.draining {
			ep.mu.Unlock()
			continue
		}
		if ep.skipTicks > 0 {
			ep.skipTicks--
			ep.mu.Unlock()
			continue
		}
		ep.draining = true
		ep.mu.Unlock()
		go func() {
			defer func() {
				ep.mu.Lock()
				ep.draining = false
				ep.mu.Unlock()
			}()
			p.drainEndpoint(ctx, ep)
		}()
	}
}

// drainEndpoint re-sends one batch of ep's queued registrations. Failures go
// back to the queue (and count toward the breaker) unless the endpoint
// rejected the registration outright (see permanentRejection); a batch that
// made no progress backs the endpoint off for a growing number of ticks.
// When a queue that had overflowed drains empty, the resync hooks fire.
func (p *Pool) drainEndpoint(ctx context.Context, ep *endpoint) {
	if batch := ep.pending.take(catchupBatchSize); len(batch) > 0 {
		p.catchupBatch(ctx, ep, batch)
	}
	ep.syncPendingGauge()
	if dropped, ok := ep.pending.drainedAfterOverflow(); ok {
		p.logger.Warn(
			"merkle-service catch-up queue had overflowed; requesting a full resync of this endpoint",
			zap.String("endpoint", ep.url),
			zap.Int("dropped", dropped),
		)
		p.fireResync(ep.url)
	}
}

// catchupBatch sends one taken batch, settles it, adjusts the endpoint's
// backoff and logs the outcome.
func (p *Pool) catchupBatch(ctx context.Context, ep *endpoint, batch []Registration) {
	r := p.sendCatchup(ctx, ep, batch)
	ep.pending.settle(len(batch))
	progress := r.landed + r.dropped
	ep.mu.Lock()
	switch {
	case progress == 0 && ctx.Err() == nil:
		if ep.backoffTicks == 0 {
			ep.backoffTicks = 1
		} else if ep.backoffTicks < catchupMaxBackoffTicks {
			ep.backoffTicks *= 2
		}
		ep.skipTicks = ep.backoffTicks
	case progress > 0:
		ep.backoffTicks = 0
	}
	ep.mu.Unlock()
	if r.landed > 0 {
		p.logger.Info(
			"merkle-service catch-up delivered missed registrations",
			zap.String("endpoint", ep.url),
			zap.Int("landed", r.landed),
			zap.Int("failed", len(batch)-progress),
			zap.Int("still_pending", ep.pending.len()),
		)
	}
	if r.dropped > 0 {
		p.logger.Warn(
			"merkle-service endpoint rejected catch-up registrations; dropped them",
			zap.String("endpoint", ep.url),
			zap.Int("dropped", r.dropped),
			zap.Error(r.sampleDropped),
		)
	}
	if r.auth > 0 {
		// 401/403 do not count toward the breaker, so this log is the only
		// signal besides the gauge. They are kept: a token fixed on the
		// merkle-service side should not need an arcade restart.
		p.logger.Warn(
			"merkle-service endpoint rejected the auth token on catch-up; check merkle_service.auth_token",
			zap.String("endpoint", ep.url),
			zap.Int("rejected", r.auth),
			zap.Int("still_pending", ep.pending.len()),
		)
	}
}

// catchupResult summarizes one catch-up batch.
type catchupResult struct {
	landed  int // acknowledged by the endpoint
	dropped int // rejected outright; not re-queued
	auth    int // refused with 401/403; re-queued
	// sampleDropped is one of the rejections behind dropped, for the log.
	sampleDropped error
}

// sendCatchup registers batch with ep alone and re-queues every failure
// except an outright rejection.
func (p *Pool) sendCatchup(ctx context.Context, ep *endpoint, batch []Registration) catchupResult {
	var r catchupResult
	for i, err := range p.runEndpointBatch(ctx, ctx, ep, opCatchup, batch, catchupConcurrency, nil) {
		switch outcome, _ := classify(ctx, err); {
		case err == nil:
			r.landed++
		case permanentRejection(err):
			r.dropped++
			if r.sampleDropped == nil {
				r.sampleDropped = err
			}
		default:
			if outcome == outcomeAuth {
				r.auth++
			}
			ep.pending.add(batch[i])
		}
	}
	return r
}

// permanentRejection reports whether err is a /watch refusal that sending the
// same registration again cannot fix: a 4xx other than 401/403 (a token
// problem, fixable on either side), 408 or 429 (the endpoint asked us to
// slow down or try again).
func permanentRejection(err error) bool {
	var regErr *RegisterError
	if !errors.As(err, &regErr) {
		return false
	}
	switch code := regErr.StatusCode; {
	case code < http.StatusBadRequest || code >= http.StatusInternalServerError:
		return false
	case code == http.StatusUnauthorized, code == http.StatusForbidden,
		code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return false
	default:
		return true
	}
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
	// runEndpointBatch refuses fast (ErrNoHealthyEndpoints, nothing sent)
	// for every entry once the breaker is open, including from the start.
	out := s.pool.runEndpointBatch(ctx, ctx, s.ep, opWatch, registrations, maxConcurrency, nil)
	// An outright rejection is not queued: re-sending it cannot succeed
	// (same rule as sendCatchup and Retain).
	for i, reg := range registrations {
		if out[i] != nil && !permanentRejection(out[i]) {
			s.ep.pending.add(reg)
		}
	}
	s.ep.syncPendingGauge()
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
