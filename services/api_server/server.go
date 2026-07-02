package api_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/logfields"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/services/httpmiddleware"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/telemetry"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/validator"
)

const (
	// submissionRecorderBuffer caps the in-memory queue depth feeding the
	// async InsertSubmission workers. 4096 absorbs ~80s of 50 TPS without
	// dropping; sustained backpressure triggers drop+metric (best-effort
	// contract preserved).
	submissionRecorderBuffer = 4096
	// submissionRecorderWorkers is the worker count draining submissionCh.
	// 8 is comfortably above expected DB write concurrency on Pebble; the
	// real limiter is the store.BatchConcurrency knob.
	submissionRecorderWorkers = 8
)

type Server struct {
	cfg          *config.Config
	logger       *zap.Logger
	producer     *kafka.Producer
	publisher    events.Publisher // nil-safe; status updates flow to SSE via Kafka — the api-server itself publishes but does not subscribe
	store        store.Store
	txTracker    *store.TxTracker
	teranode     *teranode.Client      // used by /health for datahub URL inventory; nil in tests
	merkleClient *merkleservice.Client // nil when merkle_service.url is unset; gates POST /api/v1/blocks/:blockHash/reprocess
	// validator runs synchronous policy validation in the submit handler.
	// Nil-safe: tests that use struct-literal construction may leave it
	// unset, in which case the handler skips validation. Production
	// wiring through New requires it.
	validator *validator.Validator
	server    *http.Server

	// submissionCh decouples the InsertSubmission Pebble write from the HTTP
	// handler tail latency. recordSubmission enqueues onto it via a non-
	// blocking select; a worker pool drains and writes asynchronously. Drop on
	// full is acceptable because the underlying call is already best-effort
	// (errors are logged, not surfaced to the client).
	submissionCh   chan submissionRecord
	submissionStop chan struct{}
	stopOnce       sync.Once
}

// submissionRecord is the in-memory payload the async recorder consumes.
// Kept tiny — just enough to call store.InsertSubmission with the original
// values. The original request context is intentionally NOT propagated;
// recordSubmission is best-effort and shouldn't be canceled when the HTTP
// handler returns.
type submissionRecord struct {
	sub *models.Submission
}

func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, publisher events.Publisher, st store.Store, tracker *store.TxTracker, tc *teranode.Client, mc *merkleservice.Client, val *validator.Validator) *Server {
	return &Server{
		cfg:            cfg,
		logger:         logger.Named("api-server"),
		producer:       producer,
		publisher:      publisher,
		store:          st,
		txTracker:      tracker,
		teranode:       tc,
		merkleClient:   mc,
		validator:      val,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
}

func (s *Server) Name() string { return "api-server" }

// newRouter assembles the gin engine and its middleware stack: inbound OTEL
// tracing (outermost), then panic recovery, then the request logger/metrics
// middleware, followed by every registered route. Factored out of Start so
// tests can exercise the full middleware stack (span creation, trace-id log
// correlation) through httptest without binding a real listener.
//
// otelgin must be OUTSIDE CustomRecovery: otelgin restores the pre-span
// request context in a defer, and deferred funcs run innermost-first during
// a panic unwind — were recovery outermost, the span would already be gone
// from c.Request.Context() by the time recoverPanic runs, and panic logs
// would lose their trace_id/span_id. With otelgin outermost the panic is
// recovered while the span is still live (fields attach) and otelgin's own
// post-processing then ends the span with the 500 recovery wrote, so panics
// show up on traces too. The cost — a panic inside otelgin itself has no
// recovery above it — is acceptable: otelgin is thin and widely deployed,
// and net/http still contains per-request panics at the server level.
//
// otelgin.Middleware is installed unconditionally so tracing can be turned
// on via env at runtime without a redeploy. It is NOT free when telemetry is
// disabled: on every non-filtered request otelgin still runs the propagator
// Extract, builds the full semconv attribute slice, copies the request
// context, and assembles the metric attributes (~15+ allocations) — it just
// never exports or touches the network (the no-op provider drops it all).
// That per-request overhead is negligible next to the JSON (de)serialisation
// each handler already does, so gating on Enabled isn't worth losing the
// zero-redeploy toggle. /health, /ready, and /metrics are filtered out —
// they're polled far more often than real traffic and carry no useful trace
// information.
func (s *Server) newRouter() *gin.Engine {
	router := gin.New()
	router.Use(otelgin.Middleware(s.cfg.Telemetry.ServiceName, otelgin.WithGinFilter(func(c *gin.Context) bool {
		p := c.FullPath()
		return p != "/health" && p != "/ready" && p != "/metrics"
	})))
	router.Use(gin.CustomRecovery(s.recoverPanic))
	router.Use(s.requestLogger())

	s.registerRoutes(router)
	return router
}

func (s *Server) Start(ctx context.Context) error {
	gin.SetMode(gin.ReleaseMode)
	router := s.newRouter() //nolint:contextcheck // false positive: requestLogger's telemetry.Fields(c.Request.Context()) already uses the correct per-request context; requestLogger is a gin middleware factory with no ctx parameter of its own for the linter to compare against.

	// Spin up the submission recorder pool. Workers exit on submissionStop
	// (Stop()) which is signaled before the HTTP server is closed. The
	// recorder is intentionally NOT bound to the request context — it's a
	// fire-and-forget best-effort DB write that must outlive the HTTP
	// handler that triggered it, so gosec G118 ("use request-scoped ctx")
	// does not apply here.
	var recorderWG sync.WaitGroup
	for i := 0; i < submissionRecorderWorkers; i++ {
		recorderWG.Add(1)
		go s.runSubmissionRecorder(ctx, &recorderWG)
	}

	addr := fmt.Sprintf("%s:%d", s.cfg.APIServer.Host, s.cfg.APIServer.Port)
	s.server = &http.Server{
		Addr:              addr,
		Handler:           httpmiddleware.WithCORS(router),
		ReadHeaderTimeout: 30 * time.Second,
	}

	s.logger.Info("API server listening", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		_ = s.Stop() //nolint:contextcheck // Stop uses context.Background() so the 15s drain outlives the parent ctx that just fired.
		recorderWG.Wait()
	}()

	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// runSubmissionRecorder drains submissionCh into store.InsertSubmission.
// parentCtx is the process/server lifetime context so a service shutdown
// also unwinds in-flight DB writes; per-call writes derive a short timeout
// child so a recorder write outlives the HTTP request that triggered it
// (handler returns before the row lands — that's the whole point of the
// decoupling).
func (s *Server) runSubmissionRecorder(parentCtx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-s.submissionStop:
			return
		case <-parentCtx.Done():
			return
		case rec, ok := <-s.submissionCh:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
			if err := s.store.InsertSubmission(ctx, rec.sub); err != nil {
				s.logger.Warn(
					"failed to insert submission (async)",
					logfields.TxID(rec.sub.TxID),
					zap.Error(err),
				)
			}
			cancel()
		}
	}
}

func (s *Server) requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		metrics.APIRequestsInFlight.Inc()
		defer metrics.APIRequestsInFlight.Dec()

		start := time.Now()
		c.Next()
		status := c.Writer.Status()

		// Use the matched gin route pattern (not the resolved URL) so /tx/:txid
		// reports as one bucket regardless of which txid was requested. Falls
		// back to "unmatched" for routes Gin couldn't resolve.
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		metrics.APIRequestDuration.WithLabelValues(
			route,
			c.Request.Method,
			metrics.ObserveStatusClass(status),
		).Observe(time.Since(start).Seconds())

		// Request body size — caps cardinality by routing through the route
		// label rather than per-request. ContentLength is -1 if not set; clamp
		// to 0 in that case.
		if reqLen := c.Request.ContentLength; reqLen > 0 {
			metrics.APIRequestBytes.WithLabelValues(route).Observe(float64(reqLen))
		}

		fields := append([]zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			// status_code (HTTP), not "status": the canonical "status" field is
			// reserved for the transaction-status string (logfields.Status), so
			// an HTTP status int must use a distinct key to avoid a mixed-type
			// collision in the shared log-field namespace.
			zap.Int("status_code", status),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
		}, telemetry.Fields(c.Request.Context())...)
		switch {
		case status >= 500:
			s.logger.Error("request", fields...)
		case status >= 400:
			s.logger.Warn("request", fields...)
		default:
			s.logger.Debug("request", fields...)
		}
	}
}

// recoverPanic is wired into gin.CustomRecovery so handler panics are logged
// through zap (structured) rather than gin's default stderr text writer.
// Because recovery sits inside otelgin (see newRouter), the request context
// still carries the live span here, so the panic line gets trace_id/span_id
// and the span is subsequently ended with the 500 written below.
func (s *Server) recoverPanic(c *gin.Context, recovered any) {
	fields := append([]zap.Field{
		zap.Any("panic", recovered),
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.String("client_ip", c.ClientIP()),
		zap.Stack("stack"),
	}, telemetry.Fields(c.Request.Context())...)
	s.logger.Error("panic in handler", fields...)
	c.AbortWithStatus(http.StatusInternalServerError)
}

func (s *Server) Stop() error {
	// Stop() is invoked by the Start ctx-watcher and may race with an explicit
	// supervisor caller; sync.Once is the only race-free way to ensure the
	// channel close happens exactly once.
	s.stopOnce.Do(func() {
		close(s.submissionStop)
	})
	if s.server != nil {
		s.logger.Info("shutting down API server")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}
	return nil
}
