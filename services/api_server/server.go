package api_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
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

func (s *Server) Start(ctx context.Context) error {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.CustomRecovery(s.recoverPanic))
	router.Use(s.requestLogger())

	s.registerRoutes(router)

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
		Handler:           withCORS(router),
		ReadHeaderTimeout: 30 * time.Second,
	}

	s.logger.Info("API server listening", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		_ = s.Stop()
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
					zap.String("txid", rec.sub.TxID),
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

		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", status),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
		}
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
// through zap (structured) rather than gin's default stderr text writer. The
// requestLogger middleware still runs after this and emits the request line
// at Error level for the recovered 500.
func (s *Server) recoverPanic(c *gin.Context, recovered any) {
	s.logger.Error(
		"panic in handler",
		zap.Any("panic", recovered),
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.String("client_ip", c.ClientIP()),
		zap.Stack("stack"),
	)
	c.AbortWithStatus(http.StatusInternalServerError)
}

func (s *Server) Stop() error {
	// Signal recorder workers to exit. Safe to call multiple times via the
	// guard pattern below; Stop() is invoked by the Start ctx-watcher and may
	// race with an explicit caller.
	select {
	case <-s.submissionStop:
	default:
		close(s.submissionStop)
	}
	if s.server != nil {
		s.logger.Info("shutting down API server")
		return s.server.Close()
	}
	return nil
}
