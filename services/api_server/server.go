package api_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

type Server struct {
	cfg       *config.Config
	logger    *zap.Logger
	producer  *kafka.Producer
	publisher events.Publisher // nil-safe; status updates flow to SSE via Kafka — the api-server itself publishes but does not subscribe
	store     store.Store
	txTracker *store.TxTracker
	teranode  *teranode.Client // used by /health for datahub URL inventory; nil in tests
	server    *http.Server
}

func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, publisher events.Publisher, st store.Store, tracker *store.TxTracker, tc *teranode.Client) *Server {
	return &Server{
		cfg:       cfg,
		logger:    logger.Named("api-server"),
		producer:  producer,
		publisher: publisher,
		store:     st,
		txTracker: tracker,
		teranode:  tc,
	}
}

func (s *Server) Name() string { return "api-server" }

func (s *Server) Start(ctx context.Context) error {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.CustomRecovery(s.recoverPanic))
	router.Use(s.requestLogger())

	s.registerRoutes(router)

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
	}()

	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
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
	s.logger.Error("panic in handler",
		zap.Any("panic", recovered),
		zap.String("method", c.Request.Method),
		zap.String("path", c.Request.URL.Path),
		zap.String("client_ip", c.ClientIP()),
		zap.Stack("stack"),
	)
	c.AbortWithStatus(http.StatusInternalServerError)
}

func (s *Server) Stop() error {
	if s.server != nil {
		s.logger.Info("shutting down API server")
		return s.server.Close()
	}
	return nil
}
