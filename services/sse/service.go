// Package sse hosts the standalone arcade SSE service. It owns the
// `/events` endpoint, fans transaction-status updates from the shared
// events.Publisher (a Kafka subscriber per pod) out to connected clients,
// and serves Last-Event-ID-driven catchup from the shared store.
//
// In `mode=sse` the service is the only thing running in the pod. In
// `mode=all` it runs as a goroutine alongside other services and binds
// its own HTTP port (default 8082) — operators must ensure
// api.port != sse.port to avoid the bind collision.
package sse

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
	"github.com/bsv-blockchain/arcade/store"
)

const jsonKeyError = "error"

// Service is the services.Service-conforming wrapper around the SSE
// manager. Owns its own gin.Engine + http.Server so /events lives on a
// dedicated port — slow SSE clients can't backpressure /submit on the
// api-server pod.
type Service struct {
	cfg       *config.Config
	logger    *zap.Logger
	publisher events.Publisher
	store     store.Store

	manager *Manager
	server  *http.Server
}

// New constructs the Service. Returns nil when sse.enabled is false or
// publisher is nil — callers should treat a nil Service as "don't run
// SSE in this deployment" rather than an error.
func New(cfg *config.Config, logger *zap.Logger, publisher events.Publisher, st store.Store) *Service {
	if !cfg.SSE.Enabled || publisher == nil {
		return nil
	}
	return &Service{
		cfg:       cfg,
		logger:    logger.Named("sse"),
		publisher: publisher,
		store:     st,
	}
}

// Name implements services.Service.
func (s *Service) Name() string { return "sse" }

// Start brings up the manager subscription and the HTTP server. Blocks
// until ctx is canceled or ListenAndServe returns a non-graceful error.
func (s *Service) Start(ctx context.Context) error {
	mgr, err := newManager(ctx, s.publisher, s.store, s.logger)
	if err != nil {
		return fmt.Errorf("initializing SSE manager: %w", err)
	}
	s.manager = mgr

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/events", s.handleEvents)
	router.GET("/health", s.handleHealth)
	router.GET("/ready", s.handleHealth)

	addr := fmt.Sprintf("%s:%d", s.cfg.SSE.Host, s.cfg.SSE.Port)
	s.server = &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 30 * time.Second,
	}
	s.logger.Info("SSE service listening", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		_ = s.Stop()
	}()

	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("sse server error: %w", err)
	}
	return nil
}

// Stop gracefully shuts the HTTP server.
func (s *Service) Stop() error {
	if s.server != nil {
		s.logger.Info("shutting down SSE service")
		return s.server.Close()
	}
	return nil
}

// handleHealth is a minimal liveness probe for SSE pods. The real
// readiness signal is whether the Kafka subscriber is healthy; that's
// checked at startup (Subscribe returns an error if the broker is
// unreachable) and reflected by the process exit code if it fails.
func (s *Service) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
