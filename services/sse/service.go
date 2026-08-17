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
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/services/httpmiddleware"
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
	// draining flips on shutdown and is read by /ready. Liveness deliberately
	// does NOT consult it: failing liveness mid-drain gets the pod killed
	// instead of drained.
	draining atomic.Bool
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
	// Thread the configured per-connection buffer capacity through to the
	// manager so NewClient sizes each client's send channel accordingly.
	// A non-positive value leaves clientBufferSize at 0, which NewClient
	// treats as "use defaultClientBuffer".
	if mgr != nil {
		mgr.clientBufferSize = s.cfg.SSE.ClientBufferSize
		mgr.catchupOnDrop = s.cfg.SSE.CatchupOnDrop
	}
	s.manager = mgr

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/events", s.handleEvents)
	router.GET("/health", s.handleHealth)
	router.GET("/ready", s.handleReady)

	addr := fmt.Sprintf("%s:%d", s.cfg.SSE.Host, s.cfg.SSE.Port)
	s.server = &http.Server{
		Addr:              addr,
		Handler:           httpmiddleware.WithCORS(router),
		ReadHeaderTimeout: 30 * time.Second,
	}
	s.logger.Info("SSE service listening", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		_ = s.Stop() //nolint:contextcheck // Stop uses context.Background() so the 15s drain outlives the parent ctx that just fired.
	}()

	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("sse server error: %w", err)
	}
	return nil
}

// Stop drains connected clients and then shuts the HTTP server.
//
// http.Server.Shutdown alone is the wrong tool here: it waits for connections
// to become IDLE, and an SSE connection never is. Left to it, shutdown burned
// its whole timeout, the process exited, and every stream was severed with a
// TCP RST — putting every client back on the door of the next pod at the same
// instant. So the drain is explicit and ordered:
//
//  1. Fail /ready, so kube-proxy and the ingress stop sending new connections.
//  2. Wait DrainQuiesce for that withdrawal to propagate. Cutting connections
//     before it lands just bounces clients back into this terminating pod.
//  3. Release every client with its own reconnect hint, so they come back as a
//     ramp rather than a herd, and close each stream cleanly.
//  4. Shutdown, which now completes promptly because nothing is still open.
//
// The three windows must fit inside the pod's terminationGracePeriodSeconds;
// if the kubelet SIGKILLs mid-drain the RST is back.
func (s *Service) Stop() error {
	if s.server == nil {
		return nil
	}
	s.draining.Store(true)

	quiesce := msOrDefault(s.cfg.SSE.DrainQuiesceMS, config.DefaultSSEDrainQuiesceMS)
	retryMax := time.Duration(max(s.cfg.SSE.DrainRetryMaxMS, 0)) * time.Millisecond
	s.logger.Info("draining SSE service",
		zap.Duration("quiesce", quiesce),
		zap.Duration("retry_max", retryMax),
		zap.Int("clients", s.clientCount()))

	time.Sleep(quiesce)

	if s.manager != nil {
		released := s.manager.Drain(retryMax)
		s.logger.Info("released SSE clients", zap.Int("clients", released))
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		msOrDefault(s.cfg.SSE.ShutdownTimeoutMS, config.DefaultSSEShutdownTimeoutMS))
	defer cancel()
	return s.server.Shutdown(ctx)
}

// clientCount reports registered clients, tolerating a nil manager.
func (s *Service) clientCount() int {
	if s.manager == nil {
		return 0
	}
	return s.manager.clientCount()
}

// msOrDefault converts a millisecond config value to a Duration, falling back
// to def when the value is non-positive.
func msOrDefault(ms, def int) time.Duration {
	if ms <= 0 {
		ms = def
	}
	return time.Duration(ms) * time.Millisecond
}

// handleHealth is the liveness probe. It reports the process is alive and
// deliberately keeps reporting that during a drain — a liveness failure gets
// the pod killed, which is the opposite of draining it.
func (s *Service) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// handleReady is the readiness probe. It fails as soon as shutdown begins so
// the pod is withdrawn from Service endpoints before any connection is cut.
// Splitting it from liveness is what makes the drain in Stop possible: the two
// used to be the same handler, so there was no way to say "stop sending me
// traffic" without also saying "kill me".
func (s *Service) handleReady(c *gin.Context) {
	if s.draining.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "draining"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
