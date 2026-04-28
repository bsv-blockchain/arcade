package api_server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

type Server struct {
	cfg         *config.Config
	logger      *zap.Logger
	producer    *kafka.Producer
	store       store.Store
	txTracker   *store.TxTracker
	teranode    *teranode.Client        // used by /health for datahub URL inventory; nil in tests
	server      *http.Server
	chaintracks chaintracks.Chaintracks // nil when disabled
	ctRoutes    *chaintracksRoutes      // nil when disabled
}

func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, st store.Store, tracker *store.TxTracker, tc *teranode.Client) *Server {
	return &Server{
		cfg:       cfg,
		logger:    logger.Named("api-server"),
		producer:  producer,
		store:     st,
		txTracker: tracker,
		teranode:  tc,
	}
}

func (s *Server) Name() string { return "api-server" }

func (s *Server) Start(ctx context.Context) error {
	// Bring up chaintracks BEFORE the router is assembled so registerRoutes
	// can mount its handlers only when a live instance is present.
	if err := s.initChaintracks(ctx); err != nil {
		return fmt.Errorf("initializing chaintracks: %w", err)
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.CustomRecovery(s.recoverPanic))
	router.Use(s.requestLogger())

	s.registerRoutes(router)

	addr := fmt.Sprintf("%s:%d", s.cfg.APIServer.Host, s.cfg.APIServer.Port)
	s.server = &http.Server{
		Addr:    addr,
		Handler: router,
	}

	s.logger.Info("API server listening", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// initChaintracks spins up the embedded go-chaintracks instance when
// ChaintracksServer.Enabled is true. Shutdown is driven by ctx — when the
// api-server's context is cancelled, chaintracks's P2P subscription and SSE
// broadcasters unwind themselves.
//
// Initialization failures are returned as errors so main.go can surface them
// as a fatal startup error rather than silently disabling the feature.
func (s *Server) initChaintracks(ctx context.Context) error {
	if !s.cfg.ChaintracksServer.Enabled {
		s.logger.Debug("chaintracks disabled")
		return nil
	}

	// Default chaintracks storage to <storage_path>/chaintracks/ so operators
	// only need to set a single storage root in the common case. Tilde expansion
	// happens in config.Load, so root is already a real filesystem path here.
	if s.cfg.Chaintracks.StoragePath == "" {
		root := s.cfg.StoragePath
		if root == "" {
			root = "."
		}
		if err := os.MkdirAll(root, 0o750); err != nil {
			return fmt.Errorf("creating storage directory %s: %w", root, err)
		}
		s.cfg.Chaintracks.StoragePath = path.Join(root, "chaintracks")
	}

	// Thread the top-level network into chaintracks' embedded p2p config.
	// Without this, go-chaintracks.Config.Initialize sees an empty Network and
	// silently falls back to "main" — so a testnet/teratestnet arcade would
	// still bootstrap its block headers from mainnet. Bootstrap peers are
	// injected from the same resolver the discovery service uses, so chaintracks
	// and the datahub client always agree on which network they joined.
	//
	// Chaintracks needs the upstream-strict spelling ("main"/"test"/"teratestnet")
	// because its chainmanager.getGenesisHeader switch is exact-match; the
	// p2p-client tolerates either form via its own alias map, so the discovery
	// service still gets the canonical "mainnet"/"testnet"/"teratestnet" topic.
	_, defaultBootstrap := config.ResolveP2PNetwork(s.cfg.Network)
	s.cfg.Chaintracks.P2P.Network = config.ResolveChaintracksNetwork(s.cfg.Network)
	if len(s.cfg.Chaintracks.P2P.MsgBus.BootstrapPeers) == 0 {
		s.cfg.Chaintracks.P2P.MsgBus.BootstrapPeers = defaultBootstrap
	}

	ct, err := s.cfg.Chaintracks.Initialize(ctx, "arcade", nil)
	if err != nil {
		return fmt.Errorf("chaintracks init: %w", err)
	}
	s.chaintracks = ct
	s.ctRoutes = newChaintracksRoutes(ctx, ct)

	network, _ := ct.GetNetwork(ctx)
	s.logger.Info("Chaintracks HTTP API enabled",
		zap.String("storage_path", s.cfg.Chaintracks.StoragePath),
		zap.String("network", network),
	)
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
