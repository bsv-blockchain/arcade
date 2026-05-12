// Package chaintracks_server hosts the standalone arcade chaintracks
// service. It owns the embedded go-chaintracks instance, exposes the
// HTTP API under `/chaintracks/v1` and `/chaintracks/v2`, and bridges
// chaintracks's tip + reorg channels into the shared block_processing
// store so downstream services (watchdog, api-server) consume header
// data from the database rather than calling back into chaintracks.
//
// In `mode=chaintracks` this service runs alone in the pod. In
// `mode=all` it runs as a goroutine alongside other services on its
// own port (default 8083). External clients reach the chaintracks
// HTTP API through a K8s Service that targets the chaintracks
// Deployment.
package chaintracks_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
)

const jsonKeyError = "error"

// Service is the services.Service-conforming wrapper around the
// embedded chaintracks instance. Owns its own gin.Engine + http.Server.
type Service struct {
	cfg    *config.Config
	logger *zap.Logger
	store  store.Store

	ct      chaintracks.Chaintracks
	routes  *Routes
	tracker *blockStatusTracker
	server  *http.Server
}

// New constructs the Service. Returns nil when chaintracks_server is
// disabled — callers treat nil as "don't run chaintracks in this
// deployment". The regtest network has no genesis header in go-chaintracks
// so the runtime force-disables this service when network=regtest
// (config validate already does that).
func New(cfg *config.Config, logger *zap.Logger, st store.Store) *Service {
	if !cfg.ChaintracksServer.Enabled {
		return nil
	}
	return &Service{
		cfg:    cfg,
		logger: logger.Named("chaintracks"),
		store:  st,
	}
}

// Name implements services.Service.
func (s *Service) Name() string { return "chaintracks" }

// Start initializes chaintracks, brings up the block-status bridge,
// and runs the HTTP server. Blocks until ctx is canceled or
// ListenAndServe returns a non-graceful error.
func (s *Service) Start(ctx context.Context) error {
	if err := s.initChaintracks(ctx); err != nil {
		return fmt.Errorf("initializing chaintracks: %w", err)
	}

	// Bridge chaintracks tip + reorg → block_processing store. Downstream
	// services (watchdog, api-server) read from the shared table, so this
	// service is responsible for keeping it fresh.
	s.tracker = newBlockStatusTracker(ctx, s.ct, s.store, s.logger)

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	s.registerRoutes(router)

	addr := fmt.Sprintf("%s:%d", s.cfg.ChaintracksServer.Host, s.cfg.ChaintracksServer.Port)
	s.server = &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 30 * time.Second,
	}
	s.logger.Info("chaintracks service listening", zap.String("addr", addr))

	go func() {
		<-ctx.Done()
		_ = s.Stop()
	}()

	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("chaintracks server error: %w", err)
	}
	return nil
}

// Stop gracefully shuts the HTTP server. chaintracks itself unwinds
// when ctx is canceled (its P2P subscription and SSE broadcasters key
// off the same ctx).
func (s *Service) Stop() error {
	if s.server != nil {
		s.logger.Info("shutting down chaintracks service")
		return s.server.Close()
	}
	return nil
}

// initChaintracks spins up the embedded go-chaintracks instance. Moved
// verbatim from services/api_server/server.go — the only behavioral
// change is which process this code runs in.
func (s *Service) initChaintracks(ctx context.Context) error {
	// Default chaintracks storage to <storage_path>/chaintracks/ so
	// operators only need to set a single storage root. Tilde expansion
	// happens in config.Load.
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

	// Thread the top-level network into chaintracks' embedded p2p
	// config. Without this go-chaintracks falls back to "main" silently.
	// Chaintracks needs the upstream-strict spelling
	// ("main"/"test"/"teratestnet").
	_, defaultBootstrap := config.ResolveP2PNetwork(s.cfg.Network)
	s.cfg.Chaintracks.P2P.Network = config.ResolveChaintracksNetwork(s.cfg.Network)
	if len(s.cfg.Chaintracks.P2P.MsgBus.BootstrapPeers) == 0 {
		s.cfg.Chaintracks.P2P.MsgBus.BootstrapPeers = defaultBootstrap
	}

	ct, err := s.cfg.Chaintracks.Initialize(ctx, "arcade", nil)
	if err != nil {
		return fmt.Errorf("chaintracks init: %w", err)
	}
	s.ct = ct
	s.routes = NewRoutes(ctx, ct)

	network, _ := ct.GetNetwork(ctx)
	s.logger.Info("Chaintracks HTTP API enabled",
		zap.String("storage_path", s.cfg.Chaintracks.StoragePath),
		zap.String("network", network),
	)
	return nil
}

// registerRoutes mounts /chaintracks/v1 + /chaintracks/v2 plus the bulk
// header-file handler. Health probes also live here so K8s liveness
// checks the same listener.
func (s *Service) registerRoutes(r *gin.Engine) {
	r.GET("/health", s.handleHealth)
	r.GET("/ready", s.handleHealth)

	v2 := r.Group("/chaintracks/v2")
	s.routes.Register(v2)
	v1 := r.Group("/chaintracks/v1")
	s.routes.RegisterLegacy(v1)

	storagePath := s.cfg.Chaintracks.StoragePath
	r.GET("/chaintracks/:file", func(c *gin.Context) {
		file := c.Param("file")
		if file == "" || file == "v1" || file == "v2" || containsUnsafePathChars(file) {
			c.Status(http.StatusNotFound)
			return
		}

		basePath := filepath.Clean(storagePath)
		resolvedPath := filepath.Clean(filepath.Join(basePath, file))
		baseWithSep := basePath + string(os.PathSeparator)
		if resolvedPath != basePath && !strings.HasPrefix(resolvedPath, baseWithSep) {
			c.Status(http.StatusNotFound)
			return
		}

		c.File(resolvedPath)
	})
}

func (s *Service) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// containsUnsafePathChars rejects filenames that could escape the
// storage directory. Header files are flat names like
// `mainNet_0.headers`, so any slash or dot-dot is unsafe.
func containsUnsafePathChars(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' || s[i] == '\\' {
			return true
		}
	}
	if s == ".." || len(s) >= 3 && s[:3] == "../" {
		return true
	}
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '.' && s[i+1] == '.' {
			return true
		}
	}
	return false
}
