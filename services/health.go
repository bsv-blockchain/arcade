package services

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	nhpprof "net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// HealthServer provides /health and /ready endpoints for non-API services.
type HealthServer struct {
	server *http.Server
	ready  atomic.Bool
	logger *zap.Logger
}

func NewHealthServer(port int, pprofEnabled bool, logger *zap.Logger) *HealthServer {
	hs := &HealthServer{
		logger: logger.Named("health"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if hs.ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not ready"}`))
		}
	})
	// Prometheus scrape endpoint. Uses the default registry the metrics package
	// registers all its vectors against; promhttp handles content negotiation
	// (text vs OpenMetrics) and per-collector errors.
	mux.Handle("/metrics", promhttp.Handler())

	if pprofEnabled {
		// Mount net/http/pprof handlers under /debug/pprof on the same
		// listener. The package's init() only registers on the default
		// mux, so each handler is wired explicitly here to keep this mux
		// scoped. Use kubectl port-forward to reach this in production —
		// the health port is not exposed via a Service.
		mux.HandleFunc("/debug/pprof/", nhpprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", nhpprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", nhpprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", nhpprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", nhpprof.Trace)
		hs.logger.Info("pprof handlers mounted under /debug/pprof")
	}

	hs.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}

	return hs
}

func (hs *HealthServer) Start(ctx context.Context) {
	go func() {
		hs.logger.Info("health server listening", zap.String("addr", hs.server.Addr))
		if err := hs.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			hs.logger.Error("health server error", zap.Error(err))
		}
	}()

	go func() {
		<-ctx.Done()
		_ = hs.server.Close()
	}()
}

func (hs *HealthServer) SetReady(ready bool) {
	hs.ready.Store(ready)
}
