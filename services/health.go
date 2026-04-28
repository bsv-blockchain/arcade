package services

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// HealthServer provides /health and /ready endpoints for non-API services.
type HealthServer struct {
	server *http.Server
	ready  atomic.Bool
	logger *zap.Logger
}

func NewHealthServer(port int, logger *zap.Logger) *HealthServer {
	hs := &HealthServer{
		logger: logger.Named("health"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if hs.ready.Load() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ready"}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"not ready"}`))
		}
	})
	// Prometheus scrape endpoint. Uses the default registry the metrics package
	// registers all its vectors against; promhttp handles content negotiation
	// (text vs OpenMetrics) and per-collector errors.
	mux.Handle("/metrics", promhttp.Handler())

	hs.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return hs
}

func (hs *HealthServer) Start(ctx context.Context) {
	go func() {
		hs.logger.Info("health server listening", zap.String("addr", hs.server.Addr))
		if err := hs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hs.logger.Error("health server error", zap.Error(err))
		}
	}()

	go func() {
		<-ctx.Done()
		hs.server.Close()
	}()
}

func (hs *HealthServer) SetReady(ready bool) {
	hs.ready.Store(ready)
}
