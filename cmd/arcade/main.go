package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/bsv-blockchain/arcade/app"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/services"
	"github.com/bsv-blockchain/arcade/telemetry"
	"github.com/bsv-blockchain/arcade/version"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "arcade",
		Short: "Arcade transaction management service",
		RunE:  run,
	}
	// Errors are logged through zap inside run(); the fmt.Fprintf below
	// remains the single place a bare error hits stderr, so cobra must not
	// also print its own usage/error text.
	rootCmd.SilenceErrors = true
	rootCmd.SilenceUsage = true

	config.BindFlags(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load(cmd)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	logger := newLogger(cfg.LogLevel)
	defer func() { _ = logger.Sync() }()

	// Capture stdlib log.* output (gocore and other libraries that log
	// through the standard library's package-global logger) as structured
	// JSON alongside every other log line, instead of leaking unstructured
	// text to stderr.
	undo := zap.RedirectStdLog(logger.Named("stdlog"))
	defer undo()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tShutdown, err := telemetry.Init(ctx, cfg.Telemetry, telemetry.Options{
		Version: version.Version,
		Mode:    cfg.Mode,
		Logger:  logger,
	})
	if err != nil {
		return fmt.Errorf("telemetry init: %w", err)
	}
	defer func() {
		// Bound the final flush by the configured export timeout, floored at
		// 10s so a small or unset value never truncates the last export.
		shutdownTimeout := max(10*time.Second, time.Duration(cfg.Telemetry.ExportTimeoutMs)*time.Millisecond)
		sctx, scancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer scancel()
		_ = tShutdown(sctx)
	}()

	deps, cleanup, err := app.Bootstrap(ctx, cfg, logger)
	if err != nil {
		logger.Error("bootstrap failed", zap.Error(err))
		return err
	}
	defer cleanup()

	svcs := app.BuildServices(deps)

	// Start health server for non-API modes (api-server serves /health on its own port)
	var hs *services.HealthServer
	if cfg.Mode != "api-server" {
		hs = services.NewHealthServer(cfg.Health.Port, cfg.Health.PprofEnabled, logger)
		hs.Start(ctx)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	errCh := make(chan error, len(svcs))

	for _, svc := range svcs {
		wg.Add(1)
		go func(s services.Service) {
			defer wg.Done()
			logger.Info("starting service", zap.String("service", s.Name()))
			if err := s.Start(ctx); err != nil {
				logger.Error("service failed", zap.String("service", s.Name()), zap.Error(err))
				errCh <- fmt.Errorf("service %s: %w", s.Name(), err)
			}
		}(svc)
	}

	if hs != nil {
		hs.SetReady(true)
	}

	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		logger.Error("service error, shutting down", zap.Error(err))
	}

	// Fail readiness, then wait for kube-proxy to drop the endpoint before draining.
	if hs != nil {
		hs.SetReady(false)
	}
	time.Sleep(5 * time.Second)

	cancel()

	// Bound shutdown so a hung Stop() can't outlive terminationGracePeriodSeconds.
	done := make(chan struct{})
	go func() {
		for _, svc := range svcs {
			logger.Info("stopping service", zap.String("service", svc.Name()))
			if err := svc.Stop(); err != nil {
				logger.Error("error stopping service", zap.String("service", svc.Name()), zap.Error(err))
			}
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("arcade stopped")
	case <-time.After(30 * time.Second):
		logger.Warn("shutdown timed out after 30s, forcing exit")
	}
	return nil
}

func newLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zap.DebugLevel
	case "warn":
		zapLevel = zap.WarnLevel
	case "error":
		zapLevel = zap.ErrorLevel
	default:
		zapLevel = zap.InfoLevel
	}

	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zapLevel),
		Encoding:         "json",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig:    zap.NewProductionEncoderConfig(),
	}

	logger, _ := cfg.Build()
	return logger
}
