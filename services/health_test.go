package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/bsv-blockchain/arcade/services/servicetest"
)

// TestHealthServer_PprofGating asserts that net/http/pprof routes are only
// reachable when the pprof_enabled flag is set. The gate matters because the
// CPU and trace endpoints are expensive to capture and the heap profile
// exposes information operators may not want continuously available.
func TestHealthServer_PprofGating(t *testing.T) {
	cases := []struct {
		name       string
		enabled    bool
		wantStatus int
	}{
		{"disabled returns 404", false, http.StatusNotFound},
		{"enabled returns 200", true, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			port := servicetest.FreePort(t)
			hs := NewHealthServer(port, tc.enabled, zaptest.NewLogger(t))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hs.Start(ctx)
			servicetest.WaitListening(t, fmt.Sprintf("127.0.0.1:%d", port))

			req, err := http.NewRequestWithContext(ctx, http.MethodGet,
				fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/heap", port), nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("GET /debug/pprof/heap: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, resp.Body)

			if resp.StatusCode != tc.wantStatus {
				t.Errorf("status: got %d, want %d", resp.StatusCode, tc.wantStatus)
			}
		})
	}
}

// TestHealthServer_HealthAlwaysReachable guards the regression where adding
// pprof gating accidentally guards /health or /metrics as well.
func TestHealthServer_HealthAlwaysReachable(t *testing.T) {
	port := servicetest.FreePort(t)
	hs := NewHealthServer(port, false, zaptest.NewLogger(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs.Start(ctx)
	servicetest.WaitListening(t, fmt.Sprintf("127.0.0.1:%d", port))

	for _, path := range []string{"/health", "/metrics"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		if err != nil {
			t.Fatalf("build request for %s: %v", path, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: got %d, want 200", path, resp.StatusCode)
		}
	}
}
