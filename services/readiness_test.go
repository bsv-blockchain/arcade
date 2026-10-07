package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/services/servicetest"
)

// readyStub is a Service whose startup blocks until release is closed, then
// signals readiness unless fail is set.
type readyStub struct {
	ReadyHook

	name    string
	release chan struct{}
	fail    error
	// blocked is closed once Start is waiting on release, before SignalReady.
	blocked chan struct{}
	entered chan struct{}
}

func (s *readyStub) Name() string { return s.name }
func (s *readyStub) Stop() error  { return nil }

func (s *readyStub) Start(ctx context.Context) error {
	if s.blocked != nil {
		close(s.blocked)
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.fail != nil {
		if s.entered != nil {
			close(s.entered)
		}
		return s.fail
	}
	s.SignalReady()
	if s.entered != nil {
		close(s.entered)
	}
	<-ctx.Done()
	return nil
}

func startHealthServer(t *testing.T) (*HealthServer, int) {
	t.Helper()
	port := servicetest.FreePort(t)
	hs := NewHealthServer(port, false, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	hs.Start(ctx)
	servicetest.WaitListening(t, fmt.Sprintf("127.0.0.1:%d", port))
	return hs, port
}

func TestReadyHook(t *testing.T) {
	cases := []struct {
		name string
		// run drives the hook and returns how many times the callback ran.
		run       func(h *ReadyHook) int
		wantCalls int
	}{
		{
			name: "zero value signal is a no-op",
			run: func(h *ReadyHook) int {
				h.SignalReady()
				return 0
			},
		},
		{
			name: "callback runs exactly once",
			run: func(h *ReadyHook) int {
				calls := 0
				h.NotifyReady(func() { calls++ })
				h.SignalReady()
				h.SignalReady()
				return calls
			},
			wantCalls: 1,
		},
		{
			name: "nil callback is ignored",
			run: func(h *ReadyHook) int {
				calls := 0
				h.NotifyReady(func() { calls++ })
				h.NotifyReady(nil)
				h.SignalReady()
				return calls
			},
			wantCalls: 1,
		},
		{
			name: "later callback replaces earlier",
			run: func(h *ReadyHook) int {
				calls := 0
				h.NotifyReady(func() { calls += 100 })
				h.NotifyReady(func() { calls++ })
				h.SignalReady()
				return calls
			},
			wantCalls: 1,
		},
		{
			name: "signal before notify consumes the latch",
			run: func(h *ReadyHook) int {
				calls := 0
				h.SignalReady()
				h.NotifyReady(func() { calls++ })
				h.SignalReady()
				return calls
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h ReadyHook
			if got := tc.run(&h); got != tc.wantCalls {
				t.Fatalf("callback calls: got %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestReadinessGate_ReadyOnlyAfterEveryStartup(t *testing.T) {
	hs, port := startHealthServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	first := &readyStub{name: "propagation", release: release, blocked: make(chan struct{})}
	second := &readyStub{name: "bump-builder", release: release, blocked: make(chan struct{})}
	gate := PrepareReadiness(hs, []Service{first, second})

	if got := servicetest.ReadyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("before start: got %d, want 503", got)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- first.Start(ctx) }()
	go func() { errCh <- second.Start(ctx) }()

	// Both Start calls are inside their startup sequence and have not
	// signaled. /ready must stay 503.
	<-first.blocked
	<-second.blocked
	if got := servicetest.ReadyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("during startup: got %d, want 503", got)
	}

	close(release)
	servicetest.WaitForStatus(t, port, http.StatusOK)

	gate.Disable()
	if got := servicetest.ReadyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("after shutdown: got %d, want 503", got)
	}
}

func TestReadinessGate_FailedStartupStaysNotReady(t *testing.T) {
	hs, port := startHealthServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	good := &readyStub{name: "p2p-client", entered: make(chan struct{})}
	bad := &readyStub{name: "propagation", fail: errors.New("consumer group"), entered: make(chan struct{})}
	PrepareReadiness(hs, []Service{good, bad})

	errCh := make(chan error, 2)
	go func() { errCh <- good.Start(ctx) }()
	go func() { errCh <- bad.Start(ctx) }()

	<-good.entered
	<-bad.entered
	if got := servicetest.ReadyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("partial startup: got %d, want 503", got)
	}

	cancel()
	for range 2 {
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Start did not return after cancel")
		}
	}
}

func TestReadinessGate_EmptyServiceListIsReady(t *testing.T) {
	hs, port := startHealthServer(t)
	PrepareReadiness(hs, nil)
	if got := servicetest.ReadyStatus(t, port); got != http.StatusOK {
		t.Fatalf("no services: got %d, want 200", got)
	}
}

func TestPrepareReadiness_NilHealthServer(t *testing.T) {
	stub := &readyStub{name: "api-server"}
	gate := PrepareReadiness(nil, []Service{stub})
	if gate == nil {
		t.Fatal("api-server mode still returns a gate so shutdown can call Disable")
	}
	// The service is armed; its report lands in a gate with nothing to flip.
	stub.SignalReady()
	gate.Disable()
}

func TestReadinessGate_DisableBeforeReportStaysNotReady(t *testing.T) {
	hs, port := startHealthServer(t)
	gate := NewReadinessGate(hs, 1)
	gate.Disable()
	gate.Report()
	if got := servicetest.ReadyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("report after disable: got %d, want 503", got)
	}
}

// TestReadinessGate_ConcurrentReportAndDisable races the final Report
// against Disable. Whichever lands first, Disable must leave /ready closed;
// run with -race to check the locking.
func TestReadinessGate_ConcurrentReportAndDisable(t *testing.T) {
	const n = 8
	for i := range 50 {
		hs := NewHealthServer(0, false, zap.NewNop()) // never started; SetReady only flips the flag
		gate := NewReadinessGate(hs, n)
		var wg sync.WaitGroup
		wg.Add(n + 1)
		for range n {
			go func() {
				defer wg.Done()
				gate.Report()
			}()
		}
		go func() {
			defer wg.Done()
			gate.Disable()
		}()
		wg.Wait()
		if hs.ready.Load() {
			t.Fatalf("iteration %d: /ready open after Disable", i)
		}
	}
}

func TestListenAndServeReady_SignalsAfterBind(t *testing.T) {
	port := servicetest.FreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }),
		ReadHeaderTimeout: 30 * time.Second,
	}
	var hook ReadyHook
	ready := make(chan struct{})
	var dialErr error
	hook.NotifyReady(func() {
		// One dial, no polling: the assertion is that the socket already
		// accepts connections at the moment the signal fires.
		dialer := &net.Dialer{Timeout: time.Second}
		c, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			dialErr = err
		} else {
			_ = c.Close()
		}
		close(ready)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- ListenAndServeReady(ctx, srv, &hook) }()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for readiness signal")
	}
	if dialErr != nil {
		t.Fatalf("listener was not bound when readiness was signaled: %v", dialErr)
	}

	reqCtx, reqCancel := context.WithTimeout(context.Background(), time.Second)
	defer reqCancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+"/", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET after bind: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status: got %d, want 204", resp.StatusCode)
	}
	_ = srv.Close()
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("after Close: got %v, want ErrServerClosed", err)
	}
}

func TestListenAndServeReady_ListenFailureDoesNotSignal(t *testing.T) {
	var hook ReadyHook
	signaled := false
	hook.NotifyReady(func() { signaled = true })
	srv := &http.Server{Addr: "127.0.0.1:-1", ReadHeaderTimeout: time.Second}
	err := ListenAndServeReady(context.Background(), srv, &hook)
	if err == nil {
		t.Fatal("expected listen error")
	}
	if errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("listen failure reported as a clean shutdown: %v", err)
	}
	if signaled {
		t.Fatal("readiness signaled even though listen failed")
	}
}

// TestListenAndServeReady_CanceledContextDoesNotSignal covers a shutdown
// that overtakes Start before the bind: the helper must behave like
// ListenAndServe on a server Stop already shut down, and must not report a
// listener that will never serve.
func TestListenAndServeReady_CanceledContextDoesNotSignal(t *testing.T) {
	var hook ReadyHook
	signaled := false
	hook.NotifyReady(func() { signaled = true })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	srv := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", servicetest.FreePort(t)), ReadHeaderTimeout: time.Second}
	err := ListenAndServeReady(ctx, srv, &hook)
	if !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("got %v, want ErrServerClosed", err)
	}
	if signaled {
		t.Fatal("readiness signaled for a server that never served")
	}
}
