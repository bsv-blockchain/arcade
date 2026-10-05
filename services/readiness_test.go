package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
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

type muteService struct{}

func (muteService) Start(context.Context) error { return nil }
func (muteService) Stop() error                 { return nil }
func (muteService) Name() string                { return "mute" }

func TestReadinessGate_ReadyOnlyAfterEveryStartup(t *testing.T) {
	port := freePort(t)
	hs := NewHealthServer(port, false, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs.Start(ctx)
	waitForListening(t, port)

	release := make(chan struct{})
	first := &readyStub{name: "propagation", release: release, blocked: make(chan struct{})}
	second := &readyStub{name: "bump-builder", release: release, blocked: make(chan struct{})}
	gate, err := PrepareReadiness(hs, []Service{first, second})
	if err != nil {
		t.Fatalf("PrepareReadiness: %v", err)
	}

	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("before start: got %d, want 503", got)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- first.Start(ctx) }()
	go func() { errCh <- second.Start(ctx) }()

	// Both Start calls are inside their startup sequence and have not
	// signaled. /ready must stay 503.
	<-first.blocked
	<-second.blocked
	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("during startup: got %d, want 503", got)
	}

	close(release)
	waitForStatus(t, port, http.StatusOK)
	if got := readyStatus(t, port); got != http.StatusOK {
		t.Fatalf("after startup: got %d, want 200", got)
	}

	gate.Disable()
	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("after shutdown: got %d, want 503", got)
	}
	// A report that lands after Disable must not put the pod back in service.
	gate.Report()
	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("late report: got %d, want 503", got)
	}
}

func TestReadinessGate_FailedStartupStaysNotReady(t *testing.T) {
	port := freePort(t)
	hs := NewHealthServer(port, false, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs.Start(ctx)
	waitForListening(t, port)

	good := &readyStub{name: "p2p-client", entered: make(chan struct{})}
	bad := &readyStub{name: "propagation", fail: errors.New("consumer group"), entered: make(chan struct{})}
	if _, err := PrepareReadiness(hs, []Service{good, bad}); err != nil {
		t.Fatalf("PrepareReadiness: %v", err)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- good.Start(ctx) }()
	go func() { errCh <- bad.Start(ctx) }()

	<-good.entered
	<-bad.entered
	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("partial startup: got %d, want 503", got)
	}
}

func TestReadinessGate_EmptyServiceListIsReady(t *testing.T) {
	port := freePort(t)
	hs := NewHealthServer(port, false, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs.Start(ctx)
	waitForListening(t, port)

	if _, err := PrepareReadiness(hs, nil); err != nil {
		t.Fatalf("PrepareReadiness: %v", err)
	}
	if got := readyStatus(t, port); got != http.StatusOK {
		t.Fatalf("no services: got %d, want 200", got)
	}
}

func TestPrepareReadiness_RequiresNotifier(t *testing.T) {
	hs := NewHealthServer(0, false, zap.NewNop())
	_, err := PrepareReadiness(hs, []Service{muteService{}})
	if err == nil {
		t.Fatal("expected error for a service that cannot report readiness")
	}
}

func TestPrepareReadiness_NilHealthServer(t *testing.T) {
	gate, err := PrepareReadiness(nil, []Service{&readyStub{name: "api-server"}})
	if err != nil {
		t.Fatalf("PrepareReadiness: %v", err)
	}
	if gate == nil {
		t.Fatal("api-server mode still returns a gate so shutdown can call Disable")
	}
	gate.Report()
	gate.Disable()
}

func TestReadinessGate_DisableBeforeReportStaysNotReady(t *testing.T) {
	port := freePort(t)
	hs := NewHealthServer(port, false, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs.Start(ctx)
	waitForListening(t, port)

	gate := NewReadinessGate(hs, 1)
	gate.Disable()
	gate.Report()
	if got := readyStatus(t, port); got != http.StatusServiceUnavailable {
		t.Fatalf("report after disable: got %d, want 503", got)
	}
}

func TestListenAndServeReady_SignalsAfterBind(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	var hook ReadyHook
	ready := make(chan struct{})
	var dialErr error
	hook.NotifyReady(func() {
		dialer := &net.Dialer{Timeout: time.Second}
		c, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			dialErr = err
		} else {
			_ = c.Close()
		}
		close(ready)
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }),
		ReadHeaderTimeout: 30 * time.Second,
	}
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
	var resp *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err = http.DefaultClient.Do(req)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET after bind: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status: got %d, want 204", resp.StatusCode)
	}
	_ = srv.Close()
	<-errCh
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
	if signaled {
		t.Fatal("readiness signaled even though listen failed")
	}
}

func readyStatus(t *testing.T, port int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/ready", port), nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func waitForStatus(t *testing.T, port, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last int
	for {
		last = readyStatus(t, port)
		if last == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for /ready %d, last %d", want, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
