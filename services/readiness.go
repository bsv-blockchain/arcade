package services

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
)

// ReadyNotifier is the startup signal the process supervisor arms before
// calling Start. Services that block for their whole lifetime cannot report
// readiness by returning from Start, so they call SignalReady on an embedded
// ReadyHook once initialization has succeeded.
type ReadyNotifier interface {
	// NotifyReady registers fn, which the service invokes exactly once after
	// its startup sequence has succeeded. The supervisor must call NotifyReady
	// before Start. A nil fn is ignored.
	NotifyReady(fn func())
}

// ReadyHook is embedded by every supervised service. The zero value is safe
// and SignalReady is a no-op until NotifyReady installs a callback, so unit
// tests that call Start directly are unchanged.
//
// NotifyReady must happen-before Start. SignalReady is idempotent: a second
// call does not invoke the callback again.
type ReadyHook struct {
	fn   atomic.Value // func()
	once sync.Once
}

// NotifyReady installs the supervisor callback invoked from SignalReady.
//
// Parameters:
//   - fn: called once, synchronously, from the service's Start goroutine
//     after startup has succeeded. Nil is ignored.
//
// Side Effects:
//   - Replaces any previously installed callback. A callback already consumed
//     by SignalReady is not invoked again.
func (h *ReadyHook) NotifyReady(fn func()) {
	if h == nil || fn == nil {
		return
	}
	h.fn.Store(fn)
}

// SignalReady reports that this service's startup sequence has succeeded.
// It is a no-op when no callback is installed, and it runs the callback at
// most once.
//
// Side Effects:
//   - Invokes the NotifyReady callback on the caller's goroutine.
func (h *ReadyHook) SignalReady() {
	if h == nil {
		return
	}
	h.once.Do(func() {
		v := h.fn.Load()
		if v == nil {
			return
		}
		fn, ok := v.(func())
		if !ok || fn == nil {
			return
		}
		fn()
	})
}

// ReadinessGate marks a HealthServer ready only after every supervised
// service has reported startup success. It starts not-ready. Disable drops
// readiness for shutdown and ignores a report that arrives late.
type ReadinessGate struct {
	hs        *HealthServer
	remaining atomic.Int64
	mu        sync.Mutex
	closed    bool
}

// NewReadinessGate returns a gate that marks hs ready once n services have
// reported. n <= 0 marks hs ready immediately: bootstrap succeeded and there
// is no service left to wait for.
//
// Parameters:
//   - hs: the non-API health server whose /ready endpoint this gate drives.
//     Nil is tolerated and makes every transition a no-op.
//   - n: how many Report calls are required before /ready returns 200.
//
// Side Effects:
//   - When n <= 0 and hs is non-nil, calls hs.SetReady(true).
func NewReadinessGate(hs *HealthServer, n int) *ReadinessGate {
	g := &ReadinessGate{hs: hs}
	if n <= 0 {
		g.markReady()
		return g
	}
	g.remaining.Store(int64(n))
	return g
}

// Report records one service's successful startup. The call that drops the
// remaining count to zero marks the health server ready, unless Disable has
// already run.
//
// Side Effects:
//   - May call HealthServer.SetReady(true).
func (g *ReadinessGate) Report() {
	if g == nil {
		return
	}
	if g.remaining.Add(-1) == 0 {
		g.markReady()
	}
}

// Disable fails readiness and ignores any later Report. Called when the
// process begins shutdown so kube-proxy can drop the endpoint before drain.
//
// Side Effects:
//   - Calls HealthServer.SetReady(false) when a server is attached.
func (g *ReadinessGate) Disable() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	if g.hs != nil {
		g.hs.logger.Info("readiness gate closed")
		g.hs.SetReady(false)
	}
}

func (g *ReadinessGate) markReady() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.hs == nil {
		return
	}
	g.hs.logger.Info("readiness gate open")
	g.hs.SetReady(true)
}

// PrepareReadiness arms every service to report into a gate bound to hs.
// api-server mode passes a nil HealthServer because that process serves
// /ready on its own listener once the socket is bound. The returned gate is
// a no-op in that case, and services are left unarmed.
//
// Parameters:
//   - hs: non-API health server, or nil for api-server mode.
//   - svcs: the services BuildServices returned for this process. Each must
//     implement ReadyNotifier.
//
// Returns:
//   - A gate. When hs is nil the gate is a no-op so api-server mode can still
//     call Disable on shutdown. Error when a service cannot report readiness —
//     the process must not start, or /ready would stay 503.
func PrepareReadiness(hs *HealthServer, svcs []Service) (*ReadinessGate, error) {
	if hs == nil {
		// A non-nil no-op gate keeps the supervisor's shutdown path uniform
		// without flipping a health server that this process does not run.
		return &ReadinessGate{}, nil
	}
	gate := NewReadinessGate(hs, len(svcs))
	for _, svc := range svcs {
		n, ok := svc.(ReadyNotifier)
		if !ok {
			return nil, fmt.Errorf("service %q does not report startup readiness", svc.Name())
		}
		n.NotifyReady(gate.Report)
	}
	return gate, nil
}

// ListenAndServeReady binds srv.Addr, signals hook once the socket is
// accepting connections, then blocks in Serve until the server is closed.
// A listen failure returns before the hook is signaled, matching the API
// server: /ready is reachable only after the listener exists.
//
// Parameters:
//   - ctx: cancellation aborts the bind. It does not stop Serve; the caller
//     closes srv on shutdown, the same way ListenAndServe is unwound today.
//   - srv: server to bind. Addr is the listen address. Must be non-nil.
//   - hook: signaled after the listener is bound. Nil skips the signal.
//
// Returns:
//   - The error from listen or Serve, including http.ErrServerClosed when
//     the server is shut down.
//
// Side Effects:
//   - Binds a TCP listener and, on success, calls hook.SignalReady before Serve.
func ListenAndServeReady(ctx context.Context, srv *http.Server, hook *ReadyHook) error {
	if srv == nil {
		return fmt.Errorf("listen: nil server")
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", srv.Addr, err)
	}
	if hook != nil {
		hook.SignalReady()
	}
	return srv.Serve(ln)
}
