package services

import (
	"context"
	"net"
	"net/http"
	"sync"
)

// ReadyHook is the Service.NotifyReady implementation every supervised
// service embeds. The zero value is ready to use: SignalReady is a no-op
// until NotifyReady installs a callback, so unit tests that call Start
// directly are unchanged.
//
// SignalReady is a latch. It runs the installed callback at most once, and
// a SignalReady that finds no callback still consumes the latch, so a
// callback installed afterwards is never invoked. The supervisor therefore
// calls NotifyReady before Start.
type ReadyHook struct {
	mu   sync.Mutex
	fn   func()
	done bool
}

// NotifyReady installs the supervisor callback that SignalReady invokes.
//
// Parameters:
//   - fn: called once, synchronously, on the goroutine that calls
//     SignalReady after startup has succeeded. Nil is ignored.
//
// Side Effects:
//   - Replaces any previously installed callback.
func (h *ReadyHook) NotifyReady(fn func()) {
	if fn == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fn = fn
}

// SignalReady reports that this service's startup sequence has succeeded.
//
// Side Effects:
//   - Invokes the NotifyReady callback on the caller's goroutine the first
//     time it is called. Later calls do nothing.
func (h *ReadyHook) SignalReady() {
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return
	}
	h.done = true
	fn := h.fn
	h.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// ReadinessGate marks a HealthServer ready only after every supervised
// service has reported startup success. It starts not-ready. Disable drops
// readiness for shutdown and ignores any report that arrives later.
type ReadinessGate struct {
	hs        *HealthServer
	mu        sync.Mutex
	remaining int
	closed    bool
}

// NewReadinessGate returns a gate that marks hs ready once n services have
// reported. n <= 0 marks hs ready immediately: bootstrap succeeded and there
// is no service to wait for.
//
// Parameters:
//   - hs: the non-API health server whose /ready endpoint this gate drives.
//     Nil makes every transition a no-op (api-server mode).
//   - n: how many Report calls open the gate.
//
// Side Effects:
//   - When n <= 0 and hs is non-nil, calls hs.SetReady(true).
func NewReadinessGate(hs *HealthServer, n int) *ReadinessGate {
	g := &ReadinessGate{hs: hs, remaining: n}
	if n <= 0 {
		g.mu.Lock()
		g.openLocked()
		g.mu.Unlock()
	}
	return g
}

// Report records one service's successful startup. The report that brings
// the outstanding count to zero opens the gate unless Disable already ran.
//
// Side Effects:
//   - May call HealthServer.SetReady(true).
func (g *ReadinessGate) Report() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	g.remaining--
	if g.remaining == 0 {
		g.openLocked()
	}
}

// Disable fails readiness and ignores any later Report. Called when the
// process begins shutdown so kube-proxy drops the endpoint before drain.
//
// Side Effects:
//   - Calls HealthServer.SetReady(false) when a server is attached.
func (g *ReadinessGate) Disable() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	if g.hs != nil {
		g.hs.logger.Info("readiness gate closed")
		g.hs.SetReady(false)
	}
}

// openLocked flips /ready to 200. The caller holds g.mu.
func (g *ReadinessGate) openLocked() {
	if g.hs == nil {
		return
	}
	g.hs.logger.Info("readiness gate open")
	g.hs.SetReady(true)
}

// PrepareReadiness arms every service to report into a gate bound to hs.
// It must run before any Start.
//
// Parameters:
//   - hs: the non-API health server, or nil in api-server mode, where the
//     process serves /ready on its own listener. With a nil hs the gate still
//     arms the services and accepts their reports but has nothing to flip.
//   - svcs: the services BuildServices returned for this process.
//
// Returns:
//   - The gate. The supervisor calls Disable on it at shutdown.
func PrepareReadiness(hs *HealthServer, svcs []Service) *ReadinessGate {
	gate := NewReadinessGate(hs, len(svcs))
	for _, svc := range svcs {
		svc.NotifyReady(gate.Report)
	}
	return gate
}

// ListenAndServeReady binds srv.Addr, signals hook once the socket is
// accepting connections, then blocks in Serve until srv is closed. It is
// http.Server.ListenAndServe with a readiness signal between bind and serve:
// a listen failure returns before the hook fires, so /ready can only open
// once the listener exists.
//
// Parameters:
//   - ctx: the service lifetime. A ctx that is already done when the bind
//     would happen returns http.ErrServerClosed without signaling, the same
//     outcome ListenAndServe produces once Stop has shut the server down.
//     net.ListenConfig otherwise consults ctx only while resolving Addr.
//   - srv: server to bind. Addr must be set; unlike ListenAndServe there is
//     no ":http" default.
//   - hook: signaled after the listener is bound. Nil skips the signal.
//
// Returns:
//   - The error from listen or Serve, including http.ErrServerClosed when
//     the server is shut down. The listen error is returned as-is; it already
//     names the address.
//
// Notes:
//   - A Stop that lands between the ctx check and Serve still fires the
//     hook, and Serve then returns http.ErrServerClosed at once. The
//     supervisor disables the gate before canceling ctx, so that late report
//     cannot reopen /ready.
func ListenAndServeReady(ctx context.Context, srv *http.Server, hook *ReadyHook) error {
	if ctx.Err() != nil {
		return http.ErrServerClosed
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		_ = ln.Close()
		return http.ErrServerClosed
	}
	if hook != nil {
		hook.SignalReady()
	}
	return srv.Serve(ln)
}
