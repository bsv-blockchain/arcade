// Package servicetest holds helpers shared by the readiness tests of the
// supervised services: picking a free TCP port, polling a listener, reading
// a /ready status, and asserting where a service's Start signals readiness.
package servicetest

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

const pollTimeout = 2 * time.Second

// FreePort returns a TCP port that was free on every interface a moment ago.
// The port is released before returning, so a service under test can bind
// it from its own config; the usual bind-then-close race applies.
func FreePort(t *testing.T) int {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// WaitListening polls until addr accepts a TCP connection or two seconds
// pass. Use it after a Start that binds from a goroutine.
func WaitListening(t *testing.T, addr string) {
	t.Helper()
	dialer := &net.Dialer{Timeout: 50 * time.Millisecond}
	deadline := time.Now().Add(pollTimeout)
	for {
		c, err := dialer.DialContext(t.Context(), "tcp", addr)
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing listening on %s after %s: %v", addr, pollTimeout, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ReadyStatus returns the HTTP status of GET /ready on 127.0.0.1:port.
func ReadyStatus(t *testing.T, port int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
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

// WaitForStatus polls /ready on 127.0.0.1:port until it returns want or two
// seconds pass.
func WaitForStatus(t *testing.T, port, want int) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	var last int
	for {
		last = ReadyStatus(t, port)
		if last == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for /ready %d, last %d", want, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// AssertSignalsAfterBind runs start and fails unless the readiness callback
// fires only once addr accepts TCP connections. It then cancels the context
// and requires start to return nil, which is the clean-shutdown contract of
// every HTTP service.
//
// Parameters:
//   - addr: host:port the service is configured to bind.
//   - start: the service's Start method.
//   - arm: the service's NotifyReady method.
func AssertSignalsAfterBind(t *testing.T, addr string, start func(context.Context) error, arm func(func())) {
	t.Helper()
	ready := make(chan struct{})
	var dialErr error
	arm(func() {
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
	go func() { errCh <- start(ctx) }()

	select {
	case <-ready:
	case <-time.After(pollTimeout):
		t.Fatal("timed out waiting for the readiness signal")
	}
	if dialErr != nil {
		t.Fatalf("readiness signaled before the listener was bound: %v", dialErr)
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start returned an error after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

// AssertStartFailureSilent runs start synchronously and fails unless it
// returns an error without ever firing the readiness callback.
//
// Parameters:
//   - start: the service's Start method, configured so startup must fail.
//   - arm: the service's NotifyReady method.
func AssertStartFailureSilent(t *testing.T, start func(context.Context) error, arm func(func())) {
	t.Helper()
	signaled := false
	arm(func() { signaled = true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := start(ctx); err == nil {
		t.Fatal("expected Start to fail before serving")
	}
	if signaled {
		t.Fatal("readiness signaled although Start failed")
	}
}
