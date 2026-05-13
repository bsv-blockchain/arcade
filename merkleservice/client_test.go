package merkleservice

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegister(t *testing.T) {
	var gotBody map[string]string
	var gotAuth string
	var gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "mytoken", 0)
	err := client.Register(context.Background(), "abc123", "http://callback/url", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPath != "/watch" {
		t.Errorf("expected /watch, got %s", gotPath)
	}
	if gotAuth != "Bearer mytoken" {
		t.Errorf("expected Bearer mytoken, got %s", gotAuth)
	}
	if gotBody["txid"] != "abc123" {
		t.Errorf("expected txid abc123, got %s", gotBody["txid"])
	}
	if gotBody["callbackUrl"] != "http://callback/url" {
		t.Errorf("expected callbackUrl, got %s", gotBody["callbackUrl"])
	}
}

// TestRegister_ForwardsCallbackToken pins the F-018 fix on the outbound
// /watch path: when arcade is configured with a callback token, that token
// must round-trip through the watch payload so merkle-service can stamp
// Authorization on callbacks. The inbound receiver requires it; without
// forwarding the loop 401s.
func TestRegister_ForwardsCallbackToken(t *testing.T) {
	var rawBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "auth-token", 0)
	if err := client.Register(context.Background(), "abc123", "http://callback/url", "my-token"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Field present and exact match on the wire.
	if !strings.Contains(string(rawBody), `"callbackToken":"my-token"`) {
		t.Errorf("expected body to contain callbackToken=my-token, got %s", string(rawBody))
	}

	// And it round-trips cleanly through json.Unmarshal too.
	var parsed map[string]string
	if err := json.Unmarshal(rawBody, &parsed); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if parsed["callbackToken"] != "my-token" {
		t.Errorf("expected callbackToken=my-token, got %q", parsed["callbackToken"])
	}
}

// TestRegister_OmitsEmptyCallbackToken pins the back-compat half: pre-fix
// callers (and arcade builds without a configured token) must produce a wire
// payload with NO callbackToken key, so merkle-service builds that don't yet
// know the field aren't impacted. The omitempty tag is what enforces this and
// the test exists specifically to fail loudly if someone removes it.
func TestRegister_OmitsEmptyCallbackToken(t *testing.T) {
	var rawBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	if err := client.Register(context.Background(), "abc123", "http://callback/url", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Contains(string(rawBody), "callbackToken") {
		t.Errorf("expected body to omit callbackToken when empty, got %s", string(rawBody))
	}
}

func TestRegister_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	err := client.Register(context.Background(), "abc123", "http://callback", "")
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestRegisterBatch_AllSucceed(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	regs := make([]Registration, 10)
	for i := range regs {
		regs[i] = Registration{TxID: "tx" + string(rune('0'+i)), CallbackURL: "http://cb"}
	}

	err := client.RegisterBatch(context.Background(), regs, 5)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if count.Load() != 10 {
		t.Errorf("expected 10 requests, got %d", count.Load())
	}
}

func TestRegisterBatch_FailFast(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		count.Add(1)
		if strings.Contains(string(body), "fail") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	regs := []Registration{
		{TxID: "fail-tx", CallbackURL: "http://cb"},
	}

	err := client.RegisterBatch(context.Background(), regs, 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRegisterBatch_ConcurrencyBounded(t *testing.T) {
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		// Small delay to let concurrency build up
		// (the bounded goroutine pool should cap it)
		concurrent.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	regs := make([]Registration, 50)
	for i := range regs {
		regs[i] = Registration{TxID: "tx", CallbackURL: "http://cb"}
	}

	err := client.RegisterBatch(context.Background(), regs, 3)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if maxConcurrent.Load() > 3 {
		t.Errorf("expected max concurrency <= 3, got %d", maxConcurrent.Load())
	}
}

func TestRegisterBatch_EmptyReturnsNil(t *testing.T) {
	client := NewClient("http://unused", "", 0)
	err := client.RegisterBatch(context.Background(), nil, 5)
	if err != nil {
		t.Fatalf("expected no error for empty batch, got: %v", err)
	}
}

func TestRegisterBatchWithResults_AllSucceed(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	regs := make([]Registration, 10)
	for i := range regs {
		regs[i] = Registration{TxID: "tx" + string(rune('0'+i)), CallbackURL: "http://cb"}
	}

	errs := client.RegisterBatchWithResults(context.Background(), regs, 5)
	if len(errs) != len(regs) {
		t.Fatalf("expected len(errs)=%d, got %d", len(regs), len(errs))
	}
	for i, e := range errs {
		if e != nil {
			t.Errorf("errs[%d]=%v want nil", i, e)
		}
	}
	if count.Load() != 10 {
		t.Errorf("expected 10 requests, got %d", count.Load())
	}
}

// TestRegisterBatchWithResults_PartialFailure pins the key contract difference
// versus RegisterBatch: this variant does NOT fail-fast — successes survive a
// sibling's failure so the propagator can partition the batch and broadcast
// the successes while routing failures to PENDING_RETRY.
func TestRegisterBatchWithResults_PartialFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "fail") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	regs := []Registration{
		{TxID: "ok-1", CallbackURL: "http://cb"},
		{TxID: "fail-1", CallbackURL: "http://cb"},
		{TxID: "ok-2", CallbackURL: "http://cb"},
		{TxID: "fail-2", CallbackURL: "http://cb"},
		{TxID: "ok-3", CallbackURL: "http://cb"},
	}

	errs := client.RegisterBatchWithResults(context.Background(), regs, 3)
	if len(errs) != len(regs) {
		t.Fatalf("expected len(errs)=%d, got %d", len(regs), len(errs))
	}
	wantNil := []int{0, 2, 4}
	wantErr := []int{1, 3}
	for _, i := range wantNil {
		if errs[i] != nil {
			t.Errorf("errs[%d]=%v want nil (tx %s)", i, errs[i], regs[i].TxID)
		}
	}
	for _, i := range wantErr {
		if errs[i] == nil {
			t.Errorf("errs[%d]=nil want non-nil (tx %s)", i, regs[i].TxID)
		}
	}
}

func TestRegisterBatchWithResults_AllFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	regs := make([]Registration, 4)
	for i := range regs {
		regs[i] = Registration{TxID: "tx", CallbackURL: "http://cb"}
	}

	errs := client.RegisterBatchWithResults(context.Background(), regs, 2)
	if len(errs) != len(regs) {
		t.Fatalf("expected len(errs)=%d, got %d", len(regs), len(errs))
	}
	for i, e := range errs {
		if e == nil {
			t.Errorf("errs[%d]=nil want non-nil", i)
		}
	}
}

// TestRegisterBatchWithResults_ContextCanceled verifies that a cancellation
// mid-batch still produces a per-index error slice the caller can partition —
// the propagator relies on len(errs)==len(regs) to align failures with
// pendingMsgs entries when routing to PENDING_RETRY.
func TestRegisterBatchWithResults_ContextCanceled(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(release)

	client := NewClient(server.URL, "", 0)
	regs := make([]Registration, 8)
	for i := range regs {
		regs[i] = Registration{TxID: "tx", CallbackURL: "http://cb"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	errs := client.RegisterBatchWithResults(ctx, regs, 2)
	if len(errs) != len(regs) {
		t.Fatalf("expected len(errs)=%d, got %d", len(regs), len(errs))
	}
	var sawErr bool
	for _, e := range errs {
		if e != nil {
			sawErr = true
			break
		}
	}
	if !sawErr {
		t.Errorf("expected at least one non-nil error after context cancel, got all nil")
	}
}

func TestRegisterBatchWithResults_EmptyReturnsNil(t *testing.T) {
	client := NewClient("http://unused", "", 0)
	errs := client.RegisterBatchWithResults(context.Background(), nil, 5)
	if errs != nil {
		t.Errorf("expected nil for empty batch, got %v", errs)
	}
}

// TestRegisterBatchWithResults_ConcurrencyBounded covers the semaphore code
// path at client.go:236-251, which is structurally different from
// RegisterBatch's errgroup.SetLimit path and warrants its own assertion.
func TestRegisterBatchWithResults_ConcurrencyBounded(t *testing.T) {
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		concurrent.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	regs := make([]Registration, 30)
	for i := range regs {
		regs[i] = Registration{TxID: "tx", CallbackURL: "http://cb"}
	}

	errs := client.RegisterBatchWithResults(context.Background(), regs, 3)
	for i, e := range errs {
		if e != nil {
			t.Errorf("errs[%d]=%v want nil", i, e)
		}
	}
	if maxConcurrent.Load() > 3 {
		t.Errorf("expected max concurrency <= 3, got %d", maxConcurrent.Load())
	}
}

func TestReprocess_AcceptedReturnsNil(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewClient(server.URL, "mytoken", 0)
	err := client.Reprocess(context.Background(), "blockhash123", "http://callback/url", "cbtoken")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPath != "/reprocess" {
		t.Errorf("expected /reprocess, got %s", gotPath)
	}
	if gotAuth != "Bearer mytoken" {
		t.Errorf("expected Bearer mytoken, got %s", gotAuth)
	}
	if gotBody["blockHash"] != "blockhash123" {
		t.Errorf("blockHash=%q want blockhash123", gotBody["blockHash"])
	}
	if gotBody["callbackUrl"] != "http://callback/url" {
		t.Errorf("callbackUrl=%q", gotBody["callbackUrl"])
	}
	if gotBody["callbackToken"] != "cbtoken" {
		t.Errorf("callbackToken=%q", gotBody["callbackToken"])
	}
}

func TestReprocess_OmitsEmptyCallbackToken(t *testing.T) {
	var rawBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	if err := client.Reprocess(context.Background(), "blockhash123", "http://cb", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(string(rawBody), "callbackToken") {
		t.Errorf("expected body to omit callbackToken when empty, got %s", string(rawBody))
	}
}

func TestReprocess_4xxReturnsTypedFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("block not found"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	err := client.Reprocess(context.Background(), "blockhash123", "http://cb", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var fail *ReprocessError
	if !errors.As(err, &fail) {
		t.Fatalf("expected *ReprocessError, got %T: %v", err, err)
	}
	if fail.StatusCode != http.StatusNotFound {
		t.Errorf("StatusCode=%d want 404", fail.StatusCode)
	}
	if !strings.Contains(fail.Body, "block not found") {
		t.Errorf("Body=%q missing server message", fail.Body)
	}
}

func TestReprocess_5xxReturnsTypedFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 0)
	err := client.Reprocess(context.Background(), "blockhash123", "http://cb", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var fail *ReprocessError
	if !errors.As(err, &fail) {
		t.Fatalf("expected *ReprocessError, got %T: %v", err, err)
	}
	if fail.StatusCode != http.StatusBadGateway {
		t.Errorf("StatusCode=%d want 502", fail.StatusCode)
	}
}

func TestReprocess_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Reprocess(ctx, "blockhash", "http://cb", "")
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}
