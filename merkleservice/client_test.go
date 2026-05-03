package merkleservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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
