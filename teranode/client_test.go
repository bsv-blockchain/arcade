package teranode

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSubmitTransaction(t *testing.T) {
	var gotContentType string
	var gotBody []byte
	var gotAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		if r.URL.Path == "/tx" && r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "testtoken", HealthConfig{})
	rawTx := []byte{0x01, 0x02, 0x03}

	code, err := client.SubmitTransaction(context.Background(), server.URL, rawTx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != http.StatusOK {
		t.Errorf("expected 200, got %d", code)
	}
	if gotContentType != "application/octet-stream" {
		t.Errorf("expected application/octet-stream, got %s", gotContentType)
	}
	if gotAuth != "Bearer testtoken" {
		t.Errorf("expected Bearer testtoken, got %s", gotAuth)
	}
	if len(gotBody) != 3 {
		t.Errorf("expected 3 bytes, got %d", len(gotBody))
	}
}

func TestSubmitTransactions_Batch(t *testing.T) {
	var gotBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		if r.URL.Path == "/txs" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01, 0x02}, {0x03, 0x04, 0x05}}

	code, _, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != http.StatusOK {
		t.Errorf("expected 200, got %d", code)
	}
	// Batch should concatenate: 2 + 3 = 5 bytes
	if len(gotBody) != 5 {
		t.Errorf("expected 5 concatenated bytes, got %d", len(gotBody))
	}
}

// TestSubmitTransactions_PerSlot_207 exercises the post-#881 Teranode
// response: HTTP 207 Multi-Status with a body of newline-separated per-slot
// results. The client must parse the body into a per-slot slice and return
// the status code along with an error (so the caller's err != nil branch
// kicks in and routes through the per-slot classification path).
func TestSubmitTransactions_PerSlot_207(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMultiStatus)
		// Format matches Teranode #881: one line per submission slot,
		// "OK" or "<NAME> (<num>)".
		_, _ = w.Write([]byte("OK\nTX_INVALID (31)\nOK\n"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01}, {0x02}, {0x03}}

	code, slots, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err == nil {
		t.Fatalf("expected non-nil error for 207 (caller routes off err != nil)")
	}
	if code != http.StatusMultiStatus {
		t.Errorf("expected 207, got %d", code)
	}
	if len(slots) != 3 {
		t.Fatalf("expected 3 per-slot entries, got %d", len(slots))
	}
	if slots[0] != "OK" || slots[1] != "TX_INVALID (31)" || slots[2] != "OK" {
		t.Errorf("unexpected per-slot contents: %#v", slots)
	}
}

// TestSubmitTransactions_500_NoPerSlot asserts that a 500 (genuine server
// error, e.g. echo recover middleware) returns nil per-slot and an error
// so the caller treats the batch as a pure infra failure. Per-slot is
// exclusively a 207 Multi-Status payload.
func TestSubmitTransactions_500_NoPerSlot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error\n"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01}, {0x02}}

	code, slots, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err == nil {
		t.Fatalf("expected non-nil error for 500")
	}
	if code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", code)
	}
	if slots != nil {
		t.Errorf("expected nil per-slot for 500, got %#v", slots)
	}
}

func TestGetEndpoints(t *testing.T) {
	endpoints := []string{"http://a", "http://b", "http://c"}
	client := NewClient(endpoints, "", HealthConfig{})
	got := client.GetEndpoints()
	if len(got) != 3 {
		t.Errorf("expected 3 endpoints, got %d", len(got))
	}
}
