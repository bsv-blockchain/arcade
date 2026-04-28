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

	code, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
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

func TestGetEndpoints(t *testing.T) {
	endpoints := []string{"http://a", "http://b", "http://c"}
	client := NewClient(endpoints, "", HealthConfig{})
	got := client.GetEndpoints()
	if len(got) != 3 {
		t.Errorf("expected 3 endpoints, got %d", len(got))
	}
}
