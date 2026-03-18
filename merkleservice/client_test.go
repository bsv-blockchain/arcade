package merkleservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRegister_Success(t *testing.T) {
	var receivedBody watchRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/watch" {
			t.Errorf("expected path /watch, got %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected content-type application/json, got %s", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("expected auth header, got %s", r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedBody); err != nil {
			t.Fatalf("failed to decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "test-token", 5*time.Second)
	err := client.Register(context.Background(), "abc123", "http://localhost/callback")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if receivedBody.TxID != "abc123" {
		t.Errorf("expected txid abc123, got %s", receivedBody.TxID)
	}
	if receivedBody.CallbackURL != "http://localhost/callback" {
		t.Errorf("expected callback URL, got %s", receivedBody.CallbackURL)
	}
}

func TestRegister_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 5*time.Second)
	err := client.Register(context.Background(), "abc123", "http://localhost/callback")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestRegister_NetworkError(t *testing.T) {
	client := NewClient("http://localhost:1", "", 1*time.Second)
	err := client.Register(context.Background(), "abc123", "http://localhost/callback")
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}

func TestRegister_NoAuthToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("expected no auth header, got %s", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "", 5*time.Second)
	err := client.Register(context.Background(), "abc123", "http://localhost/callback")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
