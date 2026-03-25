package teranode

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSubmitTransaction(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true

		if r.Method != http.MethodPost {
			t.Errorf("Expected POST method, got %s", r.Method)
		}

		if r.URL.Path != "/tx" {
			t.Errorf("Expected path /tx, got %s", r.URL.Path)
		}

		if r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("Expected Content-Type application/octet-stream, got %s", r.Header.Get("Content-Type"))
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read request body: %v", err)
		}

		expectedTx := []byte("test_transaction_bytes")
		if string(body) != string(expectedTx) {
			t.Errorf("Expected body %s, got %s", expectedTx, body)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "")

	statusCode, err := client.SubmitTransaction(t.Context(), server.URL, []byte("test_transaction_bytes"))
	if err != nil {
		t.Fatalf("SubmitTransaction failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, statusCode)
	}

	if !called {
		t.Error("Server handler was not called")
	}
}

func TestSubmitTransaction_Accepted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("Queued"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "")

	statusCode, err := client.SubmitTransaction(t.Context(), server.URL, []byte("test_tx"))
	if err != nil {
		t.Fatalf("SubmitTransaction failed: %v", err)
	}

	if statusCode != http.StatusAccepted {
		t.Errorf("Expected status code %d, got %d", http.StatusAccepted, statusCode)
	}
}

func TestSubmitTransaction_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Failed to process transaction"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "")

	_, err := client.SubmitTransaction(t.Context(), server.URL, []byte("test_tx"))
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if err.Error() == "" {
		t.Error("Expected error message to contain status information")
	}
}

func TestSubmitTransactions(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true

		if r.Method != http.MethodPost {
			t.Errorf("Expected POST method, got %s", r.Method)
		}

		if r.URL.Path != "/txs" {
			t.Errorf("Expected path /txs, got %s", r.URL.Path)
		}

		if r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("Expected Content-Type application/octet-stream, got %s", r.Header.Get("Content-Type"))
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read request body: %v", err)
		}

		// Body should be concatenated raw tx bytes
		expected := "tx_one_bytestx_two_bytes"
		if string(body) != expected {
			t.Errorf("Expected body %q, got %q", expected, body)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "")

	statusCode, err := client.SubmitTransactions(t.Context(), server.URL, [][]byte{
		[]byte("tx_one_bytes"),
		[]byte("tx_two_bytes"),
	})
	if err != nil {
		t.Fatalf("SubmitTransactions failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, statusCode)
	}

	if !called {
		t.Error("Server handler was not called")
	}
}

func TestSubmitTransactions_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("batch processing failed"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "")

	_, err := client.SubmitTransactions(t.Context(), server.URL, [][]byte{
		[]byte("tx_bytes"),
	})
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestGetEndpoints(t *testing.T) {
	endpoints := []string{"http://node1:8080", "http://node2:8080"}
	client := NewClient(endpoints, "")

	result := client.GetEndpoints()
	if len(result) != len(endpoints) {
		t.Errorf("Expected %d endpoints, got %d", len(endpoints), len(result))
	}

	for i, ep := range endpoints {
		if result[i] != ep {
			t.Errorf("Endpoint %d: expected %s, got %s", i, ep, result[i])
		}
	}
}

func TestContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "")
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err := client.SubmitTransaction(ctx, server.URL, []byte("test"))
	if err == nil {
		t.Fatal("Expected context deadline error, got nil")
	}
}
