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
		w.Write([]byte("OK"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.SubmitTransaction(ctx, []byte("test_transaction_bytes"))
	if err != nil {
		t.Fatalf("SubmitTransaction failed: %v", err)
	}

	if !called {
		t.Error("Server handler was not called")
	}
}

func TestSubmitTransaction_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to process transaction"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.SubmitTransaction(ctx, []byte("test_tx"))
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

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read request body: %v", err)
		}

		expected := "tx1tx2tx3"
		if string(body) != expected {
			t.Errorf("Expected concatenated body %s, got %s", expected, body)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	txs := [][]byte{
		[]byte("tx1"),
		[]byte("tx2"),
		[]byte("tx3"),
	}

	err := client.SubmitTransactions(ctx, txs)
	if err != nil {
		t.Fatalf("SubmitTransactions failed: %v", err)
	}

	if !called {
		t.Error("Server handler was not called")
	}
}

func TestFetchBlockData(t *testing.T) {
	blockHash := "00000000000000000001"
	expectedData := []byte("block_data_here")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}

		expectedPath := "/block/" + blockHash
		if r.URL.Path != expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		w.Write(expectedData)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	data, err := client.FetchBlockData(ctx, server.URL, blockHash)
	if err != nil {
		t.Fatalf("FetchBlockData failed: %v", err)
	}

	if string(data) != string(expectedData) {
		t.Errorf("Expected data %s, got %s", expectedData, data)
	}
}

func TestFetchBlockData_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	_, err := client.FetchBlockData(ctx, server.URL, "nonexistent")
	if err == nil {
		t.Fatal("Expected error for 404 response, got nil")
	}
}

func TestFetchSubtreeData(t *testing.T) {
	subtreeHash := "subtree123"
	expectedData := []byte("subtree_data_here")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}

		expectedPath := "/subtree/" + subtreeHash
		if r.URL.Path != expectedPath {
			t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		w.Write(expectedData)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	data, err := client.FetchSubtreeData(ctx, server.URL, subtreeHash)
	if err != nil {
		t.Fatalf("FetchSubtreeData failed: %v", err)
	}

	if string(data) != string(expectedData) {
		t.Errorf("Expected data %s, got %s", expectedData, data)
	}
}

func TestClientWithTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClientWithTimeout(server.URL, 50*time.Millisecond)
	ctx := context.Background()

	err := client.SubmitTransaction(ctx, []byte("test"))
	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}
}

func TestContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := client.SubmitTransaction(ctx, []byte("test"))
	if err == nil {
		t.Fatal("Expected context deadline error, got nil")
	}
}
