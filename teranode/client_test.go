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

// TestSubmitTransactions_FailureList_500 exercises Teranode upstream main
// post-#879: HTTP 500 with body "Failed to process transactions:\n<line>\n…"
// where each non-header line is "<NAME> (<num>): [ProcessTransaction][<txid>] ...".
// The client must extract the per-txid failure map and return the status
// code along with an error so the caller's err != nil branch routes through
// the per-txid classification path.
func TestSubmitTransactions_FailureList_500(t *testing.T) {
	const (
		txidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		txidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	body := "Failed to process transactions:\n" +
		"TX_INVALID (31): [ProcessTransaction][" + txidA + "] tx is invalid because UTXO_SPENT\n" +
		"UTXO_SPENT (28): [ProcessTransaction][" + txidB + "] utxo already spent\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01}, {0x02}, {0x03}}

	code, failures, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err == nil {
		t.Fatalf("expected non-nil error for 500 (caller routes off err != nil)")
	}
	if code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", code)
	}
	if len(failures) != 2 {
		t.Fatalf("expected 2 failure entries, got %d (%#v)", len(failures), failures)
	}
	if _, ok := failures[txidA]; !ok {
		t.Errorf("expected failure for txidA, got %#v", failures)
	}
	if _, ok := failures[txidB]; !ok {
		t.Errorf("expected failure for txidB, got %#v", failures)
	}
}

// TestSubmitTransactions_500_NoFailureList asserts that a 500 without the
// "Failed to process transactions:" header (e.g. echo recover middleware
// panic body, gateway proxy 500) returns nil failures so the caller treats
// the batch as a pure infra failure.
func TestSubmitTransactions_500_NoFailureList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error\n"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01}, {0x02}}

	code, failures, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err == nil {
		t.Fatalf("expected non-nil error for 500")
	}
	if code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", code)
	}
	if failures != nil {
		t.Errorf("expected nil failures for non-Teranode 500 body, got %#v", failures)
	}
}

// TestSubmitTransactions_FailureList_HeaderOnly asserts that a 500 carrying
// only the header line (no failure entries) returns nil failures — there's
// no way to tell which txs failed, so the caller must requeue the batch.
func TestSubmitTransactions_FailureList_HeaderOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Failed to process transactions:\n"))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01}, {0x02}}

	_, failures, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if failures != nil {
		t.Errorf("expected nil failures for header-only body, got %#v", failures)
	}
}

// TestSubmitTransactions_FailureList_GarbledLineWholeBatchRequeue locks in
// the fail-closed contract on parseTxsFailures: any non-empty failure line
// without an extractable txid means the response isn't fully trustworthy
// (Teranode processOne panic recovery is the known offender — it doesn't
// include the tx's id in the recover wrapper, even though the tx is in
// scope). Returning nil here drops the caller to the whole-batch requeue
// path so every tx is re-broadcast rather than risk silently marking the
// orphan's owner as ACCEPTED.
func TestSubmitTransactions_FailureList_GarbledLineWholeBatchRequeue(t *testing.T) {
	const txidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	body := "Failed to process transactions:\n" +
		"TX_INVALID (31): [ProcessTransaction][" + txidA + "] tx is invalid because UTXO_SPENT\n" +
		"GARBLED: no txid in this line at all\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01}, {0x02}}

	_, failures, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if failures != nil {
		t.Errorf("fail-closed contract: any orphan line → whole-batch requeue; expected nil failures, got %#v", failures)
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
