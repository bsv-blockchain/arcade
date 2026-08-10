package teranode

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	testSubmitTransactionsFailureList(t, http.StatusInternalServerError)
}

// TestSubmitTransactions_FailureList_422 pins the production bug where
// Teranode (or a gateway in front of it) returned HTTP 422 with a fully
// parseable "Failed to process transactions:" body containing
// PROCESSING / UTXO_SPENT lines. The old client only parsed the body on
// HTTP 500, so the failure map was nil, the peer contributed no rejection
// vote, and GET /tx ExtraInfo never saw the Teranode reason (only opaque
// infra noise from other peers, if anything). Parsing is status-agnostic.
func TestSubmitTransactions_FailureList_422(t *testing.T) {
	testSubmitTransactionsFailureList(t, http.StatusUnprocessableEntity)
}

// TestSubmitTransactions_FailureList_400 covers Teranode's policy-family
// aggregate status (TX_INVALID / TX_POLICY / TX_LOCK_TIME / … →
// httpStatusForTxError → 400).
func TestSubmitTransactions_FailureList_400(t *testing.T) {
	testSubmitTransactionsFailureList(t, http.StatusBadRequest)
}

// TestSubmitTransactions_FailureList_409 covers Teranode's conflict-family
// aggregate status: since v0.16 the upstream /txs handler returns 409 for
// UTXO_SPENT / TX_CONFLICTING / TX_INVALID_DOUBLE_SPEND / TX_LOCKED
// (httpStatusForTxError, services/propagation/Server.go) — the exact status
// observed on mainnet carrying the UTXO_SPENT verdict that pre-fix Arcade
// dropped.
func TestSubmitTransactions_FailureList_409(t *testing.T) {
	testSubmitTransactionsFailureList(t, http.StatusConflict)
}

// TestParseTxsFailures_WrapperPreferredOverBareHex pins the txid-extraction
// order: the [ProcessTransaction][<txid>] wrapper always wins over the first
// bare 64-hex string in the line. Conflict-family messages embed other
// transactions' hashes (spent outpoint, competing spender) that can precede
// any wrapper — keying by first-hex would misattribute the failure to a
// different tx (possibly another tx in the same batch, e.g. a same-batch
// parent whose output the failed tx tried to spend).
func TestParseTxsFailures_WrapperPreferredOverBareHex(t *testing.T) {
	const (
		submitted = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		outpoint  = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		spender   = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	body := []byte("Failed to process transactions:\n" +
		// Bare hex (the outpoint) appears BEFORE the wrapper — wrapper must win.
		"TX_INVALID (31): utxo " + outpoint + ":1 already spent [ProcessTransaction][" + submitted + "] tx invalid\n" +
		// Wrapper-less conflict line — falls back to first bare hex (the
		// outpoint), which callers must treat as an alien key (it names no
		// submitted tx); pinned here so the fallback shape is explicit.
		"UTXO_SPENT (70): UTXO_SPENT (70): " + outpoint + ":2 utxo already spent by tx " + spender + "[0]\n")

	failures := parseTxsFailures(body, nil)
	if len(failures) != 2 {
		t.Fatalf("failures=%#v want 2 entries", failures)
	}
	if _, ok := failures[submitted]; !ok {
		t.Errorf("wrapped line must key under the wrapper txid, got %#v", failures)
	}
	if _, ok := failures[outpoint]; !ok {
		t.Errorf("wrapper-less conflict line must fall back to first bare hex, got %#v", failures)
	}
}

// TestSubmitTransactions_TruncatedFailureList_NoPartialParse asserts that a
// failure-list body cut off mid-transfer (Content-Length promises more than
// arrives) is NOT parsed: a partial list that happens to truncate at a line
// boundary would silently grant implicit acceptance to every tx whose
// failure line was lost. The read error must route to the infra path
// (failures nil → whole-batch requeue).
func TestSubmitTransactions_TruncatedFailureList_NoPartialParse(t *testing.T) {
	const (
		txidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		txidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	full := "Failed to process transactions:\n" +
		"TX_INVALID (31): [ProcessTransaction][" + txidA + "] tx invalid\n" +
		"TX_INVALID (31): [ProcessTransaction][" + txidB + "] tx invalid\n"
	// Truncate exactly at the line boundary after txidA's line — the
	// dangerous shape: the prefix alone is a well-formed one-entry list.
	cut := strings.Index(full, txidB) - len("TX_INVALID (31): [ProcessTransaction][")
	partial := full[:cut]

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(full)))
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(partial))
		// Handler returns with fewer bytes than promised; the server tears
		// the connection down and the client's io.ReadAll sees an error.
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	code, failures, err := client.SubmitTransactions(context.Background(), server.URL, [][]byte{{0x01}, {0x02}})
	if err == nil {
		t.Fatal("expected non-nil error for truncated body")
	}
	if code != http.StatusUnprocessableEntity {
		t.Errorf("code=%d want 422", code)
	}
	if failures != nil {
		t.Errorf("failures=%#v want nil — a truncated failure list must not be parsed", failures)
	}
}

// TestSubmitTransactions_NonParseable4xx_NoFailureList asserts the
// status-agnostic parse does NOT turn arbitrary 4xx error pages (auth
// failures, proxy HTML, rate limiting, bare conflict text) into per-tx
// verdicts. failures must stay nil so the caller treats them as infra
// noise — otherwise a body-less 4xx would fabricate implicit ACCEPT votes
// for every tx in the batch.
func TestSubmitTransactions_NonParseable4xx_NoFailureList(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{name: "401 unauthorized", status: http.StatusUnauthorized, body: "Unauthorized\n"},
		{name: "404 html error page", status: http.StatusNotFound, body: "<html><body><h1>404 Not Found</h1></body></html>"},
		{name: "429 rate limited", status: http.StatusTooManyRequests, body: "rate limit exceeded"},
		{name: "409 without failure-list body", status: http.StatusConflict, body: "Conflict"},
		{name: "503 no available server", status: http.StatusServiceUnavailable, body: "no available server\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client := NewClient([]string{server.URL}, "", HealthConfig{})
			code, failures, err := client.SubmitTransactions(context.Background(), server.URL, [][]byte{{0x01}, {0x02}})
			if err == nil {
				t.Fatalf("expected non-nil error for %d", tc.status)
			}
			if code != tc.status {
				t.Errorf("code=%d want %d", code, tc.status)
			}
			if failures != nil {
				t.Errorf("failures=%#v want nil (opaque %d body must not become a per-tx verdict)", failures, tc.status)
			}
		})
	}
}

func testSubmitTransactionsFailureList(t *testing.T, status int) {
	t.Helper()
	const (
		txidA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		txidB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	body := "Failed to process transactions:\n" +
		"TX_INVALID (31): [ProcessTransaction][" + txidA + "] tx is invalid because UTXO_SPENT\n" +
		"UTXO_SPENT (28): [ProcessTransaction][" + txidB + "] utxo already spent\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	rawTxs := [][]byte{{0x01}, {0x02}, {0x03}}

	code, failures, err := client.SubmitTransactions(context.Background(), server.URL, rawTxs)
	if err == nil {
		t.Fatalf("expected non-nil error for %d (caller routes off err != nil)", status)
	}
	if code != status {
		t.Errorf("expected %d, got %d", status, code)
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

// TestSubmitTransactions_422_ProcessingOnly mirrors the exact live shape
// observed when spending an already-mined UTXO: HTTP 422 with a single
// PROCESSING catch-all line. Must still yield a one-entry failure map so
// propagation can terminalize REJECTED with that ExtraInfo.
func TestSubmitTransactions_422_ProcessingOnly(t *testing.T) {
	const txid = "d018bc98d7828b7f095e5d616f7ccba607fc228368e82e74b25304e37699f1e9"
	body := "Failed to process transactions:\n" +
		"PROCESSING (4): [ProcessTransaction][" + txid + "] failed to validate transaction\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	client := NewClient([]string{server.URL}, "", HealthConfig{})
	code, failures, err := client.SubmitTransactions(context.Background(), server.URL, [][]byte{{0x01}})
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if code != http.StatusUnprocessableEntity {
		t.Errorf("code=%d want 422", code)
	}
	if len(failures) != 1 {
		t.Fatalf("failures=%#v want 1 entry", failures)
	}
	line, ok := failures[txid]
	if !ok {
		t.Fatalf("missing failure for %s: %#v", txid, failures)
	}
	if !strings.Contains(line, "PROCESSING (4)") {
		t.Errorf("line=%q want PROCESSING (4)", line)
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
