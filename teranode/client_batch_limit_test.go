package teranode

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// teranodeBatchLimitBodies are Teranode's verbatim early-exit messages from
// its /txs handler (services/propagation/Server.go handleMultipleTx: the
// maxTransactionsPerRequest, maxDataPerRequest and submission-slot checks).
// They are pinned here so a Teranode release that rewords one fails this
// test instead of silently degrading arcade to the blind requeue path.
var teranodeBatchLimitBodies = []string{
	"Invalid request body: too many transactions",
	"Invalid request body: too much data",
	"Invalid request body: too many submissions",
}

// submitAgainst POSTs a two-tx batch to a fake Teranode that always answers
// with the given status and body.
func submitAgainst(t *testing.T, status int, body string) (int, *TxsFailures, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client := NewClient([]string{server.URL}, "", HealthConfig{})
	return client.SubmitTransactions(context.Background(), server.URL, [][]byte{{0x01}, {0x02}})
}

// TestSubmitTransactions_413_IsBatchTooLarge — an Echo BodyLimit (or any
// proxy) answering 413 refused the request by SHAPE, before reading it. The
// caller must be able to tell that apart from a gateway 502 so it narrows
// rather than requeues blind. The plain status sentinel is preserved so
// existing log text and errors.Is checks are unchanged.
func TestSubmitTransactions_413_IsBatchTooLarge(t *testing.T) {
	code, failures, err := submitAgainst(t, http.StatusRequestEntityTooLarge, `{"message":"Request Entity Too Large"}`)
	if code != http.StatusRequestEntityTooLarge {
		t.Errorf("code = %d, want 413", code)
	}
	if failures != nil {
		t.Errorf("failures = %#v, want nil (a 413 carries no per-tx verdict)", failures)
	}
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Errorf("err = %v, want errors.Is(ErrBatchTooLarge)", err)
	}
	if !errors.Is(err, errUnexpectedStatusCode) {
		t.Errorf("err = %v, want errors.Is(errUnexpectedStatusCode) preserved", err)
	}
}

// TestSubmitTransactions_400BatchLimitBodies_AreBatchTooLarge — Teranode's
// count/byte early exits answer 400 with one of three bare text bodies (no
// "Failed to process transactions:" header), with or without a trailing
// newline depending on the Echo version.
func TestSubmitTransactions_400BatchLimitBodies_AreBatchTooLarge(t *testing.T) {
	for _, body := range teranodeBatchLimitBodies {
		for _, suffix := range []string{"", "\n"} {
			t.Run(body+suffix, func(t *testing.T) {
				code, failures, err := submitAgainst(t, http.StatusBadRequest, body+suffix)
				if code != http.StatusBadRequest {
					t.Errorf("code = %d, want 400", code)
				}
				if failures != nil {
					t.Errorf("failures = %#v, want nil", failures)
				}
				if !errors.Is(err, ErrBatchTooLarge) {
					t.Errorf("err = %v, want errors.Is(ErrBatchTooLarge)", err)
				}
				if !errors.Is(err, errUnexpectedStatusCode) {
					t.Errorf("err = %v, want errors.Is(errUnexpectedStatusCode) preserved", err)
				}
			})
		}
	}
}

// TestSubmitTransactions_400OtherBodies_AreNotBatchTooLarge — the match is
// exact, not a prefix: Teranode also answers a bare 400 "request context
// cancelled" from the same handler, and a false positive costs up to 2n−1
// narrowing round trips on a batch that will never fit.
func TestSubmitTransactions_400OtherBodies_AreNotBatchTooLarge(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantParsed  bool
		wantSpecial bool
	}{
		{name: "request context cancelled", body: "request context cancelled"},
		{name: "generic bad request", body: "Bad Request"},
		{name: "unknown invalid-body variant", body: "Invalid request body: something new"},
		{name: "prefix of a limit body is not a limit body", body: "Invalid request body: too much data please retry"},
		{
			name:       "parseable failure list",
			body:       "Failed to process transactions:\nTX_INVALID (31): [ProcessTransaction][aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa] bad\n",
			wantParsed: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, failures, err := submitAgainst(t, http.StatusBadRequest, tc.body)
			if code != http.StatusBadRequest {
				t.Errorf("code = %d, want 400", code)
			}
			if tc.wantParsed && failures.Len() == 0 {
				t.Errorf("failures = %#v, want a parsed failure list", failures)
			}
			if !tc.wantParsed && failures != nil {
				t.Errorf("failures = %#v, want nil", failures)
			}
			if errors.Is(err, ErrBatchTooLarge) {
				t.Errorf("err = %v must NOT be ErrBatchTooLarge", err)
			}
			if !errors.Is(err, errUnexpectedStatusCode) {
				t.Errorf("err = %v, want errors.Is(errUnexpectedStatusCode)", err)
			}
		})
	}
}
