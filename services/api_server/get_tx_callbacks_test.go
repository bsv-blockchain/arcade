package api_server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

// TestGetTransaction_CallbackStateTokenGated pins the delivery self-diagnosis
// surface (issue #249): GET /tx/{txid}?callbackToken=… reveals the webhook
// delivery state of the submissions registered under THAT token only. Without
// the token — or with someone else's — the response is byte-identical to the
// plain status read, and the CallbackToken itself never appears in any body.
func TestGetTransaction_CallbackStateTokenGated(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const (
		txid     = "aa11000000000000000000000000000000000000000000000000000000000000"
		tokMine  = "tok-mine"
		tokOther = "tok-other"
	)
	attemptAt := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	retryAt := attemptAt.Add(90 * time.Second)

	ms := &mockStore{
		subsByTxID: map[string][]*models.Submission{
			txid: {
				{
					SubmissionID:        "sub-mine",
					TxID:                txid,
					CallbackURL:         "https://receiver.example/cb",
					CallbackToken:       tokMine,
					LastDeliveredStatus: models.StatusSeenOnNetwork,
					RetryCount:          3,
					NextRetryAt:         &retryAt,
					Attempts:            14,
					LastAttemptAt:       &attemptAt,
					LastResult:          "status 403",
				},
				{
					SubmissionID:  "sub-other",
					TxID:          txid,
					CallbackURL:   "https://someone-else.example/cb",
					CallbackToken: tokOther,
				},
			},
		},
		getOrInsertFn: nil,
	}
	ms.getStatusFn = func(q string) *models.TransactionStatus {
		if q != txid {
			return nil
		}
		return &models.TransactionStatus{TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: attemptAt}
	}

	srv := &Server{
		cfg:    &config.Config{},
		logger: zap.NewNop(),
		store:  ms,
	}
	router := gin.New()
	srv.registerRoutes(router)

	get := func(url string) (int, string) {
		t.Helper()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w.Code, w.Body.String()
	}

	// With the matching token: base fields intact + this token's callback state.
	code, body := get("/tx/" + txid + "?callbackToken=" + tokMine)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", code, body)
	}
	var resp struct {
		TxID      string `json:"txid"`
		TxStatus  string `json:"txStatus"`
		Callbacks []struct {
			CallbackURL         string     `json:"callbackUrl"`
			LastDeliveredStatus string     `json:"lastDeliveredStatus"`
			Attempts            int        `json:"attempts"`
			LastAttemptAt       *time.Time `json:"lastAttemptAt"`
			LastResult          string     `json:"lastResult"`
			NextRetryAt         *time.Time `json:"nextRetryAt"`
		} `json:"callbacks"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if resp.TxID != txid || resp.TxStatus != string(models.StatusSeenOnNetwork) {
		t.Errorf("base status fields wrong: %s", body)
	}
	if len(resp.Callbacks) != 1 {
		t.Fatalf("expected exactly 1 callback (token-scoped), got %d: %s", len(resp.Callbacks), body)
	}
	cb := resp.Callbacks[0]
	if cb.CallbackURL != "https://receiver.example/cb" ||
		cb.LastDeliveredStatus != string(models.StatusSeenOnNetwork) ||
		cb.Attempts != 14 || cb.LastResult != "status 403" ||
		cb.LastAttemptAt == nil || !cb.LastAttemptAt.Equal(attemptAt) ||
		cb.NextRetryAt == nil || !cb.NextRetryAt.Equal(retryAt) {
		t.Errorf("callback state wrong: %+v", cb)
	}
	// The other subscriber's URL must not leak, and no token ever appears.
	for _, secret := range []string{"someone-else.example", tokMine, tokOther} {
		if strings.Contains(body, secret) {
			t.Errorf("response leaks %q: %s", secret, body)
		}
	}

	// Without a token, and with a token that matches no submission: the
	// response has no callbacks key at all (byte-identical to the plain read).
	for _, url := range []string{"/tx/" + txid, "/tx/" + txid + "?callbackToken=stranger"} {
		code, body := get(url)
		if code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", url, code)
		}
		if strings.Contains(body, "callbacks") || strings.Contains(body, "callbackUrl") {
			t.Errorf("%s: unexpected callback state in body: %s", url, body)
		}
	}
}
