package api_server

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
)

// setupServerWithCallbackCfg builds a test Server whose Callback config
// is the caller's. Mirrors setupServerWithStore but lets the SSRF tests
// flip allow_private_ips on and off.
func setupServerWithCallbackCfg(broker *kafka.RecordingBroker, ms *mockStore, cb config.CallbackConfig) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	producer := kafka.NewProducer(broker)
	srv := &Server{
		cfg:      &config.Config{Callback: cb},
		logger:   zap.NewNop(),
		producer: producer,
		store:    ms,
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

// blockedURLs is the canonical set of X-CallbackUrl values the SSRF guard
// must reject when callback.allow_private_ips is false. Mirrors the threat
// model section of issue #75: loopback, link-local, RFC1918, cloud
// metadata, and unspecified addresses.
var blockedURLs = []struct {
	name string
	url  string
}{
	{"loopback v4", "http://127.0.0.1/cb"},
	{"loopback v6", "http://[::1]/cb"},
	{"unspecified v4", "http://0.0.0.0/cb"},
	{"unspecified v6", "http://[::]/cb"},
	{"metadata 169.254.169.254", "http://169.254.169.254/latest/meta-data/"},
	{"link-local v4", "http://169.254.1.1/cb"},
	{"link-local v6", "http://[fe80::1]/cb"},
	{"rfc1918 10/8", "http://10.0.0.1/cb"},
	{"rfc1918 172.16/12", "http://172.16.5.5/cb"},
	{"rfc1918 192.168/16", "http://192.168.1.1/cb"},
	{"non-http scheme", "ftp://example.com/cb"},
	{"file scheme", "file:///etc/passwd"},
}

// TestSubmitTransaction_RejectsBlockedCallbackURLs verifies the
// registration-time SSRF guard on POST /tx: every blocked URL class must
// be rejected with 400, no Kafka publish, and no submission record.
func TestSubmitTransaction_RejectsBlockedCallbackURLs(t *testing.T) {
	tx := makeRealTx(t)
	rawHex := hex.EncodeToString(tx.Bytes())
	body, err := json.Marshal(map[string]string{"rawTx": rawHex})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	for _, c := range blockedURLs {
		t.Run(c.name, func(t *testing.T) {
			broker := &kafka.RecordingBroker{}
			ms := &mockStore{}
			_, router := setupServerWithCallbackCfg(broker, ms, config.CallbackConfig{AllowPrivateIPs: false})

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-CallbackUrl", c.url)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
			}
			if !strings.Contains(strings.ToLower(w.Body.String()), "callback") {
				t.Errorf("expected error mentioning callback, got: %s", w.Body.String())
			}
			if len(broker.Sends) != 0 {
				t.Errorf("expected no Kafka send for blocked url, got %d", len(broker.Sends))
			}
			if len(ms.insertedSubmissions) != 0 {
				t.Errorf("expected no submission insert for blocked url, got %d", len(ms.insertedSubmissions))
			}
		})
	}
}

// TestSubmitTransaction_AcceptsPublicCallbackURL is the positive control:
// a public https URL flows through to a Kafka send and a submission insert.
func TestSubmitTransaction_AcceptsPublicCallbackURL(t *testing.T) {
	tx := makeRealTx(t)
	rawHex := hex.EncodeToString(tx.Bytes())
	body, err := json.Marshal(map[string]string{"rawTx": rawHex})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	_, router := setupServerWithCallbackCfg(broker, ms, config.CallbackConfig{AllowPrivateIPs: false})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CallbackUrl", "https://example.com/foo")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	if len(broker.Sends) != 1 {
		t.Errorf("expected 1 Kafka send, got %d", len(broker.Sends))
	}
	if len(ms.insertedSubmissions) != 1 {
		t.Errorf("expected 1 submission insert, got %d", len(ms.insertedSubmissions))
	}
}

// TestSubmitTransaction_AllowPrivateIPs_OptIn confirms the operator
// escape hatch: with callback.allow_private_ips=true, a previously
// rejected URL flows through to Kafka and the submission store.
func TestSubmitTransaction_AllowPrivateIPs_OptIn(t *testing.T) {
	tx := makeRealTx(t)
	rawHex := hex.EncodeToString(tx.Bytes())
	body, err := json.Marshal(map[string]string{"rawTx": rawHex})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	_, router := setupServerWithCallbackCfg(broker, ms, config.CallbackConfig{AllowPrivateIPs: true})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CallbackUrl", "http://127.0.0.1:9000/cb")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	if len(broker.Sends) != 1 {
		t.Errorf("expected 1 Kafka send (opt-in allows private), got %d", len(broker.Sends))
	}
	if len(ms.insertedSubmissions) != 1 {
		t.Errorf("expected 1 submission insert (opt-in allows private), got %d", len(ms.insertedSubmissions))
	}
}

// TestSubmitTransactions_RejectsBlockedCallbackURLs covers the batch endpoint.
// Same predicate, same rejection — verified once for each blocked class to
// guard against accidental drift between /tx and /txs.
func TestSubmitTransactions_RejectsBlockedCallbackURLs(t *testing.T) {
	tx := makeRealTx(t)
	body := tx.Bytes()

	for _, c := range blockedURLs {
		t.Run(c.name, func(t *testing.T) {
			broker := &kafka.RecordingBroker{}
			ms := &mockStore{}
			_, router := setupServerWithCallbackCfg(broker, ms, config.CallbackConfig{AllowPrivateIPs: false})

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/txs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/octet-stream")
			req.Header.Set("X-CallbackUrl", c.url)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", w.Code, w.Body.String())
			}
			if len(broker.Batches) != 0 || len(broker.Sends) != 0 {
				t.Errorf("expected no Kafka activity for blocked url")
			}
			if len(ms.insertedSubmissions) != 0 {
				t.Errorf("expected no submission insert for blocked url, got %d", len(ms.insertedSubmissions))
			}
		})
	}
}

// TestSubmitTransaction_TokenOnlySubscriptionStillAllowed makes sure the
// SSRF guard doesn't penalize SSE-only subscribers who provide just a
// callback token (no URL). They never trigger an outbound dial, so
// there's no SSRF surface to defend.
func TestSubmitTransaction_TokenOnlySubscriptionStillAllowed(t *testing.T) {
	tx := makeRealTx(t)
	rawHex := hex.EncodeToString(tx.Bytes())
	body, err := json.Marshal(map[string]string{"rawTx": rawHex})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	broker := &kafka.RecordingBroker{}
	ms := &mockStore{}
	_, router := setupServerWithCallbackCfg(broker, ms, config.CallbackConfig{AllowPrivateIPs: false})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CallbackToken", "tok-1") // no X-CallbackUrl
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", w.Code, w.Body.String())
	}
	if len(ms.insertedSubmissions) != 1 {
		t.Errorf("expected 1 submission insert (token-only), got %d", len(ms.insertedSubmissions))
	}
}
