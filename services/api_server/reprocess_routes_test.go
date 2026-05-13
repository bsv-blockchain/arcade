package api_server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/merkleservice"
)

// reprocessHarness wires a Server with a merkle-service stub the test owns,
// so each test can drive its own status/body response. The returned router
// has only the reprocess route registered — enough to exercise the handler
// without pulling in the rest of the surface.
type reprocessHarness struct {
	router *gin.Engine
	stub   *httptest.Server
	// requests is populated on every hit to the stub so tests can verify
	// what arcade actually forwarded.
	requests []recordedReprocess
}

type recordedReprocess struct {
	Path string
	Auth string
	Body map[string]string
}

func newReprocessHarness(t *testing.T, status int, respBody string) *reprocessHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)

	h := &reprocessHarness{}
	h.stub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var parsed map[string]string
		_ = json.Unmarshal(body, &parsed)
		h.requests = append(h.requests, recordedReprocess{
			Path: r.URL.Path,
			Auth: r.Header.Get("Authorization"),
			Body: parsed,
		})
		w.WriteHeader(status)
		if respBody != "" {
			_, _ = w.Write([]byte(respBody))
		}
	}))
	t.Cleanup(h.stub.Close)

	srv := &Server{
		cfg: &config.Config{
			CallbackURL:   "http://arcade/api/v1/merkle-service/callback",
			CallbackToken: "cbtoken",
		},
		logger:       zap.NewNop(),
		merkleClient: merkleservice.NewClient(h.stub.URL, "msauth", 0),
	}
	h.router = gin.New()
	srv.registerRoutes(h.router)
	return h
}

func TestReprocessBlock_AcceptedForwardsCallbackConfig(t *testing.T) {
	h := newReprocessHarness(t, http.StatusAccepted, "")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/blocks/000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da/reprocess", nil)
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s want 202", w.Code, w.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp["status"] != "accepted" || resp["blockHash"] != "000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da" {
		t.Errorf("resp=%v want status=accepted blockHash=000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da", resp)
	}

	if len(h.requests) != 1 {
		t.Fatalf("expected 1 upstream call, got %d", len(h.requests))
	}
	got := h.requests[0]
	if got.Path != "/reprocess" {
		t.Errorf("upstream path=%q want /reprocess", got.Path)
	}
	if got.Auth != "Bearer msauth" {
		t.Errorf("upstream auth=%q want Bearer msauth", got.Auth)
	}
	if got.Body["blockHash"] != "000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da" {
		t.Errorf("upstream blockHash=%q want 000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da", got.Body["blockHash"])
	}
	if got.Body["callbackUrl"] != "http://arcade/api/v1/merkle-service/callback" {
		t.Errorf("upstream callbackUrl=%q", got.Body["callbackUrl"])
	}
	if got.Body["callbackToken"] != "cbtoken" {
		t.Errorf("upstream callbackToken=%q want cbtoken", got.Body["callbackToken"])
	}
}

func TestReprocessBlock_UpstreamNotFoundReturns422(t *testing.T) {
	h := newReprocessHarness(t, http.StatusNotFound, "block not on consensus chain")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/blocks/000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da/reprocess", nil)
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s want 422", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "block not on consensus chain") {
		t.Errorf("body missing upstream message: %s", w.Body.String())
	}
}

func TestReprocessBlock_Upstream5xxReturns502(t *testing.T) {
	h := newReprocessHarness(t, http.StatusInternalServerError, "boom")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/blocks/000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da/reprocess", nil)
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s want 502", w.Code, w.Body.String())
	}
}

func TestReprocessBlock_InvalidBlockHashReturns400(t *testing.T) {
	h := newReprocessHarness(t, http.StatusAccepted, "")

	// Too short, contains non-hex, exactly 63, exactly 65 — all should 400.
	cases := []string{
		"deadbeef",
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		"000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530d",  // 63
		"000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530daa", // 65
	}
	for _, hash := range cases {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/blocks/"+hash+"/reprocess", nil)
		w := httptest.NewRecorder()
		h.router.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("hash=%q got status=%d want 400", hash, w.Code)
		}
	}

	// And the stub must not have been called for any of them — validation
	// is supposed to short-circuit before we hit merkle-service.
	if len(h.requests) != 0 {
		t.Fatalf("upstream was called %d times for invalid hashes, want 0", len(h.requests))
	}
}

func TestReprocessBlock_MerkleClientUnconfiguredReturns503(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:    &config.Config{CallbackURL: "http://cb", CallbackToken: "t"},
		logger: zap.NewNop(),
		// merkleClient intentionally nil — mirrors a deployment without
		// merkle_service.url set.
	}
	router := gin.New()
	srv.registerRoutes(router)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/blocks/000000004ec7cc6848c07caa63dcdd0dbcd3c354eee8e99e998149ad13b530da/reprocess", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s want 503", w.Code, w.Body.String())
	}
}
