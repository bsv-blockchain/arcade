package api_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/version"
)

// healthResp mirrors the server's healthResponse shape but uses generic
// Go types so test code does not depend on unexported fields.
// Chaintracks moved out of api-server in the microservice decomposition,
// so the response no longer includes a chaintracks block. The healthy/version
// fields are the ARC health contract (issue #208).
type healthResp struct {
	Healthy     bool                      `json:"healthy"`
	Version     string                    `json:"version"`
	Status      string                    `json:"status"`
	DatahubURLs []teranode.EndpointStatus `json:"datahub_urls"`
}

// doHealth exercises the real router on a given Server so we cover the Gin
// route binding and the JSON shape clients will actually receive.
func doHealth(t *testing.T, srv *Server) (int, healthResp, []byte) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	srv.registerRoutes(r)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	body := w.Body.Bytes()
	var resp healthResp
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decoding health JSON: %v (body=%s)", err, string(body))
	}
	return w.Code, resp, body
}

func TestHandleHealth_StructuredResponse(t *testing.T) {
	tc := teranode.NewClient(
		[]string{"https://a.example", "https://b.example"},
		"",
		teranode.HealthConfig{FailureThreshold: 2},
	)
	tc.AddEndpoints([]string{"https://c.example"})
	tc.RecordFailure("https://b.example")
	tc.RecordFailure("https://b.example") // trip

	srv := &Server{
		cfg:      &config.Config{},
		logger:   zap.NewNop(),
		teranode: tc,
	}

	code, resp, body := doHealth(t, srv)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", code, string(body))
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %q", resp.Status)
	}
	// ARC contract: clients gate submission on healthy == true and read version.
	if !resp.Healthy {
		t.Errorf("expected healthy=true, got %v (body=%s)", resp.Healthy, string(body))
	}
	if resp.Version != version.Version {
		t.Errorf("expected version=%q, got %q", version.Version, resp.Version)
	}

	want := []teranode.EndpointStatus{
		{URL: "https://a.example", Source: "configured", Healthy: true},
		{URL: "https://b.example", Source: "configured", Healthy: false},
		{URL: "https://c.example", Source: "discovered", Healthy: true},
	}
	if len(resp.DatahubURLs) != len(want) {
		t.Fatalf("expected %d datahub urls, got %d (%+v)", len(want), len(resp.DatahubURLs), resp.DatahubURLs)
	}
	for i, w := range want {
		if resp.DatahubURLs[i] != w {
			t.Errorf("datahub_urls[%d] = %+v, want %+v", i, resp.DatahubURLs[i], w)
		}
	}
}

func TestHandleHealth_NilTeranode_ReturnsEmptyArray(t *testing.T) {
	srv := &Server{
		cfg:    &config.Config{},
		logger: zap.NewNop(),
	}

	_, resp, body := doHealth(t, srv)

	// Crucially, the field must be `[]`, not `null` — client code iterates it.
	if resp.DatahubURLs == nil {
		t.Fatalf("expected empty array, got nil (body=%s)", string(body))
	}
	if len(resp.DatahubURLs) != 0 {
		t.Errorf("expected empty list, got %+v", resp.DatahubURLs)
	}
	// Belt-and-braces: ensure the raw JSON has `"datahub_urls":[]` not `null`.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("re-decoding: %v", err)
	}
	if string(raw["datahub_urls"]) != "[]" {
		t.Errorf("expected datahub_urls to be `[]` in JSON, got %s", string(raw["datahub_urls"]))
	}
	// ARC clients require a literal `"healthy": true` — ensure the field is
	// present and not dropped/renamed by marshalling (issue #208).
	if string(raw["healthy"]) != "true" {
		t.Errorf("expected healthy to be `true` in JSON, got %s", string(raw["healthy"]))
	}
}

// TestHandleHealth_UnreachableEndpointFlipsUnhealthy is the end-to-end proof
// for the production complaint: /health reported a registered-but-dead
// endpoint as healthy:true forever, because the api-server pod never
// broadcasts and the probe loop only targeted already-unhealthy endpoints.
// With probe-all, the endpoint flips to healthy:false with zero broadcast
// traffic — driven purely by the background probe loop.
func TestHandleHealth_UnreachableEndpointFlipsUnhealthy(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadURL := dead.URL
	dead.Close() // port now refuses connections

	tc := teranode.NewClient([]string{deadURL}, "", teranode.HealthConfig{
		FailureThreshold: 3,
		ProbeInterval:    10 * time.Millisecond,
		ProbeTimeout:     200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc.Start(ctx)
	defer tc.Close()

	srv := &Server{
		cfg:      &config.Config{},
		logger:   zap.NewNop(),
		teranode: tc,
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, resp, _ := doHealth(t, srv)
		if len(resp.DatahubURLs) == 1 && !resp.DatahubURLs[0].Healthy {
			return // /health now tells the truth
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("/health kept reporting an unreachable endpoint as healthy for 2s")
}
