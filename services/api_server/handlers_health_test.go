package api_server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/teranode"
)

// healthResp mirrors the server's healthResponse shape but uses
// generic Go types so test code does not depend on unexported fields.
type healthResp struct {
	Status      string `json:"status"`
	Chaintracks struct {
		Enabled   bool   `json:"enabled"`
		Network   string `json:"network"`
		TipHeight uint32 `json:"tip_height"`
		TipHash   string `json:"tip_hash"`
		HasTip    bool   `json:"has_tip"`
	} `json:"chaintracks"`
	DatahubURLs []teranode.EndpointStatus `json:"datahub_urls"`
}

// doHealth exercises the real router on a given Server so we cover the Gin
// route binding and the JSON shape clients will actually receive.
func doHealth(t *testing.T, srv *Server) (int, healthResp, []byte) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	srv.registerRoutes(r)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
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
	fake := newFakeChaintracks()
	fake.network = "main"
	fake.height = 42
	fake.tip = fixtureTip()

	tc := teranode.NewClient(
		[]string{"https://a.example", "https://b.example"},
		"",
		teranode.HealthConfig{FailureThreshold: 2},
	)
	tc.AddEndpoints([]string{"https://c.example"})
	tc.RecordFailure("https://b.example")
	tc.RecordFailure("https://b.example") // trip

	srv := &Server{
		cfg:         &config.Config{ChaintracksServer: config.ChaintracksServerConfig{Enabled: true}},
		logger:      zap.NewNop(),
		chaintracks: fake,
		teranode:    tc,
	}

	code, resp, body := doHealth(t, srv)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", code, string(body))
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %q", resp.Status)
	}
	if !resp.Chaintracks.Enabled {
		t.Errorf("expected chaintracks.enabled=true")
	}
	if resp.Chaintracks.Network != "main" {
		t.Errorf("expected network=main, got %q", resp.Chaintracks.Network)
	}
	if resp.Chaintracks.TipHeight != 42 {
		t.Errorf("expected tip_height=42, got %d", resp.Chaintracks.TipHeight)
	}
	if !resp.Chaintracks.HasTip {
		t.Errorf("expected has_tip=true")
	}
	if resp.Chaintracks.TipHash != fixtureTip().Hash.String() {
		t.Errorf("expected tip_hash=%s, got %s", fixtureTip().Hash.String(), resp.Chaintracks.TipHash)
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

func TestHandleHealth_ChaintracksDisabled(t *testing.T) {
	tc := teranode.NewClient([]string{"https://a.example"}, "", teranode.HealthConfig{})

	srv := &Server{
		cfg:      &config.Config{},
		logger:   zap.NewNop(),
		teranode: tc,
		// chaintracks left nil — as when ChaintracksServer.Enabled=false
	}

	code, resp, _ := doHealth(t, srv)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d", code)
	}
	if resp.Chaintracks.Enabled {
		t.Errorf("expected chaintracks.enabled=false when chaintracks is nil")
	}
	if resp.Chaintracks.HasTip {
		t.Errorf("expected has_tip=false")
	}
	if len(resp.DatahubURLs) != 1 || resp.DatahubURLs[0].URL != "https://a.example" {
		t.Errorf("expected one datahub URL, got %+v", resp.DatahubURLs)
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
}
