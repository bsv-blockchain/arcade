package api_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
	"github.com/bsv-blockchain/arcade/version"
)

const (
	testDatahubA = "https://a.example"
	testDatahubB = "https://b.example"
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
	BlockHeight uint64                    `json:"blockHeight"`
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
		[]string{testDatahubA, testDatahubB},
		"",
		teranode.HealthConfig{FailureThreshold: 2},
	)
	tc.AddEndpoints([]string{"https://c.example"})
	tc.RecordFailure(testDatahubB)
	tc.RecordFailure(testDatahubB) // trip

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
		{URL: testDatahubA, Source: "configured", Healthy: true},
		{URL: testDatahubB, Source: "configured", Healthy: false},
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

// TestHandleHealth_IncludesBlockHeight pins the chain-freshness field
// (issue #254): /health reports arcade's own processed active-tip height so
// clients can detect a stale chain view — datahub_urls[].healthy is
// reachability-only and stays green through a chain stall. The store read
// is TTL-cached (probes arrive every few seconds), and without a store the
// field is omitted rather than reading as "height 0".
func TestHandleHealth_IncludesBlockHeight(t *testing.T) {
	ms := &mockStore{tipHeight: 958_779}
	srv := &Server{
		cfg:    &config.Config{},
		logger: zap.NewNop(),
		store:  ms,
	}

	code, resp, body := doHealth(t, srv)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", code, string(body))
	}
	if resp.BlockHeight != 958_779 {
		t.Errorf("expected blockHeight=958779, got %d (body=%s)", resp.BlockHeight, string(body))
	}

	// A second probe inside the TTL must serve the cached height.
	_, resp, _ = doHealth(t, srv)
	if resp.BlockHeight != 958_779 {
		t.Errorf("cached probe: expected blockHeight=958779, got %d", resp.BlockHeight)
	}
	ms.mu.Lock()
	calls := ms.tipCalls
	ms.mu.Unlock()
	if calls != 1 {
		t.Errorf("expected 1 store tip read across 2 probes (TTL cache), got %d", calls)
	}

	// No store wired → the field is omitted entirely, not emitted as 0.
	srvNoStore := &Server{cfg: &config.Config{}, logger: zap.NewNop()}
	_, _, rawBody := doHealth(t, srvNoStore)
	if strings.Contains(string(rawBody), "blockHeight") {
		t.Errorf("expected blockHeight omitted without a store, body=%s", string(rawBody))
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

// endpointPolicyResp mirrors the policy block of a datahub_urls entry using
// generic types, for the same reason healthResp does.
type endpointPolicyResp struct {
	MiningFee struct {
		Satoshis uint64 `json:"satoshis"`
		Bytes    uint64 `json:"bytes"`
	} `json:"miningFee"`
	MaxTxSizePolicy         uint64 `json:"maxtxsizepolicy"`
	MaxScriptSizePolicy     uint64 `json:"maxscriptsizepolicy"`
	MaxTxSigopsCountsPolicy uint64 `json:"maxtxsigopscountspolicy"`
}

// datahubResp is one datahub_urls entry as a client sees it: the long-standing
// url/source/healthy triple, plus the optional advertised policy.
type datahubResp struct {
	URL     string              `json:"url"`
	Source  string              `json:"source"`
	Healthy bool                `json:"healthy"`
	Policy  *endpointPolicyResp `json:"policy"`
}

// doHealthDatahubs runs a probe and decodes only the datahub_urls array, so
// these tests are independent of the rest of the health envelope.
func doHealthDatahubs(t *testing.T, srv *Server) []datahubResp {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	srv.registerRoutes(r)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var resp struct {
		DatahubURLs []datahubResp `json:"datahub_urls"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding health JSON: %v (body=%s)", err, w.Body.String())
	}
	return resp.DatahubURLs
}

func advertisedPolicy() *store.EndpointPolicy {
	return &store.EndpointPolicy{
		MiningFeeSatoshis: 100, MiningFeeBytes: 1000,
		MaxTxSizePolicy: 100_000_000, MaxScriptSizePolicy: 500_000,
		MaxTxSigopsCountsPolicy: 4_294_967_295,
	}
}

// TestHandleHealth_ReportsAdvertisedPolicy is the operator-facing end of the
// chain: a node's node_status policy reaches the registry, and /health reports
// it against that node's endpoint. GET /policy can only report the single
// network-wide policy arcade enforces, so this is what identifies which node
// refused an oversized transaction.
func TestHandleHealth_ReportsAdvertisedPolicy(t *testing.T) {
	ms := &mockStore{datahubEndpoints: []store.DatahubEndpoint{
		{URL: testDatahubA, Network: config.NetworkMainnet, Policy: advertisedPolicy()},
		// Registered with a trailing slash — as a statically configured seed
		// URL is, since config values are stored verbatim while the teranode
		// client trims them. The two sides must still join, or this endpoint's
		// policy silently vanishes from the response.
		{URL: "https://c.example/", Network: config.NetworkMainnet, Policy: &store.EndpointPolicy{MaxTxSizePolicy: 7}},
		// A seeded URL nobody has announced carries no policy.
		{URL: testDatahubB, Network: config.NetworkMainnet},
	}}
	tc := teranode.NewClient([]string{testDatahubA, testDatahubB}, "", teranode.HealthConfig{})
	tc.AddEndpoints([]string{"https://c.example"})

	srv := &Server{
		cfg:      &config.Config{Network: config.NetworkMainnet},
		logger:   zap.NewNop(),
		store:    ms,
		teranode: tc,
	}

	got := doHealthDatahubs(t, srv)
	if len(got) != 3 {
		t.Fatalf("expected 3 endpoints, got %d: %+v", len(got), got)
	}

	byURL := map[string]datahubResp{}
	for _, d := range got {
		byURL[d.URL] = d
	}

	a := byURL[testDatahubA]
	if a.Policy == nil {
		t.Fatal("a.example lost its advertised policy")
	}
	if a.Policy.MiningFee.Satoshis != 100 || a.Policy.MiningFee.Bytes != 1000 {
		t.Errorf("miningFee = %+v, want {100 1000}", a.Policy.MiningFee)
	}
	if a.Policy.MaxTxSizePolicy != 100_000_000 || a.Policy.MaxScriptSizePolicy != 500_000 {
		t.Errorf("sizes = {%d %d}, want {100000000 500000}",
			a.Policy.MaxTxSizePolicy, a.Policy.MaxScriptSizePolicy)
	}
	if a.Policy.MaxTxSigopsCountsPolicy != 4_294_967_295 {
		t.Errorf("sigops = %d, want 4294967295", a.Policy.MaxTxSigopsCountsPolicy)
	}

	if p := byURL["https://c.example"].Policy; p == nil || p.MaxTxSizePolicy != 7 {
		t.Errorf("trailing-slash URL failed to join: %+v", p)
	}
	if p := byURL[testDatahubB].Policy; p != nil {
		t.Errorf("an unannounced endpoint must carry no policy, got %+v", *p)
	}

	// The pre-existing keys are untouched — ARC clients and health checkers
	// parse these, and policy is purely additive.
	if a.Source != "configured" || !a.Healthy {
		t.Errorf("url/source/healthy changed shape: %+v", a)
	}
}

// TestHandleHealth_PolicyOmittedNotNulled pins the wire shape: an endpoint with
// no advertised policy omits the key rather than emitting "policy": null, so a
// client can distinguish "not advertised" without a null check.
func TestHandleHealth_PolicyOmittedNotNulled(t *testing.T) {
	ms := &mockStore{datahubEndpoints: []store.DatahubEndpoint{
		{URL: testDatahubA, Network: config.NetworkMainnet},
	}}
	srv := &Server{
		cfg:      &config.Config{Network: config.NetworkMainnet},
		logger:   zap.NewNop(),
		store:    ms,
		teranode: teranode.NewClient([]string{testDatahubA}, "", teranode.HealthConfig{}),
	}

	_, _, body := doHealth(t, srv)
	if strings.Contains(string(body), "policy") {
		t.Errorf("expected the policy key omitted entirely, body=%s", string(body))
	}
}

// TestHandleHealth_PolicyReadIsCachedAndNeverFailsTheProbe covers the two
// properties the store read has to have on a liveness path: probes arrive every
// few seconds per replica, so the registry is read once per TTL rather than per
// probe; and a store failure serves the last known policies rather than
// blanking them or failing the probe.
func TestHandleHealth_PolicyReadIsCachedAndNeverFailsTheProbe(t *testing.T) {
	ms := &mockStore{datahubEndpoints: []store.DatahubEndpoint{
		{URL: testDatahubA, Network: config.NetworkMainnet, Policy: advertisedPolicy()},
	}}
	srv := &Server{
		cfg:      &config.Config{Network: config.NetworkMainnet},
		logger:   zap.NewNop(),
		store:    ms,
		teranode: teranode.NewClient([]string{testDatahubA}, "", teranode.HealthConfig{}),
	}

	if got := doHealthDatahubs(t, srv); got[0].Policy == nil {
		t.Fatal("first probe reported no policy")
	}
	// A second probe inside the TTL must serve the cached map.
	if got := doHealthDatahubs(t, srv); got[0].Policy == nil {
		t.Fatal("cached probe reported no policy")
	}
	ms.mu.Lock()
	calls := ms.datahubCalls
	ms.mu.Unlock()
	if calls != 1 {
		t.Errorf("expected 1 registry read across 2 probes (TTL cache), got %d", calls)
	}

	// Force the failure path: expire the cache, then break the store.
	srv.policyMu.Lock()
	srv.policyFetchedAt = time.Time{}
	srv.policyMu.Unlock()
	ms.mu.Lock()
	ms.datahubErr = errTest
	ms.mu.Unlock()

	got := doHealthDatahubs(t, srv)
	if len(got) != 1 {
		t.Fatalf("a store error must not change the endpoint list, got %+v", got)
	}
	if got[0].Policy == nil {
		t.Error("a store error must serve the last known policy, not blank it")
	}
}

// TestHandleHealth_NoStoreOmitsPolicy covers the struct-literal server with no
// store wired: endpoints are still listed, just without policies.
func TestHandleHealth_NoStoreOmitsPolicy(t *testing.T) {
	srv := &Server{
		cfg:      &config.Config{Network: config.NetworkMainnet},
		logger:   zap.NewNop(),
		teranode: teranode.NewClient([]string{testDatahubA}, "", teranode.HealthConfig{}),
	}

	got := doHealthDatahubs(t, srv)
	if len(got) != 1 || got[0].URL != testDatahubA {
		t.Fatalf("expected the endpoint still listed, got %+v", got)
	}
	if got[0].Policy != nil {
		t.Errorf("expected no policy without a store, got %+v", *got[0].Policy)
	}
}
