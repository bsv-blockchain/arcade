package api_server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/validator"
)

var errTest = errors.New("policy test: store failure")

func setupPolicyServer(ms *mockStore, vcfg config.ValidatorConfig, val *validator.Validator) (*Server, *gin.Engine) {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		cfg:       &config.Config{Network: "mainnet", Validator: vcfg},
		logger:    zap.NewNop(),
		store:     ms,
		validator: val,
	}
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

func getPolicy(t *testing.T, router http.Handler, path string) models.PolicyResponse {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET %s: status = %d, want 200; body=%s", path, w.Code, w.Body.String())
	}
	var resp models.PolicyResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode policy response: %v; body=%s", err, w.Body.String())
	}
	return resp
}

// TestHandlePolicy_ReportsValidatorFloor is the production path: /policy
// advertises exactly the fee floor and size limits the validator enforces.
func TestHandlePolicy_ReportsValidatorFloor(t *testing.T) {
	v := validator.NewValidator(nil) // starts at DefaultMinFeePerKB (100)
	v.SetMinFeePerKB(37)             // as the fee refresher would after observing the network
	_, router := setupPolicyServer(&mockStore{}, config.ValidatorConfig{StandardFormatSupported: true}, v)

	resp := getPolicy(t, router, "/policy")
	if resp.Policy.MiningFee != (models.FeeAmount{Satoshis: 37, Bytes: 1000}) {
		t.Errorf("miningFee = %+v, want {37 1000} (validator floor)", resp.Policy.MiningFee)
	}
	if resp.Policy.MaxTxSizePolicy != 10485760 || resp.Policy.MaxScriptSizePolicy != 500000 {
		t.Errorf("size fields = {%d %d}, want validator defaults {10485760 500000}",
			resp.Policy.MaxTxSizePolicy, resp.Policy.MaxScriptSizePolicy)
	}
	if resp.Policy.MaxTxSigopsCountsPolicy != 0 {
		t.Errorf("maxtxsigopscountspolicy = %d, want 0", resp.Policy.MaxTxSigopsCountsPolicy)
	}
	if !resp.Policy.StandardFormatSupported {
		t.Errorf("standardFormatSupported = false, want true")
	}
	if resp.Timestamp.IsZero() {
		t.Errorf("timestamp is zero, want an RFC3339 time")
	}
}

// The remaining cases exercise the no-validator fallback (struct-literal test
// servers): the reported fee comes from config, else the built-in default.
func TestHandlePolicy_ConfiguredFeeFallback(t *testing.T) {
	_, router := setupPolicyServer(&mockStore{}, config.ValidatorConfig{MinFeePerKB: 250}, nil)
	resp := getPolicy(t, router, "/policy")
	if resp.Policy.MiningFee != (models.FeeAmount{Satoshis: 250, Bytes: 1000}) {
		t.Errorf("miningFee = %+v, want {250 1000}", resp.Policy.MiningFee)
	}
}

func TestHandlePolicy_AcceptZeroFeeFallback(t *testing.T) {
	_, router := setupPolicyServer(&mockStore{}, config.ValidatorConfig{AcceptZeroFee: true, MinFeePerKB: 250}, nil)
	resp := getPolicy(t, router, "/policy")
	if resp.Policy.MiningFee != (models.FeeAmount{Satoshis: 0, Bytes: 1000}) {
		t.Errorf("miningFee = %+v, want {0 1000}", resp.Policy.MiningFee)
	}
}

func TestHandlePolicy_DefaultFeeFallback(t *testing.T) {
	_, router := setupPolicyServer(&mockStore{}, config.ValidatorConfig{MinFeePerKB: 0}, nil)
	resp := getPolicy(t, router, "/policy")
	want := models.FeeAmount{Satoshis: config.DefaultValidatorMinFeePerKB, Bytes: 1000}
	if resp.Policy.MiningFee != want {
		t.Errorf("miningFee = %+v, want %+v", resp.Policy.MiningFee, want)
	}
}

func TestHandlePolicy_SizeAndFormatFieldsFromConfig(t *testing.T) {
	vcfg := config.ValidatorConfig{
		MaxTxSizePolicy:         12345678,
		MaxScriptSizePolicy:     4242,
		MaxTxSigopsCountsPolicy: 99,
		StandardFormatSupported: true,
	}
	_, router := setupPolicyServer(&mockStore{}, vcfg, nil)

	resp := getPolicy(t, router, "/policy")
	if resp.Policy.MaxTxSizePolicy != 12345678 {
		t.Errorf("maxtxsizepolicy = %d, want 12345678", resp.Policy.MaxTxSizePolicy)
	}
	if resp.Policy.MaxScriptSizePolicy != 4242 {
		t.Errorf("maxscriptsizepolicy = %d, want 4242", resp.Policy.MaxScriptSizePolicy)
	}
	if resp.Policy.MaxTxSigopsCountsPolicy != 99 {
		t.Errorf("maxtxsigopscountspolicy = %d, want 99", resp.Policy.MaxTxSigopsCountsPolicy)
	}
	if !resp.Policy.StandardFormatSupported {
		t.Errorf("standardFormatSupported = false, want true")
	}
}

func TestHandlePolicy_V1Alias(t *testing.T) {
	_, router := setupPolicyServer(&mockStore{}, config.ValidatorConfig{MinFeePerKB: 300}, nil)
	resp := getPolicy(t, router, "/v1/policy")
	if resp.Policy.MiningFee.Satoshis != 300 {
		t.Errorf("/v1/policy miningFee.satoshis = %d, want 300", resp.Policy.MiningFee.Satoshis)
	}
}
