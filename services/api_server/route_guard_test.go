package api_server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
	"github.com/bsv-blockchain/broadcaster/store/pebble"
)

// newGuardedRouteTestServer builds a real Server (real Pebble store, real
// Kafka producer over a recording broker, real registerRoutes) with
// auth+ratelimit enabled, so the guard chain under test is the actual
// production wiring, not a stand-in.
func newGuardedRouteTestServer(t *testing.T, costs config.RateLimitCosts, tier config.TierConfig) (*Server, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	broker := &kafka.RecordingBroker{}
	srv := &Server{
		cfg: &config.Config{
			CallbackToken: "cb-token",
			Auth:          config.AuthConfig{Enabled: true},
			RateLimit: config.RateLimitConfig{
				Enabled: true, LeaseFraction: 1, ShrinkThreshold: 0.8,
				KeyCacheTTLMs: 60_000, KeyCacheStaleMaxMs: 60_000, ExpectedReplicas: 1,
				Costs:     costs,
				Anonymous: tier,
				Tiers:     map[string]config.TierConfig{config.TierFree: tier},
			},
		},
		logger:         zap.NewNop(),
		producer:       kafka.NewProducer(broker),
		store:          st,
		submissionCh:   make(chan submissionRecord, submissionRecorderBuffer),
		submissionStop: make(chan struct{}),
	}
	require.NoError(t, srv.initAccessControl())
	router := gin.New()
	srv.registerRoutes(router)
	return srv, router
}

// TestRegisterRoutesExemptsInfraAndGuardsTx drives the real registerRoutes
// with auth+ratelimit enabled end to end: /health, /metrics and the merkle
// callback must never carry a rate-limit decision (they are not behind
// guarded()), while POST /tx must charge costs.submit_tx -- not
// costs.read -- against the caller's quota.
func TestRegisterRoutesExemptsInfraAndGuardsTx(t *testing.T) {
	_, router := newGuardedRouteTestServer(t,
		config.RateLimitCosts{SubmitTx: 5, Read: 1},
		config.TierConfig{UnitsPerMinute: 100, MaxInFlight: 5, MaxBatch: 10})

	for _, path := range []string{"/health", "/metrics"} {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, path)
		require.Empty(t, w.Header().Get("RateLimit-Policy"), "%s must not be guarded", path)
	}

	// The merkle callback route runs its own (separate) bearer-token check,
	// independent of auth.enabled -- the request below carries none, so
	// handleCallback's own "unauthorized" fires. If guarded() had wrongly
	// wrapped this route, the request would still reach handleCallback (no
	// Authorization header makes apikey.Middleware treat it as anonymous,
	// not a 401), so the response alone wouldn't distinguish the two paths
	// -- the absence of any rate-limit header is the actual proof.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/merkle-service/callback", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Contains(t, w.Body.String(), "unauthorized")
	require.Empty(t, w.Header().Get("RateLimit-Policy"), "the merkle callback route must not be guarded")

	// POST /tx must charge costs.submit_tx (5), not costs.read (1).
	tx := sdkTx.NewTransaction()
	tx.AddOutput(&sdkTx.TransactionOutput{Satoshis: 0, LockingScript: &script.Script{}})
	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/tx", bytes.NewReader(tx.Bytes()))
	req.Header.Set("Content-Type", "application/octet-stream")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	// The request carries no Authorization header, so the caller is
	// anonymous (not the "free" key tier) -- irrelevant to what's under
	// test here, which is the unit cost charged, not which tier pays it.
	require.Equal(t, `"anonymous";q=100;w=60`, w.Header().Get("RateLimit-Policy"))
	require.Equal(t, "95", w.Header().Get("X-RateLimit-Remaining"), "must be charged submit_tx (5), not read (1)")
}

// octetStreamRequest builds a POST /txs request carrying n trivial,
// unrelated (no shared inputs) transactions concatenated as wire bytes --
// the same harness pattern handlers_ssrf_test.go and partition_keys_test.go
// use for octet-stream batch bodies.
func octetStreamRequest(n int) *http.Request {
	var body []byte
	for i := 0; i < n; i++ {
		tx := sdkTx.NewTransaction()
		tx.AddOutput(&sdkTx.TransactionOutput{Satoshis: uint64(i), LockingScript: &script.Script{}})
		body = append(body, tx.Bytes()...)
	}
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	return req
}

// TestSubmitTransactionsBatchRejectedBy413BeforeValidation pins that
// CheckBatch's 413 fires before any per-tx validation or publish is
// attempted: max_batch is 1, the body carries 2 txs, and nothing in this
// Server (store, validator, finality, producer) is wired for a batch this
// size to succeed, so a 413 that reached past CheckBatch would surface as
// a different failure (a different status, or a panic on a nil
// dependency) rather than this one.
func TestSubmitTransactionsBatchRejectedBy413BeforeValidation(t *testing.T) {
	_, router := newGuardedRouteTestServer(t,
		config.RateLimitCosts{SubmitTx: 10, Read: 1},
		config.TierConfig{UnitsPerMinute: 1000, MaxInFlight: 5, MaxBatch: 1})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, octetStreamRequest(2))
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "exceeds the anonymous tier limit of 1")
}

// TestSubmitTransactionsBodyOverByteCapReturns413 pins that a /txs body
// overrunning the per-tier byte cap (max_batch x maxSingleTxBytes) gets a
// 413 with CheckBatch's own response shape -- not the generic 400 "failed
// to read body" every other io.ReadAll failure gets. max_batch is 1, so
// the per-tier cap here is 1*32MiB; the body's content never matters,
// since http.MaxBytesReader rejects it before anything tries to parse a
// transaction out of it.
func TestSubmitTransactionsBodyOverByteCapReturns413(t *testing.T) {
	_, router := newGuardedRouteTestServer(t,
		config.RateLimitCosts{SubmitTx: 10, Read: 1},
		config.TierConfig{UnitsPerMinute: 1_000_000, MaxInFlight: 5, MaxBatch: 1})

	body := bytes.Repeat([]byte{0}, 32<<20+1)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/txs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "request body exceeds the anonymous tier's batch byte limit")
	require.Contains(t, w.Body.String(), `"maxBatch":1`)
}

// TestSubmitTransactionsBatchCutsOffParsingEarly pins the early-cutoff fix:
// with max_batch 1, a body carrying far more than 1 tx (50) must be
// rejected as soon as the 2nd tx is parsed, not after all 50 are parsed.
// The body's own wording proves it -- "batch of 2", not "batch of 50" --
// since CheckBatch's message always reports len(parsed) at the point it's
// called.
func TestSubmitTransactionsBatchCutsOffParsingEarly(t *testing.T) {
	_, router := newGuardedRouteTestServer(t,
		config.RateLimitCosts{SubmitTx: 10, Read: 1},
		config.TierConfig{UnitsPerMinute: 1000, MaxInFlight: 5, MaxBatch: 1})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, octetStreamRequest(50))
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "batch of 2 transactions exceeds the anonymous tier limit of 1",
		"parsing must stop the instant the cap is exceeded, not after all 50 txs are parsed")
}

// TestSubmitTransactionsBatchRejectedBy429BeforeValidation pins that
// ChargeTxs' 429 fires before any per-tx validation or publish: read (1)
// is charged up front by guarded(), leaving 19 of a 20 units_per_minute
// budget; the batch of 2 txs then needs 2×submit_tx(10)=20, which no
// longer fits, so the second charge -- not a validation failure -- is what
// produces the 429.
func TestSubmitTransactionsBatchRejectedBy429BeforeValidation(t *testing.T) {
	_, router := newGuardedRouteTestServer(t,
		config.RateLimitCosts{SubmitTx: 10, Read: 1},
		config.TierConfig{UnitsPerMinute: 20, MaxInFlight: 5, MaxBatch: 100})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, octetStreamRequest(2))
	require.Equal(t, http.StatusTooManyRequests, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"reason":"quota"`)
}
