package api_server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/store/pebble"
)

func newAdminTestServer(t *testing.T) (*Server, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	srv := &Server{
		cfg: &config.Config{
			Admin: config.AdminConfig{Enabled: true, Token: "admin-secret"},
			RateLimit: config.RateLimitConfig{Tiers: map[string]config.TierConfig{
				config.TierFree: {UnitsPerMinute: 50}, "pro": {UnitsPerMinute: 30000},
			}},
		},
		logger: zap.NewNop(),
		store:  st,
	}
	return srv, srv.newAdminRouter()
}

func adminDo(t *testing.T, h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestAdminRequiresToken(t *testing.T) {
	_, h := newAdminTestServer(t)
	require.Equal(t, http.StatusUnauthorized, adminDo(t, h, http.MethodGet, "/admin/v1/keys", "", "").Code)
	require.Equal(t, http.StatusUnauthorized, adminDo(t, h, http.MethodGet, "/admin/v1/keys", "wrong", "").Code)
	require.Equal(t, http.StatusOK, adminDo(t, h, http.MethodGet, "/admin/v1/keys", "admin-secret", "").Code)
}

// TestStartAdminRejectsSecondCall checks that calling startAdmin twice on
// the same Server fails outright instead of silently overwriting
// s.adminServer -- which would orphan the first listener (still open and
// serving, but no longer reachable by Stop).
func TestStartAdminRejectsSecondCall(t *testing.T) {
	srv := &Server{
		cfg:    &config.Config{Admin: config.AdminConfig{Enabled: true, Host: "127.0.0.1", Port: 0, Token: "admin-secret"}},
		logger: zap.NewNop(),
	}
	require.NoError(t, srv.startAdmin(t.Context()))
	t.Cleanup(func() { _ = srv.stopAdmin(context.Background()) })

	err := srv.startAdmin(t.Context())
	require.Error(t, err, "a second startAdmin call must fail, not overwrite the first listener")
}

func TestAdminKeyLifecycle(t *testing.T) {
	_, h := newAdminTestServer(t)
	const tok = "admin-secret"

	require.Equal(t, http.StatusBadRequest, adminDo(t, h, http.MethodPost, "/admin/v1/keys", tok, `{"name":"x","tier":"gold"}`).Code)

	w := adminDo(t, h, http.MethodPost, "/admin/v1/keys", tok, `{"name":"acme","tier":"pro"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var created createKeyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))
	require.True(t, strings.HasPrefix(created.Key, "bsvb_"))
	require.Equal(t, "pro", created.Tier)
	require.Equal(t, "active", created.Status)
	require.NotContains(t, w.Body.String(), `"hash"`)

	w = adminDo(t, h, http.MethodGet, "/admin/v1/keys/"+created.ID, tok, "")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), created.Key, "the plaintext key is only returned at creation")

	w = adminDo(t, h, http.MethodGet, "/admin/v1/keys", tok, "")
	require.Contains(t, w.Body.String(), created.ID)

	w = adminDo(t, h, http.MethodPatch, "/admin/v1/keys/"+created.ID, tok, `{"tier":"free"}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"tier":"free"`)

	w = adminDo(t, h, http.MethodDelete, "/admin/v1/keys/"+created.ID, tok, "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"status":"revoked"`)

	require.Equal(t, http.StatusNotFound, adminDo(t, h, http.MethodGet, "/admin/v1/keys/key_nope", tok, "").Code)
	require.Equal(t, http.StatusNotFound, adminDo(t, h, http.MethodDelete, "/admin/v1/keys/key_nope", tok, "").Code)
}

// TestAdminTierCaseInsensitive confirms that a mixed-case tier name in
// either the create or set-tier request is lowercased before validation
// against cfg.RateLimit.Tiers (whose keys Viper always lowercases), so
// "Pro" maps to the configured "pro" tier instead of erroring with
// "unknown tier".
func TestAdminTierCaseInsensitive(t *testing.T) {
	_, h := newAdminTestServer(t)
	const tok = "admin-secret"

	w := adminDo(t, h, http.MethodPost, "/admin/v1/keys", tok, `{"name":"acme","tier":"Pro"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var created createKeyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))
	require.Equal(t, "pro", created.Tier)

	w = adminDo(t, h, http.MethodPatch, "/admin/v1/keys/"+created.ID, tok, `{"tier":"FREE"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"tier":"free"`)
}

// TestAdminCreateKeyRejectsUnknownField confirms the create handler decodes
// strictly (json.Decoder.DisallowUnknownFields): a client sending the
// snake_case "expires_at" instead of the real field "expiresAt" gets a 400
// naming the problem, instead of the field being silently dropped and the
// key created without the expiry the caller asked for.
func TestAdminCreateKeyRejectsUnknownField(t *testing.T) {
	_, h := newAdminTestServer(t)
	w := adminDo(t, h, http.MethodPost, "/admin/v1/keys", "admin-secret",
		`{"name":"acme","tier":"free","expires_at":"2030-01-01T00:00:00Z"}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "invalid request body")
}

// TestAdminCreateKeyExpiresAtValidation covers both ends of the expiresAt
// check: a past timestamp is rejected, and a future one round-trips through
// GET unchanged (to millisecond precision -- adminNow's own granularity).
func TestAdminCreateKeyExpiresAtValidation(t *testing.T) {
	_, h := newAdminTestServer(t)

	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	w := adminDo(t, h, http.MethodPost, "/admin/v1/keys", "admin-secret",
		`{"name":"acme","tier":"free","expiresAt":"`+past+`"}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "expiresAt must be in the future")

	future := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	w = adminDo(t, h, http.MethodPost, "/admin/v1/keys", "admin-secret",
		`{"name":"acme","tier":"free","expiresAt":"`+future.Format(time.RFC3339Nano)+`"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var created createKeyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))
	require.NotNil(t, created.ExpiresAt)
	require.True(t, future.Equal(*created.ExpiresAt), "created expiresAt = %v, want %v", created.ExpiresAt, future)

	w = adminDo(t, h, http.MethodGet, "/admin/v1/keys/"+created.ID, "admin-secret", "")
	require.Equal(t, http.StatusOK, w.Code)
	var fetched adminKeyView
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &fetched))
	require.NotNil(t, fetched.ExpiresAt)
	require.True(t, future.Equal(*fetched.ExpiresAt), "fetched expiresAt = %v, want %v", fetched.ExpiresAt, future)
}

// TestAdminCreateKeyNameTooLong covers the 256-char cap on the name field.
func TestAdminCreateKeyNameTooLong(t *testing.T) {
	_, h := newAdminTestServer(t)
	longName := strings.Repeat("a", maxAdminKeyNameLen+1)
	w := adminDo(t, h, http.MethodPost, "/admin/v1/keys", "admin-secret",
		`{"name":"`+longName+`","tier":"free"}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "exceeds 256 characters")
}

// TestAdminSetTierMalformedBodyReportsInvalidBody pins the fix for a bug
// where a malformed set-tier body (one that fails to bind at all) was
// reported as `unknown tier ""` -- confusing, since the caller never sent
// an empty tier, the body just didn't parse. It must say "invalid request
// body" instead.
func TestAdminSetTierMalformedBodyReportsInvalidBody(t *testing.T) {
	_, h := newAdminTestServer(t)
	w := adminDo(t, h, http.MethodPost, "/admin/v1/keys", "admin-secret", `{"name":"acme","tier":"free"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var created createKeyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

	w = adminDo(t, h, http.MethodPatch, "/admin/v1/keys/"+created.ID, "admin-secret", `{"tier":`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "invalid request body")
	require.NotContains(t, w.Body.String(), "unknown tier")

	w = adminDo(t, h, http.MethodPatch, "/admin/v1/keys/"+created.ID, "admin-secret", `{"tier":"free","bogus":true}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "invalid request body")
}

// flakyGetAPIKeyStore wraps a real store.Store and makes exactly the next
// GetAPIKey call fail, regardless of id. Embedding the concrete pebble
// store (not just store.APIKeyStore) means every other method -- including
// the rest of store.Store, which the Server.store field requires -- is
// promoted unchanged.
type flakyGetAPIKeyStore struct {
	*pebble.Store

	failNextGet bool
}

func (f *flakyGetAPIKeyStore) GetAPIKey(ctx context.Context, id string) (*store.APIKey, error) {
	if f.failNextGet {
		f.failNextGet = false
		return nil, errors.New("injected read-back failure")
	}
	return f.Store.GetAPIKey(ctx, id)
}

// TestAdminMutationReadBackFailureStillReports200 pins respondAfterMutation:
// when a write (set-tier or revoke) itself succeeds but the read-back used
// to build the response fails, the caller must not see a misleading 404 or
// 500 for a write that actually committed -- it gets 200 with just the id.
func TestAdminMutationReadBackFailureStillReports200(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	flaky := &flakyGetAPIKeyStore{Store: st}

	srv := &Server{
		cfg: &config.Config{
			Admin: config.AdminConfig{Enabled: true, Token: "admin-secret"},
			RateLimit: config.RateLimitConfig{Tiers: map[string]config.TierConfig{
				config.TierFree: {UnitsPerMinute: 50}, "pro": {UnitsPerMinute: 30000},
			}},
		},
		logger: zap.NewNop(),
		store:  flaky,
	}
	h := srv.newAdminRouter()

	w := adminDo(t, h, http.MethodPost, "/admin/v1/keys", "admin-secret", `{"name":"acme","tier":"free"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var created createKeyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

	flaky.failNextGet = true
	w = adminDo(t, h, http.MethodPatch, "/admin/v1/keys/"+created.ID, "admin-secret", `{"tier":"pro"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.JSONEq(t, `{"id":"`+created.ID+`"}`, w.Body.String())

	// The write landed even though the read-back that reported it failed.
	w = adminDo(t, h, http.MethodGet, "/admin/v1/keys/"+created.ID, "admin-secret", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"tier":"pro"`)
}

func TestReprocessRoutePlacement(t *testing.T) {
	srv, admin := newAdminTestServer(t)
	public := gin.New()
	srv.registerRoutes(public)
	hash := strings.Repeat("ab", 32)
	require.Equal(t, http.StatusNotFound,
		adminDo(t, public, http.MethodPost, "/api/v1/blocks/"+hash+"/reprocess", "", "").Code,
		"with admin enabled, reprocess is not on the public API")
	require.Equal(t, http.StatusServiceUnavailable,
		adminDo(t, admin, http.MethodPost, "/admin/v1/blocks/"+hash+"/reprocess", "admin-secret", "").Code,
		"served on the admin listener (503: no merkle client in this test)")

	srv.cfg.Admin.Enabled = false
	open := gin.New()
	srv.registerRoutes(open)
	require.Equal(t, http.StatusServiceUnavailable,
		adminDo(t, open, http.MethodPost, "/api/v1/blocks/"+hash+"/reprocess", "", "").Code,
		"without the admin listener the public route stays")
}
