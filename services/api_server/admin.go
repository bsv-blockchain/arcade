package api_server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/services/apikey"
	"github.com/bsv-blockchain/arcade/store"
)

// maxAdminBodyBytes caps an admin request body: generous for a key's small
// JSON payload, tight enough to bound memory from a malicious or misbehaving
// holder of the admin token.
const maxAdminBodyBytes = 64 << 10

// maxAdminKeyNameLen caps createKeyRequest.Name: an operator-supplied label,
// never rendered as HTML, but unbounded user input has no business sitting
// in a database column or a log line indefinitely.
const maxAdminKeyNameLen = 256

// adminKeyView is the admin API's key representation. It never carries the
// hash, and the plaintext key appears only in createKeyResponse.
type adminKeyView struct {
	ID        string     `json:"id"`
	Hint      string     `json:"hint"`
	Name      string     `json:"name"`
	Tier      string     `json:"tier"`
	Status    string     `json:"status"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
	RevokedAt *time.Time `json:"revokedAt,omitempty"`
}

type createKeyRequest struct {
	Name      string     `json:"name"`
	Tier      string     `json:"tier"`
	ExpiresAt *time.Time `json:"expiresAt"`
}

type createKeyResponse struct {
	adminKeyView

	Key string `json:"key"`
}

type setTierRequest struct {
	Tier string `json:"tier"`
}

func viewOf(k *store.APIKey) adminKeyView {
	return adminKeyView{
		ID: k.ID, Hint: k.Hint, Name: k.Name, Tier: k.Tier, Status: string(k.Status),
		ExpiresAt: k.ExpiresAt, CreatedAt: k.CreatedAt, UpdatedAt: k.UpdatedAt, RevokedAt: k.RevokedAt,
	}
}

// adminNow is the timestamp admin writes use: UTC, millisecond precision,
// so every backend round-trips it exactly.
func adminNow() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

// decodeAdminJSON caps the body size and rejects unknown fields -- a typo'd
// or stale client field (e.g. "expires_at" instead of "expiresAt") must fail
// loudly rather than be silently ignored -- writing 400 "invalid request
// body: <err>" on any decode error. Returns false after writing the
// response, so callers just return.
func decodeAdminJSON(c *gin.Context, dst any) bool {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxAdminBodyBytes)
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: fmt.Sprintf("invalid request body: %s", err)})
		return false
	}
	return true
}

// newAdminRouter is the admin.* listener: key management and block
// reprocess, all behind admin.token. It is never routed publicly.
func (s *Server) newAdminRouter() *gin.Engine {
	r := gin.New()
	s.applyTrustedPlatform(r)
	r.Use(gin.CustomRecovery(s.recoverPanic))
	r.Use(s.requireAdminToken)
	v1 := r.Group("/admin/v1")
	v1.POST("/keys", s.handleAdminCreateKey)
	v1.GET("/keys", s.handleAdminListKeys)
	v1.GET("/keys/:id", s.handleAdminGetKey)
	v1.PATCH("/keys/:id", s.handleAdminSetTier)
	v1.DELETE("/keys/:id", s.handleAdminRevokeKey)
	v1.POST("/blocks/:blockHash/reprocess", s.handleReprocessBlock)
	return r
}

// requireAdminToken compares in constant time, like handleCallback.
func (s *Server) requireAdminToken(c *gin.Context) {
	const bearerPrefix = "Bearer "
	auth := c.GetHeader("Authorization")
	var presented []byte
	if strings.HasPrefix(auth, bearerPrefix) {
		presented = []byte(auth[len(bearerPrefix):])
	}
	configured := []byte(s.cfg.Admin.Token)
	if len(configured) == 0 || subtle.ConstantTimeCompare(configured, presented) != 1 {
		s.logger.Warn("admin request rejected: invalid or missing token", zap.String("client_ip", c.ClientIP()))
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{jsonKeyError: "unauthorized"})
		return
	}
	c.Next()
}

// startAdmin binds the admin listener synchronously, so a port clash fails
// Start, then serves it in the background until Stop. ctx governs only the
// address resolution during the bind (ListenConfig.Listen), not the
// listener's lifetime — the socket stays open until Stop calls
// adminServer.Shutdown, regardless of ctx's later cancellation.
//
// A second call on the same Server is rejected outright: without this,
// calling Start twice (e.g. a caller that restarts a Server after a failed
// first Start, without first calling Stop) would silently overwrite
// s.adminServer, orphaning the first listener -- it keeps its socket open
// and serving, but nothing can ever Shutdown it again.
func (s *Server) startAdmin(ctx context.Context) error {
	if s.adminServer != nil {
		return fmt.Errorf("admin listener already started")
	}
	addr := fmt.Sprintf("%s:%d", s.cfg.Admin.Host, s.cfg.Admin.Port)
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("admin listener %s: %w", addr, err)
	}
	s.adminServer = &http.Server{Handler: s.newAdminRouter(), ReadHeaderTimeout: 30 * time.Second}
	s.logger.Info("admin API listening", zap.String("addr", addr))
	go func() {
		if err := s.adminServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("admin server error", zap.Error(err))
		}
	}()
	return nil
}

func (s *Server) keyStore(c *gin.Context) (store.APIKeyStore, bool) {
	ks, ok := s.apiKeyStore()
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "store backend does not support api keys"})
	}
	return ks, ok
}

func (s *Server) knownTier(tier string) bool {
	_, ok := s.cfg.RateLimit.Tiers[tier]
	return ok
}

// adminKeyResult looks up id and responds with its current view, or the
// appropriate error if the lookup itself fails (404 for an unknown key).
// Used only for a pure read (handleAdminGetKey) — a mutation's own error
// must never be masked by this, so handlers that write first use
// respondAfterMutation instead.
func (s *Server) adminKeyResult(c *gin.Context, ks store.APIKeyStore, id string) {
	k, err := ks.GetAPIKey(c.Request.Context(), id)
	if err == nil {
		c.JSON(http.StatusOK, viewOf(k))
		return
	}
	if errors.Is(err, store.ErrAPIKeyNotFound) {
		c.JSON(http.StatusNotFound, gin.H{jsonKeyError: "api key not found"})
		return
	}
	s.logger.Error("admin api key lookup failed", zap.String("key_id", id), zap.Error(err))
	c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "store error"})
}

// respondAfterMutation reports the outcome of a write (tier change or
// revoke) that has already been attempted. mutationErr is that write's own
// error — ErrAPIKeyNotFound when id doesn't exist — and must not be masked
// by a read-back. On success it re-reads the key for the response; if that
// read-back itself fails, the write still committed, so this reports 200
// with just the id rather than a misleading 404/500 for a write that
// actually succeeded.
func (s *Server) respondAfterMutation(c *gin.Context, ks store.APIKeyStore, id string, mutationErr error) {
	if mutationErr != nil {
		if errors.Is(mutationErr, store.ErrAPIKeyNotFound) {
			c.JSON(http.StatusNotFound, gin.H{jsonKeyError: "api key not found"})
			return
		}
		s.logger.Error("admin api key operation failed", zap.String("key_id", id), zap.Error(mutationErr))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "store error"})
		return
	}
	k, err := ks.GetAPIKey(c.Request.Context(), id)
	if err == nil {
		c.JSON(http.StatusOK, viewOf(k))
		return
	}
	s.logger.Warn("mutation applied, read-back failed", zap.String("key_id", id), zap.Error(err))
	c.JSON(http.StatusOK, gin.H{"id": id})
}

func (s *Server) handleAdminCreateKey(c *gin.Context) {
	ks, ok := s.keyStore(c)
	if !ok {
		return
	}
	var req createKeyRequest
	if !decodeAdminJSON(c, &req) {
		return
	}
	if len(req.Name) > maxAdminKeyNameLen {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: fmt.Sprintf("name exceeds %d characters", maxAdminKeyNameLen)})
		return
	}
	if req.Tier == "" {
		req.Tier = config.TierFree
	}
	req.Tier = strings.ToLower(req.Tier)
	if !s.knownTier(req.Tier) {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: fmt.Sprintf("unknown tier %q", req.Tier)})
		return
	}
	now := adminNow()
	raw, err := apikey.Generate()
	if err != nil {
		s.logger.Error("api key generation failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "key generation failed"})
		return
	}
	id, err := apikey.NewID()
	if err != nil {
		s.logger.Error("api key id generation failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "key generation failed"})
		return
	}
	k := &store.APIKey{
		ID: id, Hash: apikey.Hash(raw), Hint: apikey.Hint(raw), Name: req.Name, Tier: req.Tier,
		Status: store.APIKeyActive, CreatedAt: now, UpdatedAt: now,
	}
	if req.ExpiresAt != nil {
		exp := req.ExpiresAt.UTC().Truncate(time.Millisecond)
		if !exp.After(now) {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "expiresAt must be in the future"})
			return
		}
		k.ExpiresAt = &exp
	}
	if err := ks.CreateAPIKey(c.Request.Context(), k); err != nil {
		s.logger.Error("create api key failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "store error"})
		return
	}
	s.logger.Info("api key created", zap.String("key_id", id), zap.String("tier", k.Tier))
	c.JSON(http.StatusCreated, createKeyResponse{adminKeyView: viewOf(k), Key: raw})
}

func (s *Server) handleAdminListKeys(c *gin.Context) {
	ks, ok := s.keyStore(c)
	if !ok {
		return
	}
	keys, err := ks.ListAPIKeys(c.Request.Context())
	if err != nil {
		s.logger.Error("list api keys failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "store error"})
		return
	}
	views := make([]adminKeyView, 0, len(keys))
	for _, k := range keys {
		views = append(views, viewOf(k))
	}
	c.JSON(http.StatusOK, gin.H{"keys": views})
}

func (s *Server) handleAdminGetKey(c *gin.Context) {
	if ks, ok := s.keyStore(c); ok {
		s.adminKeyResult(c, ks, c.Param("id"))
	}
}

func (s *Server) handleAdminSetTier(c *gin.Context) {
	ks, ok := s.keyStore(c)
	if !ok {
		return
	}
	var req setTierRequest
	if !decodeAdminJSON(c, &req) {
		return
	}
	req.Tier = strings.ToLower(req.Tier)
	if !s.knownTier(req.Tier) {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: fmt.Sprintf("unknown tier %q", req.Tier)})
		return
	}
	id := c.Param("id")
	err := ks.UpdateAPIKeyTier(c.Request.Context(), id, req.Tier, adminNow())
	if err == nil {
		s.logger.Info("api key tier changed", zap.String("key_id", id), zap.String("tier", req.Tier))
	}
	s.respondAfterMutation(c, ks, id, err)
}

func (s *Server) handleAdminRevokeKey(c *gin.Context) {
	ks, ok := s.keyStore(c)
	if !ok {
		return
	}
	id := c.Param("id")
	err := ks.RevokeAPIKey(c.Request.Context(), id, adminNow())
	if err == nil {
		s.logger.Info("api key revoked", zap.String("key_id", id))
	}
	s.respondAfterMutation(c, ks, id, err)
}

// stopAdmin drains the admin listener; nil-safe.
func (s *Server) stopAdmin(ctx context.Context) error {
	if s.adminServer == nil {
		return nil
	}
	return s.adminServer.Shutdown(ctx)
}
