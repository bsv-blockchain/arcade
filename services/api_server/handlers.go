package api_server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"html/template"
	"io"
	"net/http"
	"strings"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/callbackurl"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// collectInputTXIDs returns the parent txids referenced by every input of
// tx. Empty for coinbase. Used to populate the propagation envelope's
// input_txids field so the dispatcher can detect parent-child
// relationships without re-parsing the raw bytes downstream.
func collectInputTXIDs(tx *sdkTx.Transaction) []string {
	if tx == nil || len(tx.Inputs) == 0 {
		return nil
	}
	out := make([]string, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		if in == nil || in.SourceTXID == nil {
			continue
		}
		out = append(out, in.SourceTXID.String())
	}
	return out
}

const jsonKeyError = "error"

// submitOptions captures the callback subscription preferences a client
// expressed via the X-CallbackUrl / X-CallbackToken / X-FullStatusUpdates
// request headers. Empty when the client did not subscribe.
type submitOptions struct {
	CallbackURL       string
	CallbackToken     string
	FullStatusUpdates bool
}

// extractSubmitOptions reads the callback-subscription headers off a submit
// request. Mirrors the old arcade's header contract so existing clients (and
// the SSE catchup token filter) keep working unchanged.
func extractSubmitOptions(c *gin.Context) submitOptions {
	return submitOptions{
		CallbackURL:       c.GetHeader("X-CallbackUrl"),
		CallbackToken:     c.GetHeader("X-CallbackToken"),
		FullStatusUpdates: c.GetHeader("X-FullStatusUpdates") == "true",
	}
}

// validateCallbackURL applies the SSRF guard to the X-CallbackUrl header
// before the request is allowed to register a subscription. Empty URLs
// pass through — token-only subscriptions don't trigger an outbound dial,
// so there's no SSRF surface to protect. The shared callbackurl predicate
// is the same one the webhook delivery client uses at dial time, so a host
// that survives this check still gets re-validated at connection time
// (catches DNS rebinding).
//
// Returns a 400 to the client on failure and reports false; callers should
// abort processing in that case. The unsafe URL is logged at debug (not
// the value itself, just the host) so operators can correlate refusals
// without leaking attacker-controlled strings into structured logs.
func (s *Server) validateCallbackURL(c *gin.Context, url string) bool {
	if url == "" {
		return true
	}
	if err := callbackurl.ValidateURL(url, s.cfg.Callback.AllowPrivateIPs); err != nil {
		s.logger.Warn(
			"rejecting submit due to unsafe callback url",
			zap.String("client_ip", c.ClientIP()),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid callback url: " + err.Error()})
		return false
	}
	return true
}

// hasSubscription reports whether the request asked for callback / SSE
// notifications. Either a URL or a token is enough — token-only clients use
// the SSE endpoint with no webhook delivery, URL-only clients get webhooks
// without SSE filtering.
func (o submitOptions) hasSubscription() bool {
	return o.CallbackURL != "" || o.CallbackToken != ""
}

// recordSubmission queues a callback registration for txid onto the async
// recorder. Best-effort: a full queue drops with a warn+metric — the contract
// matches the prior synchronous version (InsertSubmission failures were
// already logged and non-fatal). Moving the Pebble write off the HTTP
// handler removes a per-request DB write from POST tail latency.
func (s *Server) recordSubmission(_ context.Context, txid string, opts submitOptions) {
	if !opts.hasSubscription() {
		return
	}
	id, err := newSubmissionID()
	if err != nil {
		s.logger.Warn("could not generate submission id", zap.Error(err))
		return
	}
	sub := &models.Submission{
		SubmissionID:      id,
		TxID:              txid,
		CallbackURL:       opts.CallbackURL,
		CallbackToken:     opts.CallbackToken,
		FullStatusUpdates: opts.FullStatusUpdates,
		CreatedAt:         time.Now(),
	}
	select {
	case s.submissionCh <- submissionRecord{sub: sub}:
	default:
		metrics.APISubmissionRecorderDropTotal.Inc()
		s.logger.Warn(
			"submission recorder queue full; dropping (best-effort)",
			zap.String("txid", txid),
		)
	}
}

// newSubmissionID returns a 16-byte random hex identifier. Globally unique
// per call without coordinating across pods.
func newSubmissionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

const docsTemplate = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Arcade API</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #333; }
  h1 { border-bottom: 2px solid #eee; padding-bottom: 10px; }
  table { width: 100%; border-collapse: collapse; margin-top: 20px; }
  th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid #eee; }
  th { background: #f8f8f8; font-weight: 600; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 3px; font-size: 0.9em; }
  .method { font-weight: bold; }
  .get { color: #2e7d32; }
  .post { color: #1565c0; }
</style>
</head>
<body>
<h1>Arcade API</h1>
<p>Available routes:</p>
<table>
  <tr><th>Method</th><th>Path</th><th>Description</th><th>Request</th><th>Response</th></tr>
  {{range .}}<tr>
    <td class="method {{.Method | lower}}">{{.Method}}</td>
    <td><code>{{.Path}}</code></td>
    <td>{{.Description}}</td>
    <td>{{.RequestFormat}}</td>
    <td><code>{{.ResponseFormat}}</code></td>
  </tr>{{end}}
</table>
</body>
</html>`

var docsTmpl = template.Must(template.New("docs").Funcs(template.FuncMap{
	"lower": strings.ToLower,
}).Parse(docsTemplate))

func (s *Server) handleDocs(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	if err := docsTmpl.Execute(c.Writer, routeDocs); err != nil {
		s.logger.Error("failed to render docs", zap.Error(err))
	}
}

// healthResponse is the schema of GET /health. The top-level "status":"ok"
// preserves backwards compatibility with existing health checkers that
// only grep the response for liveness. Chaintracks moved out of api-server
// in the microservice decomposition — its health is now reported by the
// standalone chaintracks pod's /health endpoint.
type healthResponse struct {
	Status      string                    `json:"status"`
	DatahubURLs []teranode.EndpointStatus `json:"datahub_urls"`
}

func (s *Server) handleHealth(c *gin.Context) {
	resp := healthResponse{Status: "ok", DatahubURLs: []teranode.EndpointStatus{}}
	if s.teranode != nil {
		resp.DatahubURLs = s.teranode.GetEndpointStatuses()
	}
	c.JSON(http.StatusOK, resp)
}

func (s *Server) handleReady(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// callbackMaxBodyBytes returns the configured upper bound on the request
// body for POST /api/v1/merkle-service/callback, falling back to the
// package default when the operator leaves the knob unset or sets a
// non-positive value. Centralized so the handler and tests stay in sync.
func (s *Server) callbackMaxBodyBytes() int64 {
	if n := s.cfg.Callback.MaxBodyBytes; n > 0 {
		return n
	}
	return config.DefaultCallbackMaxBodyBytes
}

// handleCallback processes inbound callbacks from Merkle Service.
// Uses CallbackMessage format with Type field.
//
// Bearer-token authentication is mandatory. config.validate refuses to start
// the binary when MerkleService is configured without a CallbackToken (finding
// F-018 / issue #76), so reaching this handler with an empty configured token
// means a misconfigured deployment outside the supported envelope. We still
// fail closed here as a defense-in-depth measure: an empty/missing bearer or
// any mismatch is rejected with 401 before any callback processing runs.
//
// The request body is wrapped in http.MaxBytesReader before JSON-binding so
// a malicious or malfunctioning peer can't exhaust memory by streaming an
// unbounded JSON body (notably the embedded STUMP blob). Oversize bodies
// produce a 413 Payload Too Large; the cap is configurable via
// Callback.MaxBodyBytes (default 16 MiB). See F-019 / issue #77.
func (s *Server) handleCallback(c *gin.Context) {
	// Bearer token validation — always enforced, never skipped on empty
	// configured token. subtle.ConstantTimeCompare removes the timing side
	// channel that a plain == on the secret would expose.
	auth := c.GetHeader("Authorization")
	const bearerPrefix = "Bearer "
	configured := []byte(s.cfg.CallbackToken)
	var presented []byte
	if strings.HasPrefix(auth, bearerPrefix) {
		presented = []byte(auth[len(bearerPrefix):])
	}
	if len(configured) == 0 || subtle.ConstantTimeCompare(configured, presented) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{jsonKeyError: "unauthorized"})
		return
	}

	// Cap the inbound body BEFORE JSON-binding. http.MaxBytesReader returns a
	// *http.MaxBytesError once the limit is exceeded, which we map to 413.
	// Other decode errors (malformed JSON, type mismatches) keep their
	// existing 400 mapping — the contract for legitimate malformed input
	// hasn't changed.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, s.callbackMaxBodyBytes())

	var msg models.CallbackMessage
	if err := c.ShouldBindJSON(&msg); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{jsonKeyError: "request body too large"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid request body"})
		return
	}

	logger := s.logger.With(
		zap.String("type", string(msg.Type)),
		zap.String("txid", msg.TxID),
		zap.Strings("txids", msg.TxIDs),
		zap.String("blockHash", msg.BlockHash),
	)

	switch msg.Type {
	case models.CallbackSeenOnNetwork:
		s.handleSeenOnNetwork(c, msg, logger)
		c.Status(http.StatusOK)
	case models.CallbackSeenMultipleNodes:
		s.handleSeenMultipleNodes(c, msg, logger)
		c.Status(http.StatusOK)
	case models.CallbackStump:
		s.handleStump(c, msg, logger)
	case models.CallbackBlockProcessed:
		s.handleBlockProcessed(c, msg, logger)
	default:
		logger.Warn("unknown callback type")
		c.Status(http.StatusOK)
	}
}

// handleSeenOnNetwork applies a SEEN_ON_NETWORK callback from merkle-service
// to every txid in the message. Unknown txids (i.e. callbacks for txs we
// never recorded) are dropped with a Warn — never created as phantom rows.
// See F-033 / issue #91; the store layer enforces this by returning
// store.ErrNotFound from UpdateStatus when the row is absent.
//
// Batch path: one BatchUpdateStatusReturning call (parallel per-shard
// writes, with previous rows returned for transition-age observation) plus
// one PublishBulk event covering every successful txid. The previous
// per-tx loop did N synchronous store calls and N kafka.Send calls; under
// 100 TPS with merkle-service batching ~50 txids per callback that was
// ~50× the work per callback. See plan: RECEIVED → SEEN_ON_NETWORK
// Latency.
func (s *Server) handleSeenOnNetwork(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) {
	s.applySeenCallback(c, msg, logger, models.StatusSeenOnNetwork, "SEEN_ON_NETWORK")
}

// handleSeenMultipleNodes applies a SEEN_ON_MULTIPLE_NODES callback. Same
// unknown-txid handling as handleSeenOnNetwork — the store rejects updates
// to absent rows (F-033 / #91) and we log + continue rather than creating
// phantom rows.
func (s *Server) handleSeenMultipleNodes(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) {
	s.applySeenCallback(c, msg, logger, models.StatusSeenMultipleNodes, "SEEN_MULTIPLE_NODES")
}

// applySeenCallback is the shared body of the two "seen" callback paths.
// targetStatus is the status to transition each known txid to;
// metricLabel is the type label used on the callback metrics.
func (s *Server) applySeenCallback(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger, targetStatus models.Status, metricLabel string) {
	start := time.Now()
	outcome := "success"
	defer func() {
		metrics.CallbackHandlerDuration.WithLabelValues(metricLabel, outcome).Observe(time.Since(start).Seconds())
	}()

	txids := msg.ResolveSeenTxIDs()
	metrics.CallbackBatchSize.WithLabelValues(metricLabel).Observe(float64(len(txids)))
	if len(txids) == 0 {
		return
	}

	ctx := c.Request.Context()
	now := time.Now()

	// Tracker prefilter: drop txids whose in-memory tracked status already
	// eclipses targetStatus. At 100 TPS sustained ~91% of SEEN_ON_NETWORK
	// callback txids end up lattice-skipped at the store (merkle-service
	// re-fires after the tx is already past SEEN_ON_NETWORK). The tracker
	// is updated under the same RWMutex the rest of arcade uses and is
	// always ≤ the store's status, so a prefilter hit is provably safe —
	// the store-side lattice remains authoritative for anything the
	// tracker doesn't know about. keptTxIDs maps the post-filter index
	// back to the original txid so per-row metric labels stay accurate.
	statuses := make([]*models.TransactionStatus, 0, len(txids))
	keptTxIDs := make([]string, 0, len(txids))
	for _, txid := range txids {
		if s.txTracker != nil {
			if prevStatus, ok := s.txTracker.GetStatus(txid); ok {
				if prevStatus == targetStatus || !targetStatus.CanTransitionFrom(prevStatus) {
					metrics.CallbackStaleTotal.WithLabelValues(metricLabel, string(prevStatus)).Inc()
					continue
				}
			}
		}
		statuses = append(statuses, &models.TransactionStatus{
			TxID:      txid,
			Status:    targetStatus,
			Timestamp: now,
		})
		keptTxIDs = append(keptTxIDs, txid)
	}
	if len(statuses) == 0 {
		return
	}

	prevs, err := s.store.BatchUpdateStatusReturning(ctx, statuses)
	if err != nil {
		outcome = "error"
		logger.Warn("batch update seen status failed",
			zap.String("type", metricLabel),
			zap.Int("batch_size", len(txids)),
			zap.Error(err),
		)
		// Continue: per-row errors are nil-prev in the slice; we still want
		// to publish successful transitions if any.
	}

	successful := make([]string, 0, len(statuses))
	for i, prev := range prevs {
		if prev == nil {
			outcome = "partial"
			metrics.CallbackUnknownTxIDTotal.WithLabelValues(metricLabel).Inc()
			logger.Warn("dropping callback for unknown txid",
				zap.String("type", metricLabel),
				zap.String("txid", keptTxIDs[i]))
			continue
		}
		// Observe transition age — the headline metric the user asked for.
		// Use the previous row's Timestamp (last-update wall-clock) as the
		// anchor; for the RECEIVED→SEEN_ON_NETWORK case it equals the
		// time the validator marked the tx RECEIVED, which is exactly
		// the latency we want to optimize against.
		if !prev.Timestamp.IsZero() {
			metrics.StatusTransitionAge.
				WithLabelValues(string(prev.Status), string(targetStatus)).
				Observe(time.Since(prev.Timestamp).Seconds())
		}
		// Status lattice skipped the update — no transition to fan out.
		// Record the stale-callback signal so an operator can alert on
		// upstream rate without parsing the store_updatestatus histogram.
		// Two stale sub-cases: prev == target (duplicate callback) and
		// target not reachable from prev (e.g. MINED → SEEN_ON_NETWORK).
		if prev.Status == targetStatus || !targetStatus.CanTransitionFrom(prev.Status) {
			metrics.CallbackStaleTotal.WithLabelValues(metricLabel, string(prev.Status)).Inc()
			continue
		}
		successful = append(successful, keptTxIDs[i])
		if s.txTracker != nil {
			s.txTracker.UpdateStatus(keptTxIDs[i], targetStatus)
		}
	}

	// Single PublishBulk for the whole batch — drops the per-txid Kafka send
	// down to one event regardless of N. Subscribers (SSE, webhook) unfan in
	// their own handler, where they pay the per-tx cost without saturating
	// the bounded work queue.
	if len(successful) > 0 && s.publisher != nil {
		template := &models.TransactionStatus{
			Status:    targetStatus,
			Timestamp: now,
			TxIDs:     successful,
		}
		if pubErr := s.publisher.PublishBulk(ctx, template); pubErr != nil {
			logger.Warn("failed to publish bulk seen-status",
				zap.String("type", metricLabel),
				zap.Int("count", len(successful)),
				zap.Error(pubErr),
			)
		}
	}
}

func (s *Server) handleStump(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) {
	if msg.BlockHash == "" || len(msg.Stump) == 0 {
		logger.Warn("incomplete STUMP callback")
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "blockHash and stump are required"})
		return
	}

	// Store STUMP keyed by (blockHash, subtreeIndex). Synchronous write so that
	// a 200 to merkle-service is a durability guarantee — merkle-service only
	// fires BLOCK_PROCESSED after all STUMPs succeed, so the bump builder can
	// then rely on finding them all in Aerospike.
	stump := &models.Stump{
		BlockHash:    msg.BlockHash,
		SubtreeIndex: msg.SubtreeIndex,
		StumpData:    msg.Stump,
	}
	if err := s.store.InsertStump(c.Request.Context(), stump); err != nil {
		logger.Error("failed to store STUMP", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to store stump"})
		return
	}

	c.Status(http.StatusOK)
}

func (s *Server) handleBlockProcessed(c *gin.Context, msg models.CallbackMessage, logger *zap.Logger) {
	if msg.BlockHash == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "blockHash is required"})
		return
	}
	// Record the milestone for observability before enqueueing — this is
	// load-bearing only for the /api/v1/blocks/processing-status surface,
	// not for BUMP construction. The callback message currently carries no
	// block height, so 0 is passed; UpsertBlockHeaderSeen on the chaintracks
	// path is authoritative for height and will overwrite a 0 placeholder.
	if err := s.store.MarkBlockProcessed(c.Request.Context(), msg.BlockHash, 0, time.Now()); err != nil {
		logger.Warn("failed to record block_processed status", zap.Error(err))
	}
	if err := s.producer.Send(kafka.TopicBlockProcessed, msg.BlockHash, msg); err != nil {
		logger.Error("failed to publish block_processed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to enqueue"})
		return
	}
	c.Status(http.StatusOK)
}

// handleGetTransaction retrieves a transaction status by TXID.
func (s *Server) handleGetTransaction(c *gin.Context) {
	txid := c.Param("txid")
	if txid == "" {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "txid is required"})
		return
	}

	status, err := s.store.GetStatus(c.Request.Context(), txid)
	if err != nil {
		s.logger.Error("failed to get status", zap.String("txid", txid), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "internal error"})
		return
	}

	if status == nil {
		c.JSON(http.StatusNotFound, gin.H{jsonKeyError: "transaction not found"})
		return
	}

	c.JSON(http.StatusOK, status)
}

// Per-request body size caps for the submit endpoints. A BSV transaction can
// legally be quite large, but at the API boundary we want a hard upper bound
// so a single client can't exhaust memory with a crafted body. Sized for a
// generous single transaction and a generous batch.
const (
	maxSingleTxBytes = 32 << 20  // 32 MiB per single-tx submit
	maxBatchBytes    = 256 << 20 // 256 MiB per batch submit
)

// handleSubmitTransaction accepts transactions for validation and propagation.
// Supports application/octet-stream, text/plain (hex), and JSON.
func (s *Server) handleSubmitTransaction(c *gin.Context) {
	// SSRF guard: reject before reading the body so a hostile client can't
	// exhaust ingress bandwidth alongside a banned callback host. The same
	// predicate runs again at dial time on the webhook delivery client to
	// catch DNS rebinding.
	opts := extractSubmitOptions(c)
	if !s.validateCallbackURL(c, opts.CallbackURL) {
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxSingleTxBytes)

	var rawTx []byte

	contentType := c.ContentType()
	switch {
	case strings.Contains(contentType, "octet-stream"):
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to read body"})
			return
		}
		rawTx = body
	case strings.Contains(contentType, "text/plain"):
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to read body"})
			return
		}
		decoded, err := hex.DecodeString(strings.TrimSpace(string(body)))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid hex"})
			return
		}
		rawTx = decoded
	default:
		// JSON format
		var req struct {
			RawTx string `json:"rawTx"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid request"})
			return
		}
		decoded, err := hex.DecodeString(req.RawTx)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "invalid hex in rawTx"})
			return
		}
		rawTx = decoded
	}

	if len(rawTx) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "empty transaction"})
		return
	}

	// Parse the wire bytes so we can derive the canonical txid from the
	// transaction structure. Hashing the wire bytes directly is wrong when
	// clients submit Extended Format (which includes per-input source
	// metadata) — the resulting hash matches the EF blob, not the canonical
	// Bitcoin txid the validator emits later, so SSE/webhook callbacks
	// (keyed by submissions.txid) silently fail to match.
	parsedTx, _, err := sdkTx.NewTransactionFromStream(rawTx)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to parse transaction"})
		return
	}

	// Publish to transaction topic for validation. Keying by txid pins the tx
	// to one Kafka partition, so any re-submission (retry, user double-post)
	// lands on the same consumer — a future idempotency check can then see the
	// duplicate instead of having it fan out across replicas.
	//
	// Raw tx bytes travel as []byte in the JSON payload (encoded as base64 by
	// encoding/json) so the validator and propagator never hex-decode the body
	// and re-encode it downstream.
	txid := parsedTx.TxID().String()

	// Synchronous policy validation (skipFees=true, skipScripts=true) — fee and
	// script checks remain Teranode's job. Validation failure writes a
	// terminal REJECTED row to the store and returns 400 to the client
	// so the failure is durable AND immediate.
	if s.validator != nil {
		if err := s.validator.ValidateTransaction(c.Request.Context(), parsedTx, true, true); err != nil {
			s.rejectAtIntake(c.Request.Context(), txid, err.Error(), opts)
			c.JSON(http.StatusBadRequest, gin.H{
				jsonKeyError: "transaction failed validation",
				"reason":     err.Error(),
			})
			return
		}
	}

	// Dedup CAS via GetOrInsertStatus. Two submitters racing on the
	// same txid both attempt the insert; the loser sees inserted=false
	// and returns 202 idempotently without re-publishing.
	if s.store != nil {
		row := &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusReceived,
			Timestamp: time.Now(),
			// Carry the raw bytes on the status row so the propagation
			// reaper can rebroadcast txs that are stuck in non-terminal
			// states without re-fetching from Kafka or the API caller.
			RawTx: rawTx,
		}
		existing, inserted, dedupErr := s.store.GetOrInsertStatus(c.Request.Context(), row)
		switch {
		case dedupErr != nil:
			s.logger.Error("dedup CAS failed", zap.String("txid", txid), zap.Error(dedupErr))
			// Best-effort: continue with publish. The propagator's
			// in-flight set catches duplicates that slip past.
		case !inserted && existing != nil:
			// Idempotent re-submit: row already exists. Register the txid
			// with the in-process TxTracker using the persisted status so
			// bump-builder's tracked-only filtering recognizes it. Without
			// this, a re-submit after process restart leaves the tx
			// invisible to bump-builder and subsequent MINED/IMMUTABLE
			// transitions are silently dropped.
			if s.txTracker != nil {
				s.txTracker.Add(txid, existing.Status)
			}
			s.recordSubmission(c.Request.Context(), txid, opts)
			c.JSON(http.StatusAccepted, gin.H{
				"status": "already submitted",
				"txid":   txid,
				"state":  string(existing.Status),
			})
			return
		}
	}

	// Register the tx with the in-process TxTracker so the bump-builder
	// recognizes it when its block is processed.
	if s.txTracker != nil {
		s.txTracker.Add(txid, models.StatusReceived)
	}

	// Record the callback subscription BEFORE publishing to Kafka so
	// any status events fired on this txid can find a matching row.
	s.recordSubmission(c.Request.Context(), txid, opts)

	msg := map[string]interface{}{
		"txid":        txid,
		"raw_tx":      rawTx,
		"input_txids": collectInputTXIDs(parsedTx),
	}
	if err := s.producer.Send(kafka.TopicPropagation, txid, msg); err != nil {
		if errors.Is(err, kafka.ErrBrokerBackpressure) {
			// Backpressure → shed load to the client. The tx was never queued,
			// so a retry is safe and is the contract the 503 expresses.
			s.logger.Warn("submit rejected: kafka backpressure", zap.String("txid", txid))
			c.Header("Retry-After", "1")
			c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "service overloaded, retry shortly"})
			return
		}
		s.logger.Error("failed to publish transaction", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to submit"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "submitted"})
}

// rejectAtIntake is the terminal-rejection counterpart to the intake
// success sequence. It persists a REJECTED row, records the submission
// so SSE/webhook can resolve the callback URL+token on delivery, then
// publishes the REJECTED status to TopicStatusUpdate so live
// subscribers see the terminal outcome. Every step is best-effort —
// the client has already received its 400 by the time this runs, so a
// store or publish failure does not change the HTTP outcome.
//
// Order matches the success path: persist row, then queue submission,
// then publish. Queuing the submission before the publish minimizes
// the window in which an SSE/webhook subscriber receives the event
// without a matching submission row (the recorder pool is async, so
// the window can't be eliminated entirely without making the row
// write synchronous — a trade-off the success path also accepts).
func (s *Server) rejectAtIntake(ctx context.Context, txid, reason string, opts submitOptions) {
	s.persistRejectedAtIntake(ctx, txid, reason)
	s.recordSubmission(ctx, txid, opts)
	if s.publisher == nil {
		return
	}
	status := &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusRejected,
		ExtraInfo: reason,
		Timestamp: time.Now(),
	}
	if err := s.publisher.Publish(ctx, status); err != nil {
		s.logger.Warn(
			"intake rejection publish failed",
			zap.String("txid", txid),
			zap.Error(err),
		)
	}
}

// persistRejectedAtIntake writes a terminal REJECTED row for a tx that
// failed validation at the intake handler. Best-effort: a write
// failure is logged but doesn't change the client response (the 400
// has already told them the tx was rejected). When the store is nil
// (test setups using struct-literal construction), this is a no-op.
func (s *Server) persistRejectedAtIntake(ctx context.Context, txid, reason string) {
	if s.store == nil {
		return
	}
	row := &models.TransactionStatus{
		TxID:      txid,
		Status:    models.StatusRejected,
		ExtraInfo: reason,
		Timestamp: time.Now(),
	}
	_, inserted, err := s.store.GetOrInsertStatus(ctx, row)
	if err != nil {
		s.logger.Warn(
			"intake rejection persist failed",
			zap.String("txid", txid),
			zap.Error(err),
		)
		return
	}
	if !inserted {
		if err := s.store.UpdateStatus(ctx, row); err != nil {
			s.logger.Warn(
				"intake rejection status update failed",
				zap.String("txid", txid),
				zap.Error(err),
			)
		}
	}
}

// handleSubmitTransactions accepts a batch of concatenated raw transactions.
// Mirrors handleSubmitTransaction's intake pipeline (parse → validate →
// dedup CAS → publish to TopicPropagation) for each tx in the batch. The
// publish is a single SendBatch so all txs in one HTTP request hit the
// in-memory broker as one fan-out. Each tx carries its input_txids so
// the propagation dispatcher can detect parent-child relationships and
// hold children until their parents have terminalized.
func (s *Server) handleSubmitTransactions(c *gin.Context) {
	if !strings.Contains(c.ContentType(), "octet-stream") {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "Content-Type must be application/octet-stream"})
		return
	}

	// SSRF guard: reject early so a hostile client posting a 256 MiB batch
	// with a banned callback host doesn't get to consume any ingress.
	opts := extractSubmitOptions(c)
	if !s.validateCallbackURL(c, opts.CallbackURL) {
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBatchBytes)

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to read body"})
		return
	}
	if len(body) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "empty body"})
		return
	}

	// Phase 1: parse the whole stream. Use the canonical TxID() (not a
	// hash of the wire bytes) so Extended Format submissions key the
	// same as the canonical txid the propagator broadcasts.
	type parsedItem struct {
		tx   *sdkTx.Transaction
		raw  []byte
		txid string
	}
	var parsed []parsedItem
	offset := 0
	for offset < len(body) {
		tx, bytesUsed, parseErr := sdkTx.NewTransactionFromStream(body[offset:])
		if parseErr != nil {
			s.logger.Error(
				"failed to parse transaction in batch",
				zap.Int("offset", offset),
				zap.Int("parsed", len(parsed)),
				zap.Error(parseErr),
			)
			c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "failed to parse transaction", "parsed": len(parsed)})
			return
		}
		if bytesUsed == 0 {
			break
		}
		parsed = append(parsed, parsedItem{
			tx:   tx,
			raw:  body[offset : offset+bytesUsed],
			txid: tx.TxID().String(),
		})
		offset += bytesUsed
	}
	if len(parsed) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{jsonKeyError: "no transactions parsed"})
		return
	}

	// Phase 2: synchronous policy validation per tx. Any failure aborts
	// the whole batch with 400 — matches the single-tx contract. The
	// failed tx gets a durable REJECTED row before the response so
	// SSE/webhook subscribers can resolve the outcome.
	if s.validator != nil {
		ctx := c.Request.Context()
		for _, p := range parsed {
			if vErr := s.validator.ValidateTransaction(ctx, p.tx, true, true); vErr != nil {
				s.rejectAtIntake(ctx, p.txid, vErr.Error(), opts)
				c.JSON(http.StatusBadRequest, gin.H{
					jsonKeyError: "transaction failed validation",
					"txid":       p.txid,
					"reason":     vErr.Error(),
				})
				return
			}
		}
	}

	// Phase 3: dedup CAS per tx. Duplicates are skipped from the
	// publish but counted in the response so the caller can reconcile.
	// A dedup error logs but doesn't drop the tx — matches single-tx
	// best-effort behavior.
	toPublish := make([]parsedItem, 0, len(parsed))
	duplicates := 0
	if s.store != nil {
		ctx := c.Request.Context()
		for _, p := range parsed {
			row := &models.TransactionStatus{
				TxID:      p.txid,
				Status:    models.StatusReceived,
				Timestamp: time.Now(),
				// Carry the raw bytes on the row so the propagation
				// reaper can rebroadcast stuck txs without re-fetching.
				RawTx: p.raw,
			}
			existing, inserted, dedupErr := s.store.GetOrInsertStatus(ctx, row)
			switch {
			case dedupErr != nil:
				s.logger.Error("dedup CAS failed", zap.String("txid", p.txid), zap.Error(dedupErr))
				toPublish = append(toPublish, p)
			case !inserted && existing != nil:
				duplicates++
				// Idempotent re-submit: register the txid with the
				// in-process TxTracker using the persisted status so
				// bump-builder's tracked-only filtering recognizes it.
				// Mirrors the single-submit dedup branch (handleSubmitTransaction).
				if s.txTracker != nil {
					s.txTracker.Add(p.txid, existing.Status)
				}
				s.recordSubmission(ctx, p.txid, opts)
			default:
				toPublish = append(toPublish, p)
			}
		}
	} else {
		toPublish = parsed
	}

	// Register every accepted tx with the in-process TxTracker so the
	// bump-builder's filterTrackedTxids recognizes them when their block
	// is processed. Without this, tracked-only fan-out drops every MINED
	// transition and txs stay stuck at SEEN_ON_NETWORK forever.
	if s.txTracker != nil {
		for _, p := range toPublish {
			s.txTracker.Add(p.txid, models.StatusReceived)
		}
	}

	// Record subscriptions BEFORE publishing so any status events that
	// fire on these txids find a matching row.
	ctx := c.Request.Context()
	for _, p := range toPublish {
		s.recordSubmission(ctx, p.txid, opts)
	}

	// Phase 4: build propagation envelopes and publish as one batch.
	// input_txids drives the dispatcher's dep-aware admission — children
	// of any in-flight parent are held until the parent terminalizes.
	if len(toPublish) > 0 {
		msgs := make([]kafka.KeyValue, 0, len(toPublish))
		for _, p := range toPublish {
			msgs = append(msgs, kafka.KeyValue{
				Key: p.txid,
				Value: map[string]interface{}{
					"txid":        p.txid,
					"raw_tx":      p.raw,
					"input_txids": collectInputTXIDs(p.tx),
				},
			})
		}
		if err := s.producer.SendBatch(kafka.TopicPropagation, msgs); err != nil {
			if errors.Is(err, kafka.ErrBrokerBackpressure) {
				s.logger.Warn("batch submit rejected: kafka backpressure", zap.Int("count", len(msgs)))
				c.Header("Retry-After", "1")
				c.JSON(http.StatusServiceUnavailable, gin.H{jsonKeyError: "service overloaded, retry shortly"})
				return
			}
			s.logger.Error("failed to publish transaction batch", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{jsonKeyError: "failed to submit"})
			return
		}
	}

	c.JSON(http.StatusAccepted, gin.H{
		"submitted":  len(toPublish),
		"duplicates": duplicates,
		"total":      len(parsed),
	})
}
