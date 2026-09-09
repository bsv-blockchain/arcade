package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// InsertSubmission implements store.Store; a duplicate submission id is a
// no-op, matching Postgres' ON CONFLICT DO NOTHING.
func (s *Store) InsertSubmission(ctx context.Context, sub *models.Submission) error {
	if sub == nil || sub.SubmissionID == "" {
		return errors.New("insert submission: empty submission id")
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now()
	}
	d := submissionDocFromModel(sub)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.subs.InsertOne(octx, d); err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("insert submission %s: %w", sub.SubmissionID, err)
	}
	return nil
}

// findSubmissions runs a bounded submissions query.
func (s *Store) findSubmissions(ctx context.Context, filter bson.D, opts *options.FindOptionsBuilder) ([]*models.Submission, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.subs.Find(qctx, filter, opts)
	if err != nil {
		return nil, err
	}
	var docs []submissionDoc
	if err := cur.All(qctx, &docs); err != nil {
		return nil, err
	}
	out := make([]*models.Submission, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.toModel())
	}
	return out, nil
}

// GetSubmissionsByTxID implements store.Store.
func (s *Store) GetSubmissionsByTxID(ctx context.Context, txid string) ([]*models.Submission, error) {
	out, err := s.findSubmissions(ctx, doc(kv(fTxID, txid)), options.Find())
	if err != nil {
		return nil, fmt.Errorf("get submissions by txid %s: %w", txid, err)
	}
	return out, nil
}

// GetSubmissionsByToken implements store.Store.
func (s *Store) GetSubmissionsByToken(ctx context.Context, callbackToken string) ([]*models.Submission, error) {
	out, err := s.findSubmissions(ctx, doc(kv(fCallbackToken, callbackToken)), options.Find())
	if err != nil {
		return nil, fmt.Errorf("get submissions by token: %w", err)
	}
	return out, nil
}

// tokenRow is the covered (txid, callback_token) projection.
type tokenRow struct {
	TxID  string `bson:"txid"`
	Token string `bson:"callback_token"`
}

var projTokenRow = doc(kv(fTxID, 1), kv(fCallbackToken, 1), kv(fID, 0))

// TokensForTxIDs implements store.Store from the txid side only, one
// covered index scan per chunk of txids. Tokens are deduplicated per txid in
// first-seen order; a txid with no token is absent from the result.
func (s *Store) TokensForTxIDs(ctx context.Context, txids []string) (map[string][]string, error) {
	out := make(map[string][]string, len(txids))
	for _, chunk := range chunks(dedupe(txids), inChunk) {
		filter := doc(kv(fTxID, doc(kv(opIn, chunk))), kv(fCallbackToken, doc(kv(opGt, ""))))
		qctx, cancel := s.queryCtx(ctx)
		cur, err := s.subs.Find(qctx, filter, options.Find().SetProjection(projTokenRow).SetHint(idxSubTxIDToken))
		if err != nil {
			cancel()
			return nil, fmt.Errorf("tokens for txids: %w", err)
		}
		var rows []tokenRow
		err = cur.All(qctx, &rows)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("tokens for txids: %w", err)
		}
		for _, r := range rows {
			if !containsString(out[r.TxID], r.Token) {
				out[r.TxID] = append(out[r.TxID], r.Token)
			}
		}
	}
	return out, nil
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// IterateStatusesByToken implements store.Store. The replay is bounded by
// the token's submission count before anything is materialized — past the
// bound it refuses with store.ErrReplayUnavailable, as the interface requires.
// Within it: a covered scan collects the distinct txids, their statuses are
// read in projected $in chunks, and the (small, filtered) result is sorted by
// timestamp and streamed.
func (s *Store) IterateStatusesByToken(ctx context.Context, callbackToken string, since time.Time, onlyStatuses []models.Status, fn func(*models.TransactionStatus) error) error {
	if callbackToken == "" {
		return nil
	}
	tokenFilter := doc(kv(fCallbackToken, callbackToken))
	octx, cancel := s.opCtx(ctx)
	n, err := s.subs.CountDocuments(octx, tokenFilter, options.Count().SetLimit(s.tokenReplayLimit+1).SetHint(idxSubTokenTxID))
	cancel()
	if err != nil {
		return fmt.Errorf("iterate statuses by token: count: %w", err)
	}
	if n > s.tokenReplayLimit {
		return fmt.Errorf("%w: token has more than %d submissions", store.ErrReplayUnavailable, s.tokenReplayLimit)
	}

	txids, err := s.tokenTxIDs(ctx, tokenFilter)
	if err != nil {
		return err
	}
	rows, err := s.projectedStatuses(ctx, txids, since, onlyStatuses)
	if err != nil {
		return err
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].Timestamp.Equal(rows[j].Timestamp) {
			return rows[i].Timestamp.Before(rows[j].Timestamp)
		}
		return rows[i].TxID < rows[j].TxID
	})
	for _, r := range rows {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// tokenTxIDs collects the distinct txids under a token via the covered
// {callback_token, txid} index.
func (s *Store) tokenTxIDs(ctx context.Context, tokenFilter bson.D) ([]string, error) {
	cur, err := s.subs.Find(ctx, tokenFilter, options.Find().
		SetProjection(doc(kv(fTxID, 1), kv(fID, 0))).SetHint(idxSubTokenTxID).SetBatchSize(s.cursorBatch()))
	if err != nil {
		return nil, fmt.Errorf("iterate statuses by token: submissions: %w", err)
	}
	defer closeCursor(ctx, cur)
	seen := make(map[string]struct{})
	var txids []string
	for cur.Next(ctx) {
		var r tokenRow
		if err := cur.Decode(&r); err != nil {
			return nil, fmt.Errorf("iterate statuses by token: decode: %w", err)
		}
		if _, dup := seen[r.TxID]; dup || r.TxID == "" {
			continue
		}
		seen[r.TxID] = struct{}{}
		txids = append(txids, r.TxID)
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("iterate statuses by token: submissions: %w", err)
	}
	return txids, nil
}

// projectedStatuses reads the streaming projection (txid, status, timestamp,
// block hash/height) for txids, applying the since / status filters
// server-side. Never raw_tx, never merkle enrichment.
func (s *Store) projectedStatuses(ctx context.Context, txids []string, since time.Time, only []models.Status) ([]*models.TransactionStatus, error) {
	names := make([]string, 0, len(only))
	for _, st := range only {
		names = append(names, string(st))
	}
	var rows []*models.TransactionStatus
	for _, chunk := range chunks(txids, inChunk) {
		filter := doc(kv(fID, doc(kv(opIn, chunk))))
		if !since.IsZero() {
			filter = append(filter, kv(fTimestamp, doc(kv(opGt, since))))
		}
		if len(names) > 0 {
			filter = append(filter, kv(fStatus, doc(kv(opIn, names))))
		}
		cur, err := s.tx.Find(ctx, filter, options.Find().SetProjection(projTokenReplay))
		if err != nil {
			return nil, fmt.Errorf("iterate statuses by token: statuses: %w", err)
		}
		var docs []txDoc
		if err := cur.All(ctx, &docs); err != nil {
			return nil, fmt.Errorf("iterate statuses by token: statuses: %w", err)
		}
		for _, d := range docs {
			rows = append(rows, &models.TransactionStatus{
				TxID: d.TxID, Status: models.Status(d.Status), Timestamp: d.Timestamp,
				BlockHash: d.BlockHash, BlockHeight: heightFromInt64(d.BlockHeight),
			})
		}
	}
	return rows, nil
}

// UpdateDeliveryStatus implements store.Store; a nil nextRetry clears the
// field. Missing rows are a no-op.
func (s *Store) UpdateDeliveryStatus(ctx context.Context, submissionID string, lastStatus models.Status, retryCount int, nextRetry *time.Time) error {
	set := doc(kv(fLastDeliveredStatus, string(lastStatus)), kv(fRetryCount, retryCount))
	update := doc()
	if nextRetry != nil {
		set = append(set, kv(fNextRetryAt, msTrunc(*nextRetry)))
	} else {
		update = append(update, kv(opUnset, doc(kv(fNextRetryAt, ""))))
	}
	update = append(update, kv(opSet, set))
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.subs.UpdateOne(octx, idFilter(submissionID), update); err != nil {
		return fmt.Errorf("update delivery %s: %w", submissionID, err)
	}
	return nil
}

// RecordDeliveryAttempt implements store.Store: $inc attempts, overwrite the
// last-attempt bookkeeping. Missing rows are a no-op.
func (s *Store) RecordDeliveryAttempt(ctx context.Context, submissionID string, at time.Time, result string) error {
	update := doc(
		kv(opInc, doc(kv(fAttempts, 1))),
		kv(opSet, doc(kv(fLastAttemptAt, msTrunc(at)), kv(fLastResult, result))),
	)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.subs.UpdateOne(octx, idFilter(submissionID), update); err != nil {
		return fmt.Errorf("record delivery attempt %s: %w", submissionID, err)
	}
	return nil
}

// UpdateDeliveryStatusCAS implements store.Store as one conditional update.
// An empty expected matches both an absent field and an explicit "" — the
// never-delivered row — like the Postgres NULL-or-” predicate. Infrastructure
// errors are returned and counted separately from a lost CAS so a backend
// failing every write cannot hide behind a flat "lost" metric.
func (s *Store) UpdateDeliveryStatusCAS(ctx context.Context, submissionID string, expected, next models.Status) (bool, error) {
	var expect any = string(expected)
	if expected == "" {
		expect = doc(kv(opIn, bson.A{"", nil}))
	}
	filter := doc(kv(fID, submissionID), kv(fLastDeliveredStatus, expect))
	update := doc(
		kv(opSet, doc(kv(fLastDeliveredStatus, string(next)), kv(fRetryCount, 0))),
		kv(opUnset, doc(kv(fNextRetryAt, ""))),
	)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	res, err := s.subs.UpdateOne(octx, filter, update)
	if err != nil {
		metrics.WebhookCASErrorTotal.Inc()
		return false, fmt.Errorf("update delivery cas %s: %w", submissionID, err)
	}
	return res.MatchedCount == 1, nil
}

// ListSubmissionsReadyForRetry implements store.Store via the partial
// {retry_count > 0} index, oldest backlog first.
func (s *Store) ListSubmissionsReadyForRetry(ctx context.Context, now time.Time, limit int) ([]*models.Submission, error) {
	if limit <= 0 {
		return nil, nil
	}
	filter := doc(kv(fRetryCount, doc(kv(opGt, 0))), kv(fNextRetryAt, doc(kv(opLte, now))))
	out, err := s.findSubmissions(ctx, filter, options.Find().
		SetSort(doc(kv(fNextRetryAt, 1))).SetLimit(int64(limit)).SetHint(idxSubRetryReady))
	if err != nil {
		return nil, fmt.Errorf("list submissions ready for retry: %w", err)
	}
	return out, nil
}
