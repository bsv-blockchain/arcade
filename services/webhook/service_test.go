package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// fakeStore implements just enough of store.Store for these tests.
type fakeStore struct {
	mu   sync.Mutex
	subs map[string][]*models.Submission

	deliveries []deliveryRecord
}

type deliveryRecord struct {
	SubmissionID string
	LastStatus   models.Status
	RetryCount   int
	NextRetryAt  *time.Time
}

func (s *fakeStore) GetSubmissionsByTxID(_ context.Context, txid string) ([]*models.Submission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs[txid], nil
}

func (s *fakeStore) UpdateDeliveryStatus(_ context.Context, id string, last models.Status, retry int, nextRetry *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliveries = append(s.deliveries, deliveryRecord{
		SubmissionID: id,
		LastStatus:   last,
		RetryCount:   retry,
		NextRetryAt:  nextRetry,
	})
	// Mutate the in-memory submission so subsequent lookups reflect retry
	// state — mirrors what a real store would do.
	for _, list := range s.subs {
		for _, sub := range list {
			if sub.SubmissionID == id {
				sub.LastDeliveredStatus = last
				sub.RetryCount = retry
				sub.NextRetryAt = nextRetry
			}
		}
	}
	return nil
}

// Stubs to satisfy the store.Store interface without referencing the full set.
func (s *fakeStore) GetOrInsertStatus(context.Context, *models.TransactionStatus) (*models.TransactionStatus, bool, error) {
	return nil, false, nil
}

func (s *fakeStore) BatchGetOrInsertStatus(context.Context, []*models.TransactionStatus) ([]store.BatchInsertResult, error) {
	return nil, nil
}
func (s *fakeStore) UpdateStatus(context.Context, *models.TransactionStatus) error { return nil }
func (s *fakeStore) BatchUpdateStatus(context.Context, []*models.TransactionStatus) error {
	return nil
}

//nolint:nilnil // unused stub; safe to return the zero pair.
func (s *fakeStore) GetStatus(context.Context, string) (*models.TransactionStatus, error) {
	return nil, nil
}

func (s *fakeStore) GetStatusesSince(context.Context, time.Time) ([]*models.TransactionStatus, error) {
	return nil, nil
}

func (s *fakeStore) IterateStatusesSince(context.Context, time.Time, func(*models.TransactionStatus) error) error {
	return nil
}

func (s *fakeStore) SetStatusByBlockHash(context.Context, string, models.Status) ([]string, error) {
	return nil, nil
}
func (s *fakeStore) InsertBUMP(context.Context, string, uint64, []byte) error { return nil }
func (s *fakeStore) GetBUMP(context.Context, string) (uint64, []byte, error)  { return 0, nil, nil }
func (s *fakeStore) SetMinedByTxIDs(context.Context, string, uint64, []string) ([]*models.TransactionStatus, error) {
	return nil, nil
}
func (s *fakeStore) InsertSubmission(context.Context, *models.Submission) error { return nil }
func (s *fakeStore) GetSubmissionsByToken(context.Context, string) ([]*models.Submission, error) {
	return nil, nil
}
func (s *fakeStore) InsertStump(context.Context, *models.Stump) error { return nil }
func (s *fakeStore) GetStumpsByBlockHash(context.Context, string) ([]*models.Stump, error) {
	return nil, nil
}
func (s *fakeStore) DeleteStumpsByBlockHash(context.Context, string) error { return nil }
func (s *fakeStore) BumpRetryCount(context.Context, string) (int, error)   { return 0, nil }
func (s *fakeStore) SetPendingRetryFields(context.Context, string, []byte, time.Time) error {
	return nil
}

func (s *fakeStore) GetReadyRetries(context.Context, time.Time, int) ([]*store.PendingRetry, error) {
	return nil, nil
}

func (s *fakeStore) ClearRetryState(context.Context, string, models.Status, string) error {
	return nil
}
func (s *fakeStore) EnsureIndexes() error { return nil }
func (s *fakeStore) UpsertDatahubEndpoint(context.Context, store.DatahubEndpoint) error {
	return nil
}

func (s *fakeStore) ListDatahubEndpoints(context.Context, string) ([]store.DatahubEndpoint, error) {
	return nil, nil
}
func (s *fakeStore) Close() error { return nil }

// recordingPub captures published statuses but doesn't actually subscribe —
// the webhook tests drive handleUpdate directly.
type recordingPub struct{}

func (recordingPub) Publish(context.Context, *models.TransactionStatus) error { return nil }
func (recordingPub) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return nil, errors.New("not used in tests")
}
func (recordingPub) Close() error { return nil }

// scriptedPub serves a caller-supplied channel from Subscribe so tests can
// drive Service.Start with synthetic status updates and observe how the
// channel reader / worker pool route them downstream.
type scriptedPub struct {
	ch chan *models.TransactionStatus
}

func (p *scriptedPub) Publish(context.Context, *models.TransactionStatus) error { return nil }
func (p *scriptedPub) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return p.ch, nil
}
func (p *scriptedPub) Close() error { return nil }

// TestDeliverSuccess covers the happy path: matching submission, terminal
// status, callback URL gets POSTed with the bearer token, store records
// LastDeliveredStatus.
func TestDeliverSuccess(t *testing.T) {
	var receivedAuth string
	var receivedBody []byte
	var hits atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			"txA": {{
				SubmissionID:  "sub-1",
				TxID:          "txA",
				CallbackURL:   srv.URL,
				CallbackToken: "tok-A",
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1},
		// httptest.Server listens on 127.0.0.1 — opt into private dials so
		// the SSRF guard doesn't block the test client.
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      "txA",
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	if hits.Load() != 1 {
		t.Fatalf("expected 1 callback hit, got %d", hits.Load())
	}
	if receivedAuth != "Bearer tok-A" {
		t.Errorf("Authorization = %q, want %q", receivedAuth, "Bearer tok-A")
	}
	var payload map[string]any
	if err := json.Unmarshal(receivedBody, &payload); err != nil {
		t.Fatalf("decoding callback body: %v", err)
	}
	if payload["txid"] != "txA" || payload["txStatus"] != string(models.StatusMined) {
		t.Errorf("unexpected payload: %+v", payload)
	}
	if len(st.deliveries) != 1 || st.deliveries[0].LastStatus != models.StatusMined {
		t.Errorf("expected one MINED delivery record, got %+v", st.deliveries)
	}
}

// TestSkipIntermediateWhenNotFullUpdates verifies the FullStatusUpdates
// gating: a SEEN_ON_NETWORK update should NOT fire a callback when the
// submission opted out of full updates.
func TestSkipIntermediateWhenNotFullUpdates(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			"txA": {{
				SubmissionID:      "sub-1",
				TxID:              "txA",
				CallbackURL:       srv.URL,
				FullStatusUpdates: false,
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      "txA",
		Status:    models.StatusSeenOnNetwork,
		Timestamp: time.Now(),
	})

	if hits.Load() != 0 {
		t.Fatalf("expected 0 hits for intermediate status, got %d", hits.Load())
	}
}

// TestRetryOnFailure asserts the failure path schedules a retry: RetryCount
// is incremented and NextRetryAt is in the future.
func TestRetryOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			"txA": {{SubmissionID: "sub-1", TxID: "txA", CallbackURL: srv.URL}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 5, InitialBackoffMs: 50, MaxBackoffMs: 1000},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st,
	)

	before := time.Now()
	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      "txA",
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	if len(st.deliveries) != 1 {
		t.Fatalf("expected 1 delivery record, got %d", len(st.deliveries))
	}
	d := st.deliveries[0]
	if d.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", d.RetryCount)
	}
	if d.NextRetryAt == nil || !d.NextRetryAt.After(before) {
		t.Errorf("NextRetryAt = %v, expected after %v", d.NextRetryAt, before)
	}
}

// TestDedupOnRepeatedStatus verifies that a status matching
// LastDeliveredStatus is suppressed (no second POST).
func TestDedupOnRepeatedStatus(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			"txA": {{
				SubmissionID:        "sub-1",
				TxID:                "txA",
				CallbackURL:         srv.URL,
				LastDeliveredStatus: models.StatusMined,
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3},
		config.CallbackConfig{AllowPrivateIPs: true},
		zap.NewNop(), recordingPub{}, st,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      "txA",
		Status:    models.StatusMined, // same as LastDeliveredStatus
		Timestamp: time.Now(),
	})

	if hits.Load() != 0 {
		t.Errorf("expected 0 hits (deduped), got %d", hits.Load())
	}
}

// TestSSRFGuardBlocksLoopbackDial confirms the dial-time SSRF guard:
// with AllowPrivateIPs=false (the default), a delivery whose target is
// 127.0.0.1 — i.e. an httptest.Server — is refused at dial time, the
// callback never reaches the server, and the failure is recorded as a
// retryable delivery (RetryCount bumped).
//
// This is the second layer of defense: registration-time validation
// catches IP-literal callback URLs, and this dial-time check catches
// the DNS-rebinding case where a hostname resolved to a private IP.
func TestSSRFGuardBlocksLoopbackDial(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &fakeStore{
		subs: map[string][]*models.Submission{
			"txA": {{
				SubmissionID: "sub-1",
				TxID:         "txA",
				CallbackURL:  srv.URL, // 127.0.0.1:<port>
			}},
		},
	}
	svc := New(
		config.WebhookConfig{HTTPTimeoutMs: 1000, MaxRetries: 3, InitialBackoffMs: 1, MaxBackoffMs: 100},
		// Default-safe: SSRF guard ON.
		config.CallbackConfig{AllowPrivateIPs: false},
		zap.NewNop(), recordingPub{}, st,
	)

	svc.handleUpdate(t.Context(), &models.TransactionStatus{
		TxID:      "txA",
		Status:    models.StatusMined,
		Timestamp: time.Now(),
	})

	if hits.Load() != 0 {
		t.Errorf("expected 0 hits (dial refused), got %d", hits.Load())
	}
	if len(st.deliveries) != 1 {
		t.Fatalf("expected 1 delivery record (retry scheduled), got %d", len(st.deliveries))
	}
	if st.deliveries[0].RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", st.deliveries[0].RetryCount)
	}
}

// TestStartDecouplesSlowDelivery is the regression test for the worker-pool
// fix. The previous implementation called handleUpdate (synchronous DB
// lookup + synchronous HTTP POST) on the channel-reader goroutine, so a
// single slow callback target stalled draining of the upstream
// events.Publisher subscriber channel and triggered drops there. The fix
// hands each status to a bounded worker pool; the reader stays drainable
// while workers are blocked on slow targets.
//
// The test stalls one delivery (server never responds, only ctx-cancel
// unblocks it) and asserts that subsequent statuses still reach
// handleUpdate via other workers — i.e. the reader didn't block on the
// stalled delivery.
func TestStartDecouplesSlowDelivery(t *testing.T) {
	stallStarted := make(chan struct{}, 1)
	releaseStall := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Notify on first hit, then block until releaseStall fires or the
		// request context cancels (which will happen at test cleanup).
		select {
		case stallStarted <- struct{}{}:
		default:
		}
		select {
		case <-releaseStall:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(releaseStall)

	var fastHits atomic.Int32
	fastSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fastHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer fastSrv.Close()

	// One stalled submission plus several distinct fast submissions: each
	// fast tx has its own LastDeliveredStatus so shouldDeliver doesn't
	// dedup them away after the first success.
	st := &fakeStore{
		subs: map[string][]*models.Submission{
			"txStall": {{SubmissionID: "stall", TxID: "txStall", CallbackURL: srv.URL}},
			"txFast0": {{SubmissionID: "fast0", TxID: "txFast0", CallbackURL: fastSrv.URL}},
			"txFast1": {{SubmissionID: "fast1", TxID: "txFast1", CallbackURL: fastSrv.URL}},
			"txFast2": {{SubmissionID: "fast2", TxID: "txFast2", CallbackURL: fastSrv.URL}},
			"txFast3": {{SubmissionID: "fast3", TxID: "txFast3", CallbackURL: fastSrv.URL}},
		},
	}

	pub := &scriptedPub{ch: make(chan *models.TransactionStatus, 16)}

	// Pool of 2 workers: enough to keep one stalled delivery from blocking
	// every worker, while still small enough to make the test fast.
	svc := New(
		config.WebhookConfig{
			HTTPTimeoutMs:           60_000, // long, so the stall actually stalls
			MaxRetries:              3,
			InitialBackoffMs:        1,
			MaxBackoffMs:            10,
			MaxConcurrentDeliveries: 2,
		},
		config.CallbackConfig{AllowPrivateIPs: true}, // server is on 127.0.0.1
		zap.NewNop(), pub, st,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErr := make(chan error, 1)
	go func() { startErr <- svc.Start(ctx) }()

	// First, send the stall-bound status. Wait until the stalled HTTP
	// handler has actually been entered — this proves a worker has picked
	// up the status and is blocked.
	pub.ch <- &models.TransactionStatus{TxID: "txStall", Status: models.StatusMined, Timestamp: time.Now()}
	select {
	case <-stallStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("stalled HTTP server was never hit; worker pool didn't pick up the status")
	}

	// With one worker stalled, send several fast-bound statuses. If the
	// channel reader were still synchronous (pre-fix), the second worker
	// would also be busy serving handleUpdate and the reader would
	// eventually block on the stalled HTTP call. With the worker-pool
	// decoupling, the reader stays free and the second worker handles
	// these promptly.
	for i := 0; i < 4; i++ {
		pub.ch <- &models.TransactionStatus{
			TxID:      fmt.Sprintf("txFast%d", i),
			Status:    models.StatusMined,
			Timestamp: time.Now(),
		}
	}

	// All four fast deliveries should land within a generous timeout.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fastHits.Load() >= 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := fastHits.Load(); got < 4 {
		t.Fatalf("fast deliveries got through: %d, want 4 — channel reader appears to be blocked by stalled delivery", got)
	}

	// Cleanly stop the service.
	cancel()
	select {
	case <-startErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after ctx cancel")
	}
}
