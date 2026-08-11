package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// newFanOutFixture builds a manager whose store has `txids` transactions, each
// registered under every token in `tokens`. Returns the manager, the store stub
// and a cancel for the manager goroutine.
func newFanOutFixture(t testing.TB, tokens, txids []string) (*Manager, *sseStoreStub, context.CancelFunc) {
	t.Helper()
	subsByToken := make(map[string][]*models.Submission, len(tokens))
	for _, token := range tokens {
		subs := make([]*models.Submission, 0, len(txids))
		for _, txid := range txids {
			subs = append(subs, &models.Submission{TxID: txid, CallbackToken: token})
		}
		subsByToken[token] = subs
	}
	st := &sseStoreStub{
		subsByToken: subsByToken,
		statusByTx:  map[string]*models.TransactionStatus{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
	if err != nil {
		cancel()
		t.Fatalf("newManager: %v", err)
	}
	return mgr, st, cancel
}

func makeTxIDs(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%06d", prefix, i)
	}
	return out
}

// TestFanOutResolvesTokensOncePerEvent is the regression test for the fan-out
// throughput ceiling.
//
// Fan-out used to ask the store "does this txid belong to this token" once per
// (event, CLIENT), so store round-trips scaled with the number of connected
// clients. On the manager's single fan-out goroutine, with a 0.583 ms probe
// against a 4.5M-row submissions table, that capped delivery at ~1.6k
// events/s while 1,000 TPS demanded ~4k.
//
// The invariant now: store round-trips scale with EVENTS, never with clients.
func TestFanOutResolvesTokensOncePerEvent(t *testing.T) {
	const events = 50
	tokens := []string{"tok-1", "tok-2", "tok-3", "tok-4"}
	txids := makeTxIDs("once", events)
	mgr, st, cancel := newFanOutFixture(t, tokens, txids)
	defer cancel()

	// Two clients per token: eight token-scoped consumers of every event.
	clients := make([]*Client, 0, 2*len(tokens))
	for range 2 {
		for _, token := range tokens {
			c := mgr.NewClient(token)
			mgr.Register(c)
			defer mgr.Unregister(c.ID)
			clients = append(clients, c)
		}
	}

	for _, txid := range txids {
		mgr.fanOut(context.Background(), &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now(),
		})
	}

	calls, resolved := st.storeCounters()
	if calls != events {
		t.Errorf("store round-trips = %d, want %d (one per EVENT, not per event×client)", calls, events)
	}
	if resolved != events {
		t.Errorf("txids resolved = %d, want %d", resolved, events)
	}
	// The saving must not have cost correctness: every client still gets
	// every event.
	for _, c := range clients {
		if got := len(c.Ch); got != events {
			t.Errorf("client %d (token %s) received %d frames, want %d", c.ID, c.Token, got, events)
		}
	}
}

// TestFanOutBulkResolvesWholeBatchInOneQuery covers the block-burst path. A
// bulk MINED event used to be unfanned into N synthetic per-tx statuses, each
// running a full fan-out pass with its own per-client probe — the reason block
// bursts pushed emit→applied p95 to 21.6 s. The whole txid list now resolves
// in one batch before the unfan loop.
func TestFanOutBulkResolvesWholeBatchInOneQuery(t *testing.T) {
	const batch = 500
	txids := makeTxIDs("bulk", batch)
	mgr, st, cancel := newFanOutFixture(t, []string{tokA}, txids)
	defer cancel()

	clients := make([]*Client, 0, 3)
	for range 3 {
		c := mgr.NewClient(tokA)
		mgr.Register(c)
		defer mgr.Unregister(c.ID)
		clients = append(clients, c)
	}

	mgr.fanOutBulk(context.Background(), &models.TransactionStatus{
		Status:      models.StatusMined,
		BlockHash:   "block-1",
		BlockHeight: 42,
		Timestamp:   time.Now(),
		TxIDs:       txids,
	})

	calls, resolved := st.storeCounters()
	if calls != 1 {
		t.Errorf("store round-trips for one bulk event = %d, want 1", calls)
	}
	if resolved != batch {
		t.Errorf("txids resolved = %d, want %d", resolved, batch)
	}
	for _, c := range clients {
		if got := len(c.Ch); got != batch {
			t.Errorf("client %d received %d frames, want %d", c.ID, got, batch)
		}
	}
}

// TestFanOutCachesTokenSetAcrossTransitions covers the LRU: a transaction
// emits several live transitions, and only the first may reach the store.
func TestFanOutCachesTokenSetAcrossTransitions(t *testing.T) {
	txid := "cached-tx"
	mgr, st, cancel := newFanOutFixture(t, []string{tokA}, []string{txid})
	defer cancel()

	c := mgr.NewClient(tokA)
	mgr.Register(c)
	defer mgr.Unregister(c.ID)

	hitsBefore := testutil.ToFloat64(metrics.SSETokenCacheTotal.WithLabelValues("hit"))
	transitions := []models.Status{
		models.StatusAcceptedByNetwork,
		models.StatusSeenOnNetwork,
		models.StatusSeenMultipleNodes,
	}
	for _, status := range transitions {
		mgr.fanOut(context.Background(), &models.TransactionStatus{
			TxID:      txid,
			Status:    status,
			Timestamp: time.Now(),
		})
	}

	if calls, _ := st.storeCounters(); calls != 1 {
		t.Errorf("store round-trips = %d, want 1 (later transitions served from cache)", calls)
	}
	if got := testutil.ToFloat64(metrics.SSETokenCacheTotal.WithLabelValues("hit")) - hitsBefore; got != float64(len(transitions)-1) {
		t.Errorf("cache hits = %v, want %d", got, len(transitions)-1)
	}
	if got := len(c.Ch); got != len(transitions) {
		t.Errorf("client received %d frames, want %d", got, len(transitions))
	}
}

// TestFanOutSkipsStoreWhenNoClientFiltersByToken guards the firehose case: an
// unfiltered consumer needs no membership answer, so the store must not be
// touched at all.
func TestFanOutSkipsStoreWhenNoClientFiltersByToken(t *testing.T) {
	mgr, st, cancel := newFanOutFixture(t, []string{tokA}, []string{"fire-1"})
	defer cancel()

	c := mgr.NewClient("") // firehose
	mgr.Register(c)
	defer mgr.Unregister(c.ID)

	mgr.fanOut(context.Background(), &models.TransactionStatus{
		TxID: "fire-1", Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
	})
	mgr.fanOutBulk(context.Background(), &models.TransactionStatus{
		Status: models.StatusMined, Timestamp: time.Now(), TxIDs: []string{"fire-1", "fire-2"},
	})

	if calls, _ := st.storeCounters(); calls != 0 {
		t.Errorf("store round-trips = %d, want 0 for a tokenless client", calls)
	}
	if got := len(c.Ch); got != 3 {
		t.Errorf("firehose client received %d frames, want 3", got)
	}
}

// TestFanOutRecordsObservability covers the signals that were missing when
// 1.6M drops were blamed on a "slow SSE client": a fan-out duration histogram
// and a connected-clients gauge.
func TestFanOutRecordsObservability(t *testing.T) {
	mgr, _, cancel := newFanOutFixture(t, []string{tokA}, []string{"obs-1"})
	defer cancel()

	c := mgr.NewClient(tokA)
	mgr.Register(c)
	if got := testutil.ToFloat64(metrics.SSEConnectedClients); got != 1 {
		t.Errorf("connected-clients gauge = %v after Register, want 1", got)
	}

	mgr.fanOut(context.Background(), &models.TransactionStatus{
		TxID: "obs-1", Status: models.StatusSeenOnNetwork, Timestamp: time.Now(),
	})
	if got := testutil.CollectAndCount(metrics.SSEFanoutDuration); got == 0 {
		t.Error("fan-out duration histogram has no series")
	}
	if mgr.fanoutEvents != 1 || mgr.fanoutEWMA <= 0 {
		t.Errorf("fan-out stats = (%d events, %v ewma), want a positive sample", mgr.fanoutEvents, mgr.fanoutEWMA)
	}

	mgr.Unregister(c.ID)
	if got := testutil.ToFloat64(metrics.SSEConnectedClients); got != 0 {
		t.Errorf("connected-clients gauge = %v after Unregister, want 0", got)
	}
}

// BenchmarkFanOut reports the store round-trips fan-out costs per event.
// queries/event is the number that mattered: it used to equal the connected
// token-client count, which is what made the serial fan-out goroutine the
// system's throughput ceiling. It is now 1 regardless of client count, and 0
// for events whose txid is already cached.
func BenchmarkFanOut(b *testing.B) {
	for _, clients := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("clients=%d", clients), func(b *testing.B) {
			txids := makeTxIDs("bench", b.N)
			tokens := make(map[string][]string, b.N)
			for _, txid := range txids {
				tokens[txid] = []string{tokA}
			}
			st := &tokenStoreStub{tokens: tokens}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mgr, err := newManager(ctx, &fakePublisher{}, st, zap.NewNop())
			if err != nil {
				b.Fatalf("newManager: %v", err)
			}
			for range clients {
				c := mgr.NewClient(tokA)
				// A generous buffer keeps the benchmark measuring fan-out
				// rather than the drop path.
				c.Ch = make(chan *models.TransactionStatus, b.N+1)
				mgr.Register(c)
			}

			status := &models.TransactionStatus{Status: models.StatusSeenOnNetwork, Timestamp: time.Now()}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				status.TxID = txids[i]
				mgr.fanOut(ctx, status)
			}
			b.StopTimer()
			b.ReportMetric(float64(st.calls)/float64(b.N), "queries/event")
		})
	}
}

// TestMidstreamCatchupRateMatchesLiveStream is the regression test for the
// drop→catchup→drop livelock.
//
// A mid-stream round runs on the connection's writer goroutine — the same
// goroutine that drains the client's send channel — so nothing consumes live
// frames while a round replays. With the old shared 10,000-frame budget
// against the default 8,192-slot buffer, a single capped round was guaranteed
// to overflow the buffer it was healing and re-arm its own drop flag. A
// production stream logged 34 rounds and 570 drop lines in ten minutes with
// the load generator stopped.
//
// The invariant now: a round is bounded by the client's buffer, and the live
// channel is drained between rounds.
func TestMidstreamCatchupRateMatchesLiveStream(t *testing.T) {
	const bufferSize = 512
	base := time.Now().UTC().Add(-time.Hour)
	// Three rounds' worth of replayable history.
	n := 3 * (bufferSize / 2)
	subs := make([]*models.Submission, 0, n)
	statusByTx := make(map[string]*models.TransactionStatus, n)
	for i := range n {
		txid := fmt.Sprintf("replay-%06d", i)
		subs = append(subs, &models.Submission{TxID: txid, CallbackToken: tokA})
		statusByTx[txid] = &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
		}
	}
	st := &sseStoreStub{
		subsByToken: map[string][]*models.Submission{tokA: subs},
		statusByTx:  statusByTx,
	}
	svc, _, _, cancel := setupSSEService(t, st)
	defer cancel()
	svc.manager.clientBufferSize = bufferSize

	client := svc.manager.NewClient(tokA)
	budget := midstreamRoundFrames(client)
	if budget > cap(client.Ch) {
		t.Fatalf("round budget %d exceeds the client buffer %d it must not outrun", budget, cap(client.Ch))
	}

	// A live frame is already queued when the healing starts — exactly the
	// state a real connection is in, since fan-out keeps producing.
	live := &models.TransactionStatus{TxID: "live-frame", Status: models.StatusSeenOnNetwork, Timestamp: time.Now().UTC()}
	client.Ch <- live
	client.droppedFloor.Store(base.UnixNano())
	client.dropped.Store(true)

	rec := httptest.NewRecorder()
	w := &sseWriter{w: rec, f: nopFlusher{rec}}
	if ok := svc.midstreamCatchup(t.Context(), w, client); !ok {
		t.Fatal("midstreamCatchup reported a dead connection")
	}

	body := rec.Body.String()
	livePos := strings.Index(body, fmt.Sprintf("id: %d\n", live.Timestamp.UnixNano()))
	if livePos < 0 || !strings.Contains(body, `"txid":"live-frame"`) {
		t.Fatal("live frame was never written: the replay loop starved the channel it was healing")
	}
	// The live frame is written by the first inter-round drain, so exactly one
	// round's budget of replay frames precedes it.
	if got := strings.Count(body[:livePos], "\nevent: status\n"); got != budget {
		t.Errorf("replay frames before the live frame = %d, want one round's budget %d", got, budget)
	}
	if got := strings.Count(body, "\nevent: status\n"); got != n+1 {
		t.Errorf("total frames = %d, want %d replayed + 1 live", got, n+1)
	}
	if len(client.Ch) != 0 {
		t.Errorf("%d live frames left buffered after catchup", len(client.Ch))
	}
}

// TestStatusFrameTimestampCarriesSubSecondPrecision covers the payload
// timestamp resolution. The frame `id` has always been nanoseconds; the
// payload was RFC3339 (whole seconds), so any consumer measuring latency from
// `data` saw up to ±1 s of error against the same frame's own id.
func TestStatusFrameTimestampCarriesSubSecondPrecision(t *testing.T) {
	ts := time.Unix(1700000000, 123456789).UTC()
	frame, err := buildStatusFrame(&models.TransactionStatus{
		TxID: "nanos", Status: models.StatusSeenOnNetwork, Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("buildStatusFrame: %v", err)
	}
	_, data, ok := strings.Cut(frame, "data: ")
	if !ok {
		t.Fatalf("no data field in frame %q", frame)
	}
	var payload struct {
		Timestamp string `json:"timestamp"`
	}
	if jsonErr := json.Unmarshal([]byte(strings.TrimSuffix(data, "\n\n")), &payload); jsonErr != nil {
		t.Fatalf("unmarshal data: %v", jsonErr)
	}
	got, err := time.Parse(time.RFC3339, payload.Timestamp)
	if err != nil {
		t.Fatalf("payload timestamp %q is not RFC 3339: %v", payload.Timestamp, err)
	}
	// The payload must agree with the frame id to the nanosecond.
	if !got.Equal(ts) {
		t.Errorf("payload timestamp = %s (%d ns), want %s (%d ns)",
			payload.Timestamp, got.UnixNano(), ts.Format(time.RFC3339Nano), ts.UnixNano())
	}
}

// TestStatusFrameTimestampWholeSecondsUnchanged is the compatibility half of
// the change: RFC3339Nano elides trailing zeros, so a whole-second timestamp
// still renders exactly as it did before.
func TestStatusFrameTimestampWholeSecondsUnchanged(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()
	frame, err := buildStatusFrame(&models.TransactionStatus{
		TxID: "whole", Status: models.StatusSeenOnNetwork, Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("buildStatusFrame: %v", err)
	}
	if want := `"timestamp":"2023-11-14T22:13:20Z"`; !strings.Contains(frame, want) {
		t.Errorf("frame %q does not contain %s", frame, want)
	}
}
