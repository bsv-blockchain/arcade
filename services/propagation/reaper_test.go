package propagation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// newSelectiveMerkleServer returns a /watch stub that fails (400) registration
// for txids in `fail` and accepts (200) all others — used to drive partial
// merkle-registration failures.
func newSelectiveMerkleServer(fail map[string]bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID string `json:"txid"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if fail[req.TxID] {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
}

// markTsFor returns the ts of the most recent MarkRebroadcastByTxIDs call that
// included txid.
func (m *mockStore) markTsFor(txid string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.rebroadcastMarks) - 1; i >= 0; i-- {
		for _, t := range m.rebroadcastMarks[i] {
			if t == txid {
				return m.rebroadcastMarkTs[i], true
			}
		}
	}
	return time.Time{}, false
}

// seenRow builds a SEEN_MULTIPLE_NODES row eligible for the reaper: a non-empty
// raw_tx and a timestamp older than staleSeenOnNetworkAge but within
// staleScanLookback. lastRebroadcast is the row's last_rebroadcast_at (zero ==
// never rebroadcast).
func seenRow(txid string, age time.Duration, lastRebroadcast time.Time) *models.TransactionStatus {
	return &models.TransactionStatus{
		TxID:              txid,
		Status:            models.StatusSeenMultipleNodes,
		RawTx:             []byte{0xde, 0xad, 0xbe, 0xef},
		Timestamp:         time.Now().Add(-age),
		LastRebroadcastAt: lastRebroadcast,
	}
}

func (m *mockStore) allRebroadcastMarks() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, call := range m.rebroadcastMarks {
		out = append(out, call...)
	}
	return out
}

// TestReapOnce_UsesConfiguredBatchSize proves the reaper honors
// p.reaperBatchSize. Before the fix it always used a hardcoded 200, so with 10
// due rows and a batch of 4 we must see exactly 4 rebroadcast, not 10.
func TestReapOnce_UsesConfiguredBatchSize(t *testing.T) {
	log := &eventLog{}
	merkleSrv := newMerkleServer(log, 200)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, 200)
	defer teranodeSrv.Close()

	ms := newMockStore()
	for i := 0; i < 10; i++ {
		ms.replayRows = append(ms.replayRows, seenRow(txidN(i), 2*time.Hour, time.Time{}))
	}

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reaperBatchSize = 4
	p.reaperRebroadcastInterval = time.Hour

	p.reapOnce(context.Background())

	if got := log.count("register:"); got != 4 {
		t.Fatalf("expected exactly 4 rows rebroadcast (batch cap), got %d", got)
	}
	if got := len(ms.allRebroadcastMarks()); got != 4 {
		t.Fatalf("expected 4 txids marked, got %d", got)
	}
}

// TestReapOnce_SkipsRecentlyRebroadcast confirms the per-tx interval throttle:
// a row rebroadcast within reaperRebroadcastInterval is excluded; never-sent
// and long-ago-sent rows are included.
func TestReapOnce_SkipsRecentlyRebroadcast(t *testing.T) {
	log := &eventLog{}
	merkleSrv := newMerkleServer(log, 200)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, 200)
	defer teranodeSrv.Close()

	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		seenRow("never", 2*time.Hour, time.Time{}),                      // due
		seenRow("recent", 2*time.Hour, time.Now().Add(-30*time.Minute)), // throttled
		seenRow("longago", 2*time.Hour, time.Now().Add(-2*time.Hour)),   // due
	}

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reaperBatchSize = 10
	p.reaperRebroadcastInterval = time.Hour

	p.reapOnce(context.Background())

	if got := log.count("register:"); got != 2 {
		t.Fatalf("expected 2 due rows rebroadcast, got %d", got)
	}
	for _, txid := range ms.allRebroadcastMarks() {
		if txid == "recent" {
			t.Fatalf("recently-rebroadcast row should have been skipped, but it was marked")
		}
	}
}

// TestReapOnce_MarksRebroadcastOnAttempt confirms attempted txids are stamped,
// so an immediate second tick (within the interval) rebroadcasts nothing.
func TestReapOnce_MarksRebroadcastOnAttempt(t *testing.T) {
	log := &eventLog{}
	merkleSrv := newMerkleServer(log, 200)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, 200)
	defer teranodeSrv.Close()

	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		seenRow("a", 2*time.Hour, time.Time{}),
		seenRow("b", 2*time.Hour, time.Time{}),
	}

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reaperBatchSize = 10
	p.reaperRebroadcastInterval = time.Hour

	p.reapOnce(context.Background())
	if got := log.count("register:"); got != 2 {
		t.Fatalf("first tick: expected 2 rebroadcast, got %d", got)
	}

	p.reapOnce(context.Background())
	if got := log.count("register:"); got != 2 {
		t.Fatalf("second tick: expected no new rebroadcasts (still 2 total), got %d", got)
	}
}

// TestReapOnce_FairnessAcrossTicks is the core anti-starvation assertion: with
// a backlog larger than the batch size, successive ticks rebroadcast every row
// exactly once over the interval — no row is starved, none is re-sent twice.
func TestReapOnce_FairnessAcrossTicks(t *testing.T) {
	log := &eventLog{}
	merkleSrv := newMerkleServer(log, 200)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, 200)
	defer teranodeSrv.Close()

	const backlog = 5
	ms := newMockStore()
	for i := 0; i < backlog; i++ {
		ms.replayRows = append(ms.replayRows, seenRow(txidN(i), 2*time.Hour, time.Time{}))
	}

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reaperBatchSize = 2
	p.reaperRebroadcastInterval = time.Hour

	// 3 ticks of batch 2 covers a backlog of 5 (2+2+1).
	for i := 0; i < 3; i++ {
		p.reapOnce(context.Background())
	}

	marks := ms.allRebroadcastMarks()
	seen := make(map[string]int, backlog)
	for _, txid := range marks {
		seen[txid]++
	}
	if len(seen) != backlog {
		t.Fatalf("expected all %d rows rebroadcast within the interval, got %d distinct: %v", backlog, len(seen), seen)
	}
	for txid, n := range seen {
		if n != 1 {
			t.Fatalf("row %s rebroadcast %d times within one interval, want exactly 1", txid, n)
		}
	}
}

// TestReapOnce_RecoverableNotRejected confirms a transient broadcast failure
// (requeue) leaves the SEEN_* row non-terminal — the reaper must never
// auto-reject a recoverable tx. The row is still marked on attempt so it cedes
// its slot for one interval.
func TestReapOnce_RecoverableNotRejected(t *testing.T) {
	log := &eventLog{}
	merkleSrv := newMerkleServer(log, 200)
	defer merkleSrv.Close()
	// 500 with no Teranode failure-list body → whole-batch requeue.
	teranodeSrv := newTeranodeServer(log, 500)
	defer teranodeSrv.Close()

	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		seenRow("recoverable", 2*time.Hour, time.Time{}),
	}

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reaperBatchSize = 10
	p.reaperRebroadcastInterval = time.Hour

	p.reapOnce(context.Background())

	ms.mu.Lock()
	for _, u := range ms.updates {
		if u.TxID == "recoverable" && u.Status == models.StatusRejected {
			ms.mu.Unlock()
			t.Fatalf("recoverable tx was wrongly written REJECTED on a requeue")
		}
	}
	ms.mu.Unlock()

	if got := len(ms.allRebroadcastMarks()); got != 1 {
		t.Fatalf("expected the attempted tx to be marked even on requeue, got %d marks", got)
	}
}

// TestReapOnce_PartialRegistrationFailureMarksFailedSubset covers review #1:
// a tx that fails merkle /watch must still be stamped so it can't keep sorting
// first (NULL last_rebroadcast_at) and re-fill the front of every batch — but a
// FULL outage (nothing registered) marks nothing so it isn't penalized.
func TestReapOnce_PartialRegistrationFailureMarksFailedSubset(t *testing.T) {
	t.Run("partial failure marks the failed subset", func(t *testing.T) {
		merkleSrv := newSelectiveMerkleServer(map[string]bool{"poison": true})
		defer merkleSrv.Close()
		log := &eventLog{}
		teranodeSrv := newTeranodeServer(log, 200)
		defer teranodeSrv.Close()

		ms := newMockStore()
		ms.replayRows = []*models.TransactionStatus{
			seenRow("ok1", 2*time.Hour, time.Time{}),
			seenRow("poison", 2*time.Hour, time.Time{}),
			seenRow("ok2", 2*time.Hour, time.Time{}),
		}

		p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
		p.reaperBatchSize = 10
		p.reaperRebroadcastInterval = time.Hour
		p.reaperRequeueBackoff = time.Minute

		p.reapOnce(context.Background())

		if _, ok := ms.markTsFor("poison"); !ok {
			t.Fatalf("registration-failed tx 'poison' was not stamped; it would starve the front of the batch")
		}
		if _, ok := ms.markTsFor("ok1"); !ok {
			t.Fatalf("successfully broadcast tx 'ok1' was not stamped")
		}
	})

	t.Run("full outage marks nothing", func(t *testing.T) {
		merkleSrv := newSelectiveMerkleServer(map[string]bool{"a": true, "b": true})
		defer merkleSrv.Close()
		log := &eventLog{}
		teranodeSrv := newTeranodeServer(log, 200)
		defer teranodeSrv.Close()

		ms := newMockStore()
		ms.replayRows = []*models.TransactionStatus{
			seenRow("a", 2*time.Hour, time.Time{}),
			seenRow("b", 2*time.Hour, time.Time{}),
		}

		p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
		p.reaperBatchSize = 10
		p.reaperRebroadcastInterval = time.Hour
		p.reaperRequeueBackoff = time.Minute

		p.reapOnce(context.Background())

		if got := len(ms.allRebroadcastMarks()); got != 0 {
			t.Fatalf("a full registration outage must not stamp any row, got %d marks", got)
		}
	})
}

// TestReapOnce_RequeueUsesShortBackoff covers review #2: a transient requeue is
// stamped with a backdated ts so it becomes due again after reaperRequeueBackoff
// (~1 min), not the full reaperRebroadcastInterval (1h).
func TestReapOnce_RequeueUsesShortBackoff(t *testing.T) {
	log := &eventLog{}
	merkleSrv := newMerkleServer(log, 200)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, 500) // 500, no failure body → requeue
	defer teranodeSrv.Close()

	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		seenRow("requeueme", 2*time.Hour, time.Time{}),
	}

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reaperBatchSize = 10
	p.reaperRebroadcastInterval = time.Hour
	p.reaperRequeueBackoff = time.Minute

	before := time.Now()
	p.reapOnce(context.Background())

	ts, ok := ms.markTsFor("requeueme")
	if !ok {
		t.Fatalf("requeued tx was not stamped")
	}
	// Backdated to ≈ now - (interval - backoff) = now - 59m, so it must be far
	// behind `before`, not ≈now (which is what an accepted row would get).
	if !ts.Before(before.Add(-30 * time.Minute)) {
		t.Fatalf("requeued tx stamped at %v (vs now≈%v); expected a backdated (~59m-old) stamp for the short backoff", ts, before)
	}
}

func txidN(i int) string {
	const hexd = "0123456789abcdef"
	return "tx" + string(hexd[i%16]) + string(hexd[(i/16)%16])
}
