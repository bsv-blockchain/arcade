package propagation

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// newTwoEndpointClient builds a teranode client that fans every broadcast
// out to both URLs, for mixed-peer verdict tests.
func newTwoEndpointClient(urlA, urlB string) *teranode.Client {
	return teranode.NewClient([]string{urlA, urlB}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
}

// The missing-parent safety net (#295). With arcade.propagation widened to
// multiple partitions, a child submitted in a later HTTP request than its
// parent can reach Teranode before the parent (different partition, different
// pod). Teranode has no orphan pool: the child draws TX_MISSING_PARENT.
// Pre-#295 any per-tx failure line terminalized as REJECTED — turning a pure
// ordering artifact into a permanent verdict. Per the #254 principle,
// missing-parent is a CONDITION, not a verdict: it routes to the bounded
// requeue and parks at PENDING_RETRY, and the only way it becomes REJECTED is
// arcade's own store showing an ancestor terminally REJECTED.

// --- fixtures ------------------------------------------------------------

// missingParentLineFor renders Teranode's deepest-cause missing-parent
// failure line: no [ProcessTransaction] wrapper, child txid first (inside
// the [Validate] wrapper), parent txid later in the message.
func missingParentLineFor(child, parent string) string {
	return fmt.Sprintf("TX_MISSING_PARENT (34): [Validate][%s] error getting parent transaction %s", child, parent)
}

// missingParentBody renders a /txs failure-list body from the given lines.
func missingParentBody(lines ...string) string {
	return "Failed to process transactions:\n" + strings.Join(lines, "\n") + "\n"
}

// missingParentServer answers every POST /txs with HTTP 422 and the given
// failure-list body, counting calls via log.
func missingParentServer(log *eventLog, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		log.add("broadcast")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(body))
	}))
}

// rejectedUpdates returns every REJECTED status write recorded by the store.
func rejectedUpdates(m *mockStore) []*models.TransactionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*models.TransactionStatus
	for _, u := range m.updates {
		if u.Status == models.StatusRejected {
			out = append(out, u)
		}
	}
	return out
}

// --- the line matcher ----------------------------------------------------

func TestLineIsMissingParentCondition(t *testing.T) {
	child := strings.Repeat("c", 64)
	parent := strings.Repeat("d", 64)
	cases := []struct {
		name string
		line string
		want bool
	}{
		{"deepest cause", missingParentLineFor(child, parent), true},
		{"wrapped in PROCESSING", "PROCESSING (4): [ProcessTransaction][" + child + "] failed to validate transaction: TX_MISSING_PARENT (34): missing parent tx", true},
		{"doubled prefix", "TX_MISSING_PARENT (34): TX_MISSING_PARENT (34): [Validate][" + child + "] error getting parent transaction " + parent, true},
		{"tx not found", "TX_NOT_FOUND (30): [Validate][" + child + "] UTXO not found for vout 0", true},
		{"conflict is a verdict", "UTXO_SPENT (70): " + parent + ":0 utxo already spent by tx " + child + "[0]", false},
		{"policy is a verdict", "TX_INVALID (31): [ProcessTransaction][" + child + "] transaction has no inputs", false},
		{"free text mentioning parent", "error getting parent transaction from cache", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lineIsMissingParentCondition(tc.line); got != tc.want {
				t.Errorf("lineIsMissingParentCondition(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

// --- broadcast routing ---------------------------------------------------

// TestBroadcast_MissingParentLine_RequeuesInsteadOfRejects is the core
// safety-net assertion: a child rejected by Teranode with TX_MISSING_PARENT
// (parent unknown to arcade's store too) must NOT terminalize REJECTED — it
// requeues through the dispatcher for another attempt.
func TestBroadcast_MissingParentLine_RequeuesInsteadOfRejects(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)

	log := &eventLog{}
	srv := missingParentServer(log, missingParentBody(missingParentLineFor(childTxid, parentTxid)))
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 5, time.Millisecond)

	if err := handleAndFlush(t, p, realPropMsg(t, childTxid, childRaw)); err != nil {
		t.Fatalf("handleAndFlush: %v", err)
	}

	if got := rejectedUpdates(ms); len(got) != 0 {
		t.Fatalf("missing-parent must not terminalize REJECTED, got %d REJECTED writes (first: %+v)", len(got), got[0])
	}
	// The requeue must land back on the dispatcher (retry pending).
	waitForPending(t, p, 1)
}

// TestBroadcast_MissingParentLine_SiblingsKeepImplicitAccepts pins that a
// missing-parent line keyed to a submitted tx is NOT an alien line: the
// peer processed the batch, so every tx absent from the failure list keeps
// its implicit acceptance.
func TestBroadcast_MissingParentLine_SiblingsKeepImplicitAccepts(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)
	confirmedSource := strings.Repeat("b", 64)
	siblingTxid, siblingRaw := spendingTx(t, confirmedSource, 0, 2)

	log := &eventLog{}
	srv := missingParentServer(log, missingParentBody(missingParentLineFor(childTxid, parentTxid)))
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 5, time.Millisecond)

	// Admit both, then flush once so they share a single /txs POST.
	if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, childTxid, childRaw))); err != nil {
		t.Fatalf("handleMessage child: %v", err)
	}
	if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, siblingTxid, siblingRaw))); err != nil {
		t.Fatalf("handleMessage sibling: %v", err)
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushSync: %v", err)
	}

	if got := ms.lastUpdateForTxid(siblingTxid); got == nil || got.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("sibling absent from the failure list must keep its implicit accept, got %+v", got)
	}
	if got := rejectedUpdates(ms); len(got) != 0 {
		t.Fatalf("no REJECTED writes expected, got %+v", got[0])
	}
	waitForPending(t, p, 1) // the child requeues
}

// TestBroadcast_MissingParentPlusRealVerdict_VerdictWins pins precedence: a
// genuine verdict from any peer (TX_INVALID from peer B) beats the
// missing-parent condition from peer A — the tx terminalizes REJECTED with
// the real verdict.
func TestBroadcast_MissingParentPlusRealVerdict_VerdictWins(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)

	log := &eventLog{}
	mpSrv := missingParentServer(log, missingParentBody(missingParentLineFor(childTxid, parentTxid)))
	defer mpSrv.Close()
	invalidLine := "TX_INVALID (31): [ProcessTransaction][" + childTxid + "] transaction invalid"
	invalidSrv := missingParentServer(log, missingParentBody(invalidLine))
	defer invalidSrv.Close()

	ms := newMockStore()
	p := poisonPropagator(mpSrv.URL, ms, 5, time.Millisecond)
	p.teranodeClient = newTwoEndpointClient(mpSrv.URL, invalidSrv.URL)

	if err := handleAndFlush(t, p, realPropMsg(t, childTxid, childRaw)); err != nil {
		t.Fatalf("handleAndFlush: %v", err)
	}

	got := rejectedUpdates(ms)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 REJECTED write (the real TX_INVALID verdict), got %d", len(got))
	}
	if !strings.Contains(got[0].ExtraInfo, "TX_INVALID") {
		t.Errorf("REJECTED ExtraInfo = %q, want the TX_INVALID verdict", got[0].ExtraInfo)
	}
}

// TestBroadcast_MissingParentPlusAccept_AcceptWins pins sticky acceptance:
// any peer 200 accepts the tx regardless of another peer's missing-parent.
func TestBroadcast_MissingParentPlusAccept_AcceptWins(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)

	log := &eventLog{}
	mpSrv := missingParentServer(log, missingParentBody(missingParentLineFor(childTxid, parentTxid)))
	defer mpSrv.Close()
	okSrv := newTeranodeServer(log, http.StatusOK)
	defer okSrv.Close()

	ms := newMockStore()
	p := poisonPropagator(mpSrv.URL, ms, 5, time.Millisecond)
	p.teranodeClient = newTwoEndpointClient(mpSrv.URL, okSrv.URL)

	if err := handleAndFlush(t, p, realPropMsg(t, childTxid, childRaw)); err != nil {
		t.Fatalf("handleAndFlush: %v", err)
	}

	if got := ms.lastUpdateForTxid(childTxid); got == nil || got.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("a peer 200 must win over another peer's missing-parent, got %+v", got)
	}
}

// --- store-consult cascade ----------------------------------------------

// TestBroadcast_MissingParent_AncestorRejectedInStore_CascadesRejected pins
// the one legitimate path from missing-parent to REJECTED: arcade's own
// store says a parent is terminally REJECTED, so the child inherits the
// outcome with the standard cascade reason, and the dispatcher releases the
// child's offset (commit watermark advances).
func TestBroadcast_MissingParent_AncestorRejectedInStore_CascadesRejected(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)

	log := &eventLog{}
	srv := missingParentServer(log, missingParentBody(missingParentLineFor(childTxid, parentTxid)))
	defer srv.Close()

	ms := newMockStore()
	ms.setStatus(parentTxid, models.StatusRejected)
	p := poisonPropagator(srv.URL, ms, 5, time.Millisecond)

	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 41, Value: realPropMsg(t, childTxid, childRaw)}
	waitForMark(t, claim, 41, "a store-cascaded REJECTED must release the child's offset")

	got := rejectedUpdates(ms)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 REJECTED write for the child, got %d", len(got))
	}
	if got[0].TxID != childTxid {
		t.Errorf("REJECTED txid = %s, want child %s", got[0].TxID, childTxid)
	}
	if !strings.HasPrefix(got[0].ExtraInfo, "parent rejected (ancestor "+parentTxid+")") {
		t.Errorf("ExtraInfo = %q, want the standard cascade reason naming ancestor %s", got[0].ExtraInfo, parentTxid)
	}
}

// TestBroadcast_MissingParent_AncestorNonTerminal_Requeues pins the
// conservative side: a parent at RECEIVED (or PENDING_RETRY, or unknown)
// is NOT a verdict about the child — requeue, never REJECTED.
func TestBroadcast_MissingParent_AncestorNonTerminal_Requeues(t *testing.T) {
	for _, parentStatus := range []models.Status{models.StatusReceived, models.StatusPendingRetry} {
		t.Run(string(parentStatus), func(t *testing.T) {
			parentTxid := strings.Repeat("a", 64)
			childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)

			log := &eventLog{}
			srv := missingParentServer(log, missingParentBody(missingParentLineFor(childTxid, parentTxid)))
			defer srv.Close()

			ms := newMockStore()
			ms.setStatus(parentTxid, parentStatus)
			p := poisonPropagator(srv.URL, ms, 5, time.Millisecond)

			if err := handleAndFlush(t, p, realPropMsg(t, childTxid, childRaw)); err != nil {
				t.Fatalf("handleAndFlush: %v", err)
			}
			if got := rejectedUpdates(ms); len(got) != 0 {
				t.Fatalf("parent %s must not condemn the child, got REJECTED write %+v", parentStatus, got[0])
			}
			waitForPending(t, p, 1)
		})
	}
}

// --- budget exhaustion ---------------------------------------------------

// TestRunDispatcher_MissingParentExhaustsBudget_ParksPendingRetry pins the
// no-wedge invariant: a child whose parent never shows up burns its requeue
// budget, parks at PENDING_RETRY (NOT REJECTED — the reaper owns durable
// retry from there), names the condition in ExtraInfo, and releases its
// Kafka offset so the partition's watermark advances.
func TestRunDispatcher_MissingParentExhaustsBudget_ParksPendingRetry(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)

	log := &eventLog{}
	srv := missingParentServer(log, missingParentBody(missingParentLineFor(childTxid, parentTxid)))
	defer srv.Close()

	ms := newMockStore()
	const maxAttempts = 2
	p := poisonPropagator(srv.URL, ms, maxAttempts, time.Millisecond)

	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 51, Value: realPropMsg(t, childTxid, childRaw)}
	waitForMark(t, claim, 51, "an exhausted missing-parent child must park and release its offset")

	if got := rejectedUpdates(ms); len(got) != 0 {
		t.Fatalf("missing-parent exhaustion must park, not reject; got %+v", got[0])
	}
	parked := ms.lastUpdateForTxid(childTxid)
	if parked == nil || parked.Status != models.StatusPendingRetry {
		t.Fatalf("child must end at PENDING_RETRY, got %+v", parked)
	}
	if !strings.Contains(parked.ExtraInfo, "TX_MISSING_PARENT") {
		t.Errorf("park reason %q should name the missing-parent condition", parked.ExtraInfo)
	}
	// 1 initial broadcast + maxAttempts requeued broadcasts.
	if got := log.count("broadcast"); got != 1+maxAttempts {
		t.Errorf("broadcast count = %d, want %d (bounded by the requeue budget)", got, 1+maxAttempts)
	}
}

// --- reaper path ---------------------------------------------------------

// reaperChild builds a stale PENDING_RETRY child row (real bytes so
// rejectedAncestor can parse the parent) and a propagator wired to reap it.
func reaperChild(t *testing.T, srvURL string, ms *mockStore, parentTxid string) (childTxid string) {
	t.Helper()
	childTxid, childRaw := spendingTx(t, parentTxid, 0, 1)
	ms.replayRows = []*models.TransactionStatus{{
		TxID:      childTxid,
		Status:    models.StatusPendingRetry,
		RawTx:     childRaw,
		Timestamp: time.Now().Add(-2 * time.Hour),
	}}
	return childTxid
}

// TestReapOnce_MissingParent_LeavesRowForNextTick is the regression test
// for a latent pre-#295 bug: a reaper rebroadcast of a child whose parent
// is still in flight (e.g. also PENDING_RETRY) drew TX_MISSING_PARENT and
// falsely terminalized the child REJECTED. The row must stay untouched so
// the next tick retries after the parent lands.
func TestReapOnce_MissingParent_LeavesRowForNextTick(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	ms := newMockStore()

	log := &eventLog{}
	var childTxid string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		log.add("broadcast")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(missingParentBody(missingParentLineFor(childTxid, parentTxid))))
	}))
	defer srv.Close()

	childTxid = reaperChild(t, srv.URL, ms, parentTxid)
	p := poisonPropagator(srv.URL, ms, 5, time.Millisecond)

	p.reapOnce(context.Background())

	if got := rejectedUpdates(ms); len(got) != 0 {
		t.Fatalf("reaper missing-parent must leave the row for the next tick, got REJECTED %+v", got[0])
	}
	if got := ms.lastUpdateForTxid(childTxid); got != nil {
		t.Fatalf("no status write expected for the child, got %+v", got)
	}
}

// TestReapOnce_MissingParent_AncestorRejected_CascadesRejected closes the
// durable loop: once the parent's row reads REJECTED, the reaper's next
// rebroadcast of the parked child terminalizes it with the cascade reason.
func TestReapOnce_MissingParent_AncestorRejected_CascadesRejected(t *testing.T) {
	parentTxid := strings.Repeat("a", 64)
	ms := newMockStore()
	ms.setStatus(parentTxid, models.StatusRejected)

	log := &eventLog{}
	var childTxid string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		log.add("broadcast")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(missingParentBody(missingParentLineFor(childTxid, parentTxid))))
	}))
	defer srv.Close()

	childTxid = reaperChild(t, srv.URL, ms, parentTxid)
	p := poisonPropagator(srv.URL, ms, 5, time.Millisecond)

	p.reapOnce(context.Background())

	got := rejectedUpdates(ms)
	if len(got) != 1 || got[0].TxID != childTxid {
		t.Fatalf("expected the child to cascade REJECTED via the store consult, got %+v", got)
	}
	if !strings.HasPrefix(got[0].ExtraInfo, "parent rejected (ancestor "+parentTxid+")") {
		t.Errorf("ExtraInfo = %q, want the standard cascade reason", got[0].ExtraInfo)
	}
}

// --- config wiring -------------------------------------------------------

// TestRetryBackoffMs_WiredIntoRequeueDelay pins that the (previously dead)
// propagation.retry_backoff_ms knob now drives the flat requeue delay, with
// non-positive values falling back to the 2s default.
func TestRetryBackoffMs_WiredIntoRequeueDelay(t *testing.T) {
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.RetryBackoffMs = 1234
	tc := teranode.NewClient([]string{"http://127.0.0.1:1"}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, newMockStore(), nil, tc, nil)
	if p.requeueDelay != 1234*time.Millisecond {
		t.Errorf("requeueDelay = %v, want 1234ms from retry_backoff_ms", p.requeueDelay)
	}

	cfg2 := &config.Config{}
	cfg2.Propagation.MerkleConcurrency = 10
	p2 := New(cfg2, zap.NewNop(), nil, nil, newMockStore(), nil, tc, nil)
	if p2.requeueDelay != defaultRequeueDelay {
		t.Errorf("requeueDelay = %v, want default %v when retry_backoff_ms unset", p2.requeueDelay, defaultRequeueDelay)
	}
}
