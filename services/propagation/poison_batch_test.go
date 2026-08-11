package propagation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// These tests reproduce the 2026-08-11 dev-ovh-1 propagation wedge (arcade
// v0.11.5) and pin both halves of the fix.
//
// The incident: Teranode answered a 21-tx batch of stale transactions with
// HTTP 409 and 21 conflict lines shaped
//
//	UTXO_SPENT (70): <outpoint_txid>:<vout> utxo already spent by tx <spender>[n]
//
// Those lines key under the SPENT OUTPOINT, never under a submitted txid (no
// [ProcessTransaction][<txid>] wrapper), so alienFailureLines correctly
// withheld implicit accepts (#292) — but with no attributable rejection
// either, every tx fell to the `default: txResultClassRequeue` arm and went
// round again into the identical 409. ~2,410 failures in two minutes, 42 of 43
// batches requeued, 4,679 txs stuck in the dispatcher's inFlight map pinning
// Kafka offsets, and 337,247 messages of lag on the single-partition
// arcade.propagation topic completely stationary while the leader pod burned
// 4.2 CPU cores. Nothing new could propagate.
//
// Fix 1 (attribution): the conflict line's outpoint is matched against the
// submitted transactions' own inputs, so the verdict lands on the tx that
// actually spends it — REJECTED / ARC 466 instead of an eternal requeue.
// Fix 2 (bounded requeue): anything still unattributable is parked at
// PENDING_RETRY after propagation.retry_max_attempts, which releases the
// dispatcher in-flight entry so the commit watermark can advance.

// --- fixtures ------------------------------------------------------------

// outpointRef renders an outpoint the way Teranode renders it inside a
// conflict-family failure line.
func outpointRef(txid string, vout uint32) string {
	return fmt.Sprintf("%s:%d", txid, vout)
}

// spendingTx builds a parseable transaction that spends exactly the named
// outpoint. nonce varies the locktime so two txs spending different outpoints
// (or the same one) still hash differently. Returns the display-form txid and
// the serialized bytes the propagation envelope carries.
//
// Real bytes matter here: outpoint attribution deliberately re-parses RawTx
// rather than trusting propagationMsg.InputTXIDs, because InputTXIDs carries
// parent txids only and an outpoint needs the vout to be unambiguous.
func spendingTx(t *testing.T, sourceTxIDHex string, vout, nonce uint32) (txid string, raw []byte) {
	t.Helper()
	source, err := chainhash.NewHashFromHex(sourceTxIDHex)
	if err != nil {
		t.Fatalf("NewHashFromHex(%s): %v", sourceTxIDHex, err)
	}
	tx := sdkTx.NewTransaction()
	tx.LockTime = nonce
	tx.Inputs = append(tx.Inputs, &sdkTx.TransactionInput{
		SourceTXID:       source,
		SourceTxOutIndex: vout,
		UnlockingScript:  &script.Script{},
		SequenceNumber:   0xffffffff,
	})
	return tx.TxID().String(), tx.Bytes()
}

// realPropMsg is makePropMsg with genuine transaction bytes, so the
// attribution path has inputs to match against.
func realPropMsg(t *testing.T, txid string, rawTx []byte) []byte {
	t.Helper()
	b, err := json.Marshal(propagationMsg{TXID: txid, RawTx: rawTx})
	if err != nil {
		t.Fatalf("marshal propagation msg: %v", err)
	}
	return b
}

// conflictBody renders the exact wrapper-less UTXO_SPENT body Teranode
// returned during the incident, one line per named outpoint.
func conflictBody(outpoints []string, spender string) string {
	var sb strings.Builder
	sb.WriteString("Failed to process transactions:\n")
	for i, op := range outpoints {
		fmt.Fprintf(&sb, "UTXO_SPENT (70): UTXO_SPENT (70): %s utxo already spent by tx %s[%d]\n", op, spender, i)
	}
	return sb.String()
}

// conflictServer answers every POST /txs with HTTP 409 and the supplied body,
// counting calls so a test can prove the retry loop is bounded.
func conflictServer(log *eventLog, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		log.add("broadcast")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(body))
	}))
}

// opaqueConflictBody is a 409 whose body does NOT match Teranode's failure-list
// shape, so parseTxsFailures returns nil and the peer contributes no per-tx
// information at all. This is the oldest form of the poison batch and it
// predates #292 entirely: teranode/client.go used to parse ONLY status 500, so
// every 409 produced failures = nil and the caller "treats the batch as a pure
// infra failure (whole batch requeued for another attempt)" — forever, if the
// 409 was permanent. #292 made the verdicts visible for the parseable shape;
// it neither created nor fixed the unbounded loop.
const opaqueConflictBody = "409 Conflict: transaction rejected\n"

// opaqueConflictServer answers every POST /txs with a permanent, unparseable
// 409. Batch-size independent, which keeps the bounded-requeue tests free of
// any dependency on how the dispatcher happened to group messages.
func opaqueConflictServer(log *eventLog) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		log.add("broadcast")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(opaqueConflictBody))
	}))
}

// waitForPending blocks until the dispatcher is holding at least want messages
// in its pending accumulator, i.e. a delayed requeue has landed and the next
// flush would pick it up.
func waitForPending(t *testing.T, p *Propagator, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.pendingDepth.Load() >= int64(want) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("dispatcher never accumulated %d pending msgs (have %d); the requeue never landed", want, p.pendingDepth.Load())
}

// poisonPropagator wires a Propagator with no merkle client (so registerBatch
// is a pass-through), a compressed requeue delay, and an explicit retry
// budget.
func poisonPropagator(teranodeURL string, st store.Store, maxAttempts int, delay time.Duration) *Propagator {
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.RetryMaxAttempts = maxAttempts
	tc := teranode.NewClient([]string{teranodeURL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, nil, st, nil, tc, nil)
	p.requeueDelay = delay
	return p
}

// --- fix 1: attribute conflict verdicts by outpoint ----------------------

// TestBroadcast_409ConflictLines_AttributedToSubmittedInputs is the incident
// shape at batch scale. Teranode 409s a multi-tx batch with one wrapper-less
// UTXO_SPENT line per genuinely double-spent tx, each keyed by the spent
// outpoint rather than by the submitted txid. Every named tx must terminalize
// REJECTED with the real UTXO_SPENT verdict and ARC 466 (StatusConflict) —
// pre-fix all of them fell to the requeue arm and looped forever, which is
// what froze 337,247 messages of Kafka lag.
//
// The batch deliberately also contains a tx whose outpoint is NOT named. The
// peer answered 409, so it has told us nothing good about that tx: it must
// NOT be fabricated as ACCEPTED_BY_NETWORK. The #292 withholding rule still
// applies to the whole peer response regardless of how many lines resolved.
func TestBroadcast_409ConflictLines_AttributedToSubmittedInputs(t *testing.T) {
	const (
		outA    = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		outB    = "9d2c1f0e7b5a48361c0e7f2d6b48a91c33e0f8261c086bd9fa90055dd5a7bb11"
		outC    = "11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff"
		spender = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	txidA, rawA := spendingTx(t, outA, 2, 1)
	txidB, rawB := spendingTx(t, outB, 0, 2)
	txidC, rawC := spendingTx(t, outC, 7, 3)

	// Only A's and B's outpoints appear in the failure list.
	body := conflictBody([]string{outpointRef(outA, 2), outpointRef(outB, 0)}, spender)
	srv := conflictServer(&eventLog{}, body)
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 5, time.Hour)

	for _, tx := range []struct {
		txid string
		raw  []byte
	}{{txidA, rawA}, {txidB, rawB}, {txidC, rawC}} {
		if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, tx.txid, tx.raw))); err != nil {
			t.Fatalf("handleMessage(%s): %v", tx.txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	for _, txid := range []string{txidA, txidB} {
		got := ms.lastUpdateForTxid(txid)
		if got == nil {
			t.Fatalf("%s: no status update recorded; the conflict line naming its outpoint must terminalize it (pre-fix it requeued forever)", txid)
		}
		if got.Status != models.StatusRejected {
			t.Errorf("%s: status=%s want REJECTED (extraInfo=%q)", txid, got.Status, got.ExtraInfo)
		}
		if got.StatusCode != 466 {
			t.Errorf("%s: StatusCode=%d want 466 (ARC StatusConflict)", txid, got.StatusCode)
		}
		if !strings.Contains(got.ExtraInfo, "UTXO_SPENT") || !strings.Contains(got.ExtraInfo, spender) {
			t.Errorf("%s: ExtraInfo=%q want the verbatim UTXO_SPENT line naming the competing spender", txid, got.ExtraInfo)
		}
	}

	// Attribution must not have leaked into a fabricated acceptance for the
	// tx nobody named. This is the #292 property; a 409 peer grants no
	// implicit accepts even when every one of its lines resolved.
	if got := ms.lastUpdateForTxid(txidC); got != nil {
		t.Errorf("%s: terminalized as %s (extraInfo=%q); an unnamed tx in a 409 batch must get no vote at all", txidC, got.Status, got.ExtraInfo)
	}

	// A and B must carry each other's verdicts correctly, not the same line
	// twice — attribution is per-outpoint, not best-effort broadcast.
	gotA := ms.lastUpdateForTxid(txidA)
	gotB := ms.lastUpdateForTxid(txidB)
	if gotA != nil && gotB != nil && gotA.ExtraInfo == gotB.ExtraInfo {
		t.Errorf("both txs got the same line %q; each conflict line must attribute to the single tx that spends its outpoint", gotA.ExtraInfo)
	}
	if gotA != nil && !strings.Contains(gotA.ExtraInfo, outpointRef(outA, 2)) {
		t.Errorf("txidA ExtraInfo=%q want the line naming its own outpoint %s", gotA.ExtraInfo, outpointRef(outA, 2))
	}
	if gotB != nil && !strings.Contains(gotB.ExtraInfo, outpointRef(outB, 0)) {
		t.Errorf("txidB ExtraInfo=%q want the line naming its own outpoint %s", gotB.ExtraInfo, outpointRef(outB, 0))
	}
}

// TestBroadcast_409UnattributableConflict_WithholdsAcceptsAndRejects is the
// no-fabrication guard from #292, re-pinned against the attribution path so
// the new code cannot regress it. The 409 names an outpoint no submitted tx
// spends: arcade knows SOMETHING failed but not what, so it must invent
// neither an ACCEPTED_BY_NETWORK nor a REJECTED for anyone. Both txs stay
// non-terminal and fall to the (now bounded) requeue.
func TestBroadcast_409UnattributableConflict_WithholdsAcceptsAndRejects(t *testing.T) {
	const (
		outA     = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		outB     = "9d2c1f0e7b5a48361c0e7f2d6b48a91c33e0f8261c086bd9fa90055dd5a7bb11"
		stranger = "cafebabe00000000000000000000000000000000000000000000000000001234"
		spender  = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	txidA, rawA := spendingTx(t, outA, 1, 11)
	txidB, rawB := spendingTx(t, outB, 1, 12)

	// The named outpoint belongs to neither submitted tx.
	body := conflictBody([]string{outpointRef(stranger, 3)}, spender)
	srv := conflictServer(&eventLog{}, body)
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 5, time.Hour)

	for _, tx := range []struct {
		txid string
		raw  []byte
	}{{txidA, rawA}, {txidB, rawB}} {
		if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, tx.txid, tx.raw))); err != nil {
			t.Fatalf("handleMessage(%s): %v", tx.txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	for _, txid := range []string{txidA, txidB} {
		if got := ms.lastUpdateForTxid(txid); got != nil {
			t.Errorf("%s: terminalized as %s (extraInfo=%q); an unattributable alien 409 line must produce no vote — no fabricated accept, no guessed reject",
				txid, got.Status, got.ExtraInfo)
		}
	}
}

// TestAttributeConflictLines_AmbiguousOutpointDeclines pins the one case
// where arcade holds enough information to name a tx but must refuse to: two
// submitted txs spend the SAME outpoint (an intra-batch double spend), so
// Teranode's single conflict line cannot say which one lost. Guessing would
// terminally reject a transaction that may well be the winner.
func TestAttributeConflictLines_AmbiguousOutpointDeclines(t *testing.T) {
	const (
		shared  = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		spender = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	txidA, rawA := spendingTx(t, shared, 0, 21)
	txidB, rawB := spendingTx(t, shared, 0, 22)
	if txidA == txidB {
		t.Fatal("fixture bug: the two competing txs must differ")
	}

	batch := []propagationMsg{{TXID: txidA, RawTx: rawA}, {TXID: txidB, RawTx: rawB}}
	owners := buildSubmittedOutpointIndex(batch)
	if got := owners[strings.ToLower(outpointRef(shared, 0))]; got != outpointOwnerAmbiguous {
		t.Fatalf("owners[%s]=%d want outpointOwnerAmbiguous (%d)", outpointRef(shared, 0), got, outpointOwnerAmbiguous)
	}

	line := fmt.Sprintf("UTXO_SPENT (70): %s utxo already spent by tx %s[0]", outpointRef(shared, 0), spender)
	failures := map[string]string{strings.ToLower(shared): line}
	inBatch := map[string]bool{strings.ToLower(txidA): true, strings.ToLower(txidB): true}

	if got := attributeConflictLines(failures, inBatch, owners); len(got) != 0 {
		t.Errorf("attributed %v; an outpoint spent by two submitted txs must attribute to neither", got)
	}
}

// --- fix 2: bounded requeue with a terminal escape -----------------------

// TestPoisonBatch_MultiTxConflict409_ParksInsteadOfLoopingForever is the
// incident in its exact shape: a MULTI-tx batch that permanently draws an
// HTTP 409 whose conflict line names an outpoint none of the submitted txs
// spend. Nothing is attributable, no peer accepts, so every tx lands on the
// `default: txResultClassRequeue` arm — which pre-fix meant "again, forever".
//
// The batch has to be multi-tx to reproduce it: for a single-tx batch #292's
// len(batch) == 1 rule already attributes the alien verdict to the only
// possible owner, so the loop never forms. That is exactly why the live
// incident was a 21-tx batch.
//
// Driven through the test-mode dispatcher (nil claim ⇒ no 50ms flush ticker),
// so batching is decided by this test's explicit flushSync calls and the two
// txs provably stay in one batch across every retry round.
func TestPoisonBatch_MultiTxConflict409_ParksInsteadOfLoopingForever(t *testing.T) {
	const (
		outA     = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		outB     = "9d2c1f0e7b5a48361c0e7f2d6b48a91c33e0f8261c086bd9fa90055dd5a7bb11"
		stranger = "deadbeef00000000000000000000000000000000000000000000000000009999"
		spender  = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	txidA, rawA := spendingTx(t, outA, 0, 31)
	txidB, rawB := spendingTx(t, outB, 0, 32)

	broadcasts := &eventLog{}
	srv := conflictServer(broadcasts, conflictBody([]string{outpointRef(stranger, 1)}, spender))
	defer srv.Close()

	const maxAttempts = 2
	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, maxAttempts, 10*time.Millisecond)

	exhaustedBefore := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal)

	for _, tx := range []struct {
		txid string
		raw  []byte
	}{{txidA, rawA}, {txidB, rawB}} {
		if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, tx.txid, tx.raw))); err != nil {
			t.Fatalf("handleMessage(%s): %v", tx.txid, err)
		}
	}

	// One initial flush plus maxAttempts requeued flushes is the entire budget.
	// Waiting for the requeue to land between rounds is what proves the loop
	// really is running, not just failing to start.
	for round := 0; round <= maxAttempts; round++ {
		if round > 0 {
			waitForPending(t, p, 2)
		}
		if err := flushSync(t, p); err != nil {
			t.Fatalf("round %d flush error: %v", round, err)
		}
	}

	// Give any surviving requeue a generous window to come back around; the
	// budget is spent, so nothing more may reach the peer.
	time.Sleep(150 * time.Millisecond)
	if got := broadcasts.count("broadcast"); got != maxAttempts+1 {
		t.Errorf("broadcast attempts=%d want %d (1 initial + %d requeues); the poison batch is still looping",
			got, maxAttempts+1, maxAttempts)
	}

	for _, txid := range []string{txidA, txidB} {
		got := ms.lastUpdateForTxid(txid)
		if got == nil {
			t.Fatalf("%s: no status update — an exhausted tx must reach a state that releases its in-flight entry", txid)
		}
		if got.Status != models.StatusPendingRetry {
			t.Errorf("%s: status=%s want PENDING_RETRY — the escape must not invent a verdict (extraInfo=%q)", txid, got.Status, got.ExtraInfo)
		}
		if !strings.Contains(got.ExtraInfo, "no network verdict") {
			t.Errorf("%s: ExtraInfo=%q want an operator-legible park reason", txid, got.ExtraInfo)
		}
		// The in-flight entry is genuinely released, not merely status-flipped:
		// a re-admission comes back as a fresh admit rather than the duplicate
		// a still-in-flight entry would produce.
		if res := p.admitToDispatcher(propagationMsg{TXID: txid, RawTx: []byte{0x01}}, 90); !res.admitted {
			t.Errorf("%s: re-admit got %+v, want admitted — a parked tx must have left the dispatcher's in-flight set", txid, res)
		}
	}

	if d := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal) - exhaustedBefore; d != 2 {
		t.Errorf("requeue-exhausted counter delta=%v want 2", d)
	}
}

// TestRunDispatcher_PoisonBatch_ExhaustsRetriesAndAdvancesWatermark asserts the
// property that actually unwedges the consumer: claim.MarkMessage — the Kafka
// commit signal — must eventually fire for a transaction that never gets a
// verdict.
//
// The peer here answers a permanent 409 with an unparseable body, so it
// contributes no per-tx information at all. That is the oldest form of the
// poison batch and it long predates #292: teranode/client.go used to parse
// only status 500, so ANY persistent non-500 produced failures = nil and the
// whole batch requeued for another attempt, forever. Pre-fix the tx stayed in
// inFlight, its offset kept pinning offsetTracker.LowestUnfinished(),
// advanceMarks never marked it, and on the single-partition
// arcade.propagation topic every later message queued behind it — 337,247 of
// them, stationary, on dev-ovh-1.
func TestRunDispatcher_PoisonBatch_ExhaustsRetriesAndAdvancesWatermark(t *testing.T) {
	const out = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
	txid, raw := spendingTx(t, out, 0, 41)

	broadcasts := &eventLog{}
	srv := opaqueConflictServer(broadcasts)
	defer srv.Close()

	const maxAttempts = 2
	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, maxAttempts, 20*time.Millisecond)

	exhaustedBefore := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal)

	claim, stop := runDispatcherWithClaim(t, p)
	defer stop()

	claim.ch <- &kafka.Message{Offset: 41, Value: realPropMsg(t, txid, raw)}
	waitForMark(t, claim, 41,
		"a permanently unresolvable batch must exhaust its retry budget and release the offset; without the bound the commit watermark pins forever and the whole partition wedges")

	got := ms.lastUpdateForTxid(txid)
	if got == nil {
		t.Fatal("no status update recorded for the parked tx")
	}
	if got.Status != models.StatusPendingRetry {
		t.Errorf("status=%s want PENDING_RETRY — the escape must not invent a verdict (extraInfo=%q)", got.Status, got.ExtraInfo)
	}
	if !strings.Contains(got.ExtraInfo, "no network verdict") {
		t.Errorf("ExtraInfo=%q want an operator-legible park reason", got.ExtraInfo)
	}
	if d := testutil.ToFloat64(metrics.PropagationRequeueExhaustedTotal) - exhaustedBefore; d != 1 {
		t.Errorf("requeue-exhausted counter delta=%v want 1", d)
	}

	// Bounded, not merely eventually-terminal: nothing may reach the peer once
	// the budget is spent.
	settled := broadcasts.count("broadcast")
	if settled != maxAttempts+1 {
		t.Errorf("broadcast attempts=%d want %d (1 initial + %d requeues)", settled, maxAttempts+1, maxAttempts)
	}
	time.Sleep(150 * time.Millisecond) // ≫ requeueDelay: a surviving loop would fire several more times
	if again := broadcasts.count("broadcast"); again != settled {
		t.Errorf("broadcasts kept firing after the park (%d → %d); the requeue loop is still alive", settled, again)
	}

	// The in-flight entry is genuinely gone, not just offset-marked.
	if res := p.admitToDispatcher(propagationMsg{TXID: txid, RawTx: raw}, 42); !res.admitted {
		t.Errorf("re-admit got %+v, want admitted — the parked tx must have left the dispatcher's in-flight set", res)
	}
}

// TestRequeueBudget_ConfigurableAndDefault pins both halves of "configurable
// with a sensible default": the propagation.retry_max_attempts value is
// honored verbatim, and an unset (or nonsensical) value falls back to
// defaultRetryMaxAttempts rather than to "unbounded" — an unbounded requeue is
// the failure this budget exists to prevent, so it must not be configurable
// away.
func TestRequeueBudget_ConfigurableAndDefault(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		for _, configured := range []int{0, -1} {
			cfg := &config.Config{}
			cfg.Propagation.RetryMaxAttempts = configured
			p := New(cfg, zap.NewNop(), nil, nil, newMockStore(), nil, nil, nil)
			if p.retryMaxAttempts != defaultRetryMaxAttempts {
				t.Errorf("retry_max_attempts=%d → budget %d, want the default %d", configured, p.retryMaxAttempts, defaultRetryMaxAttempts)
			}
			if p.requeueDelay != defaultRequeueDelay {
				t.Errorf("requeueDelay=%v want %v", p.requeueDelay, defaultRequeueDelay)
			}
		}
		if defaultRetryMaxAttempts != 5 {
			t.Errorf("defaultRetryMaxAttempts=%d; it must stay equal to the shipped propagation.retry_max_attempts viper default (5)", defaultRetryMaxAttempts)
		}
	})

	// Behavioral half: the configured budget, not a hardcoded constant, decides
	// how many broadcasts a poison tx gets before it is parked.
	t.Run("configured budget bounds the broadcast count", func(t *testing.T) {
		for _, maxAttempts := range []uint32{1, 3} {
			t.Run(fmt.Sprintf("max=%d", maxAttempts), func(t *testing.T) {
				const out = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
				txid, raw := spendingTx(t, out, 0, 100+maxAttempts)

				broadcasts := &eventLog{}
				srv := opaqueConflictServer(broadcasts)
				defer srv.Close()

				ms := newMockStore()
				p := poisonPropagator(srv.URL, ms, int(maxAttempts), 10*time.Millisecond)
				if p.retryMaxAttempts != int(maxAttempts) {
					t.Fatalf("configured budget not honored: got %d want %d", p.retryMaxAttempts, maxAttempts)
				}

				claim, stop := runDispatcherWithClaim(t, p)
				defer stop()

				claim.ch <- &kafka.Message{Offset: 61, Value: realPropMsg(t, txid, raw)}
				waitForMark(t, claim, 61, "the poison tx must park and release its offset")

				if got := broadcasts.count("broadcast"); got != int(maxAttempts)+1 {
					t.Errorf("broadcast attempts=%d want %d (1 initial + %d requeues)", got, maxAttempts+1, maxAttempts)
				}
				if got := ms.lastUpdateForTxid(txid); got == nil || got.Status != models.StatusPendingRetry {
					t.Errorf("final status=%v want PENDING_RETRY", got)
				}
			})
		}
	})
}

// TestHandleTerminal_PendingRetry_ReleasesOffsetAndCascades is the
// dispatcher-level contract behind the escape hatch, asserted in isolation:
// a park must Done the tx's offset (so LowestUnfinished stops pinning) and
// must also release every held descendant's offset. Missing either one leaves
// the commit watermark exactly as frozen as the unbounded requeue did.
func TestHandleTerminal_PendingRetry_ReleasesOffsetAndCascades(t *testing.T) {
	s := newDispatcherState()

	if r := s.admit("parent", 1); !r.admitted {
		t.Fatalf("parent should admit; got %+v", r)
	}
	s.pendingMsgs = nil
	if r := s.admit("child", 2, "parent"); !r.held {
		t.Fatalf("child should be held behind parent; got %+v", r)
	}

	res := s.terminal("parent", models.StatusPendingRetry)
	if len(res.cascaded) != 1 || res.cascaded[0].txid != "child" {
		t.Fatalf("parking a parent must cascade its held descendants; got %v", res.cascaded)
	}
	if !s.tracker.Empty() {
		t.Fatal("PENDING_RETRY must Done the parked tx's offset AND every cascaded descendant's; tracker should be empty")
	}
	if _, ok := s.inFlight["parent"]; ok {
		t.Error("parked tx must leave the in-flight set")
	}
	if _, ok := s.inFlight["child"]; ok {
		t.Error("cascaded descendant must leave the in-flight set")
	}
}
