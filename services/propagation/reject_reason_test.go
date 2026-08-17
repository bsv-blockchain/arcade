package propagation

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// Issue #301. arcade parses Teranode's reject reason, persists it on the row
// and logs it — and then publishes REJECTED through the payload-free bulk
// fan-out, so every SSE and webhook subscriber sees a terminal REJECTED with an
// empty extraInfo. The row keeps the reason and GET /tx/<txid> serves it, so
// nothing is lost; what is missing is that the push event does not describe
// itself. That costs a round trip on the one status whose payload decides what
// the client does next (UTXO_SPENT means "your view of the chain is stale,
// resync"; TX_INVALID means "these bytes will never be accepted, stop
// retrying") — and REJECTED is terminal, so a client that correctly stops
// polling on a terminal status stops exactly when polling becomes the only way
// to learn why.
//
// These tests pin the property end to end: every REJECTED event a subscriber
// can receive carries the SAME reason string arcade wrote to the row.

// rejectionPropagator wires a propagator with a recording publisher and no
// merkle client (registerBatch is then a pass-through), so a flush's published
// events can be inspected directly. The requeue delay is parked at an hour:
// these tests assert on the terminal publish, never on a retry landing.
func rejectionPropagator(teranodeURL string, st store.Store, pub events.Publisher) *Propagator {
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.RetryMaxAttempts = 5
	tc := teranode.NewClient([]string{teranodeURL}, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, pub, st, nil, tc, nil)
	p.requeueDelay = time.Hour
	return p
}

// publishedReasons flattens every bulk event the publisher recorded into a
// txid → extraInfo map, mirroring the unfan both real subscribers perform
// (services/sse.Manager.fanOutBulk and services/webhook.Service.handleUpdate
// both copy the whole struct per txid).
func publishedReasons(pub *recordingPublisher, status models.Status) map[string]string {
	out := make(map[string]string)
	for _, ev := range pub.bulkSnapshot() {
		if ev.Status != status {
			continue
		}
		for _, txid := range ev.TxIDs {
			out[txid] = ev.ExtraInfo
		}
	}
	return out
}

// persistedReasons collects the reason arcade wrote to each REJECTED row, so a
// test can assert the published string is that exact string rather than a
// second rendering of the same idea.
func persistedReasons(ms *mockStore) map[string]string {
	out := make(map[string]string)
	for _, u := range rejectedUpdates(ms) {
		out[u.TxID] = u.ExtraInfo
	}
	return out
}

// TestApplyTerminalStatuses_RejectionCarriesTeranodeReason is the reported bug
// at batch scale: two txs double-spend two different outpoints, Teranode 409s
// the batch with one UTXO_SPENT line each, and both terminalize REJECTED. Each
// tx's event must carry that tx's own line and its ARC code — pre-fix the whole
// batch shared ONE event whose extraInfo was "" and whose status code was 0.
func TestApplyTerminalStatuses_RejectionCarriesTeranodeReason(t *testing.T) {
	const (
		outA    = "256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43"
		outB    = "9d2c1f0e7b5a48361c0e7f2d6b48a91c33e0f8261c086bd9fa90055dd5a7bb11"
		spender = "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489"
	)
	txidA, rawA := spendingTx(t, outA, 2, 1)
	txidB, rawB := spendingTx(t, outB, 0, 2)

	body := conflictBody([]string{outpointRef(outA, 2), outpointRef(outB, 0)}, spender)
	srv := conflictServer(&eventLog{}, body)
	defer srv.Close()

	ms := newMockStore()
	pub := &recordingPublisher{}
	p := rejectionPropagator(srv.URL, ms, pub)

	for _, tx := range []struct {
		txid string
		raw  []byte
	}{{txidA, rawA}, {txidB, rawB}} {
		if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, tx.txid, tx.raw))); err != nil {
			t.Fatalf("handleMessage %s: %v", tx.txid, err)
		}
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	persisted := persistedReasons(ms)
	if len(persisted) != 2 {
		t.Fatalf("expected 2 REJECTED rows, got %d — the fixture stopped producing verdicts", len(persisted))
	}
	published := publishedReasons(pub, models.StatusRejected)
	for _, txid := range []string{txidA, txidB} {
		reason, ok := published[txid]
		if !ok {
			t.Fatalf("no REJECTED event published for %s", txid)
		}
		if reason == "" {
			t.Errorf("REJECTED event for %s carries no reason, so a subscriber cannot tell a "+
				"double-spend from a policy failure without a GET /tx round trip (#301)", txid)
		}
		if reason != persisted[txid] {
			t.Errorf("published reason for %s = %q, persisted = %q; the event and the row must "+
				"agree on one string", txid, reason, persisted[txid])
		}
		if !strings.Contains(reason, "UTXO_SPENT (70)") {
			t.Errorf("published reason for %s = %q, want Teranode's UTXO_SPENT verdict", txid, reason)
		}
	}

	// The ARC code rides along for the same reason the intake rejection path
	// (api_server.rejectAtIntake) publishes it: 466 is the machine-readable
	// form of "conflict", and a client that switches on prose is a client that
	// breaks when the prose changes.
	for _, ev := range pub.bulkSnapshot() {
		if ev.Status == models.StatusRejected && ev.StatusCode != 466 {
			t.Errorf("REJECTED event %v carries status code %d, want 466 (ARC conflict)", ev.TxIDs, ev.StatusCode)
		}
	}
}

// TestPersistCascade_RejectionGroupsByAncestorReason covers the cascade path
// and the batching property in one shot.
//
// A cascade is where rejection volume actually comes from: one rejected tx at
// the head of a chain condemns every descendant behind it, and the report came
// from a 128-chain covenant workload where exactly that happened. Every
// descendant of ONE ancestor shares one cascadeReason string, so they must
// still ride ONE event — publishing per transaction would cost the SSE fan-out
// a store round-trip per descendant on its single goroutine, which is too much
// to pay for a convenience improvement. Two ancestors mean two reasons, and
// therefore two events: pre-fix all four txs shared one event with no reason.
func TestPersistCascade_RejectionGroupsByAncestorReason(t *testing.T) {
	const (
		ancestorA = "aaaa1f0e7b5a48361c0e7f2d6b48a91c33e0f8261c086bd9fa90055dd5a7bb11"
		ancestorB = "bbbb1f0e7b5a48361c0e7f2d6b48a91c33e0f8261c086bd9fa90055dd5a7bb11"
	)
	ms := newMockStore()
	pub := &recordingPublisher{}
	p := rejectionPropagator("http://127.0.0.1:0", ms, pub)

	cascaded := []cascadeRejection{
		{txid: "child-a1", ancestor: ancestorA},
		{txid: "child-a2", ancestor: ancestorA},
		{txid: "child-a3", ancestor: ancestorA},
		{txid: "child-b1", ancestor: ancestorB},
	}
	p.persistCascade(context.Background(), cascaded, models.StatusRejected, time.Now())

	emitted := pub.bulkSnapshot()
	if len(emitted) != 2 {
		t.Fatalf("expected 1 event per distinct cascade reason (2), got %d: %+v", len(emitted), emitted)
	}
	byReason := make(map[string][]string, 2)
	for _, ev := range emitted {
		if ev.TxID != "" {
			t.Errorf("bulk event carries TxID %q; the contract is TxIDs populated, TxID empty", ev.TxID)
		}
		byReason[ev.ExtraInfo] = ev.TxIDs
	}

	wantA := cascadeReason(models.StatusRejected, ancestorA)
	if got := byReason[wantA]; len(got) != 3 {
		t.Errorf("ancestor %s condemned 3 descendants; its event carries %v — descendants of one "+
			"ancestor share a reason and must stay batched", ancestorA, got)
	}
	wantB := cascadeReason(models.StatusRejected, ancestorB)
	if got := byReason[wantB]; len(got) != 1 || got[0] != "child-b1" {
		t.Errorf("event for ancestor %s carries %v, want [child-b1]", ancestorB, got)
	}

	// Same invariant as the direct path: what was published is what was
	// persisted, character for character.
	published := publishedReasons(pub, models.StatusRejected)
	persisted := persistedReasons(ms)
	for _, cr := range cascaded {
		if published[cr.txid] != persisted[cr.txid] {
			t.Errorf("%s: published reason %q != persisted reason %q",
				cr.txid, published[cr.txid], persisted[cr.txid])
		}
	}
}

// TestSchedulePendingRetry_GiveUpCarriesReason covers the third and least
// self-evident way a tx reaches REJECTED: the durable retry budget runs out
// because a parent it spends never reached the network, so there is no peer
// verdict to quote and nothing about the transaction itself explains the
// outcome.
func TestSchedulePendingRetry_GiveUpCarriesReason(t *testing.T) {
	root := strings.Repeat("99", 32)
	orphan := buildChain(t, root, 1)[0]

	ms := newMockStore()
	pub := &recordingPublisher{}
	p := rejectionPropagator("http://127.0.0.1:0", ms, pub)
	p.pendingRetryMaxAttempts = 3
	p.pendingRetryBackoff = time.Millisecond
	p.pendingRetryMaxBackoff = time.Millisecond

	// Attempts 1..3 reschedule; the 4th exceeds the budget and terminalizes.
	for i := 0; i < 4; i++ {
		p.schedulePendingRetry(context.Background(), orphan.txid, orphan.raw, orphanRetryReason)
	}

	emitted := pub.bulkSnapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected exactly 1 REJECTED event for the give-up, got %d: %+v", len(emitted), emitted)
	}
	ev := emitted[0]
	if ev.Status != models.StatusRejected || len(ev.TxIDs) != 1 || ev.TxIDs[0] != orphan.txid {
		t.Fatalf("give-up event = %+v, want REJECTED for %s", ev, orphan.txid)
	}
	if !strings.Contains(ev.ExtraInfo, "giving up") {
		t.Errorf("give-up event reason = %q, want the same budget-exhausted reason written to the "+
			"row", ev.ExtraInfo)
	}

	ms.mu.Lock()
	cleared := append([]clearedCall(nil), ms.cleared...)
	ms.mu.Unlock()
	if len(cleared) != 1 {
		t.Fatalf("expected exactly 1 ClearRetryState call, got %d", len(cleared))
	}
	if ev.ExtraInfo != cleared[0].extraInfo {
		t.Errorf("published reason %q != the reason written to the row %q", ev.ExtraInfo, cleared[0].extraInfo)
	}
}

// TestPublishBulkStatus_NonRejectionPayloadUnchanged guards the blast radius.
// The reason plumbing is scoped to REJECTED: ACCEPTED_BY_NETWORK and
// PENDING_RETRY keep the payload-free bulk event they have always emitted, so
// no existing subscriber sees a byte it did not see before. PENDING_RETRY does
// carry a reason on its row, but it is transient — the reaper resolves it — and
// widening the wire for it belongs in its own change.
func TestPublishBulkStatus_NonRejectionPayloadUnchanged(t *testing.T) {
	pub := &recordingPublisher{}
	p := rejectionPropagator("http://127.0.0.1:0", newMockStore(), pub)

	now := time.Now()
	p.publishBulkStatus(context.Background(), models.StatusAcceptedByNetwork, []string{"tx1", "tx2"}, now)
	p.publishBulkStatus(context.Background(), models.StatusPendingRetry, []string{"tx3"}, now)

	emitted := pub.bulkSnapshot()
	if len(emitted) != 2 {
		t.Fatalf("expected 2 bulk events, got %d", len(emitted))
	}
	for _, ev := range emitted {
		if ev.ExtraInfo != "" || ev.StatusCode != 0 {
			t.Errorf("non-rejection bulk event %s gained a payload (extraInfo=%q, status=%d)",
				ev.Status, ev.ExtraInfo, ev.StatusCode)
		}
	}
	if got := len(emitted[0].TxIDs); got != 2 {
		t.Errorf("ACCEPTED_BY_NETWORK event carries %d txids, want 2 — accepts must stay batched", got)
	}
}

// TestBulkUnfanCarriesRejectionReason proves the consumer side of the fix
// without reaching into services/sse or services/webhook: both unfan a bulk
// event by copying the whole struct and overwriting TxID/TxIDs, so a reason on
// the event is a reason on every per-tx frame. This is why the fix needs no
// wire format change and no subscriber change — the field was always
// serialized, the publisher just never set it.
func TestBulkUnfanCarriesRejectionReason(t *testing.T) {
	bulk := &models.TransactionStatus{
		Status:     models.StatusRejected,
		StatusCode: 466,
		ExtraInfo:  "UTXO_SPENT (70): outpoint already spent",
		TxIDs:      []string{"tx1", "tx2"},
		Timestamp:  time.Now(),
	}
	for _, txid := range bulk.TxIDs {
		perTx := *bulk
		perTx.TxID = txid
		perTx.TxIDs = nil
		if perTx.ExtraInfo != bulk.ExtraInfo || perTx.StatusCode != bulk.StatusCode {
			t.Fatalf("unfanned frame for %s lost its payload: %+v", txid, perTx)
		}
	}
}
