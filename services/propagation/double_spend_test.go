package propagation

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/teranode"
)

// A double spend submitted to mainnet on 2026-10-01 (44c4356e…, spending
// 3d4df1f3…:0, already spent by the mined 212b108b…) sat at PENDING_RETRY for
// a day. Every peer ran Teranode v0.15.9, whose public error boundary renders
// only the outermost wrapper, so all of them answered
//
//	500  PROCESSING (4): [ProcessTransaction][<txid>] failed to validate transaction
//
// — a line arcade correctly cannot tell apart from a node fault (#313). From
// Teranode #1295/#1595 on, the same submission draws
//
//	409  UTXO_SPENT (70): [ProcessTransaction][<txid>] <outpoint>:<vout> utxo already spent by tx <spender>[<vin>]
//
// These tests pin what arcade must do once ANY peer gives that answer, while
// the rest of the fleet still gives the opaque one: REJECTED, ARC 466, the
// competing spender on the row and on the event subscribers receive.

const (
	dsOutpointTxid = "3d4df1f3769abac5b892366210a79e2f41d21e4ce3e0b1ad65335b1278b381d0"
	dsSpenderTxid  = "212b108b6fcf762b3bba5aec0d0de4db0ee2fa0c6f76f9838ad257812b6d9ac5"
)

// failureListServer answers every POST /txs with status and a one-line
// Teranode failure list built by line(txid).
func failureListServer(status int, line func(txid string) string, txid string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, "Failed to process transactions:\n%s\n", line(txid))
	}))
}

func opaqueProcessingLine(txid string) string {
	return "PROCESSING (4): [ProcessTransaction][" + txid + "] failed to validate transaction"
}

func multiPeerPropagator(urls []string, ms *mockStore, pub *recordingPublisher) *Propagator {
	cfg := &config.Config{}
	cfg.Propagation.MerkleConcurrency = 10
	cfg.Propagation.RetryMaxAttempts = 5
	tc := teranode.NewClient(urls, "", teranode.HealthConfig{FailureThreshold: 1 << 20})
	p := New(cfg, zap.NewNop(), nil, pub, ms, nil, tc, nil)
	p.requeueDelay = time.Hour
	return p
}

func TestDoubleSpend_OneUpgradedPeerRejectsWithCompetingTx(t *testing.T) {
	txid, raw := spendingTx(t, dsOutpointTxid, 0, 1)

	cases := map[string]func(string) string{
		// #1595 shape: the per-tx wrapper names the submitted tx.
		"wrapped": func(id string) string {
			return "UTXO_SPENT (70): [ProcessTransaction][" + id + "] " + outpointRef(dsOutpointTxid, 0) +
				" utxo already spent by tx " + dsSpenderTxid + "[0]"
		},
		// #1295-only shape: the cause alone, keyed by the spent outpoint and
		// placed by outpoint attribution.
		"outpoint-keyed": func(string) string {
			return "UTXO_SPENT (70): " + outpointRef(dsOutpointTxid, 0) +
				" utxo already spent by tx " + dsSpenderTxid + "[0]"
		},
	}
	for name, upgradedLine := range cases {
		t.Run(name, func(t *testing.T) {
			old1 := failureListServer(http.StatusInternalServerError, opaqueProcessingLine, txid)
			defer old1.Close()
			old2 := failureListServer(http.StatusInternalServerError, opaqueProcessingLine, txid)
			defer old2.Close()
			upgraded := failureListServer(http.StatusConflict, upgradedLine, txid)
			defer upgraded.Close()

			ms := newMockStore()
			pub := &recordingPublisher{}
			p := multiPeerPropagator([]string{old1.URL, upgraded.URL, old2.URL}, ms, pub)

			if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, txid, raw))); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if err := flushSync(t, p); err != nil {
				t.Fatalf("flush: %v", err)
			}

			got := ms.lastUpdateForTxid(txid)
			if got == nil {
				t.Fatal("no status written: the upgraded peer's UTXO_SPENT verdict must terminalize the tx")
			}
			if got.Status != models.StatusRejected || got.StatusCode != 466 {
				t.Fatalf("status = %s/%d, want REJECTED/466 (extraInfo=%q)", got.Status, got.StatusCode, got.ExtraInfo)
			}
			if !strings.HasPrefix(got.ExtraInfo, "UTXO_SPENT (70)") {
				t.Errorf("extraInfo = %q, want the UTXO_SPENT line, not an opaque PROCESSING one", got.ExtraInfo)
			}
			if !slices.Equal(got.CompetingTxs, []string{dsSpenderTxid}) {
				t.Errorf("persisted CompetingTxs = %v, want [%s]", got.CompetingTxs, dsSpenderTxid)
			}

			var event *models.TransactionStatus
			for _, ev := range pub.bulkSnapshot() {
				if ev.Status == models.StatusRejected && slices.Contains(ev.TxIDs, txid) {
					event = ev
				}
			}
			if event == nil {
				t.Fatal("no REJECTED event published for the double spend")
			}
			if event.StatusCode != 466 || !slices.Equal(event.CompetingTxs, []string{dsSpenderTxid}) {
				t.Errorf("event status=%d competingTxs=%v, want 466 and [%s]", event.StatusCode, event.CompetingTxs, dsSpenderTxid)
			}
		})
	}
}

// TestDoubleSpend_AllPeersOpaque_DoesNotTerminalize is the other half of the
// contract: with no peer naming a cause, an opaque PROCESSING 500 still means
// "no verdict" and must not become a reasonless REJECTED (#313).
func TestDoubleSpend_AllPeersOpaque_DoesNotTerminalize(t *testing.T) {
	txid, raw := spendingTx(t, dsOutpointTxid, 0, 2)
	a := failureListServer(http.StatusInternalServerError, opaqueProcessingLine, txid)
	defer a.Close()
	b := failureListServer(http.StatusInternalServerError, opaqueProcessingLine, txid)
	defer b.Close()

	ms := newMockStore()
	p := multiPeerPropagator([]string{a.URL, b.URL}, ms, &recordingPublisher{})
	if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, txid, raw))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := ms.lastUpdateForTxid(txid); got != nil && got.Status == models.StatusRejected {
		t.Fatalf("opaque PROCESSING from every peer terminalized the tx: %+v", got)
	}
}

// TestReaperGiveUp_QuotesLastNetworkResponse covers the durable-retry half of
// the incident: a parked double spend that every peer keeps answering with the
// opaque PROCESSING line. When its budget runs out, the REJECTED reason must
// quote that line — the reaper used to drop it and report "no peer ever
// answered", which was false and hid the one clue the submitter had.
func TestReaperGiveUp_QuotesLastNetworkResponse(t *testing.T) {
	txid, raw := spendingTx(t, dsOutpointTxid, 0, 3)
	srv := failureListServer(http.StatusInternalServerError, opaqueProcessingLine, txid)
	defer srv.Close()

	ms := newMockStore()
	ms.parkTx(txid, raw, time.Now(), time.Now())
	p := newReaperPropagator(t, srv.URL, ms, 0)
	p.pendingRetryMaxAttempts = 1
	p.pendingRetryBackoff = time.Millisecond
	p.pendingRetryMaxBackoff = time.Millisecond

	// Attempt 1 reschedules; attempt 2 exceeds the budget and gives up.
	for i := 0; i < 2; i++ {
		p.rebroadcastStuck(context.Background(), []propagationMsg{{TXID: txid, RawTx: raw}}, true)
	}

	last := ms.lastUpdateForTxid(txid)
	if last == nil || last.Status != models.StatusRejected {
		t.Fatalf("last status = %v, want REJECTED after the durable budget", statusOrNone(last))
	}
	if !strings.Contains(last.ExtraInfo, opaqueProcessingLine(txid)) {
		t.Errorf("give-up reason = %q, want it to quote the last network response %q", last.ExtraInfo, opaqueProcessingLine(txid))
	}
	if strings.Contains(last.ExtraInfo, "no peer ever answered") {
		t.Errorf("give-up reason = %q claims no peer answered, but every peer did", last.ExtraInfo)
	}
}

// TestReaperGiveUp_KeepsResponseFromEarlierAttempt is the cross-attempt form
// of the test above. The durable queue drains by rebuilding each message from
// the store, so a response heard on attempt N is only available on attempt
// N+1 if it was persisted. Here attempt 1 draws the opaque PROCESSING line and
// the budget-exhausting attempt 2 gets a body-less 500 — no line at all. The
// give-up must still quote what attempt 1 heard.
func TestReaperGiveUp_KeepsResponseFromEarlierAttempt(t *testing.T) {
	txid, raw := spendingTx(t, dsOutpointTxid, 0, 4)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		if hits.Add(1) == 1 {
			_, _ = fmt.Fprintf(w, "Failed to process transactions:\n%s\n", opaqueProcessingLine(txid))
		}
	}))
	defer srv.Close()

	ms := newMockStore()
	ms.parkTx(txid, raw, time.Now(), time.Now().Add(-time.Second)) // retry_count 1
	p := newReaperPropagator(t, srv.URL, ms, 0)
	p.pendingRetryMaxAttempts = 2
	p.pendingRetryBackoff = time.Millisecond
	p.pendingRetryMaxBackoff = time.Millisecond

	// Attempt 1 (count 2): PROCESSING line, rescheduled. Attempt 2 (count 3):
	// nothing heard, budget exceeded. Each drain rebuilds the message from
	// the store.
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		time.Sleep(5 * time.Millisecond) // let the millisecond backoff elapse
		p.drainParkedRetries(ctx, time.Now(), time.Now().Add(-time.Hour))
	}

	if got := hits.Load(); got != 2 {
		t.Fatalf("teranode hit %d times, want 2 (one per durable attempt)", got)
	}
	last := ms.lastUpdateForTxid(txid)
	if last == nil || last.Status != models.StatusRejected {
		t.Fatalf("last status = %v, want REJECTED after the durable budget", statusOrNone(last))
	}
	if !strings.Contains(last.ExtraInfo, opaqueProcessingLine(txid)) {
		t.Errorf("give-up reason = %q, want it to quote attempt 1's response %q", last.ExtraInfo, opaqueProcessingLine(txid))
	}
}

// TestReaperGiveUp_NewEpisodeDoesNotInheritOldResponse pins the other edge of
// the persisted reason: it belongs to one stay in the retry queue. A tx that
// is parked with a PROCESSING response, then accepted by a rebroadcast, then
// parked again by a later episode that heard nothing must not give up quoting
// the response from before its acceptance.
func TestReaperGiveUp_NewEpisodeDoesNotInheritOldResponse(t *testing.T) {
	txid, raw := spendingTx(t, dsOutpointTxid, 0, 5)
	var accept atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if accept.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Body-less 500: no network response at all.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ms := newMockStore()
	p := newReaperPropagator(t, srv.URL, ms, 0)
	p.pendingRetryMaxAttempts = 2
	p.pendingRetryBackoff = time.Millisecond
	p.pendingRetryMaxBackoff = time.Millisecond
	ctx := context.Background()
	drain := func() {
		time.Sleep(5 * time.Millisecond) // let the millisecond backoff elapse
		p.drainParkedRetries(ctx, time.Now(), time.Now().Add(-time.Hour))
	}

	// Episode 1: parked having heard the opaque PROCESSING line.
	p.parkExhaustedRequeues(ctx, []propagationMsg{{TXID: txid, RawTx: raw, retryReason: opaqueProcessingLine(txid)}}, p.defaultIO)

	// A rebroadcast is accepted: the tx leaves the retry queue.
	accept.Store(true)
	drain()
	if got := ms.lastUpdateForTxid(txid); got == nil || got.Status != models.StatusAcceptedByNetwork {
		t.Fatalf("status after accepted rebroadcast = %v, want ACCEPTED_BY_NETWORK", statusOrNone(got))
	}

	// Episode 2: parked again having heard nothing, and never answered after.
	accept.Store(false)
	p.parkExhaustedRequeues(ctx, []propagationMsg{{TXID: txid, RawTx: raw}}, p.defaultIO)
	for i := 0; i < 3; i++ {
		drain()
	}

	last := ms.lastUpdateForTxid(txid)
	if last == nil || last.Status != models.StatusRejected {
		t.Fatalf("last status = %v, want REJECTED after the durable budget", statusOrNone(last))
	}
	if strings.Contains(last.ExtraInfo, "PROCESSING") {
		t.Errorf("give-up reason = %q quotes a response from before the tx was accepted", last.ExtraInfo)
	}
}

// TestDoubleSpend_SpenderSurvivesTieWithTxConflicting: two upgraded peers give
// equally ranked conflict verdicts, only one naming the spender. Whichever
// answers first, the row and event must carry competingTxs.
func TestDoubleSpend_SpenderSurvivesTieWithTxConflicting(t *testing.T) {
	txid, raw := spendingTx(t, dsOutpointTxid, 0, 6)
	conflicting := failureListServer(http.StatusConflict, func(id string) string {
		return "TX_CONFLICTING (36): [ProcessTransaction][" + id + "] tx is conflicting"
	}, txid)
	defer conflicting.Close()
	spent := failureListServer(http.StatusConflict, func(id string) string {
		return "UTXO_SPENT (70): [ProcessTransaction][" + id + "] " + outpointRef(dsOutpointTxid, 0) +
			" utxo already spent by tx " + dsSpenderTxid + "[0]"
	}, txid)
	defer spent.Close()

	for name, urls := range map[string][]string{
		"conflicting first": {conflicting.URL, spent.URL},
		"spent first":       {spent.URL, conflicting.URL},
	} {
		t.Run(name, func(t *testing.T) {
			ms := newMockStore()
			pub := &recordingPublisher{}
			p := multiPeerPropagator(urls, ms, pub)
			if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, txid, raw))); err != nil {
				t.Fatalf("handleMessage: %v", err)
			}
			if err := flushSync(t, p); err != nil {
				t.Fatalf("flush: %v", err)
			}
			got := ms.lastUpdateForTxid(txid)
			if got == nil || got.Status != models.StatusRejected || got.StatusCode != 466 {
				t.Fatalf("status = %+v, want REJECTED/466", got)
			}
			if !slices.Equal(got.CompetingTxs, []string{dsSpenderTxid}) {
				t.Errorf("CompetingTxs = %v, want [%s] (extraInfo=%q)", got.CompetingTxs, dsSpenderTxid, got.ExtraInfo)
			}
		})
	}
}
