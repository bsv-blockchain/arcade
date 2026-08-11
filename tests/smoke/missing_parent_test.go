//go:build smoke

package smoke

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/config"
)

// These tests pin the #295 missing-parent safety net end-to-end: with a
// multi-partition arcade.propagation topic, a child submitted in a LATER
// HTTP request than its parent has no ordering guarantee — it can reach
// teranode first and draw TX_MISSING_PARENT. Arcade must treat that as a
// retryable condition (requeue → eventual accept once the parent lands, or
// PENDING_RETRY park when it never does), and must NEVER mark the child
// REJECTED for what is purely an ordering artifact.

// dependencyAwareTeranode is a stateful fake teranode: it accepts a tx only
// when every input's source tx is already in its accepted set, and answers
// HTTP 422 with a real deepest-cause missing-parent failure line for each
// tx whose parent it hasn't seen — the deployed-teranode behavior arcade's
// parser is built against. Txs absent from the failure list are implicitly
// accepted, matching teranode's "Failed to process transactions:" contract.
type dependencyAwareTeranode struct {
	server *httptest.Server

	mu             sync.Mutex
	accepted       map[string]bool
	rejectionCount int // /txs responses that carried missing-parent lines
	batches        []BatchRecord
}

func newDependencyAwareTeranode(t *testing.T) *dependencyAwareTeranode {
	t.Helper()
	d := &dependencyAwareTeranode{accepted: make(map[string]bool)}
	// BuildChains roots spend a synthetic confirmed outpoint (32 bytes of
	// 0x01, symmetric so byte order is moot) — treat it as on-chain so
	// roots are accepted on sight, exactly like a real node that has the
	// funding tx in its UTXO set.
	d.accepted[strings.Repeat("01", 32)] = true
	d.server = httptest.NewServer(http.HandlerFunc(d.handle))
	t.Cleanup(d.server.Close)
	return d
}

func (d *dependencyAwareTeranode) URL() string { return d.server.URL }

func (d *dependencyAwareTeranode) handle(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost || (req.URL.Path != "/txs" && req.URL.Path != "/tx") {
		w.WriteHeader(http.StatusOK)
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the concatenated txs, keeping inputs for the parent check.
	type parsed struct {
		txid    string
		parents []string
	}
	var txs []parsed
	offset := 0
	for offset < len(body) {
		tx, used, perr := sdkTx.NewTransactionFromStream(body[offset:])
		if perr != nil {
			http.Error(w, fmt.Sprintf("parse at %d: %v", offset, perr), http.StatusBadRequest)
			return
		}
		if used == 0 {
			break
		}
		p := parsed{txid: tx.TxID().String()}
		for _, in := range tx.Inputs {
			if in != nil && in.SourceTXID != nil {
				p.parents = append(p.parents, in.SourceTXID.String())
			}
		}
		txs = append(txs, p)
		offset += used
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	var failureLines []string
	var batchTxids []string
	for _, p := range txs {
		batchTxids = append(batchTxids, p.txid)
		missing := ""
		for _, parent := range p.parents {
			if !d.accepted[parent] {
				missing = parent
				break
			}
		}
		if missing != "" {
			failureLines = append(failureLines,
				fmt.Sprintf("TX_MISSING_PARENT (34): [Validate][%s] error getting parent transaction %s", p.txid, missing))
			continue
		}
		d.accepted[p.txid] = true
	}
	d.batches = append(d.batches, BatchRecord{
		Seq:        len(d.batches),
		TxIDs:      batchTxids,
		ReceivedAt: time.Now(),
	})

	if len(failureLines) == 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}
	d.rejectionCount++
	w.WriteHeader(http.StatusUnprocessableEntity)
	_, _ = w.Write([]byte("Failed to process transactions:\n" + strings.Join(failureLines, "\n") + "\n"))
}

func (d *dependencyAwareTeranode) hasAccepted(txid string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.accepted[txid]
}

func (d *dependencyAwareTeranode) missingParentResponses() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.rejectionCount
}

func (d *dependencyAwareTeranode) snapshotBatches() []BatchRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]BatchRecord, len(d.batches))
	copy(out, d.batches)
	return out
}

// waitForAccepted polls until the fake teranode has accepted txid.
func (d *dependencyAwareTeranode) waitForAccepted(t *testing.T, txid string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d.hasAccepted(txid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("teranode never accepted %s within %s", txid, timeout)
}

// waitForMissingParentResponse polls until the fake teranode has answered
// at least n missing-parent 422s.
func (d *dependencyAwareTeranode) waitForMissingParentResponse(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d.missingParentResponses() >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("teranode never answered %d missing-parent responses within %s", n, timeout)
}

// arcadeTxStatus fetches GET /tx/:txid and returns the persisted status.
func arcadeTxStatus(t *testing.T, rt *arcadeRuntime, txid string) string {
	t.Helper()
	resp, err := http.Get(rt.baseURL + "/tx/" + txid)
	if err != nil {
		t.Fatalf("GET /tx/%s: %v", txid, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Status string `json:"txStatus"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode /tx/%s: %v", txid, err)
	}
	return out.Status
}

// waitForArcadeStatus polls GET /tx/:txid until it reports want.
func waitForArcadeStatus(t *testing.T, rt *arcadeRuntime, txid, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		last = arcadeTxStatus(t, rt, txid)
		if last == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tx %s never reached status %s within %s (last: %s)", txid, want, timeout, last)
}

// TestSmoke_OutOfOrderChild_RecoversViaMissingParentRetry is the #295
// cross-submission race, end to end on a 4-partition topic: the child is
// submitted (and broadcast) BEFORE its parent ever reaches arcade. Teranode
// 422s it with TX_MISSING_PARENT; arcade requeues instead of rejecting;
// once the parent is submitted and accepted, the child's retry lands.
// Assertions: both txs end ACCEPTED_BY_NETWORK, the child was genuinely
// rejected at least once by teranode (the race really happened), no batch
// ever co-housed parent and child, and NOTHING was marked REJECTED.
func TestSmoke_OutOfOrderChild_RecoversViaMissingParentRetry(t *testing.T) {
	teranode := newDependencyAwareTeranode(t)
	rt := startArcadeSmoke(t, smokeOptions{
		TeranodeURL:           teranode.URL(),
		PropagationPartitions: 4,
		MutateConfig: func(cfg *config.Config) {
			// A wide in-memory retry window so the child keeps retrying
			// until the parent (submitted only after the child's first
			// 422) lands — without depending on reaper cadence.
			cfg.Propagation.RetryMaxAttempts = 50
			cfg.Propagation.RetryBackoffMs = 25
		},
	})

	chain := BuildChains(ChainOpts{TotalTxs: 2, MinDepth: 2, MaxDepth: 2, Seed: 7})[0]
	parent, child := chain[0], chain[1]

	// Child first — the out-of-order submission.
	if err := postTxs(rt, []*bt.Tx{child}); err != nil {
		t.Fatalf("submit child: %v", err)
	}
	// The race must actually happen: teranode rejects the child at least
	// once before we even submit the parent.
	teranode.waitForMissingParentResponse(t, 1, 10*time.Second)

	if err := postTxs(rt, []*bt.Tx{parent}); err != nil {
		t.Fatalf("submit parent: %v", err)
	}
	teranode.waitForAccepted(t, parent.TxID(), 10*time.Second)
	teranode.waitForAccepted(t, child.TxID(), 15*time.Second)

	waitForArcadeStatus(t, rt, parent.TxID(), "ACCEPTED_BY_NETWORK", 10*time.Second)
	waitForArcadeStatus(t, rt, child.TxID(), "ACCEPTED_BY_NETWORK", 10*time.Second)

	// The ordering artifact must never have produced a verdict.
	if got := arcadeTxStatus(t, rt, child.TxID()); got == "REJECTED" {
		t.Fatalf("child ended REJECTED — the missing-parent safety net failed")
	}
	assertParentChildNeverCohabits(t, teranode.snapshotBatches(), [][]*bt.Tx{chain})
}

// TestSmoke_OrphanChild_ParksWithoutWedgingPipeline pins the no-wedge
// invariant: a child whose parent NEVER arrives exhausts its (small)
// requeue budget and parks at PENDING_RETRY — not REJECTED — while
// unrelated traffic keeps flowing through the same partitioned pipeline.
func TestSmoke_OrphanChild_ParksWithoutWedgingPipeline(t *testing.T) {
	teranode := newDependencyAwareTeranode(t)
	rt := startArcadeSmoke(t, smokeOptions{
		TeranodeURL:           teranode.URL(),
		PropagationPartitions: 4,
		// Harness defaults: RetryMaxAttempts=1, RetryBackoffMs=50 — the
		// child parks after one retry.
	})

	chain := BuildChains(ChainOpts{TotalTxs: 2, MinDepth: 2, MaxDepth: 2, Seed: 11})[0]
	orphanChild := chain[1] // its parent is never submitted

	if err := postTxs(rt, []*bt.Tx{orphanChild}); err != nil {
		t.Fatalf("submit orphan child: %v", err)
	}
	waitForArcadeStatus(t, rt, orphanChild.TxID(), "PENDING_RETRY", 15*time.Second)

	// The pipeline must not be wedged: an unrelated tx submitted after the
	// orphan still broadcasts and terminalizes.
	unrelated := BuildChains(ChainOpts{TotalTxs: 1, MinDepth: 1, MaxDepth: 1, Seed: 13})[0][0]
	if err := postTxs(rt, []*bt.Tx{unrelated}); err != nil {
		t.Fatalf("submit unrelated: %v", err)
	}
	waitForArcadeStatus(t, rt, unrelated.TxID(), "ACCEPTED_BY_NETWORK", 15*time.Second)

	if got := arcadeTxStatus(t, rt, orphanChild.TxID()); got == "REJECTED" {
		t.Fatalf("orphan child was REJECTED — missing-parent must park, never reject")
	}
}
