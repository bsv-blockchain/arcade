//go:build smoke

package smoke

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
)

// TestSmoke_ChainedTxBatchOrdering is the load-bearing smoke test for
// arcade's propagation dispatcher's parent-child ordering guarantee.
//
// It builds a forest of ~10k chained txs (mixed depths 2–10), submits
// them via concurrent POST /txs calls, then captures every batch arcade
// sends to a fake teranode and asserts:
//
//  1. No batch ever contains both a parent and one of its children.
//  2. For every parent→child edge, the parent's batch was sent BEFORE
//     the child's batch.
//  3. Every tx broadcasts exactly once — no losses, no duplicates.
//  4. No batch exceeds Propagation.TeranodeMaxBatchSize.
//  5. The whole pipeline completes within the test window.
//
// (1) is the production invariant teranode's /txs endpoint relies on —
// the dispatcher holds children in heldMsgs until every parent leaves
// inFlight via ACCEPTED_BY_NETWORK. (2)–(4) are weaker corollaries that
// also catch interesting regressions: an ordering inversion would
// surface in (2), a requeue bug in (3), a chunking bug in (4).
func TestSmoke_ChainedTxBatchOrdering(t *testing.T) {
	const (
		totalTxs             = 10_000
		minDepth             = 2
		maxDepth             = 10
		submissionBatchSize  = 100
		concurrentSubmitters = 8
		broadcastWaitTimeout = 60 * time.Second
		teranodeMaxBatchSize = 1024 // matches buildSmokeConfig
		pipelineWallBudget   = 60 * time.Second
	)

	recorder := newRecordingTeranode(t)
	rt := startArcadeSmoke(t, smokeOptions{TeranodeURL: recorder.URL()})

	chains := BuildChains(ChainOpts{
		TotalTxs: totalTxs,
		MinDepth: minDepth,
		MaxDepth: maxDepth,
		Seed:     1, // deterministic; bump to time.Now() for stochastic runs
	})
	t.Logf("built %d chains, %d total txs", len(chains), countTxs(chains))

	// Flatten into submission batches. Each chain lives entirely
	// inside one submission so the dispatcher's ordering guarantee
	// holds: a single SendBatch on a single-partition Kafka topic
	// preserves order within the batch, so parent always reaches the
	// dispatcher before child. Concurrent submitters race the
	// submissions themselves, but since chains don't span submissions
	// and different chains share no edges, no cross-submission race
	// can produce a child-before-parent on the wire.
	submissions := buildSubmissionBatches(chains, submissionBatchSize)

	start := time.Now()

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrentSubmitters)
	for i, batch := range submissions {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, batch []*bt.Tx) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := postTxs(rt, batch); err != nil {
				t.Errorf("submission %d: %v", idx, err)
			}
		}(i, batch)
	}
	wg.Wait()
	t.Logf("submitted %d batches in %s", len(submissions), time.Since(start))

	if err := recorder.WaitForTxCount(countTxs(chains), broadcastWaitTimeout); err != nil {
		t.Fatalf("waiting for broadcast: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("pipeline drained in %s", elapsed)

	batches := recorder.Snapshot()
	t.Logf("recorded %d /txs batches", len(batches))

	assertParentChildNeverCohabits(t, batches, chains)
	assertParentPrecedesChild(t, batches, chains)
	assertEveryTxBroadcastExactlyOnce(t, batches, chains)
	assertChunkSize(t, batches, teranodeMaxBatchSize)

	if elapsed > pipelineWallBudget {
		t.Errorf("pipeline took %s for %d txs (budget %s) — likely a perf regression",
			elapsed, countTxs(chains), pipelineWallBudget)
	}
}

// buildSubmissionBatches groups chains into HTTP submission batches such
// that each chain lives entirely inside one submission. This is the
// crucial invariant for the test: a SendBatch on the single-partition
// Kafka topic preserves order, so parent reaches the dispatcher before
// child for txs in the same submission. Different chains share no
// edges, so the order in which submissions arrive at Kafka doesn't
// matter — there's no cross-submission race that can produce a
// child-before-parent on the wire.
//
// Chain order across submissions is shuffled (deterministically by
// seed) so concurrent submitters still stress the dispatcher with txs
// arriving in random order.
func buildSubmissionBatches(chains [][]*bt.Tx, batchSize int) [][]*bt.Tx {
	order := make([]int, len(chains))
	for i := range order {
		order[i] = i
	}
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test fixture
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

	batches := make([][]*bt.Tx, 0)
	cur := make([]*bt.Tx, 0, batchSize)
	for _, ci := range order {
		chain := chains[ci]
		// If adding the whole chain would push us past batchSize,
		// flush the current submission first. The chain is then
		// appended to the new (empty) submission.
		if len(cur)+len(chain) > batchSize && len(cur) > 0 {
			batches = append(batches, cur)
			cur = make([]*bt.Tx, 0, batchSize)
		}
		cur = append(cur, chain...)
	}
	if len(cur) > 0 {
		batches = append(batches, cur)
	}
	return batches
}

// postTxs POSTs a batch to arcade's /txs endpoint and returns nil on a
// 2xx response. The body is concatenated raw tx bytes in BSV Extended
// Format — arcade's validator (post PR #171) calls spv.Verify
// unconditionally, which needs each input's PreviousTxScript +
// PreviousTxSatoshis. Those bins live on the wire only in EF; sending
// standard-format bytes is rejected with "'PreviousTx' not supplied".
func postTxs(rt *arcadeRuntime, txs []*bt.Tx) error {
	var body bytes.Buffer
	for _, tx := range txs {
		body.Write(tx.ExtendedBytes())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rt.baseURL+"/txs", &body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("post /txs: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("/txs returned %d: %s", resp.StatusCode, respBody)
	}
	return nil
}

// countTxs sums total tx count across the forest.
func countTxs(chains [][]*bt.Tx) int {
	n := 0
	for _, c := range chains {
		n += len(c)
	}
	return n
}

// parentEdges builds a map child-txid → parent-txid from the chain
// forest. Roots (chain[0]) are absent.
func parentEdges(chains [][]*bt.Tx) map[string]string {
	parentOf := make(map[string]string)
	for _, c := range chains {
		for i := 1; i < len(c); i++ {
			parentOf[c[i].TxID()] = c[i-1].TxID()
		}
	}
	return parentOf
}

// assertParentChildNeverCohabits is the load-bearing assertion. For every
// recorded batch, it builds the txid set S and checks that no tx in S has
// a parent also in S. A failure prints the offending parent + child + the
// batch sequence number so the operator can map the regression back to a
// dispatcher state-machine bug.
func assertParentChildNeverCohabits(t *testing.T, batches []BatchRecord, chains [][]*bt.Tx) {
	t.Helper()
	parentOf := parentEdges(chains)
	for _, b := range batches {
		members := make(map[string]struct{}, len(b.TxIDs))
		for _, id := range b.TxIDs {
			members[id] = struct{}{}
		}
		for _, id := range b.TxIDs {
			parent, ok := parentOf[id]
			if !ok {
				continue
			}
			if _, parentInBatch := members[parent]; parentInBatch {
				t.Fatalf("batch seq=%d contains both parent %s and child %s — dispatcher parent-child invariant violated",
					b.Seq, parent, id)
			}
		}
	}
}

// assertParentPrecedesChild verifies that for every parent→child edge,
// the parent's first-appearance batch precedes the child's first-
// appearance batch. The dispatcher releases children only after the
// parent terminalizes (ACCEPTED_BY_NETWORK), which by construction is
// after the parent's batch has been broadcast.
func assertParentPrecedesChild(t *testing.T, batches []BatchRecord, chains [][]*bt.Tx) {
	t.Helper()
	parentOf := parentEdges(chains)
	firstBatchOf := make(map[string]int, countTxs(chains))
	for _, b := range batches {
		for _, id := range b.TxIDs {
			if _, exists := firstBatchOf[id]; !exists {
				firstBatchOf[id] = b.Seq
			}
		}
	}
	for child, parent := range parentOf {
		ps, pok := firstBatchOf[parent]
		cs, cok := firstBatchOf[child]
		if !pok || !cok {
			// Coverage gap is reported by assertEveryTxBroadcastExactlyOnce.
			continue
		}
		if ps >= cs {
			t.Errorf("child %s appeared in batch %d but parent %s appeared in batch %d (parent must precede child)",
				child, cs, parent, ps)
		}
	}
}

// assertEveryTxBroadcastExactlyOnce checks that every submitted tx is in
// exactly one batch. Missing txs reveal a loss path (e.g., dispatcher
// dropping a held tx on release); duplicate txs reveal a requeue bug.
func assertEveryTxBroadcastExactlyOnce(t *testing.T, batches []BatchRecord, chains [][]*bt.Tx) {
	t.Helper()
	count := make(map[string]int, countTxs(chains))
	for _, b := range batches {
		for _, id := range b.TxIDs {
			count[id]++
		}
	}
	missing, duplicated := 0, 0
	for _, c := range chains {
		for _, tx := range c {
			id := tx.TxID()
			switch count[id] {
			case 0:
				missing++
				if missing <= 5 {
					t.Errorf("tx %s never broadcast", id)
				}
			case 1:
				// OK
			default:
				duplicated++
				if duplicated <= 5 {
					t.Errorf("tx %s broadcast %d times (expected 1)", id, count[id])
				}
			}
		}
	}
	if missing > 5 {
		t.Errorf("(plus %d more missing txs, not printed)", missing-5)
	}
	if duplicated > 5 {
		t.Errorf("(plus %d more duplicated txs, not printed)", duplicated-5)
	}
}

// assertChunkSize sanity-checks the broadcastInChunks layer: every
// outbound batch fits the configured TeranodeMaxBatchSize. A regression
// here would indicate the chunker is no longer slicing.
func assertChunkSize(t *testing.T, batches []BatchRecord, maxBatchSize int) {
	t.Helper()
	for _, b := range batches {
		if len(b.TxIDs) > maxBatchSize {
			t.Errorf("batch seq=%d has %d txs, exceeds TeranodeMaxBatchSize=%d",
				b.Seq, len(b.TxIDs), maxBatchSize)
		}
	}
}
