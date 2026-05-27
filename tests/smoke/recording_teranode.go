//go:build smoke

package smoke

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// BatchRecord captures one POST that arcade made to the fake teranode
// /txs endpoint. Seq is monotonic in the order requests were received
// (assigned under the recorder's mutex), so assertions can compare two
// batches' relative positions deterministically. ReceivedAt is wall-clock
// time at the moment the recorder grabbed the lock.
type BatchRecord struct {
	Seq        int
	TxIDs      []string
	ReceivedAt time.Time
}

// recordingTeranode is the httptest.Server stand-in arcade points at via
// DatahubURLs. It accepts both POST /tx (single) and POST /txs (batch)
// because the propagator can fall back to the single-tx path; either way
// the body is concatenated raw transaction bytes, which we parse back
// with the same sdkTx.NewTransactionFromStream call arcade's intake uses
// — round-trip symmetry guarantees the txids match.
//
// Responses are always 200 OK with empty body, matching the existing
// e2e datahub stub (tests/e2e/harness/datahub.go:179) and satisfying
// teranode.Client.SubmitTransactions's "all peers returned 200 → accepted"
// classification.
type recordingTeranode struct {
	server *httptest.Server

	mu      sync.Mutex
	batches []BatchRecord
	seen    map[string]struct{}
	cond    *sync.Cond
}

func newRecordingTeranode(t *testing.T) *recordingTeranode {
	t.Helper()
	r := &recordingTeranode{
		seen: make(map[string]struct{}),
	}
	r.cond = sync.NewCond(&r.mu)
	r.server = httptest.NewServer(http.HandlerFunc(r.handle))
	t.Cleanup(r.server.Close)
	return r
}

// URL returns the base URL the test feeds into arcade as a DatahubURL.
func (r *recordingTeranode) URL() string {
	return r.server.URL
}

func (r *recordingTeranode) handle(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.Method == http.MethodPost && (req.URL.Path == "/txs" || req.URL.Path == "/tx"):
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		txids, err := parseBatchTxIDs(body)
		if err != nil {
			http.Error(w, "parse batch: "+err.Error(), http.StatusBadRequest)
			return
		}
		r.recordBatch(txids)
		w.WriteHeader(http.StatusOK)
	default:
		// Health probes and unknown paths fall through to 200 so the
		// teranode client's connectivity check doesn't mark the
		// endpoint unhealthy and stop routing batches at it.
		w.WriteHeader(http.StatusOK)
	}
}

// parseBatchTxIDs walks the concatenated-raw-bytes body in the same way
// handleSubmitTransactions parses inbound batches. Returns the txids in
// the order they appeared on the wire.
func parseBatchTxIDs(body []byte) ([]string, error) {
	var txids []string
	offset := 0
	for offset < len(body) {
		tx, used, err := sdkTx.NewTransactionFromStream(body[offset:])
		if err != nil {
			return nil, fmt.Errorf("offset %d (%d txs parsed): %w", offset, len(txids), err)
		}
		if used == 0 {
			break
		}
		txids = append(txids, tx.TxID().String())
		offset += used
	}
	return txids, nil
}

func (r *recordingTeranode) recordBatch(txids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, BatchRecord{
		Seq:        len(r.batches),
		TxIDs:      append([]string(nil), txids...),
		ReceivedAt: time.Now(),
	})
	for _, id := range txids {
		r.seen[id] = struct{}{}
	}
	r.cond.Broadcast()
}

// Snapshot returns a deep copy of every batch recorded so far. Safe to
// call concurrently with active POSTs.
func (r *recordingTeranode) Snapshot() []BatchRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]BatchRecord, len(r.batches))
	for i, b := range r.batches {
		ids := make([]string, len(b.TxIDs))
		copy(ids, b.TxIDs)
		out[i] = BatchRecord{Seq: b.Seq, TxIDs: ids, ReceivedAt: b.ReceivedAt}
	}
	return out
}

// WaitForTxCount blocks until at least n distinct txids have been
// observed across all batches, or timeout elapses. Returns an error
// reporting the observed count so callers can fail with a useful
// diagnostic. Uses a sync.Cond + a deadline goroutine instead of a poll
// loop so a fast pipeline doesn't burn CPU on tight intervals.
func (r *recordingTeranode) WaitForTxCount(n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	r.mu.Lock()
	defer r.mu.Unlock()
	// Wake the waiter when the deadline arrives so it can return a
	// timeout instead of blocking forever on Wait().
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-done:
		case <-time.After(timeout):
			r.mu.Lock()
			r.cond.Broadcast()
			r.mu.Unlock()
		}
	}()
	for len(r.seen) < n {
		if time.Now().After(deadline) {
			return fmt.Errorf("waited %s for %d txs, only saw %d", timeout, n, len(r.seen))
		}
		r.cond.Wait()
	}
	return nil
}
