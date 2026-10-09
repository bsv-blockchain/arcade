package propagation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/arcade/models"
)

// scriptedMerkle fails /watch with HTTP 500 for a per-txid quota, then
// returns 200. The quota is the number of failures still owed, so a tx
// with failsLeft=6 is registered on the seventh call.
type scriptedMerkle struct {
	mu        sync.Mutex
	failsLeft map[string]int
	calls     map[string]int
}

func (s *scriptedMerkle) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TxID string `json:"txid"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	s.calls[req.TxID]++
	left := s.failsLeft[req.TxID]
	if left > 0 {
		s.failsLeft[req.TxID] = left - 1
	}
	s.mu.Unlock()
	if left > 0 {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("merkle watch unavailable"))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *scriptedMerkle) callCount(txid string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[txid]
}

// countingTeranode records how many times each raw-tx marker was submitted.
// Markers are unique per tx so a duplicate broadcast is visible even when
// several txs share one POST /txs body.
type countingTeranode struct {
	mu    sync.Mutex
	posts int
	hits  map[string]int
}

func (c *countingTeranode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body := make([]byte, r.ContentLength)
	_, _ = r.Body.Read(body)
	c.mu.Lock()
	c.posts++
	for marker := range c.hits {
		if containsBytes(body, []byte(marker)) {
			c.hits[marker]++
		}
	}
	c.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (c *countingTeranode) count(marker string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits[marker]
}

func containsBytes(haystack, needle []byte) bool {
	if len(needle) == 0 || len(haystack) < len(needle) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func propMarker(i int) string {
	return fmt.Sprintf("MK%04d", i)
}

func propPayload(i int) (txid string, raw []byte) {
	raw = []byte(propMarker(i))
	return fmt.Sprintf("tx-%04d", i), raw
}

// pumpUntilTerminal flushes until every tx has a terminal propagation status
// or the deadline passes. Merkle and network requeues both land back on the
// dispatcher after requeueDelay, so an empty flush while work is still
// in flight just waits for that delay.
func pumpUntilTerminal(t *testing.T, p *Propagator, ms *mockStore, txids []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := flushSync(t, p); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if allPropagationTerminal(ms, txids) {
			return
		}
		wait := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(wait) && time.Now().Before(deadline) {
			if p.pendingDepth.Load() > 0 || allPropagationTerminal(ms, txids) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if allPropagationTerminal(ms, txids) {
			return
		}
	}
	t.Fatalf("txs not terminal after %s (pending=%d inflight=%d)", timeout, p.pendingDepth.Load(), p.inflightDepth.Load())
}

func allPropagationTerminal(ms *mockStore, txids []string) bool {
	for _, txid := range txids {
		st := ms.lastUpdateForTxid(txid)
		if st == nil {
			return false
		}
		switch st.Status {
		case models.StatusAcceptedByNetwork, models.StatusPendingRetry, models.StatusRejected:
		default:
			return false
		}
	}
	return true
}

func countStatus(ms *mockStore, txid string, status models.Status) int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	n := 0
	for _, upd := range ms.updates {
		if upd.TxID == txid && upd.Status == status {
			n++
		}
	}
	return n
}

// TestMerkleWatch_ExceedingSharedBudget_StillReachesNetwork is the
// production-shaped regression: six Merkle /watch HTTP 500s is enough to
// exceed propagation.retry_max_attempts (the fast path parks when the
// shared counter passes 5). Registration then succeeds and Teranode
// accepts. The tx must be ACCEPTED_BY_NETWORK, not parked, and Teranode
// must see it exactly once.
//
// On the coupled budget this parks at PENDING_RETRY on the sixth /watch
// failure, before any broadcast.
func TestMerkleWatch_ExceedingSharedBudget_StillReachesNetwork(t *testing.T) {
	const txid = "tx-six-watch-failures"
	marker := "MK-SIX"
	merkle := &scriptedMerkle{
		failsLeft: map[string]int{txid: 6},
		calls:     map[string]int{},
	}
	merkleSrv := httptest.NewServer(merkle)
	defer merkleSrv.Close()
	node := &countingTeranode{hits: map[string]int{marker: 0}}
	nodeSrv := httptest.NewServer(node)
	defer nodeSrv.Close()

	ms := newMockStore()
	p := newPropagator(merkleSrv.URL, nodeSrv.URL, ms)
	p.requeueDelay = 5 * time.Millisecond
	defer func() {
		p.dispatcherCancel()
		<-p.dispatcherDone
	}()

	raw := []byte(marker)
	if err := p.handleMessage(context.Background(), consumerMsg(mustPropMsg(txid, raw))); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	pumpUntilTerminal(t, p, ms, []string{txid}, 5*time.Second)

	st := ms.lastUpdateForTxid(txid)
	if st == nil || st.Status != models.StatusAcceptedByNetwork {
		got := "<nil>"
		if st != nil {
			got = string(st.Status) + " " + st.ExtraInfo
		}
		t.Fatalf("status = %s, want ACCEPTED_BY_NETWORK (a /watch burst past five attempts must not park the tx before Teranode)", got)
	}
	if got := countStatus(ms, txid, models.StatusPendingRetry); got != 0 {
		t.Errorf("PENDING_RETRY writes = %d, want 0", got)
	}
	if got := countStatus(ms, txid, models.StatusAcceptedByNetwork); got != 1 {
		t.Errorf("ACCEPTED_BY_NETWORK writes = %d, want 1 (no duplicate terminalization)", got)
	}
	if got := node.count(marker); got != 1 {
		t.Errorf("teranode submissions of %s = %d, want 1", marker, got)
	}
	if got := merkle.callCount(txid); got != 7 {
		t.Errorf("/watch calls = %d, want 7 (six failures then success)", got)
	}
}

func mustPropMsg(txid string, raw []byte) []byte {
	b, err := json.Marshal(propagationMsg{TXID: txid, RawTx: raw})
	if err != nil {
		panic(err)
	}
	return b
}
