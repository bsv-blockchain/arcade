package bumpcache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/internal/synthblock"
	"github.com/bsv-blockchain/arcade/models"
)

const testBlockHash = "0000000000000000000000000000000000000000000000000000000000000abc"

// countingFetch returns a fetch func serving the given BUMP and a counter of
// how many times it was invoked.
func countingFetch(data []byte) (func() ([]byte, error), *atomic.Int64) {
	var calls atomic.Int64
	return func() ([]byte, error) {
		calls.Add(1)
		return data, nil
	}, &calls
}

func minedStatus(txid string) *models.TransactionStatus {
	return &models.TransactionStatus{TxID: txid, Status: models.StatusMined, BlockHash: testBlockHash}
}

func TestEnrich_PopulatesVerifyingPathAndCaches(t *testing.T) {
	blk, err := synthblock.Build(8, 900100)
	if err != nil {
		t.Fatalf("synthblock.Build: %v", err)
	}
	c := New()
	fetch, calls := countingFetch(blk.BumpBytes)

	for _, txid := range blk.Txids {
		st := minedStatus(txid)
		c.Enrich(st, fetch)
		if len(st.MerklePath) == 0 {
			t.Fatalf("no merklePath for %s", txid)
		}
		if vErr := synthblock.VerifyMerklePath(st.MerklePath, txid, blk.Root); vErr != nil {
			t.Fatalf("merklePath does not verify for %s: %v", txid, vErr)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("fetch called %d times for one block, want 1", got)
	}
	if !c.Contains(testBlockHash) {
		t.Fatal("block should be cached after enrichment")
	}

	// Remove invalidates: the next enrichment re-fetches.
	c.Remove(testBlockHash)
	if c.Contains(testBlockHash) {
		t.Fatal("Remove must drop the cached entry")
	}
	c.Enrich(minedStatus(blk.Txids[0]), fetch)
	if got := calls.Load(); got != 2 {
		t.Fatalf("fetch called %d times after Remove, want 2", got)
	}
}

func TestEnrich_GuardsAreNoOps(t *testing.T) {
	c := New()
	fetch := func() ([]byte, error) {
		t.Fatal("fetch must not be called for guarded statuses")
		return nil, nil
	}

	c.Enrich(nil, fetch)
	c.Enrich(&models.TransactionStatus{TxID: "a", Status: models.StatusMined}, fetch) // no BlockHash
	c.Enrich(&models.TransactionStatus{TxID: "a", Status: models.StatusSeenOnNetwork, BlockHash: testBlockHash}, fetch)
	c.Enrich(&models.TransactionStatus{TxID: "a", Status: models.StatusMined, BlockHash: testBlockHash, MerklePath: []byte{1}}, fetch)
}

func TestEnrich_FetchErrorIsBestEffortAndRetried(t *testing.T) {
	blk, err := synthblock.Build(4, 900101)
	if err != nil {
		t.Fatalf("synthblock.Build: %v", err)
	}
	c := New()
	var calls atomic.Int64
	fail := errors.New("backend down")
	fetch := func() ([]byte, error) {
		if calls.Add(1) == 1 {
			return nil, fail
		}
		return blk.BumpBytes, nil
	}

	st := minedStatus(blk.Txids[0])
	c.Enrich(st, fetch)
	if len(st.MerklePath) != 0 {
		t.Fatal("failed fetch must leave the status unenriched")
	}
	if c.Contains(testBlockHash) {
		t.Fatal("errors must not be cached")
	}

	c.Enrich(st, fetch)
	if len(st.MerklePath) == 0 {
		t.Fatal("second enrichment should succeed once the backend recovers")
	}
}

func TestEnrich_ConcurrentMissesShareOneFetch(t *testing.T) {
	blk, err := synthblock.Build(16, 900102)
	if err != nil {
		t.Fatalf("synthblock.Build: %v", err)
	}
	c := New()

	var calls atomic.Int64
	gate := make(chan struct{})
	fetch := func() ([]byte, error) {
		calls.Add(1)
		<-gate // hold the flight open until every goroutine has joined
		return blk.BumpBytes, nil
	}

	const workers = 16
	var started, done sync.WaitGroup
	started.Add(workers)
	done.Add(workers)
	statuses := make([]*models.TransactionStatus, workers)
	for i := 0; i < workers; i++ {
		statuses[i] = minedStatus(blk.Txids[i])
		go func(st *models.TransactionStatus) {
			defer done.Done()
			started.Done()
			c.Enrich(st, fetch)
		}(statuses[i])
	}
	started.Wait()
	close(gate)
	done.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("fetch called %d times under concurrent misses, want 1 (singleflight)", got)
	}
	for i, st := range statuses {
		if len(st.MerklePath) == 0 {
			t.Fatalf("worker %d not enriched", i)
		}
		if vErr := synthblock.VerifyMerklePath(st.MerklePath, blk.Txids[i], blk.Root); vErr != nil {
			t.Fatalf("worker %d path does not verify: %v", i, vErr)
		}
	}
}

func TestLeafBudget_EvictsOldestButKeepsNewest(t *testing.T) {
	// Three 8-tx blocks; budget only fits one block's leaves, so each insert
	// evicts the previous entry but the newest always stays resident.
	blkA, err := synthblock.Build(8, 900103)
	if err != nil {
		t.Fatalf("Build A: %v", err)
	}
	blkB, err := synthblock.Build(8, 900104)
	if err != nil {
		t.Fatalf("Build B: %v", err)
	}

	hashA := "00000000000000000000000000000000000000000000000000000000000000aa"
	hashB := "00000000000000000000000000000000000000000000000000000000000000bb"

	c := newWithLimits(16, 1) // 1-leaf budget: any real compound exceeds it alone

	stA := &models.TransactionStatus{TxID: blkA.Txids[0], Status: models.StatusMined, BlockHash: hashA}
	fetchA, _ := countingFetch(blkA.BumpBytes)
	c.Enrich(stA, fetchA)
	if len(stA.MerklePath) == 0 {
		t.Fatal("A not enriched")
	}
	if !c.Contains(hashA) {
		t.Fatal("newest entry must stay cached even when alone over budget")
	}

	stB := &models.TransactionStatus{TxID: blkB.Txids[0], Status: models.StatusMined, BlockHash: hashB}
	fetchB, _ := countingFetch(blkB.BumpBytes)
	c.Enrich(stB, fetchB)
	if c.Contains(hashA) {
		t.Fatal("over-budget insert must evict the older entry")
	}
	if !c.Contains(hashB) {
		t.Fatal("newest entry must survive the budget sweep")
	}

	c.mu.Lock()
	total, entries := c.totalLeaves, c.lru.Len()
	c.mu.Unlock()
	if entries != 1 {
		t.Fatalf("want 1 resident entry, got %d", entries)
	}
	if want := idxLeaves(t, blkB.BumpBytes); total != want {
		t.Fatalf("leaf accounting drifted: total=%d want=%d", total, want)
	}
}

func TestEntryCap_EvictsAndKeepsAccountingExact(t *testing.T) {
	c := newWithLimits(2, 1<<20)
	hashes := []string{
		"0000000000000000000000000000000000000000000000000000000000000001",
		"0000000000000000000000000000000000000000000000000000000000000002",
		"0000000000000000000000000000000000000000000000000000000000000003",
	}
	var perBlockLeaves int
	for i, h := range hashes {
		blk, err := synthblock.Build(4, uint64(900110+i))
		if err != nil {
			t.Fatalf("Build %d: %v", i, err)
		}
		perBlockLeaves = idxLeaves(t, blk.BumpBytes)
		st := &models.TransactionStatus{TxID: blk.Txids[0], Status: models.StatusMined, BlockHash: h}
		fetch, _ := countingFetch(blk.BumpBytes)
		c.Enrich(st, fetch)
	}
	if c.Contains(hashes[0]) {
		t.Fatal("entry cap must evict the oldest block")
	}
	if !c.Contains(hashes[1]) || !c.Contains(hashes[2]) {
		t.Fatal("newer blocks must remain")
	}
	c.mu.Lock()
	total := c.totalLeaves
	c.mu.Unlock()
	if want := 2 * perBlockLeaves; total != want {
		t.Fatalf("leaf accounting drifted after cap eviction: total=%d want=%d", total, want)
	}
}

// idxLeaves parses a BUMP and reports its indexed leaf count.
func idxLeaves(t *testing.T, bumpBytes []byte) int {
	t.Helper()
	idx, err := bump.IndexCompound(bumpBytes)
	if err != nil {
		t.Fatalf("IndexCompound: %v", err)
	}
	return idx.Leaves()
}
