package propagation

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
)

// TestReapOnce_RebroadcastsStaleReceived is the issue #83 / F-025 regression:
// a validated tx stranded at RECEIVED past staleReceivedAge (its no-verdict
// broadcast requeue was lost, or an intake Kafka-publish failure was never
// retried) must be rebroadcast by the reaper. Before RECEIVED was brought into
// reapOnce's scope this asserted register count = 0; it now expects 1.
func TestReapOnce_RebroadcastsStaleReceived(t *testing.T) {
	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{
			TxID:      "tx-stranded",
			Status:    models.StatusReceived,
			RawTx:     []byte{0xde, 0xad, 0xbe, 0xef},
			Timestamp: time.Now().Add(-2 * time.Hour), // older than staleReceivedAge
		},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	p.reapOnce(context.Background())

	if got := log.count("register:"); got != 1 {
		t.Fatalf("register count = %d, want 1 (stale RECEIVED row should be rebroadcast)", got)
	}
	if got := log.count("register:tx-stranded"); got != 1 {
		t.Errorf("stale tx not the one registered; events=%v", log.all())
	}
	if got := log.count("broadcast"); got != 1 {
		t.Errorf("broadcast count = %d, want 1", got)
	}
}

// TestReapOnce_ReceivedSelection pins which rows the RECEIVED arm picks up:
// only rows past staleReceivedAge that still carry RawTx. Recent rows (still in
// the normal intake/requeue path), body-less rows, and terminal rows are left
// alone. The stale SEEN_ON_NETWORK row is a regression guard that the original
// rebroadcast behavior is unchanged.
func TestReapOnce_ReceivedSelection(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * time.Hour)    // > staleReceivedAge, < staleScanLookback
	recent := now.Add(-1 * time.Minute) // < staleReceivedAge

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "recv-stale", Status: models.StatusReceived, RawTx: []byte{0x01}, Timestamp: stale},
		{TxID: "recv-recent", Status: models.StatusReceived, RawTx: []byte{0x02}, Timestamp: recent},
		{TxID: "recv-nobody", Status: models.StatusReceived, Timestamp: stale}, // no RawTx → skip
		{TxID: "seen-stale", Status: models.StatusSeenOnNetwork, RawTx: []byte{0x03}, Timestamp: stale},
		{TxID: "mined-old", Status: models.StatusMined, RawTx: []byte{0x04}, Timestamp: stale}, // terminal → skip
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)

	p.reapOnce(context.Background())

	want := map[string]int{
		"register:recv-stale":  1, // stale RECEIVED with body → rebroadcast
		"register:seen-stale":  1, // stale SEEN with body → rebroadcast (unchanged)
		"register:recv-recent": 0, // too recent
		"register:recv-nobody": 0, // no RawTx
		"register:mined-old":   0, // terminal
	}
	for prefix, exp := range want {
		if got := log.count(prefix); got != exp {
			t.Errorf("%q count = %d, want %d; events=%v", prefix, got, exp, log.all())
		}
	}
	if got := log.count("register:"); got != 2 {
		t.Errorf("total register count = %d, want 2; events=%v", got, log.all())
	}
}

// gaugeVal reads a labeled gauge value for assertions.
func gaugeVal(t *testing.T, vec *prometheus.GaugeVec, label string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(label))
}

// TestReapOnce_StuckTransientCensus pins the stuck-transient gauges (issue:
// alerting on txs whose SEEN state-transfer never arrived):
//   - RECEIVED and ACCEPTED_BY_NETWORK rows older than stuckTransientAge are
//     counted — INCLUDING body-less rows the rebroadcast arm skips;
//   - ACCEPTED_BY_NETWORK is counted but NOT rebroadcast (no behavior change);
//   - fresh transient rows and terminal rows are not counted;
//   - the oldest-age gauge reflects the oldest stuck row per status.
func TestReapOnce_StuckTransientCensus(t *testing.T) {
	now := time.Now()
	stale2h := now.Add(-2 * time.Hour)
	stale3h := now.Add(-3 * time.Hour)
	recent := now.Add(-10 * time.Minute)

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		// counted: RECEIVED stuck (with and without body)
		{TxID: "recv-stale-body", Status: models.StatusReceived, RawTx: []byte{0x01}, Timestamp: stale2h},
		{TxID: "recv-stale-nobody", Status: models.StatusReceived, Timestamp: stale3h},
		// counted: ACCEPTED_BY_NETWORK stuck (the previously-invisible case)
		{TxID: "acc-stale", Status: models.StatusAcceptedByNetwork, RawTx: []byte{0x02}, Timestamp: stale2h},
		// not counted: fresh transient rows
		{TxID: "recv-recent", Status: models.StatusReceived, RawTx: []byte{0x03}, Timestamp: recent},
		{TxID: "acc-recent", Status: models.StatusAcceptedByNetwork, Timestamp: recent},
		// not counted: terminal + other states
		{TxID: "mined-old", Status: models.StatusMined, Timestamp: stale3h},
		{TxID: "seen-old", Status: models.StatusSeenOnNetwork, RawTx: []byte{0x04}, Timestamp: stale2h},
	}

	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	if got := gaugeVal(t, metrics.StuckTransientTxs, string(models.StatusReceived)); got != 2 {
		t.Errorf("stuck RECEIVED = %v, want 2 (body-less rows must be counted)", got)
	}
	if got := gaugeVal(t, metrics.StuckTransientTxs, string(models.StatusAcceptedByNetwork)); got != 1 {
		t.Errorf("stuck ACCEPTED_BY_NETWORK = %v, want 1", got)
	}
	// Oldest RECEIVED is the 3h row: age must exceed 2.5h.
	if got := gaugeVal(t, metrics.OldestTransientTxAge, string(models.StatusReceived)); got < 2.5*3600 {
		t.Errorf("oldest RECEIVED age = %vs, want > 9000s", got)
	}
	// ACCEPTED_BY_NETWORK must NOT be rebroadcast (census only).
	if got := log.count("register:acc-stale"); got != 0 {
		t.Errorf("ACCEPTED_BY_NETWORK row was rebroadcast; census must not change rebroadcast behavior")
	}
	// Rebroadcast arm unchanged: stale RECEIVED with body + stale SEEN.
	if got := log.count("register:recv-stale-body"); got != 1 {
		t.Errorf("stale RECEIVED with body not rebroadcast; events=%v", log.all())
	}
}

// TestReapOnce_StuckTransientCensus_ZeroesWhenClear: a scan finding nothing
// stuck must reset the gauges to 0 (no stale non-zero readings after a
// backlog drains).
func TestReapOnce_StuckTransientCensus_ZeroesWhenClear(t *testing.T) {
	// Seed non-zero values as if a previous tick found stuck rows.
	metrics.StuckTransientTxs.WithLabelValues(string(models.StatusReceived)).Set(7)
	metrics.OldestTransientTxAge.WithLabelValues(string(models.StatusAcceptedByNetwork)).Set(1234)

	log := &eventLog{}
	ms := newMockStore()
	ms.replayRows = []*models.TransactionStatus{
		{TxID: "recv-recent", Status: models.StatusReceived, RawTx: []byte{0x01}, Timestamp: time.Now().Add(-time.Minute)},
	}
	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	for _, status := range stuckTransientStatuses {
		if got := gaugeVal(t, metrics.StuckTransientTxs, string(status)); got != 0 {
			t.Errorf("stuck %s = %v, want 0 after clean scan", status, got)
		}
		if got := gaugeVal(t, metrics.OldestTransientTxAge, string(status)); got != 0 {
			t.Errorf("oldest %s age = %v, want 0 after clean scan", status, got)
		}
	}
}

// TestReapOnce_CensusUncappedByRebroadcastBatch: the census must keep
// counting past the rebroadcast batch cap (the old walk aborted at
// reaperRebroadcastBatch, silently truncating any census).
func TestReapOnce_CensusUncappedByRebroadcastBatch(t *testing.T) {
	now := time.Now()
	stale := now.Add(-2 * time.Hour)

	log := &eventLog{}
	ms := newMockStore()
	total := reaperRebroadcastBatch + 50
	for i := 0; i < total; i++ {
		ms.replayRows = append(ms.replayRows, &models.TransactionStatus{
			TxID:      fmt.Sprintf("recv-%04d", i),
			Status:    models.StatusReceived,
			RawTx:     []byte{0x01},
			Timestamp: stale,
		})
	}
	merkleSrv := newMerkleServer(log, http.StatusOK)
	defer merkleSrv.Close()
	teranodeSrv := newTeranodeServer(log, http.StatusOK)
	defer teranodeSrv.Close()

	p := newPropagator(merkleSrv.URL, teranodeSrv.URL, ms)
	p.reapOnce(context.Background())

	if got := gaugeVal(t, metrics.StuckTransientTxs, string(models.StatusReceived)); got != float64(total) {
		t.Errorf("stuck RECEIVED = %v, want %d (census must not be capped at the rebroadcast batch)", got, total)
	}
	if got := log.count("register:"); got != reaperRebroadcastBatch {
		t.Errorf("rebroadcast count = %d, want %d (batch cap unchanged)", got, reaperRebroadcastBatch)
	}
}
