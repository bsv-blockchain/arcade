package propagation

import (
	"context"
	"net/http"
	"testing"
	"time"

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
