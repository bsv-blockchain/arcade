package aerospike

import (
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"

	"github.com/bsv-blockchain/arcade/models"
)

func TestStatusUpdateReadBinsAreStatusFieldsOnly(t *testing.T) {
	want := map[string]struct{}{
		"status":       {},
		"timestamp":    {},
		"block_hash":   {},
		"block_height": {},
		"extra_info":   {},
	}
	if len(statusUpdateReadBins) != len(want) {
		t.Fatalf("statusUpdateReadBins = %v, want the five status fields", statusUpdateReadBins)
	}
	for _, bin := range statusUpdateReadBins {
		if _, ok := want[bin]; !ok {
			t.Fatalf("status update read fetches unrelated bin %q", bin)
		}
		delete(want, bin)
	}
	for _, forbidden := range []string{"raw_tx", "merkle_path", "competing_txs"} {
		for _, bin := range statusUpdateReadBins {
			if bin == forbidden {
				t.Fatalf("status update read must not fetch %s", forbidden)
			}
		}
	}
}

func TestStatusFromUpdateBinsIgnoresUnrelatedBins(t *testing.T) {
	rec := &aero.Record{Bins: aero.BinMap{
		"status":        "MINED",
		"timestamp":     1_700_000_000_000,
		"block_hash":    "abc",
		"block_height":  10,
		"extra_info":    "note",
		"raw_tx":        []byte("do-not-decode"),
		"merkle_path":   []byte("do-not-decode"),
		"competing_txs": []byte(`["other"]`),
	}}
	got := statusFromUpdateBins(rec, "txid")
	if got.TxID != "txid" || got.Status != models.StatusMined || got.BlockHash != "abc" || got.BlockHeight != 10 || got.ExtraInfo != "note" {
		t.Fatalf("decoded status = %+v", got)
	}
	if !got.Timestamp.Equal(time.UnixMilli(1_700_000_000_000)) {
		t.Fatalf("timestamp = %s", got.Timestamp)
	}
	if len(got.RawTx) != 0 || len(got.MerklePath) != 0 || len(got.CompetingTxs) != 0 {
		t.Fatalf("unrelated bins were decoded: %+v", got)
	}
}
