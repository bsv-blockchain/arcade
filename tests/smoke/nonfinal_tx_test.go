//go:build smoke

package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/block"
)

// stubChaintracksMTP is the median-time-past the stub chain reports: every
// header carries this timestamp, so the BIP113 median is exactly this value.
const stubChaintracksMTP = uint32(1_600_000_000)

// newStubChaintracks serves the two go-chaintracks remote-client endpoints
// the finality gate's Checker uses: /v2/tip (JSON BlockHeader) and
// /v2/headers (raw concatenated 80-byte headers). All headers carry
// stubChaintracksMTP as their timestamp.
func newStubChaintracks(t *testing.T) *httptest.Server {
	t.Helper()
	tipHeight := uint32(120)

	makeHeader := func() *block.Header {
		return &block.Header{Version: 1, Timestamp: stubChaintracksMTP, Bits: 0x207fffff}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/tip", func(w http.ResponseWriter, _ *http.Request) {
		h := makeHeader()
		tip := &chaintracks.BlockHeader{Header: h, Height: tipHeight, Hash: h.Hash()}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(tip)
	})
	mux.HandleFunc("/v2/headers", func(w http.ResponseWriter, r *http.Request) {
		count, err := strconv.Atoi(r.URL.Query().Get("count"))
		if err != nil || count <= 0 {
			http.Error(w, "bad count", http.StatusBadRequest)
			return
		}
		for i := 0; i < count; i++ {
			_, _ = w.Write(makeHeader().Bytes())
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// newLockTimeTx mints a validator-passing tx (same OP_TRUE fake-utxo shape
// as newValidatableTx) with an explicit locktime and input sequence.
func newLockTimeTx(lockTime, sequence uint32) *bt.Tx {
	tx := newValidatableTx(bytes.Repeat([]byte{0x02}, 32), 0, 0)
	tx.LockTime = lockTime
	tx.Inputs[0].SequenceNumber = sequence
	return tx
}

// postSingleTx POSTs one tx in EF encoding to arcade's /tx endpoint and
// returns the status code and response body.
func postSingleTx(rt *arcadeRuntime, tx *bt.Tx) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rt.baseURL+"/tx", bytes.NewReader(tx.ExtendedBytes()))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("post /tx: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), nil
}

func getTxStatus(rt *arcadeRuntime, txid string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rt.baseURL+"/tx/"+txid, nil)
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("get /tx: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), nil
}

// TestSmoke_NonFinalTxRejectedAtIntake exercises the nLockTime/BIP113
// finality gate end-to-end through the real boot path (issues #245/#254):
// arcade boots with chaintracks.mode=remote pointed at a stub chain, a
// non-final tx is answered synchronously with an actionable 400 carrying
// ARC code 476 (never reaching teranode) — but NO verdict is persisted:
// non-finality is a condition of arcade's chain view, so GET /tx/{txid}
// keeps returning 404 ("no verdict") and resubmission policies keyed on
// no-verdict recover once the locktime expires. An otherwise-identical
// final tx broadcasts normally.
func TestSmoke_NonFinalTxRejectedAtIntake(t *testing.T) {
	recorder := newRecordingTeranode(t)
	stub := newStubChaintracks(t)
	rt := startArcadeSmoke(t, smokeOptions{
		TeranodeURL:          recorder.URL(),
		ChaintracksRemoteURL: stub.URL,
	})

	// Non-final: timestamp locktime one hour past the stub chain's MTP,
	// with a non-final input sequence — the exact shape from issue #245.
	nonFinal := newLockTimeTx(stubChaintracksMTP+3600, 0xfffffffe)
	code, body, err := postSingleTx(rt, nonFinal)
	if err != nil {
		t.Fatalf("submit non-final tx: %v", err)
	}
	if code != http.StatusBadRequest {
		t.Fatalf("non-final tx: expected 400, got %d: %s", code, body)
	}
	if !strings.Contains(body, "transaction is not final") {
		t.Errorf("response %q missing finality reason", body)
	}
	if !strings.Contains(body, `"status":476`) {
		t.Errorf("response %q missing ARC 476 status code", body)
	}

	// Zero-state contract: no verdict is persisted for a non-final tx, so
	// GET stays 404 — the "no verdict" answer resubmission policies key on.
	code, body, err = getTxStatus(rt, nonFinal.TxID())
	if err != nil {
		t.Fatalf("get non-final tx status: %v", err)
	}
	if code != http.StatusNotFound {
		t.Fatalf("GET /tx/%s: expected 404 (no verdict persisted), got %d: %s", nonFinal.TxID(), code, body)
	}

	// Final: locktime already below the MTP — must broadcast normally.
	final := newLockTimeTx(stubChaintracksMTP-3600, 0xfffffffe)
	code, body, err = postSingleTx(rt, final)
	if err != nil {
		t.Fatalf("submit final tx: %v", err)
	}
	if code != http.StatusAccepted {
		t.Fatalf("final tx: expected 202, got %d: %s", code, body)
	}
	if err := recorder.WaitForTxCount(1, 30*time.Second); err != nil {
		t.Fatalf("final tx never reached teranode: %v", err)
	}
	for _, batch := range recorder.Snapshot() {
		for _, txid := range batch.TxIDs {
			if txid == nonFinal.TxID() {
				t.Fatalf("non-final tx %s was broadcast to teranode", txid)
			}
		}
	}
}
