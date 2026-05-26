//go:build e2e

package harness

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// BroadcastTx submits a single transaction to arcade via POST /tx in
// Extended Format. Arcade's intake validator runs script execution and
// fee verification against the per-input source data EF carries; raw
// (non-EF) bytes would be rejected for "PreviousTx not supplied".
// Returns the txid arcade derived from the parsed transaction.
func BroadcastTx(ctx context.Context, t *testing.T, rt *ArcadeRuntime, tx *bt.Tx) (string, error) {
	t.Helper()
	body := bytes.NewReader(tx.ExtendedBytes())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rt.BaseURL+"/tx", body)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("post tx: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("POST /tx status %d: %s", resp.StatusCode, respBody)
	}
	// arcade returns 202 Accepted with a JSON envelope identifying the
	// canonical txid it parsed (which can differ from the caller's
	// computed txid when the input is in extended format). Prefer the
	// server-side txid; fall back to the caller's hash.
	var resp1 struct {
		TxID string `json:"txid"`
	}
	_ = json.Unmarshal(respBody, &resp1)
	if resp1.TxID != "" {
		return resp1.TxID, nil
	}
	return tx.TxID(), nil
}

// txStatusResponse is the shape returned by GET /tx/:txid. We only
// decode the fields we actually assert on.
type txStatusResponse struct {
	TxID        string `json:"txid"`
	TxStatus    string `json:"txStatus"`
	BlockHash   string `json:"blockHash,omitempty"`
	BlockHeight uint64 `json:"blockHeight,omitempty"`
	MerklePath  string `json:"merklePath,omitempty"`
	ExtraInfo   string `json:"extraInfo,omitempty"`
}

// GetTxStatus fetches a single tx's status from arcade. Returns
// (status, true) on 200, (zero, false) on 404, error otherwise.
func GetTxStatus(ctx context.Context, rt *ArcadeRuntime, txid string) (txStatusResponse, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rt.BaseURL+"/tx/"+txid, nil)
	if err != nil {
		return txStatusResponse{}, false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return txStatusResponse{}, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return txStatusResponse{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return txStatusResponse{}, false, fmt.Errorf("GET /tx/%s status %d: %s", txid, resp.StatusCode, body)
	}
	var out txStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return txStatusResponse{}, false, fmt.Errorf("decode tx status: %w", err)
	}
	return out, true, nil
}

// BroadcastRawTxs submits each (txid, rawTxBytes) pair via POST /tx
// and returns the txids in stable order. Pacing matches the e2e
// smoke test pattern — one POST per tx, no batching, since per-tx
// status assertions are easier to read than batch acknowledgements.
//
// txids in the returned slice are sorted so callers can pass them to
// WaitForMined without worrying about map iteration order.
func BroadcastRawTxs(ctx context.Context, t *testing.T, rt *ArcadeRuntime, rawTxs map[string][]byte) ([]string, error) {
	t.Helper()
	got := make([]string, 0, len(rawTxs))
	for id, raw := range rawTxs {
		tx, err := bt.NewTxFromBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("parse raw tx %s: %w", id, err)
		}
		assigned, err := BroadcastTx(ctx, t, rt, tx)
		if err != nil {
			return nil, fmt.Errorf("broadcast %s: %w", id, err)
		}
		if assigned != id {
			// arcade's POST /tx returns the canonical txid arcade
			// derived from the parsed bytes; if it disagrees with
			// our fixture's recorded txid, something is wrong with
			// the raw-tx fetch (or the tx version differs from
			// what WoC normalized).
			return nil, fmt.Errorf("txid mismatch for %s: arcade reports %s", id, assigned)
		}
		got = append(got, id)
	}
	sort.Strings(got)
	return got, nil
}

// ReprocessResponse is the 202-Accepted body merkle-service returns
// from POST /reprocess. DataHubURL echoes the candidate URL the
// endpoint resolved to (i.e., which fallback / registered datahub
// served the block on the probe).
type ReprocessResponse struct {
	Status     string `json:"status"`
	BlockHash  string `json:"blockHash"`
	DataHubURL string `json:"dataHubUrl"`
}

// TriggerReprocess POSTs to merkle-service's /reprocess endpoint
// with the supplied block hash + arcade callback URL + token. It
// expects HTTP 202 and returns the parsed response so callers can
// assert which datahub URL got picked up. Any non-202 surfaces as
// an error with the body included for diagnosis.
func TriggerReprocess(ctx context.Context, merkleHostURL, blockHash, callbackURL, callbackToken string) (*ReprocessResponse, error) {
	body, err := json.Marshal(map[string]string{
		"blockHash":     blockHash,
		"callbackUrl":   callbackURL,
		"callbackToken": callbackToken,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal reprocess body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, merkleHostURL+"/reprocess", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build reprocess request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post reprocess: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("POST /reprocess: status %d body=%s", resp.StatusCode, respBody)
	}
	var out ReprocessResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decode reprocess response: %w (body=%s)", err, respBody)
	}
	return &out, nil
}

// WaitForMerkleRegistration polls merkle-service's GET /api/lookup/<txid>
// until the registered callback URL list is non-empty or the timeout
// elapses. Hoisted from smoke_test.go so real-block scenarios can
// reuse it.
func WaitForMerkleRegistration(ctx context.Context, merkleHostURL, txid string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("%s/api/lookup/%s", merkleHostURL, txid)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var lookup struct {
					CallbackUrls []string `json:"callbackUrls"`
				}
				if jErr := json.Unmarshal(body, &lookup); jErr == nil && len(lookup.CallbackUrls) > 0 {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out polling %s", url)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// AssertMerklePathsMatchHeaderRoot fetches each tx's status from arcade,
// parses its merklePath, and asserts ComputeRoot reproduces the header
// merkle root recorded in the fixture. Catches "MINED happened but the
// path is wrong" silently — a confused-deputy bug between merkle-service
// and arcade's compound BUMP build that WaitForMined alone wouldn't
// catch.
//
// expectedHeaderRoot is the display-order hex string from
// fixture.MerkleRoot.
func AssertMerklePathsMatchHeaderRoot(t *testing.T, rt *ArcadeRuntime, expectedHeaderRoot string, txids []string) {
	t.Helper()
	wantBytes, err := hex.DecodeString(expectedHeaderRoot)
	if err != nil {
		t.Fatalf("decode expected merkle root %s: %v", expectedHeaderRoot, err)
	}
	if len(wantBytes) != chainhash.HashSize {
		t.Fatalf("expected 32-byte merkle root, got %d", len(wantBytes))
	}
	// Reverse to internal byte order for comparison against
	// MerklePath.ComputeRoot's output.
	wantInternal := make([]byte, chainhash.HashSize)
	for i := range wantBytes {
		wantInternal[i] = wantBytes[chainhash.HashSize-1-i]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, id := range txids {
		st, ok, err := GetTxStatus(ctx, rt, id)
		if err != nil {
			t.Errorf("status %s: %v", id, err)
			continue
		}
		if !ok {
			t.Errorf("status %s: not found", id)
			continue
		}
		if st.MerklePath == "" {
			t.Errorf("status %s: empty merklePath", id)
			continue
		}
		mpBytes, err := hex.DecodeString(st.MerklePath)
		if err != nil {
			t.Errorf("decode merklePath for %s: %v", id, err)
			continue
		}
		mp, err := sdkTx.NewMerklePathFromBinary(mpBytes)
		if err != nil {
			t.Errorf("parse merklePath for %s: %v", id, err)
			continue
		}
		txidHash, err := chainhash.NewHashFromHex(id)
		if err != nil {
			t.Errorf("parse txid %s: %v", id, err)
			continue
		}
		root, err := mp.ComputeRoot(txidHash)
		if err != nil {
			t.Errorf("ComputeRoot for %s: %v", id, err)
			continue
		}
		if !bytes.Equal(root[:], wantInternal) {
			t.Errorf("merkle root mismatch for %s: got %x want %x", id, root[:], wantInternal)
		}
	}
}

// WaitForMined polls /tx/:txid for every txid in the slice until each
// reports MINED (or IMMUTABLE) with a non-empty merklePath, or the
// timeout elapses. On failure, the first stuck txid is reported with
// its last-observed status so debugging starts with a concrete signal.
func WaitForMined(ctx context.Context, t *testing.T, rt *ArcadeRuntime, txids []string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	pending := make(map[string]struct{}, len(txids))
	for _, id := range txids {
		pending[id] = struct{}{}
	}
	last := txStatusResponse{}
	lastSeen := ""
	for len(pending) > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for MINED: %d/%d still pending; last seen txid=%s status=%s extra=%s",
				len(pending), len(txids), lastSeen, last.TxStatus, last.ExtraInfo)
		}
		for id := range pending {
			st, ok, err := GetTxStatus(ctx, rt, id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			last = st
			lastSeen = id
			if st.TxStatus == "REJECTED" || st.TxStatus == "DOUBLE_SPEND_ATTEMPTED" {
				return fmt.Errorf("tx %s rejected with status %s extra=%s", id, st.TxStatus, st.ExtraInfo)
			}
			if (st.TxStatus == "MINED" || st.TxStatus == "IMMUTABLE") && st.MerklePath != "" {
				delete(pending, id)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil
}
