//go:build e2e

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
)

// BroadcastTx submits a single transaction to arcade via POST /tx.
// The body is the raw tx bytes; arcade auto-detects the format.
// Returns the txid arcade derived from the parsed transaction.
func BroadcastTx(ctx context.Context, t *testing.T, rt *ArcadeRuntime, tx *bt.Tx) (string, error) {
	t.Helper()
	body := bytes.NewReader(tx.Bytes())
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
