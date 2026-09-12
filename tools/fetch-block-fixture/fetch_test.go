package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	whatsonchain "github.com/mrz1836/go-whatsonchain"
)

// committedBlockHash is the mainnet block the e2e fixture was generated from.
// tests/e2e/fixtures/blocks/<hash>/block.bin is a real teranode block binary
// (txCount=1911, single subtree) we reuse to drive run() against real bytes.
const committedBlockHash = "000000000000000001bc8a601dd5f0659d36a9b077808850375dfa2d9f009396"

const (
	committedTxCount     = 1911
	committedCoinbaseTx  = "30e099b7318973c83020132c6b1b77f0a6bade9a4d28d36c004076d8a674d7c6"
	committedBlockBinRel = "../../tests/e2e/fixtures/blocks/" + committedBlockHash + "/block.bin"
)

// wocServer is a configurable httptest stand-in for the WhatsOnChain REST API.
// It serves the three endpoints the fixture tool uses: block-by-hash, block
// pages, and bulk raw transactions (POST /txs/hex).
type wocServer struct {
	*httptest.Server

	blockInline []string          // inline Tx from GetBlockByHash
	blockPages  [][]string        // additional pages; index i => page (i+1)
	rawByTxID   map[string]string // txid -> raw tx hex, for /txs/hex
	bulkCalls   atomic.Int32      // number of POST /txs/hex requests served
}

func newWOCServer(t *testing.T) *wocServer {
	t.Helper()
	ws := &wocServer{rawByTxID: map[string]string{}}
	ws.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && strings.Contains(path, "/block/hash/") && strings.Contains(path, "/page/"):
			parts := strings.Split(path, "/page/")
			n, err := strconv.Atoi(parts[len(parts)-1])
			if err != nil || n-1 < 0 || n-1 >= len(ws.blockPages) {
				http.Error(w, "no such page", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(ws.blockPages[n-1])
		case r.Method == http.MethodGet && strings.Contains(path, "/block/hash/"):
			resp := map[string]any{"tx": ws.blockInline}
			if len(ws.blockPages) > 0 {
				uris := make([]string, len(ws.blockPages))
				for i := range ws.blockPages {
					uris[i] = fmt.Sprintf("/block/hash/x/page/%d", i+1)
				}
				resp["pages"] = map[string]any{"uri": uris}
			}
			_ = json.NewEncoder(w).Encode(resp)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/txs/hex"):
			ws.bulkCalls.Add(1)
			var req struct {
				TxIDs []string `json:"txids"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			out := make([]map[string]string, 0, len(req.TxIDs))
			for _, id := range req.TxIDs {
				if h, ok := ws.rawByTxID[id]; ok {
					out = append(out, map[string]string{"txid": id, "hex": h})
				}
			}
			_ = json.NewEncoder(w).Encode(out)
		default:
			http.Error(w, "unexpected path: "+path, http.StatusNotFound)
		}
	}))
	t.Cleanup(ws.Close)
	return ws
}

// testClient builds a WhatsOnChain client pointed at baseURL with a high rate
// limit and no retries so unit tests stay fast.
func testClient(t *testing.T, baseURL string) whatsonchain.ClientInterface {
	t.Helper()
	c, err := whatsonchain.NewClient(context.Background(),
		whatsonchain.WithBaseURL(strings.TrimRight(baseURL, "/")+"/v1/"),
		whatsonchain.WithRateLimit(1000),
		whatsonchain.WithRequestRetryCount(0),
	)
	if err != nil {
		t.Fatalf("new woc client: %v", err)
	}
	return c
}

func mustScript(t *testing.T, hexStr string) *script.Script {
	t.Helper()
	s, err := script.NewFromHex(hexStr)
	if err != nil {
		t.Fatalf("script from hex: %v", err)
	}
	return s
}

// p2pkh returns a distinct throwaway locking script keyed by fill byte.
func p2pkh(t *testing.T, fill byte) *script.Script {
	t.Helper()
	return mustScript(t, "76a914"+strings.Repeat(fmt.Sprintf("%02x", fill), 20)+"88ac")
}

// TestFetchBlockTxIDs_SinglePage verifies the inline tx list is returned when a
// block has no additional pages.
func TestFetchBlockTxIDs_SinglePage(t *testing.T) {
	ws := newWOCServer(t)
	ws.blockInline = []string{"a", "b", "c"}

	got, err := fetchBlockTxIDs(context.Background(), testClient(t, ws.URL), "hash")
	if err != nil {
		t.Fatalf("fetchBlockTxIDs: %v", err)
	}
	if want := []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestFetchBlockTxIDs_Pagination verifies inline txids are concatenated with
// every advertised page, in order.
func TestFetchBlockTxIDs_Pagination(t *testing.T) {
	ws := newWOCServer(t)
	ws.blockInline = []string{"a", "b"}
	ws.blockPages = [][]string{{"c", "d"}, {"e"}}

	got, err := fetchBlockTxIDs(context.Background(), testClient(t, ws.URL), "hash")
	if err != nil {
		t.Fatalf("fetchBlockTxIDs: %v", err)
	}
	if want := []string{"a", "b", "c", "d", "e"}; !equalStrings(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestFetchBlockTxIDs_Empty verifies an empty tx list is treated as an error.
func TestFetchBlockTxIDs_Empty(t *testing.T) {
	ws := newWOCServer(t)
	ws.blockInline = nil

	_, err := fetchBlockTxIDs(context.Background(), testClient(t, ws.URL), "hash")
	if err == nil {
		t.Fatal("expected error for empty tx list, got nil")
	}
}

// TestBulkRawTxs_Batching verifies more than one batch is issued for >20 ids and
// that every id is returned, hex-decoded.
func TestBulkRawTxs_Batching(t *testing.T) {
	ws := newWOCServer(t)
	ids := make([]string, 25)
	for i := range ids {
		id := fmt.Sprintf("%064x", i)
		ids[i] = id
		ws.rawByTxID[id] = fmt.Sprintf("%02x00", i%256) // arbitrary valid hex
	}

	got, err := bulkRawTxs(context.Background(), testClient(t, ws.URL), ids)
	if err != nil {
		t.Fatalf("bulkRawTxs: %v", err)
	}
	if len(got) != len(ids) {
		t.Fatalf("got %d txs, want %d", len(got), len(ids))
	}
	if calls := ws.bulkCalls.Load(); calls != 2 {
		t.Fatalf("expected 2 batches for 25 ids (max 20/batch), got %d", calls)
	}
	wantFirst, _ := hex.DecodeString(ws.rawByTxID[ids[0]])
	if string(got[ids[0]]) != string(wantFirst) {
		t.Fatalf("decoded bytes mismatch for %s", ids[0])
	}
}

// TestBulkRawTxs_MissingTx verifies a requested id absent from the response is
// an error.
func TestBulkRawTxs_MissingTx(t *testing.T) {
	ws := newWOCServer(t)
	ws.rawByTxID["a"] = "0100"

	_, err := bulkRawTxs(context.Background(), testClient(t, ws.URL), []string{"a", "b"})
	if err == nil || !strings.Contains(err.Error(), "did not return tx b") {
		t.Fatalf("expected missing-tx error, got %v", err)
	}
}

// TestBulkRawTxs_BadHex verifies a non-hex payload is surfaced as an error.
func TestBulkRawTxs_BadHex(t *testing.T) {
	ws := newWOCServer(t)
	ws.rawByTxID["a"] = "zzzz"

	_, err := bulkRawTxs(context.Background(), testClient(t, ws.URL), []string{"a"})
	if err == nil || !strings.Contains(err.Error(), "decode hex") {
		t.Fatalf("expected decode-hex error, got %v", err)
	}
}

// TestBulkRawTxs_Empty verifies no request is made for an empty id list.
func TestBulkRawTxs_Empty(t *testing.T) {
	ws := newWOCServer(t)
	got, err := bulkRawTxs(context.Background(), testClient(t, ws.URL), nil)
	if err != nil {
		t.Fatalf("bulkRawTxs(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %d", len(got))
	}
	if calls := ws.bulkCalls.Load(); calls != 0 {
		t.Fatalf("expected 0 batches for empty input, got %d", calls)
	}
}

// TestEnrichToEF verifies the source script + satoshis are injected from the
// parent and that EF bytes are produced.
func TestEnrichToEF(t *testing.T) {
	lock0 := p2pkh(t, 0x00)
	parent := sdkTx.NewTransaction()
	parent.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1000, LockingScript: lock0})

	child := sdkTx.NewTransaction()
	child.AddInputFromTx(parent, 0, nil)
	child.AddOutput(&sdkTx.TransactionOutput{Satoshis: 900, LockingScript: p2pkh(t, 0x22)})

	// Re-parse from canonical bytes so the input carries no source output,
	// mirroring what WhatsOnChain serves.
	parsed, err := sdkTx.NewTransactionFromBytes(child.Bytes())
	if err != nil {
		t.Fatalf("parse child: %v", err)
	}
	cache := map[string]*sdkTx.Transaction{parent.TxID().String(): parent}

	ef, err := enrichToEF(parsed, cache)
	if err != nil {
		t.Fatalf("enrichToEF: %v", err)
	}
	if len(ef) == 0 {
		t.Fatal("expected non-empty EF bytes")
	}
	in := parsed.Inputs[0]
	if in.SourceTxSatoshis() == nil || *in.SourceTxSatoshis() != 1000 {
		t.Fatalf("source satoshis = %v, want 1000", in.SourceTxSatoshis())
	}
	if in.SourceTxScript() == nil || in.SourceTxScript().String() != lock0.String() {
		t.Fatalf("source script = %v, want %s", in.SourceTxScript(), lock0.String())
	}
}

// TestEnrichToEF_MissingParent verifies a missing parent is an error.
func TestEnrichToEF_MissingParent(t *testing.T) {
	parent := sdkTx.NewTransaction()
	parent.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1000, LockingScript: p2pkh(t, 0x00)})
	child := sdkTx.NewTransaction()
	child.AddInputFromTx(parent, 0, nil)
	parsed, err := sdkTx.NewTransactionFromBytes(child.Bytes())
	if err != nil {
		t.Fatalf("parse child: %v", err)
	}

	_, err = enrichToEF(parsed, map[string]*sdkTx.Transaction{})
	if err == nil || !strings.Contains(err.Error(), "missing parent") {
		t.Fatalf("expected missing-parent error, got %v", err)
	}
}

// TestEnrichToEF_VoutOutOfRange verifies a vout beyond the parent's outputs is
// an error.
func TestEnrichToEF_VoutOutOfRange(t *testing.T) {
	parent := sdkTx.NewTransaction()
	parent.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1000, LockingScript: p2pkh(t, 0x00)})
	child := sdkTx.NewTransaction()
	child.AddInputFromTx(parent, 0, nil)
	parsed, err := sdkTx.NewTransactionFromBytes(child.Bytes())
	if err != nil {
		t.Fatalf("parse child: %v", err)
	}
	parsed.Inputs[0].SourceTxOutIndex = 5 // parent has only 1 output

	_, err = enrichToEF(parsed, map[string]*sdkTx.Transaction{parent.TxID().String(): parent})
	if err == nil || !strings.Contains(err.Error(), "vout=5") {
		t.Fatalf("expected vout-out-of-range error, got %v", err)
	}
}

// TestBuildPickedEFTxs verifies the two-phase bulk flow: picked txs and their
// (de-duplicated) parents are fetched, EF bytes are built, and output is
// deterministic for a fixed pick.
func TestBuildPickedEFTxs(t *testing.T) {
	// One parent with two outputs, spent by two different children.
	parent := sdkTx.NewTransaction()
	parent.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1000, LockingScript: p2pkh(t, 0x01)})
	parent.AddOutput(&sdkTx.TransactionOutput{Satoshis: 2000, LockingScript: p2pkh(t, 0x02)})

	child1 := sdkTx.NewTransaction()
	child1.AddInputFromTx(parent, 0, nil)
	child1.AddOutput(&sdkTx.TransactionOutput{Satoshis: 900, LockingScript: p2pkh(t, 0x03)})

	child2 := sdkTx.NewTransaction()
	child2.AddInputFromTx(parent, 1, nil)
	child2.AddOutput(&sdkTx.TransactionOutput{Satoshis: 1900, LockingScript: p2pkh(t, 0x04)})

	ws := newWOCServer(t)
	for _, tx := range []*sdkTx.Transaction{parent, child1, child2} {
		ws.rawByTxID[tx.TxID().String()] = hex.EncodeToString(tx.Bytes())
	}
	picked := sortedStrings(child1.TxID().String(), child2.TxID().String())

	got, err := buildPickedEFTxs(context.Background(), testClient(t, ws.URL), picked)
	if err != nil {
		t.Fatalf("buildPickedEFTxs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d EF txs, want 2", len(got))
	}
	for _, id := range picked {
		if len(got[id]) == 0 {
			t.Fatalf("missing EF bytes for %s", id)
		}
	}
	// One batch for the picked txs + one batch for the single shared parent.
	if calls := ws.bulkCalls.Load(); calls != 2 {
		t.Fatalf("expected 2 bulk batches (picked + deduped parents), got %d", calls)
	}

	// Determinism: a second identical run yields byte-identical EF output.
	got2, err := buildPickedEFTxs(context.Background(), testClient(t, ws.URL), picked)
	if err != nil {
		t.Fatalf("buildPickedEFTxs (2nd): %v", err)
	}
	for _, id := range picked {
		if string(got[id]) != string(got2[id]) {
			t.Fatalf("non-deterministic EF output for %s", id)
		}
	}
}

// Test429Retry verifies the WhatsOnChain client retries a 429 and then
// succeeds, confirming retry is wired through the fixture tool's call path.
func Test429Retry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tx": []string{"a", "b"}})
	}))
	defer srv.Close()

	client, err := whatsonchain.NewClient(context.Background(),
		whatsonchain.WithBaseURL(srv.URL+"/v1/"),
		whatsonchain.WithRequestRetryCount(2),
		whatsonchain.WithBackoff(time.Millisecond, 5*time.Millisecond, 2.0, 0),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	got, err := fetchBlockTxIDs(context.Background(), client, "hash")
	if err != nil {
		t.Fatalf("fetchBlockTxIDs: %v", err)
	}
	if want := []string{"a", "b"}; !equalStrings(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 attempts (429 then 200), got %d", calls.Load())
	}
}

// TestNewWOCClient_BaseURL verifies --woc is wired through WithBaseURL.
func TestNewWOCClient_BaseURL(t *testing.T) {
	client, err := newWOCClient(config{wocURL: "https://example.com"})
	if err != nil {
		t.Fatalf("newWOCClient: %v", err)
	}
	if got, want := client.BaseURL(), "https://example.com/v1/"; got != want {
		t.Fatalf("BaseURL() = %q, want %q", got, want)
	}
}

// TestRun_TxidCountMismatch drives run() with the real committed block binary
// (served by a datahub stand-in) and a WhatsOnChain stand-in whose txid list
// disagrees with the block's txCount, asserting the guard fires.
func TestRun_TxidCountMismatch(t *testing.T) {
	datahub := newDatahubServer(t)
	ws := newWOCServer(t)
	ws.blockInline = []string{committedCoinbaseTx} // len 1 != committedTxCount

	err := run(runConfig(t, datahub.URL, ws.URL))
	if err == nil || !strings.Contains(err.Error(), "txid count mismatch") {
		t.Fatalf("expected txid count mismatch, got %v", err)
	}
}

// TestRun_CoinbaseMismatch drives run() with a WhatsOnChain stand-in that
// returns the right number of txids but a wrong coinbase at index 0.
func TestRun_CoinbaseMismatch(t *testing.T) {
	datahub := newDatahubServer(t)
	ws := newWOCServer(t)
	txids := make([]string, committedTxCount)
	for i := range txids {
		txids[i] = fmt.Sprintf("%064x", i) // none equal the real coinbase
	}
	ws.blockInline = txids

	err := run(runConfig(t, datahub.URL, ws.URL))
	if err == nil || !strings.Contains(err.Error(), "coinbase txid mismatch") {
		t.Fatalf("expected coinbase txid mismatch, got %v", err)
	}
}

// newDatahubServer serves the committed block binary at /block/<hash>, mirroring
// the teranode datahub the tool fetches from.
func newDatahubServer(t *testing.T) *httptest.Server {
	t.Helper()
	blockBin, err := os.ReadFile(committedBlockBinRel)
	if err != nil {
		t.Fatalf("read committed block.bin: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/block/") {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		_, _ = w.Write(blockBin)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func runConfig(t *testing.T, datahubURL, wocURL string) config {
	t.Helper()
	return config{
		blockHash:  committedBlockHash,
		outDir:     t.TempDir(),
		datahubURL: datahubURL,
		wocURL:     wocURL,
		pickN:      1,
		rng:        rand.New(rand.NewPCG(1, 1)), //nolint:gosec // deterministic test seed
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedStrings(vals ...string) []string {
	out := append([]string(nil), vals...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
