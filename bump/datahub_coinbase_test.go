package bump

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/util"
	"go.uber.org/zap"
)

func decodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(s), "\n", ""))
	if err != nil {
		t.Fatalf("decode hex: %v", err)
	}
	return b
}

// TestCoinbaseBUMPReconciles_RealData exercises the new fetch-time check with
// the real coinbase BUMPs captured from production for block 950675: the
// consistent one reconciles to the header root, the inconsistent one (served by
// a stale peer) does not.
func TestCoinbaseBUMPReconciles_RealData(t *testing.T) {
	good := decodeHex(t, issue167CoinbaseBUMPHex)
	bad := decodeHex(t, issue167BadCoinbaseBUMPHex)
	headerRoot, _ := chainhash.NewHashFromHex(issue167HeaderMerkleRoot)

	if err := coinbaseBUMPReconciles(good, headerRoot); err != nil {
		t.Errorf("good coinbase BUMP should reconcile, got: %v", err)
	}
	if err := coinbaseBUMPReconciles(bad, headerRoot); err == nil {
		t.Error("inconsistent coinbase BUMP should be rejected, got nil")
	}
	// Guard rails: nothing to check => nil.
	if err := coinbaseBUMPReconciles(nil, headerRoot); err != nil {
		t.Errorf("empty coinbase BUMP should be a no-op, got: %v", err)
	}
	if err := coinbaseBUMPReconciles(good, nil); err != nil {
		t.Errorf("nil header root should be a no-op, got: %v", err)
	}
}

// blockWithCoinbaseBUMP builds a datahub block-binary response carrying one
// subtree hash plus a coinbase transaction and coinbase BUMP, matching the
// format parseBlockBinary expects.
func blockWithCoinbaseBUMP(t *testing.T, headerMerkleRoot chainhash.Hash, coinbaseBUMP []byte) []byte {
	t.Helper()
	out := make([]byte, 0, 256+len(coinbaseBUMP))
	header := make([]byte, 80)
	copy(header[36:68], headerMerkleRoot[:]) // internal byte order
	out = append(out, header...)
	out = append(out, 0x00, 0x00) // txCount=0, sizeBytes=0
	out = append(out, 0x01)       // subtreeCount=1
	subtreeHash := make([]byte, 32)
	for i := range subtreeHash {
		subtreeHash[i] = 0xAB
	}
	out = append(out, subtreeHash...)
	// coinbase tx (skipped by the parser, but must be parseable)
	cbTx := transaction.NewTransaction().Bytes()
	out = append(out, cbTx...)
	out = append(out, util.VarInt(950675).Bytes()...)                    // blockHeight
	out = append(out, util.VarInt(uint64(len(coinbaseBUMP))).Bytes()...) // coinbaseBUMP length
	out = append(out, coinbaseBUMP...)
	return out
}

// TestFetchBlockData_SkipsInconsistentCoinbaseEndpoint reproduces the block
// 950675 production scenario: one datahub peer serves a coinbase BUMP that does
// not reconcile to the (correct) header root it reports, while another serves a
// consistent one. The fetch loop must skip the bad peer and return the good
// peer's data, rather than deterministically selecting the bad peer on every
// fetch and blocking the block from ever building a valid BUMP.
func TestFetchBlockData_SkipsInconsistentCoinbaseEndpoint(t *testing.T) {
	headerRoot, _ := chainhash.NewHashFromHex(issue167HeaderMerkleRoot)
	good := decodeHex(t, issue167CoinbaseBUMPHex)
	bad := decodeHex(t, issue167BadCoinbaseBUMPHex)

	// Sanity: the helper produces a body the parser reads a coinbase BUMP from.
	if _, cb, _, perr := parseBlockBinary(blockWithCoinbaseBUMP(t, *headerRoot, good)); perr != nil || len(cb) == 0 {
		t.Fatalf("test fixture invalid: parse err=%v cbLen=%d", perr, len(cb))
	}

	badBody := blockWithCoinbaseBUMP(t, *headerRoot, bad)
	goodBody := blockWithCoinbaseBUMP(t, *headerRoot, good)

	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(badBody)
	}))
	t.Cleanup(badSrv.Close)
	goodSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(goodBody)
	}))
	t.Cleanup(goodSrv.Close)

	_, cbBUMP, root, err := FetchBlockDataForBUMPWithOptions(
		context.Background(),
		[]string{badSrv.URL, goodSrv.URL}, // bad peer sorts first
		"00000000000000000eb6ea115a5cb140ffeae7b673b4c5b6f3fac62e0adae3c5",
		1<<20, nil, zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("expected fall-through to the consistent peer, got: %v", err)
	}
	if root == nil || !root.IsEqual(headerRoot) {
		t.Fatalf("unexpected header root: %v", root)
	}
	if !bytesEqual(cbBUMP, good) {
		t.Fatal("expected the good peer's coinbase BUMP to be selected")
	}

	// And if ONLY the bad peer exists, the loop surfaces the mismatch error.
	_, _, _, err = FetchBlockDataForBUMPWithOptions(
		context.Background(),
		[]string{badSrv.URL},
		"00000000000000000eb6ea115a5cb140ffeae7b673b4c5b6f3fac62e0adae3c5",
		1<<20, nil, zap.NewNop(),
	)
	if err == nil || !strings.Contains(err.Error(), "coinbase BUMP mismatch") {
		t.Fatalf("expected coinbase BUMP mismatch error, got: %v", err)
	}
}

func bytesEqual(a, b []byte) bool {
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
