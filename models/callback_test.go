package models

import (
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// TestCallbackMessage_BlockProcessedEnrichmentRoundTrip pins that the four
// enrichment fields merkle-service attaches to BLOCK_PROCESSED (issue #195)
// survive a JSON marshal/unmarshal round-trip with their values intact, and
// that coinbaseBump decodes from hex into the raw bytes HexBytes carries.
func TestCallbackMessage_BlockProcessedEnrichmentRoundTrip(t *testing.T) {
	const (
		merkleRoot = "215b459eb39fb46220c591d89dffa89def1cf03f327765e6009b949aed741414"
		subtree0   = "1111111111111111111111111111111111111111111111111111111111111111"
		subtree1   = "2222222222222222222222222222222222222222222222222222222222222222"
	)
	in := CallbackMessage{
		Type:          CallbackBlockProcessed,
		BlockHash:     "00000000c57cfda6619dd099aa447a28353cfaca71942770273f5818778f64f7",
		MerkleRoot:    merkleRoot,
		SubtreeCount:  2,
		SubtreeHashes: []string{subtree0, subtree1},
		CoinbaseBUMP:  HexBytes{0xde, 0xad, 0xbe, 0xef},
	}

	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out CallbackMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.MerkleRoot != merkleRoot {
		t.Errorf("MerkleRoot = %q, want %q", out.MerkleRoot, merkleRoot)
	}
	if out.SubtreeCount != 2 {
		t.Errorf("SubtreeCount = %d, want 2", out.SubtreeCount)
	}
	if len(out.SubtreeHashes) != 2 || out.SubtreeHashes[0] != subtree0 || out.SubtreeHashes[1] != subtree1 {
		t.Errorf("SubtreeHashes = %v, want [%s %s]", out.SubtreeHashes, subtree0, subtree1)
	}
	if string(out.CoinbaseBUMP) != string([]byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("CoinbaseBUMP = %x, want deadbeef", out.CoinbaseBUMP)
	}
}

// TestCallbackMessage_LegacyBlockProcessedNoEnrichment pins backwards
// compatibility: a BLOCK_PROCESSED from an older merkle-service (or one that
// couldn't build the enrichment) carries none of the new fields, parses
// without error, and leaves them at their zero values so bump-builder falls
// back to the datahub path.
func TestCallbackMessage_LegacyBlockProcessedNoEnrichment(t *testing.T) {
	const legacy = `{"type":"BLOCK_PROCESSED","blockHash":"00000000c57cfda6619dd099aa447a28353cfaca71942770273f5818778f64f7"}`

	var msg CallbackMessage
	if err := json.Unmarshal([]byte(legacy), &msg); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}

	if msg.Type != CallbackBlockProcessed {
		t.Errorf("Type = %q, want %q", msg.Type, CallbackBlockProcessed)
	}
	if msg.MerkleRoot != "" {
		t.Errorf("MerkleRoot = %q, want empty", msg.MerkleRoot)
	}
	if msg.SubtreeCount != 0 {
		t.Errorf("SubtreeCount = %d, want 0", msg.SubtreeCount)
	}
	if msg.SubtreeHashes != nil {
		t.Errorf("SubtreeHashes = %v, want nil", msg.SubtreeHashes)
	}
	if msg.CoinbaseBUMP != nil {
		t.Errorf("CoinbaseBUMP = %x, want nil", msg.CoinbaseBUMP)
	}
}

// TestCallbackMessage_EnrichmentOmittedWhenEmpty pins that the new fields use
// omitempty so a STUMP / SEEN callback (which never sets them) does not grow
// extra keys on the wire.
func TestCallbackMessage_EnrichmentOmittedWhenEmpty(t *testing.T) {
	raw, err := json.Marshal(CallbackMessage{Type: CallbackStump, BlockHash: "abc", SubtreeIndex: 3})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{"merkleRoot", "subtreeCount", "subtreeHashes", "coinbaseBump"} {
		if got := string(raw); contains(got, key) {
			t.Errorf("marshaled STUMP callback unexpectedly contains %q: %s", key, got)
		}
	}
}

// TestCallbackMessage_MerkleRootDisplayHexSemantics pins the byte-order
// contract the bump-builder relies on: merkleRoot/subtreeHashes are
// display-order hex, so decoding with chainhash.NewHashFromHex (which reverses
// display->internal) yields a Hash whose String() round-trips back to the same
// hex. This is the property that makes callback-supplied hashes byte-identical
// to the datahub binary parser's chainhash.NewHash(rawBytes) output.
func TestCallbackMessage_MerkleRootDisplayHexSemantics(t *testing.T) {
	const displayHex = "215b459eb39fb46220c591d89dffa89def1cf03f327765e6009b949aed741414"

	h, err := chainhash.NewHashFromHex(displayHex)
	if err != nil {
		t.Fatalf("NewHashFromHex: %v", err)
	}
	if h.String() != displayHex {
		t.Errorf("round-trip String() = %q, want %q", h.String(), displayHex)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
