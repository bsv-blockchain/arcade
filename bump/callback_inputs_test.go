package bump

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/arcade/models"
)

// TestBuildCompoundBUMP_CallbackInputs_16Subtrees is the issue #195 acceptance
// regression. It reproduces the incident-block shape — a 16-subtree block where
// the tracked txids live in subtrees other than 0 — and drives BuildCompoundBUMP
// with inputs that have been round-tripped through the DISPLAY-ORDER HEX
// encoding merkle-service uses on the enriched BLOCK_PROCESSED callback:
// subtreeHashes and merkleRoot are encoded as chainhash.Hash.String() and
// decoded back with chainhash.NewHashFromHex — the exact decode bump-builder's
// callbackBlockData performs.
//
// The compound BUMP must validate against the (callback-decoded) header merkle
// root and fold every tracked tx to the real block root. This proves the
// callback-decoded inputs are byte-identical to the datahub binary parser's
// chainhash.NewHash output, so the datahub-independent path yields a correct,
// validatable BUMP for the case that originally DLQ'd against a poisoned peer.
func TestBuildCompoundBUMP_CallbackInputs_16Subtrees(t *testing.T) {
	const (
		numSubtrees = 16
		subtreeSize = 8
		blockHeight = 18603 // the incident block height
	)
	allLeaves, trueAllLeaves, subtreeHashes, coinbaseTxID, trueBlockRoot := setupCoinbaseBlock(numSubtrees, subtreeSize)
	cbBUMP := buildCoinbaseBUMP(trueAllLeaves[0], coinbaseTxID, blockHeight, subtreeHashes)

	// Tracked txids in subtrees 3 and 9 — no subtree-0 STUMP, so the corrected
	// subtree-0 root must be derived from the coinbase BUMP alone (the incident
	// shape).
	tracked := []int{3, 9}
	stumps := []*models.Stump{
		{BlockHash: "incident", SubtreeIndex: 3, StumpData: buildFullSTUMP(allLeaves[3], 0, blockHeight)},
		{BlockHash: "incident", SubtreeIndex: 9, StumpData: buildFullSTUMP(allLeaves[9], 0, blockHeight)},
	}

	// Simulate the wire round-trip: encode as display-order hex, decode back via
	// chainhash.NewHashFromHex.
	cbSubtreeHashes := decodeHashesViaDisplayHex(t, subtreeHashes)
	cbRoot := decodeHashViaDisplayHex(t, trueBlockRoot)

	compound, _, err := BuildCompoundBUMP(stumps, cbSubtreeHashes, cbBUMP)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP (callback inputs): %v", err)
	}
	if err := ValidateCompoundRoot(compound, cbRoot); err != nil {
		t.Fatalf("compound did not validate against callback merkleRoot: %v", err)
	}

	for _, s := range tracked {
		for i := 0; i < subtreeSize; i++ {
			leaf := trueAllLeaves[s][i]
			root, rErr := compound.ComputeRoot(&leaf)
			if rErr != nil {
				t.Fatalf("subtree %d tx %d: ComputeRoot: %v", s, i, rErr)
			}
			if *root != trueBlockRoot {
				t.Fatalf("subtree %d tx %d: root %s != header merkle root %s", s, i, root, trueBlockRoot)
			}
		}
	}
}

// decodeHashesViaDisplayHex encodes each hash as display-order hex and decodes
// it back, mirroring the merkle-service callback wire format + bump-builder's
// chainhash.NewHashFromHex decode.
func decodeHashesViaDisplayHex(t *testing.T, hashes []chainhash.Hash) []chainhash.Hash {
	t.Helper()
	out := make([]chainhash.Hash, len(hashes))
	for i := range hashes {
		out[i] = *decodeHashViaDisplayHex(t, hashes[i])
	}
	return out
}

func decodeHashViaDisplayHex(t *testing.T, h chainhash.Hash) *chainhash.Hash {
	t.Helper()
	decoded, err := chainhash.NewHashFromHex(h.String())
	if err != nil {
		t.Fatalf("NewHashFromHex(%s): %v", h.String(), err)
	}
	return decoded
}
