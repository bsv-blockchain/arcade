//go:build e2e

package harness

import (
	"bytes"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/arcade/validator"
)

// TestBuildValidatableTxs_PassesArcadePolicyValidator confirms our
// synthetic-tx builder produces transactions that survive arcade's
// structural validator. Without this assertion, smoke_test.go's
// BroadcastTx would silently route everything to REJECTED before
// merkle-service ever sees a watch registration.
func TestBuildValidatableTxs_PassesArcadePolicyValidator(t *testing.T) {
	v := validator.NewValidator(nil)
	txs := BuildValidatableTxs(5, 100)

	for i, tx := range txs {
		// arcade parses txs via go-sdk, not go-bt, and clients POST the
		// extended (EF) serialization — round-trip via ExtendedBytes so the
		// parsed tx carries the per-input source script + satoshis the BDK
		// validator needs (BroadcastTx posts tx.ExtendedBytes() too).
		raw := tx.ExtendedBytes()
		sdkParsed, err := sdkTx.NewTransactionFromBytes(raw)
		if err != nil {
			t.Fatalf("tx %d: sdk parse: %v (raw=%x)", i, err, raw)
		}
		if err := v.ValidatePolicy(sdkParsed); err != nil {
			t.Errorf("tx %d: ValidatePolicy: %v (size=%d)", i, err, sdkParsed.Size())
		}
	}
}

// TestBuildValidatableTxs_UniqueTxIDs confirms LockTime variation
// produces distinct txid hashes — the smoke test relies on this for
// per-tx status assertions.
func TestBuildValidatableTxs_UniqueTxIDs(t *testing.T) {
	txs := BuildValidatableTxs(50, 0)
	seen := make(map[string]struct{}, len(txs))
	for i, tx := range txs {
		id := tx.TxID()
		if _, dup := seen[id]; dup {
			t.Fatalf("tx %d: duplicate txid %s", i, id)
		}
		seen[id] = struct{}{}
	}
}

// TestBuildSyntheticBlock_Composition pins the teranode composition
// rules the reorg tests depend on: the announced subtree root is over
// [placeholder, txids...], the header merkle root is the same tree with
// the real coinbase txid at leaf 0, the ground nonce satisfies the
// regtest target, and two same-height competitors get distinct hashes.
func TestBuildSyntheticBlock_Composition(t *testing.T) {
	txids := TxIDs(BuildTxs(7, 100))
	spec := SyntheticBlockSpec{
		PrevHash:  RegtestGenesisHash(),
		Height:    1,
		Timestamp: 1700000000,
		TxIDs:     txids,
	}
	blk, err := BuildSyntheticBlock(spec)
	if err != nil {
		t.Fatalf("BuildSyntheticBlock: %v", err)
	}

	// Announced subtree: placeholder at leaf 0.
	wantSubtree := SubtreeRoot(append([]chainhash.Hash{coinbasePlaceholder}, txids...))
	if !blk.SubtreeHash.IsEqual(&wantSubtree) {
		t.Errorf("subtree hash: got %s want %s", blk.SubtreeHash, wantSubtree)
	}
	if len(blk.SubtreeBin) != 8*32 {
		t.Errorf("subtree binary: got %d bytes want %d", len(blk.SubtreeBin), 8*32)
	}
	if !bytes.Equal(blk.SubtreeBin[:32], coinbasePlaceholder[:]) {
		t.Error("subtree binary leaf 0 must be the coinbase placeholder")
	}

	// Header root: coinbase-corrected tree.
	wantRoot := SubtreeRoot(append([]chainhash.Hash{blk.CoinbaseID}, txids...))
	if !blk.MerkleRoot.IsEqual(&wantRoot) {
		t.Errorf("merkle root: got %s want %s", blk.MerkleRoot, wantRoot)
	}

	// Simulate merkle-service's coinbase-BUMP self-validation: fold the
	// coinbase txid up the placeholder tree's proof-of-leaf-0 siblings;
	// the result must equal the header merkle root.
	folded := blk.CoinbaseID
	level := append([]chainhash.Hash{coinbasePlaceholder}, txids...)
	for len(level) > 1 {
		sibling := level[1] // leaf 0 climbs the left spine
		folded = sha256d(folded[:], sibling[:])
		next := make([]chainhash.Hash, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			next = append(next, sha256d(level[i][:], level[i+1][:]))
		}
		level = next
	}
	if !folded.IsEqual(&blk.MerkleRoot) {
		t.Errorf("coinbase fold: got %s want %s", folded, blk.MerkleRoot)
	}

	// Header parses back to the block hash and satisfies the regtest
	// compact target (top byte of display-order hash <= 0x7f).
	if got := chainhash.DoubleHashH(blk.HeaderBin); !got.IsEqual(&blk.Hash) {
		t.Errorf("header hash: got %s want %s", got, blk.Hash)
	}
	if blk.Hash[31] > 0x7f {
		t.Errorf("block hash %s does not satisfy the regtest target", blk.Hash)
	}

	// A same-height competitor with a different coinbase + overlapping
	// txs must hash differently.
	spec2 := spec
	spec2.CBExtra = 7
	spec2.TxIDs = append(append([]chainhash.Hash(nil), txids[:5]...), TxIDs(BuildTxs(2, 500))...)
	blk2, err := BuildSyntheticBlock(spec2)
	if err != nil {
		t.Fatalf("BuildSyntheticBlock competitor: %v", err)
	}
	if blk2.Hash.IsEqual(&blk.Hash) {
		t.Error("competitors must have distinct block hashes")
	}
	if blk2.CoinbaseID.IsEqual(&blk.CoinbaseID) {
		t.Error("CBExtra must vary the coinbase txid")
	}

	// Determinism: same spec, same block.
	blk3, err := BuildSyntheticBlock(spec)
	if err != nil {
		t.Fatalf("BuildSyntheticBlock repeat: %v", err)
	}
	if !blk3.Hash.IsEqual(&blk.Hash) {
		t.Error("BuildSyntheticBlock must be deterministic for a fixed spec")
	}
}
