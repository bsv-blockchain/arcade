// Package synthblock builds synthetic mined blocks for tests: a full compound
// BUMP over a set of deterministic txids whose merkle root is known. It exists
// so tests in different packages (webhook, sse, store) can share one importable
// block builder + proof verifier — the equivalent helpers in bump/bump_test.go
// are package-private and unreachable from other packages.
//
// A "full" compound holds every node at every tree level, so bump.ExtractMinimalPath
// (the same extractor the store uses for per-tx enrichment) can pull a valid
// minimal path for any of the block's txids.
package synthblock

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// Block is a synthetic mined block.
type Block struct {
	// BumpBytes is the compound BUMP, ready to pass to store.InsertBUMP.
	BumpBytes []byte
	// Txids are the block's level-0 transaction ids, display-order hex.
	Txids []string
	// Root is the block's merkle root, display-order hex (as in a header).
	Root string
	// Height is the block height encoded in the compound BUMP.
	Height uint64
}

// Build constructs a Block with numTxs leaves. numTxs must be a power of two
// (1, 2, 4, 8, …) so the tree is perfectly balanced and every internal node is
// present — keeping the builder small and the extracted paths unambiguous.
func Build(numTxs int, height uint64) (Block, error) {
	if numTxs < 1 || numTxs&(numTxs-1) != 0 {
		return Block{}, fmt.Errorf("synthblock: numTxs must be a power of two ≥ 1, got %d", numTxs)
	}

	// Level 0: deterministic, distinct leaf hashes. The counter is 32-bit so
	// leaf seeds stay unique for any block size a test could realistically
	// build (a 16-bit counter would silently wrap past 65 535 leaves and
	// produce duplicate txids).
	leaves := make([]*chainhash.Hash, numTxs)
	txids := make([]string, numTxs)
	for i := range leaves {
		var seed [14]byte
		copy(seed[:], "synthblock")
		binary.BigEndian.PutUint32(seed[10:], uint32(i))
		h := chainhash.DoubleHashH(seed[:])
		leaves[i] = &h
		txids[i] = h.String()
	}

	// treeHeight is the number of BUMP levels: log2(numTxs), but at least 1 so a
	// single-tx block still carries its lone txid leaf in Path[0] (ComputeRoot
	// then returns that txid as the root).
	treeHeight := bits.Len64(uint64(numTxs)) - 1
	if treeHeight < 1 {
		treeHeight = 1
	}

	path := make([][]*transaction.PathElement, treeHeight)
	txidFlag := true

	// Level 0 holds every leaf, flagged as a txid.
	path[0] = make([]*transaction.PathElement, numTxs)
	for i, leaf := range leaves {
		path[0][i] = &transaction.PathElement{
			Offset: uint64(i),
			Hash:   leaf,
			Txid:   &txidFlag,
		}
	}

	// Build each higher level from the one below, keeping the running hashes so
	// we can also compute the root. cur starts at the leaf level.
	cur := leaves
	for level := 1; level < treeHeight; level++ {
		next := make([]*chainhash.Hash, len(cur)/2)
		path[level] = make([]*transaction.PathElement, len(next))
		for i := range next {
			parent := transaction.MerkleTreeParent(cur[2*i], cur[2*i+1])
			next[i] = parent
			path[level][i] = &transaction.PathElement{
				Offset: uint64(i),
				Hash:   parent,
			}
		}
		cur = next
	}

	// The root is the parent of the final pair (or the sole txid for a 1-tx block).
	var root *chainhash.Hash
	switch {
	case len(cur) == 1:
		root = cur[0]
	default:
		root = transaction.MerkleTreeParent(cur[0], cur[1])
	}

	mp := &transaction.MerklePath{BlockHeight: uint32(height), Path: path} //nolint:gosec // test data
	return Block{
		BumpBytes: mp.Bytes(),
		Txids:     txids,
		Root:      root.String(),
		Height:    height,
	}, nil
}

// VerifyMerklePathHex parses a hex-encoded BUMP (a per-tx minimal path or a
// compound) and checks that it proves txid against wantRoot. Both txid and
// wantRoot are display-order hex; ComputeRoot's output is compared in the same
// order, so no manual byte reversal is needed. Returns a descriptive error on
// any parse/compute/mismatch failure.
func VerifyMerklePathHex(merklePathHex, txid, wantRoot string) error {
	raw, err := hex.DecodeString(merklePathHex)
	if err != nil {
		return fmt.Errorf("decode merklePath hex: %w", err)
	}
	return VerifyMerklePath(raw, txid, wantRoot)
}

// VerifyMerklePath is VerifyMerklePathHex for raw BUMP bytes.
func VerifyMerklePath(bumpBytes []byte, txid, wantRoot string) error {
	if len(bumpBytes) == 0 {
		return fmt.Errorf("empty merklePath")
	}
	mp, err := transaction.NewMerklePathFromBinary(bumpBytes)
	if err != nil {
		return fmt.Errorf("parse merklePath: %w", err)
	}
	txHash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("parse txid %q: %w", txid, err)
	}
	root, err := mp.ComputeRoot(txHash)
	if err != nil {
		return fmt.Errorf("compute root for %s: %w", txid, err)
	}
	if root.String() != wantRoot {
		return fmt.Errorf("merkle root mismatch for %s: got %s want %s", txid, root.String(), wantRoot)
	}
	return nil
}
