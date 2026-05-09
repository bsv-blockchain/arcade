package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// hashFromHex parses a 64-char hex txid in DISPLAY order (the way
// WhatsOnChain and BSV explorers print them) into a chainhash.Hash
// stored in INTERNAL byte order. Bitcoin's display orientation is
// the byte-reverse of the wire orientation.
func hashFromHex(displayHex string) (*chainhash.Hash, error) {
	if len(displayHex) != 64 {
		return nil, fmt.Errorf("expected 64 hex chars, got %d", len(displayHex))
	}
	b, err := hex.DecodeString(displayHex)
	if err != nil {
		return nil, fmt.Errorf("hex decode: %w", err)
	}
	// Reverse to get internal byte order.
	rev := make([]byte, 32)
	for i := range b {
		rev[i] = b[31-i]
	}
	h, err := chainhash.NewHash(rev)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// parseHashes converts a slice of display-order hex txids to internal-
// order chainhash.Hash values. Used everywhere we feed real-block data
// into merkle math.
func parseHashes(txids []string) []chainhash.Hash {
	out := make([]chainhash.Hash, len(txids))
	for i, id := range txids {
		h, err := hashFromHex(id)
		if err != nil {
			// Refuse to silently skip — the fixture would be wrong.
			panic(fmt.Sprintf("parseHashes: %v at index %d (%q)", err, i, id))
		}
		out[i] = *h
	}
	return out
}

// concatHashes produces the wire-format subtree binary (concatenated
// 32-byte hashes, internal byte order, no length prefix).
func concatHashes(hashes []chainhash.Hash) []byte {
	out := make([]byte, 0, len(hashes)*chainhash.HashSize)
	for i := range hashes {
		out = append(out, hashes[i][:]...)
	}
	return out
}

// buildMerkleTree returns all levels of a merkle tree from leaves.
// tree[0] = leaves, tree[len-1] = [root]. Odd-count levels duplicate
// the last leaf — the standard Bitcoin convention.
func buildMerkleTree(leaves []chainhash.Hash) [][]chainhash.Hash {
	if len(leaves) == 0 {
		return nil
	}
	tree := [][]chainhash.Hash{leaves}
	current := leaves
	for len(current) > 1 {
		if len(current)%2 == 1 {
			current = append(current, current[len(current)-1])
		}
		next := make([]chainhash.Hash, 0, len(current)/2)
		for i := 0; i < len(current); i += 2 {
			parent := sdkTx.MerkleTreeParent(&current[i], &current[i+1])
			next = append(next, *parent)
		}
		tree = append(tree, next)
		current = next
	}
	return tree
}

// merkleRoot returns the root hash of leaves' Bitcoin merkle tree.
func merkleRoot(leaves []chainhash.Hash) chainhash.Hash {
	tree := buildMerkleTree(leaves)
	if len(tree) == 0 {
		return chainhash.Hash{}
	}
	return tree[len(tree)-1][0]
}

// recoverSubtreeBoundaries reconstructs (start, end) ranges for each
// subtree by trying progressively-larger contiguous batches of txids
// and matching their merkle root to the block-binary subtree-hash
// list. subtree[0] uses the coinbase placeholder at index 0; later
// subtrees use the txids as-is.
//
// The search is greedy left-to-right: for each subtree position we
// try every legal subtree size (powers of two starting from the next
// size that fits) and pick the one whose root matches.
func recoverSubtreeBoundaries(txidsInternal []chainhash.Hash, subtreeHashesHex []string) ([][2]int, error) {
	if len(subtreeHashesHex) == 1 {
		return [][2]int{{0, len(txidsInternal)}}, nil
	}
	expected := make([]chainhash.Hash, len(subtreeHashesHex))
	for i, h := range subtreeHashesHex {
		// Subtree hashes in the block binary are already internal-order.
		raw, err := hex.DecodeString(h)
		if err != nil {
			return nil, fmt.Errorf("decode subtree hash %d: %w", i, err)
		}
		hh, err := chainhash.NewHash(raw)
		if err != nil {
			return nil, fmt.Errorf("subtree hash %d: %w", i, err)
		}
		expected[i] = *hh
	}

	// Standard teranode subtree sizes (per
	// https://teranode.network/docs) are powers of two, typically
	// starting at 1024 leaves. We try sizes from max down to 2 to
	// pick the largest that matches.
	candidateSizes := []int{}
	for size := 1 << 20; size >= 2; size >>= 1 {
		candidateSizes = append(candidateSizes, size)
	}

	bounds := make([][2]int, len(subtreeHashesHex))
	cursor := 0
	for s := 0; s < len(subtreeHashesHex); s++ {
		matched := false
		for _, size := range candidateSizes {
			if cursor+size > len(txidsInternal) {
				continue
			}
			leaves := make([]chainhash.Hash, size)
			copy(leaves, txidsInternal[cursor:cursor+size])
			if s == 0 {
				leaves[0] = chainhash.Hash{} // coinbase placeholder
			}
			got := merkleRoot(leaves)
			if got.IsEqual(&expected[s]) {
				bounds[s] = [2]int{cursor, cursor + size}
				cursor += size
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("could not recover boundary for subtree[%d] starting at txid offset %d (tried sizes 2..2^20)", s, cursor)
		}
	}
	if cursor != len(txidsInternal) {
		return nil, fmt.Errorf("subtree boundaries cover %d of %d txids; some leftover", cursor, len(txidsInternal))
	}
	return bounds, nil
}

// buildMultiSubtreeBinaries handles subtreeCount > 1. Per-subtree
// binaries are concatenated 32-byte hashes; subtree[0] uses the
// coinbase placeholder at index 0.
func buildMultiSubtreeBinaries(txids, subtreeHashesHex []string) (map[string][]byte, error) {
	internal := parseHashes(txids)
	bounds, err := recoverSubtreeBoundaries(internal, subtreeHashesHex)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(subtreeHashesHex))
	for i, b := range bounds {
		leaves := make([]chainhash.Hash, b[1]-b[0])
		copy(leaves, internal[b[0]:b[1]])
		if i == 0 {
			leaves[0] = chainhash.Hash{}
		}
		out[subtreeHashesHex[i]] = concatHashes(leaves)
	}
	return out, nil
}

// verifyHeaderMatchesReconstruction is the consistency self-check.
// Builds the merkle root from {coinbase, tx_1, ..., tx_N} (single
// subtree case) or merge of subtree roots (multi-subtree) and
// asserts it equals the parsed merkle root from the block header.
func verifyHeaderMatchesReconstruction(parsed *teranodeBlock, txids []string) error {
	internal := parseHashes(txids)
	if len(parsed.subtreeHashes) == 1 {
		root := merkleRoot(internal)
		if root.String() != parsed.merkleRootHex {
			return fmt.Errorf("merkle root mismatch: computed=%s header=%s", root.String(), parsed.merkleRootHex)
		}
		return nil
	}
	// Multi-subtree: subtree roots already live in parsed.subtreeHashes.
	// Compute the block-level root over the (coinbase-substituted)
	// subtree-0 root + the remaining subtree roots.
	bounds, err := recoverSubtreeBoundaries(internal, parsed.subtreeHashes)
	if err != nil {
		return err
	}
	subtreeRoots := make([]chainhash.Hash, len(bounds))
	for i, b := range bounds {
		leaves := make([]chainhash.Hash, b[1]-b[0])
		copy(leaves, internal[b[0]:b[1]])
		// Subtree roots that go into the block tree are computed
		// WITH the real coinbase (not placeholder). The block header
		// merkle root is the tree of these "effective" subtree roots.
		subtreeRoots[i] = merkleRoot(leaves)
	}
	root := merkleRoot(subtreeRoots)
	if root.String() != parsed.merkleRootHex {
		return fmt.Errorf("block merkle root mismatch: computed=%s header=%s", root.String(), parsed.merkleRootHex)
	}
	return nil
}

// encodeVarint writes a Bitcoin compact-size unsigned integer.
func encodeVarint(v uint64) []byte {
	switch {
	case v < 0xfd:
		return []byte{byte(v)}
	case v <= 0xffff:
		out := make([]byte, 3)
		out[0] = 0xfd
		binary.LittleEndian.PutUint16(out[1:], uint16(v))
		return out
	case v <= 0xffffffff:
		out := make([]byte, 5)
		out[0] = 0xfe
		binary.LittleEndian.PutUint32(out[1:], uint32(v))
		return out
	default:
		out := make([]byte, 9)
		out[0] = 0xff
		binary.LittleEndian.PutUint64(out[1:], v)
		return out
	}
}

// cursor is a tiny byte-slice reader with varint support. Avoids the
// boilerplate of bytes.Reader + var declarations for every call.
type cursor struct {
	buf []byte
	pos int
}

func newCursor(buf []byte, start int) *cursor { return &cursor{buf: buf, pos: start} }

func (c *cursor) read(n int) ([]byte, error) {
	if c.pos+n > len(c.buf) {
		return nil, fmt.Errorf("read %d bytes at %d: buffer length %d", n, c.pos, len(c.buf))
	}
	out := c.buf[c.pos : c.pos+n]
	c.pos += n
	return out, nil
}

func (c *cursor) varint() (uint64, error) {
	if c.pos >= len(c.buf) {
		return 0, fmt.Errorf("varint at %d: EOF", c.pos)
	}
	t := c.buf[c.pos]
	c.pos++
	switch {
	case t < 0xfd:
		return uint64(t), nil
	case t == 0xfd:
		b, err := c.read(2)
		if err != nil {
			return 0, err
		}
		return uint64(binary.LittleEndian.Uint16(b)), nil
	case t == 0xfe:
		b, err := c.read(4)
		if err != nil {
			return 0, err
		}
		return uint64(binary.LittleEndian.Uint32(b)), nil
	default:
		b, err := c.read(8)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(b), nil
	}
}
