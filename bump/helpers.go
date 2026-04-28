package bump

import (
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// addLeaf adds a PathElement to the given level of a MerklePath.
func addLeaf(mp *transaction.MerklePath, level int, elem *transaction.PathElement) {
	for level >= len(mp.Path) {
		mp.Path = append(mp.Path, nil)
	}
	mp.Path[level] = append(mp.Path[level], elem)
}

// findLeafByOffset finds a PathElement at the given level and offset.
func findLeafByOffset(mp *transaction.MerklePath, level int, offset uint64) *transaction.PathElement {
	if level >= len(mp.Path) {
		return nil
	}
	for _, elem := range mp.Path[level] {
		if elem.Offset == offset {
			return elem
		}
	}
	return nil
}

// computeMissingHashes fills in intermediate nodes of the merkle path by computing
// parent hashes from pairs of children. If the sibling is a Duplicate marker
// (BRC-74 flag=0x01, no hash), the parent is MerkleTreeParent(self, self) per
// the canonical Bitcoin merkle rule that an odd leaf duplicates itself.
func computeMissingHashes(mp *transaction.MerklePath) {
	for level := 0; level < len(mp.Path)-1; level++ {
		for _, elem := range mp.Path[level] {
			if elem.Hash == nil {
				continue
			}
			parentOffset := elem.Offset / 2
			if findLeafByOffset(mp, level+1, parentOffset) != nil {
				continue
			}
			siblingOffset := elem.Offset ^ 1
			sibling := findLeafByOffset(mp, level, siblingOffset)
			if sibling == nil {
				continue
			}

			var parent *chainhash.Hash
			switch {
			case isDuplicate(sibling):
				parent = merkleTreeParent(elem.Hash, elem.Hash)
			case sibling.Hash == nil:
				continue
			case elem.Offset%2 == 0:
				parent = merkleTreeParent(elem.Hash, sibling.Hash)
			default:
				parent = merkleTreeParent(sibling.Hash, elem.Hash)
			}

			addLeaf(mp, level+1, &transaction.PathElement{
				Offset: parentOffset,
				Hash:   parent,
			})
		}
	}
}

// isDuplicate reports whether a PathElement carries BRC-74's "duplicate"
// flag — i.e. the verifier must fold the working hash with itself at this
// slot rather than consuming a stored hash.
func isDuplicate(elem *transaction.PathElement) bool {
	return elem != nil && elem.Duplicate != nil && *elem.Duplicate
}

// padAndComputeBlockLevel completes the compound BUMP's block-level tree from
// `fromLevel` (the subtree-root layer) up to the highest stored level. It
// implements Bitcoin's canonical merkle-root algorithm: whenever a level has
// an odd number of real nodes, a Duplicate-marked PathElement is inserted at
// offset = realCount so verifiers fold the last real hash with itself. Then
// it computes parent hashes for the next level, re-using any parents that
// were already produced by computeMissingHashes. Without this, blocks whose
// subtree count is not a power of two (e.g. 39 subtrees) generate a compound
// BUMP missing every sibling that would sit inside the padded region, and
// ComputeRoot fails with "we do not have a hash for this index at height: N".
func padAndComputeBlockLevel(mp *transaction.MerklePath, fromLevel, realNodes int) {
	dupTrue := true
	realCount := realNodes
	for level := fromLevel; level < len(mp.Path)-1; level++ {
		if realCount%2 == 1 {
			if findLeafByOffset(mp, level, uint64(realCount)) == nil {
				addLeaf(mp, level, &transaction.PathElement{
					Offset:    uint64(realCount),
					Duplicate: &dupTrue,
				})
			}
			realCount++
		}
		parentCount := realCount / 2
		for i := 0; i < parentCount; i++ {
			parentOffset := uint64(i)
			if findLeafByOffset(mp, level+1, parentOffset) != nil {
				continue
			}
			left := findLeafByOffset(mp, level, uint64(2*i))
			right := findLeafByOffset(mp, level, uint64(2*i+1))
			if left == nil || right == nil {
				continue
			}
			var parent *chainhash.Hash
			switch {
			case isDuplicate(right):
				if left.Hash == nil {
					continue
				}
				parent = merkleTreeParent(left.Hash, left.Hash)
			case isDuplicate(left):
				if right.Hash == nil {
					continue
				}
				parent = merkleTreeParent(right.Hash, right.Hash)
			default:
				if left.Hash == nil || right.Hash == nil {
					continue
				}
				parent = merkleTreeParent(left.Hash, right.Hash)
			}
			addLeaf(mp, level+1, &transaction.PathElement{
				Offset: parentOffset,
				Hash:   parent,
			})
		}
		realCount = parentCount
	}
}

// merkleTreeParent computes the parent hash of two child nodes.
func merkleTreeParent(left, right *chainhash.Hash) *chainhash.Hash {
	return transaction.MerkleTreeParent(left, right)
}
