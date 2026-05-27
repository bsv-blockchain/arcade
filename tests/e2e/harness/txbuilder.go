//go:build e2e

package harness

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// nonDataScript returns a single-byte OP_TRUE locking script. arcade's
// validator considers this non-data (so checkOutputs accepts it) and
// it's accepted by checkInputs as a non-coinbase witness.
func nonDataScript() *bscript.Script {
	s := bscript.Script([]byte{0x51})
	return &s
}

// minimalPushScript is a single-byte OP_0 push, the smallest push-only
// script that's non-empty. arcade's policy pushDataCheck requires the
// unlocking script to be push-only, and go-sdk's SatoshisPerKilobyte
// fee model rejects inputs with an empty unlocking script
// (ErrNoUnlockingScript) because it can't size the witness. OP_0
// pushes an empty byte string onto the stack, which the OP_TRUE
// locking script ignores when it pushes 1 — script execution still
// succeeds.
func minimalPushScript() *bscript.Script {
	s := bscript.Script([]byte{0x00})
	return &s
}

// toChainHashBT widens a 32-byte slice into a chainhash.Hash. Used for
// PreviousTxIDAdd which expects the bytes in network byte order.
func toChainHashBT(b []byte) *chainhash.Hash {
	var h chainhash.Hash
	copy(h[:], b)
	return &h
}

// We use go-bt/v2 types throughout this file because teranode's
// model.Block API requires *bt.Tx and bt/chainhash.Hash. arcade itself
// is bilingual (uses both go-bt and go-sdk in different code paths);
// the harness picks bt because that's what gets us through model.Block.

// BuildTxs returns n synthetic transactions whose txids are guaranteed
// unique. We don't need valid scripts — for the smoke test we only care
// that each tx has a stable, distinguishable hash. Varying LockTime is
// the cheapest way to get that without constructing inputs/outputs.
//
// startNonce lets callers stitch multiple batches into one stream where
// each tx in the entire stream has a distinct LockTime.
//
// These txs DO NOT pass arcade's structural validator (no inputs, no
// outputs, under-minimum size). For broadcasting through arcade's
// POST /tx, use BuildValidatableTxs.
func BuildTxs(n int, startNonce uint32) []*bt.Tx {
	out := make([]*bt.Tx, n)
	for i := 0; i < n; i++ {
		tx := bt.NewTx()
		tx.LockTime = startNonce + uint32(i) //nolint:gosec // bounded by caller
		out[i] = tx
	}
	return out
}

// BuildValidatableTxs returns n synthetic transactions that pass
// arcade's intake validator (structural policy + script execution +
// fee check; merkle proofs on parents are trusted). Each tx has:
//
//   - One input pointing at a non-zero source txid + OP_TRUE locking
//     script + 10_000 satoshis (passes checkInputs as non-coinbase and
//     covers the fee floor by a wide margin).
//   - One output with OP_TRUE locking script + 1 satoshi (passes
//     checkOutputs; remaining ~9999 sats become fee).
//   - LockTime = startNonce + i, ensuring each tx hashes differently.
//
// OP_TRUE (0x51) is a trivially-satisfied locking script — an empty
// unlocking script evaluates to true against it, so script execution
// inside spv.Verify succeeds without any signing. Callers must serialize
// these via ExtendedBytes() so arcade's go-sdk parser sees the per-input
// source script + satoshis (otherwise GetFee returns "PreviousTx not
// supplied" inside the fee check).
func BuildValidatableTxs(n int, startNonce uint32) []*bt.Tx {
	out := make([]*bt.Tx, n)
	// Reused across all txs so we don't re-allocate the same constant
	// shapes. Each tx's LockTime varies so the txid stays unique.
	prevTxIDHex := "0101010101010101010101010101010101010101010101010101010101010101"
	prevTxIDBytes, _ := hex.DecodeString(prevTxIDHex)
	for i := 0; i < n; i++ {
		tx := bt.NewTx()
		tx.LockTime = startNonce + uint32(i) //nolint:gosec // bounded by caller
		input := &bt.Input{
			SequenceNumber: 0xffffffff,
		}
		_ = input.PreviousTxIDAdd(toChainHashBT(prevTxIDBytes))
		input.PreviousTxOutIndex = 0
		input.PreviousTxScript = nonDataScript()
		input.PreviousTxSatoshis = 10000
		input.UnlockingScript = minimalPushScript()
		tx.Inputs = append(tx.Inputs, input)
		tx.AddOutput(&bt.Output{
			Satoshis:      1,
			LockingScript: nonDataScript(),
		})
		out[i] = tx
	}
	return out
}

// TxIDs returns the txid hashes of txs in network byte order.
func TxIDs(txs []*bt.Tx) []chainhash.Hash {
	out := make([]chainhash.Hash, len(txs))
	for i, tx := range txs {
		id := tx.TxIDChainHash()
		out[i] = *id
	}
	return out
}

// SubtreeBinary packs the txid hashes into the on-the-wire format the
// teranode DataHub returns at /subtree/<hash>: concatenated 32-byte
// hashes, no length prefix. ParseRawNodes / ParseRawTxids in
// merkle-service expect exactly this shape.
func SubtreeBinary(txids []chainhash.Hash) []byte {
	out := make([]byte, 0, len(txids)*chainhash.HashSize)
	for _, h := range txids {
		out = append(out, h[:]...)
	}
	return out
}

// SubtreeRoot returns the merkle root of the supplied tx hashes. The
// computation is the standard Bitcoin merkle: pair-wise SHA-256d up the
// tree, duplicating the last leaf when the level has an odd count.
//
// Used by callers that need to derive the block header's merkle root
// from a single subtree (the smoke-test layout).
func SubtreeRoot(txids []chainhash.Hash) chainhash.Hash {
	if len(txids) == 0 {
		return chainhash.Hash{}
	}
	level := make([]chainhash.Hash, len(txids))
	copy(level, txids)
	for len(level) > 1 {
		if len(level)%2 == 1 {
			level = append(level, level[len(level)-1])
		}
		next := make([]chainhash.Hash, len(level)/2)
		for i := 0; i < len(level); i += 2 {
			next[i/2] = sha256d(level[i][:], level[i+1][:])
		}
		level = next
	}
	return level[0]
}

// MerkleRootFromCoinbaseAndSubtree computes the block-header merkle root
// for a block laid out as one subtree. Layout: leaf 0 is the coinbase
// txid, leaf 1 is the subtree root. SHA-256d concatenated.
func MerkleRootFromCoinbaseAndSubtree(coinbase, subtreeRoot chainhash.Hash) chainhash.Hash {
	return sha256d(coinbase[:], subtreeRoot[:])
}

func sha256d(left, right []byte) chainhash.Hash {
	first := sha256.New()
	first.Write(left)
	first.Write(right)
	tmp := first.Sum(nil)
	second := sha256.Sum256(tmp)
	return chainhash.Hash(second)
}

// BuildCoinbase returns a synthetic coinbase tx whose hash varies with
// height. Real Bitcoin coinbases need a unique scriptSig so each block's
// coinbase txid is distinct; we ensure uniqueness by setting LockTime to
// the height. The result is parseable by arcade's parseBlockBinary.
func BuildCoinbase(height uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = height
	return tx
}

// BuildBlockBinary constructs the binary block payload that arcade's
// bump.parseBlockBinary and merkle-service's model.NewBlockFromBytes
// both decode. Layout:
//
//	header(80) | txCount(varint) | sizeBytes(varint) | subtreeCount(varint) |
//	subtreeHashes (N×32) | coinbaseTx | height(varint) | coinbaseBUMPLen(varint) |
//	coinbaseBUMP(variable)
//
// merkleRoot is what gets written into the header. Callers compute it
// from coinbase + subtree root via MerkleRootFromCoinbaseAndSubtree (or
// any other layout-appropriate composition).
func BuildBlockBinary(
	prevHash chainhash.Hash,
	merkleRoot chainhash.Hash,
	height uint32,
	timestamp uint32,
	bits *model.NBit,
	nonce uint32,
	subtreeHashes []chainhash.Hash,
	coinbase *bt.Tx,
	coinbaseBUMP []byte,
	txCount uint64,
	sizeBytes uint64,
) ([]byte, chainhash.Hash, error) {
	if bits == nil {
		// Regtest-friendly default: trivially-easy difficulty so tests
		// don't have to mine. NBit is little-endian; 0x207fffff in
		// network byte order is the standard regtest target.
		nb, err := model.NewNBitFromSlice([]byte{0xff, 0xff, 0x7f, 0x20})
		if err != nil {
			return nil, chainhash.Hash{}, fmt.Errorf("default nbit: %w", err)
		}
		bits = nb
	}

	prevPtr := prevHash
	rootPtr := merkleRoot
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &prevPtr,
		HashMerkleRoot: &rootPtr,
		Timestamp:      timestamp,
		Bits:           *bits,
		Nonce:          nonce,
	}

	subtreePtrs := make([]*chainhash.Hash, len(subtreeHashes))
	for i := range subtreeHashes {
		h := subtreeHashes[i]
		subtreePtrs[i] = &h
	}

	cb := coinbase
	if cb == nil {
		cb = BuildCoinbase(height)
	}

	block, err := model.NewBlock(header, cb, subtreePtrs, txCount, sizeBytes, height, 0)
	if err != nil {
		return nil, chainhash.Hash{}, fmt.Errorf("model.NewBlock: %w", err)
	}
	if len(coinbaseBUMP) > 0 {
		block.CoinbaseBUMP = coinbaseBUMP
	}
	payload, err := block.Bytes()
	if err != nil {
		return nil, chainhash.Hash{}, fmt.Errorf("block.Bytes: %w", err)
	}
	blockHash := *block.Header.Hash()
	return payload, blockHash, nil
}

// EncodeUint32LE is a small helper exported for tests that need to
// roll their own header bytes (e.g., when asserting the header's hash
// directly without going through model.BlockHeader).
func EncodeUint32LE(v uint32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, v)
	return out
}
