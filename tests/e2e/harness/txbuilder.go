//go:build e2e

package harness

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"
)

// nonDataScript returns an OP_DROP OP_TRUE locking script. Paired with the
// OP_0 unlocking script (minimalPushScript), the full evaluation is
// OP_0 OP_DROP OP_TRUE: the empty push is dropped and OP_TRUE leaves a single
// truthy element, so the stack is clean. The BDK validator enforces the
// post-Genesis CLEANSTACK rule (exactly one truthy element must remain after
// execution), which a bare OP_TRUE paired with OP_0 would violate (it leaves
// two elements). The script is non-standard, which the BDK validator accepts
// because it runs with RequireStandard=false.
func nonDataScript() *bscript.Script {
	s := bscript.Script([]byte{0x75, 0x51})
	return &s
}

// minimalPushScript is a single-byte OP_0 push, the smallest non-empty
// unlocking script. OP_0 pushes an empty byte string that the OP_DROP in
// nonDataScript pops back off, leaving a clean stack — see nonDataScript for
// why the drop is required.
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
//   - One input pointing at a non-zero source txid + OP_DROP OP_TRUE
//     locking script + 10_000 satoshis (non-coinbase, covering the fee
//     floor by a wide margin).
//   - One output with OP_DROP OP_TRUE locking script + 1 satoshi
//     (remaining ~9999 sats become fee).
//   - LockTime = startNonce + i, ensuring each tx hashes differently.
//
// The OP_0 unlocking script + OP_DROP OP_TRUE locking script evaluate to a
// single truthy element on a clean stack, which the BDK validator's CLEANSTACK
// rule requires (see nonDataScript). Callers must serialize these via
// ExtendedBytes() so arcade's go-sdk parser sees the per-input source script +
// satoshis (otherwise the fee check fails with "PreviousTx not supplied").
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

// TxIDsFromHex converts display-order txid hex strings (as returned by
// BroadcastTx / GET /tx) into chainhash form for block fabrication.
func TxIDsFromHex(t *testing.T, ids []string) []chainhash.Hash {
	t.Helper()
	out := make([]chainhash.Hash, len(ids))
	for i, id := range ids {
		h, err := chainhash.NewHashFromStr(id)
		if err != nil {
			t.Fatalf("parse txid %s: %v", id, err)
		}
		out[i] = *h
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

// coinbasePlaceholder is teranode's subtree-0/leaf-0 coinbase stand-in:
// 32 bytes of 0xFF (go-subtree's CoinbasePlaceholder). The subtree an
// announcing node publishes carries this placeholder at leaf 0; the
// block header's merkle root is computed with the placeholder replaced
// by the real coinbase txid. merkle-service's coinbase-BUMP builder and
// arcade's applyCoinbaseToSTUMP both rely on this convention.
var coinbasePlaceholder = chainhash.Hash{
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
}

// regtestBits is the standard regtest compact difficulty (0x207fffff),
// little-endian as serialized in the header.
var regtestBits = [4]byte{0xff, 0xff, 0x7f, 0x20}

// RegtestGenesisHash returns the regtest genesis block hash — the
// prevHash for a synthetic block at height 1. Matches go-chaincfg's
// RegressionNetParams (what embedded go-chaintracks initializes from).
func RegtestGenesisHash() chainhash.Hash {
	// 0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206
	// (display order). chainhash bytes are the reverse.
	h, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	return *h
}

// SyntheticBlock is a fully fabricated single-subtree block: everything
// a test needs to stage it on a Datahub and announce it over libp2p so
// that merkle-service, arcade, and embedded chaintracks all accept it.
//
// Layout is teranode's: the announced subtree's leaf 0 is the coinbase
// placeholder; the header merkle root is the same tree with leaf 0
// replaced by the real coinbase txid (single-subtree block ⇒ the
// corrected subtree root IS the block root).
type SyntheticBlock struct {
	Hash        chainhash.Hash   // block hash (header hash)
	Height      uint32           //
	HeaderBin   []byte           // 80-byte serialized header (StageHeader input)
	HeaderHex   string           // hex(HeaderBin) — BlockMessage.Header
	CoinbaseHex string           // hex(coinbase bytes) — BlockMessage.Coinbase
	CoinbaseID  chainhash.Hash   // coinbase txid
	MerkleRoot  chainhash.Hash   // header merkle root (coinbase-corrected)
	SubtreeHash chainhash.Hash   // announced subtree id (placeholder-based root)
	SubtreeBin  []byte           // bytes served at /subtree/<SubtreeHash>
	BlockBin    []byte           // bytes served at /block/<Hash>
	TxIDs       []chainhash.Hash // non-coinbase txids, in leaf order 1..n
}

// SyntheticBlockSpec parameterizes BuildSyntheticBlock. Two same-height
// competitors are built by calling it twice with the same PrevHash and
// Height but different TxIDs and/or CBExtra (which varies the coinbase
// txid the way two different miners' coinbases differ).
type SyntheticBlockSpec struct {
	PrevHash  chainhash.Hash
	Height    uint32
	Timestamp uint32
	CBExtra   uint32
	// TxIDs are the non-coinbase txids in leaf order. len(TxIDs)+1
	// should ideally be a power of two (e.g. 7 txids → 8 leaves) so no
	// odd-level duplication rules come into play anywhere in the
	// pipeline, though go-subtree and this builder share the standard
	// duplicate-when-odd rule either way.
	TxIDs []chainhash.Hash
}

// BuildSyntheticBlock fabricates a single-subtree block per spec. The
// header nonce is ground so the block hash satisfies the regtest
// difficulty target (cheap: ~2 attempts expected).
func BuildSyntheticBlock(spec SyntheticBlockSpec) (*SyntheticBlock, error) {
	if len(spec.TxIDs) == 0 {
		return nil, fmt.Errorf("synthetic block needs at least one non-coinbase txid")
	}

	coinbase := BuildCoinbase(spec.Height)
	// Vary the coinbase txid between same-height competitors the way two
	// miners' coinbases naturally differ. Version is otherwise unused by
	// every consumer in the pipeline (only the txid matters).
	if spec.CBExtra != 0 {
		coinbase.Version = spec.CBExtra
	}
	cbID := *coinbase.TxIDChainHash()

	// Announced subtree: placeholder at leaf 0.
	announcedLeaves := append([]chainhash.Hash{coinbasePlaceholder}, spec.TxIDs...)
	subtreeHash := SubtreeRoot(announcedLeaves)
	subtreeBin := SubtreeBinary(announcedLeaves)

	// Header root: same tree with the real coinbase txid at leaf 0.
	correctedLeaves := append([]chainhash.Hash{cbID}, spec.TxIDs...)
	merkleRoot := SubtreeRoot(correctedLeaves)

	// Grind the nonce against the regtest target so anything that
	// validates PoW accepts the header.
	nonce, headerBin, blockHash, err := grindHeaderNonce(spec.PrevHash, merkleRoot, spec.Timestamp)
	if err != nil {
		return nil, err
	}

	bits, err := model.NewNBitFromSlice(regtestBits[:])
	if err != nil {
		return nil, fmt.Errorf("regtest nbit: %w", err)
	}
	// sizeBytes is advisory — nothing in the pipeline validates it, but
	// zero looks broken in logs; use a plausible per-tx estimate.
	sizeBytes := uint64(80 + (len(spec.TxIDs)+1)*256)
	blockBin, builtHash, err := BuildBlockBinary(
		spec.PrevHash, merkleRoot, spec.Height, spec.Timestamp, bits, nonce,
		[]chainhash.Hash{subtreeHash}, coinbase, nil,
		uint64(len(spec.TxIDs)+1), sizeBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("build block binary: %w", err)
	}
	if !builtHash.IsEqual(&blockHash) {
		return nil, fmt.Errorf("header serialization mismatch: ground %s, model built %s", blockHash, builtHash)
	}

	return &SyntheticBlock{
		Hash:        blockHash,
		Height:      spec.Height,
		HeaderBin:   headerBin,
		HeaderHex:   hex.EncodeToString(headerBin),
		CoinbaseHex: hexEncodeTx(coinbase),
		CoinbaseID:  cbID,
		MerkleRoot:  merkleRoot,
		SubtreeHash: subtreeHash,
		SubtreeBin:  subtreeBin,
		BlockBin:    blockBin,
		TxIDs:       append([]chainhash.Hash(nil), spec.TxIDs...),
	}, nil
}

// Stage registers the block, its subtree, and its header on the datahub.
func (b *SyntheticBlock) Stage(d *Datahub) {
	d.StageBlock(b.Hash, b.BlockBin)
	if len(b.SubtreeBin) > 0 {
		d.StageSubtree(b.SubtreeHash, b.SubtreeBin)
	}
	d.StageHeader(b.Hash, b.HeaderBin)
}

// BlockMessage renders the teranode gossip announcement for this block,
// pointing consumers at dataHubURL for the block/subtree fetches.
func (b *SyntheticBlock) BlockMessage(dataHubURL string) teranode.BlockMessage {
	return teranode.BlockMessage{
		Hash:       b.Hash.String(),
		Height:     b.Height,
		DataHubURL: dataHubURL,
		Header:     b.HeaderHex,
		Coinbase:   b.CoinbaseHex,
	}
}

// BuildEmptySyntheticBlock fabricates a coinbase-only block (zero
// subtrees) — the cheapest way to extend a chain by one height in a
// reorg scenario. Its header merkle root is the coinbase txid, matching
// Bitcoin's single-tx-block rule; merkle-service takes its coinbase-only
// BLOCK_PROCESSED path and chaintracks gets a well-formed header.
func BuildEmptySyntheticBlock(prevHash chainhash.Hash, height, timestamp, cbExtra uint32) (*SyntheticBlock, error) {
	coinbase := BuildCoinbase(height)
	if cbExtra != 0 {
		coinbase.Version = cbExtra
	}
	cbID := *coinbase.TxIDChainHash()

	nonce, headerBin, blockHash, err := grindHeaderNonce(prevHash, cbID, timestamp)
	if err != nil {
		return nil, err
	}
	bits, err := model.NewNBitFromSlice(regtestBits[:])
	if err != nil {
		return nil, fmt.Errorf("regtest nbit: %w", err)
	}
	blockBin, builtHash, err := BuildBlockBinary(
		prevHash, cbID, height, timestamp, bits, nonce,
		nil, coinbase, nil, 1, 320,
	)
	if err != nil {
		return nil, fmt.Errorf("build block binary: %w", err)
	}
	if !builtHash.IsEqual(&blockHash) {
		return nil, fmt.Errorf("header serialization mismatch: ground %s, model built %s", blockHash, builtHash)
	}
	return &SyntheticBlock{
		Hash:        blockHash,
		Height:      height,
		HeaderBin:   headerBin,
		HeaderHex:   hex.EncodeToString(headerBin),
		CoinbaseHex: hexEncodeTx(coinbase),
		CoinbaseID:  cbID,
		MerkleRoot:  cbID,
		BlockBin:    blockBin,
	}, nil
}

func hexEncodeTx(tx *bt.Tx) string {
	return hex.EncodeToString(tx.Bytes())
}

// grindHeaderNonce serializes the 80-byte header with increasing nonces
// until the hash satisfies the regtest compact target 0x207fffff
// (mantissa 0x7fffff at exponent 0x20 ⇒ target = 0x7fffff << 232; a
// uniformly random hash passes with p≈1/2). Returns the winning nonce,
// header bytes, and block hash.
func grindHeaderNonce(prev, root chainhash.Hash, timestamp uint32) (uint32, []byte, chainhash.Hash, error) {
	target := new(big.Int).Lsh(big.NewInt(0x7fffff), 232)
	header := make([]byte, 80)
	binary.LittleEndian.PutUint32(header[0:4], 1)
	copy(header[4:36], prev[:])
	copy(header[36:68], root[:])
	binary.LittleEndian.PutUint32(header[68:72], timestamp)
	copy(header[72:76], regtestBits[:])
	for nonce := uint32(0); nonce < 1<<20; nonce++ {
		binary.LittleEndian.PutUint32(header[76:80], nonce)
		hash := chainhash.DoubleHashH(header)
		// Numeric comparison uses display order (reversed bytes) as a
		// big-endian integer.
		rev := make([]byte, 32)
		for i := range hash {
			rev[i] = hash[31-i]
		}
		if new(big.Int).SetBytes(rev).Cmp(target) <= 0 {
			out := make([]byte, 80)
			copy(out, header)
			return nonce, out, hash, nil
		}
	}
	return 0, nil, chainhash.Hash{}, fmt.Errorf("no nonce satisfied the regtest target in 2^20 attempts")
}
