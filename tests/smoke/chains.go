//go:build smoke

package smoke

import (
	"encoding/hex"
	"math/rand"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// ChainOpts tunes the forest BuildChains produces. Defaults aim at the
// dispatcher's interesting regime: enough txs to chunk across multiple
// teranode batches, mixed depths so release cascades have to interleave.
type ChainOpts struct {
	// TotalTxs is the approximate total tx count across all chains.
	// Actual count may be ±MaxDepth-1 because we stop adding chains
	// once we've crossed the threshold.
	TotalTxs int
	// MinDepth and MaxDepth bound per-chain depth (inclusive). Depth=1
	// is a single tx with no children; depth=2 is parent + one child.
	MinDepth int
	MaxDepth int
	// Seed feeds the RNG. Same seed → same forest; same forest → same
	// txids, which makes test failures reproducible.
	Seed int64
}

// BuildChains returns a forest of validator-passing transactions where
// each chain is a parent → child → … leaf list. Children spend their
// parent's single output (index 0) and carry that linkage on their
// PreviousTxID — which is what handlers.collectInputTXIDs reads to
// populate the propagation envelope's input_txids field.
//
// Callers MUST serialize each tx via tx.ExtendedBytes() before posting
// to arcade — the validator's spv.Verify call reads the per-input
// PreviousTxScript + PreviousTxSatoshis the EF wire encoding carries,
// and rejects standard-format txs with "'PreviousTx' not supplied".
// See validator/validator.go:ValidateTransaction and PR #171.
//
// The returned slice is shaped [chain][tx-in-chain]; flatten as needed.
func BuildChains(opts ChainOpts) [][]*bt.Tx {
	if opts.MinDepth < 1 {
		opts.MinDepth = 1
	}
	if opts.MaxDepth < opts.MinDepth {
		opts.MaxDepth = opts.MinDepth
	}
	rng := rand.New(rand.NewSource(opts.Seed)) //nolint:gosec // deterministic test fixture, not crypto

	chains := make([][]*bt.Tx, 0, opts.TotalTxs/((opts.MinDepth+opts.MaxDepth)/2))
	total := 0
	for total < opts.TotalTxs {
		depth := opts.MinDepth + rng.Intn(opts.MaxDepth-opts.MinDepth+1)
		chain := buildOneChain(depth, uint32(total))
		chains = append(chains, chain)
		total += len(chain)
	}
	return chains
}

// buildOneChain builds a depth-element chain. The root carries a synthetic
// PreviousTxID (the same fake-utxo trick BuildValidatableTxs uses); every
// subsequent tx's input points at its predecessor's output 0, so the
// chain hangs off one parent → child edge per level.
func buildOneChain(depth int, nonceBase uint32) []*bt.Tx {
	chain := make([]*bt.Tx, 0, depth)
	// Root: synthetic parent — same trick BuildValidatableTxs uses,
	// 32 bytes of 0x01 as the previous-output. LockTime varies so each
	// chain's root hashes differently.
	const syntheticPrevTxIDHex = "0101010101010101010101010101010101010101010101010101010101010101"
	syntheticPrev, _ := hex.DecodeString(syntheticPrevTxIDHex)
	root := newValidatableTx(syntheticPrev, 0, nonceBase)
	chain = append(chain, root)
	prev := root
	for i := 1; i < depth; i++ {
		// Child spends prev's output 0. PreviousTxID must be the
		// parent's TxID in chainhash (network) byte order, which is
		// what TxIDChainHash returns.
		prevHash := prev.TxIDChainHash()
		child := newValidatableTx(prevHash[:], 0, nonceBase+uint32(i))
		chain = append(chain, child)
		prev = child
	}
	return chain
}

// newValidatableTx mints a single validator-passing tx that spends
// (prevTxID, prevOutputIdx). LockTime is set to nonce so the txid stays
// unique across the entire forest — without that, two chains of identical
// shape would collide.
func newValidatableTx(prevTxID []byte, prevOutputIdx uint32, nonce uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = nonce
	input := &bt.Input{
		SequenceNumber: 0xffffffff,
	}
	_ = input.PreviousTxIDAdd(asChainHash(prevTxID))
	input.PreviousTxOutIndex = prevOutputIdx
	input.PreviousTxScript = opTrueScript()
	// Generous source-satoshi budget so the fee check (now enabled by
	// the validator's spv.Verify call) has plenty of headroom: with a
	// 1-sat output this leaves 9999 sats of fee against a ~62-byte tx,
	// far above any plausible MinFeePerKB. Mirror the e2e harness's
	// BuildValidatableTxs which raised this from 1000 → 10000 when
	// PR #171 tightened the validator.
	input.PreviousTxSatoshis = 10000
	// OP_0 (single byte 0x00) is the smallest push-only, non-empty
	// unlocking script. arcade's pushDataCheck rejects empty unlocking
	// scripts (ErrEmptyUnlockingScript / ErrUnlockingScriptNotPushOnly),
	// and OP_0 pushes an empty byte string the OP_TRUE locking script
	// happily ignores when it pushes 1 — script execution still succeeds.
	input.UnlockingScript = minimalPushScript()
	tx.Inputs = append(tx.Inputs, input)
	tx.AddOutput(&bt.Output{
		Satoshis:      1,
		LockingScript: opTrueScript(),
	})
	return tx
}

func asChainHash(b []byte) *chainhash.Hash {
	var h chainhash.Hash
	copy(h[:], b)
	return &h
}

// opTrueScript is a single-byte OP_TRUE script. arcade's validator
// classifies it as non-data so checkInputs and checkOutputs both accept
// it without needing real signing or pushdata.
func opTrueScript() *bscript.Script {
	s := bscript.Script([]byte{0x51})
	return &s
}

// minimalPushScript is a single-byte OP_0 (0x00) push, the smallest
// push-only script that's non-empty. arcade's policy pushDataCheck
// requires the unlocking script to be push-only, and go-sdk's
// SatoshisPerKilobyte fee model rejects inputs with an empty unlocking
// script (ErrNoUnlockingScript) because it can't size the witness.
// OP_0 pushes an empty byte string onto the stack, which the OP_TRUE
// locking script ignores when it pushes 1 — script execution still
// succeeds. Mirrors tests/e2e/harness/txbuilder.go:minimalPushScript.
func minimalPushScript() *bscript.Script {
	s := bscript.Script([]byte{0x00})
	return &s
}
