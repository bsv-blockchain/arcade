package bump_builder

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
)

// TestBuilder_HandleMessage_SetMinedFailure_StaysRecoverable pins the MINED
// durability contract. SetMinedByTxIDs failing must not look like a finished
// block:
//
//   - handleMessage returns nil. ConsumerGroup.processOne commits the offset
//     on a nil error (kafka.TestProcessOne_HappyPathMarks). Kafka redelivery
//     is not the recovery path; a non-nil error would retry and then
//     dead-letter a message that cannot finish on its own.
//   - processed_at stays unset, which is the predicate
//     ListStaleBlockProcessingStatus uses (processed_at IS NULL). The
//     watchdog can therefore re-drive the block.
//   - the compound BUMP is already stored, so that re-drive takes
//     tryShortCircuit and re-mines. Rows already MINED are a no-op.
func TestBuilder_HandleMessage_SetMinedFailure_StaysRecoverable(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)
	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	ms.setMinedErr = errors.New("store: set mined failed")
	b := newTestBuilder(ms, datahub.URL)

	beforeFailed := bumpOutcomeSampleCount(t, "store_failed")
	beforeBenign := bumpOutcomeSampleCount(t, "finalized_complete_no_grace")

	err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0}))
	if err != nil {
		t.Fatalf("SetMined failure must return nil so the offset is committed; Kafka redelivery is not the recovery path, got: %v", err)
	}
	if got, want := bumpOutcomeSampleCount(t, "store_failed"), beforeFailed+1; got != want {
		t.Fatalf("store_failed samples = %d, want %d", got, want)
	}
	if got := bumpOutcomeSampleCount(t, "finalized_complete_no_grace"); got != beforeBenign {
		t.Fatalf("finalized_complete_no_grace samples = %d, want %d; a failed mine must not look finished", got, beforeBenign)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.setMinedAttempts != 1 {
		t.Fatalf("SetMinedByTxIDs attempts = %d, want 1", ms.setMinedAttempts)
	}
	if len(ms.minedCalls) != 0 {
		t.Fatalf("a failed SetMinedByTxIDs must not record a completed mine, got %+v", ms.minedCalls)
	}
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Fatal("BUMP must stay stored so the watchdog re-drive can short-circuit and re-mine")
	}
	if len(ms.processedCalls) != 0 {
		t.Fatalf("processed_at must stay unset so ListStaleBlockProcessingStatus still returns this block, got %+v", ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_GracePathSetMinedFailure_StoreFailedOutcome is
// the other fresh-build disposition. An absent expected set with stored
// STUMPs lands on grace_waited. A failed mine must replace that benign
// label with store_failed and still leave processed_at unset.
func TestBuilder_HandleMessage_GracePathSetMinedFailure_StoreFailedOutcome(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)
	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	ms.setMinedErr = errors.New("store: set mined failed")
	b := newTestBuilder(ms, datahub.URL)
	b.cfg.BumpBuilder.GraceWindowMs = 0

	beforeFailed := bumpOutcomeSampleCount(t, "store_failed")
	beforeGrace := bumpOutcomeSampleCount(t, "grace_waited")

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash))
	if err != nil {
		t.Fatalf("grace-path SetMined failure must return nil, got: %v", err)
	}
	if got, want := bumpOutcomeSampleCount(t, "store_failed"), beforeFailed+1; got != want {
		t.Fatalf("store_failed samples = %d, want %d", got, want)
	}
	if got := bumpOutcomeSampleCount(t, "grace_waited"); got != beforeGrace {
		t.Fatalf("grace_waited samples = %d, want %d; a failed mine must not look finished", got, beforeGrace)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.processedCalls) != 0 {
		t.Fatalf("processed_at must stay unset, got %+v", ms.processedCalls)
	}
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Fatal("BUMP must stay stored so the watchdog can re-drive")
	}
}

// TestBuilder_HandleMessage_ShortCircuitSetMinedFailure_StoreFailedOutcome
// covers the redelivery path. A stored BUMP skips the rebuild. A failed
// mine must be store_failed, not short_circuited, and processed_at stays
// unset so the watchdog can re-drive.
func TestBuilder_HandleMessage_ShortCircuitSetMinedFailure_StoreFailedOutcome(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	ms.mu.Lock()
	ms.bumps[blockHash] = makeMinimalSTUMPAtHeight(t, testTxidHex, 1)
	ms.bumpHeights[blockHash] = 1
	ms.mu.Unlock()
	ms.setMinedErr = errors.New("store: set mined failed")

	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}

	beforeFailed := bumpOutcomeSampleCount(t, "store_failed")
	beforeShort := bumpOutcomeSampleCount(t, "short_circuited")

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash))
	if err != nil {
		t.Fatalf("short-circuit SetMined failure must return nil, got: %v", err)
	}
	if got, want := bumpOutcomeSampleCount(t, "store_failed"), beforeFailed+1; got != want {
		t.Fatalf("store_failed samples = %d, want %d", got, want)
	}
	if got := bumpOutcomeSampleCount(t, "short_circuited"); got != beforeShort {
		t.Fatalf("short_circuited samples = %d, want %d; a failed mine must not look skipped-and-done", got, beforeShort)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.setMinedAttempts != 1 {
		t.Fatalf("SetMinedByTxIDs attempts = %d, want 1", ms.setMinedAttempts)
	}
	if len(ms.processedCalls) != 0 {
		t.Fatalf("processed_at must stay unset so the watchdog can re-drive, got %+v", ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_ZeroStumps_NoExpectedSet_StampsProcessedAt pins
// finalizeEmptyBlock. A BLOCK_PROCESSED with no stored STUMPs and no
// expectedSubtreeIndices on the wire is the "no tracked txs" case: Merkle
// omits the expected set only when it has nothing to report. processed_at
// is stamped (height 0, which does not clobber a chaintracks height) so the
// watchdog does not re-drive the block. A missing expected set that still
// has STUMP indices to satisfy is a different path and must not finalize.
func TestBuilder_HandleMessage_ZeroStumps_NoExpectedSet_StampsProcessedAt(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	raw := []byte(`{"type":"BLOCK_PROCESSED","blockHash":"` + blockHash + `"}`)
	if bytes.Contains(raw, []byte("expectedSubtreeIndices")) {
		t.Fatal("fixture must omit expectedSubtreeIndices")
	}

	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}
	msg := &kafka.Message{Topic: kafka.TopicBlockProcessed, Value: raw}
	if err := b.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("zero-STUMP finalize must return nil, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.stumps[blockHash]) != 0 {
		t.Fatalf("fixture must start with zero STUMPs, got %d", len(ms.stumps[blockHash]))
	}
	if len(ms.bumps) != 0 {
		t.Fatalf("an empty block must not store a BUMP, got %d", len(ms.bumps))
	}
	if ms.setMinedAttempts != 0 {
		t.Fatalf("an empty block must not call SetMinedByTxIDs, got %d", ms.setMinedAttempts)
	}
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash || ms.processedCalls[0].blockHeight != 0 {
		t.Fatalf("finalizeEmptyBlock must stamp processed_at at height 0, got %+v", ms.processedCalls)
	}
}
