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

	err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0}))
	if err != nil {
		t.Fatalf("SetMined failure must return nil so the offset is committed; Kafka redelivery is not the recovery path, got: %v", err)
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
