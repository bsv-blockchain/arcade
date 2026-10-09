package bump_builder

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

// With several merkle-services every block's BLOCK_PROCESSED arrives once
// per service. The redelivery takes the short-circuit path, which re-mines
// the stored BUMP's level-0 set; rows already MINED against this block must
// not fan out a second MINED event, while a tx that was still SEEN_* (e.g.
// registered after the first build) must still publish.
func TestBuilder_HandleMessage_DuplicateBlockProcessed_PublishesOnlyNewlyMined(t *testing.T) {
	const alreadyMinedTx = testTxidHex
	const lateTx = "2222222222222222222222222222222222222222222222222222222222222222"
	ms := newMockStore()
	blockHash := testBlockHash

	stumpA := makeMinimalSTUMP(alreadyMinedTx)
	stumpB := makeMinimalSTUMP(lateTx)
	compound, _, err := bump.BuildCompoundBUMP(
		[]*models.Stump{
			{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpA},
			{BlockHash: blockHash, SubtreeIndex: 1, StumpData: stumpB},
		},
		[]chainhash.Hash{mustHash(t, alreadyMinedTx), mustHash(t, lateTx)}, nil, nil,
	)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	ms.bumps[blockHash] = compound.Bytes()
	ms.bumpHeights[blockHash] = uint64(compound.BlockHeight)
	ms.alreadyMined = map[string]string{alreadyMinedTx: blockHash}

	pub := &recordingPublisher{}
	b := &Builder{
		cfg:       &config.Config{},
		logger:    zap.NewNop().Named("bump-builder"),
		store:     ms,
		publisher: pub,
	}

	before := bumpOutcomeSampleCount(t, "short_circuited")
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("duplicate BLOCK_PROCESSED returned error: %v", err)
	}
	if got := bumpOutcomeSampleCount(t, "short_circuited"); got != before+1 {
		t.Fatalf("short_circuited samples = %d, want %d", got, before+1)
	}

	ms.mu.Lock()
	minedCalls := len(ms.minedCalls)
	processed := len(ms.processedCalls)
	ms.mu.Unlock()
	if minedCalls != 1 {
		t.Fatalf("SetMinedByTxIDs calls = %d, want 1 (idempotent re-mine of the stored set)", minedCalls)
	}
	if processed != 1 {
		t.Fatalf("processed_at must be (re)stamped on the redelivery, got %d calls", processed)
	}

	emitted := pub.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected exactly one MINED event (the late tx), got %d: %+v", len(emitted), emitted)
	}
	if emitted[0].TxID != lateTx || emitted[0].Status != models.StatusMined || emitted[0].BlockHash != blockHash {
		t.Fatalf("published = %+v, want MINED %s@%s", emitted[0], lateTx, blockHash)
	}
}

// A redelivery where every row is already MINED against this block is a pure
// no-op for subscribers: no MINED event at all.
func TestBuilder_HandleMessage_DuplicateBlockProcessed_AllMined_PublishesNothing(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	compound, _, err := bump.BuildCompoundBUMP(
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: makeMinimalSTUMP(testTxidHex)}},
		[]chainhash.Hash{mustHash(t, testTxidHex)}, nil, nil,
	)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	ms.bumps[blockHash] = compound.Bytes()
	ms.bumpHeights[blockHash] = uint64(compound.BlockHeight)
	ms.alreadyMined = map[string]string{testTxidHex: blockHash}

	pub := &recordingPublisher{}
	b := &Builder{cfg: &config.Config{}, logger: zap.NewNop(), store: ms, publisher: pub}
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if emitted := pub.snapshot(); len(emitted) != 0 {
		t.Fatalf("already-MINED rows must not be re-published, got %+v", emitted)
	}
}
