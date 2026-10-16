package bump_builder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

// With several merkle-services every block's BLOCK_PROCESSED arrives once
// per service, and each redelivery takes the short-circuit, which re-mines the
// stored BUMP's level-0 set. Without a way to un-stamp a block whose re-mine
// or publish fails, the short-circuit cannot safely suppress rows already
// MINED (see tryShortCircuit), so it republishes the whole set, even on a
// stamped block: a duplicate event rather than a lost one.
func TestBuilder_HandleMessage_DuplicateBlockProcessed_RepublishesStoredSet(t *testing.T) {
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
	stampedBlock(ms, blockHash)

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

	got := map[string]bool{}
	for _, st := range pub.snapshot() {
		if st.Status != models.StatusMined || st.BlockHash != blockHash {
			t.Fatalf("unexpected event %+v", st)
		}
		got[st.TxID] = true
	}
	if len(got) != 2 || !got[alreadyMinedTx] || !got[lateTx] {
		t.Fatalf("the short-circuit must republish the whole stored set, got %v", got)
	}
}

// stampedBlock gives blockHash a block_processing row with processed_at set:
// an earlier delivery finalized the block, so its MINED rows were published.
func stampedBlock(ms *mockStore, blockHash string) {
	now := time.Now()
	ms.blockProc = append(ms.blockProc, &models.BlockProcessingStatus{BlockHash: blockHash, ProcessedAt: &now})
}

// A crash between SetMinedByTxIDs and PublishBulk leaves rows MINED but
// unpublished, and the block un-stamped. The redelivery must not take "already
// MINED" as "already published": with processed_at unset it republishes the
// whole stored set.
func TestBuilder_HandleMessage_RedeliveryOfUnstampedBlock_RepublishesMinedRows(t *testing.T) {
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
	emitted := pub.snapshot()
	if len(emitted) != 1 || emitted[0].TxID != testTxidHex || emitted[0].Status != models.StatusMined {
		t.Fatalf("an unstamped block's MINED rows must be republished, got %+v", emitted)
	}
}

// failingBulkPublisher refuses every PublishBulk.
type failingBulkPublisher struct{ recordingPublisher }

func (p *failingBulkPublisher) PublishBulk(context.Context, *models.TransactionStatus) error {
	return errors.New("kafka down")
}

// processed_at is the proof blockFinalized relies on that a block's MINED
// events went out, so a failed PublishBulk must withhold it: the watchdog's
// re-drive then sees an unstamped block and republishes in full.
func TestBuilder_HandleMessage_PublishFailure_LeavesBlockUnstamped(t *testing.T) {
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

	b := &Builder{cfg: &config.Config{}, logger: zap.NewNop(), store: ms, publisher: &failingBulkPublisher{}}
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.minedCalls) != 1 {
		t.Fatalf("the rows must still be mined, SetMinedByTxIDs calls = %d", len(ms.minedCalls))
	}
	if len(ms.processedCalls) != 0 {
		t.Fatalf("a failed MINED publish must leave processed_at unset, got %d stamps", len(ms.processedCalls))
	}
}
