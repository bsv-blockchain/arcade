package bump_builder

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
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

// fourLeafVariant encodes a BRC-74 STUMP for a 4-leaf subtree that tracks
// leaves[tracked]: level 0 holds the tracked leaf (Txid marker) and its
// sibling, level 1 the other pair's parent. It is what one merkle-service
// emits when it only has a registration for that one tx.
func fourLeafVariant(leaves [4]chainhash.Hash, tracked int) []byte {
	sibling := tracked ^ 1
	otherPair := (tracked / 2) ^ 1
	parent := transaction.MerkleTreeParent(&leaves[otherPair*2], &leaves[otherPair*2+1])
	isTx := true
	tl, sl := leaves[tracked], leaves[sibling]
	mp := &transaction.MerklePath{
		BlockHeight: 1,
		Path: [][]*transaction.PathElement{
			{
				{Offset: uint64(tracked), Hash: &tl, Txid: &isTx}, //nolint:gosec // tiny test index
				{Offset: uint64(sibling), Hash: &sl},              //nolint:gosec // tiny test index
			},
			{{Offset: uint64(otherPair), Hash: parent}}, //nolint:gosec // tiny test index
		},
	}
	return mp.Bytes()
}

func fourLeafRoot(leaves [4]chainhash.Hash) chainhash.Hash {
	p01 := transaction.MerkleTreeParent(&leaves[0], &leaves[1])
	p23 := transaction.MerkleTreeParent(&leaves[2], &leaves[3])
	return *transaction.MerkleTreeParent(p01, p23)
}

// multiSourceFixture is a single-subtree block of four txs. Service A had
// registrations for L0 only; service B (down while L2 was registered, so it
// never learned about L0) tracks L2. The datahub serves the block so the
// build path has subtree hashes and a header root to validate against.
type multiSourceFixture struct {
	ms        *mockStore
	b         *Builder
	pub       *recordingPublisher
	blockHash string
	leaves    [4]chainhash.Hash
	variantA  []byte
	variantB  []byte
}

func newMultiSourceFixture(t *testing.T) *multiSourceFixture {
	t.Helper()
	f := &multiSourceFixture{ms: newMockStore(), pub: &recordingPublisher{}, blockHash: testBlockHash}
	hexes := [4]string{
		"1111111111111111111111111111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222222222222222222222222222",
		"3333333333333333333333333333333333333333333333333333333333333333",
		"4444444444444444444444444444444444444444444444444444444444444444",
	}
	for i, h := range hexes {
		f.leaves[i] = mustHash(t, h)
	}
	f.variantA = fourLeafVariant(f.leaves, 0)
	f.variantB = fourLeafVariant(f.leaves, 2)
	root := fourLeafRoot(f.leaves)
	datahub := newDatahubServer(root.CloneBytes(), []chainhash.Hash{root})
	t.Cleanup(datahub.Close)
	f.b = newTestBuilder(f.ms, datahub.URL)
	f.b.publisher = f.pub
	return f
}

func (f *multiSourceFixture) storedLeafCount(t *testing.T) int {
	t.Helper()
	f.ms.mu.Lock()
	data := f.ms.bumps[f.blockHash]
	f.ms.mu.Unlock()
	txids, err := levelZeroTxidsFromBUMP(data)
	if err != nil {
		t.Fatalf("stored BUMP: %v", err)
	}
	return len(txids)
}

// Service A's BLOCK_PROCESSED builds from its STUMP alone. Service B's
// delivery then carries leaves the stored BUMP lacks: the compound is rebuilt
// from both variants, the BUMP is overwritten, and only the newly covered txs
// are published. A third delivery (identical set) short-circuits silently.
func TestBuilder_HandleMessage_RedeliveryWithNewLeaves_RebuildsAndPublishesOnlyNew(t *testing.T) {
	f := newMultiSourceFixture(t)
	ctx := context.Background()

	// 1. Service A: STUMP tracking L0, then BLOCK_PROCESSED.
	f.ms.addStump(f.blockHash, 0, f.variantA)
	if err := f.b.handleMessage(ctx, makeBlockProcessedMsg(f.blockHash)); err != nil {
		t.Fatalf("first build: %v", err)
	}
	if got := f.storedLeafCount(t); got != 2 {
		t.Fatalf("first build leaves = %d, want 2 (L0 + sibling)", got)
	}
	if got := len(f.pub.snapshot()); got != 2 {
		t.Fatalf("first build published %d, want 2", got)
	}
	f.ms.mu.Lock()
	if len(f.ms.stumps[f.blockHash]) != 1 || len(f.ms.deletedBlocks) != 0 {
		t.Fatalf("STUMPs must be retained after the build: stumps=%d deletes=%v", len(f.ms.stumps[f.blockHash]), f.ms.deletedBlocks)
	}
	// The rows the first build mined are now MINED in the store.
	f.ms.alreadyMined = map[string]string{f.leaves[0].String(): f.blockHash, f.leaves[1].String(): f.blockHash}
	f.ms.mu.Unlock()

	// 2. Service B: STUMP tracking L2 (a different variant), then BLOCK_PROCESSED.
	f.ms.addStump(f.blockHash, 0, f.variantB)
	beforeRebuilt := bumpOutcomeSampleCount(t, "rebuilt")
	if err := f.b.handleMessage(ctx, makeBlockProcessedMsg(f.blockHash)); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := bumpOutcomeSampleCount(t, "rebuilt"); got != beforeRebuilt+1 {
		t.Fatalf("rebuilt samples = %d, want %d", got, beforeRebuilt+1)
	}
	if got := f.storedLeafCount(t); got != 4 {
		t.Fatalf("rebuilt BUMP leaves = %d, want 4", got)
	}
	emitted := f.pub.snapshot()
	if len(emitted) != 4 {
		t.Fatalf("after rebuild published total = %d, want 4 (2 from the first build + L2, L3)", len(emitted))
	}
	newly := map[string]bool{}
	for _, st := range emitted[2:] {
		newly[st.TxID] = true
	}
	if !newly[f.leaves[2].String()] || !newly[f.leaves[3].String()] {
		t.Fatalf("rebuild must publish exactly the newly covered txs, got %v", newly)
	}
	f.ms.mu.Lock()
	if len(f.ms.processedCalls) != 2 {
		t.Fatalf("processed_at must be stamped on the rebuild too, got %d stamps", len(f.ms.processedCalls))
	}
	f.ms.alreadyMined[f.leaves[2].String()] = f.blockHash
	f.ms.alreadyMined[f.leaves[3].String()] = f.blockHash
	f.ms.mu.Unlock()

	// 3. Service C delivers the same STUMP as B: nothing new, short-circuit.
	f.ms.addStump(f.blockHash, 0, f.variantB)
	beforeShort := bumpOutcomeSampleCount(t, "short_circuited")
	if err := f.b.handleMessage(ctx, makeBlockProcessedMsg(f.blockHash)); err != nil {
		t.Fatalf("third delivery: %v", err)
	}
	if got := bumpOutcomeSampleCount(t, "short_circuited"); got != beforeShort+1 {
		t.Fatalf("short_circuited samples = %d, want %d", got, beforeShort+1)
	}
	if got := len(f.pub.snapshot()); got != 4 {
		t.Fatalf("short-circuit must publish nothing, total = %d", got)
	}
	if got := f.storedLeafCount(t); got != 4 {
		t.Fatalf("short-circuit must not rewrite the BUMP, leaves = %d", got)
	}
}

// Both variants are present before the first build (the common ordering when
// services run in lockstep): one build, one BUMP covering the union.
func TestBuilder_HandleMessage_VariantsBeforeFirstBuild_SingleBuildCoversUnion(t *testing.T) {
	f := newMultiSourceFixture(t)
	f.ms.addStump(f.blockHash, 0, f.variantA)
	f.ms.addStump(f.blockHash, 0, f.variantB)
	if err := f.b.handleMessage(context.Background(), makeBlockProcessedMsg(f.blockHash)); err != nil {
		t.Fatalf("build: %v", err)
	}
	if got := f.storedLeafCount(t); got != 4 {
		t.Fatalf("leaves = %d, want 4", got)
	}
	if got := len(f.pub.snapshot()); got != 4 {
		t.Fatalf("published = %d, want 4", got)
	}
}

// Service A's build stamped processed_at. Service B's STUMP for a new subtree
// arrived, but B's expected set names another subtree whose STUMP is still
// missing after the grace window: the block is deferred AND the stamp is
// cleared so the watchdog re-drives it. The stored BUMP is untouched.
func TestBuilder_HandleMessage_DeferredAfterStamp_ClearsProcessedAt(t *testing.T) {
	f := newMultiSourceFixture(t)
	ctx := context.Background()
	f.ms.addStump(f.blockHash, 0, f.variantA)
	if err := f.b.handleMessage(ctx, makeBlockProcessedMsg(f.blockHash)); err != nil {
		t.Fatalf("first build: %v", err)
	}
	stamped := time.Now()
	f.ms.mu.Lock()
	f.ms.blockProc = []*models.BlockProcessingStatus{{BlockHash: f.blockHash, BlockHeight: 1, ProcessedAt: &stamped}}
	f.ms.mu.Unlock()
	f.b.cfg.BumpBuilder.GraceWindowMs = 0

	const lateTx = "5555555555555555555555555555555555555555555555555555555555555555"
	f.ms.addStump(f.blockHash, 1, makeMinimalSTUMP(lateTx)) // new leaf ⇒ rebuild path
	beforeDeferred := bumpOutcomeSampleCount(t, "deferred_incomplete")
	if err := f.b.handleMessage(ctx, makeBlockProcessedMsgWithExpected(f.blockHash, []int{1, 2})); err != nil {
		t.Fatalf("deferred delivery must return nil (watchdog recovers), got %v", err)
	}
	if got := bumpOutcomeSampleCount(t, "deferred_incomplete"); got != beforeDeferred+1 {
		t.Fatalf("deferred_incomplete samples = %d, want %d", got, beforeDeferred+1)
	}
	f.ms.mu.Lock()
	defer f.ms.mu.Unlock()
	if len(f.ms.clearedBlocks) != 1 || f.ms.clearedBlocks[0] != f.blockHash {
		t.Fatalf("ClearBlockProcessed calls = %v, want [%s]", f.ms.clearedBlocks, f.blockHash)
	}
	if f.ms.blockProc[0].ProcessedAt != nil {
		t.Fatal("processed_at must be cleared on the fixture row")
	}
	if got := len(f.pub.snapshot()); got != 2 {
		t.Fatalf("deferral must publish nothing new, total = %d", got)
	}
}

// If the janitor already pruned service A's STUMP, a rebuild from B alone
// would drop L0/L1 from the stored BUMP. The builder keeps the stored BUMP,
// publishes nothing, and hands the block back to the watchdog.
func TestBuilder_HandleMessage_RebuildWouldDropLeaves_KeepsBUMPAndDefers(t *testing.T) {
	f := newMultiSourceFixture(t)
	ctx := context.Background()
	f.ms.addStump(f.blockHash, 0, f.variantA)
	if err := f.b.handleMessage(ctx, makeBlockProcessedMsg(f.blockHash)); err != nil {
		t.Fatalf("first build: %v", err)
	}
	stamped := time.Now()
	f.ms.mu.Lock()
	f.ms.blockProc = []*models.BlockProcessingStatus{{BlockHash: f.blockHash, BlockHeight: 1, ProcessedAt: &stamped}}
	delete(f.ms.stumps, f.blockHash) // janitor pruned A's STUMP
	f.ms.alreadyMined = map[string]string{f.leaves[0].String(): f.blockHash, f.leaves[1].String(): f.blockHash}
	before := f.ms.bumps[f.blockHash]
	f.ms.mu.Unlock()

	f.ms.addStump(f.blockHash, 0, f.variantB)
	if err := f.b.handleMessage(ctx, makeBlockProcessedMsg(f.blockHash)); err != nil {
		t.Fatalf("delivery: %v", err)
	}
	f.ms.mu.Lock()
	defer f.ms.mu.Unlock()
	if string(f.ms.bumps[f.blockHash]) != string(before) {
		t.Fatal("stored BUMP must not be overwritten by a compound that drops leaves")
	}
	if len(f.ms.clearedBlocks) != 1 {
		t.Fatalf("block must be handed back to the watchdog, cleared=%v", f.ms.clearedBlocks)
	}
	if got := len(f.pub.snapshot()); got != 2 {
		t.Fatalf("nothing new may be published, total = %d", got)
	}
}
