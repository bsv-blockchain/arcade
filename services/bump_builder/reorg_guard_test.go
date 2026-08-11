package bump_builder

import (
	"context"
	"sync"
	"testing"
	"time"

	chaintrackslib "github.com/bsv-blockchain/go-chaintracks/chaintracks"
	sdkchainhash "github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

// Anchor-guard unit tests (issue #279): bump-builder must refuse to mark a
// block's txs MINED when chaintracks' active chain provably holds a
// different block at its height, on both the fresh-build and the
// short-circuit redelivery paths — and must fail open on any uncertainty.

// orphanRecordingStore extends mockStore with a recording
// MarkBlocksOrphaned (the embedded interface would panic).
type orphanRecordingStore struct {
	*mockStore

	orphanMu      sync.Mutex
	orphanedCalls [][]string
}

func (s *orphanRecordingStore) MarkBlocksOrphaned(_ context.Context, hashes []string, _ time.Time) error {
	s.orphanMu.Lock()
	defer s.orphanMu.Unlock()
	s.orphanedCalls = append(s.orphanedCalls, append([]string(nil), hashes...))
	return nil
}

// capturePublisher records PublishBulk templates.
type capturePublisher struct {
	mu        sync.Mutex
	bulks     []*models.TransactionStatus
	singles   []*models.TransactionStatus
	subscribe chan *models.TransactionStatus
}

func (p *capturePublisher) Publish(_ context.Context, st *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.singles = append(p.singles, st)
	return nil
}

func (p *capturePublisher) PublishBulk(_ context.Context, template *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := *template
	cp.TxIDs = append([]string(nil), template.TxIDs...)
	p.bulks = append(p.bulks, &cp)
	return nil
}

func (p *capturePublisher) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	if p.subscribe == nil {
		p.subscribe = make(chan *models.TransactionStatus, 16)
	}
	return p.subscribe, nil
}

func (p *capturePublisher) Close() error { return nil }

func (p *capturePublisher) bulkEvents() []*models.TransactionStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*models.TransactionStatus(nil), p.bulks...)
}

func headerWithHash(t *testing.T, hashHex string, height uint32) *chaintrackslib.BlockHeader {
	t.Helper()
	h, err := sdkchainhash.NewHashFromHex(hashHex)
	if err != nil {
		t.Fatalf("parse hash %s: %v", hashHex, err)
	}
	return &chaintrackslib.BlockHeader{Height: height, Hash: *h}
}

const (
	guardBlockA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	guardBlockB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	guardTxid   = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func guardBuilder(st *orphanRecordingStore, stub *stubChaintracks, enabled bool) *Builder {
	return &Builder{
		cfg: &config.Config{
			BumpBuilder: config.BumpBuilderConfig{AnchorGuardEnabled: enabled},
		},
		logger:      zap.NewNop(),
		store:       st,
		chainHeader: stub,
	}
}

func TestAnchorDecision_AllowsActiveBlock(t *testing.T) {
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, guardBlockA, 10))
	b := guardBuilder(&orphanRecordingStore{mockStore: newMockStore()}, stub, true)

	if got := b.anchorDecision(context.Background(), zap.NewNop(), guardBlockA, 10); got != anchorAllow {
		t.Fatalf("active-chain block must be allowed, got %v", got)
	}
}

func TestAnchorDecision_DeniesSameHeightLoser(t *testing.T) {
	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, guardBlockA, 10))
	b := guardBuilder(&orphanRecordingStore{mockStore: newMockStore()}, stub, true)

	if got := b.anchorDecision(context.Background(), zap.NewNop(), guardBlockB, 10); got != anchorDeny {
		t.Fatalf("competitor of the active block must be denied, got %v", got)
	}
}

// TestAnchorDecision_FailsOpen pins the fail-open posture: denial requires
// positive evidence, everything else allows.
func TestAnchorDecision_FailsOpen(t *testing.T) {
	ctx := context.Background()
	logger := zap.NewNop()

	// Unknown height: chaintracks lags the Kafka path.
	stub := &stubChaintracks{}
	b := guardBuilder(&orphanRecordingStore{mockStore: newMockStore()}, stub, true)
	if got := b.anchorDecision(ctx, logger, guardBlockB, 10); got != anchorAllow {
		t.Fatalf("unknown height must fail open, got %v", got)
	}

	// Lookup error.
	stubErr := &stubChaintracks{err: context.DeadlineExceeded}
	b = guardBuilder(&orphanRecordingStore{mockStore: newMockStore()}, stubErr, true)
	if got := b.anchorDecision(ctx, logger, guardBlockB, 10); got != anchorAllow {
		t.Fatalf("lookup error must fail open, got %v", got)
	}

	// Height 0 (placeholder rows / unknown height).
	if got := b.anchorDecision(ctx, logger, guardBlockB, 0); got != anchorAllow {
		t.Fatalf("height 0 must fail open, got %v", got)
	}

	// Guard disabled by config.
	stub2 := &stubChaintracks{}
	stub2.setHeightHeader(10, headerWithHash(t, guardBlockA, 10))
	b = guardBuilder(&orphanRecordingStore{mockStore: newMockStore()}, stub2, false)
	if got := b.anchorDecision(ctx, logger, guardBlockB, 10); got != anchorAllow {
		t.Fatalf("disabled guard must allow, got %v", got)
	}

	// No chainHeader wired at all.
	b = &Builder{cfg: &config.Config{BumpBuilder: config.BumpBuilderConfig{AnchorGuardEnabled: true}}, logger: logger, store: newMockStore()}
	if got := b.anchorDecision(ctx, logger, guardBlockB, 10); got != anchorAllow {
		t.Fatalf("nil chainHeader must allow, got %v", got)
	}
}

// TestTryShortCircuit_GuardDeniesRedelivery: a replayed BLOCK_PROCESSED for
// a block that lost its height must not re-anchor txs from its stored BUMP.
// The block is finalized (processed stamp), orphan-marked into the
// reconciler queue, and its STUMPs pruned — with ZERO MINED writes.
func TestTryShortCircuit_GuardDeniesRedelivery(t *testing.T) {
	ms := &orphanRecordingStore{mockStore: newMockStore()}
	ms.mu.Lock()
	ms.bumps[guardBlockB] = makeMinimalSTUMPAtHeight(t, guardTxid, 10)
	ms.bumpHeights[guardBlockB] = 10
	ms.mu.Unlock()

	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, guardBlockA, 10)) // A holds height 10

	b := guardBuilder(ms, stub, true)
	if !b.tryShortCircuit(context.Background(), zap.NewNop(), guardBlockB) {
		t.Fatal("short-circuit should have handled the redelivery")
	}

	ms.mu.Lock()
	mined := len(ms.minedCalls)
	processed := len(ms.processedCalls)
	deleted := append([]string(nil), ms.deletedBlocks...)
	ms.mu.Unlock()
	if mined != 0 {
		t.Fatalf("guard-denied redelivery must not mark MINED, got %d calls", mined)
	}
	if processed != 1 {
		t.Fatalf("denied block must still be stamped processed, got %d", processed)
	}
	if len(deleted) != 1 || deleted[0] != guardBlockB {
		t.Fatalf("denied block's STUMPs must be pruned, got %v", deleted)
	}
	ms.orphanMu.Lock()
	orphaned := append([][]string(nil), ms.orphanedCalls...)
	ms.orphanMu.Unlock()
	if len(orphaned) != 1 || len(orphaned[0]) != 1 || orphaned[0][0] != guardBlockB {
		t.Fatalf("denied block must be orphan-marked for the reconciler, got %v", orphaned)
	}
}

// TestTryShortCircuit_GuardAllowsActiveBlock: the same redelivery marks
// MINED when the block IS the active-chain block at its height.
func TestTryShortCircuit_GuardAllowsActiveBlock(t *testing.T) {
	ms := &orphanRecordingStore{mockStore: newMockStore()}
	ms.mu.Lock()
	ms.bumps[guardBlockB] = makeMinimalSTUMPAtHeight(t, guardTxid, 10)
	ms.bumpHeights[guardBlockB] = 10
	ms.mu.Unlock()

	stub := &stubChaintracks{}
	stub.setHeightHeader(10, headerWithHash(t, guardBlockB, 10))

	b := guardBuilder(ms, stub, true)
	if !b.tryShortCircuit(context.Background(), zap.NewNop(), guardBlockB) {
		t.Fatal("short-circuit should have handled the redelivery")
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.minedCalls) != 1 || ms.minedCalls[0].blockHash != guardBlockB {
		t.Fatalf("active block's redelivery must mark MINED, got %v", ms.minedCalls)
	}
}

// prevSeedingStore lets a test script the prevs SetMinedByTxIDs returns, to
// exercise setMinedAndPublish's onlyChanged filtering.
type prevSeedingStore struct {
	*mockStore

	prevByTxid map[string]*models.TransactionStatus
}

func (s *prevSeedingStore) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	_, _, _ = s.mockStore.SetMinedByTxIDs(ctx, blockHash, blockHeight, txids) // record the call
	prevs := make([]*models.TransactionStatus, 0, len(txids))
	mined := make([]*models.TransactionStatus, 0, len(txids))
	for _, txid := range txids {
		prev := s.prevByTxid[txid]
		if prev == nil {
			prev = &models.TransactionStatus{TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Now()}
		}
		prevs = append(prevs, prev)
		mined = append(mined, &models.TransactionStatus{
			TxID: txid, Status: models.StatusMined,
			BlockHash: blockHash, BlockHeight: blockHeight, Timestamp: time.Now(),
		})
	}
	return prevs, mined, nil
}

// TestSetMinedAndPublish_OnlyChangedFiltersEvents: rows already anchored to
// the target block must not produce duplicate events on a reconciler
// re-mine; rows re-anchored from another block and fresh mines must.
func TestSetMinedAndPublish_OnlyChangedFiltersEvents(t *testing.T) {
	txAlready, txReanchor, txFresh := "1111", "2222", "3333"
	st := &prevSeedingStore{
		mockStore: newMockStore(),
		prevByTxid: map[string]*models.TransactionStatus{
			txAlready:  {TxID: txAlready, Status: models.StatusMined, BlockHash: guardBlockA, Timestamp: time.Now()},
			txReanchor: {TxID: txReanchor, Status: models.StatusMined, BlockHash: guardBlockB, Timestamp: time.Now()},
			txFresh:    {TxID: txFresh, Status: models.StatusSeenOnNetwork, Timestamp: time.Now()},
		},
	}
	pub := &capturePublisher{}

	changed := setMinedAndPublish(context.Background(), zap.NewNop(), st, pub,
		guardBlockA, 10, []string{txAlready, txReanchor, txFresh},
		models.ExtraInfoReorgReanchor, true)
	if changed != 2 {
		t.Fatalf("expected 2 changed rows (re-anchor + fresh), got %d", changed)
	}

	bulks := pub.bulkEvents()
	if len(bulks) != 1 {
		t.Fatalf("expected 1 bulk event, got %d", len(bulks))
	}
	ev := bulks[0]
	if ev.ExtraInfo != models.ExtraInfoReorgReanchor {
		t.Fatalf("bulk event must carry the re-anchor marker, got %q", ev.ExtraInfo)
	}
	if ev.BlockHash != guardBlockA || len(ev.TxIDs) != 2 {
		t.Fatalf("unexpected bulk event: block=%s txids=%v", ev.BlockHash, ev.TxIDs)
	}
	for _, id := range ev.TxIDs {
		if id == txAlready {
			t.Fatalf("already-anchored tx must be filtered from the fan-out, got %v", ev.TxIDs)
		}
	}
}

// mustHashGuard keeps the transaction import used (NewMerklePath helpers
// live in builder_test.go); referenced here to avoid an unused-import churn
// if helpers move.
var _ = transaction.NewMerklePath
