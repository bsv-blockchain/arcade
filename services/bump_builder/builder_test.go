package bump_builder

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	chaintrackslib "github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/util"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// testBlockHash is a synthetic block hash reused across the table-style tests
// in this file. Lifted to a constant to satisfy goconst now that the
// regression tests for issue #87 push the literal count past the threshold.
const testBlockHash = "aabbccdd00000000000000000000000000000000000000000000000000000000"

// testTxidHex is the canonical synthetic txid hash used across the
// happy-path / multi-subtree / mark-bump-built tests. Same goconst-
// satisfaction motivation as testBlockHash above.
const testTxidHex = "1111111111111111111111111111111111111111111111111111111111111111"

// --- Mock Store ---

type mockStore struct {
	store.Store // embed — panics on unimplemented methods

	mu               sync.Mutex
	stumps           map[string][]*models.Stump // blockHash → stumps
	bumps            map[string][]byte          // blockHash → bumpData
	bumpHeights      map[string]uint64          // blockHash → blockHeight
	minedCalls       []minedCall
	deletedBlocks    []string
	bumpBuiltCalls   []bumpBuiltCall
	processedCalls   []bumpBuiltCall
	getStumpsErr     error
	insertBUMPErr    error
	insertStumpErr   error
	setMinedErr      error
	deleteStumpsErr  error
	markBumpBuiltErr error
	markProcessedErr error

	// Janitor fixtures: tipHeight feeds GetActiveTipBlockHeight; blockProc
	// feeds ListBlockProcessingStatus. Both default empty so existing tests
	// (which never set them) see the "fresh deployment" early-exit path.
	tipHeight uint64
	blockProc []*models.BlockProcessingStatus
}

type bumpBuiltCall struct {
	blockHash   string
	blockHeight uint64
}

type minedCall struct {
	blockHash   string
	blockHeight uint64
	txids       []string
}

func newMockStore() *mockStore {
	return &mockStore{
		stumps:      make(map[string][]*models.Stump),
		bumps:       make(map[string][]byte),
		bumpHeights: make(map[string]uint64),
	}
}

// GetBUMP returns a stored compound BUMP if InsertBUMP previously wrote one
// for this block, or ErrNotFound otherwise. The short-circuit path in
// handleMessage calls this before any other work, so the default empty-map
// behavior keeps the existing tests on the rebuild path.
func (m *mockStore) GetBUMP(_ context.Context, blockHash string) (uint64, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.bumps[blockHash]
	if !ok {
		return 0, nil, store.ErrNotFound
	}
	return m.bumpHeights[blockHash], data, nil
}

// GetActiveTipBlockHeight feeds the orphan-stump janitor's startup scan.
// Tests that exercise the janitor set tipHeight directly; everything else
// gets 0 (signals "empty store", janitor exits early).
func (m *mockStore) GetActiveTipBlockHeight(_ context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tipHeight, nil
}

// ListBlockProcessingStatus returns rows from blockProc ordered by
// block_height DESC, with a "block_height < beforeHeight" cursor. Matches
// the production keyset paging contract used by the orphan-stump janitor.
func (m *mockStore) ListBlockProcessingStatus(_ context.Context, beforeHeight uint64, limit int) ([]*models.BlockProcessingStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		return nil, nil
	}
	out := make([]*models.BlockProcessingStatus, 0, limit)
	// Iterate a sorted copy so test order is deterministic.
	rows := append([]*models.BlockProcessingStatus(nil), m.blockProc...)
	// Sort by height descending.
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			if rows[j].BlockHeight > rows[i].BlockHeight {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
	for _, r := range rows {
		if beforeHeight > 0 && r.BlockHeight >= beforeHeight {
			continue
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *mockStore) EnsureIndexes() error { return nil }

func (m *mockStore) InsertStump(_ context.Context, stump *models.Stump) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertStumpErr != nil {
		return m.insertStumpErr
	}
	m.stumps[stump.BlockHash] = append(m.stumps[stump.BlockHash], stump)
	return nil
}

func (m *mockStore) GetStumpsByBlockHash(_ context.Context, blockHash string) ([]*models.Stump, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getStumpsErr != nil {
		return nil, m.getStumpsErr
	}
	return m.stumps[blockHash], nil
}

func (m *mockStore) InsertBUMP(_ context.Context, blockHash string, _ uint64, bumpData []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertBUMPErr != nil {
		return m.insertBUMPErr
	}
	m.bumps[blockHash] = bumpData
	return nil
}

func (m *mockStore) SetMinedByTxIDs(_ context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setMinedErr != nil {
		return nil, nil, m.setMinedErr
	}
	m.minedCalls = append(m.minedCalls, minedCall{blockHash, blockHeight, txids})
	var prevs, statuses []*models.TransactionStatus
	for _, txid := range txids {
		prevs = append(prevs, &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusSeenOnNetwork,
			Timestamp: time.Now(),
		})
		statuses = append(statuses, &models.TransactionStatus{
			TxID:        txid,
			Status:      models.StatusMined,
			BlockHash:   blockHash,
			BlockHeight: blockHeight,
			Timestamp:   time.Now(),
		})
	}
	return prevs, statuses, nil
}

func (m *mockStore) DeleteStumpsByBlockHash(_ context.Context, blockHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteStumpsErr != nil {
		return m.deleteStumpsErr
	}
	m.deletedBlocks = append(m.deletedBlocks, blockHash)
	delete(m.stumps, blockHash)
	return nil
}

// MarkBlockBUMPBuilt records the call so tests can assert observability
// reaches the store after a successful BUMP build. markBumpBuiltErr lets a
// test inject a failure to verify the builder logs and continues.
func (m *mockStore) MarkBlockBUMPBuilt(_ context.Context, blockHash string, blockHeight uint64, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markBumpBuiltErr != nil {
		return m.markBumpBuiltErr
	}
	m.bumpBuiltCalls = append(m.bumpBuiltCalls, bumpBuiltCall{blockHash: blockHash, blockHeight: blockHeight})
	return nil
}

// MarkBlockProcessed records the call so tests can assert that bump-builder —
// not the HTTP handler — stamps processed_at, and only on a finalized block.
// markProcessedErr injects a failure to verify the soft-fail (warn + continue)
// path that the watchdog backstops.
func (m *mockStore) MarkBlockProcessed(_ context.Context, blockHash string, blockHeight uint64, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markProcessedErr != nil {
		return m.markProcessedErr
	}
	m.processedCalls = append(m.processedCalls, bumpBuiltCall{blockHash: blockHash, blockHeight: blockHeight})
	return nil
}

func (m *mockStore) addStump(blockHash string, subtreeIndex int, stumpData []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stumps[blockHash] = append(m.stumps[blockHash], &models.Stump{
		BlockHash:    blockHash,
		SubtreeIndex: subtreeIndex,
		StumpData:    stumpData,
	})
}

// --- Helpers ---

// makeBlockProcessedMsg creates a kafka.Message with a block_processed payload.
func makeBlockProcessedMsg(blockHash string) *kafka.Message {
	callback := models.CallbackMessage{
		Type:      models.CallbackBlockProcessed,
		BlockHash: blockHash,
	}
	data, err := json.Marshal(callback)
	if err != nil {
		panic(err)
	}
	return &kafka.Message{
		Topic: "arcade.block_processed",
		Value: data,
	}
}

// makeBlockProcessedMsgWithExpected is makeBlockProcessedMsg plus the merkle
// expected-STUMP index set (PR #162), so completeness-gate tests can pin which
// subtrees merkle says should have produced a STUMP for this block.
func makeBlockProcessedMsgWithExpected(blockHash string, expected []int) *kafka.Message {
	callback := models.CallbackMessage{
		Type:                   models.CallbackBlockProcessed,
		BlockHash:              blockHash,
		ExpectedSubtreeIndices: expected,
	}
	data, err := json.Marshal(callback)
	if err != nil {
		panic(err)
	}
	return &kafka.Message{
		Topic: "arcade.block_processed",
		Value: data,
	}
}

// buildBlockHeader returns an 80-byte block header with the given merkle root
// embedded at bytes 36:68 (standard BSV header layout). Other fields are zero.
func buildBlockHeader(merkleRoot []byte) []byte {
	header := make([]byte, 80)
	if len(merkleRoot) == 32 {
		copy(header[36:68], merkleRoot)
	}
	return header
}

// newDatahubServer creates a test HTTP server that serves binary block data for BUMP construction.
// Binary format: header (80) | txCount (varint) | sizeBytes (varint) |
// subtreeCount (varint) | subtreeHashes (N×32) | coinbaseTx (variable) |
// blockHeight (varint) | coinbaseBUMPLen (varint)
//
// merkleRoot is written into the header at bytes 36:68 so the builder's
// validation step can compare the compound BUMP's computed root against it.
// Pass zeroMerkleRoot() when the test does not reach validation.
//
// Subtree hashes are written in internal byte order (CloneBytes), matching
// what parseBlockBinary expects (it uses chainhash.NewHash on raw bytes, which
// does not reverse — unlike chainhash.NewHashFromHex).
func newDatahubServer(merkleRoot []byte, subtreeHashes []chainhash.Hash) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var buf bytes.Buffer

		buf.Write(buildBlockHeader(merkleRoot))

		// Transaction count (varint: 1)
		buf.WriteByte(0x01)

		// Size in bytes (varint: 0 — not used by parser)
		buf.WriteByte(0x00)

		// Subtree count (varint)
		buf.Write(util.VarInt(len(subtreeHashes)).Bytes())

		// Subtree hashes (32 bytes each, internal byte order)
		for i := range subtreeHashes {
			buf.Write(subtreeHashes[i].CloneBytes())
		}

		// Minimal coinbase transaction
		coinbaseTx := transaction.NewTransaction()
		buf.Write(coinbaseTx.Bytes())

		// Block height (varint: 1)
		buf.WriteByte(0x01)

		// Coinbase BUMP length (varint: 0 = no BUMP)
		buf.WriteByte(0x00)

		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(buf.Bytes())
	}))
}

// mustHash converts a hex string into a chainhash.Hash or fatal-fails the test.
// Uses raw byte interpretation (not NewHashFromHex's reversed form) since the
// test hexes represent internal byte order for fake/synthetic hashes.
func mustHash(t *testing.T, hexStr string) chainhash.Hash {
	t.Helper()
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("bad hex: %v", err)
	}
	var h chainhash.Hash
	if err := h.SetBytes(b); err != nil {
		t.Fatalf("SetBytes: %v", err)
	}
	return h
}

func newFailingDatahubServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

func newTestBuilder(st store.Store, datahubURL string) *Builder {
	cfg := &config.Config{
		DatahubURLs: []string{datahubURL},
	}
	tc := teranode.NewClient([]string{datahubURL}, "", teranode.HealthConfig{})
	return &Builder{
		cfg:      cfg,
		logger:   zap.NewNop().Named("bump-builder"),
		store:    st,
		teranode: tc,
	}
}

// makeMinimalSTUMP creates a minimal valid BRC-74 STUMP binary with a single level-0 leaf.
// This is the simplest valid merkle path: one transaction, one level, block height 1.
func makeMinimalSTUMP(txidHex string) []byte {
	// BRC-74 flag bits: bit 0 = duplicate (no hash follows), bit 1 = txid.
	// We want to encode the tracked txid itself, so flags = 0x02.
	txidBytes, _ := hex.DecodeString(txidHex)
	if len(txidBytes) != 32 {
		padded := make([]byte, 32)
		copy(padded, txidBytes)
		txidBytes = padded
	}

	buf := make([]byte, 0, 5+len(txidBytes))
	buf = append(
		buf,
		0x01, // blockHeight = 1
		0x01, // treeHeight = 1 (one level)
		0x01, // nLeaves at level 0 = 1
		0x00, // offset = 0
		0x02, // flags: bit 1 = txid (hash follows)
	)
	buf = append(buf, txidBytes...)
	return buf
}

// expectedCompoundRoot runs the same assembly the builder will run and returns
// the merkle root the compound will produce. Test setups use this so the
// datahub-served header merkle root matches the compound that will actually
// be built, letting the validation step succeed on happy paths.
func expectedCompoundRoot(t *testing.T, stumps []*models.Stump, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) []byte {
	t.Helper()
	// Pass copies so we don't mutate the caller's slices (BuildCompoundBUMP
	// rewrites subtreeHashes[0] when a coinbase BUMP is present).
	stumpsCopy := make([]*models.Stump, len(stumps))
	for i, s := range stumps {
		sc := *s
		stumpsCopy[i] = &sc
	}
	hashesCopy := make([]chainhash.Hash, len(subtreeHashes))
	copy(hashesCopy, subtreeHashes)

	compound, _, err := bump.BuildCompoundBUMP(stumpsCopy, hashesCopy, coinbaseBUMP)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP failed: %v", err)
	}
	var leaf *chainhash.Hash
	for _, l := range compound.Path[0] {
		if l.Hash != nil {
			leaf = l.Hash
			break
		}
	}
	if leaf == nil {
		t.Fatalf("compound has no level-0 hash")
	}
	root, err := compound.ComputeRoot(leaf)
	if err != nil {
		t.Fatalf("ComputeRoot failed: %v", err)
	}
	return root.CloneBytes()
}

// zeroMerkleRoot returns 32 zero bytes for tests that never reach validation
// (getStumps error, empty blocks, datahub failure, bad JSON).
func zeroMerkleRoot() []byte { return make([]byte, 32) }

// makeTwoLeafSTUMP builds a BRC-74 STUMP for a 2-tx subtree with the tracked
// txid at offset 0 and a sibling hash at offset 1. Used in multi-subtree tests
// so each subtree's internalHeight (=1) matches a real subtreeSize (=2).
func makeTwoLeafSTUMP(txidHex, siblingHex string) []byte {
	txidBytes, _ := hex.DecodeString(txidHex)
	sibBytes, _ := hex.DecodeString(siblingHex)
	if len(txidBytes) != 32 {
		p := make([]byte, 32)
		copy(p, txidBytes)
		txidBytes = p
	}
	if len(sibBytes) != 32 {
		p := make([]byte, 32)
		copy(p, sibBytes)
		sibBytes = p
	}

	buf := make([]byte, 0, 5+len(txidBytes)+2+len(sibBytes))
	buf = append(
		buf,
		0x01, // blockHeight = 1
		0x01, // treeHeight = 1
		0x02, // nLeaves at level 0 = 2
		0x00, // offset 0
		0x02, // flags: txid
	)
	buf = append(buf, txidBytes...)
	buf = append(
		buf,
		0x01, // offset 1
		0x00, // flags: data
	)
	buf = append(buf, sibBytes...)
	return buf
}

// subtreeRootFromTwoLeafSTUMP returns the merkle root of the 2-leaf subtree
// encoded by makeTwoLeafSTUMP (i.e. MerkleTreeParent(txid, sibling)). Uses raw
// byte interpretation of the hex so it matches the bytes written by
// makeTwoLeafSTUMP and read back by the STUMP parser.
func subtreeRootFromTwoLeafSTUMP(t *testing.T, txidHex, siblingHex string) chainhash.Hash {
	t.Helper()
	txid := mustHash(t, txidHex)
	sib := mustHash(t, siblingHex)
	return *transaction.MerkleTreeParent(&txid, &sib)
}

// --- Tests ---

func TestBuilder_HandleMessage_NoSTUMPs_ReturnsNil(t *testing.T) {
	// A block with no STUMPs means no tracked transactions in it — this is
	// legitimate (STUMPs are sparse) and must not return an error or trigger DLQ.
	ms := newMockStore()
	datahub := newDatahubServer(zeroMerkleRoot(), nil)
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg("blockhash123")); err != nil {
		t.Fatalf("expected nil when no STUMPs found, got: %v", err)
	}
	if len(ms.bumps) != 0 {
		t.Errorf("expected no BUMP stored, got %d", len(ms.bumps))
	}
}

func TestBuilder_HandleMessage_GetStumpsError_Propagated(t *testing.T) {
	ms := newMockStore()
	ms.getStumpsErr = errors.New("INDEX_NOTFOUND")

	datahub := newDatahubServer(zeroMerkleRoot(), nil)
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg("blockhash123"))
	if err == nil {
		t.Fatal("expected error from GetStumpsByBlockHash, got nil")
	}
	if !containsStr(err.Error(), "INDEX_NOTFOUND") {
		t.Errorf("expected INDEX_NOTFOUND in error, got: %v", err)
	}
}

func TestBuilder_HandleMessage_DatahubFailure_ReturnsError(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	stumpData := makeMinimalSTUMP(testTxidHex)
	ms.addStump(blockHash, 0, stumpData)

	datahub := newFailingDatahubServer()
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash))
	if err == nil {
		t.Fatal("expected error from datahub failure, got nil")
	}
	if !containsStr(err.Error(), "fetching block data") {
		t.Errorf("expected 'fetching block data' error, got: %v", err)
	}
}

func TestBuilder_HandleMessage_InsertBUMPError_ReturnsError(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash

	// Need a valid STUMP that produces a valid BUMP. Use a single-subtree scenario.
	txidHex := testTxidHex
	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)
	ms.insertBUMPErr = errors.New("SERVER_MEM_ERROR")

	// Single-subtree with a single-leaf STUMP: compound root == the txid itself.
	// The subtreeHash slot is unused by assembleFullPath in the 1-subtree case.
	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash))
	if err == nil {
		t.Fatal("expected error from InsertBUMP, got nil")
	}
	if !containsStr(err.Error(), "storing BUMP") {
		t.Errorf("expected 'storing BUMP' error, got: %v", err)
	}
}

// TestBuilder_HandleMessage_ShortCircuit_BUMPAlreadyExists checks the
// short-circuit path: when a compound BUMP is already stored for the block,
// handleMessage must not re-fetch from datahub or rebuild. It should re-run
// SetMinedByTxIDs with the level-0 hashes from the stored BUMP so any
// tracked tx registered AFTER the original build gets marked MINED on
// redelivery, then ack and return.
func TestBuilder_HandleMessage_ShortCircuit_BUMPAlreadyExists(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash

	// Construct a valid stored BUMP for the block by running BuildCompoundBUMP
	// on a minimal STUMP. We stash the BUMP directly in the mock and then
	// invoke handleMessage — there are NO STUMP rows, NO datahub server, and
	// the test still must succeed via the short-circuit path.
	stumpData := makeMinimalSTUMP(testTxidHex)
	subtreeHash := mustHash(t, testTxidHex)
	compound, _, err := bump.BuildCompoundBUMP(
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil,
	)
	if err != nil {
		t.Fatalf("BuildCompoundBUMP: %v", err)
	}
	ms.bumps[blockHash] = compound.Bytes()
	ms.bumpHeights[blockHash] = uint64(compound.BlockHeight)

	// No datahub server wired — if the short-circuit fails, the test fails
	// with a connection error or panic, surfacing the regression.
	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
		// teranode left nil so the rebuild path would crash on use,
		// proving the short-circuit must run.
	}

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("short-circuit handleMessage returned error: %v", err)
	}

	// SetMinedByTxIDs should have been called once with the level-0 hash list
	// from the stored BUMP.
	if len(ms.minedCalls) != 1 {
		t.Fatalf("expected 1 SetMinedByTxIDs call, got %d", len(ms.minedCalls))
	}
	got := ms.minedCalls[0]
	if got.blockHash != blockHash {
		t.Errorf("mined blockHash=%q want %q", got.blockHash, blockHash)
	}
	if len(got.txids) != 1 || got.txids[0] != testTxidHex {
		t.Errorf("mined txids=%v want [%q]", got.txids, testTxidHex)
	}

	// The short-circuit must stamp processed_at: the typical trigger is the
	// watchdog re-firing BLOCK_PROCESSED for a built-but-unstamped block, and
	// without this stamp the block would loop in the watchdog forever.
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("short-circuit must stamp MarkBlockProcessed(%s), got %+v", blockHash, ms.processedCalls)
	}
}

// TestBuilder_PruneOrphanStumps_DeletesAfterSuccess verifies the startup
// janitor: STUMPs left behind for blocks whose BUMP already built (i.e.
// BUMPBuiltAt is set) get cleaned up on boot. Blocks still in flight
// (BUMPBuiltAt == nil) keep their STUMPs.
func TestBuilder_PruneOrphanStumps_DeletesAfterSuccess(t *testing.T) {
	ms := newMockStore()
	ms.tipHeight = 1000
	built := time.Now()
	ms.blockProc = []*models.BlockProcessingStatus{
		{BlockHash: "done-1", BlockHeight: 999, BUMPBuiltAt: &built}, // BUMP built — prune
		{BlockHash: "done-2", BlockHeight: 998, BUMPBuiltAt: &built}, // BUMP built — prune
		{BlockHash: "in-flight", BlockHeight: 997, BUMPBuiltAt: nil}, // still pending — keep
	}
	// Seed stumps for all three so we can verify the in-flight one survives.
	ms.addStump("done-1", 0, []byte("orphan-1"))
	ms.addStump("done-2", 0, []byte("orphan-2"))
	ms.addStump("in-flight", 0, []byte("pending"))

	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}
	// Set recency depth manually since cfg is bare in this test.
	b.cfg.Watchdog.RecencyDepth = 144

	b.pruneOrphanStumps(context.Background())

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, exists := ms.stumps["done-1"]; exists && len(ms.stumps["done-1"]) > 0 {
		t.Errorf("done-1 STUMPs should have been pruned, still have %d", len(ms.stumps["done-1"]))
	}
	if _, exists := ms.stumps["done-2"]; exists && len(ms.stumps["done-2"]) > 0 {
		t.Errorf("done-2 STUMPs should have been pruned, still have %d", len(ms.stumps["done-2"]))
	}
	if len(ms.stumps["in-flight"]) != 1 {
		t.Errorf("in-flight STUMPs should have been preserved, got %d", len(ms.stumps["in-flight"]))
	}
}

// TestBuilder_PruneOrphanStumps_EmptyStore exits early when there's no tip.
func TestBuilder_PruneOrphanStumps_EmptyStore(_ *testing.T) {
	ms := newMockStore() // tipHeight = 0
	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}
	// Should not panic, should not call anything that would.
	b.pruneOrphanStumps(context.Background())
}

func TestBuilder_HandleMessage_HappyPath_SingleSubtree(t *testing.T) {
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

	b := newTestBuilder(ms, datahub.URL)

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify BUMP was stored
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored")
	}

	// Verify STUMPs were pruned
	if len(ms.deletedBlocks) != 1 || ms.deletedBlocks[0] != blockHash {
		t.Errorf("expected STUMPs for %s to be deleted, got: %v", blockHash, ms.deletedBlocks)
	}

	// Verify the block-processing observability row was updated.
	if len(ms.bumpBuiltCalls) != 1 || ms.bumpBuiltCalls[0].blockHash != blockHash {
		t.Errorf("expected MarkBlockBUMPBuilt(%s), got %+v", blockHash, ms.bumpBuiltCalls)
	}

	// Finalization: bump-builder (not the HTTP handler) now owns the
	// processed_at stamp, and must set it on a fully-built block.
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("expected MarkBlockProcessed(%s) on a finalized block, got %+v", blockHash, ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_ProcessedStampSoftFails_SelfHealsOnRedelivery
// verifies the recovery contract for a transient store error while stamping
// processed_at on a successful build: the build still completes (BUMP stored,
// txs MINED) but processed_at stays NULL, so the block remains in the watchdog's
// stale scan. When the watchdog re-drives BLOCK_PROCESSED, the short-circuit
// finds the existing BUMP and re-stamps processed_at — the block self-heals
// rather than wedging.
func TestBuilder_HandleMessage_ProcessedStampSoftFails_SelfHealsOnRedelivery(t *testing.T) {
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

	b := newTestBuilder(ms, datahub.URL)

	// First delivery: the processed_at stamp fails transiently.
	ms.markProcessedErr = errors.New("store down")
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0})); err != nil {
		t.Fatalf("a soft processed_at failure must not fail the build, got: %v", err)
	}
	ms.mu.Lock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("BUMP should be stored even though the processed_at stamp failed")
	}
	if len(ms.processedCalls) != 0 {
		t.Errorf("processed_at must remain unstamped after a soft failure, got %+v", ms.processedCalls)
	}
	ms.mu.Unlock()

	// Watchdog re-drives BLOCK_PROCESSED; the store has recovered. The
	// short-circuit must find the existing BUMP and re-stamp processed_at.
	ms.markProcessedErr = nil
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0})); err != nil {
		t.Fatalf("redelivery short-circuit returned error: %v", err)
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("short-circuit must re-stamp MarkBlockProcessed(%s) on redelivery, got %+v", blockHash, ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_IncompleteExpectedSet_DefersFinalization is the
// core of the silent-loss fix: merkle says subtrees {0,1,2} should each have a
// STUMP, but only {0,2} arrived. The block must NOT be finalized — no BUMP, no
// processed_at stamp — so the watchdog can recover it via /reprocess. Returns
// nil (not an error) so Kafka doesn't replay against the same gap.
func TestBuilder_HandleMessage_IncompleteExpectedSet_DefersFinalization(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash

	// Store STUMPs for subtrees 0 and 2 only; merkle expected 0, 1, 2.
	ms.addStump(blockHash, 0, makeMinimalSTUMP(testTxidHex))
	ms.addStump(blockHash, 2, makeMinimalSTUMP(testTxidHex))

	// No datahub server, teranode nil: if the builder wrongly proceeds to build,
	// it crashes on the fetch path — which would itself fail the test.
	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0, 1, 2})); err != nil {
		t.Fatalf("incomplete-set handleMessage must return nil (watchdog recovers), got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.bumps) != 0 {
		t.Errorf("no BUMP should be built for an incomplete block, got %d", len(ms.bumps))
	}
	if len(ms.processedCalls) != 0 {
		t.Errorf("processed_at must NOT be stamped for an incomplete block (watchdog recovery depends on it), got %+v", ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_IndexMismatchSameCount_Incomplete proves the gate
// compares set membership, not counts: merkle expected {0,2}, but the stored
// STUMPs are {0,3} — same count, yet subtree 2 is genuinely missing (and the
// extra 3 must not mask it).
func TestBuilder_HandleMessage_IndexMismatchSameCount_Incomplete(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash

	ms.addStump(blockHash, 0, makeMinimalSTUMP(testTxidHex))
	ms.addStump(blockHash, 3, makeMinimalSTUMP(testTxidHex))

	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0, 2})); err != nil {
		t.Fatalf("count-equal index-mismatch must return nil, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.bumps) != 0 || len(ms.processedCalls) != 0 {
		t.Errorf("count-equal but index-mismatched set must be treated as incomplete; bumps=%d processed=%d", len(ms.bumps), len(ms.processedCalls))
	}
}

// TestBuilder_HandleMessage_CompleteExpectedSet_Finalizes is the happy path with
// the expected set fully satisfied: merkle expected {0}, subtree 0's STUMP is
// present, so the BUMP builds and processed_at is stamped.
func TestBuilder_HandleMessage_CompleteExpectedSet_Finalizes(t *testing.T) {
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

	b := newTestBuilder(ms, datahub.URL)

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsgWithExpected(blockHash, []int{0})); err != nil {
		t.Fatalf("complete-set handleMessage returned error: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored for a complete block")
	}
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("expected MarkBlockProcessed(%s) on a complete block, got %+v", blockHash, ms.processedCalls)
	}
}

// TestBuilder_HandleMessage_EmptyExpectedSet_ZeroStumps_Finalizes covers the
// pre-#162 merkle case and the legitimate no-tracked-tx block: an empty/absent
// expected set with zero stored STUMPs must finalize (stamp processed_at) so the
// watchdog doesn't endlessly re-drive a complete block.
func TestBuilder_HandleMessage_EmptyExpectedSet_ZeroStumps_Finalizes(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash

	b := &Builder{
		cfg:    &config.Config{},
		logger: zap.NewNop().Named("bump-builder"),
		store:  ms,
	}

	// No expected set, no STUMPs — exactly today's behavior, plus the stamp.
	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("empty-set zero-stump handleMessage returned error: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.bumps) != 0 {
		t.Errorf("no BUMP should be built with zero STUMPs, got %d", len(ms.bumps))
	}
	if len(ms.processedCalls) != 1 || ms.processedCalls[0].blockHash != blockHash {
		t.Errorf("a no-tracked-tx block IS finalized — expected MarkBlockProcessed(%s), got %+v", blockHash, ms.processedCalls)
	}
}

func TestBuilder_HandleMessage_BumpBuiltStoreError_StillSucceeds(t *testing.T) {
	ms := newMockStore()
	ms.markBumpBuiltErr = errors.New("store down")
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

	b := newTestBuilder(ms, datahub.URL)

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("MarkBlockBUMPBuilt error must not fail handler, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("BUMP should still be stored despite the observability write failure")
	}
}

func TestBuilder_HandleMessage_EmptyBlockHash_ReturnsError(t *testing.T) {
	ms := newMockStore()
	datahub := newDatahubServer(zeroMerkleRoot(), nil)
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(""))
	if err == nil {
		t.Fatal("expected error for empty block hash, got nil")
	}
}

func TestBuilder_HandleMessage_InvalidJSON_ReturnsError(t *testing.T) {
	ms := newMockStore()
	datahub := newDatahubServer(zeroMerkleRoot(), nil)
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	msg := &kafka.Message{
		Topic: "arcade.block_processed",
		Value: []byte("not json"),
	}
	err := b.handleMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

// TestBuilder_LateSTUMP_ArrivesDuringGraceWindow simulates a late-retry STUMP that
// lands after BLOCK_PROCESSED but within the grace window. The builder should wait
// the grace window, then find the STUMP and build the BUMP successfully.
func TestBuilder_LateSTUMP_ArrivesDuringGraceWindow(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	// Compute what the late-arriving STUMP will produce so the datahub header
	// merkle root set up ahead of time matches the compound the builder builds
	// after the grace window.
	lateStump := makeMinimalSTUMP(txidHex)
	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: lateStump}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)
	b.cfg.BumpBuilder.GraceWindowMs = 100 // short grace for the test

	// Insert STUMP mid-grace-window from another goroutine
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = ms.InsertStump(context.Background(), &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: 0,
			StumpData:    lateStump,
		})
	}()

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored after late STUMP landed in grace window")
	}
}

// TestBuilder_E2E_InsertStump_GetStumps_BuildBUMP verifies the full STUMP→BUMP workflow:
// store STUMPs via InsertStump → query via GetStumpsByBlockHash → build BUMP.
// Uses 2-leaf STUMPs so each subtree has internalHeight=1 matching subtreeSize=2,
// which is the geometric invariant BuildCompoundBUMP's shift math assumes.
func TestBuilder_E2E_InsertStump_GetStumps_BuildBUMP(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txid1 := testTxidHex
	txid2 := "3333333333333333333333333333333333333333333333333333333333333333"
	sib1 := "5555555555555555555555555555555555555555555555555555555555555555"
	sib2 := "7777777777777777777777777777777777777777777777777777777777777777"

	// Store STUMPs via the store interface (mimicking stump consumer)
	stumpData := [][]byte{
		makeTwoLeafSTUMP(txid1, sib1),
		makeTwoLeafSTUMP(txid2, sib2),
	}
	for i, data := range stumpData {
		stump := &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: i,
			StumpData:    data,
		}
		if err := ms.InsertStump(context.Background(), stump); err != nil {
			t.Fatalf("InsertStump %d failed: %v", i, err)
		}
	}

	// Verify STUMPs are retrievable
	stumps, err := ms.GetStumpsByBlockHash(context.Background(), blockHash)
	if err != nil {
		t.Fatalf("GetStumpsByBlockHash failed: %v", err)
	}
	if len(stumps) != 2 {
		t.Fatalf("expected 2 STUMPs, got %d", len(stumps))
	}

	// subtreeHashes[i] must match the root that each STUMP's leaves produce,
	// otherwise the merged compound will not verify against any header root.
	subtreeHashes := []chainhash.Hash{
		subtreeRootFromTwoLeafSTUMP(t, txid1, sib1),
		subtreeRootFromTwoLeafSTUMP(t, txid2, sib2),
	}
	root := expectedCompoundRoot(t, stumps, subtreeHashes, nil)
	datahub := newDatahubServer(root, subtreeHashes)
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage failed: %v", err)
	}

	// Verify BUMP was stored
	ms.mu.Lock()
	defer ms.mu.Unlock()
	bumpData, ok := ms.bumps[blockHash]
	if !ok {
		t.Fatal("expected BUMP to be stored")
	}
	if len(bumpData) == 0 {
		t.Error("expected non-empty BUMP data")
	}

	// Verify STUMPs were cleaned up
	if len(ms.deletedBlocks) != 1 || ms.deletedBlocks[0] != blockHash {
		t.Errorf("expected STUMPs for %s to be deleted, got: %v", blockHash, ms.deletedBlocks)
	}
}

// TestBuilder_HandleMessage_PassesAllLevelZeroHashesToSetMined locks in the
// post-fix contract: bump-builder hands every level-0 hash from the compound
// BUMP to SetMinedByTxIDs and lets the store filter by row existence.
//
// Prior behavior pre-filtered against an in-memory TxTracker before calling
// the store. In microservice deployments the bump-builder pod's tracker is
// hydrated once at boot and never refreshed, so any tx submitted after that
// (to a separate api-server pod) was silently dropped from MINED fan-out —
// even when its row existed in postgres at SEEN_* and its leaf was in the
// stored compound BUMP. The fix removes the pre-filter; this test fails if
// anyone reintroduces it.
//
// Uses a two-leaf STUMP so the level-0 list has more than one entry, and
// asserts both leaf hex strings appear in the SetMinedByTxIDs call.
func TestBuilder_HandleMessage_PassesAllLevelZeroHashesToSetMined(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex
	siblingHex := "2222222222222222222222222222222222222222222222222222222222222222"

	stumpData := makeTwoLeafSTUMP(txidHex, siblingHex)
	ms.addStump(blockHash, 0, stumpData)

	subtreeHashes := []chainhash.Hash{
		subtreeRootFromTwoLeafSTUMP(t, txidHex, siblingHex),
	}
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		subtreeHashes, nil)
	datahub := newDatahubServer(root, subtreeHashes)
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if len(ms.minedCalls) != 1 {
		t.Fatalf("expected 1 SetMinedByTxIDs call, got %d", len(ms.minedCalls))
	}
	got := ms.minedCalls[0].txids
	saw := make(map[string]bool, len(got))
	for _, s := range got {
		saw[s] = true
	}
	for _, want := range []string{txidHex, siblingHex} {
		if !saw[want] {
			t.Errorf("SetMinedByTxIDs missing leaf %q; got %v", want, got)
		}
	}
}

func TestBuilder_E2E_BUMPExtractionWithRealisticSTUMP(t *testing.T) {
	// This test uses realistic STUMPs (built via go-sdk types) to verify
	// the full flow: store STUMPs → build BUMP → extract per-tx path.
	// The bump/bump_test.go suite covers ExtractMinimalPathForTx exhaustively;
	// this test verifies the builder stores data that extraction can consume.
	ms := newMockStore()
	blockHash := testBlockHash

	// Build a realistic STUMP with 2 leaves using go-sdk types
	txHash1Bytes, _ := hex.DecodeString(testTxidHex)
	txHash2Bytes, _ := hex.DecodeString("2222222222222222222222222222222222222222222222222222222222222222")
	h1, _ := chainhash.NewHash(txHash1Bytes)
	h2, _ := chainhash.NewHash(txHash2Bytes)

	isTxid := true
	isNotTxid := false
	stumpMP := transaction.NewMerklePath(1, [][]*transaction.PathElement{
		{
			{Offset: 0, Hash: h1, Txid: &isTxid},
			{Offset: 1, Hash: h2, Txid: &isNotTxid},
		},
	})
	stumpData := stumpMP.Bytes()
	ms.addStump(blockHash, 0, stumpData)

	// Single-subtree, two-leaf STUMP: compound root == MerkleTreeParent(h1, h2).
	subtreeHash := transaction.MerkleTreeParent(h1, h2)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{*subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{*subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage failed: %v", err)
	}

	ms.mu.Lock()
	bumpData, ok := ms.bumps[blockHash]
	ms.mu.Unlock()
	if !ok || len(bumpData) == 0 {
		t.Fatal("expected non-empty BUMP data to be stored")
	}

	// Extract minimal path for the tracked txid
	result := bump.ExtractMinimalPathForTx(bumpData, h1.String())
	if result == nil {
		t.Fatal("ExtractMinimalPathForTx returned nil for tracked txid")
	}
	if len(result) == 0 {
		t.Fatal("ExtractMinimalPathForTx returned empty bytes")
	}
}

// TestBuilder_HandleMessage_RootMismatch_SkipsPersistence asserts that a
// compound BUMP whose computed root differs from the block-header merkle root
// causes handleMessage to short-circuit: the BUMP is not stored, txs are not
// marked mined, and STUMPs are not pruned — so the next retry (or a follow-up
// fix) can rebuild from the same inputs.
func TestBuilder_HandleMessage_RootMismatch_SkipsPersistence(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)

	// Serve a header merkle root that deliberately does NOT match what the
	// compound will produce (all 0xaa bytes vs the real txid-derived root).
	wrongRoot := make([]byte, 32)
	for i := range wrongRoot {
		wrongRoot[i] = 0xaa
	}
	subtreeHash := mustHash(t, txidHex)
	datahub := newDatahubServer(wrongRoot, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash))
	if err == nil {
		t.Fatal("expected error on compound root mismatch, got nil")
	}
	if !containsStr(err.Error(), "compound BUMP root mismatch") {
		t.Errorf("expected 'compound BUMP root mismatch' in error, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; ok {
		t.Error("expected no BUMP to be stored on validation failure")
	}
	if len(ms.minedCalls) != 0 {
		t.Errorf("expected SetMinedByTxIDs not to be called on validation failure, got %d calls", len(ms.minedCalls))
	}
	if len(ms.deletedBlocks) != 0 {
		t.Errorf("expected STUMPs not to be pruned on validation failure, got deletes: %v", ms.deletedBlocks)
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// --- Publisher mock + block-height regression tests for issue #87 / F-029 ---

// recordingPublisher captures every TransactionStatus the builder publishes
// downstream so tests can assert on what SSE / webhook subscribers would see.
type recordingPublisher struct {
	mu        sync.Mutex
	published []*models.TransactionStatus
}

func (p *recordingPublisher) Publish(_ context.Context, status *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Copy so later mutation by the builder (or the store) cannot retroactively
	// repair a height we want to assert was missing at publish time.
	cp := *status
	p.published = append(p.published, &cp)
	return nil
}

// PublishBulk fans the template into one published entry per TxID so tests
// that drive the bump-builder MINED path can assert per-tx behavior with
// the existing snapshot() helper, unchanged.
func (p *recordingPublisher) PublishBulk(_ context.Context, template *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, txid := range template.TxIDs {
		cp := *template
		cp.TxID = txid
		cp.TxIDs = nil
		p.published = append(p.published, &cp)
	}
	return nil
}

func (p *recordingPublisher) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return nil, errors.New("recordingPublisher: Subscribe not used in tests")
}

func (p *recordingPublisher) Close() error { return nil }

func (p *recordingPublisher) snapshot() []*models.TransactionStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*models.TransactionStatus, len(p.published))
	copy(out, p.published)
	return out
}

// makeMinimalSTUMPAtHeight builds a single-leaf STUMP for txidHex at the given
// block height. Uses go-sdk's MerklePath.Bytes() so the height is encoded as a
// proper BRC-74 varint (the hand-rolled makeMinimalSTUMP only supports heights
// < 0xfd because it writes a single raw byte).
func makeMinimalSTUMPAtHeight(t *testing.T, txidHex string, blockHeight uint32) []byte {
	t.Helper()
	txHash := mustHash(t, txidHex)
	isTxid := true
	mp := transaction.NewMerklePath(blockHeight, [][]*transaction.PathElement{
		{{Offset: 0, Hash: &txHash, Txid: &isTxid}},
	})
	return mp.Bytes()
}

// TestBuilder_MarkMinedAndPublish_UsesBulkEvent covers the coalesced fan-out:
// markMinedAndPublish must call publisher.PublishBulk exactly once for the
// whole block (instead of one Publish per tx), and the bulk event must
// carry every mined txid in TxIDs[]. Without this, a 14k-tx block produced
// 14k publish events and overran the webhook service's 1024-cap work queue
// (~185k drops observed in production).
func TestBuilder_MarkMinedAndPublish_UsesBulkEvent(t *testing.T) {
	ms := newMockStore()
	pub := &recordingPublisher{}

	blockHash := testBlockHash
	const blockHeight uint64 = 18700

	// Three mined txids — enough to prove the bulk packing, small enough to
	// keep the test trivial. The mock store accepts any txids; the actual
	// MINED transition cascade is exercised by other tests.
	txids := []string{
		"1111111111111111111111111111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222222222222222222222222222",
		"3333333333333333333333333333333333333333333333333333333333333333",
	}

	b := newTestBuilder(ms, "http://unused.example/api/v1")
	b.publisher = pub

	b.markMinedAndPublish(context.Background(), zap.NewNop(), blockHash, blockHeight, txids)

	// recordingPublisher.PublishBulk unfans into one entry per TxID for
	// assertion convenience, so snapshot() shows N entries — but the
	// publisher saw exactly one PublishBulk call. Each entry must carry the
	// shared template fields and TxIDs must be cleared on the unfanned copy.
	emitted := pub.snapshot()
	if len(emitted) != len(txids) {
		t.Fatalf("expected %d unfanned entries from one bulk event, got %d", len(txids), len(emitted))
	}
	for i, st := range emitted {
		if st.TxID != txids[i] {
			t.Errorf("entry[%d].TxID = %q, want %q", i, st.TxID, txids[i])
		}
		if st.Status != models.StatusMined {
			t.Errorf("entry[%d].Status = %q, want MINED", i, st.Status)
		}
		if st.BlockHash != blockHash {
			t.Errorf("entry[%d].BlockHash = %q, want %q", i, st.BlockHash, blockHash)
		}
		if st.BlockHeight != blockHeight {
			t.Errorf("entry[%d].BlockHeight = %d, want %d", i, st.BlockHeight, blockHeight)
		}
		if len(st.TxIDs) != 0 {
			t.Errorf("entry[%d] still carries TxIDs after unfan: %v", i, st.TxIDs)
		}
	}
}

// TestBuilder_HandleMessage_PublishesMinedStatusWithBlockHeight is the
// regression test for issue #87 / F-029: the builder must thread the
// compound BUMP's block height all the way through SetMinedByTxIDs and
// onto the TransactionStatus that gets published. A previous code path
// dropped the height before publish, leaving downstream SSE/webhook
// consumers with BlockHash but BlockHeight=0.
func TestBuilder_HandleMessage_PublishesMinedStatusWithBlockHeight(t *testing.T) {
	const wantHeight uint32 = 850123
	ms := newMockStore()
	pub := &recordingPublisher{}

	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMPAtHeight(t, txidHex, wantHeight)
	ms.addStump(blockHash, 0, stumpData)

	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)
	b.publisher = pub

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	// 1) The store call itself must receive the height — without this, no
	//    backend can persist it even if the publish path were correct.
	ms.mu.Lock()
	if len(ms.minedCalls) != 1 {
		ms.mu.Unlock()
		t.Fatalf("expected 1 SetMinedByTxIDs call, got %d", len(ms.minedCalls))
	}
	got := ms.minedCalls[0]
	ms.mu.Unlock()
	if got.blockHeight != uint64(wantHeight) {
		t.Errorf("SetMinedByTxIDs got blockHeight=%d, want %d", got.blockHeight, wantHeight)
	}
	if got.blockHash != blockHash {
		t.Errorf("SetMinedByTxIDs got blockHash=%q, want %q", got.blockHash, blockHash)
	}

	// 2) The published TransactionStatus must carry both fields. This is the
	//    contract SSE / webhook subscribers see; F-029 was that BlockHeight
	//    was zero here even though BlockHash was set.
	emitted := pub.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 published status, got %d", len(emitted))
	}
	st := emitted[0]
	if st.Status != models.StatusMined {
		t.Errorf("published status = %q, want MINED", st.Status)
	}
	if st.BlockHash != blockHash {
		t.Errorf("published BlockHash=%q, want %q", st.BlockHash, blockHash)
	}
	if st.BlockHeight != uint64(wantHeight) {
		t.Errorf("published BlockHeight=%d, want %d", st.BlockHeight, wantHeight)
	}
}

// TestBuilder_HandleMessage_PublishedHeightIsNeverZero is the narrow
// regression guard: anyone refactoring SetMinedByTxIDs or the publish
// loop and zero-valuing BlockHeight will fail this test even if every
// other assertion happens to pass (e.g. a future test that compares
// published statuses to mock-returned statuses without checking height).
func TestBuilder_HandleMessage_PublishedHeightIsNeverZero(t *testing.T) {
	ms := newMockStore()
	pub := &recordingPublisher{}

	blockHash := testBlockHash
	txidHex := testTxidHex

	// Use the existing 0x01 minimal STUMP — it encodes blockHeight=1, which is
	// non-zero, so any code path that zero-values the height will be caught.
	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)

	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)
	datahub := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	b := newTestBuilder(ms, datahub.URL)
	b.publisher = pub

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	emitted := pub.snapshot()
	if len(emitted) == 0 {
		t.Fatal("expected at least one published status")
	}
	for i, st := range emitted {
		if st.BlockHeight == 0 {
			t.Errorf("published status %d has BlockHeight=0; F-029 regression: %+v", i, st)
		}
	}
}

// TestBuilder_HandleMessage_DefensivelyRestoresHeightIfStoreDropsIt
// asserts the safety net the builder added on top of SetMinedByTxIDs:
// if a backend regresses and forgets to populate BlockHeight on its
// returned status, the publish path repairs it from the compound BUMP's
// height before fanning out. This guards against a partial revert that
// undoes only the store change.
func TestBuilder_HandleMessage_DefensivelyRestoresHeightIfStoreDropsIt(t *testing.T) {
	ms := newMockStore()
	pub := &recordingPublisher{}

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

	b := newTestBuilder(ms, datahub.URL)
	b.publisher = pub

	// Wrap the mock so SetMinedByTxIDs returns BlockHeight=0 — simulating a
	// backend that regresses on the persistence side. The builder must still
	// publish a non-zero height by falling back to the compound's height.
	heightDroppingStore := &heightDroppingMockStore{mockStore: ms}
	b.store = heightDroppingStore

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}

	emitted := pub.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 published status, got %d", len(emitted))
	}
	if emitted[0].BlockHeight == 0 {
		t.Errorf("publish path failed to defensively restore BlockHeight: %+v", emitted[0])
	}
}

// heightDroppingMockStore wraps mockStore and zeroes BlockHeight on every
// returned status to simulate a buggy backend.
type heightDroppingMockStore struct {
	*mockStore
}

func (h *heightDroppingMockStore) SetMinedByTxIDs(ctx context.Context, blockHash string, blockHeight uint64, txids []string) ([]*models.TransactionStatus, []*models.TransactionStatus, error) {
	prevs, statuses, err := h.mockStore.SetMinedByTxIDs(ctx, blockHash, blockHeight, txids)
	for _, s := range statuses {
		s.BlockHeight = 0
	}
	return prevs, statuses, err
}

// stubChaintracks is a minimal ChainHeaderReader for tests. nil header
// returns simulate "chaintracks doesn't know this block yet" (soft fail).
// Non-nil headers simulate the canonical chain.
type stubChaintracks struct {
	headers map[string]*chaintrackslib.BlockHeader
	err     error
}

func (s *stubChaintracks) GetHeaderByHash(_ context.Context, h *chainhash.Hash) (*chaintrackslib.BlockHeader, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.headers[h.String()], nil
}

// TestBuilder_HandleMessage_PrunedPeerFallthrough simulates the production
// failure mode for block 18603: the first datahub returns 200 with
// subtree_count=1 while the block actually has more subtrees. Fix 1 must
// reject that response and fall through to a healthy peer, so the BUMP
// builds successfully instead of looping the consumer retry.
func TestBuilder_HandleMessage_PrunedPeerFallthrough(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)

	subtreeHash := mustHash(t, txidHex)
	root := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)

	// Pruned peer: returns 200 with a *different* subtree list of length 0.
	// Since min_subtrees is computed from STUMP indexes (max 0 → min=1) and
	// the pruned response has 0 subtrees, the validator rejects it.
	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var buf bytes.Buffer
		buf.Write(buildBlockHeader(make([]byte, 32)))
		buf.WriteByte(0x01) // txCount
		buf.WriteByte(0x00) // sizeBytes
		buf.WriteByte(0x00) // subtreeCount=0
		coinbaseTx := transaction.NewTransaction()
		buf.Write(coinbaseTx.Bytes())
		buf.WriteByte(0x01) // blockHeight
		buf.WriteByte(0x00) // coinbaseBUMPLen
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(buf.Bytes())
	}))
	defer pruned.Close()

	healthy := newDatahubServer(root, []chainhash.Hash{subtreeHash})
	defer healthy.Close()

	cfg := &config.Config{DatahubURLs: []string{pruned.URL, healthy.URL}}
	tc := teranode.NewClient([]string{pruned.URL, healthy.URL}, "", teranode.HealthConfig{})
	b := &Builder{
		cfg:      cfg,
		logger:   zap.NewNop().Named("bump-builder"),
		store:    ms,
		teranode: tc,
	}

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("expected fall-through to healthy peer, got: %v", err)
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored after fall-through to healthy peer")
	}
}

// TestBuilder_HandleMessage_ChaintracksRootMismatch exercises Fix 2: a
// datahub returns a self-consistent block whose header merkle root does
// not match chaintracks's canonical merkle root for the same hash. The
// validator must reject the response and the build must fail (no healthy
// peer to fall through to in this test setup).
func TestBuilder_HandleMessage_ChaintracksRootMismatch(t *testing.T) {
	ms := newMockStore()
	blockHash := testBlockHash
	txidHex := testTxidHex

	stumpData := makeMinimalSTUMP(txidHex)
	ms.addStump(blockHash, 0, stumpData)

	subtreeHash := mustHash(t, txidHex)
	datahubRoot := expectedCompoundRoot(t,
		[]*models.Stump{{BlockHash: blockHash, SubtreeIndex: 0, StumpData: stumpData}},
		[]chainhash.Hash{subtreeHash}, nil)

	datahub := newDatahubServer(datahubRoot, []chainhash.Hash{subtreeHash})
	defer datahub.Close()

	// Canonical merkle root differs from what the datahub serves.
	canonicalRoot := chainhash.Hash{}
	for i := range canonicalRoot {
		canonicalRoot[i] = 0xFF
	}
	hashObj, _ := chainhash.NewHashFromHex(blockHash)
	canonicalHeader := &chaintrackslib.BlockHeader{Header: &block.Header{MerkleRoot: canonicalRoot}}
	stub := &stubChaintracks{headers: map[string]*chaintrackslib.BlockHeader{
		hashObj.String(): canonicalHeader,
	}}

	cfg := &config.Config{DatahubURLs: []string{datahub.URL}}
	tc := teranode.NewClient([]string{datahub.URL}, "", teranode.HealthConfig{})
	b := &Builder{
		cfg:         cfg,
		logger:      zap.NewNop().Named("bump-builder"),
		store:       ms,
		teranode:    tc,
		chainHeader: stub,
	}

	err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash))
	if err == nil {
		t.Fatal("expected error when datahub root disagrees with chaintracks")
	}
	if !strings.Contains(err.Error(), "merkle_root mismatch") && !strings.Contains(err.Error(), "validator rejected") {
		t.Errorf("expected error mentioning merkle_root mismatch, got: %v", err)
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; ok {
		t.Error("BUMP must NOT be stored when datahub disagrees with chaintracks")
	}
}

// TestBuilder_HandleMessage_ChaintracksUnknownHeader_SoftFails verifies the
// fall-back path: chaintracks doesn't know the block (returns nil header
// with no error). The validator must NOT reject the response — the
// post-build ValidateCompoundRoot is still the final safety net.
func TestBuilder_HandleMessage_ChaintracksUnknownHeader_SoftFails(t *testing.T) {
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

	// Empty header map → GetHeaderByHash returns (nil, nil), the soft-fail path.
	stub := &stubChaintracks{headers: map[string]*chaintrackslib.BlockHeader{}}

	cfg := &config.Config{DatahubURLs: []string{datahub.URL}}
	tc := teranode.NewClient([]string{datahub.URL}, "", teranode.HealthConfig{})
	b := &Builder{
		cfg:         cfg,
		logger:      zap.NewNop().Named("bump-builder"),
		store:       ms,
		teranode:    tc,
		chainHeader: stub,
	}

	if err := b.handleMessage(context.Background(), makeBlockProcessedMsg(blockHash)); err != nil {
		t.Fatalf("expected soft-fail to accept response, got: %v", err)
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("BUMP should still be stored when chaintracks doesn't know the header (soft-fail)")
	}
}
