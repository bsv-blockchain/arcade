package bump_builder

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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

// --- Mock Store ---

type mockStore struct {
	store.Store // embed — panics on unimplemented methods

	mu              sync.Mutex
	stumps          map[string][]*models.Stump // blockHash → stumps
	bumps           map[string][]byte          // blockHash → bumpData
	minedCalls      []minedCall
	deletedBlocks   []string
	getStumpsErr    error
	insertBUMPErr   error
	insertStumpErr  error
	setMinedErr     error
	deleteStumpsErr error
}

type minedCall struct {
	blockHash string
	txids     []string
}

func newMockStore() *mockStore {
	return &mockStore{
		stumps: make(map[string][]*models.Stump),
		bumps:  make(map[string][]byte),
	}
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

func (m *mockStore) SetMinedByTxIDs(_ context.Context, blockHash string, txids []string) ([]*models.TransactionStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setMinedErr != nil {
		return nil, m.setMinedErr
	}
	m.minedCalls = append(m.minedCalls, minedCall{blockHash, txids})
	var statuses []*models.TransactionStatus
	for _, txid := range txids {
		statuses = append(statuses, &models.TransactionStatus{
			TxID:      txid,
			Status:    models.StatusMined,
			BlockHash: blockHash,
			Timestamp: time.Now(),
		})
	}
	return statuses, nil
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
	data, _ := json.Marshal(callback)
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
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		w.Write(buf.Bytes())
	}))
}

// newDatahubServerWithCoinbaseBUMP creates a test server that includes coinbase BUMP data.
func newDatahubServerWithCoinbaseBUMP(merkleRoot []byte, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer

		buf.Write(buildBlockHeader(merkleRoot))                   // header
		buf.WriteByte(0x01)                                       // txCount
		buf.WriteByte(0x00)                                       // sizeBytes
		buf.Write(util.VarInt(len(subtreeHashes)).Bytes()) // subtreeCount

		for i := range subtreeHashes {
			buf.Write(subtreeHashes[i].CloneBytes())
		}

		coinbaseTx := transaction.NewTransaction()
		buf.Write(coinbaseTx.Bytes())

		buf.WriteByte(0x01) // blockHeight

		// Coinbase BUMP length + data
		buf.Write(util.VarInt(len(coinbaseBUMP)).Bytes())
		buf.Write(coinbaseBUMP)

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(buf.Bytes())
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
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	buf := []byte{
		0x01, // blockHeight = 1
		0x01, // treeHeight = 1 (one level)
		0x01, // nLeaves at level 0 = 1
		0x00, // offset = 0
		0x02, // flags: bit 1 = txid (hash follows)
	}
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

	buf := []byte{
		0x01, // blockHeight = 1
		0x01, // treeHeight = 1
		0x02, // nLeaves at level 0 = 2
		0x00, // offset 0
		0x02, // flags: txid
	}
	buf = append(buf, txidBytes...)
	buf = append(buf,
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
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"
	stumpData := makeMinimalSTUMP("1111111111111111111111111111111111111111111111111111111111111111")
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
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"

	// Need a valid STUMP that produces a valid BUMP. Use a single-subtree scenario.
	txidHex := "1111111111111111111111111111111111111111111111111111111111111111"
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

func TestBuilder_HandleMessage_HappyPath_SingleSubtree(t *testing.T) {
	ms := newMockStore()
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"
	txidHex := "1111111111111111111111111111111111111111111111111111111111111111"

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
}

func TestBuilder_HandleMessage_HappyPath_WithTracker(t *testing.T) {
	ms := newMockStore()
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"
	txidHex := "1111111111111111111111111111111111111111111111111111111111111111"

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

	// Verify BUMP was stored and STUMPs pruned
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.bumps[blockHash]; !ok {
		t.Error("expected BUMP to be stored")
	}
	if len(ms.deletedBlocks) != 1 {
		t.Errorf("expected STUMPs to be pruned, got %d deletes", len(ms.deletedBlocks))
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
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"
	txidHex := "1111111111111111111111111111111111111111111111111111111111111111"

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
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"
	txid1 := "1111111111111111111111111111111111111111111111111111111111111111"
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

func TestBuilder_E2E_BUMPExtractionWithRealisticSTUMP(t *testing.T) {
	// This test uses realistic STUMPs (built via go-sdk types) to verify
	// the full flow: store STUMPs → build BUMP → extract per-tx path.
	// The bump/bump_test.go suite covers ExtractMinimalPathForTx exhaustively;
	// this test verifies the builder stores data that extraction can consume.
	ms := newMockStore()
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"

	// Build a realistic STUMP with 2 leaves using go-sdk types
	txHash1Bytes, _ := hex.DecodeString("1111111111111111111111111111111111111111111111111111111111111111")
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
	blockHash := "aabbccdd00000000000000000000000000000000000000000000000000000000"
	txidHex := "1111111111111111111111111111111111111111111111111111111111111111"

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
