package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/events"
	"github.com/bsv-blockchain/arcade/internal/synthblock"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store/pebble"
)

// These tests wire the SSE service against a REAL pebble store (not the stub)
// so merkle-path enrichment actually runs, and verify the proof in the emitted
// frame recomputes the block merkle root.

const (
	sseBlockHash = "000000000000000000000000000000000000000000000000000000000000cafe"
	sseHeight    = 880456
)

type merkleSSEHarness struct {
	store   *pebble.Store
	pub     *events.KafkaPublisher
	broker  kafka.Broker
	httpSrv *httptest.Server
	block   synthblock.Block
}

func (h *merkleSSEHarness) Close() {
	h.httpSrv.Close()
	_ = h.broker.Close()
}

// newMerkleSSEHarness builds a pebble store holding one mined block's compound
// BUMP, wires the SSE service over the in-memory broker, and registers a
// submission for block.Txids[idx] under the given token.
func newMerkleSSEHarness(t *testing.T, token string, subIdx int) *merkleSSEHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ctx := context.Background()

	st, err := pebble.New(config.Pebble{Path: t.TempDir(), MemTableSizeMB: 4, L0CompactionThreshold: 2})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	blk, err := synthblock.Build(8, sseHeight)
	if err != nil {
		t.Fatalf("synthblock.Build: %v", err)
	}
	for _, txid := range blk.Txids {
		if _, _, sErr := st.GetOrInsertStatus(ctx, &models.TransactionStatus{
			TxID: txid, Status: models.StatusSeenOnNetwork, Timestamp: time.Now().UTC(),
		}); sErr != nil {
			t.Fatalf("seed row: %v", sErr)
		}
	}
	if _, _, mErr := st.SetMinedByTxIDs(ctx, sseBlockHash, sseHeight, blk.Txids); mErr != nil {
		t.Fatalf("SetMinedByTxIDs: %v", mErr)
	}
	if bErr := st.InsertBUMP(ctx, sseBlockHash, sseHeight, blk.BumpBytes); bErr != nil {
		t.Fatalf("InsertBUMP: %v", bErr)
	}
	if subErr := st.InsertSubmission(ctx, &models.Submission{
		SubmissionID: "sub-" + token, TxID: blk.Txids[subIdx], CallbackToken: token,
	}); subErr != nil {
		t.Fatalf("InsertSubmission: %v", subErr)
	}

	broker := kafka.NewMemoryBroker(64)
	pub := events.NewKafkaPublisher(kafka.NewProducer(broker), zap.NewNop(), 0)

	svc := &Service{
		cfg:       &config.Config{SSE: config.SSEConfig{Enabled: true}},
		logger:    zap.NewNop(),
		store:     st,
		publisher: pub,
	}
	mgr, err := newManager(t.Context(), pub, st, zap.NewNop())
	if err != nil {
		_ = broker.Close()
		t.Fatalf("newManager: %v", err)
	}
	svc.manager = mgr

	router := gin.New()
	router.GET("/events", svc.handleEvents)

	return &merkleSSEHarness{store: st, pub: pub, broker: broker, httpSrv: httptest.NewServer(router), block: blk}
}

// frameData extracts and decodes the JSON object from a `data:` line of an SSE frame.
func frameData(t *testing.T, frame string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(frame, "\n") {
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			var m map[string]any
			if err := json.Unmarshal([]byte(data), &m); err != nil {
				t.Fatalf("decode SSE data %q: %v", data, err)
			}
			return m
		}
	}
	t.Fatalf("no data line in frame %q", frame)
	return nil
}

func assertMinedFrameCarriesProof(t *testing.T, frame, txid, root string) {
	t.Helper()
	m := frameData(t, frame)
	if m["txid"] != txid || m["txStatus"] != string(models.StatusMined) {
		t.Fatalf("unexpected frame for %s: %v", txid, m)
	}
	if m["blockHash"] != sseBlockHash {
		t.Fatalf("frame missing blockHash: %v", m["blockHash"])
	}
	if _, ok := m["blockHeight"]; !ok {
		t.Fatalf("frame missing blockHeight: %v", m)
	}
	mp, ok := m["merklePath"].(string)
	if !ok || mp == "" {
		t.Fatalf("frame missing merklePath: %v", m["merklePath"])
	}
	if err := synthblock.VerifyMerklePathHex(mp, txid, root); err != nil {
		t.Fatalf("SSE frame merklePath does not verify: %v", err)
	}
}

func TestSSE_E2E_MinedLiveFrameCarriesProof(t *testing.T) {
	const liveTok = "tok-sse-live"
	h := newMerkleSSEHarness(t, liveTok, 3)
	defer h.Close()
	waitForSubscriberReady()

	subTxid := h.block.Txids[3]

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+liveTok, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	time.Sleep(100 * time.Millisecond)

	// The bulk MINED event bump_builder publishes: block fields + all txids, no path.
	if pErr := h.pub.PublishBulk(t.Context(), &models.TransactionStatus{
		Status:      models.StatusMined,
		BlockHash:   sseBlockHash,
		BlockHeight: sseHeight,
		Timestamp:   time.Now().UTC(),
		TxIDs:       h.block.Txids,
	}); pErr != nil {
		t.Fatalf("PublishBulk: %v", pErr)
	}

	frame, err := readNextSSEFrame(resp.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	assertMinedFrameCarriesProof(t, frame, subTxid, h.block.Root)
}

func TestSSE_E2E_MinedCatchupFrameCarriesProof(t *testing.T) {
	const catchupTok = "tok-sse-catchup"
	h := newMerkleSSEHarness(t, catchupTok, 5)
	defer h.Close()
	waitForSubscriberReady()

	subTxid := h.block.Txids[5]

	// Reconnect with a Last-Event-ID well before the mined row's timestamp, so
	// the catchup replay emits the terminal MINED frame.
	since := time.Now().Add(-time.Hour)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		h.httpSrv.URL+"/events?callbackToken="+catchupTok, nil)
	req.Header.Set("Last-Event-ID", fmt.Sprintf("%d", since.UnixNano()))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	frame, err := readNextSSEFrame(resp.Body, 3*time.Second)
	if err != nil {
		t.Fatalf("read catchup frame: %v", err)
	}
	assertMinedFrameCarriesProof(t, frame, subTxid, h.block.Root)
}
