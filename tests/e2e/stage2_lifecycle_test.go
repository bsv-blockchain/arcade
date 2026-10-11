//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teranode "github.com/bsv-blockchain/teranode/services/p2p"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/tests/e2e/harness"
)

// merkleServiceV045 is the linux/amd64 manifest digest of merkle-service
// v0.4.5. That release is the minimum that puts expectedSubtreeIndices on
// BLOCK_PROCESSED. The mutable tag is not the pin.
const merkleServiceV045 = "ghcr.io/bsv-blockchain/merkle-service@sha256:8393a456cac5f5bf24537b519d82a4d96fded0b43005fa0412a7545e43ecf168"

// TestStage2_IsolatedLifecycle drives one synthetic regtest transaction
// through the harness chain: in-process datahub, one libp2p announcer,
// merkle-service v0.4.5, and arcade --mode all on Pebble. Chaintracks
// dials only that announcer. No public bootstrap list is configured.
func TestStage2_IsolatedLifecycle(t *testing.T) {
	skipIfNoDocker(t)

	h := harness.New(t,
		harness.WithMerkleImage(merkleServiceV045),
		harness.WithReprocessReady(),
	)
	msDatahub := h.Datahub
	arcadeDatahub := h.NewDatahub(t)

	proxy := newCallbackProxy(t)
	rt := harness.StartArcade(t, harness.ArcadeOptions{
		MerkleServiceURL:  h.Containers.MerkleHostURL,
		DatahubURL:        arcadeDatahub.LocalURL(),
		LibP2PBootstrap:   h.LibP2P.BootstrapMultiaddr(),
		LibP2PLoopback:    h.LibP2P.LoopbackMultiaddr(),
		MerkleAuthToken:   "synthetic-stage2-merkle-auth-bearer",
		CallbackToken:     "synthetic-stage2-callback-bearer",
		CallbackURL:       proxy.callbackURL,
		EnableChaintracks: true,
		SyncWrites:        true,
	})
	proxy.setTarget(rt.BaseURL)
	assertNoPublicBootstrap(t, rt)

	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()

	releaseBroadcast := make(chan struct{})
	var broadcastOnce sync.Once
	broadcastEntered := make(chan struct{})
	arcadeDatahub.HoldTxPosts = func() {
		broadcastOnce.Do(func() { close(broadcastEntered) })
		select {
		case <-releaseBroadcast:
		case <-ctx.Done():
		}
	}

	txs := harness.BuildValidatableTxs(7, 1000)
	txid, err := harness.BroadcastTx(ctx, t, rt, txs[0])
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	txids := []string{txid}
	observe := newStatusObserver(rt, txid)
	observe.start(ctx)
	defer observe.stop()

	select {
	case <-broadcastEntered:
	case <-ctx.Done():
		t.Fatal("broadcast never reached the isolated datahub")
	}
	if err := harness.WaitForMerkleRegistration(ctx, h.Containers.MerkleHostURL, txid, 30*time.Second); err != nil {
		t.Fatal("Merkle /watch was not registered before broadcast returned")
	}
	callbackURLs := lookupCallbackURLs(t, ctx, h.Containers.MerkleHostURL, txid)
	if !containsCallbackPath(callbackURLs) {
		t.Fatalf("registered callback URLs = %v", callbackURLs)
	}
	close(releaseBroadcast)

	for _, tx := range txs[1:] {
		id, err := harness.BroadcastTx(ctx, t, rt, tx)
		if err != nil {
			t.Fatalf("broadcast: %v", err)
		}
		txids = append(txids, id)
	}
	for _, id := range txids {
		if err := harness.WaitForMerkleRegistration(ctx, h.Containers.MerkleHostURL, id, 60*time.Second); err != nil {
			t.Fatalf("watch %s: %v", id, err)
		}
	}

	proxy.holdSTUMP.Store(true)
	blk := publishSynthetic(t, ctx, h, rt, msDatahub, arcadeDatahub, txids, harness.RegtestGenesisHash(), 1)
	blockProcessed := proxy.waitType(ctx, "BLOCK_PROCESSED")
	if blockProcessed == nil {
		t.Fatal("BLOCK_PROCESSED was not observed")
	}
	indices, hasIndices := expectedIndices(blockProcessed)
	if !hasIndices || len(indices) == 0 {
		t.Fatalf("BLOCK_PROCESSED missing expectedSubtreeIndices: %s", blockProcessed)
	}
	if indices[0] != 0 {
		t.Fatalf("expectedSubtreeIndices = %v, want the tracked subtree", indices)
	}
	time.Sleep(2 * time.Second)
	if processedAt(t, ctx, rt, blk.Hash.String()) != "" {
		t.Fatal("processedAt was set while the required STUMP was withheld")
	}

	st, ok, err := harness.GetTxStatus(ctx, rt, txid)
	if err != nil || !ok {
		t.Fatalf("status before restart: ok=%v err=%v", ok, err)
	}
	if st.TxStatus == "MINED" || st.TxStatus == "IMMUTABLE" {
		t.Fatalf("status before restart = %s", st.TxStatus)
	}
	rt.Restart(t)
	proxy.setTarget(rt.BaseURL)
	st, ok, err = harness.GetTxStatus(ctx, rt, txid)
	if err != nil || !ok {
		t.Fatalf("status after restart: ok=%v err=%v", ok, err)
	}
	if st.TxStatus == "" || st.TxStatus == "MINED" {
		t.Fatalf("status after restart = %s", st.TxStatus)
	}
	if err := harness.WaitForMerkleRegistration(ctx, h.Containers.MerkleHostURL, txid, 15*time.Second); err != nil {
		t.Fatalf("watch missing after arcade restart: %v", err)
	}
	if processedAt(t, ctx, rt, blk.Hash.String()) != "" {
		t.Fatal("restart stamped processedAt without the withheld STUMP")
	}

	proxy.holdSTUMP.Store(false)
	proxy.flushHeld(t)
	if _, err := harness.TriggerReprocess(ctx, h.Containers.MerkleHostURL, blk.Hash.String(), proxy.callbackURL, "synthetic-stage2-callback-bearer"); err != nil {
		t.Fatalf("reprocess: %v", err)
	}
	if err := harness.WaitForMinedInBlock(ctx, t, rt, []string{txid}, blk.Hash.String(), 90*time.Second); err != nil {
		t.Fatalf("MINED: %v", err)
	}
	mined, _, err := harness.GetTxStatus(ctx, rt, txid)
	if err != nil {
		t.Fatalf("mined status: %v", err)
	}
	replayBlockProcessed(t, rt, blockProcessed)
	again, _, err := harness.GetTxStatus(ctx, rt, txid)
	if err != nil {
		t.Fatalf("status after duplicate callback: %v", err)
	}
	if again.TxStatus != "MINED" || again.BlockHash != mined.BlockHash {
		t.Fatalf("duplicate callback changed status to %s block %s", again.TxStatus, again.BlockHash)
	}

	empty, err := harness.BuildEmptySyntheticBlock(blk.Hash, 2, uint32(time.Now().Unix()), 1)
	if err != nil {
		t.Fatalf("empty block: %v", err)
	}
	empty.Stage(msDatahub)
	empty.Stage(arcadeDatahub)
	emptyMsg := empty.BlockMessage(msDatahub.HostURL())
	deadline := time.Now().Add(45 * time.Second)
	var emptyProcessed []byte
	for emptyProcessed == nil && time.Now().Before(deadline) {
		if err := h.LibP2P.PublishBlock(ctx, emptyMsg); err != nil {
			t.Fatalf("publish empty block: %v", err)
		}
		wait, cancelWait := context.WithTimeout(ctx, 2*time.Second)
		emptyProcessed = proxy.waitBlock(wait, empty.Hash.String())
		cancelWait()
	}
	if emptyProcessed == nil {
		t.Fatal("empty block BLOCK_PROCESSED was not observed")
	}
	if _, present := expectedIndices(emptyProcessed); present {
		t.Fatalf("empty block carried expectedSubtreeIndices: %s", emptyProcessed)
	}
	finalizeBy := time.Now().Add(20 * time.Second)
	var emptyProcessedAt string
	for time.Now().Before(finalizeBy) {
		emptyProcessedAt = processedAt(t, ctx, rt, empty.Hash.String())
		if emptyProcessedAt != "" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if emptyProcessedAt == "" {
		t.Fatal("empty block did not finalize")
	}

	observe.stop()
	evidence := map[string]any{
		"txid":                              txid,
		"block_hash":                        blk.Hash.String(),
		"callback_urls":                     callbackURLs,
		"expected_subtree_indices":          indices,
		"processed_at_while_stump_withheld": "",
		"empty_block_hash":                  empty.Hash.String(),
		"empty_block_processed_at":          emptyProcessedAt,
		"empty_block_has_expected_set":      false,
		"seen_multiple_nodes":               proxy.sawType("SEEN_MULTIPLE_NODES"),
		"status_observations":               observe.snapshot(),
		"external_bootstrap_connections":    0,
		"merkle_image":                      merkleServiceV045,
	}
	writeEvidence(t, evidence)
	t.Logf("STAGE2_TXID=%s", txid)
	t.Logf("STAGE2_BLOCK=%s", blk.Hash)
	t.Logf("STAGE2_EXPECTED_INDICES=%v", indices)
	t.Logf("STAGE2_STATUSES=%s", strings.Join(observe.statuses(), " "))
}

func assertNoPublicBootstrap(t *testing.T, rt *harness.ArcadeRuntime) {
	t.Helper()
	if rt.Cfg.Network != config.NetworkRegtest {
		t.Fatalf("network = %s", rt.Cfg.Network)
	}
	_, defaults := config.ResolveP2PNetwork(rt.Cfg.Network)
	if len(defaults) != 0 {
		t.Fatalf("regtest default bootstrap = %v", defaults)
	}
	for _, peer := range rt.Cfg.P2P.BootstrapPeers {
		if strings.Contains(peer, "bsvb.tech") || strings.Contains(peer, "dnsaddr") {
			t.Fatalf("public bootstrap peer configured: %s", peer)
		}
	}
	for _, peer := range rt.Cfg.Chaintracks.P2P.MsgBus.StaticPeers {
		if strings.Contains(peer, "bsvb.tech") || strings.Contains(peer, "dnsaddr") {
			t.Fatalf("chaintracks public peer configured: %s", peer)
		}
	}
	if rt.Cfg.Chaintracks.P2P.MsgBus.DHTMode != "off" {
		t.Fatalf("chaintracks dht mode = %s", rt.Cfg.Chaintracks.P2P.MsgBus.DHTMode)
	}
}

func publishSynthetic(t *testing.T, ctx context.Context, h *harness.Harness, rt *harness.ArcadeRuntime, msDatahub, arcadeDatahub *harness.Datahub, txids []string, prev chainhash.Hash, height uint32) *harness.SyntheticBlock {
	t.Helper()
	blk, err := harness.BuildSyntheticBlock(harness.SyntheticBlockSpec{
		PrevHash:  prev,
		Height:    height,
		Timestamp: uint32(time.Now().Unix()), //nolint:gosec // wall clock fits uint32 until 2106
		TxIDs:     harness.TxIDsFromHex(t, txids),
	})
	if err != nil {
		t.Fatalf("build block: %v", err)
	}
	blk.Stage(msDatahub)
	blk.Stage(arcadeDatahub)
	if len(blk.SubtreeBin) > 0 {
		if err := h.LibP2P.PublishSubtree(ctx, teranode.SubtreeMessage{
			Hash:       blk.SubtreeHash.String(),
			DataHubURL: msDatahub.HostURL(),
		}); err != nil {
			t.Fatalf("publish subtree: %v", err)
		}
	}
	msg := teranode.BlockMessage{
		Hash:       blk.Hash.String(),
		Height:     blk.Height,
		DataHubURL: msDatahub.HostURL(),
		Header:     blk.HeaderHex,
		Coinbase:   blk.CoinbaseHex,
	}
	if err := harness.PublishBlockUntilTip(ctx, rt, h.LibP2P, msg, 90*time.Second); err != nil {
		t.Fatalf("chaintracks tip: %v", err)
	}
	return blk
}

type callbackProxy struct {
	callbackURL string
	target      string
	holdSTUMP   atomic.Bool
	mu          sync.Mutex
	bodies      [][]byte
	held        [][]byte
	seen        chan struct{}
}

func newCallbackProxy(t *testing.T) *callbackProxy {
	t.Helper()
	p := &callbackProxy{seen: make(chan struct{}, 64)}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	p.callbackURL = fmt.Sprintf("http://%s:%d/api/v1/merkle-service/callback", harness.CallbackHost(), port)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(p.serve))
	_ = srv.Listener.Close()
	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	return p
}

func (p *callbackProxy) setTarget(base string) {
	p.mu.Lock()
	p.target = base
	p.mu.Unlock()
}

func (p *callbackProxy) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var msg struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(body, &msg)
	if msg.Type == "STUMP" && p.holdSTUMP.Load() {
		p.mu.Lock()
		p.held = append(p.held, append([]byte(nil), body...))
		p.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	}
	p.record(body)
	p.forward(w, r, body)
}

func (p *callbackProxy) record(body []byte) {
	p.mu.Lock()
	p.bodies = append(p.bodies, append([]byte(nil), body...))
	p.mu.Unlock()
	select {
	case p.seen <- struct{}{}:
	default:
	}
}

func (p *callbackProxy) forward(w http.ResponseWriter, r *http.Request, body []byte) {
	p.mu.Lock()
	target := p.target
	p.mu.Unlock()
	if target == "" {
		http.Error(w, "no target", http.StatusBadGateway)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target+"/api/v1/merkle-service/callback", bytes.NewReader(body))
	if err != nil {
		http.Error(w, "forward", http.StatusBadGateway)
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "forward", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
}

func (p *callbackProxy) flushHeld(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	held := p.held
	p.held = nil
	target := p.target
	p.mu.Unlock()
	for _, body := range held {
		req, err := http.NewRequest(http.MethodPost, target+"/api/v1/merkle-service/callback", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("flush stump: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer synthetic-stage2-callback-bearer")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("flush stump: %v", err)
		}
		_ = resp.Body.Close()
		p.record(body)
	}
}

func (p *callbackProxy) waitType(ctx context.Context, want string) []byte {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if body := p.findType(want, ""); body != nil {
			return body
		}
		select {
		case <-ctx.Done():
			return nil
		case <-p.seen:
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil
}

func (p *callbackProxy) waitBlock(ctx context.Context, blockHash string) []byte {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if body := p.findType("BLOCK_PROCESSED", blockHash); body != nil {
			return body
		}
		select {
		case <-ctx.Done():
			return nil
		case <-p.seen:
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil
}

func (p *callbackProxy) findType(want, blockHash string) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, body := range p.bodies {
		var msg struct {
			Type      string `json:"type"`
			BlockHash string `json:"blockHash"`
		}
		if json.Unmarshal(body, &msg) != nil {
			continue
		}
		if msg.Type == want && (blockHash == "" || msg.BlockHash == blockHash) {
			return body
		}
	}
	return nil
}

func (p *callbackProxy) sawType(want string) bool {
	return p.findType(want, "") != nil
}

func expectedIndices(body []byte) ([]int, bool) {
	var msg struct {
		Indices []int `json:"expectedSubtreeIndices"`
	}
	if json.Unmarshal(body, &msg) != nil {
		return nil, false
	}
	if !bytes.Contains(body, []byte(`"expectedSubtreeIndices"`)) {
		return nil, false
	}
	return msg.Indices, true
}

func processedAt(t *testing.T, ctx context.Context, rt *harness.ArcadeRuntime, blockHash string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rt.BaseURL+"/api/v1/blocks/processing-status/"+blockHash, nil)
	if err != nil {
		t.Fatalf("block status: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("block status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return ""
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("block status %d: %s", resp.StatusCode, b)
	}
	var row struct {
		ProcessedAt string `json:"processedAt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&row); err != nil {
		t.Fatalf("decode block status: %v", err)
	}
	return row.ProcessedAt
}

func lookupCallbackURLs(t *testing.T, ctx context.Context, merkleURL, txid string) []string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, merkleURL+"/api/lookup/"+txid, nil)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var lookup struct {
		CallbackUrls []string `json:"callbackUrls"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lookup); err != nil {
		t.Fatalf("decode lookup: %v", err)
	}
	return lookup.CallbackUrls
}

func containsCallbackPath(urls []string) bool {
	for _, u := range urls {
		if strings.Contains(u, "/api/v1/merkle-service/callback") {
			return true
		}
	}
	return false
}

func replayBlockProcessed(t *testing.T, rt *harness.ArcadeRuntime, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, rt.BaseURL+"/api/v1/merkle-service/callback", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer synthetic-stage2-callback-bearer")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("replay status %d: %s", resp.StatusCode, b)
	}
}

type statusObserver struct {
	rt     *harness.ArcadeRuntime
	txid   string
	mu     sync.Mutex
	rows   []statusRow
	stopCh chan struct{}
	done   chan struct{}
}

type statusRow struct {
	At     string `json:"at"`
	Status string `json:"status"`
}

func newStatusObserver(rt *harness.ArcadeRuntime, txid string) *statusObserver {
	return &statusObserver{
		rt:     rt,
		txid:   txid,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (o *statusObserver) start(ctx context.Context) {
	go func() {
		defer close(o.done)
		var last string
		for {
			select {
			case <-o.stopCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			st, ok, err := harness.GetTxStatus(ctx, o.rt, o.txid)
			if err != nil || !ok || st.TxStatus == last {
				continue
			}
			last = st.TxStatus
			o.mu.Lock()
			o.rows = append(o.rows, statusRow{At: time.Now().UTC().Format(time.RFC3339Nano), Status: st.TxStatus})
			o.mu.Unlock()
		}
	}()
}

func (o *statusObserver) stop() {
	select {
	case <-o.stopCh:
	default:
		close(o.stopCh)
	}
	<-o.done
}

func (o *statusObserver) snapshot() []statusRow {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]statusRow, len(o.rows))
	copy(out, o.rows)
	return out
}

func (o *statusObserver) statuses() []string {
	rows := o.snapshot()
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = row.Status
	}
	return out
}

func writeEvidence(t *testing.T, evidence map[string]any) {
	t.Helper()
	path := os.Getenv("STAGE2_EVIDENCE")
	if path == "" {
		return
	}
	raw, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("evidence: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("evidence: %v", err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		t.Fatalf("evidence: %v", err)
	}
}
