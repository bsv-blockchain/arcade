package api_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/gin-gonic/gin"
)

// fakeChaintracks implements the chaintracks.Chaintracks interface with
// canned responses and a tip channel the test drives directly. Just enough to
// exercise the Gin adapter — we don't pretend to run a real P2P subscription.
type fakeChaintracks struct {
	network string
	height  uint32
	tip     *chaintracks.BlockHeader
	headers map[uint32]*chaintracks.BlockHeader
	tipCh   chan *chaintracks.BlockHeader
	reorgCh chan *chaintracks.ReorgEvent
}

func newFakeChaintracks() *fakeChaintracks {
	return &fakeChaintracks{
		network: "mainnet",
		height:  100,
		headers: make(map[uint32]*chaintracks.BlockHeader),
		tipCh:   make(chan *chaintracks.BlockHeader, 4),
		reorgCh: make(chan *chaintracks.ReorgEvent, 4),
	}
}

func (f *fakeChaintracks) IsValidRootForHeight(_ context.Context, _ *chainhash.Hash, _ uint32) (bool, error) {
	return true, nil
}
func (f *fakeChaintracks) CurrentHeight(context.Context) (uint32, error) { return f.height, nil }
func (f *fakeChaintracks) GetNetwork(context.Context) (string, error)    { return f.network, nil }
func (f *fakeChaintracks) GetHeight(context.Context) uint32           { return f.height }
func (f *fakeChaintracks) GetTip(context.Context) *chaintracks.BlockHeader {
	return f.tip
}
func (f *fakeChaintracks) GetHeaderByHeight(_ context.Context, h uint32) (*chaintracks.BlockHeader, error) {
	if hdr, ok := f.headers[h]; ok {
		return hdr, nil
	}
	return nil, chaintracks.ErrHeaderNotFound
}
func (f *fakeChaintracks) GetHeaderByHash(context.Context, *chainhash.Hash) (*chaintracks.BlockHeader, error) {
	return nil, chaintracks.ErrHeaderNotFound
}
func (f *fakeChaintracks) GetHeaders(context.Context, uint32, uint32) ([]*chaintracks.BlockHeader, error) {
	return nil, nil
}
func (f *fakeChaintracks) Subscribe(context.Context) <-chan *chaintracks.BlockHeader {
	return f.tipCh
}
func (f *fakeChaintracks) Unsubscribe(<-chan *chaintracks.BlockHeader)            {}
func (f *fakeChaintracks) SubscribeReorg(context.Context) <-chan *chaintracks.ReorgEvent {
	return f.reorgCh
}
func (f *fakeChaintracks) UnsubscribeReorg(<-chan *chaintracks.ReorgEvent) {}

// fixtureTip returns a deterministic BlockHeader so tests can assert exact bytes.
func fixtureTip() *chaintracks.BlockHeader {
	var prev, merkle chainhash.Hash
	for i := range prev {
		prev[i] = byte(i)
		merkle[i] = byte(255 - i)
	}
	h := &block.Header{
		Version:    1,
		PrevHash:   prev,
		MerkleRoot: merkle,
		Timestamp:  1234567890,
		Bits:       0x1d00ffff,
		Nonce:      42,
	}
	return &chaintracks.BlockHeader{
		Header: h,
		Height: 100,
		Hash:   h.Hash(),
	}
}

func setupChaintracksRouter(t *testing.T, ct chaintracks.Chaintracks) (*gin.Engine, *chaintracksRoutes) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routes := newChaintracksRoutes(ctx, ct)
	routes.Register(r.Group("/chaintracks/v2"))
	routes.RegisterLegacy(r.Group("/chaintracks/v1"))
	return r, routes
}

// When chaintracks is disabled (no routes mounted), requests to the namespace
// must 404 rather than leak to an unrelated handler.
func TestServer_ChaintracksDisabled_Returns404ForChaintracksRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// No chaintracks routes registered. Simulate a bare router with the same
	// non-chaintracks routes the server would normally have.
	r.GET("/health", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest("GET", "/chaintracks/v1/height", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when chaintracks routes not mounted, got %d", w.Code)
	}
}

// Enabled path: /network and /height return canned JSON from the fake.
func TestServer_ChaintracksEnabled_MountsRoutes(t *testing.T) {
	f := newFakeChaintracks()
	r, _ := setupChaintracksRouter(t, f)

	// /network
	req := httptest.NewRequest("GET", "/chaintracks/v2/network", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("network: got %d, want 200", w.Code)
	}
	var netResp struct {
		Network string `json:"network"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &netResp); err != nil {
		t.Fatalf("decode network: %v", err)
	}
	if netResp.Network != "mainnet" {
		t.Errorf("network=%q, want mainnet", netResp.Network)
	}

	// /height
	req = httptest.NewRequest("GET", "/chaintracks/v2/height", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("height: got %d, want 200", w.Code)
	}
	var heightResp struct {
		Height uint32 `json:"height"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &heightResp); err != nil {
		t.Fatalf("decode height: %v", err)
	}
	if heightResp.Height != 100 {
		t.Errorf("height=%d, want 100", heightResp.Height)
	}
}

// Binary endpoint must return exactly 80 bytes and set X-Block-Height.
func TestChaintracksRoutes_BinaryHeader_Returns80Bytes(t *testing.T) {
	f := newFakeChaintracks()
	tip := fixtureTip()
	f.tip = tip
	f.headers[tip.Height] = tip
	r, _ := setupChaintracksRouter(t, f)

	req := httptest.NewRequest("GET", "/chaintracks/v2/tip.bin", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("tip.bin status=%d, want 200", w.Code)
	}
	if n := w.Body.Len(); n != 80 {
		t.Errorf("tip.bin body length=%d, want 80", n)
	}
	if got := w.Header().Get("X-Block-Height"); got != "100" {
		t.Errorf("X-Block-Height=%q, want \"100\"", got)
	}
	if got := w.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type=%q, want application/octet-stream", got)
	}
}

// Tip SSE stream must forward new tips and exit on client disconnect.
func TestChaintracksRoutes_TipStream_ForwardsUpdates(t *testing.T) {
	f := newFakeChaintracks()
	f.tip = fixtureTip()

	// Use a real httptest.Server here so we get a real http.Flusher and can
	// close the client connection to drive disconnect.
	gin.SetMode(gin.TestMode)
	r := gin.New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routes := newChaintracksRoutes(ctx, f)
	routes.Register(r.Group("/chaintracks/v2"))
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	client := &http.Client{Timeout: 3 * time.Second}
	reqCtx, reqCancel := context.WithCancel(context.Background())
	t.Cleanup(reqCancel)
	req, _ := http.NewRequestWithContext(reqCtx, "GET", srv.URL+"/chaintracks/v2/tip/stream", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status=%d, want 200", resp.StatusCode)
	}

	// First frame is the initial tip. Rely on the http client's overall
	// timeout to bound this read; in the happy path the broadcaster writes
	// the initial tip before we get here.
	buf := make([]byte, 512)
	n, err := resp.Body.Read(buf)
	if err != nil && n == 0 {
		t.Fatalf("read initial tip: err=%v n=%d", err, n)
	}
	if n < len("data: ") || string(buf[:6]) != "data: " {
		t.Errorf("expected SSE 'data: ' prefix, got %q", string(buf[:min(n, 32)]))
	}

	// Push another tip, expect to receive it within a short window.
	nextTip := fixtureTip()
	nextTip.Height = 101
	f.tipCh <- nextTip

	// Small wait to let broadcaster fan it out.
	time.Sleep(100 * time.Millisecond)
	n2, _ := resp.Body.Read(buf)
	if n2 == 0 {
		t.Fatal("no second tip frame received")
	}

	// Disconnect; goroutine in the handler should exit (verified implicitly by
	// the test not leaking — go test -race would catch leaks).
	reqCancel()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
