//go:build e2e

package harness

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Datahub is the in-process HTTP server that stands in for a teranode
// DataHub during e2e tests. Both arcade's bump-builder and merkle-service
// fetch synthetic blocks/subtrees from here:
//
//   - GET /block/<hash>   → returns the binary written by Stage(blockHash, ...)
//   - GET /subtree/<hash> → returns the binary written by Stage(subtreeHash, ...)
//
// The server listens on 0.0.0.0 and exposes URLs reachable from
// merkle-service via host.docker.internal — same convention as the libp2p
// host. Tests Stage block + subtree binaries before publishing the
// matching libp2p announcement, so by the time merkle-service / arcade
// fetches, the bytes are ready.
type Datahub struct {
	server  *httptest.Server
	port    int
	hostURL string

	mu       sync.RWMutex
	blocks   map[string][]byte
	subtrees map[string][]byte
}

// DatahubOptions tunes the announce hostname/IP the datahub publishes
// in its HostURL(). Zero-value falls back to host.docker.internal,
// preserving legacy behavior. For runtimes where host.docker.internal
// isn't routable (rootless podman with pasta's --no-map-gw), pass the
// docker-network gateway IP from Containers.GatewayIP — same recipe
// the libp2p host uses.
type DatahubOptions struct {
	// AnnounceHost overrides the hostname/IP in the URL the datahub
	// reports via HostURL(). Defaults to "host.docker.internal".
	AnnounceHost string
}

// NewDatahub spins up a fresh datahub HTTP server announcing on
// host.docker.internal. Equivalent to NewDatahubWith(t,
// DatahubOptions{}). Kept for callers that don't need to override the
// announce host.
func NewDatahub(t *testing.T) (*Datahub, error) {
	return NewDatahubWith(t, DatahubOptions{})
}

// NewDatahubWith is the option-bag variant of NewDatahub. Use this
// when host.docker.internal isn't routable from your container
// runtime (in particular rootless podman) and pass the docker-network
// gateway IP from Containers.GatewayIP via opts.AnnounceHost.
func NewDatahubWith(t *testing.T, opts DatahubOptions) (*Datahub, error) {
	return NewDatahubOnPort(t, opts, 0)
}

// NewDatahubOnPort spins up the datahub bound to a specific TCP port.
// Pass port=0 to let the OS pick — equivalent to NewDatahubWith. Use
// a pre-picked port (via pickFreeTCPPort) when the caller needs the
// datahub URL constructible BEFORE the listener actually binds —
// e.g., harness.WithReprocessReady() builds the URL and stuffs it into
// merkle-service's DATAHUB_FALLBACK_URLS env var at container creation
// time, then opens the listener after the container is up.
func NewDatahubOnPort(t *testing.T, opts DatahubOptions, port int) (*Datahub, error) {
	t.Helper()

	announceHost := opts.AnnounceHost
	if announceHost == "" {
		announceHost = "host.docker.internal"
	}

	d := &Datahub{
		blocks:   make(map[string][]byte),
		subtrees: make(map[string][]byte),
	}

	// Bind on 0.0.0.0 so the merkle-service container can reach us via
	// host.docker.internal / gateway-IP:<port>. httptest.NewUnstartedServer
	// binds on 127.0.0.1 by default which is unreachable from a container
	// even with host-gateway, so we replace its listener with a 0.0.0.0 one.
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen 0.0.0.0:%d: %w", port, err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(d.handle))
	_ = srv.Listener.Close()
	srv.Listener = listener
	srv.Start()

	d.server = srv
	d.port = listener.Addr().(*net.TCPAddr).Port
	d.hostURL = fmt.Sprintf("http://%s:%d", announceHost, d.port)

	t.Cleanup(d.Close)
	return d, nil
}

// HostURL is the base URL the merkle-service container should be told
// about (via BlockMessage.DataHubURL / SubtreeMessage.DataHubURL).
func (d *Datahub) HostURL() string { return d.hostURL }

// LocalURL is the host-side base URL the test process can hit directly
// for sanity checks. Containers can't use this — it's loopback-bound on
// the test runner.
func (d *Datahub) LocalURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", d.port)
}

// StageBlock registers a block binary keyed by its hash. The hash must
// match the hash merkle-service / arcade will GET — typically the block
// hash the test publishes via libp2p.
func (d *Datahub) StageBlock(hash chainhash.Hash, payload []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.blocks[hash.String()] = append([]byte(nil), payload...)
}

// StageSubtree registers a subtree binary keyed by its hash.
func (d *Datahub) StageSubtree(hash chainhash.Hash, payload []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.subtrees[hash.String()] = append([]byte(nil), payload...)
}

// StageFixture stages every binary a RealBlockFixture carries: the
// block.bin under /block/<hash> and each subtree under /subtree/<hash>.
// One call per fixture is enough to make merkle-service's block-fetch
// pipeline succeed when it consumes the published BlockMessage /
// SubtreeMessage announcements.
func (d *Datahub) StageFixture(fix *RealBlockFixture) {
	d.StageBlock(fix.BlockHash, fix.BlockBin)
	for _, st := range fix.Subtrees {
		d.StageSubtree(st.Hash, st.Bin)
	}
}

// Close stops the server. Idempotent.
func (d *Datahub) Close() {
	if d == nil || d.server == nil {
		return
	}
	d.server.Close()
}

func (d *Datahub) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/block/"):
		hash := strings.TrimPrefix(r.URL.Path, "/block/")
		d.mu.RLock()
		payload, ok := d.blocks[hash]
		d.mu.RUnlock()
		if !ok {
			http.Error(w, "block not staged", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payload)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/subtree/"):
		hash := strings.TrimPrefix(r.URL.Path, "/subtree/")
		d.mu.RLock()
		payload, ok := d.subtrees[hash]
		d.mu.RUnlock()
		if !ok {
			http.Error(w, "subtree not staged", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payload)
	case r.Method == http.MethodPost && (r.URL.Path == "/tx" || r.URL.Path == "/txs"):
		// Arcade's propagation service POSTs validated raw txs here
		// expecting a real teranode would accept them into mempool.
		// For e2e tests we just acknowledge — the mempool side-effect
		// isn't observable, and rejecting (404) cascades into arcade
		// marking the txs themselves REJECTED, blocking the
		// merkle-proof-return-path scenarios. Returning 200 with no
		// body satisfies arcade's teranode.Client.Submit*.
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}
