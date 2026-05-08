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

// NewDatahub spins up a fresh datahub HTTP server. The server's URL,
// reachable from inside containers via host.docker.internal, is
// available via HostURL(). Tests register staged binaries via Stage*.
func NewDatahub(t *testing.T) (*Datahub, error) {
	t.Helper()

	d := &Datahub{
		blocks:   make(map[string][]byte),
		subtrees: make(map[string][]byte),
	}

	// Bind on 0.0.0.0 so the merkle-service container can reach us via
	// host.docker.internal:<port>. httptest.NewUnstartedServer binds on
	// 127.0.0.1 by default which is unreachable from a container even
	// with host-gateway, so we replace its listener with a 0.0.0.0 one.
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("listen 0.0.0.0:0: %w", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(d.handle))
	_ = srv.Listener.Close()
	srv.Listener = listener
	srv.Start()

	d.server = srv
	d.port = listener.Addr().(*net.TCPAddr).Port
	d.hostURL = fmt.Sprintf("http://host.docker.internal:%d", d.port)

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

// Close stops the server. Idempotent.
func (d *Datahub) Close() {
	if d == nil || d.server == nil {
		return
	}
	d.server.Close()
}

func (d *Datahub) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/block/"):
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
	case strings.HasPrefix(r.URL.Path, "/subtree/"):
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
	default:
		http.NotFound(w, r)
	}
}
