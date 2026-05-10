//go:build e2e

// Package e2e_test holds the end-to-end smoke scenarios. Each file
// drives the harness through a representative arcade ↔ merkle-service
// flow and asserts the user-visible outcome (tx reaching MINED,
// callbacks delivered, etc.).
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/bsv-blockchain/arcade/tests/e2e/harness"
)

// TestSmoke_TxRegistersWithMerkleService is the v1 smoke test: it
// verifies that submitting a tx to arcade flows through the broadcast
// path far enough that arcade calls merkle-service's /watch endpoint
// with the parsed txid + arcade's callback URL/token.
//
// The check exercises every wiring the harness touches:
//
//   - arcade boots in-process and accepts POST /tx
//   - the tx-validator parses and structurally validates the tx
//   - the propagation service consumes from kafka and calls
//     merkleClient.Register
//   - merkle-service in its container reaches its registration store
//   - GET /api/lookup/<txid> on merkle-service returns the configured
//     callback URL
//
// What this test does NOT yet assert (tracked in MERKLE_SERVICE_GAPS):
// the tx reaching MINED status. That requires constructing blocks
// whose merkle root composition matches what arcade's
// ValidateCompoundRoot expects, which is the next step.
func TestSmoke_TxRegistersWithMerkleService(t *testing.T) {
	skipIfNoDocker(t)

	datahub, err := harness.NewDatahub(t)
	if err != nil {
		t.Fatalf("datahub: %v", err)
	}

	h := harness.New(t)

	rt := harness.StartArcade(t, harness.ArcadeOptions{
		MerkleServiceURL: h.Containers.MerkleHostURL,
		DatahubURL:       datahub.HostURL(),
		LibP2PBootstrap:  h.LibP2P.BootstrapMultiaddr(),
		MerkleAuthToken:  "e2e-watch-token",
		CallbackToken:    "e2e-callback-token",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tx := harness.BuildValidatableTxs(1, 0)[0]
	txid, err := harness.BroadcastTx(ctx, t, rt, tx)
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	t.Logf("broadcast tx %s", txid)

	// arcade's tx-validator + propagation pipeline is asynchronous.
	// Poll merkle-service's lookup endpoint until the registration
	// shows up.
	if err := waitForMerkleRegistration(ctx, h.Containers.MerkleHostURL, txid, 30*time.Second); err != nil {
		t.Fatalf("merkle-service registration: %v", err)
	}
}

func waitForMerkleRegistration(ctx context.Context, merkleURL, txid string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("%s/api/lookup/%s", merkleURL, txid)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var lookup struct {
					CallbackUrls []string `json:"callbackUrls"`
				}
				if jErr := json.Unmarshal(body, &lookup); jErr == nil && len(lookup.CallbackUrls) > 0 {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out polling %s", url)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// skipIfNoDocker mirrors the harness package helper so smoke tests
// don't need access to its private function.
func skipIfNoDocker(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("no container runtime reachable (NewDockerProvider): %v", err)
	}
	defer func() { _ = provider.Close() }()
	if err := provider.Health(ctx); err != nil {
		t.Skipf("container runtime not healthy: %v", err)
	}
}
