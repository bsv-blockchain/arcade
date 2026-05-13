package bump

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// failingDatahub returns 500 on every request so FetchBlockDataForBUMP runs
// the per-URL log path before aggregating into the final error.
func failingDatahub(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFetchBlockDataForBUMP_PerAttemptDebugLog(t *testing.T) {
	a := failingDatahub(t, http.StatusInternalServerError)
	b := failingDatahub(t, http.StatusServiceUnavailable)

	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	_, _, _, err := FetchBlockDataForBUMP(
		context.Background(),
		[]string{a.URL, b.URL},
		"deadbeef",
		logger,
	)
	if err == nil {
		t.Fatal("expected error when all URLs fail")
	}

	entries := recorded.FilterMessage("datahub fetch attempt").All()
	if len(entries) != 2 {
		t.Fatalf("expected 2 per-attempt log entries, got %d", len(entries))
	}

	// Each entry should carry idx, url, status, error.
	statuses := []int{http.StatusInternalServerError, http.StatusServiceUnavailable}
	for i, e := range entries {
		fields := e.ContextMap()
		if got := fields["idx"]; got != int64(i) {
			t.Errorf("entry[%d] idx: got %v want %d", i, got, i)
		}
		if got := fields["status"]; got != int64(statuses[i]) {
			t.Errorf("entry[%d] status: got %v want %d", i, got, statuses[i])
		}
		urlStr, _ := fields["url"].(string)
		if urlStr == "" {
			t.Errorf("entry[%d] url missing", i)
		}
		errStr, _ := fields["error"].(string)
		if !strings.Contains(errStr, "status") {
			t.Errorf("entry[%d] error should include status, got %q", i, errStr)
		}
	}
}

func TestFetchBlockDataForBUMP_EmptySliceFailsClearly(t *testing.T) {
	_, _, _, err := FetchBlockDataForBUMP(context.Background(), nil, "deadbeef", zap.NewNop())
	if err == nil {
		t.Fatal("expected error when no URLs are configured")
	}
	if !strings.Contains(err.Error(), "no DataHub URLs") {
		t.Errorf("expected explicit empty-list error, got %q", err.Error())
	}
}

// --- Response body size cap tests (F-007) --------------------------------

// minimalValidBlockBytes returns a minimal binary-block payload that
// parseBlockBinary accepts: 80-byte header + four varints (txCount=0,
// sizeBytes=0, subtreeCount=0, blockHeight=0) + zero-length coinbase tx
// fragments. The coinbase parse will fail (no tx bytes), which the parser
// treats as "no coinbase BUMP available" — fine for size-cap tests where
// we only care that the read succeeded.
func minimalValidBlockBytes(t *testing.T) []byte {
	t.Helper()
	// 80 bytes of zeroed header (bytes 36..68 carry the merkle root, but
	// chainhash.NewHash only validates length, so any 32 bytes work) plus
	// three single-byte varints: txCount=0, sizeBytes=0, subtreeCount=0.
	out := make([]byte, 83)
	out[80], out[81], out[82] = 0x00, 0x00, 0x00
	return out
}

// blockWithSubtrees builds a minimal-but-valid binary block body with
// `count` subtree hashes (filled with 0xAA…). Used by the multi-URL
// validator tests to simulate a pruned peer (count=1) vs a healthy peer
// (count=N). Caller-chosen merkleRoot lets tests assert mismatches.
func blockWithSubtrees(t *testing.T, count int, merkleRoot [32]byte) []byte {
	t.Helper()
	out := make([]byte, 0, 80+3+count*32)
	// header: 80 bytes; merkle root lives at [36..68).
	header := make([]byte, 80)
	copy(header[36:68], merkleRoot[:])
	out = append(out, header...)
	// varints: txCount=0, sizeBytes=0, subtreeCount=count (single byte if <0xfd)
	out = append(out, 0x00, 0x00)
	if count < 0 || count >= 0xfd {
		t.Fatalf("test helper only supports 0<=count<0xfd, got %d", count)
	}
	out = append(out, byte(count)) //nolint:gosec // bounds-checked above
	for i := 0; i < count; i++ {
		hash := make([]byte, 32)
		for j := range hash {
			hash[j] = byte(0xA0 | (i & 0x0F)) //nolint:gosec // i&0x0F is in [0,15]
		}
		out = append(out, hash...)
	}
	return out
}

// TestFetchBlockDataForBUMP_SubtreeCountValidator_FallsThrough exercises
// Fix 1: a pruned peer returning fewer subtrees than the caller demands
// must be skipped, and the next healthy peer's response must be returned.
// Without this guard, the first 200 response — even if it's wrong — was
// accepted, poisoning every subsequent BUMP assembly attempt.
func TestFetchBlockDataForBUMP_SubtreeCountValidator_FallsThrough(t *testing.T) {
	prunedBody := blockWithSubtrees(t, 1, [32]byte{0xCA, 0xFE})
	healthyBody := blockWithSubtrees(t, 16, [32]byte{0xBE, 0xEF})

	pruned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prunedBody)
	}))
	t.Cleanup(pruned.Close)

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(healthyBody)
	}))
	t.Cleanup(healthy.Close)

	validator := SubtreeCountValidator(16)
	hashes, _, root, err := FetchBlockDataForBUMPWithOptions(
		context.Background(),
		[]string{pruned.URL, healthy.URL},
		"deadbeef",
		1<<20,
		validator,
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("expected to fall through to healthy peer, got error: %v", err)
	}
	if len(hashes) != 16 {
		t.Errorf("expected 16 subtree hashes from healthy peer, got %d", len(hashes))
	}
	if root == nil || root[0] != 0xBE {
		t.Errorf("expected merkle root from healthy peer (BEEF prefix), got %v", root)
	}
}

// TestFetchBlockDataForBUMP_SubtreeCountValidator_AllFail confirms the
// loop surfaces the validator's rejection in the aggregated error when
// every URL fails the check — no silent success.
func TestFetchBlockDataForBUMP_SubtreeCountValidator_AllFail(t *testing.T) {
	body := blockWithSubtrees(t, 1, [32]byte{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	_, _, _, err := FetchBlockDataForBUMPWithOptions(
		context.Background(),
		[]string{srv.URL},
		"deadbeef",
		1<<20,
		SubtreeCountValidator(5),
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("expected error when validator rejects every URL")
	}
	if !strings.Contains(err.Error(), "validator rejected") {
		t.Errorf("expected error to mention validator rejection, got: %v", err)
	}
}

// TestSubtreeCountValidator_DisabledAtZero documents the contract: zero
// minSubtrees returns a nil validator so the legacy "first 200 wins"
// behavior is preserved for callers that haven't yet collected STUMPs.
func TestSubtreeCountValidator_DisabledAtZero(t *testing.T) {
	if v := SubtreeCountValidator(0); v != nil {
		t.Errorf("expected nil validator for minSubtrees=0, got non-nil")
	}
	if v := SubtreeCountValidator(-3); v != nil {
		t.Errorf("expected nil validator for negative minSubtrees, got non-nil")
	}
}

// padToSize right-pads payload with zero bytes so the response body is
// exactly size bytes long.
func padToSize(payload []byte, size int) []byte {
	if len(payload) >= size {
		return payload[:size]
	}
	out := make([]byte, size)
	copy(out, payload)
	return out
}

// TestFetchBlockDataForBUMP_BodyExceedsCap verifies that a response body
// larger than the cap is rejected with an error mentioning the cap, and
// that the error does not embed the response content.
func TestFetchBlockDataForBUMP_BodyExceedsCap(t *testing.T) {
	const maxBytes = 256
	// Distinctive content so we can assert it does not leak into the error.
	oversize := strings.Repeat("A", maxBytes+1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Don't set Content-Length so the cap is enforced by LimitReader,
		// not the pre-read Content-Length check (covered separately).
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(oversize))
	}))
	t.Cleanup(srv.Close)

	_, _, _, err := FetchBlockDataForBUMPWithCap(
		context.Background(),
		[]string{srv.URL},
		"deadbeef",
		maxBytes,
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("expected error for oversize response body")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected error mentioning the cap, got: %v", err)
	}
	if strings.Contains(err.Error(), "AAAA") {
		t.Errorf("error must not embed response content, got: %v", err)
	}
	if !strings.Contains(err.Error(), strconv.Itoa(maxBytes)) {
		t.Errorf("expected error to include the cap (%d), got: %v", maxBytes, err)
	}
}

// TestFetchBlockDataForBUMP_BodyAtCap verifies that a body exactly at the
// cap is accepted (the LimitReader+1 trick must not reject the boundary
// case).
func TestFetchBlockDataForBUMP_BodyAtCap(t *testing.T) {
	payload := minimalValidBlockBytes(t)
	// Pad up to a known size (>= header+3 varints, which is 83 bytes).
	const target = 256
	body := padToSize(payload, target)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	hashes, _, root, err := FetchBlockDataForBUMPWithCap(
		context.Background(),
		[]string{srv.URL},
		"deadbeef",
		int64(target),
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("expected success at cap boundary, got: %v", err)
	}
	// subtreeCount=0 so hashes is empty; the merkle-root pointer is always
	// populated when the header parses, even if the coinbase tail does not.
	if len(hashes) != 0 {
		t.Errorf("expected 0 subtree hashes, got %d", len(hashes))
	}
	if root == nil {
		t.Errorf("expected non-nil header merkle root")
	}
}

// TestFetchBlockDataForBUMP_ContentLengthExceedsCap verifies that an
// advertised oversize Content-Length is rejected before the body is read.
func TestFetchBlockDataForBUMP_ContentLengthExceedsCap(t *testing.T) {
	const maxBytes = 256
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Advertise an oversize Content-Length explicitly. The body itself
		// stays small — the client must reject based on the header alone.
		w.Header().Set("Content-Length", strconv.Itoa(1<<20)) // 1 MiB
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 1024))
	}))
	t.Cleanup(srv.Close)

	_, _, _, err := FetchBlockDataForBUMPWithCap(
		context.Background(),
		[]string{srv.URL},
		"deadbeef",
		maxBytes,
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("expected error for advertised oversize Content-Length")
	}
	if !strings.Contains(err.Error(), "Content-Length") || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected error mentioning Content-Length and exceeds, got: %v", err)
	}
}

// TestFetchBlockDataForBUMP_ZeroCapUsesDefault confirms the cap-aware
// variant falls back to DefaultMaxBlockBytes when a non-positive cap is
// passed, instead of silently disabling the protection. The default is
// 1 GiB so a tiny payload is trivially within it.
func TestFetchBlockDataForBUMP_ZeroCapUsesDefault(t *testing.T) {
	payload := minimalValidBlockBytes(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)

	if _, _, _, err := FetchBlockDataForBUMPWithCap(
		context.Background(),
		[]string{srv.URL},
		"deadbeef",
		0,
		zap.NewNop(),
	); err != nil {
		t.Fatalf("zero cap should select the default and succeed, got: %v", err)
	}
	if _, _, _, err := FetchBlockDataForBUMPWithCap(
		context.Background(),
		[]string{srv.URL},
		"deadbeef",
		-1,
		zap.NewNop(),
	); err != nil {
		t.Fatalf("negative cap should select the default and succeed, got: %v", err)
	}
}
