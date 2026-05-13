package bump

import (
	"bytes"
	"context"
	"encoding/binary"
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

// --- Untrusted-varint allocation tests (F-008) ---------------------------

// blockBytesWithSubtreeCountVarint builds a binary block payload with the
// supplied subtreeCount written verbatim as a 9-byte 0xFF varint. The body
// itself is short, so a parser that allocates based on the varint will trip
// long before it tries to fill the buffer.
func blockBytesWithSubtreeCountVarint(t *testing.T, subtreeCount uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(make([]byte, 80)) // header (zeroed; merkle-root only needs 32 bytes)
	buf.WriteByte(0x00)         // txCount = 0
	buf.WriteByte(0x00)         // sizeBytes = 0
	// subtreeCount as 0xFF + uint64 LE — the largest VarInt encoding form,
	// so we can dial in any uint64 value the test wants.
	buf.WriteByte(0xff)
	var le [8]byte
	binary.LittleEndian.PutUint64(le[:], subtreeCount)
	buf.Write(le[:])
	return buf.Bytes()
}

// TestFetchBlockDataForBUMP_RejectsHugeSubtreeCount verifies that a varint
// claiming far more subtrees than maxSubtreeCount is rejected without
// attempting a giant preallocation. We run the test through the public
// fetcher to also exercise the body-cap and HTTP plumbing.
func TestFetchBlockDataForBUMP_RejectsHugeSubtreeCount(t *testing.T) {
	body := blockBytesWithSubtreeCountVarint(t, 1<<60) // ~1.15 quintillion

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	_, _, _, err := FetchBlockDataForBUMP(
		context.Background(),
		[]string{srv.URL},
		"deadbeef",
		zap.NewNop(),
	)
	if err == nil {
		t.Fatal("expected error for oversized subtree count varint")
	}
	if !strings.Contains(err.Error(), "subtree count") {
		t.Errorf("expected error mentioning subtree count, got: %v", err)
	}
}

// TestParseBlockBinary_RejectsSubtreeCountAboveBodyCapacity verifies that a
// subtreeCount which is below maxSubtreeCount but still cannot fit in the
// remaining body bytes is rejected before the make() call. This catches
// "plausible-but-impossible" counts that would otherwise allocate hundreds
// of MiB for a body only kilobytes long.
func TestParseBlockBinary_RejectsSubtreeCountAboveBodyCapacity(t *testing.T) {
	// 1,000,000 < maxSubtreeCount (10,000,000) so the absolute cap passes,
	// but the body has zero bytes after the varint, so the body-capacity
	// check must reject.
	body := blockBytesWithSubtreeCountVarint(t, 1_000_000)
	_, _, _, err := parseBlockBinary(body)
	if err == nil {
		t.Fatal("expected error for subtree count exceeding body capacity")
	}
	if !strings.Contains(err.Error(), "remaining body capacity") {
		t.Errorf("expected body-capacity error, got: %v", err)
	}
}

// TestParseBlockBinary_AcceptsZeroSubtreeCount keeps the boundary case
// covered: an empty subtree list must continue to parse successfully and
// return zero hashes.
func TestParseBlockBinary_AcceptsZeroSubtreeCount(t *testing.T) {
	body := minimalValidBlockBytes(t)
	hashes, _, root, err := parseBlockBinary(body)
	if err != nil {
		t.Fatalf("expected success for zero subtree count, got: %v", err)
	}
	if len(hashes) != 0 {
		t.Errorf("expected 0 hashes, got %d", len(hashes))
	}
	if root == nil {
		t.Errorf("expected non-nil header merkle root")
	}
}

// TestParseBlockBinary_RejectsCoinbaseBUMPLengthAboveBodyCapacity covers the
// sibling unbounded allocation: cbBUMPLen is a varint and was previously
// fed straight into make([]byte, ...). We construct a payload with a valid
// header + zero subtree hashes + minimal coinbase tx, then a bogus 2^60
// cbBUMPLen, and confirm the parser short-circuits to "no coinbase BUMP"
// instead of allocating an exabyte of memory.
func TestParseBlockBinary_RejectsCoinbaseBUMPLengthAboveBodyCapacity(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(make([]byte, 80)) // header
	buf.WriteByte(0x00)         // txCount = 0
	buf.WriteByte(0x00)         // sizeBytes = 0
	buf.WriteByte(0x00)         // subtreeCount = 0
	// Minimal-ish coinbase tx: version (4) | inCount=1 | prev hash (32) |
	// prev index (4) | scriptLen=0 | sequence (4) | outCount=0 | locktime (4).
	// The bsv-sdk tx parser is happy with this skeleton even though it would
	// be rejected by consensus — we only need txBytesUsed to advance.
	tx := make([]byte, 0, 64)
	tx = append(tx, 0x01, 0x00, 0x00, 0x00) // version
	tx = append(tx, 0x01)                   // inCount = 1
	tx = append(tx, make([]byte, 32)...)    // prev hash
	tx = append(tx, 0xff, 0xff, 0xff, 0xff) // prev index
	tx = append(tx, 0x00)                   // scriptLen = 0
	tx = append(tx, 0xff, 0xff, 0xff, 0xff) // sequence
	tx = append(tx, 0x00)                   // outCount = 0
	tx = append(tx, 0x00, 0x00, 0x00, 0x00) // locktime
	buf.Write(tx)
	buf.WriteByte(0x00) // blockHeight varint = 0
	// cbBUMPLen as a 0xFF varint with a wildly oversized value.
	buf.WriteByte(0xff)
	var le [8]byte
	binary.LittleEndian.PutUint64(le[:], 1<<60)
	buf.Write(le[:])

	hashes, cb, root, err := parseBlockBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("parser should not error on bogus cbBUMPLen, got: %v", err)
	}
	if cb != nil {
		t.Errorf("expected nil coinbase BUMP for oversize cbBUMPLen, got %d bytes", len(cb))
	}
	if len(hashes) != 0 {
		t.Errorf("expected 0 hashes, got %d", len(hashes))
	}
	if root == nil {
		t.Errorf("expected non-nil header merkle root")
	}
}
