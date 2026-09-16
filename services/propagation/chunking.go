package propagation

// chunkSpan is a half-open [start, end) index range into a batch.
type chunkSpan struct {
	start, end int
}

// planChunks slices rawTxs into contiguous, disjoint, non-empty spans covering
// [0, len(rawTxs)) such that each span holds at most maxCount transactions and
// at most maxBytes total bytes (issue #271). A non-positive cap disables that
// bound. A single transaction larger than maxBytes always lands alone in its
// own span: the byte cap decides where chunks END, never whether a tx is sent
// — Teranode stays the oracle for per-tx size policy.
//
// The never-empty, never-overlapping guarantee is load-bearing:
// broadcastInChunks writes each span's results into results[start:end] from
// concurrent goroutines with no mutex, which is only safe because spans are
// disjoint.
func planChunks(rawTxs [][]byte, maxCount, maxBytes int) []chunkSpan {
	if len(rawTxs) == 0 {
		return nil
	}
	spans := make([]chunkSpan, 0, 1)
	start, bytes := 0, 0
	for i, tx := range rawTxs {
		n := len(tx)
		if count := i - start; count > 0 && spanFull(count, bytes, n, maxCount, maxBytes) {
			spans = append(spans, chunkSpan{start: start, end: i})
			start, bytes = i, 0
		}
		bytes += n
	}
	return append(spans, chunkSpan{start: start, end: len(rawTxs)})
}

// spanFull reports whether adding one more transaction of `next` bytes to a
// span already holding `count` transactions and `bytes` bytes would breach
// either cap. Both comparisons are inclusive on arcade's side; the margin
// under Teranode's strict "<" limits lives in the shipped defaults
// (config.DefaultTeranodeMaxBatchSize / DefaultTeranodeMaxBatchBytes).
func spanFull(count, bytes, next, maxCount, maxBytes int) bool {
	return (maxCount > 0 && count >= maxCount) || (maxBytes > 0 && bytes+next > maxBytes)
}

// rawTxsBytes is the payload size of a chunk: POST /txs is the plain
// concatenation of the raw transactions, so this equals the Content-Length.
func rawTxsBytes(rawTxs [][]byte) int {
	total := 0
	for _, tx := range rawTxs {
		total += len(tx)
	}
	return total
}
