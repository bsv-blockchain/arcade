// Package logfields provides canonical zap.Field constructors for the
// identifiers arcade logs at every transaction-lifecycle transition: txid,
// block_hash, block_height, and friends. Using these constructors instead
// of ad-hoc zap.String/Strings/Int/Uint64 calls keeps field names
// consistent across arcade and merkle-service, so searching Coralogix for
// a txid or block_hash surfaces every log line touching that transaction
// regardless of which component emitted it.
//
// enforce_test.go statically enforces this: any zap.String/Strings/Int/
// Uint64 call anywhere else in the repo whose first argument is one of the
// canonical (or retired) field-name literals fails the build's test suite.
//
// This package must stay a leaf: it imports only go.uber.org/zap so it can
// be pulled into kafka, services/*, and merkleservice without creating an
// import cycle.
package logfields

import "go.uber.org/zap"

// maxTxIDsPerLine caps how many txids TxIDBatch and ForEachTxIDChunk place
// in a single log line. A txid is 64 hex chars (~67 bytes of JSON with
// quoting/comma), so 1000 keeps a single line around 67 KB — comfortably
// inside typical log-shipper line-length limits while still covering the
// large majority of batches with a single line.
const maxTxIDsPerLine = 1000

// TxID returns the canonical field for a single transaction id.
func TxID(txid string) zap.Field {
	return zap.String("txid", txid)
}

// TxIDs returns the canonical field for a list of transaction ids. It does
// not cap the list — callers logging a batch that may be large should use
// TxIDBatch (bounded single line) or ForEachTxIDChunk (bounded lines,
// full coverage) instead.
func TxIDs(txids []string) zap.Field {
	return zap.Strings("txids", txids)
}

// TxIDCount returns the canonical field for a transaction-id batch size.
func TxIDCount(n int) zap.Field {
	return zap.Int("txid_count", n)
}

// BlockHash returns the canonical field for a block hash.
func BlockHash(hash string) zap.Field {
	return zap.String("block_hash", hash)
}

// BlockHeight returns the canonical field for a block height.
func BlockHeight(height uint64) zap.Field {
	return zap.Uint64("block_height", height)
}

// SubtreeHash returns the canonical field for a merkle subtree hash.
func SubtreeHash(hash string) zap.Field {
	return zap.String("subtree_hash", hash)
}

// CallbackURL returns the canonical field for a callback/webhook URL.
func CallbackURL(url string) zap.Field {
	return zap.String("callback_url", url)
}

// Status returns the canonical field for a transaction status value (e.g.
// "RECEIVED", "MINED").
func Status(status string) zap.Field {
	return zap.String("status", status)
}

// Stage returns the canonical field for the pipeline stage a rejection (or
// other lifecycle event) occurred at (e.g. "intake", "network", "cascade").
func Stage(stage string) zap.Field {
	return zap.String("stage", stage)
}

// TxIDBatch returns the canonical field pair for logging a batch of txids
// on a single line: TxIDCount always carries the TRUE total, even when the
// list itself is capped at maxTxIDsPerLine. Use this for lines where an
// operational preview of the batch is enough — the count field is what
// disambiguates a truncated list from the real size. For lines that must
// surface every txid for full searchability (e.g. MINED), use
// ForEachTxIDChunk instead.
func TxIDBatch(ids []string) []zap.Field {
	capped := ids
	if len(capped) > maxTxIDsPerLine {
		capped = capped[:maxTxIDsPerLine]
	}
	return []zap.Field{TxIDCount(len(ids)), TxIDs(capped)}
}

// ForEachTxIDChunk invokes fn once per maxTxIDsPerLine-sized chunk of ids,
// in order, so a caller can emit one log line per chunk and guarantee every
// txid appears somewhere in the log stream — unlike TxIDBatch, which caps
// the list and can silently drop ids past the first chunk. fn receives the
// chunk, its zero-based index, and the total number of chunks so each line
// can carry "chunk 3/14"-style context. A nil or empty ids invokes fn zero
// times.
func ForEachTxIDChunk(ids []string, fn func(chunk []string, chunkIdx, totalChunks int)) {
	if len(ids) == 0 {
		return
	}
	total := (len(ids) + maxTxIDsPerLine - 1) / maxTxIDsPerLine
	for i := 0; i < total; i++ {
		start := i * maxTxIDsPerLine
		end := start + maxTxIDsPerLine
		if end > len(ids) {
			end = len(ids)
		}
		fn(ids[start:end], i, total)
	}
}
