package pebble

import (
	"fmt"
)

// Pebble is a flat KV store, so we encode primary records and secondary
// indexes into a single keyspace using short ASCII prefixes. The split
// between primary rows ("tx:…", "bump:…", "sub:…", …) and index entries
// ("idx:…") matters on writes because we batch them together in one
// IndexedBatch so reads of the in-flight index see the primary rows.
//
// Index entries store no value — their existence is the payload. We read
// the referenced txid/id off the end of the key. This keeps index writes
// to a single Set() per index and avoids a second lookup round-trip.
const (
	prefixTx       = "tx:"
	prefixBump     = "bump:"
	prefixStump    = "stump:"
	prefixSub      = "sub:"
	prefixLease    = "lease:"
	prefixDatahub  = "dh:"

	prefixIdxTxStatus     = "idx:tx:status:"
	prefixIdxTxBlock      = "idx:tx:block:"
	prefixIdxTxRetryReady = "idx:tx:retry_ready:"
	prefixIdxTxUpdated    = "idx:tx:updated:"
	prefixIdxSubTxID      = "idx:sub:txid:"
	prefixIdxSubToken     = "idx:sub:token:"
	prefixIdxStumpBlock   = "idx:stump:block:"
)

func txKey(txid string) []byte            { return []byte(prefixTx + txid) }
func bumpKey(blockHash string) []byte     { return []byte(prefixBump + blockHash) }
func subKey(id string) []byte             { return []byte(prefixSub + id) }
func leaseKey(name string) []byte         { return []byte(prefixLease + name) }
func datahubEndpointKey(url string) []byte { return []byte(prefixDatahub + url) }
func datahubEndpointPrefix() []byte        { return []byte(prefixDatahub) }

func stumpKey(blockHash string, idx int) []byte {
	return []byte(fmt.Sprintf("%s%s:%010d", prefixStump, blockHash, idx))
}

// stumpBlockPrefix returns the prefix used to iterate all STUMP rows for a block.
func stumpBlockPrefix(blockHash string) []byte {
	return []byte(prefixStump + blockHash + ":")
}

// Secondary index key builders. All index keys embed the primary key at the end
// so the iterator can recover the target row without a second lookup.

func idxTxStatusKey(status, txid string) []byte {
	return []byte(prefixIdxTxStatus + status + ":" + txid)
}

func idxTxStatusPrefix(status string) []byte {
	return []byte(prefixIdxTxStatus + status + ":")
}

func idxTxBlockKey(blockHash, txid string) []byte {
	return []byte(prefixIdxTxBlock + blockHash + ":" + txid)
}

func idxTxBlockPrefix(blockHash string) []byte {
	return []byte(prefixIdxTxBlock + blockHash + ":")
}

// idxTxRetryReadyKey encodes next_retry_at as 16 zero-padded hex digits so
// prefix iteration returns rows in time order. Using unix-nanoseconds keeps
// the ordering stable across the epoch boundary.
func idxTxRetryReadyKey(nextRetryUnixNs int64, txid string) []byte {
	return []byte(fmt.Sprintf("%s%016x:%s", prefixIdxTxRetryReady, uint64(nextRetryUnixNs), txid))
}

func idxTxRetryReadyPrefix() []byte {
	return []byte(prefixIdxTxRetryReady)
}

func idxTxUpdatedKey(updatedUnixNs int64, txid string) []byte {
	return []byte(fmt.Sprintf("%s%016x:%s", prefixIdxTxUpdated, uint64(updatedUnixNs), txid))
}

func idxTxUpdatedPrefix() []byte {
	return []byte(prefixIdxTxUpdated)
}

func idxSubTxIDKey(txid, id string) []byte {
	return []byte(prefixIdxSubTxID + txid + ":" + id)
}

func idxSubTxIDPrefix(txid string) []byte {
	return []byte(prefixIdxSubTxID + txid + ":")
}

func idxSubTokenKey(token, id string) []byte {
	return []byte(prefixIdxSubToken + token + ":" + id)
}

func idxSubTokenPrefix(token string) []byte {
	return []byte(prefixIdxSubToken + token + ":")
}

func idxStumpBlockKey(blockHash string, subtreeIndex int) []byte {
	return []byte(fmt.Sprintf("%s%s:%010d", prefixIdxStumpBlock, blockHash, subtreeIndex))
}

// idEndOfPrefix returns the upper bound of a prefix range for use with
// pebble.IterOptions.UpperBound. The returned slice is the shortest bytes
// that's strictly greater than any key starting with prefix.
func endOfPrefix(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[: i+1]
		}
	}
	return nil // all 0xff — unbounded
}

// lastSegment returns the substring after the last ':' in key. Index keys
// always end with ":<txid>" or ":<id>" so this pulls out the referenced row.
func lastSegment(key []byte) string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == ':' {
			return string(key[i+1:])
		}
	}
	return string(key)
}

