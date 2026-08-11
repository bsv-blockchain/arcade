package store

import (
	"context"

	"github.com/bsv-blockchain/arcade/models"
)

// DistinctTokens collects the distinct non-empty callback tokens carried by a
// txid's submissions, preserving first-seen order. Returns nil (not an empty
// slice) when none of the submissions carry a token, which is what
// TokensForTxIDs implementations use to decide whether a txid belongs in the
// result map at all.
//
// The input is one txid's submission list — a handful of rows. It is never a
// token's submission list; see the TokensForTxIDs contract.
func DistinctTokens(subs []*models.Submission) []string {
	var tokens []string
	for _, sub := range subs {
		if sub == nil || sub.CallbackToken == "" {
			continue
		}
		dup := false
		for _, seen := range tokens {
			if seen == sub.CallbackToken {
				dup = true
				break
			}
		}
		if !dup {
			tokens = append(tokens, sub.CallbackToken)
		}
	}
	return tokens
}

// TokensForTxIDsViaPerTxID is the TokensForTxIDs implementation for backends
// whose only txid-side access path is a single-txid lookup (Pebble's by-txid
// index prefix scan, Aerospike's txid secondary-index query). It is a plain
// loop — the batching win for those backends is that fan-out asks once per
// txid instead of once per (txid, client), not that the backend can round-trip
// a batch. Postgres overrides this with a real set-returning query.
//
// getByTxID is the backend's GetSubmissionsByTxID. Duplicate txids are
// resolved once.
func TokensForTxIDsViaPerTxID(
	ctx context.Context,
	txids []string,
	getByTxID func(context.Context, string) ([]*models.Submission, error),
) (map[string][]string, error) {
	out := make(map[string][]string, len(txids))
	seen := make(map[string]struct{}, len(txids))
	for _, txid := range txids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if txid == "" {
			continue
		}
		if _, dup := seen[txid]; dup {
			continue
		}
		seen[txid] = struct{}{}
		subs, err := getByTxID(ctx, txid)
		if err != nil {
			return nil, err
		}
		if tokens := DistinctTokens(subs); len(tokens) > 0 {
			out[txid] = tokens
		}
	}
	return out, nil
}
