package api_server

import (
	"context"
	"sort"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/logfields"
	"github.com/bsv-blockchain/broadcaster/models"
)

// maxBatchParentLookups caps the external parents one POST /txs consults, so
// a very wide batch costs one bounded projected read, never an unbounded one.
const maxBatchParentLookups = 256

// partitionKeyForSubmit decides the propagation partition key of a single
// POST /tx submission.
//
// familyPartitionKeys keeps the txs of ONE request together, but a wallet
// paying out of its own unconfirmed change submits a chain one request at a
// time, and each hop keyed by its own txid landed on a random partition. On
// the hosted 8-partition topology a child therefore reached a dispatcher
// that had never heard of its parent, drew TX_MISSING_PARENT from the
// datahub, burned the flat fast-path budget and parked for the reaper
// (hosted-service report, issue 2: hops 1–21 of a 50-deep chain in seconds,
// hops 22–50 parked for minutes).
//
// The fix: look the submission's parents up in the store and, when one is
// still at a status the network may not have acknowledged (RECEIVED or
// PENDING_RETRY), reuse the partition key that parent was published with.
// Same key ⇒ same partition ⇒ the dep-aware dispatcher holds the child behind
// its in-flight parent exactly as it does within one request. A parent at
// ACCEPTED_BY_NETWORK or beyond is already known to Teranode, so the child
// keys by its own txid and the funnel onto one partition lasts only as long
// as the parent is in flight. A resubmission reuses its own row's key so it
// rejoins the family its earlier message belongs to.
//
// Best-effort: a lookup failure, a parent written before keys were persisted,
// or more parents than ParentKeyLookupMaxInputs all fall back to the own-txid
// key and the missing-parent safety net.
func (s *Server) partitionKeyForSubmit(ctx context.Context, txid string, inputs []string) string {
	maxInputs := s.cfg.Propagation.ParentKeyLookupMaxInputs
	if s.store == nil || maxInputs <= 0 {
		return txid
	}
	parents := distinctParents(txid, inputs, maxInputs)
	rows, err := s.store.LookupPartitionKeys(ctx, append(parents, txid))
	if err != nil {
		s.logger.Warn("parent partition-key lookup failed; keying by txid", logfields.TxID(txid), zap.Error(err))
		return txid
	}
	if own, ok := rows[txid]; ok && own.PartitionKey != "" {
		return own.PartitionKey
	}
	for _, p := range parents {
		if pk, ok := rows[p]; ok && pk.PartitionKey != "" && parentStillPending(pk.Status) {
			s.logger.Debug("partition key inherited from in-flight parent",
				logfields.TxID(txid), zap.String("parent", p), zap.String("parent_status", string(pk.Status)))
			return pk.PartitionKey
		}
	}
	return txid
}

// inheritExternalParentKeys re-keys the dependency families of a POST /txs
// batch whose parents OUTSIDE the batch are still in flight, so a family
// joins the partition its external parent is on. keyByTxid is updated in
// place; families with no pending external parent keep their own key.
func (s *Server) inheritExternalParentKeys(ctx context.Context, keyByTxid map[string]string, inputsByTxid map[string][]string) {
	maxInputs := s.cfg.Propagation.ParentKeyLookupMaxInputs
	if s.store == nil || maxInputs <= 0 || len(keyByTxid) == 0 {
		return
	}
	txids := make([]string, 0, len(inputsByTxid))
	for txid := range inputsByTxid {
		txids = append(txids, txid)
	}
	sort.Strings(txids) // deterministic lookup order and remap precedence

	external := make([]string, 0, 16)
	seen := make(map[string]struct{})
	parentsOf := make(map[string][]string, len(txids))
	for _, txid := range txids {
		var ext []string
		for _, p := range distinctParents(txid, inputsByTxid[txid], maxInputs) {
			if _, inBatch := keyByTxid[p]; inBatch {
				continue
			}
			ext = append(ext, p)
			if _, dup := seen[p]; !dup && len(external) < maxBatchParentLookups {
				seen[p] = struct{}{}
				external = append(external, p)
			}
		}
		parentsOf[txid] = ext
	}
	if len(external) == 0 {
		return
	}
	rows, err := s.store.LookupPartitionKeys(ctx, external)
	if err != nil {
		s.logger.Warn("batch parent partition-key lookup failed; keying by family", zap.Int("parents", len(external)), zap.Error(err))
		return
	}
	remap := make(map[string]string) // family key -> inherited key, first pending parent wins
	for _, txid := range txids {
		fam, ok := keyByTxid[txid]
		if !ok {
			continue
		}
		if _, done := remap[fam]; done {
			continue
		}
		for _, p := range parentsOf[txid] {
			if pk, ok := rows[p]; ok && pk.PartitionKey != "" && parentStillPending(pk.Status) {
				remap[fam] = pk.PartitionKey
				break
			}
		}
	}
	for txid, fam := range keyByTxid {
		if nk, ok := remap[fam]; ok {
			keyByTxid[txid] = nk
		}
	}
}

// distinctParents returns up to limit distinct source txids of a transaction,
// in input order, skipping empties and self-references.
func distinctParents(txid string, inputs []string, limit int) []string {
	out := make([]string, 0, min(len(inputs), limit))
	seen := map[string]struct{}{txid: {}}
	for _, p := range inputs {
		if len(out) >= limit {
			break
		}
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// parentStillPending reports whether a parent's row is at a status the network
// may not have acknowledged yet, so its child must reach the same dispatcher
// to be held behind it. ACCEPTED_BY_NETWORK and beyond mean Teranode has the
// parent and the child can go anywhere; a REJECTED parent is left to the
// missing-parent cascade, which rejects the child from the store.
func parentStillPending(st models.Status) bool {
	switch st {
	case models.StatusReceived, models.StatusSentToNetwork, models.StatusPendingRetry:
		return true
	default:
		return false
	}
}
