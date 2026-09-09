package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/store"
)

// UpsertDatahubEndpoint implements store.Store. The policy is $set only when
// the caller carries one: a write with a nil policy leaves any recorded
// policy in place, the rule every backend implements (Postgres with
// COALESCE, Aerospike by omitting bins).
func (s *Store) UpsertDatahubEndpoint(ctx context.Context, ep store.DatahubEndpoint) error {
	if ep.URL == "" {
		return errors.New("upsert datahub endpoint: empty url")
	}
	set := doc(kv(fNetwork, ep.Network), kv(fSource, ep.Source), kv(fLastSeen, msTrunc(ep.LastSeen)))
	if p := ep.Policy; p != nil {
		if err := p.Validate(); err != nil {
			return err
		}
		set = append(set, kv(fPolicy, endpointPolicyToDoc(p)))
	}
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.datahubs.UpdateOne(octx, idFilter(ep.URL), doc(kv(opSet, set)), options.UpdateOne().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert datahub endpoint %s: %w", ep.URL, err)
	}
	return nil
}

// ListDatahubEndpoints implements store.Store. Legacy rows with an empty
// network never match a real network filter and are thereby excluded.
func (s *Store) ListDatahubEndpoints(ctx context.Context, network string) ([]store.DatahubEndpoint, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.datahubs.Find(qctx, doc(kv(fNetwork, network)))
	if err != nil {
		return nil, fmt.Errorf("list datahub endpoints: %w", err)
	}
	var docs []datahubEndpointDoc
	if err := cur.All(qctx, &docs); err != nil {
		return nil, fmt.Errorf("list datahub endpoints: %w", err)
	}
	out := make([]store.DatahubEndpoint, 0, len(docs))
	for _, d := range docs {
		if d.Network == "" {
			continue
		}
		out = append(out, d.toModel())
	}
	return out, nil
}

// UpsertPeerPolicy implements store.Store as a full-document replace.
func (s *Store) UpsertPeerPolicy(ctx context.Context, pp store.PeerPolicy) error {
	if err := pp.Validate(); err != nil {
		return err
	}
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.peers.ReplaceOne(octx, idFilter(pp.PeerID), peerPolicyToDoc(pp), options.Replace().SetUpsert(true)); err != nil {
		return fmt.Errorf("upsert peer policy %s: %w", pp.PeerID, err)
	}
	return nil
}

// ListPeerPolicies implements store.Store.
func (s *Store) ListPeerPolicies(ctx context.Context, network string) ([]store.PeerPolicy, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.peers.Find(qctx, doc(kv(fNetwork, network)))
	if err != nil {
		return nil, fmt.Errorf("list peer policies: %w", err)
	}
	var docs []peerPolicyDoc
	if err := cur.All(qctx, &docs); err != nil {
		return nil, fmt.Errorf("list peer policies: %w", err)
	}
	out := make([]store.PeerPolicy, 0, len(docs))
	for _, d := range docs {
		if d.Network == "" {
			continue
		}
		out = append(out, d.toModel())
	}
	return out, nil
}
