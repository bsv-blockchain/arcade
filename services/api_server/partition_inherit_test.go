package api_server

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/models"
	"github.com/bsv-blockchain/broadcaster/store"
)

// keyLookupStore answers LookupPartitionKeys from a canned map and records
// every lookup so the input cap can be asserted.
type keyLookupStore struct {
	store.Store

	rows  map[string]store.ParentKey
	asked [][]string
	err   error
}

func (f *keyLookupStore) LookupPartitionKeys(_ context.Context, txids []string) (map[string]store.ParentKey, error) {
	f.asked = append(f.asked, append([]string(nil), txids...))
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]store.ParentKey{}
	for _, id := range txids {
		if r, ok := f.rows[id]; ok {
			out[id] = r
		}
	}
	return out, nil
}

func keyingServer(st store.Store, maxInputs int) *Server {
	return &Server{
		cfg:    &config.Config{Propagation: config.PropagationConfig{ParentKeyLookupMaxInputs: maxInputs}},
		store:  st,
		logger: zap.NewNop(),
	}
}

func TestPartitionKeyForSubmit(t *testing.T) {
	const child = "cc"
	cases := []struct {
		name   string
		rows   map[string]store.ParentKey
		inputs []string
		max    int
		err    error
		want   string
	}{
		{name: "no parents keys by own txid", inputs: nil, max: 8, want: child},
		{name: "unknown parent keys by own txid", inputs: []string{"p1"}, max: 8, want: child},
		{
			name:   "in-flight parent lends its key",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusReceived, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   "root",
		},
		{
			name:   "parked parent lends its key",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusPendingRetry, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   "root",
		},
		{
			name:   "accepted parent does not",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusAcceptedByNetwork, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   child,
		},
		{
			name:   "rejected parent does not",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusRejected, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    8,
			want:   child,
		},
		{
			name:   "parent written before keys were persisted does not",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusReceived}},
			inputs: []string{"p1"},
			max:    8,
			want:   child,
		},
		{
			name: "first pending parent in input order wins",
			rows: map[string]store.ParentKey{
				"p1": {Status: models.StatusMined, PartitionKey: "old"},
				"p2": {Status: models.StatusReceived, PartitionKey: "k2"},
				"p3": {Status: models.StatusReceived, PartitionKey: "k3"},
			},
			inputs: []string{"p1", "p2", "p3"},
			max:    8,
			want:   "k2",
		},
		{
			name: "own row keeps its earlier key",
			rows: map[string]store.ParentKey{
				child: {Status: models.StatusRejected, PartitionKey: "mine"},
				"p1":  {Status: models.StatusReceived, PartitionKey: "root"},
			},
			inputs: []string{"p1"},
			max:    8,
			want:   "mine",
		},
		{name: "lookup error keys by own txid", inputs: []string{"p1"}, max: 8, err: errors.New("down"), want: child},
		{
			name:   "disabled keys by own txid",
			rows:   map[string]store.ParentKey{"p1": {Status: models.StatusReceived, PartitionKey: "root"}},
			inputs: []string{"p1"},
			max:    0,
			want:   child,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &keyLookupStore{rows: tc.rows, err: tc.err}
			got := keyingServer(st, tc.max).partitionKeyForSubmit(context.Background(), child, tc.inputs)
			if got != tc.want {
				t.Fatalf("key = %q, want %q", got, tc.want)
			}
			if tc.max == 0 && len(st.asked) != 0 {
				t.Fatalf("disabled lookup still asked the store: %v", st.asked)
			}
		})
	}
}

func TestPartitionKeyForSubmit_CapsLookups(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{"p9": {Status: models.StatusReceived, PartitionKey: "late"}}}
	inputs := []string{"p1", "p1", "", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"}
	got := keyingServer(st, 3).partitionKeyForSubmit(context.Background(), "cc", inputs)
	if got != "cc" {
		t.Fatalf("key = %q, want own txid (p9 is beyond the cap)", got)
	}
	if len(st.asked) != 1 || len(st.asked[0]) != 4 { // p1, p2, p3 + own txid
		t.Fatalf("asked = %v, want one lookup of 3 distinct parents plus the txid", st.asked)
	}
}

func TestInheritExternalParentKeys(t *testing.T) {
	st := &keyLookupStore{rows: map[string]store.ParentKey{
		"P": {Status: models.StatusReceived, PartitionKey: "kP"},
		"Q": {Status: models.StatusSeenOnNetwork, PartitionKey: "kQ"},
	}}
	keyByTxid := map[string]string{"c1": "c1", "c2": "c1", "d": "d", "e": "e"}
	inputsByTxid := map[string][]string{
		"c1": {"P"},  // external, in flight: the whole family follows P
		"c2": {"c1"}, // in-batch parent: same family as c1
		"d":  {"Q"},  // external but already on the network: keep own key
		"e":  {},     // nothing to inherit
	}
	keyingServer(st, 8).inheritExternalParentKeys(context.Background(), keyByTxid, inputsByTxid)
	want := map[string]string{"c1": "kP", "c2": "kP", "d": "d", "e": "e"}
	for txid, k := range want {
		if keyByTxid[txid] != k {
			t.Errorf("%s: key = %q, want %q", txid, keyByTxid[txid], k)
		}
	}
	if len(st.asked) != 1 {
		t.Fatalf("expected exactly one store lookup for the batch, got %d", len(st.asked))
	}
}

func TestInheritExternalParentKeys_NoExternalParentsSkipsStore(t *testing.T) {
	st := &keyLookupStore{}
	keyByTxid := map[string]string{"a": "a", "b": "a"}
	keyingServer(st, 8).inheritExternalParentKeys(context.Background(), keyByTxid, map[string][]string{"a": {}, "b": {"a"}})
	if len(st.asked) != 0 {
		t.Fatalf("store consulted with no external parents: %v", st.asked)
	}
}
