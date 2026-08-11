package api_server

// familyPartitionKeys returns the Kafka partition key for each tx of a
// submitted batch. Txs connected through in-batch spends — tx i lists an
// input whose source txid is also in the batch — form a dependency family
// and share one key: the lexicographically smallest txid of the family.
// Isolated txs key by their own txid (keys[i] == txids[i]), matching the
// pre-#295 behavior and the single-submit path.
//
// Same key ⇒ same partition ⇒ per-partition Kafka order delivers every
// parent's admit to its dispatcher before its children's, which is what
// lets the dep-aware dispatcher keep honoring Teranode's "no parent and
// child in one /txs batch" contract without a cross-partition dep index.
// The root is the smallest member (not the first submitted) so the key is
// order-independent: a client retrying the same family with the batch
// shuffled maps to the same partition as the original messages.
//
// Only same-submission edges join a family. A parent submitted in an
// earlier HTTP request may land on a different partition than its child;
// that residual race is absorbed by the propagator's missing-parent
// safety net rather than by keying (see PropagationConfig.Partitions).
//
// inputs[i] holds the source txids of tx i's inputs (collectInputTXIDs).
// Empty strings and self-references are ignored, mirroring the dispatcher's
// admission guards. O((N+E)·α(N)) for N txs and E total inputs.
func familyPartitionKeys(txids []string, inputs [][]string) []string {
	n := len(txids)
	if n == 0 {
		return nil
	}

	index := make(map[string]int, n)
	for i, id := range txids {
		// First occurrence wins; a duplicate txid (store==nil intake path)
		// unions with its first occurrence below, so both share a key.
		if _, ok := index[id]; !ok {
			index[id] = i
		}
	}

	parent := make([]int, n)
	size := make([]int, n)
	for i := range parent {
		parent[i] = i
		size[i] = 1
	}
	find := func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]] // path halving
			i = parent[i]
		}
		return i
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra == rb {
			return
		}
		if size[ra] < size[rb] {
			ra, rb = rb, ra
		}
		parent[rb] = ra
		size[ra] += size[rb]
	}

	for i, ins := range inputs {
		for _, src := range ins {
			if src == "" || src == txids[i] {
				continue
			}
			if j, ok := index[src]; ok {
				union(i, j)
			}
		}
	}
	// Duplicate txids: union every later occurrence with the first so the
	// whole family (including children referencing the txid) shares a key.
	for i, id := range txids {
		if first := index[id]; first != i {
			union(i, first)
		}
	}

	// Smallest member txid per component, then one key per tx.
	keyOf := make(map[int]string, n)
	for i, id := range txids {
		r := find(i)
		if cur, ok := keyOf[r]; !ok || id < cur {
			keyOf[r] = id
		}
	}
	keys := make([]string, n)
	for i := range txids {
		keys[i] = keyOf[find(i)]
	}
	return keys
}
