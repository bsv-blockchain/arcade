package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// kv and doc are terse, keyed constructors for bson.D literals. Keyed
// bson.E literals keep go vet's composites check quiet and read better than
// the {Key:, Value:} spelling repeated hundreds of times.
func kv(key string, value any) bson.E { return bson.E{Key: key, Value: value} }

// doc always returns a non-nil document: a nil bson.D marshals as BSON null,
// which the server rejects as a filter or update.
func doc(elems ...bson.E) bson.D {
	if elems == nil {
		return bson.D{}
	}
	return bson.D(elems)
}

// idFilter matches one document by primary key.
func idFilter(id string) bson.D { return doc(kv(fID, id)) }

// closeCursor releases a server cursor even when the caller's ctx is already
// cancelled — the usual reason iteration stopped early. Without a live
// context the killCursors command would be skipped and the server would hold
// the cursor until its idle timeout.
func closeCursor(ctx context.Context, cur *mongo.Cursor) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	_ = cur.Close(cctx)
}

// dedupe drops empty and repeated ids, preserving first-seen order.
func dedupe(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// chunks splits ids into slices of at most n.
func chunks(ids []string, n int) [][]string {
	if n <= 0 {
		n = defaultBatchSize
	}
	out := make([][]string, 0, len(ids)/n+1)
	for start := 0; start < len(ids); start += n {
		out = append(out, ids[start:min(start+n, len(ids))])
	}
	return out
}

// msNow is the current time at BSON resolution.
func msNow() time.Time { return msTrunc(time.Now()) }
