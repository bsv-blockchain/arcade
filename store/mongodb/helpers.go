package mongodb

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/bsv-blockchain/arcade/store"
)

// upsertAttempts bounds the duplicate-key retry of an upsert: the loser of an
// insert race for a brand-new _id retries once or twice and updates the
// document the winner inserted; anything persistent is a real conflict.
const upsertAttempts = 3

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

// msNow is the millisecond-aligned write timestamp, in UTC so that a value
// this package hands back to a caller compares equal (==, not just Equal) to
// the same instant read back from the server, which always decodes as UTC.
func msNow() time.Time { return msTrunc(time.Now().UTC()) }

// forEach runs fn(i) for every i in [0, n) with at most store.BatchConcurrency
// calls in flight — the same operator knob the shared batch helpers honour.
// The first error is returned after every started call has finished; once
// ctx is done, or any call has failed, no further calls start.
//
// Stopping at the first failure is what keeps a per-row loop from turning an
// outage into a marathon: with the server gone, a million-row block would
// otherwise pay op_timeout_ms for every remaining row before returning, and
// the rows it did write meanwhile are exactly as partial as they would be
// after one failure. Callers get the same (partial results, first error)
// pair either way, just promptly.
func forEach(ctx context.Context, n int, fn func(i int) error) error {
	sem := make(chan struct{}, store.BatchConcurrency())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	record := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firstErr != nil
	}
	for i := 0; i < n; i++ {
		// Checked explicitly: a select with both a free slot and a done ctx
		// ready picks at random, which is how a cancelled loop still started
		// most of its calls.
		if err := ctx.Err(); err != nil {
			record(err)
			break
		}
		if failed() {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			record(ctx.Err())
			wg.Wait()
			return firstErr
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(i); err != nil {
				record(err)
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

// withDupKeyRetry runs an upsert and retries it when it lost the insert race
// for a brand-new _id (E11000): the retry finds the winner's document and
// updates it, which is what the caller meant all along.
func withDupKeyRetry(op func() error) error {
	var err error
	for attempt := 0; attempt < upsertAttempts; attempt++ {
		if err = op(); err == nil || !mongo.IsDuplicateKeyError(err) {
			return err
		}
	}
	return err
}

// awaitWithin runs fn on its own goroutine and returns its result, or ctx's
// error if the deadline passes first. It exists for calls that cannot take a
// context themselves — the SRV/TXT lookups behind a mongodb+srv:// URI are
// the case in this package — so that boot can stop waiting on them.
//
// What it bounds is the caller's wait, not fn: an abandoned fn keeps running
// until it returns on its own, and its result is discarded. For a DNS lookup
// that is the OS resolver's own timeout (attempts × per-query timeout, tens of
// seconds), which is what makes abandoning it acceptable — the goroutine is
// leaked for a bounded time, not forever.
func awaitWithin[T any](ctx context.Context, fn func() T) (T, error) {
	done := make(chan T, 1)
	go func() { done <- fn() }()
	select {
	case v := <-done:
		return v, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
