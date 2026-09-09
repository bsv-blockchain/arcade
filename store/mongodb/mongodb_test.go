//go:build mongodb

// Integration tests for the MongoDB backend. They need a reachable mongod:
//
//	podman run -d --name arcade-mongo -p 27017:27017 docker.io/library/mongo:7
//	go test -tags=mongodb ./store/mongodb/...
//
// ARCADE_MONGODB_URI overrides the default mongodb://127.0.0.1:27017. When no
// server answers, every test skips rather than fails — the suite is gated by
// the build tag, and no CI workflow passes it today (see
// store/pebble/conformance_test.go for the rationale).
package mongodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/bsv-blockchain/arcade/config"
)

var (
	sharedClient *mongo.Client
	sharedErr    error
)

func TestMain(m *testing.M) {
	uri := os.Getenv("ARCADE_MONGODB_URI")
	if uri == "" {
		// 127.0.0.1 rather than localhost: rootless podman forwards the IPv4 port
		// only, and Go dials ::1 first, which the forwarder resets.
		uri = "mongodb://127.0.0.1:27017"
	}
	sharedClient, sharedErr = connectForTests(uri)
	code := m.Run()
	if sharedClient != nil {
		_ = sharedClient.Disconnect(context.Background())
	}
	os.Exit(code)
}

func connectForTests(uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).
		SetConnectTimeout(3 * time.Second).SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

// newTestStore binds a Store to a fresh, uniquely named database and drops it
// on cleanup. Per-test databases (rather than a shared one plus truncation)
// make the suite parallel-safe and take the GridFS collections with them.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	if sharedClient == nil {
		t.Skipf("mongodb unavailable (set ARCADE_MONGODB_URI), skipping: %v", sharedErr)
	}
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	dbName := fmt.Sprintf("arcade_test_%s", hex.EncodeToString(b[:]))
	s := newWithClient(sharedClient, dbName, config.Mongo{
		OpTimeoutMs: 5000, QueryTimeoutMs: 15000, BatchSize: 100,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		_ = sharedClient.Database(dbName).Drop(dctx)
	})
	return s
}

// EnsureIndexes must be safe to call on every boot: a second call against
// an already-provisioned database is a no-op, and the index set is exactly
// the plan (no stray defaults).
func TestEnsureIndexes_Idempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.EnsureIndexes(ctx); err != nil {
		t.Fatalf("second EnsureIndexes: %v", err)
	}
	want := map[string][]string{
		collTransactions:    {"_id_", idxTxStatusHeight, idxTxStatusTS, idxTxBlockHash, idxTxTimestamp, idxTxRetryReady},
		collSubmissions:     {"_id_", idxSubTxIDToken, idxSubTokenTxID, idxSubRetryReady},
		collBlockProcessing: {"_id_", idxBPHeightDesc, idxBPStatusHeight, idxBPStaleSeen, idxBPOrphaned},
		collLeases:          {"_id_", idxLeaseExpires},
		"bumps.files":       {"_id_", idxGridFSFiles, idxBumpsBlockHash},
		"bumps.chunks":      {"_id_", idxGridFSChunks},
		"stumps.files":      {"_id_", idxGridFSFiles, idxStumpsBlockHash},
	}
	for coll, names := range want {
		got := indexNames(t, s.db.Collection(coll))
		for _, n := range names {
			if !got[n] {
				t.Errorf("%s: index %s missing; have %v", coll, n, got)
			}
		}
		if len(got) != len(names) {
			t.Errorf("%s: expected %d indexes, have %v", coll, len(names), got)
		}
	}
}

func indexNames(t *testing.T, coll *mongo.Collection) map[string]bool {
	t.Helper()
	cur, err := coll.Indexes().List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var specs []struct {
		Name string `bson:"name"`
	}
	if err := cur.All(context.Background(), &specs); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]bool, len(specs))
	for _, sp := range specs {
		out[sp.Name] = true
	}
	return out
}
