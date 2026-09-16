//go:build mongodb

// Integration tests for the MongoDB backend. They need a reachable mongod:
//
//	podman run -d --name arcade-mongo -p 27017:27017 docker.io/library/mongo:7
//	go test -tags=mongodb ./store/mongodb/...
//
// ARCADE_MONGODB_URI overrides the default mongodb://127.0.0.1:27017. When no
// server answers, every test skips rather than fails, unless
// ARCADE_MONGODB_REQUIRED is set — which .github/workflows/mongodb-store.yml
// does after provisioning a mongo service container, so CI can never pass
// by skipping.
package mongodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/bsv-blockchain/arcade/config"
)

var (
	sharedClient *mongo.Client
	sharedErr    error
	sharedURI    string
)

func TestMain(m *testing.M) {
	uri := os.Getenv("ARCADE_MONGODB_URI")
	if uri == "" {
		// 127.0.0.1 rather than localhost: rootless podman forwards the IPv4 port
		// only, and Go dials ::1 first, which the forwarder resets.
		uri = "mongodb://127.0.0.1:27017"
	}
	sharedURI = uri
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
		if os.Getenv("ARCADE_MONGODB_REQUIRED") != "" {
			// CI provisions a server and must fail loudly, never skip.
			t.Fatalf("mongodb required but unavailable at ARCADE_MONGODB_URI: %v", sharedErr)
		}
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
		collStumpManifests:  {"_id_", idxStumpManifests},
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

// A non-primary read preference is legal configuration that costs this
// backend's guards their meaning, so New warns and carries on rather than
// refusing to start. The warning has to actually reach the log, and has to
// name which preference tripped it: it is the only signal an operator gets
// before the symptoms (lost status transitions, deleted blobs) show up under
// concurrency.
func TestNew_WarnsOnNonPrimaryReadPreference(t *testing.T) {
	if sharedClient == nil {
		if os.Getenv("ARCADE_MONGODB_REQUIRED") != "" {
			// CI provisions a server and must fail loudly, never skip.
			t.Fatalf("mongodb required but unavailable at ARCADE_MONGODB_URI: %v", sharedErr)
		}
		t.Skipf("mongodb unavailable (set ARCADE_MONGODB_URI), skipping: %v", sharedErr)
	}
	// A query string needs a path separator after the host: both mongodb://
	// and mongodb+srv:// carry their own "//", so look past the scheme.
	withOpt := func(opt string) string {
		if strings.Contains(sharedURI, "?") {
			return sharedURI + "&" + opt
		}
		base := sharedURI
		if _, hostAndRest, ok := strings.Cut(base, "://"); ok && !strings.Contains(hostAndRest, "/") {
			base += "/"
		}
		return base + "?" + opt
	}

	for _, tc := range []struct {
		name     string
		uri      string
		wantMode string // empty: no warning expected
	}{
		{"secondaryPreferred warns", withOpt("readPreference=secondaryPreferred"), "secondaryPreferred"},
		{"secondary warns", withOpt("readPreference=secondary"), "secondary"},
		{"primary is silent", withOpt("readPreference=primary"), ""},
		{"unset is silent", sharedURI, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.uri == sharedURI && strings.Contains(strings.ToLower(sharedURI), "readpreference") {
				t.Skip("ARCADE_MONGODB_URI already sets a read preference; nothing to assert about the unset case")
			}
			core, logs := observer.New(zapcore.WarnLevel)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s, err := New(ctx, config.Mongo{URI: tc.uri, Database: "arcade_readpref_probe"}, zap.New(core))
			if err != nil {
				t.Fatalf("New must not refuse a legal read preference: %v", err)
			}
			defer func() { _ = s.Close() }()

			warnings := logs.FilterMessageSnippet("non-primary read preference").All()
			if tc.wantMode == "" {
				if len(warnings) != 0 {
					t.Fatalf("expected no warning, got %d: %+v", len(warnings), warnings)
				}
				return
			}
			if len(warnings) != 1 {
				t.Fatalf("expected exactly one startup warning, got %d", len(warnings))
			}
			// The field, not just the message: an operator needs to know WHICH
			// preference tripped it, and a message-only assertion passes even
			// when the field has been dropped.
			got, ok := warnings[0].ContextMap()["read_preference"]
			if !ok {
				t.Fatalf("warning must carry the read_preference field, got %+v", warnings[0].ContextMap())
			}
			if got != tc.wantMode {
				t.Fatalf("read_preference = %v, want %q", got, tc.wantMode)
			}
		})
	}
}
