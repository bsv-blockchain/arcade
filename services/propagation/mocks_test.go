package propagation

import "github.com/bsv-blockchain/arcade/store"

// mockStore satisfies the store.Store interface by embedding it (so
// every unimplemented method panics on call, surfacing accidental
// dependencies in tests). Tests that need specific behavior compose
// over this by embedding *mockStore and overriding the relevant
// methods — see broadcastTestStore and pipelineTestStore.
type mockStore struct {
	store.Store
}

func newMockStore() *mockStore { return &mockStore{} }
