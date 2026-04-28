package teranode

import (
	"sync"
	"testing"
)

func TestClient_AddEndpoints_Dedup(t *testing.T) {
	cases := []struct {
		name       string
		seed       []string
		add        []string
		wantAdded  int
		wantTotal  int
		wantInList []string
	}{
		{
			name:       "novel url is added",
			seed:       []string{"https://a.example"},
			add:        []string{"https://b.example"},
			wantAdded:  1,
			wantTotal:  2,
			wantInList: []string{"https://a.example", "https://b.example"},
		},
		{
			name:       "exact duplicate ignored",
			seed:       []string{"https://a.example"},
			add:        []string{"https://a.example"},
			wantAdded:  0,
			wantTotal:  1,
			wantInList: []string{"https://a.example"},
		},
		{
			name:       "trailing slash variant deduplicated",
			seed:       []string{"https://a.example"},
			add:        []string{"https://a.example/"},
			wantAdded:  0,
			wantTotal:  1,
			wantInList: []string{"https://a.example"},
		},
		{
			name:       "statically configured url later announced by peer is skipped",
			seed:       []string{"https://static.example/"},
			add:        []string{"https://static.example"},
			wantAdded:  0,
			wantTotal:  1,
			wantInList: []string{"https://static.example"},
		},
		{
			name:       "two adds, one duplicate one novel",
			seed:       []string{"https://a.example"},
			add:        []string{"https://a.example", "https://c.example"},
			wantAdded:  1,
			wantTotal:  2,
			wantInList: []string{"https://a.example", "https://c.example"},
		},
		{
			name:       "empty string is ignored",
			seed:       []string{"https://a.example"},
			add:        []string{""},
			wantAdded:  0,
			wantTotal:  1,
			wantInList: []string{"https://a.example"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(tc.seed, "", HealthConfig{})
			got := c.AddEndpoints(tc.add)
			if got != tc.wantAdded {
				t.Errorf("AddEndpoints returned %d, want %d", got, tc.wantAdded)
			}
			eps := c.GetEndpoints()
			if len(eps) != tc.wantTotal {
				t.Errorf("endpoint count = %d, want %d (got %v)", len(eps), tc.wantTotal, eps)
			}
			for _, want := range tc.wantInList {
				found := false
				for _, ep := range eps {
					if ep == want {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected endpoint %q not in list %v", want, eps)
				}
			}
		})
	}
}

func TestClient_SeedDedup(t *testing.T) {
	// Static config can itself contain duplicates; NewClient should dedupe on
	// the way in so AddEndpoints's first call doesn't see phantoms.
	c := NewClient([]string{"https://a.example", "https://a.example/", "https://b.example"}, "", HealthConfig{})
	eps := c.GetEndpoints()
	if len(eps) != 2 {
		t.Fatalf("seed dedup failed: got %v", eps)
	}
}

func TestClient_GetEndpoints_SnapshotIndependence(t *testing.T) {
	c := NewClient([]string{"https://a.example"}, "", HealthConfig{})
	snap := c.GetEndpoints()
	c.AddEndpoints([]string{"https://b.example"})
	if len(snap) != 1 {
		t.Fatalf("snapshot was mutated by subsequent AddEndpoints: %v", snap)
	}
}

// TestClient_EndpointsConcurrency spawns interleaved writers and readers so
// -race flags any unsynchronized access. The counts don't need to be exact;
// the test fails if the race detector trips or the data-structure corrupts.
func TestClient_EndpointsConcurrency(t *testing.T) {
	c := NewClient([]string{"https://seed.example"}, "", HealthConfig{})

	const writers = 8
	const readers = 8
	const perWorker = 100

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				url := "https://w" + string(rune('A'+w)) + "-" + string(rune('a'+i%26)) + ".example"
				c.AddEndpoints([]string{url})
			}
		}()
	}
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				eps := c.GetEndpoints()
				for range eps {
					// just iterate
				}
			}
		}()
	}
	wg.Wait()

	eps := c.GetEndpoints()
	if len(eps) == 0 {
		t.Fatal("expected at least the seed endpoint")
	}
}
