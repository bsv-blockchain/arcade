package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/bsv-blockchain/arcade/models"
)

// gatherFamily returns the MetricFamily with the given name from the default
// registry, or nil if no series for it exist yet.
func gatherFamily(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// labelValue extracts a label value from a metric, or "" if absent.
func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

func TestPreRegisterStatusTransitionsCreatesLatticeValidChildrenAtZero(t *testing.T) {
	PreRegisterStatusTransitions(models.StatusMined)

	fam := gatherFamily(t, "arcade_status_transition_age_seconds")
	if fam == nil {
		t.Fatal("arcade_status_transition_age_seconds has no series after pre-registration")
	}

	found := map[[2]string]*dto.Metric{}
	for _, m := range fam.GetMetric() {
		found[[2]string{labelValue(m, "from"), labelValue(m, "to")}] = m
	}

	// Every lattice-valid predecessor of MINED must exist as a child. Pairs
	// no other test observes must sit at zero — proving they were created by
	// pre-registration, not by an observation.
	for _, from := range []models.Status{
		models.StatusStumpProcessing, models.StatusPendingRetry,
		models.StatusSeenMultipleNodes, models.StatusReceived,
	} {
		m, ok := found[[2]string{string(from), string(models.StatusMined)}]
		if !ok {
			t.Errorf("child series {from=%s,to=MINED} not pre-registered", from)
			continue
		}
		if got := m.GetHistogram().GetSampleCount(); got != 0 {
			t.Errorf("child {from=%s,to=MINED} sample count = %d, want 0", from, got)
		}
	}

	// Lattice-disallowed pairs must NOT be fabricated.
	if _, ok := found[[2]string{string(models.StatusImmutable), string(models.StatusMined)}]; ok {
		t.Error("child {from=IMMUTABLE,to=MINED} was pre-registered but IMMUTABLE→MINED is lattice-disallowed")
	}
}

func TestPreRegisterTxSubmissionsCreatesAllRouteResultChildrenAtZero(t *testing.T) {
	PreRegisterTxSubmissions()

	fam := gatherFamily(t, "arcade_api_txs_submitted_total")
	if fam == nil {
		t.Fatal("arcade_api_txs_submitted_total has no series after pre-registration")
	}

	found := map[[2]string]*dto.Metric{}
	for _, m := range fam.GetMetric() {
		found[[2]string{labelValue(m, "route"), labelValue(m, "result")}] = m
	}

	for _, route := range []string{"/tx", "/txs"} {
		for _, result := range []string{"new", "duplicate", "retry_rejected"} {
			m, ok := found[[2]string{route, result}]
			if !ok {
				t.Errorf("child series {route=%s,result=%s} not pre-registered", route, result)
				continue
			}
			if got := m.GetCounter().GetValue(); got != 0 {
				t.Errorf("child {route=%s,result=%s} = %v, want 0", route, result, got)
			}
		}
	}
}

// TestBumpOutcomesIncludeGraceDispositions pins the outcome-label migration
// that landed with completeness-first grace handling: the flat `success`
// label split into the two benign build dispositions
// (`finalized_complete_no_grace` when the expected-STUMP set was already
// complete on arrival, `grace_waited` otherwise), and the failure label
// `incomplete_stumps` was renamed `deferred_incomplete`. The closed set must
// carry the new labels — and must NOT resurrect the retired ones, or the
// pre-registration guarantee (and the alert recipe in README.md) silently
// diverges from what builder.go actually stamps.
func TestBumpOutcomesIncludeGraceDispositions(t *testing.T) {
	got := make(map[string]bool, len(bumpBuildOutcomes))
	for _, o := range bumpBuildOutcomes {
		got[o] = true
	}
	for _, want := range []string{"finalized_complete_no_grace", "grace_waited", "deferred_incomplete"} {
		if !got[want] {
			t.Errorf("bumpBuildOutcomes missing %q", want)
		}
	}
	for _, retired := range []string{"success", "incomplete_stumps"} {
		if got[retired] {
			t.Errorf("bumpBuildOutcomes still contains retired label %q", retired)
		}
	}
}

func TestPreRegisterBumpOutcomesCreatesEveryOutcomeChildAtZero(t *testing.T) {
	PreRegisterBumpOutcomes()

	fam := gatherFamily(t, "arcade_bump_builder_build_duration_seconds")
	if fam == nil {
		t.Fatal("arcade_bump_builder_build_duration_seconds has no series after pre-registration")
	}

	found := map[string]*dto.Metric{}
	for _, m := range fam.GetMetric() {
		found[labelValue(m, "outcome")] = m
	}

	// Every outcome — the failure labels especially — must exist at zero so a
	// first-occurrence build failure is countable by increase() immediately.
	for _, outcome := range bumpBuildOutcomes {
		m, ok := found[outcome]
		if !ok {
			t.Errorf("outcome child %q not pre-registered", outcome)
			continue
		}
		if got := m.GetHistogram().GetSampleCount(); got != 0 {
			t.Errorf("outcome %q sample count = %d, want 0", outcome, got)
		}
	}
}
