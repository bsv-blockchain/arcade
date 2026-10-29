package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/bsv-blockchain/broadcaster/models"
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

	fam := gatherFamily(t, "bsv_broadcaster_status_transition_age_seconds")
	if fam == nil {
		t.Fatal("bsv_broadcaster_status_transition_age_seconds has no series after pre-registration")
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

	fam := gatherFamily(t, "bsv_broadcaster_api_txs_submitted_total")
	if fam == nil {
		t.Fatal("bsv_broadcaster_api_txs_submitted_total has no series after pre-registration")
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

	fam := gatherFamily(t, "bsv_broadcaster_bump_builder_build_duration_seconds")
	if fam == nil {
		t.Fatal("bsv_broadcaster_bump_builder_build_duration_seconds has no series after pre-registration")
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

func TestPreRegisterBlockStatusTransitionsCreatesEveryPairAtZero(t *testing.T) {
	PreRegisterBlockStatusTransitions()

	fam := gatherFamily(t, "bsv_broadcaster_block_status_transitions_total")
	if fam == nil {
		t.Fatal("bsv_broadcaster_block_status_transitions_total has no series after pre-registration")
	}

	found := map[[2]string]*dto.Metric{}
	for _, m := range fam.GetMetric() {
		found[[2]string{labelValue(m, "transition"), labelValue(m, "source")}] = m
	}

	// A same-height flip-flop (issue #339) is rare: the reactivated series
	// must already exist at zero, or increase() swallows the one burst that
	// matters.
	for _, pair := range blockStatusTransitions {
		m, ok := found[pair]
		if !ok {
			t.Errorf("child %v not pre-registered", pair)
			continue
		}
		if got := m.GetCounter().GetValue(); got != 0 {
			t.Errorf("child %v value = %v, want 0", pair, got)
		}
	}
	for _, want := range [][2]string{
		{BlockTransitionReactivated, BlockTransitionSourceReorgEvent},
		{BlockTransitionReactivated, BlockTransitionSourceTieScan},
		{BlockTransitionReactivated, BlockTransitionSourceFullScan},
		{BlockTransitionReactivated, BlockTransitionSourceReconciler},
		{BlockTransitionOrphaned, BlockTransitionSourceReorgEvent},
		{BlockTransitionOrphaned, BlockTransitionSourceTieScan},
		{BlockTransitionOrphaned, BlockTransitionSourceFullScan},
		// The reconciler re-orphans a canonical row the tracker reactivated
		// while its re-mine was failing part-way (keepPartialRemineQueued).
		{BlockTransitionOrphaned, BlockTransitionSourceReconciler},
	} {
		if _, ok := found[want]; !ok {
			t.Errorf("emitted pair %v missing from the pre-registered set", want)
		}
	}
	// Every transition × every source: the emitters' label constants are the
	// whole label space, so a pair missing here would be born mid-incident.
	for _, transition := range []string{BlockTransitionOrphaned, BlockTransitionReactivated} {
		for _, source := range []string{
			BlockTransitionSourceReorgEvent, BlockTransitionSourceTieScan,
			BlockTransitionSourceFullScan, BlockTransitionSourceReconciler,
		} {
			if _, ok := found[[2]string{transition, source}]; !ok {
				t.Errorf("pair {%s, %s} not pre-registered", transition, source)
			}
		}
	}
}

// TestPreRegisterRateLimitCreatesEveryChildAtZero mirrors the other
// pre-registration tests: every {tier, result} decision pair for the
// configured tiers, and every reservation result, must exist at zero after
// calling PreRegisterRateLimit -- so a first-occurrence quota rejection is
// countable by increase() immediately instead of being swallowed as the
// series' birth sample. The apikey-identification series ({none,*}
// decisions, cache results) are covered by PreRegisterAPIKey, not this
// helper -- see TestPreRegisterAPIKeyCreatesEveryChildAtZero.
func TestPreRegisterRateLimitCreatesEveryChildAtZero(t *testing.T) {
	tiers := []string{"free", "pro"}
	PreRegisterRateLimit(tiers)

	decisionsFam := gatherFamily(t, "bsv_broadcaster_ratelimit_decisions_total")
	if decisionsFam == nil {
		t.Fatal("bsv_broadcaster_ratelimit_decisions_total has no series after pre-registration")
	}
	foundDecisions := map[[2]string]*dto.Metric{}
	for _, m := range decisionsFam.GetMetric() {
		foundDecisions[[2]string{labelValue(m, "tier"), labelValue(m, "result")}] = m
	}
	for _, tier := range tiers {
		for _, result := range []string{"allowed", "quota", "concurrency", "batch_too_large", "unpayable"} {
			m, ok := foundDecisions[[2]string{tier, result}]
			if !ok {
				t.Errorf("child {tier=%s,result=%s} not pre-registered", tier, result)
				continue
			}
			if got := m.GetCounter().GetValue(); got != 0 {
				t.Errorf("child {tier=%s,result=%s} = %v, want 0", tier, result, got)
			}
		}
	}

	reservationsFam := gatherFamily(t, "bsv_broadcaster_ratelimit_reservations_total")
	if reservationsFam == nil {
		t.Fatal("bsv_broadcaster_ratelimit_reservations_total has no series after pre-registration")
	}
	foundReservations := map[string]*dto.Metric{}
	for _, m := range reservationsFam.GetMetric() {
		foundReservations[labelValue(m, "result")] = m
	}
	for _, result := range []string{"granted", "partial", "exhausted", "error", "canceled"} {
		m, ok := foundReservations[result]
		if !ok {
			t.Errorf("reservation result %q not pre-registered", result)
			continue
		}
		if got := m.GetCounter().GetValue(); got != 0 {
			t.Errorf("reservation result %q = %v, want 0", result, got)
		}
	}
}

// TestPreRegisterAPIKeyCreatesEveryChildAtZero checks the apikey-
// identification series PreRegisterAPIKey owns: the {none,*} decision pairs
// recorded before any tier is known, and every API-key cache result
// (including "canceled", the caller's own context ending the wait).
func TestPreRegisterAPIKeyCreatesEveryChildAtZero(t *testing.T) {
	PreRegisterAPIKey()

	decisionsFam := gatherFamily(t, "bsv_broadcaster_ratelimit_decisions_total")
	if decisionsFam == nil {
		t.Fatal("bsv_broadcaster_ratelimit_decisions_total has no series after pre-registration")
	}
	foundDecisions := map[[2]string]*dto.Metric{}
	for _, m := range decisionsFam.GetMetric() {
		foundDecisions[[2]string{labelValue(m, "tier"), labelValue(m, "result")}] = m
	}
	for _, result := range []string{"unauthorized", "auth_failures"} {
		m, ok := foundDecisions[[2]string{"none", result}]
		if !ok {
			t.Errorf("child {tier=none,result=%s} not pre-registered", result)
			continue
		}
		if got := m.GetCounter().GetValue(); got != 0 {
			t.Errorf("child {tier=none,result=%s} = %v, want 0", result, got)
		}
	}

	cacheFam := gatherFamily(t, "bsv_broadcaster_apikey_cache_total")
	if cacheFam == nil {
		t.Fatal("bsv_broadcaster_apikey_cache_total has no series after pre-registration")
	}
	foundCache := map[string]*dto.Metric{}
	for _, m := range cacheFam.GetMetric() {
		foundCache[labelValue(m, "result")] = m
	}
	for _, result := range []string{"hit", "miss", "negative", "failing", "stale", "error", "canceled"} {
		m, ok := foundCache[result]
		if !ok {
			t.Errorf("cache result %q not pre-registered", result)
			continue
		}
		if got := m.GetCounter().GetValue(); got != 0 {
			t.Errorf("cache result %q = %v, want 0", result, got)
		}
	}
}
