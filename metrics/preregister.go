package metrics

import "github.com/bsv-blockchain/arcade/models"

// Vec children (labeled series) only come into existence on their first
// observation. A series born between two scrapes starts at its burst value,
// and PromQL increase()/rate() can never count anything before a series'
// first sample — so every deploy or pod restart silently swallowed the first
// burst per label pair (a single block marking hundreds of txs MINED
// registered as ~0 on dashboards). Services call these helpers at
// construction time so every series they can emit is exported at 0 from the
// first scrape.

// PreRegisterStatusTransitions instantiates the StatusTransitionAge children
// for every lattice-valid (from → to) pair of the given target statuses.
// Each service passes only the transitions it actually observes to keep the
// exported-series count bounded per pod.
func PreRegisterStatusTransitions(tos ...models.Status) {
	for _, to := range tos {
		for _, from := range models.AllStatuses() {
			if to.CanTransitionFrom(from) {
				StatusTransitionAge.WithLabelValues(string(from), string(to))
			}
		}
	}
}

// PreRegisterTxSubmissions instantiates every {route, result} child of
// APITxsSubmittedTotal. The api-server calls this at construction time.
func PreRegisterTxSubmissions() {
	for _, route := range []string{"/tx", "/txs"} {
		for _, result := range []string{"new", "duplicate", "retry_rejected"} {
			APITxsSubmittedTotal.WithLabelValues(route, result)
		}
		APIFinalityRejectionsTotal.WithLabelValues(route)
	}
}

// bumpBuildOutcomes is the closed set of outcome labels handleMessage can
// stamp on BumpBuilderBuildDuration. Keep it in sync with the outcome
// assignments in services/bump_builder/builder.go and the doc comment on
// BumpBuilderBuildDuration. A failure alert that never fires because its
// outcome series was never born is exactly the blind spot pre-registration
// exists to close, so the failure outcomes matter most here. Unexported so no
// other package can mutate the shared slice.
var bumpBuildOutcomes = []string{
	// benign — the two build dispositions (finalized_complete_no_grace when
	// the expected-STUMP set was already complete on arrival and the grace
	// window was skipped, grace_waited otherwise) plus the non-build ones
	"finalized_complete_no_grace", "grace_waited",
	"short_circuited", "no_stumps", "context_canceled",
	// failures
	"parse_failed", "deferred_incomplete", "fetch_failed",
	"no_subtrees", "build_failed", "validation_failed", "store_failed",
}

// PreRegisterBumpOutcomes instantiates every outcome child of
// BumpBuilderBuildDuration so each series is exported from the first scrape.
// The bump-builder calls this at construction time. Without it a
// first-occurrence failure (e.g. the first validation_failed after a deploy)
// is invisible to increase()/rate() until its second sample — precisely when
// an operator most needs the alert to fire.
func PreRegisterBumpOutcomes() {
	for _, outcome := range bumpBuildOutcomes {
		BumpBuilderBuildDuration.WithLabelValues(outcome)
	}
}
