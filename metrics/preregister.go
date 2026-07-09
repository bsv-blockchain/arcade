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
	}
}
