package webhook

import (
	"testing"

	"github.com/bsv-blockchain/arcade/models"
)

// TestShouldDeliver_ReorgReanchorBypassesSameStatusDedup pins the issue
// #279 exception: a MINED event flagged as a reorg re-anchor is the one
// same-status transition that must be redelivered — the block hash and
// merkle path changed, and suppressing it would leave the receiver holding
// a proof anchored to an orphaned block. Every other same-status replay
// stays deduped.
func TestShouldDeliver_ReorgReanchorBypassesSameStatusDedup(t *testing.T) {
	minedSub := &models.Submission{LastDeliveredStatus: models.StatusMined}
	seenSub := &models.Submission{LastDeliveredStatus: models.StatusSeenOnNetwork, FullStatusUpdates: true}

	cases := []struct {
		name   string
		sub    *models.Submission
		status *models.TransactionStatus
		want   bool
	}{
		{
			name:   "reorg re-anchor bypasses MINED dedup",
			sub:    minedSub,
			status: &models.TransactionStatus{Status: models.StatusMined, ExtraInfo: models.ExtraInfoReorgReanchor},
			want:   true,
		},
		{
			name:   "plain MINED replay stays deduped",
			sub:    minedSub,
			status: &models.TransactionStatus{Status: models.StatusMined},
			want:   false,
		},
		{
			name:   "MINED with unrelated extraInfo stays deduped",
			sub:    minedSub,
			status: &models.TransactionStatus{Status: models.StatusMined, ExtraInfo: "something else"},
			want:   false,
		},
		{
			name:   "same-status SEEN revert replay stays deduped",
			sub:    seenSub,
			status: &models.TransactionStatus{Status: models.StatusSeenOnNetwork, ExtraInfo: models.ExtraInfoReorgUnmined},
			want:   false,
		},
		{
			name: "reorg revert delivers as a normal transition",
			sub:  minedSub,
			status: &models.TransactionStatus{
				Status: models.StatusSeenOnNetwork, ExtraInfo: models.ExtraInfoReorgUnmined,
			},
			// FullStatusUpdates=false and SEEN is not terminal — the revert
			// rides the store/API state, not a webhook push, unless the
			// subscriber asked for full updates.
			want: false,
		},
		{
			name: "reorg revert delivers with FullStatusUpdates",
			sub:  &models.Submission{LastDeliveredStatus: models.StatusMined, FullStatusUpdates: true},
			status: &models.TransactionStatus{
				Status: models.StatusSeenOnNetwork, ExtraInfo: models.ExtraInfoReorgUnmined,
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldDeliver(tc.sub, tc.status); got != tc.want {
				t.Fatalf("shouldDeliver = %v, want %v", got, tc.want)
			}
		})
	}
}
