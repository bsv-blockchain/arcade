package embedded

import (
	"testing"

	"github.com/bsv-blockchain/arcade/models"
)

func TestStatusPriority(t *testing.T) {
	tests := []struct {
		name     string
		status   models.Status
		expected int
	}{
		{"accepted is highest", models.StatusAcceptedByNetwork, 3},
		{"sent is middle", models.StatusSentToNetwork, 2},
		{"rejected is lowest non-zero", models.StatusRejected, 1},
		{"unknown is zero", models.StatusUnknown, 0},
		{"received is zero", models.StatusReceived, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := statusPriority(tt.status)
			if got != tt.expected {
				t.Errorf("statusPriority(%q) = %d, want %d", tt.status, got, tt.expected)
			}
		})
	}
}

func TestStatusPriority_AcceptedBeatsRejected(t *testing.T) {
	if statusPriority(models.StatusAcceptedByNetwork) <= statusPriority(models.StatusRejected) {
		t.Error("ACCEPTED_BY_NETWORK should have higher priority than REJECTED")
	}
}

func TestStatusPriority_AcceptedBeatsSent(t *testing.T) {
	if statusPriority(models.StatusAcceptedByNetwork) <= statusPriority(models.StatusSentToNetwork) {
		t.Error("ACCEPTED_BY_NETWORK should have higher priority than SENT_TO_NETWORK")
	}
}

func TestStatusPriority_SentBeatsRejected(t *testing.T) {
	if statusPriority(models.StatusSentToNetwork) <= statusPriority(models.StatusRejected) {
		t.Error("SENT_TO_NETWORK should have higher priority than REJECTED")
	}
}
