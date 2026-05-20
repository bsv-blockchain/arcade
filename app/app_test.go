package app

import "testing"

func TestModeNeedsChaintracks(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{"all", true},
		{"chaintracks", true},
		{"bump-builder", true},
		{"api-server", false},
		{"sse", false},
		{"propagation", false},
		{"p2p-client", false},
		{"watchdog", false},
		{"", false},
		{"unknown-mode", false},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			if got := modeNeedsChaintracks(tc.mode); got != tc.want {
				t.Errorf("modeNeedsChaintracks(%q) = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}
