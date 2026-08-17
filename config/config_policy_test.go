package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

// loadValidatorDefaults exercises the real setDefaults + env wiring (minus the
// config file and validate step) so the /policy config surface is tested the
// way Load actually resolves it.
func loadValidatorDefaults(t *testing.T) ValidatorConfig {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetEnvPrefix("ARCADE")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	setDefaults()

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return cfg.Validator
}

func TestPolicyConfigDefaults(t *testing.T) {
	v := loadValidatorDefaults(t)

	// min_fee_per_kb and the two size limits default to 0, the
	// "not operator-configured" sentinel: intake and /policy then track what
	// the network will accept, falling back to DefaultValidator* until a peer
	// is heard (issue #212). A non-zero default here would read as an operator
	// override and pin the value, disabling tracking for everyone.
	if v.MinFeePerKB != 0 {
		t.Errorf("MinFeePerKB default = %d, want 0", v.MinFeePerKB)
	}
	if v.MaxTxSizePolicy != 0 {
		t.Errorf("MaxTxSizePolicy default = %d, want 0 (track the network)", v.MaxTxSizePolicy)
	}
	if v.MaxScriptSizePolicy != 0 {
		t.Errorf("MaxScriptSizePolicy default = %d, want 0 (track the network)", v.MaxScriptSizePolicy)
	}
	// The sigop cap is the exception: 0 already means "unlimited" here, so it
	// cannot double as an unset sentinel and is never network-tracked.
	if v.MaxTxSigopsCountsPolicy != 0 {
		t.Errorf("MaxTxSigopsCountsPolicy default = %d, want 0", v.MaxTxSigopsCountsPolicy)
	}
	if !v.StandardFormatSupported {
		t.Errorf("StandardFormatSupported default = false, want true")
	}
	if v.ObservedFeeTTLMs != DefaultValidatorObservedFeeTTLMs {
		t.Errorf("ObservedFeeTTLMs default = %d, want %d", v.ObservedFeeTTLMs, DefaultValidatorObservedFeeTTLMs)
	}
	if v.ObservedFeeRefreshMs != DefaultValidatorObservedFeeRefreshMs {
		t.Errorf("ObservedFeeRefreshMs default = %d, want %d", v.ObservedFeeRefreshMs, DefaultValidatorObservedFeeRefreshMs)
	}
}

func TestPolicyConfigEnvOverrides(t *testing.T) {
	t.Setenv("ARCADE_VALIDATOR_MIN_FEE_PER_KB", "250")
	t.Setenv("ARCADE_VALIDATOR_MAX_TX_SIZE_POLICY", "7777")
	t.Setenv("ARCADE_VALIDATOR_MAX_SCRIPT_SIZE_POLICY", "8888")
	t.Setenv("ARCADE_VALIDATOR_MAX_TX_SIGOPS_COUNTS_POLICY", "99")
	t.Setenv("ARCADE_VALIDATOR_STANDARD_FORMAT_SUPPORTED", "false")
	t.Setenv("ARCADE_VALIDATOR_OBSERVED_FEE_TTL_MS", "60000")
	t.Setenv("ARCADE_VALIDATOR_OBSERVED_FEE_REFRESH_MS", "5000")

	v := loadValidatorDefaults(t)

	if v.MinFeePerKB != 250 {
		t.Errorf("MinFeePerKB = %d, want 250", v.MinFeePerKB)
	}
	if v.MaxTxSizePolicy != 7777 {
		t.Errorf("MaxTxSizePolicy = %d, want 7777", v.MaxTxSizePolicy)
	}
	if v.MaxScriptSizePolicy != 8888 {
		t.Errorf("MaxScriptSizePolicy = %d, want 8888", v.MaxScriptSizePolicy)
	}
	if v.MaxTxSigopsCountsPolicy != 99 {
		t.Errorf("MaxTxSigopsCountsPolicy = %d, want 99", v.MaxTxSigopsCountsPolicy)
	}
	if v.StandardFormatSupported {
		t.Errorf("StandardFormatSupported = true, want false (env override)")
	}
	if v.ObservedFeeTTLMs != 60000 {
		t.Errorf("ObservedFeeTTLMs = %d, want 60000", v.ObservedFeeTTLMs)
	}
	if v.ObservedFeeRefreshMs != 5000 {
		t.Errorf("ObservedFeeRefreshMs = %d, want 5000", v.ObservedFeeRefreshMs)
	}
}
