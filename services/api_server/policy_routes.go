package api_server

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/models"
)

// miningFeeBytesBasis is the byte basis arcade advertises fee rates against:
// satoshis per 1000 bytes (== satoshis/kB), matching the node_status
// FeePolicy.MiningFee convention.
const miningFeeBytesBasis = 1000

// arcUnlimitedSigops is the value ARC uses in maxtxsigopscountspolicy to mean
// "unlimited" (max uint32). Arcade/teranode represent unlimited internally as
// 0; the /policy response maps 0 to this so ARC clients don't read it as
// "zero sigops allowed".
const arcUnlimitedSigops = 4294967295

// arcSigops maps arcade's internal sigop cap to the ARC wire convention: an
// internal 0 (unlimited, BSV post-Genesis) becomes ARC's unlimited sentinel; a
// real cap is reported verbatim.
func arcSigops(internal int64) int64 {
	if internal == 0 {
		return arcUnlimitedSigops
	}
	return internal
}

// firstNonZero returns v, or fallback when v is 0.
func firstNonZero(v, fallback uint64) uint64 {
	if v == 0 {
		return fallback
	}
	return v
}

// handlePolicy serves the ARC-compatible GET /policy document (issue #212).
// It reports arcade's effective transaction policy: the size/sigop limits and
// mining fee floor the validator actually enforces, plus the standard-format
// flag from config. The mining fee is read straight off the validator
// (MinFeePerKB), which the fee refresher keeps in step with the lowest fee the
// network will accept — so /policy always advertises exactly what intake
// enforces. Read-only; always returns 200.
func (s *Server) handlePolicy(c *gin.Context) {
	pol := models.Policy{
		StandardFormatSupported: s.cfg.Validator.StandardFormatSupported,
		MiningFee:               s.resolveMiningFee(),
	}

	// Size/sigop limits: prefer the validator's resolved values so /policy
	// advertises exactly what intake enforces. Fall back to config when the
	// validator is unset (struct-literal test servers), and to the canonical
	// teranode defaults when the config value is 0, so the document is never
	// meaningless (e.g. maxtxsizepolicy: 0).
	if s.validator != nil {
		pol.MaxTxSizePolicy = uint64(s.validator.MaxTxSizePolicy())         //nolint:gosec // validator returns a non-negative byte size
		pol.MaxScriptSizePolicy = uint64(s.validator.MaxScriptSizePolicy()) //nolint:gosec // validator returns a non-negative byte size
		pol.MaxTxSigopsCountsPolicy = arcSigops(s.validator.MaxTxSigopsCountsPolicy())
	} else {
		pol.MaxTxSizePolicy = firstNonZero(s.cfg.Validator.MaxTxSizePolicy, config.DefaultValidatorMaxTxSizePolicy)
		pol.MaxScriptSizePolicy = firstNonZero(s.cfg.Validator.MaxScriptSizePolicy, config.DefaultValidatorMaxScriptSizePolicy)
		pol.MaxTxSigopsCountsPolicy = arcSigops(s.cfg.Validator.MaxTxSigopsCountsPolicy)
	}

	c.JSON(http.StatusOK, models.PolicyResponse{
		Policy:    pol,
		Timestamp: time.Now().UTC(),
	})
}

// resolveMiningFee reports the fee floor the validator currently enforces. When
// the validator is present (production), that is authoritative — the refresher
// keeps it equal to the network minimum when the operator hasn't pinned a fee.
// The config fallback covers struct-literal test servers with no validator.
func (s *Server) resolveMiningFee() models.FeeAmount {
	if s.validator != nil {
		return models.FeeAmount{Satoshis: s.validator.MinFeePerKB(), Bytes: miningFeeBytesBasis}
	}
	if s.cfg.Validator.AcceptZeroFee {
		return models.FeeAmount{Satoshis: 0, Bytes: miningFeeBytesBasis}
	}
	if s.cfg.Validator.MinFeePerKB != 0 {
		return models.FeeAmount{Satoshis: s.cfg.Validator.MinFeePerKB, Bytes: miningFeeBytesBasis}
	}
	return models.FeeAmount{Satoshis: config.DefaultValidatorMinFeePerKB, Bytes: miningFeeBytesBasis}
}
