package validator

import (
	"errors"
	"strings"

	tnerr "github.com/bsv-blockchain/teranode/errors"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

// mapTeranodeError translates an error from the BDK-backed TxValidator (or the
// extended-format conversion) into an arcade ArcError carrying an ARC status
// code. The original message is preserved as ExtraInfo so the client-facing
// reason and the persisted StatusRejected/ExtraInfo are unchanged.
func mapTeranodeError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, errMissingSourceData) {
		return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusTxFormat, err.Error())
	}

	msg := err.Error()

	var te *tnerr.Error
	if errors.As(err, &te) {
		switch te.Code() {
		case tnerr.ERR_PROCESSING:
			// Internal BDK/cgo failure (e.g. SCRIPT_ERR_CGO_EXCEPTION) — not
			// attributable to the submitted transaction.
			return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusGeneric, msg)
		case tnerr.ERR_INVALID_ARGUMENT:
			return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusMalformed, msg)
		default:
			// Every other teranode code (notably ERR_TX_INVALID, which wraps
			// fee/script/policy failures) is classified from the message text.
			return classifyByMessage(err, msg)
		}
	}

	return classifyByMessage(err, msg)
}

// classifyByMessage picks an ARC status from teranode's deterministic error
// messages. The BDK adapter wraps fee/script/policy failures under
// ERR_TX_INVALID, so the precise status is recovered from the message text.
func classifyByMessage(err error, msg string) error {
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "fee is too low"),
		strings.Contains(lower, "input satoshis is less than output"):
		return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusFees, msg)
	case strings.Contains(lower, "coinbase"),
		strings.Contains(lower, "bad-txns-inputs-too-large"):
		return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusInputs, msg)
	case strings.Contains(lower, "oversize"),
		strings.Contains(lower, "tx-size"),
		strings.Contains(lower, "too big"):
		return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusTxSize, msg)
	case strings.Contains(lower, "gobdk"),
		strings.Contains(lower, "script"):
		// Script execution / standardness / policy failure from the engine.
		return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusUnlockingScripts, msg)
	default:
		return arcerrors.NewArcErrorWithInfo(err, arcerrors.StatusGeneric, msg)
	}
}
