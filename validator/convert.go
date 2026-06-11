package validator

import (
	"errors"
	"fmt"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// errMissingSourceData is returned when an input lacks the previous-output data
// (satoshis + locking script) required to build an extended transaction. The
// BDK validator needs this to compute fees and execute scripts; arcade obtains
// it from the EF/BEEF carried with the submission.
var errMissingSourceData = errors.New("transaction could not be transformed to extended format: missing input source data")

// toExtendedBT converts a go-sdk transaction into an extended go-bt transaction
// (each input carrying PreviousTxSatoshis and PreviousTxScript), which is the
// form teranode's TxValidator requires. Source data is copied from the go-sdk
// input's resolved source output (populated from EF/BEEF at parse time).
func toExtendedBT(tx *sdkTx.Transaction) (*bt.Tx, error) {
	btx, err := bt.NewTxFromBytes(tx.Bytes())
	if err != nil {
		return nil, fmt.Errorf("re-parse transaction into go-bt: %w", err)
	}

	if len(btx.Inputs) != len(tx.Inputs) {
		return nil, fmt.Errorf("input count mismatch after re-parse: bt=%d sdk=%d", len(btx.Inputs), len(tx.Inputs))
	}

	for i, in := range tx.Inputs {
		sats := in.SourceTxSatoshis()
		lock := in.SourceTxScript()
		if sats == nil || lock == nil {
			return nil, fmt.Errorf("%w: input %d", errMissingSourceData, i)
		}

		btx.Inputs[i].PreviousTxSatoshis = *sats
		// script.Script and bscript.Script are both []byte; convert directly.
		prevScript := bscript.Script(*lock)
		btx.Inputs[i].PreviousTxScript = &prevScript
	}

	return btx, nil
}
