## MODIFIED Requirements

### Requirement: Full transaction validation using go-sdk
The TX validator SHALL parse transactions using `go-sdk/transaction.NewTransactionFromBytes` (with BEEF fallback via `ParseBeef`), compute TXIDs via `tx.TxID().String()`, and validate using policy checks, fee validation, and script validation from the go-sdk.

#### Scenario: Parse and compute TXID
- **WHEN** a raw transaction is received
- **THEN** the validator SHALL parse it via go-sdk, compute the TXID as double-SHA256 reversed hex, and use this as the canonical transaction identifier

#### Scenario: BEEF format transaction
- **WHEN** a transaction in BEEF format is received
- **THEN** the validator SHALL attempt `ParseBeef` first, falling back to `NewTransactionFromBytes`

### Requirement: Policy validation
The validator SHALL check: inputs and outputs exist, transaction size within bounds (61 bytes to 4GB), input/output satoshi values within bounds (dust limit to 21M BTC), coinbase inputs rejected, sigops count within policy limit, unlocking scripts are non-empty and push-only.

#### Scenario: Transaction too small
- **WHEN** a transaction is less than 61 bytes
- **THEN** the validator SHALL return ArcError with StatusTxSize (474)

#### Scenario: Empty unlocking script
- **WHEN** a transaction input has a nil or empty unlocking script
- **THEN** the validator SHALL return ArcError with StatusUnlockingScripts (461)

#### Scenario: Output with non-zero OP_RETURN
- **WHEN** a transaction has an OP_RETURN output with non-zero satoshis
- **THEN** the validator SHALL return ArcError with StatusOutputs (464)

### Requirement: Fee and script validation via SPV
The validator SHALL use `spv.Verify` from go-sdk for fee validation (with `SatoshisPerKilobyte` fee model) and script validation (with chain tracker). Both validations SHALL be independently skippable via `SkipFeeValidation` and `SkipScriptValidation` options.

#### Scenario: Fee too low
- **WHEN** a transaction's fee is below the minimum per-KB rate
- **THEN** the validator SHALL return ArcError with StatusFees (465) including the fee details in ExtraInfo

#### Scenario: Script validation with chain tracker
- **WHEN** script validation is enabled and a transaction has invalid scripts
- **THEN** the validator SHALL return ArcError with StatusUnlockingScripts (461)
