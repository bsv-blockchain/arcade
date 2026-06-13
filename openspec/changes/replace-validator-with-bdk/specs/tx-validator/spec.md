## MODIFIED Requirements

### Requirement: Validate transactions

The TX validator SHALL validate transactions using teranode's BDK-backed
`TxValidator` (`github.com/bsv-blockchain/teranode/services/validator`) — the
same engine teranode/svnode use — so that arcade's accept/reject decision
matches node consensus across protocol upgrades. The validator SHALL NOT
re-implement consensus or policy script rules in Go. Valid transactions are
routed to propagation; invalid transactions are rejected with the rejection
reason recorded.

The validator SHALL convert each submitted transaction to extended form (every
input carrying its previous output's satoshis and locking script, drawn from the
EF/BEEF source data) before validation, and SHALL validate against current
consensus rules by supplying a block height at which all protocol upgrades
(including Chronicle) are active.

#### Scenario: Valid transaction

- **WHEN** a transaction passes BDK validation (structure, standardness, script
  execution, sigops, and — unless fee checks are skipped — the fee floor)
- **THEN** the validator SHALL route it to propagation and record its initial state

#### Scenario: Invalid transaction

- **WHEN** a transaction fails BDK validation
- **THEN** the validator SHALL mark it REJECTED with a reason mapped to the
  matching ARC status code (e.g. fee too low → 465, invalid scripts → 461,
  invalid inputs → 462) and SHALL NOT propagate it

#### Scenario: Chronicle version-gated unlocking scripts

- **WHEN** a version≥2 transaction carries a non-push-only (functional-opcode)
  unlocking script that otherwise evaluates successfully
- **THEN** the validator SHALL accept it (the Chronicle relaxation, applied by
  the BDK engine)
- **AND WHEN** the same unlocking script appears in a version<2 transaction
- **THEN** the validator SHALL reject it with ARC status 461 (push-only rule)

#### Scenario: Missing source data

- **WHEN** a submitted transaction has an input with no resolvable source output
  (satoshis + locking script)
- **THEN** the validator SHALL reject it with ARC status 460 (not extended
  format) without attempting BDK validation

#### Scenario: Duplicate transaction

- **WHEN** a transaction is submitted that already exists in the store
- **THEN** the validator SHALL return the existing transaction state without
  re-processing
