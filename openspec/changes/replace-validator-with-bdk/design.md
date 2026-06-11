# Design

## Engine seam

`validator.Validator` keeps its public methods so the api-server intake call
sites (`handlers.go` single + batch submit) and `app.go` wiring change
minimally. Internally it holds two teranode `*TxValidator` instances — one that
enforces the fee floor and one with `MinMiningTxFee=0` — so `ValidateTransaction`
can honour `skipFees` without setting `SkipPolicyChecks=true` (which in teranode
also disables script/standardness checks, turning on consensus mode).

`NewValidatorForNetwork(network, *Policy)` resolves `chaincfg.GetChainParams`
from arcade's existing `network` config (the param `.Name` values —
`mainnet`/`testnet`/`teratestnet`/`regtest` — map 1:1 to BDK's chain names) and
returns an error for unknown networks instead of letting the BDK adapter call
`Fatalf`. The legacy `NewValidator(*Policy)` defaults to mainnet for tests.

## Policy settings

`settings.NewPolicySettings()` returns an all-zero struct (its defaults are a
TODO), and the BDK adapter reads ~14 fields directly. Several zeros are unsafe —
e.g. `MaxStackMemoryUsagePolicy=0` would reject any memory-using script — so the
validator sets every field explicitly to teranode's canonical BSV values
(`defaultPolicySettings`). `DataCarrier` is enabled because arcade accepts
OP_RETURN data transactions. The operator-facing `MinFeePerKB` (satoshis/kB) is
converted to BSV/kB for `SetMinMiningTxFee` (100 sat/kB → 0.000001 BSV/kB;
teranode converts back via `*1e8/1000`). An explicit `MaxTxSizePolicy` override
is capped at BDK's 1 GB consensus limit to avoid a construction panic.

## go-sdk → extended go-bt conversion

TxValidator needs a go-bt `*bt.Tx` in extended format. `toExtendedBT` serializes
the go-sdk transaction's standard bytes, re-parses them with `bt.NewTxFromBytes`,
then copies each input's `PreviousTxSatoshis` and `PreviousTxScript` from the
go-sdk input's resolved source output (`SourceTxSatoshis()`/`SourceTxScript()`,
populated from EF/BEEF at parse time). `script.Script` and `bscript.Script` are
both `[]byte`, so the script copy is a direct conversion. A nil source output is
returned as `errMissingSourceData` and mapped to ARC status 460 — a deliberate
tightening, since BDK cannot compute fees or run scripts without it.

## Static block height

BDK gates fork activation (Genesis, Chronicle) by comparing the supplied block
height against the per-network activation heights baked into `chaincfg.Params`.
Arcade passes `allForksActiveHeight = 2_000_000_000`: above every network's
activation heights (so all upgrades evaluate as active and arcade validates
against current rules) yet below `math.MaxInt32`. `math.MaxUint32` cannot be
used — the BDK adapter converts the height to int32 and, in policy mode,
evaluates `blockHeight-1`, so it would overflow and reject every transaction.

Per-input `utxoHeights` are unknown at intake (arcade does not track per-UTXO
confirmation heights), so a full-length slice of a low sentinel height is passed:
every input appears mature. This matches arcade's prior behaviour of not
enforcing coinbase maturity / BIP68 at intake — teranode re-checks with real
heights downstream and remains the authority.

## Error mapping

The BDK adapter wraps fee/script/policy failures under `ERR_TX_INVALID`, so the
precise ARC status is recovered from teranode's deterministic message text
(`mapTeranodeError` → `classifyByMessage`): "fee is too low"/"input satoshis is
less than output" → 465; "coinbase"/"bad-txns-inputs-too-large" → 462; GoBDK
script/policy failures → 461; `ERR_PROCESSING` (cgo exception) → 467;
`ERR_INVALID_ARGUMENT` → 463. The original message is preserved as `ExtraInfo`.

## Build & runtime

`gobdk` links the BSV BDK C++ library via cgo, pulling in a prebuilt static
archive per platform from the module cache (`bdkcgo/libGoBDK_linux_*.a`). The
resulting binary is dynamically linked against glibc + libstdc++, so: builds use
`CGO_ENABLED=1` with a C/C++ toolchain; CI builds each arch natively (the gobdk
archive matches the runner arch — no cross-compiler); and the runtime image
moves to a glibc base (`distroless/cc`).
