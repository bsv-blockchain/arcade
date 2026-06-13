## Why

Arcade validates transactions at intake with go-sdk's pure-Go script interpreter (`spv.Verify` in the `validator` package), while teranode/svnode admit transactions with go-bdk — the BSV C++ consensus library. Because arcade re-implements the rules in Go, its view of validity drifts from the node across protocol upgrades. The Chronicle upgrade already forced a hand-patch: arcade special-cased the push-only relaxation for version≥2 transactions (`validator/validator.go`), and deeper script-execution divergence still lives in `spv.Verify` (which hardcodes fork-activation flags and exposes no `WithAfterChronicle` passthrough). See issue #192.

The fix is to stop re-implementing consensus and instead reuse the exact validator teranode uses: `github.com/bsv-blockchain/teranode/services/validator.TxValidator`. Arcade already depends on teranode, so this aligns the intake decision with node consensus by construction. The cost is that TxValidator binds the BSV BDK C++ library via cgo, so arcade can no longer build as a pure-Go static binary.

## What Changes

- Replace the `validator` package internals with a thin wrapper over teranode's `TxValidator`. The public surface (`NewValidator`, `ValidateTransaction(ctx, *sdkTx.Transaction, skipFees)`, `ValidatePolicy`, `MinFeePerKB`) is preserved so the api-server intake call sites are unchanged.
- Add `NewValidatorForNetwork(network, *Policy)`, which resolves `go-chaincfg` params from arcade's `network` config and builds the validator; `app.go` wires it and propagates the error.
- Convert the go-sdk transaction to an extended go-bt `*bt.Tx` (per-input `PreviousTxSatoshis`/`PreviousTxScript` from the EF/BEEF source data) — the form TxValidator requires. A missing source output is now a hard rejection (ARC status 460).
- Pass a static "all-forks-active" block height (`2_000_000_000`) so BDK applies current consensus rules; arcade's intake is height-agnostic post-Genesis and does not track the chain tip.
- Map teranode validation errors to arcade's ARC status codes, preserving the client-facing reason and the existing `StatusRejected`/`ExtraInfo` behavior.
- Remove the hand-patched Chronicle push-only rule — BDK now enforces the version gate natively.
- Switch the whole build to `CGO_ENABLED=1` and the runtime image to a glibc base (distroless/cc instead of alpine), since the binary dynamically links glibc + libstdc++.

## Capabilities

### Modified Capabilities

- `tx-validator`: the validation engine changes from arcade's pure-Go checks to teranode's BDK-backed `TxValidator`, so intake accept/reject decisions match node consensus across protocol upgrades. The service's consume/route/state-update behavior is unchanged; only the rule engine and its rejection-reason mapping change.

## Impact

- **Build toolchain**: `CGO_ENABLED=1` everywhere (Makefile, `build.yml`); a C/C++ toolchain (gcc/g++, libstdc++) is required at build time. Pure-Go static builds and easy cross-compilation are no longer possible; each arch builds natively (gobdk ships a prebuilt static archive per platform, so no cross-compiler is needed).
- **Runtime image**: Dockerfile base changes from `alpine:3.23` (musl) to `gcr.io/distroless/cc-debian12` (glibc + libstdc++ + ca-certificates). A musl base will not run the cgo binary.
- **New transitive dependency**: `github.com/bitcoin-sv/bdk/module/gobdk` (cgo, recorded `// indirect`); `github.com/bsv-blockchain/go-chaincfg` becomes a direct dependency.
- **Behavior tightening**: transactions whose inputs lack source data are rejected at intake with status 460 (previously they passed the policy stage and failed later in `spv.Verify`). Max-tx-size policy moves from arcade's 4 GiB ceiling to teranode's canonical 10 MB mempool default (BDK also caps the policy setting at 1 GB).
- **Hot-path cost**: each submit crosses the cgo boundary into C++ once per transaction, plus a go-sdk→go-bt serialize/re-parse. The C++ validator is built once and reused.
- **Tests**: the `validator` package and api-server tests now require cgo + the gobdk archive (delivered by `go mod download`); they will not run under `CGO_ENABLED=0`.
