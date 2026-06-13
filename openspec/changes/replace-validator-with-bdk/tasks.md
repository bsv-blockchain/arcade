## 1. Validator engine

- [x] 1.1 Replace `validator/validator.go` internals with a wrapper over teranode `TxValidator`; keep `NewValidator`, `ValidateTransaction(ctx, *sdkTx.Transaction, skipFees)`, `ValidatePolicy`, `MinFeePerKB` and the `Policy` type.
- [x] 1.2 Add `NewValidatorForNetwork(network, *Policy)` resolving `chaincfg.GetChainParams`; build fee-checked and fee-skipping `*TxValidator` instances.
- [x] 1.3 Set all BDK-read `PolicySettings` fields to canonical BSV defaults (`defaultPolicySettings`); convert `MinFeePerKB` sat/kB → BSV/kB; cap `MaxTxSizePolicy` overrides at the 1 GB BDK limit; enable `DataCarrier`.
- [x] 1.4 Use a static `allForksActiveHeight = 2_000_000_000` and a low-sentinel `utxoHeights` slice; document why `math.MaxUint32` is unusable.

## 2. Conversion and error mapping

- [x] 2.1 Add `toExtendedBT` (`validator/convert.go`): serialize → `bt.NewTxFromBytes` → copy `PreviousTxSatoshis`/`PreviousTxScript`; return `errMissingSourceData` when source data is absent.
- [x] 2.2 Add `mapTeranodeError`/`classifyByMessage` (`validator/errmap.go`): teranode `*errors.Error` codes + message text → ARC status; preserve reason as `ExtraInfo`; `errMissingSourceData` → 460.

## 3. Wiring and config

- [x] 3.1 In `app/app.go`, construct via `validator.NewValidatorForNetwork(cfg.Network, validatorPolicyFromConfig(cfg))` and propagate the error (clean up store/producer).
- [ ] 3.2 Confirm `config.go`'s `ValidatorConfig` comment no longer references removed pure-Go policy fields; no new config fields required for the first cut.

## 4. Build system (cgo everywhere)

- [x] 4.1 `Makefile`: `CGO_ENABLED=1` in `build`, `test`, `docker-build`.
- [x] 4.2 `Dockerfile`: switch runtime base from `alpine` (musl) to `gcr.io/distroless/cc-debian12` (glibc + libstdc++ + ca-certs); use an absolute ENTRYPOINT path.
- [x] 4.3 `.github/workflows/build.yml`: `CGO_ENABLED=1`; install `build-essential` before the build.
- [ ] 4.4 Verify the `fortress-*` workflows (test/vet/lint/bench/fuzz) build with cgo — they rely on the runner's default `CGO_ENABLED=1` and an installed C toolchain; add an explicit toolchain step if any leg lacks gcc/g++.

## 5. Tests

- [x] 5.1 Rewrite `validator/validator_test.go` for the BDK engine: construction, sat/kB→BSV/kB, `toExtendedBT` (+ missing-source-data), `mapTeranodeError` table.
- [x] 5.2 Chronicle regression (#192) through the real BDK engine: version≥2 non-push-only unlocking script accepted; version<2 rejected with status 461.
- [x] 5.3 Fee path: underpaid tx → 465; same tx accepted with `skipFees`.
- [ ] 5.4 Document in the package/test that `go test ./validator/...` requires cgo + a C toolchain.

## 6. Docs

- [x] 6.1 Author this OpenSpec change; run `openspec validate replace-validator-with-bdk --strict`.
- [ ] 6.2 Note the cgo build requirement and the glibc runtime base in the repo README / build docs.
