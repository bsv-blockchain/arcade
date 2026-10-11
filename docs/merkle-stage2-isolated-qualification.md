# Isolated Merkle Stage-2 qualification

This note records an isolated qualification of the Stage-2 topology. It does
not enable Merkle in production, does not change the production host, and
does not authorise a later gate.

The governing design is the Votari Stage-2 architecture and execution plan
dated 2026-10-01. This stack follows that design:

- Arcade is one container, `--mode all`.
- The store is Pebble on a volume used only by that container.
- Merkle Service is a separate container.
- Kafka is a separate container on the same private network.
- The callback URL is `http://votari-arcade:8080/api/v1/merkle-service/callback`.
- That hostname is the Compose service on the private network. It is not the public Arcade name.
- The callback port and the Merkle port are not published to the host.
- Merkle is started with private-IP callback support enabled. The published Merkle image leaves that support off unless it is set.
- Arcade holds two distinct synthetic bearers. One is presented to Merkle. One is required on the callback.
- No production secret, volume, broker, or network is used.

## Topology

```text
votari-arcade (--mode all, Pebble)
    |  private network votari-stage2-isolated
    |  POST /watch  Authorization: Bearer <synthetic merkle auth>
    v
votari-stage2-merkle
    |  callback URL above, Authorization: Bearer <synthetic callback bearer>
    v
votari-arcade /api/v1/merkle-service/callback

votari-stage2-kafka is the only broker. Merkle uses its own Postgres.
Neither Arcade nor Merkle publishes a host port.
```

Compose project: `compose/stage2-isolated/docker-compose.yml`.

Registration remains the current Arcade contract: propagation calls Merkle
`/watch` and only then broadcasts the transactions that registered. A failed
registration is not broadcast. This qualification did not submit a
transaction, so that order was not observed live.

## Image provenance

Arcade is built with `scripts/provenance-build.sh` and is not pushed. The
local image reference is `arcade:provenance`. `local_image_id` is the local
image ID. It is not a registry manifest digest. The registry digest for this
qualification is none.

Observed on the isolated qualification run
`36978016885` (2026-10-02):

```text
SOURCE_SHA=e6aab82f0f3c1cd7cf3f9985e9e06ee39fb5e162
OCI_REVISION=e6aab82f0f3c1cd7cf3f9985e9e06ee39fb5e162
OCI_SOURCE=https://github.com/ruidasilva/arcade
OCI_CREATED=2026-10-02T07:20:54Z
OCI_VERSION=stage2-isolated-qualification
GO_VCS_REVISION=e6aab82f0f3c1cd7cf3f9985e9e06ee39fb5e162
GO_VCS_MODIFIED=false
LOCAL_IMAGE_ID=sha256:9d4a19aa005c239c6af0c171e0bb76dceb13f0525086e4d283bd95f2e571ae1c
REGISTRY_MANIFEST_DIGEST=none
SBOM=sbom.spdx.json SPDX-2.3 name=arcade packages=305
SBOM_SHA256=27eca7d0df554d3fb949b582e50a3b21e1d3c6a8c4a9d733d22bc822cef394ae
RELEASE_MANIFEST=release-manifest.json source_clean=true
```

The Merkle container is the digest already pinned by the Arcade end-to-end
harness:

```text
ghcr.io/bsv-blockchain/merkle-service@sha256:ff4ae409df2680c54267c27887dc1744fb3b64e5ddc5636882185ce82b35ced3
```

That digest is merkle-service v0.2.5. It does not implement the
`expectedSubtreeIndices` producer contract, which requires merkle-service
v0.4.5 or newer.

## Synthetic secrets

| Direction | Config field | Fixture prefix |
| --- | --- | --- |
| Arcade to Merkle | `merkle_service.auth_token` | `synthetic-stage2-merkle-auth-` |
| Merkle to Arcade | `callback_token` | `synthetic-stage2-callback-` |
| Merkle database | Postgres password | `synthetic-stage2-merkle-db` |

The two bearers differ. The probe refuses a callback fixture that does not
use the `synthetic-stage2-` prefix. Logs are scanned for both bearers and
the run fails if either appears. The fixture values live only in the
isolated Compose config. They are not production secrets.

## What this environment can prove

The probe joins the private network. Run `36978016885` recorded:

```text
PRIVATE_CALLBACK_HOST_PORT=unpublished
MERKLE_HOST_PORT=unpublished
ARCADE_HEALTH=200
MERKLE_HEALTH=200
CALLBACK_MISSING_BEARER=401
CALLBACK_WRONG_BEARER=401
CALLBACK_CORRECT_BEARER=200
SECRET_IN_LOGS=absent
RESTART_HEALTH=200
RESTART_CORRECT_BEARER=200
```

Arcade logged `starting arcade` with `mode=all`, `kafka_backend=sarama`,
and `store_backend=pebble`, then listened on `0.0.0.0:8080` inside the
network. Both accepted callbacks were `SEEN_ON_NETWORK` for one unknown
synthetic txid. Arcade logged `dropping callback for unknown txid` and did
not create a lifecycle row. The same drop was logged again after restart.
No `MINED` line was logged.

Restart replayed the Pebble WAL (`replayed 10 keys`). Those keys are the
embedded chaintracks store, not a tracked transaction. No transaction was
pending, so this restart does not prove retention of a tracked row.

The accepted call proves authentication. It does not prove a lifecycle
transition.

## What this environment cannot prove

No synthetic transaction was submitted. There is no regtest chain and no
DataHub in this stack, so Arcade cannot move a transaction through broadcast
and mining. The pinned Merkle image cannot emit `expectedSubtreeIndices`.
There is no fault hook that fails a Pebble write on the first SEEN callback
or fails `SetMinedByTxIDs` once.

`--mode all` also starts embedded chaintracks. With `network: teratestnet`
that process dialed the built-in teratestnet bootstrap peers and logged that
those peers connected. `p2p.datahub_discovery` was off. That dial is not the
production host and it is not a production change. A later isolated run that
must stay off public peers needs an explicit empty bootstrap, which this
qualification did not add.

Observed lifecycle states: none.

| Gate | Result | Why |
| --- | --- | --- |
| Tracked subtree supplies `expectedSubtreeIndices`, and a missing STUMP leaves `processed_at` unset | BLOCKED | Merkle image is v0.2.5 and no tracked block was produced |
| True empty block has no expected set and may finalize | BLOCKED | No empty block was produced |
| SEEN store failure returns HTTP 500, then a retry applies once | BLOCKED | No store-fault injection in the isolated process. The durability tests in this tree cover that contract in process |
| `SetMinedByTxIDs` failure leaves `processed_at` unset and the watchdog later persists MINED | BLOCKED | No mined block and no mine-fault injection |
| Restart keeps a pending tracked transaction, does not regress it, and does not publish a second terminal status | BLOCKED | No tracked transaction existed. The probe only shows the process accepts the same bearer after restart |

`EXPECTED_SUBTREE_INDICES_CONTRACT` is BLOCKED. `MINED_WATCHDOG_RECOVERY` is
BLOCKED. Neither result authorises production Stage-2.

## Remaining production-only gates

From the Stage-2 execution plan, still closed:

- provenance-ready image in the production registry, including a Merkle image at v0.4.5 or newer with an immutable digest;
- production secrets in the secret store, distinct from these fixtures;
- production callback preflight on the production host;
- production read-only preflight;
- enabling Merkle registration;
- a fresh production synthetic transaction;
- rollback readiness.

This qualification does not open those gates.
