# Migrating from arcade to BSV Broadcaster

The project formerly known as **arcade** is now **BSV Broadcaster**. The rename covers the
repository, Go module, binary, container image, configuration surface, Kafka
topics, metrics and storage defaults. This page lists what changed and what an
existing deployment must do to upgrade without losing state.

## At a glance

| What | Before | After | Compat |
|---|---|---|---|
| Repository / Go module | `github.com/bsv-blockchain/arcade` | `github.com/bsv-blockchain/bsv-broadcaster` | GitHub redirects the old URL |
| Binary / `cmd` | `arcade`, `./cmd/arcade` | `bsv-broadcaster`, `./cmd/bsv-broadcaster` | — |
| Container image | `ghcr.io/bsv-blockchain/arcade` | `ghcr.io/bsv-blockchain/bsv-broadcaster` | — |
| Env var prefix | `ARCADE_*` | `BSV_BROADCASTER_*` | `ARCADE_*` still honored (deprecated) |
| Config directory | `/etc/arcade` | `/etc/bsv-broadcaster` | `/etc/arcade` still searched (deprecated) |
| Kafka topics | `arcade.<name>` | `bsv-broadcaster.<name>` | pin `kafka.topic_prefix: arcade` |
| Kafka consumer group | `arcade` | `bsv-broadcaster` | pin `kafka.consumer_group: arcade` |
| Prometheus metrics | `arcade_*` | `bsv_broadcaster_*` | **none**: update dashboards and alerts |
| Storage path | `~/.arcade` | `~/.bsv-broadcaster` | pin `storage_path` / `store.pebble.path` |
| Embedded Postgres user / password / db | `arcade` | `bsv_broadcaster` | pin `store.postgres.embedded_{user,password,database}` |
| Embedded Postgres data dir | `~/.arcade/postgres` | `~/.bsv-broadcaster/postgres` | pin `store.postgres.embedded_data_dir` |
| MongoDB database | `arcade` | `bsv_broadcaster` | pin `store.mongodb.database` |
| Aerospike namespace | `arcade` | `bsv_broadcaster` | pin `store.aerospike.namespace` |
| OTel `service.name` | `arcade` | `bsv-broadcaster` | pin `telemetry.service_name` |
| OTel resource attribute | `arcade.mode` | `bsv_broadcaster.mode` | none |
| Event fan-out consumer group ids | `arcade-events-…` | `bsv-broadcaster-events-…` | none (ephemeral per process) |
| Webhook `User-Agent` | `arcade-webhook/<version>` | `bsv-broadcaster-webhook/<version>` | none: receivers that allowlist the prefix must add the new one |

These are **unchanged** because they name persisted state:

- Aerospike set and secondary-index names (`arcade_transactions`, `arcade_idx_tx_status`, …).
- The Postgres schema-migration advisory-lock key.
- Postgres table names and MongoDB collection names. These never carried the old name.
- In the Kubernetes manifests under `deploy/`: the `arcade` namespace, the
  `arcade-chaintracks-data` PVC, and the Aerospike server namespace `arcade` with
  its device file `/opt/aerospike/data/arcade.dat`. The manifests also pin the
  Kafka topic prefix and consumer group to `arcade`. See
  [Kubernetes manifests](#kubernetes-manifests-deploy).

## Upgrading an existing deployment

Legacy `ARCADE_*` environment variables are still read. Each one is copied to its
`BSV_BROADCASTER_*` name unless that name is already set, so an explicit
`BSV_BROADCASTER_*` value always wins. At startup the binary logs one
`deprecated configuration` warning that lists every legacy variable it mapped. A
config file found under `/etc/arcade` also works and logs a warning.

The env shim keeps your **explicit** settings. It cannot help with values you
never set, because those now get the new defaults. Before rolling out the new
image, pin every default your deployment relies on. The table is ordered by
how badly an unpinned value fails:

| Pin | If left unpinned |
|---|---|
| `kafka.consumer_group: arcade` | **Fails silently.** Consumers join a brand-new group at `auto.offset.reset` and replay or skip `tx_status` / `block_processed` traffic. |
| `store.mongodb.database: arcade`, `store.postgres.embedded_{user,password,database}: arcade` | Fails silently. Writes go to a new, empty database. |
| `store.postgres.embedded_data_dir: ~/.arcade/postgres` | Fails silently. Embedded Postgres initializes a brand-new cluster. This path does not derive from `storage_path`, so pinning `storage_path` alone does not cover it. |
| `storage_path`, `store.pebble.path` (`~/.arcade/pebble`) | Fails silently. Pebble, chaintracks and the p2p identity start from an empty local store. `storage_path` covers chaintracks and p2p only; Pebble needs its own pin. |
| `store.aerospike.namespace: arcade` | Fails loudly. Aerospike rejects the unknown namespace, and adding one needs a server restart. |
| `kafka.topic_prefix: arcade` | Fails loudly. The propagation-topic partition check fails closed at startup. |
| `telemetry.service_name: arcade` | Cosmetic, but traces and metrics regroup under a new service in your backend. |

As environment variables, using the legacy prefix (still honored) or the new
one:

```bash
BSV_BROADCASTER_KAFKA_CONSUMER_GROUP=arcade
BSV_BROADCASTER_KAFKA_TOPIC_PREFIX=arcade
BSV_BROADCASTER_STORE_MONGODB_DATABASE=arcade
BSV_BROADCASTER_STORE_AEROSPIKE_NAMESPACE=arcade
BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_USER=arcade
BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_PASSWORD=arcade
BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_DATABASE=arcade
BSV_BROADCASTER_STORE_POSTGRES_EMBEDDED_DATA_DIR=~/.arcade/postgres
BSV_BROADCASTER_STORE_PEBBLE_PATH=~/.arcade/pebble
BSV_BROADCASTER_STORAGE_PATH=~/.arcade
BSV_BROADCASTER_TELEMETRY_SERVICE_NAME=arcade
```

Pin only the backends you run. If you set non-default values for any of these
before, the env shim already carries them over. `store.postgres.embedded_cache_dir`
only caches the extracted Postgres binary, so it can move to its new default.

Then:

1. Point the image at `ghcr.io/bsv-blockchain/bsv-broadcaster`.
2. Update Prometheus/Coralogix dashboards and alerts from `arcade_*` to
   `bsv_broadcaster_*` in the same change. Every metric keeps its suffix, labels and
   semantics; only the prefix changes.
3. If webhook receivers allowlist the `arcade-webhook/` User-Agent prefix, ask
   them to allow `bsv-broadcaster-webhook/` before upgrading.
4. Once deployed, rename the remaining `ARCADE_*` variables to `BSV_BROADCASTER_*`
   to clear the deprecation warning.

Moving to the new topic names, consumer group or database names is optional.
It needs a planned data migration (drain and recreate topics, or copy the
database), and you can do it at any later point by removing the pins.

## Kubernetes manifests (`deploy/`)

The manifests keep every name that identifies stored data, so `kubectl apply -f
deploy/` upgrades an existing install in place:

- Namespace `arcade`. Kafka's and Aerospike's StatefulSet names are unchanged,
  so their `volumeClaimTemplates` PVCs (`data-kafka-0`, `data-aerospike-0`) are
  reused.
- PVC `arcade-chaintracks-data` for chaintracks headers.
- Aerospike server namespace `arcade`, backed by
  `/opt/aerospike/data/arcade.dat`. Every service sets
  `BSV_BROADCASTER_STORE_AEROSPIKE_NAMESPACE=arcade` to match.
- `BSV_BROADCASTER_KAFKA_TOPIC_PREFIX=arcade` and
  `BSV_BROADCASTER_KAFKA_CONSUMER_GROUP=arcade` on every service.

The Deployments and their Services were renamed from `arcade-<service>` to
`bsv-broadcaster-<service>`. Applying the manifests creates the new ones beside
the old ones, and the new chaintracks pod stays `Pending` until the old pod
releases its ReadWriteOnce PVC. Apply, then delete the old objects. They are the
only ones still labelled `part-of=arcade`, because the apply relabels everything
it keeps:

```bash
kubectl apply -f deploy/
kubectl -n arcade delete deployment,service -l app.kubernetes.io/part-of=arcade
```

A fresh install that does not need the old names can change them before the
first apply. Change the namespace (including every `*.arcade.svc.cluster.local`
host), the PVC name, the Aerospike namespace and device file, and the matching
env values, all together.

The manifests before this release set `ARCADE_AEROSPIKE_HOSTS` and
`ARCADE_AEROSPIKE_NAMESPACE`, which never matched a config key, so they had no
effect. The manifests now use the correct `BSV_BROADCASTER_STORE_AEROSPIKE_*`
names. Check any copies you maintain.

## Local compose stack

`docker-compose.yaml` now initializes Postgres as user/database
`bsv_broadcaster`. A Postgres volume created by the old stack was initialized
as `arcade`, so the services fail to authenticate. Remove the old volume
before `make docker-up`. The local stack holds no data worth keeping.
