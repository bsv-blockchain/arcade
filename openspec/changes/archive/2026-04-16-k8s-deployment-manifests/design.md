## Context

The arcade-refactor application is a single Go binary that runs different service modes via `--mode` flag: `api-server`, `propagation`, `block-processor`, `bump-builder`, `tx-validator`, `p2p-client`, and `all`. It depends on Kafka and Aerospike as external infrastructure. The API server listens on port 8080, and all modes expose a health endpoint on port 8081. Configuration is via environment variables with `ARCADE_` prefix.

Currently there are no Kubernetes manifests. The container image is published to `ghcr.io/galt-tr/arcade-refactor:latest`.

## Goals / Non-Goals

**Goals:**
- Provide a Deployment manifest for each service mode with 1 replica
- Expose the API server via a ClusterIP Service on port 8080
- Configure liveness/readiness probes using the health endpoint (port 8081)
- Use environment variables for Kafka, Aerospike, and other config
- Place all manifests under `deploy/`

**Non-Goals:**
- Helm charts or Kustomize overlays
- Ingress/load balancer configuration
- Kafka or Aerospike deployment manifests (assumed pre-existing)
- Horizontal pod autoscaling or production tuning
- Secrets management (tokens passed as plain env vars; secrets integration is a follow-up)

## Decisions

**One Deployment per service mode** — Each mode gets its own Deployment file rather than a single `all`-mode deployment. This matches the microservice architecture and allows independent scaling later. An `all`-mode deployment is also included for simple single-instance setups.

**Plain manifests over Helm/Kustomize** — Keeps things simple and dependency-free. Helm can be layered on later if templating is needed.

**ClusterIP Service for API server only** — Only `api-server` needs inbound traffic. Other services are consumers (Kafka, P2P) with no inbound ports. Health port is used for probes only, not external access.

**Environment variable configuration** — The app already reads `ARCADE_*` env vars via viper. Manifests set these directly in the Deployment spec. Operators can override with ConfigMaps or external secret stores.

## Risks / Trade-offs

**[No secrets management]** → Sensitive values (auth tokens) are placeholder env vars. Operators must wire in Kubernetes Secrets or external secret stores before production use.

**[No resource limits]** → Initial manifests omit CPU/memory limits. These should be tuned based on observed usage before production.

**[Single replica]** → Replica count of 1 means no HA. This is intentional for initial deployment; scaling is a follow-up concern.
