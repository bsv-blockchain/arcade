## Why

The arcade-refactor service has no Kubernetes deployment manifests, making it impossible to deploy to a cluster. Adding manifests enables consistent, repeatable deployments of each service mode.

## What Changes

- Add Kubernetes Deployment manifests for each service mode (`api-server`, `propagation`, `block-processor`, `bump-builder`, `tx-validator`, `p2p-client`) plus an `all` mode deployment
- Each deployment uses `ghcr.io/galt-tr/arcade-refactor:latest` as the container image with `--mode` flag to select the service
- All deployments use replica count of 1
- API server deployment includes a Service resource to expose port 8080
- All deployments include health check configuration on port 8081
- Manifests placed in the `deploy/` directory

## Capabilities

### New Capabilities
- `k8s-manifests`: Kubernetes deployment and service manifests for all arcade service modes

### Modified Capabilities

## Impact

- New `deploy/` directory with Kubernetes YAML manifests
- No code changes — manifests only
- Requires external Kafka and Aerospike to be available in the cluster (configured via environment variables with `ARCADE_` prefix)
