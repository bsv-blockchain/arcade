## 1. Directory Setup

- [x] 1.1 Create `deploy/` directory at repository root

## 2. Service Deployments

- [x] 2.1 Create `deploy/api-server.yaml` — Deployment with `--mode api-server`, ports 8080+8081, health probes
- [x] 2.2 Create `deploy/propagation.yaml` — Deployment with `--mode propagation`, port 8081, health probes
- [x] 2.3 Create `deploy/block-processor.yaml` — Deployment with `--mode block-processor`, port 8081, health probes
- [x] 2.4 Create `deploy/bump-builder.yaml` — Deployment with `--mode bump-builder`, port 8081, health probes
- [x] 2.5 Create `deploy/tx-validator.yaml` — Deployment with `--mode tx-validator`, port 8081, health probes
- [x] 2.6 Create `deploy/p2p-client.yaml` — Deployment with `--mode p2p-client`, port 8081, health probes
- [x] 2.7 Create `deploy/all.yaml` — Deployment with `--mode all`, ports 8080+8081, health probes

## 3. Service Resource

- [x] 3.1 Create `deploy/api-server-service.yaml` — ClusterIP Service targeting api-server pods on port 8080

## 4. Verification

- [x] 4.1 Verify all manifests pass `kubectl apply --dry-run=client` (if kubectl available) or manual YAML validation
