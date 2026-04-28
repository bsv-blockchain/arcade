## ADDED Requirements

### Requirement: Per-service Deployment manifests
The project SHALL provide a Kubernetes Deployment manifest for each service mode: `api-server`, `propagation`, `block-processor`, `bump-builder`, `tx-validator`, and `p2p-client`. An additional `all`-mode Deployment SHALL also be provided.

#### Scenario: Each service mode has a Deployment
- **WHEN** an operator lists the manifest files in `deploy/`
- **THEN** there SHALL be a Deployment YAML file for each of `api-server`, `propagation`, `block-processor`, `bump-builder`, `tx-validator`, `p2p-client`, and `all`

#### Scenario: Deployment uses correct mode flag
- **WHEN** a service Deployment is applied to a cluster
- **THEN** the container SHALL run with args `["--mode", "<service-mode>"]` matching the deployment's service

### Requirement: Container image specification
All Deployments SHALL use the container image `ghcr.io/galt-tr/arcade-refactor:latest`.

#### Scenario: Image reference
- **WHEN** any Deployment manifest is inspected
- **THEN** the container image SHALL be `ghcr.io/galt-tr/arcade-refactor:latest`

### Requirement: Replica count
All Deployments SHALL specify exactly 1 replica.

#### Scenario: Single replica
- **WHEN** any Deployment manifest is inspected
- **THEN** `spec.replicas` SHALL be `1`

### Requirement: Health probes
All Deployments SHALL configure liveness and readiness probes using HTTP GET on the health endpoint at port 8081.

#### Scenario: Liveness probe configured
- **WHEN** any Deployment manifest is inspected
- **THEN** the container SHALL have a livenessProbe with `httpGet` on port `8081` at path `/health`

#### Scenario: Readiness probe configured
- **WHEN** any Deployment manifest is inspected
- **THEN** the container SHALL have a readinessProbe with `httpGet` on port `8081` at path `/health`

### Requirement: API server Service
A Kubernetes Service manifest SHALL expose the `api-server` Deployment on port 8080 using ClusterIP type.

#### Scenario: Service targets api-server pods
- **WHEN** the api-server Service manifest is applied
- **THEN** it SHALL select pods with the `api-server` Deployment's labels and forward port 8080 to container port 8080

### Requirement: Environment variable configuration
All Deployments SHALL include environment variables for Kafka brokers (`ARCADE_KAFKA_BROKERS`), Aerospike hosts (`ARCADE_AEROSPIKE_HOSTS`), and Aerospike namespace (`ARCADE_AEROSPIKE_NAMESPACE`) with placeholder values that operators can override.

#### Scenario: Required env vars present
- **WHEN** any Deployment manifest is inspected
- **THEN** the container spec SHALL include `ARCADE_KAFKA_BROKERS`, `ARCADE_AEROSPIKE_HOSTS`, and `ARCADE_AEROSPIKE_NAMESPACE` environment variables

### Requirement: Manifest directory structure
All Kubernetes manifests SHALL be placed in the `deploy/` directory at the repository root.

#### Scenario: Manifests in deploy directory
- **WHEN** an operator looks for Kubernetes manifests
- **THEN** all manifests SHALL be located under `deploy/`
