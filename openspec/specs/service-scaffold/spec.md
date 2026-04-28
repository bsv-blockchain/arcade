# service-scaffold Specification

## Purpose
TBD - created by archiving change arcade-microservice-scaffold. Update Purpose after archive.
## Requirements
### Requirement: Common service interface
All services SHALL implement a common `Service` interface with `Start(ctx context.Context) error` and `Stop() error` methods, following the Teranode daemon pattern.

#### Scenario: Start a service
- **WHEN** the main binary starts the API server service
- **THEN** it SHALL call `Start(ctx)` which initializes connections (Kafka, Aerospike) and begins processing

#### Scenario: Graceful shutdown
- **WHEN** the process receives SIGTERM or SIGINT
- **THEN** each running service's `Stop()` method SHALL be called, allowing in-flight operations to complete before exiting

### Requirement: All-in-one deployment mode
The single `arcade` binary SHALL support running all services in one process, configured via environment variables or config file.

#### Scenario: Run all services
- **WHEN** the binary is started with `--mode=all` or `ARCADE_MODE=all`
- **THEN** all services (api-server, p2p-client, block-processor, bump-builder, tx-validator, propagation) SHALL start in the same process

#### Scenario: Run single service
- **WHEN** the binary is started with `--mode=api-server`
- **THEN** only the API server service SHALL start

### Requirement: Configuration management
The system SHALL use Viper-based configuration supporting environment variables, config files (YAML), and CLI flags with the following precedence: CLI flags > env vars > config file > defaults.

#### Scenario: Configure via environment variables
- **WHEN** `ARCADE_KAFKA_BROKERS=broker1:9092,broker2:9092` is set
- **THEN** the Kafka client SHALL connect to both brokers

#### Scenario: Configure via config file
- **WHEN** a `config.yaml` file specifies `aerospike.hosts: ["aero1:3000"]`
- **THEN** the Aerospike client SHALL connect to that host

#### Scenario: Missing required configuration
- **WHEN** required configuration (e.g., Kafka brokers) is not provided
- **THEN** the service SHALL fail to start with a clear error message indicating the missing configuration

### Requirement: Health and readiness endpoints
Each service SHALL expose HTTP health (`/health`) and readiness (`/ready`) endpoints for orchestration systems.

#### Scenario: Service healthy
- **WHEN** a service is running and all dependencies (Kafka, Aerospike) are connected
- **THEN** `/health` SHALL return HTTP 200 and `/ready` SHALL return HTTP 200

#### Scenario: Dependency unavailable
- **WHEN** a service is running but Kafka is unreachable
- **THEN** `/health` SHALL return HTTP 200 (process is alive) and `/ready` SHALL return HTTP 503 (not ready to serve)

### Requirement: Structured logging
All services SHALL use structured logging (JSON format) with configurable log levels and consistent fields: service name, timestamp, log level, and correlation IDs where applicable.

#### Scenario: Log with correlation ID
- **WHEN** processing a callback for TXID `tx123`
- **THEN** all log entries for that processing chain SHALL include `txid: tx123` as a structured field

### Requirement: Go module setup
The project SHALL be organized as a single Go module with `cmd/arcade/main.go` as the entrypoint, `services/` for service implementations, `store/` for database access, `kafka/` for messaging, and `models/` for shared data types.

#### Scenario: Build single binary
- **WHEN** `go build ./cmd/arcade` is executed
- **THEN** a single `arcade` binary SHALL be produced containing all service implementations

