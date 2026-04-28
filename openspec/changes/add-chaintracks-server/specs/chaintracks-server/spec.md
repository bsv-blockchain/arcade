## ADDED Requirements

### Requirement: Default-on chaintracks initialization in api-server

The `api-server` service SHALL initialize an embedded `go-chaintracks` instance on startup whenever `chaintracks_server.enabled` is `true` (the default) AND the active run mode is `all` or `api-server`. Other modes (`tx-validator`, `propagation`, `bump-builder`) MUST NOT initialize chaintracks.

#### Scenario: Default config starts chaintracks alongside api-server
- **WHEN** arcade is started with `mode=all` and no overrides to `chaintracks_server`
- **THEN** the api-server logs `Chaintracks HTTP API enabled` at info level
- **AND** routes under `/chaintracks/v1/*` and `/chaintracks/v2/*` respond with 2xx for valid requests
- **AND** the chaintracks storage directory is created at `<storage_path>/chaintracks/`

#### Scenario: Explicitly disabled skips initialization
- **WHEN** arcade is started with `chaintracks_server.enabled: false`
- **THEN** no chaintracks instance is created
- **AND** GET `/chaintracks/v1/height` responds with 404
- **AND** no `go-teranode-p2p-client` connections are established by arcade

#### Scenario: Non-api-server modes do not start chaintracks
- **WHEN** arcade is started with `mode=tx-validator` and default `chaintracks_server.enabled`
- **THEN** no chaintracks instance is created and no chaintracks routes are registered

### Requirement: Chaintracks HTTP routes on Gin router

When chaintracks is enabled, the api-server SHALL register HTTP routes that mirror the `go-chaintracks` v1 and v2 URL surface on the existing Gin router. The routes MUST call the `chaintracks.Chaintracks` interface for all data access (no direct storage access from the handlers).

#### Scenario: Network endpoint returns configured network
- **WHEN** a client GETs `/chaintracks/v1/network`
- **THEN** the response is 200 with body `{"network":"mainnet"}` (or the configured network name)

#### Scenario: Height endpoint returns current tip height
- **WHEN** a client GETs `/chaintracks/v1/height`
- **THEN** the response is 200 with body `{"height":<uint32>}`

#### Scenario: Header-by-height returns JSON header
- **WHEN** a client GETs `/chaintracks/v1/header/height/100`
- **THEN** the response is 200 with a JSON body containing at minimum `hash`, `height`, `version`, `prevHash`, `merkleRoot`, `time`, `bits`, `nonce`

#### Scenario: Header-by-hash returns JSON header
- **WHEN** a client GETs `/chaintracks/v1/header/hash/<64-hex>`
- **THEN** the response is 200 with the corresponding header JSON, OR 404 if unknown

#### Scenario: Binary header endpoint returns 80-byte header
- **WHEN** a client GETs `/chaintracks/v1/header/height/100.bin`
- **THEN** the response is 200, Content-Type `application/octet-stream`, body exactly 80 bytes (the raw block header), and the `X-Block-Height` response header carries the matching height as a decimal string

#### Scenario: Unknown route under /chaintracks returns 404
- **WHEN** a client GETs `/chaintracks/v1/nonexistent`
- **THEN** the response is 404

### Requirement: Tip and reorg Server-Sent Events streams

When chaintracks is enabled, `/chaintracks/v2/tip/stream` and `/chaintracks/v2/reorg/stream` SHALL emit Server-Sent Events. The server MUST keep the connection open until the client disconnects or the service shuts down, and MUST forward every tip / reorg event produced by the underlying `chaintracks.Subscribe` / `chaintracks.SubscribeReorg` channels.

#### Scenario: Tip stream delivers new-tip events
- **GIVEN** a client has an open `GET /chaintracks/v2/tip/stream` connection
- **WHEN** the embedded chaintracks sees a new block tip
- **THEN** the client receives an SSE event whose JSON payload contains the new tip's `hash` and `height`

#### Scenario: Client disconnect frees server goroutine
- **GIVEN** a client is connected to `/chaintracks/v2/tip/stream`
- **WHEN** the client closes the connection
- **THEN** the server releases its per-connection goroutine within one tip event
- **AND** no goroutine leak is observable after shutdown

#### Scenario: Service shutdown closes all streams
- **WHEN** the api-server's context is cancelled
- **THEN** all active SSE connections are closed cleanly (no panics, no half-written frames) and their goroutines exit

### Requirement: Static file serving for bulk header downloads

When chaintracks is enabled, the api-server SHALL serve the contents of the chaintracks storage directory at `/chaintracks/<file>` as static files, supporting HTTP Range requests for partial downloads of the larger header files.

#### Scenario: Full download of a headers file
- **GIVEN** `<storage_path>/chaintracks/mainNet_0.headers` exists
- **WHEN** a client GETs `/chaintracks/mainNet_0.headers`
- **THEN** the response is 200 with the file's full contents

#### Scenario: Range request returns a byte slice
- **GIVEN** `<storage_path>/chaintracks/mainNet_0.headers` exists
- **WHEN** a client GETs `/chaintracks/mainNet_0.headers` with `Range: bytes=0-8000`
- **THEN** the response is 206 Partial Content with bytes 0-8000 of the file

#### Scenario: Directory listing is disabled
- **WHEN** a client GETs `/chaintracks/` (the directory itself)
- **THEN** the response is 403 or 404 (not a directory listing)

### Requirement: Lifecycle tied to api-server context

The chaintracks instance's lifecycle SHALL be driven by the `context.Context` passed to `api_server.Server.Start`. When the context is cancelled, all chaintracks-owned goroutines (P2P subscription, SSE broadcasters) MUST exit.

#### Scenario: Graceful shutdown
- **GIVEN** chaintracks is running and has active P2P peers and one SSE client
- **WHEN** the api-server receives SIGTERM
- **THEN** within the shutdown grace period, the process exits cleanly with no leaked goroutines, no "use of closed network connection" panics surfaced to the log, and a final `INFO` log message indicating shutdown

### Requirement: Configuration surface

The config schema SHALL expose the following new fields, all with safe defaults so an operator can start arcade with no chaintracks config at all:

- `chaintracks_server.enabled` (`bool`, default `true`) — gates whether routes and the instance run.
- `chaintracks.*` — delegated in full to `github.com/bsv-blockchain/go-chaintracks/config.Config.SetDefaults`, with our `storage_path` defaulting to `<arcade_storage_path>/chaintracks`.

#### Scenario: Missing chaintracks block in config.yaml uses library defaults
- **WHEN** arcade starts with no `chaintracks:` section in config.yaml
- **THEN** config loads without error
- **AND** chaintracks runs with `mode=embedded`, `network=main`, storage under the arcade storage path, and the library's default bootstrap mode

#### Scenario: Operator overrides storage path
- **GIVEN** `chaintracks.storage_path=/srv/arcade/chaintracks` is set
- **WHEN** arcade starts
- **THEN** headers are written to `/srv/arcade/chaintracks/`, NOT `<arcade_storage_path>/chaintracks/`
