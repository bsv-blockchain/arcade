## ADDED Requirements

### Requirement: Register transaction via /watch endpoint
The merkle-service client SHALL register transactions by POSTing JSON `{txid, callbackUrl}` to the `/watch` endpoint of the configured merkle-service URL.

#### Scenario: Successful registration
- **WHEN** `Register(ctx, txid, callbackURL)` is called
- **THEN** it SHALL POST `{"txid": "<txid>", "callbackUrl": "<url>"}` to `{baseURL}/watch` and return nil on 2xx response

#### Scenario: Registration failure
- **WHEN** the merkle-service returns a non-2xx status code
- **THEN** `Register` SHALL return an error including the status code

### Requirement: Bearer token authentication
The client SHALL include `Authorization: Bearer {token}` header when an auth token is configured.

#### Scenario: Authenticated registration
- **WHEN** a client is created with authToken and registers a transaction
- **THEN** the request SHALL include the Authorization header

### Requirement: Configurable timeout
The client SHALL support a configurable HTTP timeout, defaulting to 30 seconds.

#### Scenario: Default timeout
- **WHEN** a client is created with timeout 0
- **THEN** the HTTP client SHALL use a 30-second timeout
