## ADDED Requirements

### Requirement: Root route serves API documentation page
The server SHALL serve an HTML documentation page at `GET /` that lists all available API routes.

#### Scenario: Visiting the root URL
- **WHEN** a client sends `GET /`
- **THEN** the server responds with HTTP 200 and `Content-Type: text/html`
- **THEN** the response body contains an HTML page

### Requirement: Documentation page lists all routes
The documentation page SHALL display each API route with its HTTP method, path, and a brief description.

#### Scenario: All routes are shown
- **WHEN** the documentation page is rendered
- **THEN** it lists the following routes: `GET /health`, `GET /ready`, `GET /tx/:txid`, `POST /tx`, `POST /api/v1/merkle-service/callback`

#### Scenario: Each route shows method and path
- **WHEN** a route entry is displayed
- **THEN** it shows the HTTP method (e.g. GET, POST) and the URL path

### Requirement: Documentation page shows request and response formats
Each route entry on the documentation page SHALL include its expected request format and response format.

#### Scenario: Route with JSON request body
- **WHEN** the `POST /tx` route is displayed
- **THEN** it shows the supported request content types (application/octet-stream, text/plain, application/json) and the response format

#### Scenario: Route with path parameter
- **WHEN** the `GET /tx/:txid` route is displayed
- **THEN** it shows that `txid` is a path parameter and describes the response fields
