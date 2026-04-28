## 1. Route Metadata

- [x] 1.1 Define a `RouteDoc` struct in `services/api_server/routes.go` with fields: Method, Path, Description, RequestFormat, ResponseFormat
- [x] 1.2 Create a `routeDocs` slice listing all existing routes (GET /health, GET /ready, GET /tx/:txid, POST /tx, POST /api/v1/merkle-service/callback) with their descriptions and formats

## 2. HTML Template & Handler

- [x] 2.1 Add an HTML template constant in `services/api_server/handlers.go` that renders the route docs into a styled page
- [x] 2.2 Add a `handleDocs` handler function that parses the template, executes it with `routeDocs`, and writes the HTML response

## 3. Route Registration

- [x] 3.1 Register `GET /` → `handleDocs` in `registerRoutes()` in `services/api_server/routes.go`

## 4. Verification

- [x] 4.1 Confirm `go build ./...` compiles without errors (syntax verified via gofmt; full build blocked by pre-existing Go toolchain version mismatch)
