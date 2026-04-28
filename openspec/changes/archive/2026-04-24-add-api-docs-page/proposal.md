## Why

There is no documentation endpoint on the API server. When hitting the root `/` route, there's no response — making it hard to discover available routes and their expected request/response formats without reading source code.

## What Changes

- Add a new `GET /` route that serves an HTML documentation page listing all API routes
- The page will show each route's HTTP method, path, description, request format, and response format
- Built from route metadata defined in code so it stays in sync with the actual API

## Capabilities

### New Capabilities
- `api-docs-page`: An HTML documentation page served at `GET /` that lists all API routes with their methods, paths, descriptions, and request/response formats

### Modified Capabilities

_(none)_

## Impact

- **Code**: New handler and route registration in `services/api_server/`
- **APIs**: Adds `GET /` — no impact on existing routes
- **Dependencies**: None — uses Go's `html/template` from the standard library
