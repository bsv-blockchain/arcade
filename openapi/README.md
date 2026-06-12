# OpenAPI specification

[`arcade.openapi.yaml`](./arcade.openapi.yaml) is the OpenAPI 3.0.3 description of
Arcade's HTTP API, derived from the Gin route handlers in `services/`.

It covers all three HTTP services, each of which binds its own port:

| Service       | Default port | Endpoints                                                                 |
|---------------|--------------|---------------------------------------------------------------------------|
| `api-server`  | `8080`       | `/`, `/health`, `/ready`, `/metrics`, `/tx`, `/txs`, `/tx/{txid}`, `/api/v1/*` |
| `sse`         | `8082`       | `/events`                                                                 |
| `chaintracks` | `8083`       | `/chaintracks/v1/*`, `/chaintracks/v2/*`                                   |

Operations served by a service other than `api-server` carry their own
`servers` override so the correct port is documented per-endpoint.

## Source of truth

The route table lives in `services/api_server/routes.go` (plus
`block_status_routes.go` / `reprocess_routes.go`), `services/sse/service.go`,
and `services/chaintracks_server/routes.go`. Response/request schemas come from
the `models` package. Keep this spec in sync when those change.

## Validate / preview

```sh
# Lint
npx @redocly/cli lint openapi/arcade.openapi.yaml

# Bundle (resolve $refs into a single file)
npx @redocly/cli bundle openapi/arcade.openapi.yaml -o bundle.yaml

# Preview rendered docs
npx @redocly/cli preview-docs openapi/arcade.openapi.yaml
```

The remaining lint warnings are intentional: the `localhost` / `example.com`
server URLs document local-dev and example deployments, and the
`no-ambiguous-paths` warning reflects the real (gin-resolved) routes
`/api/v1/blocks/processing-status/{blockHash}` and
`/api/v1/blocks/{blockHash}/reprocess`.
