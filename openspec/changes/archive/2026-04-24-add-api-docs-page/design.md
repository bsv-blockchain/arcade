## Context

The Arcade API server (Gin-based, in `services/api_server/`) has 5 routes but no discovery mechanism. The root `/` path returns a 404. Adding a docs page there gives developers immediate visibility into what the API offers.

## Goals / Non-Goals

**Goals:**
- Serve a human-readable HTML page at `GET /` listing all routes
- Keep route metadata co-located with route registration so docs stay in sync

**Non-Goals:**
- OpenAPI/Swagger spec generation
- Interactive API explorer (e.g. try-it-now forms)
- Machine-readable JSON docs endpoint

## Decisions

### 1. Route metadata as a Go slice, rendered with html/template

Define a `[]RouteDoc` slice in `routes.go` alongside the existing `registerRoutes()` function. An `html/template` renders it into a single self-contained HTML page.

**Rationale:** Keeps metadata next to route definitions so they're updated together. No external dependencies — `html/template` is stdlib and auto-escapes output.

**Alternative considered:** Generating docs by reflecting on Gin's registered routes at runtime. Rejected because Gin's route tree only exposes method + path, not descriptions or request/response formats.

### 2. Inline HTML template string (no external file)

Embed the template as a Go string constant rather than loading from a file.

**Rationale:** Simpler deployment (single binary, no static assets to ship). The template is small enough that a constant is manageable.

## Risks / Trade-offs

- **[Docs can drift from code]** → Mitigated by placing the route metadata slice directly in `routes.go`, right next to `registerRoutes()`. A developer adding a route sees the docs list in the same file.
- **[HTML styling is minimal]** → Acceptable for a developer-facing page. Can be enhanced later without changing the architecture.
