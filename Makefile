.PHONY: build test lint docker-up docker-down docker-build provenance-build provenance-verify provenance-negative run

GOARCH ?= $(shell go env GOARCH)

# Version stamped into the binary (exposed via GET /health, issue #208). Derived
# from the git tag; falls back to "dev" outside a tagged checkout.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# CGO is required: the transaction validator links the BSV BDK C++ consensus
# library (gobdk) via cgo, so a C/C++ toolchain (gcc/g++, libstdc++) must be
# present to build and test. Builds are native per arch — gobdk ships a
# prebuilt static archive per platform, so there is no cross-compilation.
build:
	CGO_ENABLED=1 go build ./...

test:
	CGO_ENABLED=1 go test ./...

lint:
	golangci-lint run

# Full microservices stack (see docker-compose.yaml). Runs the published
# image by default; ARCADE_IMAGE=arcade:local make docker-up runs a working
# tree build produced by `make docker-build`.
docker-up:
	podman-compose up -d

docker-down:
	podman-compose down

# Build a single-arch image for local use that matches the CI layout. The
# Dockerfile expects a binary at dist/linux-<arch>/arcade; this target produces
# that layout for the host's architecture and tags the image arcade:local.
# Requires a Linux host: CGO + gobdk's per-platform static archive cannot
# cross-compile to linux, so building the binary off-Linux would fail.
docker-build:
	@if [ "$$(go env GOOS)" != "linux" ]; then \
		echo "docker-build requires a Linux host: CGO + gobdk cannot cross-compile a linux binary from $$(go env GOOS). Run on Linux or inside a Linux container."; \
		exit 1; \
	fi
	mkdir -p dist/linux-$(GOARCH)
	CGO_ENABLED=1 GOOS=linux GOARCH=$(GOARCH) go build -trimpath -ldflags="-s -w -X github.com/bsv-blockchain/arcade/version.Version=$(VERSION)" -o dist/linux-$(GOARCH)/arcade ./cmd/arcade
	docker build --platform=linux/$(GOARCH) -t arcade:local .

# Fail-closed release candidate. Requires a clean full commit, a Linux host,
# Docker, and syft. See docs/container-build-provenance.md. Does not publish.
provenance-build:
	bash scripts/provenance-build.sh

# Inspect a locally built provenance image.
IMAGE ?= arcade:provenance
provenance-verify:
	bash scripts/verify-image-provenance.sh "$(IMAGE)"

# Reject a mismatched revision, a non-SHA revision, and a dirty tree.
# Restores the worktree. Requires a clean checkout.
provenance-negative:
	bash scripts/provenance-negative.sh

run:
	go run ./cmd/arcade
