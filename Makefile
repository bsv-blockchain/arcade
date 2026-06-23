.PHONY: build test lint docker-up docker-down docker-build run

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

run:
	go run ./cmd/arcade
