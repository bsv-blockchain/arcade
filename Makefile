.PHONY: build test lint docker-up docker-down docker-build run

GOARCH ?= $(shell go env GOARCH)

build:
	go build ./...

test:
	go test ./...

lint:
	golangci-lint run

docker-up:
	podman-compose up -d

docker-down:
	podman-compose down

# Build a single-arch image for local use that matches the CI layout. The
# Dockerfile expects a binary at dist/linux-<arch>/arcade; this target produces
# that layout for the host's architecture and tags the image arcade:local.
#
# CGO_ENABLED=1 is required: transaction validation links go-bdk's C++ script
# engine. The build is therefore native (no cross-compilation) — run this on a
# linux host whose arch matches $(GOARCH), with a C++ toolchain (g++) and
# libstdc++ dev headers available.
docker-build:
	mkdir -p dist/linux-$(GOARCH)
	CGO_ENABLED=1 GOOS=linux GOARCH=$(GOARCH) go build -trimpath -ldflags="-s -w" -o dist/linux-$(GOARCH)/arcade ./cmd/arcade
	docker build --platform=linux/$(GOARCH) -t arcade:local .

run:
	go run ./cmd/arcade
