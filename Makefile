.PHONY: build test lint docker-up docker-down docker-build run

GOARCH ?= $(shell go env GOARCH)

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
docker-build:
	mkdir -p dist/linux-$(GOARCH)
	CGO_ENABLED=1 GOOS=linux GOARCH=$(GOARCH) go build -trimpath -ldflags="-s -w" -o dist/linux-$(GOARCH)/arcade ./cmd/arcade
	docker build --platform=linux/$(GOARCH) -t arcade:local .

run:
	go run ./cmd/arcade
