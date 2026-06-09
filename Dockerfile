# syntax=docker/dockerfile:1
#
# Runtime-only image. The arcade binary is built in CI on a native runner per
# architecture (CGO_ENABLED=1, since transaction validation links go-bdk's C++
# script engine) and copied in here, so this Dockerfile contains no Go toolchain
# and no cross-compilation. To build locally, run `make docker-build` (which
# produces the dist/linux-<arch>/arcade layout this Dockerfile expects).
#
# Base image: arcade now links go-bdk (cgo), so the binary is dynamically linked
# against glibc and libstdc++. The previous static-musl Alpine base cannot run a
# glibc binary, so the runtime is a glibc image (debian-slim) with libstdc++6.
# The go-bdk native library itself is statically linked into the binary, so no
# extra shared object or LD_LIBRARY_PATH is required.
#
# Required runtime packages:
#   - ca-certificates: arcade is an outbound HTTPS client (Teranode, merkle
#     service, datahub, webhook delivery) and TLS handshakes fail without the
#     system CA bundle.
#   - libstdc++6: go-bdk's C++ engine dynamically links the GNU C++ runtime.

# Intentionally not pinned to @sha256: — debian:bookworm-slim is a multi-arch
# manifest list consumed by the matrix amd64/arm64 build in
# .github/workflows/build.yml, and dependabot's docker ecosystem
# (.github/dependabot.yml) already tracks upstream changes.
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETOS
ARG TARGETARCH
COPY dist/${TARGETOS}-${TARGETARCH}/arcade /usr/local/bin/arcade

ENTRYPOINT ["arcade"]
