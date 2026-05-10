# syntax=docker/dockerfile:1
#
# Runtime-only image. The arcade binary is cross-compiled in CI on a native
# runner per architecture and copied in here, so this Dockerfile contains no
# Go toolchain and no cross-compilation. To build locally, run `make
# docker-build` (which produces the dist/linux-<arch>/arcade layout this
# Dockerfile expects).
#
# ca-certificates is required: arcade is an outbound HTTPS client (Teranode,
# merkle service, datahub, webhook delivery) and TLS handshakes fail without
# the system CA bundle.

# Intentionally not pinned to @sha256: — alpine:3.23 is a multi-arch manifest
# list consumed by the matrix amd64/arm64 build in .github/workflows/build.yml,
# and dependabot's docker ecosystem (.github/dependabot.yml) already tracks
# upstream changes. See Scorecard alert #58 (dismissed: "won't fix").
FROM alpine:3.23

RUN apk --no-cache add ca-certificates

ARG TARGETOS
ARG TARGETARCH
COPY dist/${TARGETOS}-${TARGETARCH}/arcade /usr/local/bin/arcade

ENTRYPOINT ["arcade"]
