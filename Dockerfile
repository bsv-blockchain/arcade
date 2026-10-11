# syntax=docker/dockerfile:1
#
# Runtime-only image. The arcade binary is built in CI on a native runner per
# architecture and copied in here, so this Dockerfile contains no Go toolchain
# and no cross-compilation. To build locally, run `make docker-build` (which
# produces the dist/linux-<arch>/arcade layout this Dockerfile expects).
#
# The binary is built with CGO_ENABLED=1 — it links the BSV BDK C++ consensus
# library used by the transaction validator (see the validator package), so it
# is dynamically linked against glibc + libstdc++ and will NOT run on a musl
# base such as alpine. The distroless/cc base below provides glibc, libstdc++,
# libgcc and ca-certificates.
#
# ca-certificates is required: arcade is an outbound HTTPS client (Teranode,
# merkle service, datahub, webhook delivery) and TLS handshakes fail without
# the system CA bundle. distroless/cc ships it, so no extra install step.

# Intentionally not pinned to @sha256: — gcr.io/distroless/cc-debian12 is a
# multi-arch manifest list consumed by the matrix amd64/arm64 build in
# .github/workflows/build.yml, and dependabot's docker ecosystem
# (.github/dependabot.yml) tracks upstream changes. See Scorecard alert #58
# (dismissed: "won't fix").
FROM gcr.io/distroless/cc-debian12

ARG TARGETOS
ARG TARGETARCH
# Provenance labels. Empty for an ordinary developer image. A
# provenance-qualified build passes the source URL, the full git commit,
# the build time, and the version string, then checks that the image
# labels match the binary's embedded VCS stamp.
ARG ARCADE_IMAGE_SOURCE=""
ARG ARCADE_REVISION=""
ARG ARCADE_CREATED=""
ARG ARCADE_VERSION=""
LABEL org.opencontainers.image.source="${ARCADE_IMAGE_SOURCE}" \
      org.opencontainers.image.revision="${ARCADE_REVISION}" \
      org.opencontainers.image.created="${ARCADE_CREATED}" \
      org.opencontainers.image.version="${ARCADE_VERSION}"
COPY dist/${TARGETOS}-${TARGETARCH}/arcade /usr/local/bin/arcade

ENTRYPOINT ["/usr/local/bin/arcade"]
