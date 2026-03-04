# syntax=docker/dockerfile:1

# Build stage
FROM golang:1.25.4-alpine AS builder

# Install build dependencies
# build-base: includes gcc, g++, make and other build tools for CGO
RUN apk add --no-cache \
    git \
    ca-certificates \
    tzdata \
    build-base

WORKDIR /build

# Copy dependency files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build arguments for version information
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

# Build the application with optimizations
# -ldflags explanation:
#   -s: omit symbol table
#   -w: omit DWARF debug info
RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-s -w" \
    -trimpath \
    -o arcade \
    ./cmd/arcade

# Runtime stage - using Alpine for small size and C library compatibility (SQLite)
FROM alpine:3.21

# Install runtime dependencies
# ca-certificates: for HTTPS connections to Teranode
# tzdata: for proper timezone handling
RUN apk add --no-cache \
    ca-certificates \
    tzdata

# Create non-root user for security
RUN addgroup -g 1000 arcade && \
    adduser -D -u 1000 -G arcade arcade

# Create data directory with proper permissions
RUN mkdir -p /data && \
    chown -R arcade:arcade /data

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/arcade /app/arcade

# Copy example config (users should mount their own config)
COPY --from=builder /build/config.example.yaml /app/config.example.yaml

# Switch to non-root user
USER arcade

# Expose default HTTP port
EXPOSE 3011

# Set default environment variables
ENV ARCADE_STORAGE_PATH=/data \
    ARCADE_SERVER_ADDRESS=:3011 \
    ARCADE_LOG_LEVEL=info

# Volume for persistent data (SQLite database, chain state, etc.)
VOLUME ["/data"]

# Health check using the /health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3011/health || exit 1

# Run the application
# Users should provide config via:
# - Volume mount: -v /path/to/config.yaml:/app/config.yaml
# - Environment variables: ARCADE_* prefix
CMD ["/app/arcade", "-config", "/app/config.yaml"]
