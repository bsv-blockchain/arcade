# Docker Guide for Arcade

This guide covers building, running, and deploying Arcade using Docker.

## Quick Start

### Using Docker Compose (Recommended for Local Development)

1. **Create a config file:**

```bash
cp config.example.yaml config.yaml
# Edit config.yaml and set at least one teranode.broadcast_urls
```

2. **Start Arcade:**

```bash
docker compose up -d
```

3. **View logs:**

```bash
docker compose logs -f arcade
```

4. **Stop Arcade:**

```bash
docker compose down
```

### Using Docker Directly

1. **Build the image:**

```bash
docker build -t arcade:latest .
```

2. **Run the container:**

```bash
docker run -d \
  --name arcade \
  -p 3011:3011 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v arcade-data:/data \
  arcade:latest
```

## Building

### Basic Build

```bash
docker build -t arcade:latest .
```

### Build with Version Information

```bash
docker build \
  --build-arg VERSION=0.1.0 \
  --build-arg COMMIT=$(git rev-parse --short HEAD) \
  --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
  -t arcade:0.1.0 \
  .
```

### Multi-Architecture Build

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --build-arg VERSION=0.1.0 \
  -t your-registry/arcade:0.1.0 \
  --push \
  .
```

## Configuration

### Configuration Methods (in order of precedence)

1. **Environment Variables** - Highest priority
2. **Mounted Config File** - Common for production
3. **Default Values** - Fallback

### Environment Variable Configuration

```bash
docker run -d \
  --name arcade \
  -p 3011:3011 \
  -e ARCADE_NETWORK=main \
  -e ARCADE_LOG_LEVEL=debug \
  -e ARCADE_TERANODE_BROADCAST_URLS="https://arc.taal.com" \
  -e ARCADE_SERVER_ADDRESS=:3011 \
  -v arcade-data:/data \
  arcade:latest
```

### Config File Mount (Recommended)

```bash
# Create config.yaml with your settings
cat > config.yaml <<EOF
network: main
storage_path: /data

server:
  address: ":3011"

teranode:
  broadcast_urls:
    - "https://arc.taal.com"
  timeout: 30s

database:
  type: sqlite
  sqlite_path: /data/arcade.db

events:
  type: memory
  buffer_size: 1000

validator:
  min_fee_per_kb: 100
EOF

# Run with mounted config
docker run -d \
  --name arcade \
  -p 3011:3011 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v arcade-data:/data \
  arcade:latest
```

## Data Persistence

### Using Named Volumes (Recommended)

```bash
# Create named volume
docker volume create arcade-data

# Run with named volume
docker run -d \
  --name arcade \
  -p 3011:3011 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v arcade-data:/data \
  arcade:latest

# Inspect volume
docker volume inspect arcade-data

# Backup volume
docker run --rm \
  -v arcade-data:/data \
  -v $(pwd)/backup:/backup \
  alpine tar czf /backup/arcade-backup-$(date +%Y%m%d).tar.gz -C /data .

# Restore volume
docker run --rm \
  -v arcade-data:/data \
  -v $(pwd)/backup:/backup \
  alpine tar xzf /backup/arcade-backup-20240115.tar.gz -C /data
```

### Using Bind Mounts

```bash
# Create data directory
mkdir -p ./data

# Set permissions (UID/GID 1000 for arcade user)
chown -R 1000:1000 ./data

# Run with bind mount
docker run -d \
  --name arcade \
  -p 3011:3011 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/data:/data \
  arcade:latest
```

## Networking

### Expose Ports

```bash
# Default port 3011
docker run -d \
  --name arcade \
  -p 3011:3011 \
  arcade:latest

# Custom external port
docker run -d \
  --name arcade \
  -p 8080:3011 \
  arcade:latest
```

### Connect to External Services

```bash
# Use host network (not recommended for production)
docker run -d \
  --name arcade \
  --network host \
  arcade:latest

# Use custom network
docker network create arcade-net
docker run -d \
  --name arcade \
  --network arcade-net \
  -p 3011:3011 \
  arcade:latest
```

## Health Checks

The container includes a built-in health check that queries the `/health` endpoint.

### View Health Status

```bash
docker ps
# Look for STATUS column showing "healthy" or "unhealthy"

# Detailed health check info
docker inspect --format='{{json .State.Health}}' arcade | jq
```

### Custom Health Check

```bash
# Override default health check
docker run -d \
  --name arcade \
  --health-cmd="wget --no-verbose --tries=1 --spider http://localhost:3011/health || exit 1" \
  --health-interval=10s \
  --health-timeout=3s \
  --health-retries=3 \
  -p 3011:3011 \
  arcade:latest
```

## Security

### Run as Non-Root User

The container automatically runs as user `arcade` (UID 1000, GID 1000) for security.

### Read-Only Root Filesystem

```bash
docker run -d \
  --name arcade \
  --read-only \
  -v arcade-data:/data \
  -v /tmp \
  -p 3011:3011 \
  arcade:latest
```

### Resource Limits

```bash
docker run -d \
  --name arcade \
  --memory="512m" \
  --memory-swap="1g" \
  --cpus="2" \
  --pids-limit=100 \
  -p 3011:3011 \
  arcade:latest
```

### Security Options

```bash
docker run -d \
  --name arcade \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  -p 3011:3011 \
  arcade:latest
```

## Production Deployment

### Docker Compose Production Example

```yaml
version: '3.8'

services:
  arcade:
    image: your-registry/arcade:0.1.0
    container_name: arcade
    restart: always
    ports:
      - "3011:3011"
    volumes:
      - ./config.yaml:/app/config.yaml:ro
      - arcade-data:/data
    environment:
      ARCADE_LOG_LEVEL: info
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:3011/health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 512M
        reservations:
          cpus: '0.5'
          memory: 256M
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    read_only: true
    tmpfs:
      - /tmp:noexec,nosuid,size=100m

volumes:
  arcade-data:
    driver: local
```

### Kubernetes Deployment Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: arcade
spec:
  replicas: 3
  selector:
    matchLabels:
      app: arcade
  template:
    metadata:
      labels:
        app: arcade
    spec:
      containers:
      - name: arcade
        image: your-registry/arcade:0.1.0
        ports:
        - containerPort: 3011
          name: http
        env:
        - name: ARCADE_NETWORK
          value: "main"
        - name: ARCADE_LOG_LEVEL
          value: "info"
        volumeMounts:
        - name: config
          mountPath: /app/config.yaml
          subPath: config.yaml
          readOnly: true
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "256Mi"
            cpu: "500m"
          limits:
            memory: "512Mi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 3011
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 3011
          initialDelaySeconds: 5
          periodSeconds: 10
        securityContext:
          runAsNonRoot: true
          runAsUser: 1000
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
          readOnlyRootFilesystem: true
      volumes:
      - name: config
        configMap:
          name: arcade-config
      - name: data
        persistentVolumeClaim:
          claimName: arcade-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: arcade
spec:
  selector:
    app: arcade
  ports:
  - protocol: TCP
    port: 3011
    targetPort: 3011
  type: ClusterIP
```

## Logging

### View Logs

```bash
# Follow logs
docker logs -f arcade

# Last 100 lines
docker logs --tail 100 arcade

# Logs since specific time
docker logs --since 1h arcade

# Logs with timestamps
docker logs -t arcade
```

### Configure Log Driver

```bash
# JSON file logging with rotation
docker run -d \
  --name arcade \
  --log-driver json-file \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  -p 3011:3011 \
  arcade:latest

# Syslog logging
docker run -d \
  --name arcade \
  --log-driver syslog \
  --log-opt syslog-address=udp://192.168.1.100:514 \
  -p 3011:3011 \
  arcade:latest
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs arcade

# Inspect container
docker inspect arcade

# Check if port is already in use
sudo netstat -tlnp | grep 3011

# Run interactively for debugging
docker run -it --rm \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  arcade:latest /bin/sh
```

### Permission Issues

```bash
# Check data directory permissions
ls -la data/

# Fix permissions (data directory)
chown -R 1000:1000 ./data

# Run with different user (not recommended)
docker run -d \
  --name arcade \
  --user root \
  -p 3011:3011 \
  arcade:latest
```

### Database Locked

```bash
# SQLite database is locked by another process
# Stop all containers using the database
docker stop arcade

# Check for stale lock files
ls -la data/*.db-shm data/*.db-wal

# Remove stale lock files (only if container is stopped!)
rm -f data/*.db-shm data/*.db-wal

# Restart container
docker start arcade
```

### Network Connectivity

```bash
# Test network from container
docker exec arcade wget -O- https://arc.taal.com

# Check DNS resolution
docker exec arcade nslookup arc.taal.com

# Test connection to Teranode
docker exec arcade wget --spider https://arc.taal.com/tx
```

## Maintenance

### Update Container

```bash
# Pull new image
docker pull your-registry/arcade:latest

# Stop and remove old container
docker stop arcade
docker rm arcade

# Start new container
docker run -d \
  --name arcade \
  -p 3011:3011 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v arcade-data:/data \
  your-registry/arcade:latest
```

### Cleanup

```bash
# Remove stopped containers
docker container prune

# Remove unused images
docker image prune

# Remove unused volumes (CAUTION: may delete data!)
docker volume prune

# Remove everything unused
docker system prune -a
```

## Best Practices

1. **Always use named volumes** for data persistence
2. **Mount config as read-only** (-v config.yaml:/app/config.yaml:ro)
3. **Use specific version tags** instead of :latest in production
4. **Set resource limits** to prevent resource exhaustion
5. **Enable health checks** for automatic restart on failure
6. **Run as non-root user** (default in this image)
7. **Use secrets management** for sensitive config (auth tokens)
8. **Implement log rotation** to prevent disk space issues
9. **Backup volumes regularly** using automated scripts
10. **Monitor container metrics** (CPU, memory, network)

## Additional Resources

- [Dockerfile Reference](https://docs.docker.com/engine/reference/builder/)
- [Docker Compose File Reference](https://docs.docker.com/compose/compose-file/)
- [Docker Security Best Practices](https://docs.docker.com/engine/security/)
- [Arcade Configuration Guide](README.md#configuration)
