# Deployment Guide

This guide covers deploying Arcade using Docker Compose or Kubernetes.

## Prerequisites

- A Teranode broadcast URL for the target network
- Arcade container image (`ghcr.io/bsv-blockchain/arcade:<tag>`) or source code to build from

## Configuration Reference

Arcade is configured via environment variables or a `config.yaml` file. Environment variables take precedence.

| Variable | Description | Default |
|---|---|---|
| `ARCADE_NETWORK` | BSV network: `main`, `test`, or `teratestnet` | `main` |
| `ARCADE_STORAGE_PATH` | Root directory for persistent data | `/data` |
| `ARCADE_DATABASE_SQLITE_PATH` | Path to SQLite database file | `/data/arcade.db` |
| `ARCADE_CHAINTRACKS_STORAGE_PATH` | Path for chain header storage | `/data/chaintracks` |
| `ARCADE_CHAINTRACKS_BOOTSTRAP_URL` | URL to headers.bin for initial header sync | _(none)_ |
| `ARCADE_TERANODE_BROADCAST_URLS` | Comma-separated Teranode propagation URLs | _(none)_ |
| `ARCADE_TERANODE_DATAHUB_URLS` | Comma-separated Teranode DataHub URLs (fallback) | _(none)_ |
| `ARCADE_TERANODE_AUTH_TOKEN` | **Bearer token for Teranode authentication (required)** | _(none)_ |
| `ARCADE_SERVER_ADDRESS` | Listen address | `:3011` |
| `ARCADE_LOG_LEVEL` | Log level: `debug`, `info`, `warn`, `error` | `info` |
| `ARCADE_AUTH_ENABLED` | Enable authentication | `false` |
| `ARCADE_AUTH_TOKEN` | Auth bearer token (if auth enabled) | _(none)_ |

See `config.example.yaml` for the full config file format.

## Health Check

The container exposes `GET /health` on port 3011. Use this for readiness/liveness probes and load balancer health checks.

---

## Docker Compose

The `docker-compose.yaml` includes production mainnet configuration by default.

### Authentication Required

⚠️ **You must provide a Teranode authentication token** to submit transactions:

```bash
export ARCADE_TERANODE_AUTH_TOKEN="your-token-here"
```

### Quick Start

```bash
# Start Arcade (uses mainnet by default)
docker compose up -d

# View logs
docker compose logs -f arcade

# Check health
curl http://localhost:3011/health

# Stop
docker compose down
```

Data is persisted in the `arcade-data` Docker volume and survives restarts.

### Using Different Networks

For testnet:
```bash
ARCADE_NETWORK=test \
ARCADE_TERANODE_BROADCAST_URLS="https://teranode-eks-testnet-eu-1-propagation.bsvb.tech,https://teranode-eks-testnet-us-1-propagation.bsvb.tech" \
ARCADE_TERANODE_DATAHUB_URLS="https://teranode-eks-testnet-eu-1.bsvb.tech/api/v1" \
docker compose up -d
```

For teratestnet:
```bash
ARCADE_NETWORK=teratestnet \
ARCADE_TERANODE_BROADCAST_URLS="https://teranode-eks-ttn-eu-1-propagation.bsvb.tech,https://teranode-eks-ttn-us-1-propagation.bsvb.tech" \
ARCADE_TERANODE_DATAHUB_URLS="https://teranode-eks-ttn-eu-1.bsvb.tech/api/v1" \
docker compose up -d
```

### Environment-only configuration (no config file)

If you prefer not to mount a config file, pass everything as environment variables:

```bash
docker run -d \
  --name arcade \
  -p 3011:3011 \
  -e ARCADE_NETWORK=main \
  -e ARCADE_TERANODE_BROADCAST_URLS="https://teranode-1.example.com,https://teranode-2.example.com" \
  -v arcade-data:/data \
  ghcr.io/bsv-blockchain/arcade:v0.1.6
```

---

## Kubernetes

### Concepts

Arcade uses SQLite for storage, so it must run as a **single replica** with a `Recreate` deployment strategy. A `PersistentVolumeClaim` provides durable storage across pod restarts.

### Namespace

Create a namespace for your environment:

```bash
kubectl create namespace arcade
```

### Persistent Volume Claim

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: arcade-data
  namespace: arcade
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

Adjust the `storageClassName` and size for your cluster.

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: arcade
  namespace: arcade
spec:
  selector:
    matchLabels:
      app: arcade
  strategy:
    type: Recreate
  template:
    metadata:
      labels:
        app: arcade
    spec:
      securityContext:
        fsGroup: 1000
      containers:
        - name: arcade
          image: ghcr.io/bsv-blockchain/arcade:v0.1.6
          ports:
            - containerPort: 3011
          env:
            - name: ARCADE_NETWORK
              value: "main"
            - name: ARCADE_STORAGE_PATH
              value: /data
            - name: ARCADE_DATABASE_SQLITE_PATH
              value: /data/arcade.db
            - name: ARCADE_TERANODE_BROADCAST_URLS
              value: "https://teranode-1.example.com,https://teranode-2.example.com"
            - name: ARCADE_CHAINTRACKS_STORAGE_PATH
              value: /data/chaintracks
          volumeMounts:
            - name: arcade-data
              mountPath: /data
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
      volumes:
        - name: arcade-data
          persistentVolumeClaim:
            claimName: arcade-data
```

Key points:
- **`strategy: Recreate`** is required because SQLite does not support concurrent writers.
- **`fsGroup: 1000`** matches the `arcade` user in the container image so the mounted volume is writable.
- Set `ARCADE_NETWORK` to `main`, `test`, or `teratestnet` and point `ARCADE_TERANODE_BROADCAST_URLS` to the appropriate Teranode propagation endpoints for that network.

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: arcade
  namespace: arcade
spec:
  selector:
    app: arcade
  ports:
    - port: 3011
      targetPort: 3011
  type: ClusterIP
```

### Ingress (optional)

Expose Arcade externally with an Ingress. The example below uses cert-manager for TLS; adapt the annotations and `ingressClassName` for your ingress controller:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: arcade
  namespace: arcade
  annotations:
    cert-manager.io/cluster-issuer: "letsencrypt"
spec:
  ingressClassName: nginx  # or traefik, etc.
  tls:
    - secretName: arcade-tls
      hosts:
        - arcade.example.com
  rules:
    - host: arcade.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: arcade
                port:
                  number: 3011
```

### Multiple Environments

To run mainnet, testnet, and teratestnet side by side, deploy each into its own namespace with environment-specific values:

| Environment | Namespace | `ARCADE_NETWORK` |
|---|---|---|
| Mainnet | `arcade` | `main` |
| Testnet | `arcade-testnet` | `test` |
| Teratestnet | `arcade-ttn` | `teratestnet` |

Each environment needs its own PVC, Deployment, Service, and (optionally) Ingress. The only differences between environments are the namespace, `ARCADE_NETWORK` value, and `ARCADE_TERANODE_BROADCAST_URLS`.

If you use Kustomize, you can keep the manifests above as a base and create overlays that patch the namespace, network, and broadcast URLs per environment.

---

## Updating

To deploy a new version:

1. Build and push a new image tag (or use a tag published by CI).
2. Update the image reference in your `docker-compose.yaml` or Kubernetes Deployment manifest.
3. Redeploy:
   - **Docker Compose:** `docker compose up -d`
   - **Kubernetes:** `kubectl apply -f deployment.yaml` or let your GitOps tool reconcile the change.

## Troubleshooting

### Docker Compose

```bash
docker compose logs -f arcade
docker compose exec arcade wget -qO- http://localhost:3011/health
```

### Kubernetes

```bash
kubectl -n arcade get pods
kubectl -n arcade logs deploy/arcade
kubectl -n arcade describe deploy/arcade
kubectl -n arcade get pvc arcade-data
kubectl -n arcade get ingress
```
