# Getting Started

## What You Need

- VictoriaLogs reachable from the proxy (`http://<host>:9428`)
- Grafana using a Loki datasource
- Go `1.26.1+` if running from source, or Docker for containerized runs

## Quick Local Run

```bash
go build -o loki-vl-proxy ./cmd/proxy
./loki-vl-proxy -backend=http://127.0.0.1:9428
```

Proxy frontend defaults to `:3100`.

## Run With Docker

```bash
docker run --rm -p 3100:3100 ghcr.io/reliablyobserve/loki-vl-proxy:<release> \
  -backend=http://host.docker.internal:9428
```

Docker image sources:

- Primary: `ghcr.io/reliablyobserve/loki-vl-proxy:<release>`
- Mirror (when enabled in release secrets): `docker.io/reliablyobserve/loki-vl-proxy:<release>`

## Install With Helm

```bash
helm install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428
```

The chart defaults to image `ghcr.io/reliablyobserve/loki-vl-proxy`. If you need Docker Hub:

```bash
helm install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set image.repository=docker.io/reliablyobserve/loki-vl-proxy \
  --set extraArgs.backend=http://victorialogs:9428
```

### Helm Deployment Recipes

```bash
# 1) Basic deployment
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428

# 2) StatefulSet + persistent disk cache
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set workload.kind=StatefulSet \
  --set persistence.enabled=true \
  --set persistence.size=20Gi \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set extraArgs.disk-cache-max-bytes=20Gi

# 3) Multi-replica fleet with peer cache
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set replicaCount=3 \
  --set peerCache.enabled=true \
  --set extraArgs.backend=http://victorialogs:9428

# 4) OTLP metrics push to in-cluster collector
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set-string extraArgs.metrics\\.otlp-endpoint=http://otel-collector.monitoring.svc.cluster.local:4318/v1/metrics \
  --set-string extraArgs.server\\.register-instrumentation=false
```

### Image Selection

Default chart image:
- `ghcr.io/reliablyobserve/loki-vl-proxy:<release>`

Alternative published image:
- `docker.io/reliablyobserve/loki-vl-proxy:<release>`

Explicit image override:

```bash
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set image.repository=docker.io/reliablyobserve/loki-vl-proxy \
  --set image.tag=<release> \
  --set extraArgs.backend=http://victorialogs:9428
```

For additional production guidance, see [Operations](operations.md).

## Grafana Datasource

```yaml
datasources:
  - name: Loki (via VL proxy)
    type: loki
    access: proxy
    url: http://loki-vl-proxy:3100
```

## Verify The Setup

```bash
curl -sS http://127.0.0.1:3100/ready
curl -sS http://127.0.0.1:3100/loki/api/v1/labels
```

If `/ready` is not `ok`, check backend connectivity and proxy logs first.

## Next Docs

- [Configuration](configuration.md)
- [Operations](operations.md)
- [Testing](testing.md)
- [Compatibility Matrix](compatibility-matrix.md)
