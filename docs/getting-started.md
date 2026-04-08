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
- Mirror (when enabled in release secrets): `docker.io/slaskoss/loki-vl-proxy:<release>`

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
  --set image.repository=docker.io/slaskoss/loki-vl-proxy \
  --set extraArgs.backend=http://victorialogs:9428
```

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
