---
sidebar_position: 1
sidebar_label: Getting Started
description: Install and run loki-vl-proxy in minutes — binary, Docker, or Helm. Connect Grafana to VictoriaLogs using the native Loki datasource.
---

# Getting Started

## What You Need

- VictoriaLogs reachable from the proxy (`http://<host>:9428`)
- Grafana using a Loki datasource
- Go `1.26.3+` if running from source, or Docker for containerized runs

:::tip Latest release
Replace `<release>` throughout this page with the current version tag (no `v` prefix). The latest release is **`1.36.3`**. Check [GitHub Releases](https://github.com/ReliablyObserve/loki-vl-proxy/releases) for newer versions.
:::

## Quick Local Run

```bash
go build -o loki-vl-proxy ./cmd/proxy
./loki-vl-proxy -backend=http://127.0.0.1:9428
```

Proxy frontend defaults to `:3100`.

## Direct Binary — Maximum Loki Compatibility

Use this as your starting point when running the binary directly (no Docker, no Kubernetes). Copy the block, adjust the backend URL and listen address, and run.

```bash
./loki-vl-proxy \
  # ── Server ────────────────────────────────────────────────────────────────
  -listen=:3100 \                         # Grafana points its Loki datasource here
  -backend=http://127.0.0.1:9428 \        # VictoriaLogs HTTP API

  # ── Maximum Loki compatibility ─────────────────────────────────────────────
  # Translate OTel dotted labels (service.name → service_name) in every response.
  # Grafana label filters, variable queries, and LogQL all use underscores.
  -label-style=underscores \

  # Show only underscore-style labels in the Explore fields panel.
  # No dotted duplicates — the field list looks identical to real Loki.
  # Use "hybrid" instead if you need OTel trace correlation via service.name.
  -metadata-field-mode=translated \

  # Enable 3-tuple [timestamp, line, metadata] responses required by
  # Grafana 10+ Loki datasource for per-line label context in log details.
  -emit-structured-metadata=true \

  # Enable GET /loki/api/v1/patterns — the Patterns tab in Logs Drilldown.
  -patterns-enabled=true \

  # ── Tenant routing ─────────────────────────────────────────────────────────
  # Single-tenant (global VL default): X-Scope-OrgID "0", "fake", or "default"
  # map automatically to VictoriaLogs 0:0 — no flag needed.
  #
  # Named tenants: uncomment one option below.
  #
  # Option A — inline JSON (small, static maps):
  # -tenant-map='{"team-alpha":{"account_id":"1","project_id":"1"},"team-beta":{"account_id":"1","project_id":"2"}}' \
  #
  # Option B — file with hot-reload on SIGHUP (large or changing maps):
  # -tenant-map-file=/etc/loki-vl-proxy/tenant-map.yaml \
  # -tenant-map-reload-interval=30s \

  # ── Optional tuning ────────────────────────────────────────────────────────
  -log-level=info                         # debug | info | warn | error
```

**Environment-variable equivalent** — create an `.env` file and `source` it before running:

```bash
# /etc/loki-vl-proxy/loki-vl-proxy.env
VL_BACKEND_URL=http://127.0.0.1:9428
LISTEN_ADDR=:3100
LABEL_STYLE=underscores
METADATA_FIELD_MODE=translated
# TENANT_MAP='{"team-alpha":{"account_id":"1","project_id":"1"}}'
```

```bash
source /etc/loki-vl-proxy/loki-vl-proxy.env
./loki-vl-proxy -emit-structured-metadata=true -patterns-enabled=true
```

(`-emit-structured-metadata` and `-patterns-enabled` have no env-variable form; pass them as flags.)

**systemd unit** (production Linux hosts):

```ini
# /etc/systemd/system/loki-vl-proxy.service
[Unit]
Description=loki-vl-proxy — Loki-compatible proxy for VictoriaLogs
After=network.target

[Service]
User=loki-vl-proxy
EnvironmentFile=/etc/loki-vl-proxy/loki-vl-proxy.env
ExecStart=/usr/local/bin/loki-vl-proxy \
  -emit-structured-metadata=true \
  -patterns-enabled=true
Restart=on-failure
RestartSec=5s
# Protect the host — proxy needs no special privileges
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now loki-vl-proxy
sudo journalctl -u loki-vl-proxy -f
```

**Grafana datasource** — global single-tenant:

```yaml
datasources:
  - name: Logs
    type: loki
    url: http://localhost:3100
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: "0"   # or "fake" or "default" — all resolve to VL 0:0
```

For named tenants, set `httpHeaderValue1` to the org ID string that matches a key in your `-tenant-map`.

See [Configuration Reference](configuration.md) for the full flag list and [Translation Modes](translation-modes.md) for when to switch from `translated` to `hybrid`.

## Run With Docker

```bash
docker run --rm -p 3100:3100 ghcr.io/reliablyobserve/loki-vl-proxy:1.36.3 \
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
  --set-string extraArgs.otlp-endpoint=http://otel-collector.monitoring.svc.cluster.local:4318/v1/metrics \
  --set-string extraArgs.server\\.register-instrumentation=false

# 5) Indexed label-values browse cache (hotset + paging/search)
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set extraArgs.label-values-indexed-cache=true \
  --set extraArgs.label-values-hot-limit=200 \
  --set extraArgs.label-values-index-max-entries=200000 \
  --set extraArgs.label-values-index-persist-path=/cache/label-values-index.json \
  --set extraArgs.label-values-index-persist-interval=30s \
  --set extraArgs.label-values-index-startup-stale-threshold=60s \
  --set extraArgs.label-values-index-startup-peer-warm-timeout=5s
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

## Metrics Dashboard Out-Of-Box Setup

The `Loki-VL-Proxy Metrics` dashboard supports both scrape and OTLP push. Select datasource from the dashboard `Datasource` variable:

1. Scrape mode: choose the datasource fed by your `ServiceMonitor`/Prometheus scrape path.
2. OTLP push mode: choose the datasource fed by your OTLP metrics path.
3. If VictoriaMetrics receives both scrape and OTLP metrics, use the same `VictoriaMetrics` datasource for both modes.

Minimal checks:

```promql
loki_vl_proxy_uptime_seconds
```

If this returns data in the selected datasource, dashboard panels should render without extra query edits.

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
