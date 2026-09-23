# loki-vl-proxy Helm Chart

Kubernetes Helm chart for [loki-vl-proxy](https://reliablyobserve.github.io/loki-vl-proxy/) — a read-only Loki-compatible proxy that translates LogQL to VictoriaLogs LogsQL.

## Quick Install

```bash
helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --set extraArgs.backend=http://victorialogs:9428
```

## Configuration

| Value | Default | Description |
|---|---|---|
| `extraArgs.backend` | `http://victorialogs:9428` | VictoriaLogs backend URL |
| `extraArgs.listen` | `:3100` | Proxy listen address |
| `extraArgs.cache-ttl` | `60s` | In-memory cache entry TTL |
| `extraArgs.cache-max` | `50000` | Maximum in-memory cache entries |
| `extraArgs.label-style` | _(binary default: underscores)_ | Label translation mode: `passthrough` or `underscores` |
| `extraArgs.metadata-field-mode` | _(binary default: translated)_ | Structured metadata exposure: `native`, `translated`, or `hybrid` |
| `extraArgs.max-concurrent` | `64` | Per-replica in-flight request cap (excess gets `503`); also bounds concurrent backend operations. `0` = unlimited |
| `extraArgs.backend-max-concurrent-heavy-queries` | `2` | Heavy VictoriaLogs queries (ranges at or above the threshold below) running at once, per replica. Excess requests wait, then get Loki's `429`. `0` = unlimited |
| `extraArgs.backend-heavy-query-queue-wait` | `20s` | How long a heavy query waits for a slot before the `429` |
| `extraArgs.backend-heavy-query-min-range` | `6h` | Query range from which a request counts as heavy |
| `extraArgs.rate-limit-per-second` / `extraArgs.rate-limit-burst` | `50` / `100` | Per-client (source IP) token bucket; `rate-limit-per-second=0` disables it |
| `extraArgs.server.register-instrumentation` | `true` | Serve `/metrics` (the binary default is `false`) |
| `extraArgs.metrics-listen` | `:9091` | Dedicated `/metrics` listener, exposed as the `metrics` Service port |
| `extraArgs.admin-listen` | `127.0.0.1:3101` | Loopback-only admin/debug listener; never published through the Service |
| `service.metrics.enabled` | `true` | Add the `metrics` Service port; requires a non-loopback `metrics-listen` and instrumentation enabled |
| `extraArgs.patterns-enabled` | `true` | Enable Drilldown patterns endpoint |
| `extraArgs.exact-parser-series-identity` | _(binary default: `false`)_ | Name `\| json` / `\| logfmt` metric series with the labels those parsers extracted, as Loki does; such queries are then evaluated from rows instead of pushed down to VictoriaLogs stats |
| `extraArgs.log-level` | `info` | Log verbosity: `debug`, `info`, `warn`, `error` |
| `persistence.enabled` | `false` | Enable bbolt disk cache via PVC |
| `persistence.size` | `10Gi` | PVC size for disk cache |
| `peerCache.enabled` | `false` | Enable distributed cache sharing across replicas |
| `serviceMonitor.enabled` | `false` | Create Prometheus Operator ServiceMonitor |
| `networkPolicy.enabled` | `true` | Restrict ingress to Grafana pods on port 3100 and egress to port 9428 (VictoriaLogs) and DNS |
| `networkPolicy.monitoringNamespace` | `""` | Namespace allowed to scrape the metrics port when ServiceMonitor is enabled |
| `networkPolicy.monitoringFrom` | `[]` | Explicit NetworkPolicy peers allowed to scrape the metrics port |
| `networkPolicy.monitoringAllowAll` | `false` | Explicitly allow all sources to scrape metrics when ServiceMonitor is enabled |
| `resources.requests.cpu` | `100m` | CPU request |
| `resources.requests.memory` | `128Mi` | Memory request |
| `resources.limits.cpu` | `1000m` | CPU limit |
| `resources.limits.memory` | `512Mi` | Memory limit |
| `replicaCount` | `1` | Number of proxy replicas |
| `workload.kind` | `Deployment` | Workload type: `Deployment` or `StatefulSet` |
| `horizontalPodAutoscaling.enabled` | `false` | Enable HPA (omits `spec.replicas` from workload) |
| `podDisruptionBudget.enabled` | `false` | Enable PodDisruptionBudget |

## Production Setup (StatefulSet + Peer Cache + Disk Cache)

```yaml
# values-production.yaml example
replicaCount: 3

workload:
  kind: StatefulSet

extraArgs:
  backend: "http://victorialogs:9428"
  label-style: "underscores"
  metadata-field-mode: "translated"
  patterns-enabled: "true"
  query-range-windowing: "true"

persistence:
  enabled: true
  size: 20Gi

peerCache:
  enabled: true
  discovery: dns

podDisruptionBudget:
  enabled: true
  minAvailable: 1

horizontalPodAutoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10

resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 2000m
    memory: 1Gi
```

## Multi-Tenant Setup

```yaml
extraArgs:
  tenant-map: '{"orgA":{"account_id":"1","project_id":"1"}}'
  require-tenant-header: "true"
  forward-tenant-header: "true"
```

`require-tenant-header` only checks that `X-Scope-OrgID` is present (`401` otherwise); it does not authenticate the value. With `extraArgs.tenant-label`, the named field must be a VictoriaLogs stream field. An unmapped `X-Scope-OrgID: *` is rejected unless `extraArgs.tenant.allow-global` is `"true"`.

For secret tenant maps, use `envFrom` to mount from a ConfigMap or Secret instead of embedding credentials in `extraArgs`.

## Observability

```yaml
observability:
  otlp:
    endpoint: http://otel-collector:4318/v1/metrics
  otel:
    serviceName: loki-vl-proxy

serviceMonitor:
  enabled: true

# networkPolicy is enabled by default; with serviceMonitor.enabled=true the chart
# refuses to render until the scrape source is scoped (or explicitly opened).
networkPolicy:
  monitoringNamespace: monitoring   # or monitoringFrom: [...] / monitoringAllowAll: true

prometheusRule:
  enabled: true
```

The ServiceMonitor scrapes the `metrics` port (`extraArgs.metrics-listen`, default `:9091`). To push metrics over OTLP without a scrape endpoint, set `extraArgs.server.register-instrumentation: "false"`, `extraArgs.metrics-listen: ""` and `service.metrics.enabled: false` together.

## Listeners and Secrets

- **Proxy listener** — `extraArgs.listen` (`:3100`), exposed by the Service for Grafana and peer-cache traffic.
- **Metrics listener** — `extraArgs.metrics-listen` (`:9091`) serves only `/metrics` and is exposed as the `metrics` Service port.
- **Admin listener** — `extraArgs.admin-listen` (`127.0.0.1:3101`) serves `/admin/cache/flush` and, when enabled, `/debug/pprof/*` and `/debug/queries`. It is loopback-only and not exposed; reach it with `kubectl port-forward`. Setting `extraArgs.server.admin-auth-token` moves these routes onto the main listener behind that token.
- **Peer auth token** — with `peerCache.enabled=true` the proxy requires a shared token. The chart creates a `<release>-peer-auth` Secret (key `token`, random 32 characters, reused on upgrade through Helm `lookup`) unless `peerCache.authToken` (rendered as `<release>-peer-auth-literal`) or `peerCache.existingSecret` is set. Pin the token with one of those two values for render-only GitOps flows or restricted RBAC, where `lookup` cannot read the existing Secret. Do not set `extraArgs.peer-auth-token` together with `peerCache.enabled=true`; the chart fails rendering.

## Cold Storage (Victoria Lakehouse)

Route historical queries to an S3-backed Victoria Lakehouse instance:

```yaml
extraArgs:
  cold-enabled: "true"
  cold-backend: "http://victoria-lakehouse:9428"
  cold-boundary: "168h"   # queries older than 7d go to cold tier
  cold-overlap: "1h"
```

## Full values reference

See [values.yaml](values.yaml) for all available configuration options.
