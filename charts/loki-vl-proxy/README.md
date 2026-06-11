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
| `extraArgs.label-style` | _(passthrough)_ | Label translation mode: `passthrough` or `underscores` |
| `extraArgs.metadata-field-mode` | _(hybrid)_ | Structured metadata exposure: `native`, `translated`, or `hybrid` |
| `extraArgs.patterns-enabled` | `true` | Enable Drilldown patterns endpoint |
| `extraArgs.log-level` | `info` | Log verbosity: `debug`, `info`, `warn`, `error` |
| `persistence.enabled` | `false` | Enable bbolt disk cache via PVC |
| `persistence.size` | `10Gi` | PVC size for disk cache |
| `peerCache.enabled` | `false` | Enable distributed cache sharing across replicas |
| `serviceMonitor.enabled` | `false` | Create Prometheus Operator ServiceMonitor |
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
  label-style: "passthrough"
  metadata-field-mode: "hybrid"
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

prometheusRule:
  enabled: true
```

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
