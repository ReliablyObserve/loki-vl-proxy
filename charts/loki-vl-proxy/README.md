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
| `extraArgs.backend-heavy-query-min-range` | `6h` | Query range from which a request counts as heavy, and a metadata listing as a long-range scan |
| `extraArgs.backend-max-concurrent-metadata-scans` | `8` | Ceiling of the adaptive limit on long-range VictoriaLogs metadata scans (`/labels`, `/label/{name}/values`, `/series`, `detected_fields` at or above the threshold above, and the label inventory's day buckets) running at once, per replica. Selects VictoriaLogs runs for others count against it, so a fleet stays near one replica's limit; it starts at 2 and adapts to scan latency, failures and VictoriaLogs memory; excess requests wait, then get Loki's `429`; background warm-ups skip instead. `0` disables it |
| `extraArgs.backend-min-concurrent-metadata-scans` | `1` | Floor of that adaptive limit |
| `extraArgs.backend-metadata-scan-memory-headroom` | `0.4` | Share of VictoriaLogs' available memory (from its `/metrics`) that long-range scans must leave free; `0` disables the memory gate |
| `extraArgs.backend-metadata-scan-latency-tolerance` | `1.5` | How many times its no-load duration a concurrent scan may take before the limit shrinks |
| `extraArgs.metadata-inventory-parallelism` | `4` | Label names and values are listed from a time-bucketed inventory (day, hour, 5-minute and minute buckets cached and shared with peers), so a refresh reads only its edges; bucket listings in flight per request. `0` turns it off |
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
| `adminAuth.existingSecret` | `""` | Existing Secret holding the admin/debug bearer token. The chart renders `-server.admin-auth-token=$(ADMIN_AUTH_TOKEN)` from a `secretKeyRef`, so the token is not in the pod spec or `helm get values`. Setting it together with `extraArgs."server.admin-auth-token"` fails rendering |
| `adminAuth.existingSecretKey` | `token` | Key within `adminAuth.existingSecret` |
| `backendAuth.existingSecret` | `""` | Existing Secret holding the VictoriaLogs basic-auth credentials as one `user:password` value. The chart renders `-backend-basic-auth=$(BACKEND_BASIC_AUTH)` from a `secretKeyRef`. Setting it together with `extraArgs.backend-basic-auth` fails rendering |
| `backendAuth.existingSecretKey` | `basic-auth` | Key within `backendAuth.existingSecret` |
| `observability.otlp.existingSecret` | `""` | Existing Secret holding the OTLP exporter headers (`key=value[,key=value]`, usually an `Authorization` value), exposed to the proxy as `OTLP_HEADERS` from a `secretKeyRef`. Setting it together with `observability.otlp.headers` or `extraArgs.otlp-headers` fails rendering |
| `observability.otlp.existingSecretKey` | `headers` | Key within `observability.otlp.existingSecret` |
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
| `systemMetrics.hostProc.enabled` | `false` | Mount five host `/proc` files (`stat`, `meminfo`, `pressure/{cpu,memory,io}`) read-only as `hostPath` volumes and pass `-host-proc-root`, for host-level CPU, memory and PSI metrics. Off by default so the pod passes Pod Security `restricted`; see [Host system metrics](#host-system-metrics-opt-in) |
| `systemMetrics.hostProc.mountPath` | `/host/proc` | Where those files are mounted, and the value passed to `-host-proc-root` |

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

`require-tenant-header` only checks that `X-Scope-OrgID` is present (`401` otherwise); it does not authenticate the value. With `extraArgs.tenant-label`, the named field must be a VictoriaLogs stream field. An unmapped `X-Scope-OrgID: *` is rejected unless `extraArgs.tenant.allow-global` is `"true"`; with `extraArgs.tenant-label`, an unmapped default alias (`0`, `fake`, `default`) or empty org is rejected the same way.

A numeric `X-Scope-OrgID` routes to the VictoriaLogs account of the same number only in its canonical spelling (`7`, not `007`, `00`, `+7` or `-1`; at most `4294967295`), because Loki keeps `007` and `7` as different tenants. Any other numeric spelling is refused with `403` unless it is listed in `extraArgs.tenant-map`, and the proxy logs a warning naming the org and the fix. Set `extraArgs.tenant.numeric-passthrough: "false"` for strict routing, where only orgs listed in `tenant-map` are served (the aliases `0`, `fake`, `default` and a missing org then need `tenant.allow-global: "true"` or a map entry); it has no effect with `tenant-label`.

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

## Host system metrics (opt-in)

By default the chart mounts no `hostPath` volume, so the rendered pod passes the Pod Security `baseline` and `restricted` profiles. The proxy then reads `process_*`, `runtime_*` and network metrics from its own container, and reads the host-scope files (`stat`, `meminfo`, `pressure/{cpu,memory,io}`) from the container's own `/proc`.

To report host-level CPU, memory and PSI (pressure) metrics from the node's own files, opt in:

```yaml
systemMetrics:
  hostProc:
    enabled: true
    mountPath: /host/proc
```

This mounts exactly five files read-only as `hostPath` volumes of type `File` (`/proc/stat`, `/proc/meminfo`, `/proc/pressure/cpu`, `/proc/pressure/memory`, `/proc/pressure/io`) and adds `-host-proc-root=/host/proc`. The host `/proc` directory and other workloads' per-process data are not mounted. Pod Security `baseline` and `restricted` reject `hostPath` volumes, so label the namespace `pod-security.kubernetes.io/enforce: privileged` or exempt the workload in your admission policy. Security scanners (Trivy `KSV-0121`) flag these volumes when it is enabled. See [`docs/security.md`](../../docs/security.md) and [`docs/observability.md`](../../docs/observability.md).

## Listeners and Secrets

- **Proxy listener** — `extraArgs.listen` (`:3100`), exposed by the Service for Grafana and peer-cache traffic.
- **Metrics listener** — `extraArgs.metrics-listen` (`:9091`) serves only `/metrics` and is exposed as the `metrics` Service port.
- **Admin listener** — `extraArgs.admin-listen` (`127.0.0.1:3101`) serves `/admin/cache/flush` and, when enabled, `/debug/pprof/*` and `/debug/queries`. It is loopback-only and not exposed; reach it with `kubectl port-forward`. Setting `extraArgs.server.admin-auth-token` moves these routes onto the main listener behind that token.
- **Peer auth token** — with `peerCache.enabled=true` the proxy requires a shared token. The chart creates a `<release>-peer-auth` Secret (key `token`, random 32 characters, reused on upgrade through Helm `lookup`) unless `peerCache.authToken` (rendered as `<release>-peer-auth-literal`) or `peerCache.existingSecret` is set. Pin the token with one of those two values for render-only GitOps flows or restricted RBAC, where `lookup` cannot read the existing Secret. Do not set `extraArgs.peer-auth-token` together with `peerCache.enabled=true`; the chart fails rendering.

### Secrets from an existing Secret

The plain values `extraArgs."server.admin-auth-token"`, `extraArgs.backend-basic-auth` and `observability.otlp.headers` still work, but they are written into the pod spec and are readable with `helm get values` and `kubectl get pod -o yaml`. To keep them out of both, create the Secret yourself (Vault, External Secrets Operator, sealed-secrets, SOPS) and reference it:

```yaml
adminAuth:
  existingSecret: loki-vl-proxy-admin      # key "token" (override with existingSecretKey)
backendAuth:
  existingSecret: loki-vl-proxy-backend    # key "basic-auth", value "user:password"
observability:
  otlp:
    existingSecret: loki-vl-proxy-otlp     # key "headers", value "Authorization=Bearer ..."
```

The chart adds a `secretKeyRef` environment variable (`ADMIN_AUTH_TOKEN`, `BACKEND_BASIC_AUTH`, `OTLP_HEADERS`) and, for the first two, passes the flag as `$(VAR)`, which the kubelet expands at container start. `OTLP_HEADERS` is read by the proxy itself when `-otlp-headers` is unset. Setting the plain value and `existingSecret` for the same item fails rendering. The peer-cache token has the same option as `peerCache.existingSecret`.

Other sensitive inputs need no extra chart value:

- **TLS** — `tls-cert-file`, `tls-key-file` and `tls-client-ca-file` are file paths, so no key material is in the pod spec. Mount a TLS Secret with `extraVolumes` and `extraVolumeMounts` and point the flags at it.
- **Other flags** (for example a backend URL with credentials): put the value in a Secret, expose it with `extraEnv` (`valueFrom.secretKeyRef`) and reference it as `$(NAME)` in the `extraArgs` value.
- The proxy has no tenant credentials, OAuth or OIDC settings: `tenant-map` holds account and project IDs, not secrets.

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
