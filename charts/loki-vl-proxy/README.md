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
| `extraArgs.field-mapping-label-mode` | _(binary default: unset)_ | Names of `field-mapping` fields in label lists, stream and series labels, structured metadata and detected fields: unset (one name per field), `original`, `alias` or `both` |
| `extraArgs.max-concurrent` | `64` | Per-replica in-flight request cap (excess gets `503`); also bounds concurrent backend operations. `0` = unlimited |
| `extraArgs.backend-max-concurrent-heavy-queries` | `2` | Heavy VictoriaLogs queries (ranges at or above the threshold below) running at once, per replica. Excess requests wait, then get Loki's `429`. `0` = unlimited |
| `extraArgs.backend-heavy-query-queue-wait` | `20s` | How long a heavy query waits for a slot before the `429` |
| `extraArgs.backend-heavy-query-min-range` | `6h` | Query range from which a request counts as heavy, and a metadata listing as a long-range scan |
| `extraArgs.backend-max-concurrent-metadata-scans` | `8` | Ceiling of the adaptive limit on long-range VictoriaLogs metadata scans (`/labels`, `/label/{name}/values`, `/series`, `detected_fields` at or above the threshold above, and the label inventory's day buckets) running at once, per replica. Selects VictoriaLogs runs for others count against it, so a fleet stays near one replica's limit; it starts at 2 and adapts to scan latency, failures and VictoriaLogs memory; excess requests wait, then get Loki's `429`; background warm-ups skip instead. `0` disables it |
| `extraArgs.backend-min-concurrent-metadata-scans` | `1` | Floor of that adaptive limit |
| `extraArgs.backend-metadata-scan-memory-headroom` | `0.4` | Share of VictoriaLogs' available memory (from its `/metrics`) that long-range scans must leave free; `0` disables the memory gate |
| `extraArgs.backend-metadata-scan-latency-tolerance` | `1.5` | How many times its no-load duration a concurrent scan may take before the limit shrinks |
| `extraArgs.metadata-inventory-parallelism` | `4` | Label names and values are listed from a time-bucketed inventory (day, hour, 5-minute and minute buckets cached and shared with peers), so a refresh reads only its edges; bucket listings in flight per request. `0` turns it off |
| `extraArgs.metadata-inventory-prefetch` | `true` | After a `/labels` listing, read in the background the hour buckets that let a moved window be answered from memory (with `-metadata-max-staleness` above 0), at most the window's own width per listing. `false` keeps memory answers to the buckets requests read |
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
| `image.digest` | `""` | Pins the image as `sha256:<64 hex>`, the multi-arch index digest in the release's `image-digests.txt`. Renders `<repository>@<digest>`, or `<repository>:<tag>@<digest>` when `image.tag` is set; the digest decides what runs. See [Verify before deploying](../../SECURITY.md#verify-before-deploying) |
| `peerCache.enabled` | `false` | Enable distributed cache sharing across replicas |
| `adminAuth.existingSecret` | `""` | Existing Secret holding the admin/debug bearer token. The chart mounts the key as a file and renders `-server.admin-auth-token-file`, so the token is not in the pod spec, `helm get values` or the process arguments. Setting it together with `extraArgs."server.admin-auth-token"` fails rendering |
| `adminAuth.existingSecretKey` | `token` | Key within `adminAuth.existingSecret` |
| `deleteAuth.existingSecret` | `""` | Existing Secret holding the delete API's token, required by `extraArgs.delete-api-enabled: "true"` (rendering fails without it or `extraArgs.delete-api-auth-token`). The chart mounts the key as a file and renders `-delete-api-auth-token-file`. Every delete request must carry the token in `X-Delete-Token` or `Authorization: Bearer`. It is separate from the admin token: enabling deletes leaves the admin listener on loopback. Setting it together with `extraArgs.delete-api-auth-token` fails rendering |
| `deleteAuth.existingSecretKey` | `token` | Key within `deleteAuth.existingSecret` |
| `clientAuth.existingSecret` | `""` | Caller authentication, off when empty: an existing Secret holding the users file. The chart mounts the Secret's directory read-only at `clientAuth.mountPath` (no `subPath`, so a changed Secret reaches the pod and the proxy re-reads it) and sets `-server.auth-users-file`. Every route except the probes, `/metrics` and the peer routes then needs Basic or bearer credentials. Setting it together with `extraArgs."server.auth-users-file"` fails rendering |
| `clientAuth.existingSecretKey` | `users.yaml` | Key within `clientAuth.existingSecret` that holds the users file |
| `clientAuth.mountPath` | `/etc/loki-vl-proxy/auth` | Directory the users file is mounted in |
| `backendAuth.existingSecret` | `""` | Existing Secret holding the VictoriaLogs basic-auth credentials as one `user:password` value. The chart mounts the key as a file and renders `-backend-basic-auth-file`. Setting it together with `extraArgs.backend-basic-auth` fails rendering |
| `backendAuth.existingSecretKey` | `basic-auth` | Key within `backendAuth.existingSecret` |
| `backendVersionAuth.existingSecret` | `""` | Existing Secret holding the `user:password` of `extraArgs.backend-version-url`. The chart mounts the key as a file and renders `-backend-version-basic-auth-file`. Setting it together with `extraArgs.backend-version-basic-auth` fails rendering |
| `backendVersionAuth.existingSecretKey` | `basic-auth` | Key within `backendVersionAuth.existingSecret` |
| `cacheKeySecret.existingSecret` | `""` | Existing Secret holding the key for credential digests in cache keys (forwarded `Authorization` headers and cookies, `backendAuth`). The chart mounts the key as a file and renders `-cache-key-secret-file`. Unset, the chart keeps a generated key (`cacheKeySecret.generate`). Setting it together with `extraArgs.cache-key-secret` fails rendering |
| `cacheKeySecret.existingSecretKey` | `secret` | Key within `cacheKeySecret.existingSecret` |
| `cacheKeySecret.generate` | `true` | With `existingSecret` unset, keep a random 64-character key in `<release>-cache-key`: created on the first install, kept on upgrades (`lookup`), so restarts keep credential-scoped cache entries. A render without cluster access (`helm template`, Argo CD) renders and mounts no key rather than a new one per render; use `existingSecret` there. Deleting the Secret rotates the key (credential-scoped entries miss once). `false`: the proxy derives the key from the peer token, else per process |
| `observability.otlp.existingSecret` | `""` | Existing Secret holding the OTLP exporter headers (`key=value[,key=value]`, usually an `Authorization` value), exposed to the proxy as `OTLP_HEADERS` from a `secretKeyRef`. Setting it together with `observability.otlp.headers` or `extraArgs.otlp-headers` fails rendering |
| `observability.otlp.existingSecretKey` | `headers` | Key within `observability.otlp.existingSecret` |
| `serviceMonitor.enabled` | `false` | Create Prometheus Operator ServiceMonitor |
| `serviceMonitor.honorLabels` | `true` | Keep the proxy's `endpoint` label instead of the scrape target's (`false`: it arrives as `exported_endpoint`) |
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
| `podDisruptionBudget.enabled` | unset | `true` renders a PodDisruptionBudget from `minAvailable` / `maxUnavailable`, `false` none; unset renders none, except with `cacheHandover.enabled` and `replicaCount > 1` or HPA (`maxUnavailable: 1`) |
| `workload.progressDeadlineSeconds` | unset | Deployment `progressDeadlineSeconds`. Unset renders none (600 s), except with a disk cache and its startup probe: 935 s, or 1170 s with `cacheHandover.enabled`, at the defaults. See [Startup probe](#startup-probe) |
| `probe.startup.enabled` | unset | Startup probe; unset is off, or on with a disk cache (`persistence.enabled` or `extraArgs.disk-cache-path`) |
| `probe.startup.path` / `periodSeconds` | `/health` / `5` | What the startup probe asks and how often; never `/ready`, which also checks VictoriaLogs |
| `probe.startup.failureThreshold` | unset | Unset is `30`, or `120` with a disk cache (605 s with the 5 s initial delay and period) |
| `terminationGracePeriodSeconds` | `30` | Pod grace period; with `cacheHandover.enabled` raised to at least the preStop sleep + 60 (HTTP drain of the main, then the admin/metrics listeners) + `shutdownFlushTimeoutSeconds` + 5 |
| `lifecycle` | `{}` | Container lifecycle hooks; a non-empty value replaces the preStop sleep `cacheHandover` adds |
| `cacheHandover.enabled` | `false` | Keep the response cache warm across restarts: flush the memory hot set to the disk cache on SIGTERM and load it back before `/ready`; needs `persistence.enabled` or `extraArgs.disk-cache-path`. See [Rolling restarts with a warm cache](#rolling-restarts-with-a-warm-cache) |
| `cacheHandover.shutdownFlushTimeoutSeconds` | `60` | `-cache-shutdown-flush-timeout` |
| `cacheHandover.startupWarmBytes` | `134217728` | `-cache-startup-warm-bytes`, integer bytes (at most half of `-cache-max-bytes` is warmed) |
| `cacheHandover.startupWarmTimeoutSeconds` | `120` | `-cache-startup-warm-timeout`: the pod is ready when the warm-up finishes, or partly warm after this long |
| `cacheHandover.peerRestore` | `true` | A pod with nothing local to warm from (a new volume, an `emptyDir` disk) pulls the entries it owns or replicates from its peers instead, within the same bytes and timeout (needs `peerCache`); `false` renders `-cache-startup-peer-restore=false`. See [Restore from peers](../../docs/disk-cache.md#restore-from-peers) |
| `cacheHandover.preStopSleepSeconds` | `5` | preStop sleep (native `sleep` action, rendered on Kubernetes 1.30+ only) before SIGTERM; `0` adds none |
| `cacheHandover.minReadySeconds` | `15` | Workload `minReadySeconds` with the hand-over on; `0` renders none |
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

cacheHandover:
  enabled: true

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

## Rolling restarts with a warm cache

`cacheHandover.enabled=true` keeps each replica's memory cache warm across a restart that keeps its disk (a rollout, a config change, a node drain with a StatefulSet PVC):

1. The preStop sleep (`preStopSleepSeconds`) lets the endpoint removal reach clients before the proxy receives SIGTERM.
2. On SIGTERM the proxy drains its HTTP listeners (up to 30 s for the main one, then up to 30 s for the admin and metrics ones), then writes its memory hot set to the disk cache within `shutdownFlushTimeoutSeconds`; the chart raises `terminationGracePeriodSeconds` to cover that sequence (130 s with the defaults; the pod exits as soon as the proxy is done). A configured exec or httpGet preStop hook is not counted: raise `terminationGracePeriodSeconds` yourself for it. A flush that does not finish leaves no hand-over, and the replica starts cold as before.
3. The new process loads that hot set back into memory while `/ready` answers 503. The pod becomes ready when the warm-up finishes, or partly warm after `startupWarmTimeoutSeconds` (120; the warm-up stops there and keeps what it loaded). The timeout counts from before the listener opens. The readiness probe holds the rollout meanwhile, so the next replica restarts only after this one is ready.

### Startup probe

Before it listens, the proxy opens the disk cache and rebuilds its index (one read per record, one at a time), checks the VictoriaLogs version (bounded by short timeouts; an unreachable backend does not hold it, unless `-backend-version-strict` makes it fail) and restores the label values and patterns snapshots when those are enabled. Without a startup probe the liveness probe (5 s delay, 3 failures 15 s apart) would restart a pod whose listener is not up within about 50 s, and the rebuild would start over. So a disk cache (`persistence.enabled` or a non-empty `extraArgs.disk-cache-path`), with or without `cacheHandover.enabled`, renders a startup probe on `/health`: every 5 s after a 5 s delay, 120 failures, so 605 s. That is about twice the estimated 288 s to rebuild 94,000 records on a volume with 3 ms reads (an estimate, not a measurement; [restore time targets](../../docs/disk-cache.md#restore-time-targets-by-storage)). Set `probe.startup.failureThreshold` higher for a slower volume. A pod that hangs before it listens is restarted only after that window. A replacement pod that waits for the old pod's lock on a shared cache file spends up to the old pod's grace period of it.

A Deployment with that probe also gets `progressDeadlineSeconds`, so the longer start is not reported as a failed rollout: grace period + startup window + (with `cacheHandover.enabled`) `startupWarmTimeoutSeconds` + `minReadySeconds` + 300 s for scheduling, volume attach, image pull and readiness; 935 s with a disk cache and 1170 s with the hand-over at the defaults, rendered only above the Kubernetes default of 600 s. A stuck rollout is reported that much later. A warm-up timeout set through `extraArgs` is not counted. `workload.progressDeadlineSeconds` replaces the value. A StatefulSet has no progress deadline.

A Deployment with a disk cache on one `ReadWriteOnce` claim and without the hand-over keeps the Kubernetes default strategy (for one to three replicas: one surge pod, none unavailable). The surge pod waits for the old pod's cache file lock, or for the volume on another node, while the old pod stays until the new one is ready: the rollout does not finish, and with the longer window the pod is restarted after 605 s (before: about 50 s) and the deadline passes at 935 s (before: 600 s). Use a StatefulSet, `cacheHandover.enabled`, or `workload.deploymentStrategy` with `maxSurge: 0`.

While the startup probe has not succeeded, Kubernetes runs neither the liveness nor the readiness probe, and their `initialDelaySeconds` count from its success ([Kubernetes probes](https://kubernetes.io/docs/concepts/workloads/pods/probes/)). The warm-up runs behind an open listener, where only readiness holds traffic. Keep the startup probe on `/health` or `/alive`: `/ready` also checks VictoriaLogs, so a backend outage would restart every proxy. `probe.startup.enabled=false` renders none, and any other `probe.startup` value you set is used as set. A values file copied from an earlier chart sets `probe.startup.enabled: false`, `probe.startup.failureThreshold: 30` and `podDisruptionBudget.enabled: false` explicitly: remove those keys (or set them as you want them) when you use a disk cache or turn the hand-over on, or it renders no startup probe (and, for the hand-over, no PodDisruptionBudget).

### One replica at a time

With `cacheHandover.enabled` the chart renders this, so the other replicas keep serving from their caches:

| Rendered | When | Your setting that replaces it |
|---|---|---|
| Deployment `strategy`: `RollingUpdate`, `maxSurge: 0`, `maxUnavailable: 1` | Deployment | any non-empty `workload.deploymentStrategy` (used alone: set both fields of `rollingUpdate`) |
| nothing: the default `updateStrategy` `RollingUpdate` updates one pod at a time, whatever `podManagementPolicy` is ([StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#rolling-updates)) | StatefulSet | `workload.statefulSet.updateStrategy` |
| PodDisruptionBudget with `maxUnavailable: 1` | `replicaCount > 1` or HPA | `podDisruptionBudget.enabled` (`true`: your `minAvailable` / `maxUnavailable`; `false`: none) |
| `minReadySeconds: 15` | always | `cacheHandover.minReadySeconds` |

- `maxSurge: 0` stops the old pod before the new one starts. A surge pod on the same volume would wait, without listening, for the file lock the old pod holds on the disk cache, while the rollout waits for the surge pod. **With `replicaCount: 1` a rollout therefore has a gap**: no proxy serves from the old pod's termination until the new one is ready.
- With `podDisruptionBudget.enabled` unset, a changed `maxUnavailable` or `minAvailable` is used in place of `maxUnavailable: 1`; an explicit `minAvailable: 1` cannot be told from the default, so set `enabled: true` for it. If you already manage a budget for these pods, set `enabled: false`: Kubernetes refuses to evict a pod that two budgets cover.
- `/ready` also checks VictoriaLogs. While the backend is down no pod is ready, so a rollout waits and the budget blocks node drains.
- A Deployment with `persistence.enabled` mounts one claim in every replica, and one process at a time can open the disk cache file. For more than one replica use `workload.kind: StatefulSet`, which gives each pod its own claim.
- `helm upgrade --wait` must allow every replica its termination, start, warm-up and `minReadySeconds` in turn: raise `--timeout` above the 5 m default for several replicas with large caches (one replica may use its whole startup window, 605 s, before `helm` sees it fail).

```yaml
cacheHandover:
  enabled: true
persistence:
  enabled: true
replicaCount: 3
workload:
  kind: StatefulSet    # one pod at a time; per-pod PVCs
```

The hand-over covers a replica that keeps its disk; one that starts with a new disk refills from its peers and VictoriaLogs. What is restored, and the safety rules, are in [Disk cache](../../docs/disk-cache.md#restart-hand-over); the `loki_vl_proxy_cache_restart_*` metrics show each flush and warm-up.

## Multi-Tenant Setup

```yaml
extraArgs:
  tenant-map: '{"orgA":{"account_id":"1","project_id":"1"}}'
  require-tenant-header: "true"
  forward-tenant-header: "true"
```

`require-tenant-header` only checks that `X-Scope-OrgID` is present (`401` otherwise); it does not authenticate the value. With `extraArgs.tenant-label`, the named field must be a VictoriaLogs stream field. An unmapped `X-Scope-OrgID: *` is rejected unless `extraArgs.tenant.allow-global` is `"true"`; with `extraArgs.tenant-label`, an unmapped default alias (`0`, `fake`, `default`) or empty org is rejected the same way.

Tenancy is off by default: with no tenant setting the proxy ignores `X-Scope-OrgID` and every user reads VictoriaLogs account `0:0`, as Loki with `auth_enabled: false`. Any of `tenant-map`, `tenant-map-file`, `tenant-label`, `auth.enabled`, `require-tenant-header` or `tenant.numeric-passthrough: "true"` turns it on; see [Tenancy and authentication](../../docs/tenancy-and-auth.md). With tenancy on, a numeric `X-Scope-OrgID` routes to the VictoriaLogs account of the same number only in its canonical spelling (`7`, not `007`, `00`, `+7` or `-1`; at most `4294967295`), because Loki keeps `007` and `7` as different tenants. Any other numeric spelling is refused with `403` unless it is listed in `extraArgs.tenant-map`, and the proxy logs a warning naming the org and the fix. Set `extraArgs.tenant.numeric-passthrough: "false"` for strict routing, where only orgs listed in `tenant-map` are served (the aliases `0`, `fake`, `default` and a missing org then need `tenant.allow-global: "true"` or a map entry); it has no effect with `tenant-label`.

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

The ServiceMonitor sets `honorLabels: true` (`serviceMonitor.honorLabels`), so the proxy's `endpoint` label (`query_range`, `labels`, `select_logsql_query`, ...) is stored under that name and the dashboard and alerts can select and group by it. The VictoriaMetrics operator copies the setting into the `VMServiceScrape` it converts the ServiceMonitor to. A scrape object you write yourself needs `honorLabels: true` on its endpoint (`honor_labels: true` in a Prometheus or vmagent scrape config). See [Scrape label collisions](../../docs/observability.md#scrape-label-collisions-endpoint) before an upgrade if your own queries use `exported_endpoint`.

`observability.metrics.durationBuckets` (flag `-metrics.duration-buckets`) replaces the bounds of the duration histograms; empty keeps the defaults, which reach 120 s.

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
- **Peer auth token** — with `peerCache.enabled=true` the proxy requires a shared token. The chart creates a `<release>-peer-auth` Secret (key `token`, random 32 characters, reused on upgrade through Helm `lookup`) unless `peerCache.authToken` (rendered as `<release>-peer-auth-literal`) or `peerCache.existingSecret` is set. Pin the token with one of those two values for render-only GitOps flows or restricted RBAC, where `lookup` cannot read the existing Secret. The token is mounted as a file (`-peer-auth-token-file`). Do not set `extraArgs.peer-auth-token` or `extraArgs.peer-auth-token-file` together with `peerCache.enabled=true`; the chart fails rendering.

### Secrets from an existing Secret

The plain values `extraArgs."server.admin-auth-token"`, `extraArgs.delete-api-auth-token`, `extraArgs.backend-basic-auth` and `observability.otlp.headers` still work, but they are written into the pod spec, are readable with `helm get values` and `kubectl get pod -o yaml`, and flag values are in the process command line. To keep them out of all three, create the Secret yourself (Vault, External Secrets Operator, sealed-secrets, SOPS) and reference it:

```yaml
adminAuth:
  existingSecret: loki-vl-proxy-admin      # key "token" (override with existingSecretKey)
backendAuth:
  existingSecret: loki-vl-proxy-backend    # key "basic-auth", value "user:password"
observability:
  otlp:
    existingSecret: loki-vl-proxy-otlp     # key "headers", value "Authorization=Bearer ..."
```

For `adminAuth`, `deleteAuth`, `backendAuth`, `backendVersionAuth`, `cacheKeySecret` and the peer token, the chart mounts the Secret key as a read-only file under `/var/run/secrets/loki-vl-proxy/` and passes the proxy its path with the matching `-...-file` flag, so the value is in no process argument or environment variable. The proxy reads the file once at startup: restart the pods after rotating a Secret. `OTLP_HEADERS` stays a `secretKeyRef` environment variable, read by the proxy when `-otlp-headers` is unset. Setting the plain value (or the `-file` flag) in `extraArgs` and `existingSecret` for the same item fails rendering.

**Image version:** the `-...-file` flags need an image of the same release as the chart or newer. An older proxy exits with `flag provided but not defined` instead of starting without the credential, so a pinned older `image.tag` with any of these secrets does not roll out: keep chart and image together, or stay on the older chart.

Other sensitive inputs need no extra chart value:

- **TLS** — `tls-cert-file`, `tls-key-file` and `tls-client-ca-file` are file paths, so no key material is in the pod spec. Mount a TLS Secret with `extraVolumes` and `extraVolumeMounts` and point the flags at it.
- **Other flags** (for example a backend URL with credentials): put the value in a Secret, expose it with `extraEnv` (`valueFrom.secretKeyRef`) and reference it as `$(NAME)` in the `extraArgs` value. This keeps it out of the pod spec but not out of the process command line.
- The proxy has no tenant credentials, OAuth or OIDC settings: `tenant-map` holds account and project IDs, not secrets.

### Caller authentication

To make the proxy check a credential on every Grafana request (no TLS needed between Grafana and the proxy), put a users file in a Secret and reference it:

```bash
htpasswd -nbBC 12 grafana '<password>' | cut -d: -f2    # bcrypt hash for password_hash
printf %s '<token>' | sha256sum | cut -d' ' -f1         # digest for token_sha256
kubectl create secret generic loki-vl-proxy-users --from-file=users.yaml
```

```yaml
# users.yaml
users:
  - name: grafana
    password_hash: "$2y$12$..."
    tenant: team-a            # optional: the only X-Scope-OrgID this user may send
  - name: ci
    token_sha256: "<64 hex>"
```

```yaml
clientAuth:
  existingSecret: loki-vl-proxy-users
```

Then turn on Basic authentication in the Grafana Loki data source. The liveness, readiness and startup probes (`/alive`, `/ready`, `/health`), `/metrics` and the peer cache routes stay open; every other route answers `401` with `WWW-Authenticate` without a valid credential. With it on, the admin and delete tokens go in `X-Admin-Token` and `X-Delete-Token`. Keep `networkPolicy.enabled` (the default admits only Grafana pods on port 3100). See [Tenancy and authentication](../../docs/tenancy-and-auth.md#caller-authentication).

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
