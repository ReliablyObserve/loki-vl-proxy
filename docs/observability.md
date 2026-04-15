# Observability Guide

Loki-VL-proxy emits the same core operational signals in two forms:

| Signal | Transport | Format | Best use |
|---|---|---|---|
| Metrics | Pull | Prometheus text at `/metrics` | Prometheus, Grafana Agent, Alloy, VictoriaMetrics, kube scraping |
| Metrics | Push | OTLP HTTP JSON to `/v1/metrics` | OpenTelemetry Collector, vendor OTLP gateways |
| Logs | Stream | Structured JSON on stdout/stderr | Fluent Bit, Vector, OpenTelemetry Collector, Docker/Kubernetes log agents |

The intent is parity, not two separate products. Prometheus scrape and OTLP push carry the same proxy-centric metric families, units, and low-cardinality request dimensions for the important operational paths. Prometheus uses label keys such as `system`, `direction`, `endpoint`, `route`, and `status`; OTLP exports the same dimensions as semantically aligned attributes such as `loki.api.system`, `proxy.direction`, `loki.request.type`, `http.route`, and `http.response.status_code`.

That shared model is what makes the packaged dashboard portable across scrape-backed and OTLP-backed setups without rewriting the operator view:

- request volume and latency
- backend latency
- cache hit/miss behavior
- translation failures
- tenant and client hot spots
- client status and query-length outliers
- process/runtime and host-level health

The proxy also now keeps more expensive metadata paths deliberately warmer than live log queries:

- live query and tail paths stay on short TTLs so fresh log visibility is not stretched unnecessarily
- slower-changing metadata such as labels, field lists, field values, and patterns are cached more aggressively
- Drilldown prefers backend-native metadata discovery where it is safe, which reduces proxy-side rescans and lowers CPU pressure on repeated field/label browsing

## Observability Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /ready` | Readiness probe (checks backend `/health` and circuit-breaker state) |
| `GET /metrics` | Prometheus text exposition (`-server.register-instrumentation`, bounded by `-server.metrics-max-concurrency`) |
| `GET /debug/queries` | Query analytics endpoint (disabled by default, `-server.enable-query-analytics`) |
| `GET /debug/pprof/` | Go pprof profiling endpoints (disabled by default, `-server.enable-pprof`) |

## Logs

### JSON Log Shape

Default logs are emitted as JSON and already use OTel-friendly top-level keys:

```json
{
  "timestamp": "2026-04-05T18:03:27.214918Z",
  "severity": {
    "text": "INFO",
    "number": 9
  },
  "body": "request",
  "component": "proxy",
  "http.route": "/loki/api/v1/query_range",
  "url.path": "/loki/api/v1/query_range",
  "http.request.method": "GET",
  "http.response.status_code": 200,
  "loki.request.type": "query_range",
  "loki.api.system": "loki",
  "proxy.direction": "downstream",
  "event.duration": 42000000,
  "loki.tenant.id": "team-a",
  "loki.query": "{service_name=\"api\"} |= \"error\"",
  "client.address": "10.0.0.12:51884",
  "enduser.id": "grafana-user@example.com",
  "enduser.source": "grafana_user",
  "cache.result": "miss",
  "proxy.duration_ms": 42,
  "upstream.calls": 1,
  "upstream.status_code": 200,
  "upstream.duration_ms": 31,
  "proxy.overhead_ms": 11
}
```

That makes the log stream usable in two ways:

- plain JSON ingestion with no transformation
- low-friction mapping into the OpenTelemetry log data model

### Log Sources

The proxy writes structured logs for:

- request lifecycle and status
- query translation and backend request flow
- tail/WebSocket behavior
- delete audit events
- cache warmer and disk cache internals
- OTLP export failures

### OpenTelemetry Fields Used in Logs

| Field | Meaning |
|---|---|
| `timestamp` | event time |
| `severity.text` / `severity.number` | log severity |
| `body` | message body |
| `component` | internal subsystem (`proxy`, `disk_cache`, `cache_warmer`, `otlp_metrics`) |
| `http.*` / `url.path` | request semantics and normalized route vs actual request path |
| `event.duration` | request or upstream call duration in nanoseconds |
| `client.address` | remote address |
| `enduser.id` | stable trusted user/client identity when available |
| `enduser.name` | display/login user name from trusted user headers when available |
| `enduser.source` | trusted header source for end-user attribution (`grafana_user`, `forwarded_user`, etc.) |
| `auth.*` | datasource/auth principal context (separate from `enduser.id`) |
| `cache.result` | compatibility cache result (`hit`, `miss`, `bypass`) |
| `proxy.*` | proxy-facing convenience fields such as total request duration and measured proxy overhead |
| `upstream.*` | backend call count, status, and latency |
| `loki.*` | Loki/proxy-specific attributes |

## Metrics

### Export Modes

#### Prometheus Scrape

```yaml
scrape_configs:
  - job_name: loki-vl-proxy
    scrape_interval: 15s
    static_configs:
      - targets:
          - loki-vl-proxy:3100
```

#### OTLP Push

```bash
./loki-vl-proxy \
  -backend=http://victorialogs:9428 \
  -otlp-endpoint=http://otel-collector:4318/v1/metrics \
  -otlp-interval=30s \
  -otlp-compression=gzip \
  -otlp-headers='Authorization=Bearer example-token'
```

If the OTLP endpoint is passed as a collector base URL like `http://collector:4318` or `http://collector:4318/v1`, the proxy normalizes it to `/v1/metrics`.

### OpenTelemetry Resource Attributes for Metrics and Logs

These flags shape OTLP metric resource attributes. Structured logs intentionally do
not duplicate resource attributes per line; keep service identity in collector/OTLP
resource metadata to avoid `message.service.*` duplication in storage.

| Flag | Meaning |
|---|---|
| `-otel-service-name` | `service.name` |
| `-otel-service-namespace` | `service.namespace` |
| `-otel-service-instance-id` | `service.instance.id` |
| `-deployment-environment` | `deployment.environment.name` |

### Request Dimensions

Request-oriented metrics use stable low-cardinality dimensions so dashboards can slice by user-visible API shape without leaking raw paths or query content.

| Dimension | Prometheus scrape | OTLP push | Example |
|---|---|---|---|
| API system | `system` | `loki.api.system` | `loki`, `vl` |
| Direction | `direction` | `proxy.direction` | `downstream`, `upstream` |
| Request type | `endpoint` | `loki.request.type` | `query_range`, `labels`, `patterns` |
| Route template | `route` | `http.route` | `/loki/api/v1/query_range`, `/select/logsql/query` |
| Final status | `status` | `http.response.status_code` | `200`, `429`, `500` |

Downstream routes are the normalized Loki API templates registered by the proxy. Upstream routes are the stable VictoriaLogs or rules/alerts backend path templates used by the proxy itself. Raw request paths and query strings stay in logs, not in metric labels.

### Core Proxy Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loki_vl_proxy_requests_total` | counter | `system`, `direction`, `endpoint`, `route`, `status` | all proxied requests, sliced by downstream Loki path or upstream backend path |
| `loki_vl_proxy_request_duration_seconds` | histogram | `system`, `direction`, `endpoint`, `route` | end-to-end request latency |
| `loki_vl_proxy_backend_duration_seconds` | histogram | `system`, `direction`, `endpoint`, `route` | upstream backend latency only (`system="vl"`, `direction="upstream"`) |
| `loki_vl_proxy_cache_hits_total` | counter | none | global cache hits |
| `loki_vl_proxy_cache_misses_total` | counter | none | global cache misses |
| `loki_vl_proxy_cache_hits_by_endpoint` | counter | `system`, `direction`, `endpoint`, `route` | cache hits per normalized route |
| `loki_vl_proxy_cache_misses_by_endpoint` | counter | `system`, `direction`, `endpoint`, `route` | cache misses per normalized route |
| `loki_vl_proxy_translations_total` | counter | none | successful LogQL to LogsQL translations |
| `loki_vl_proxy_translation_errors_total` | counter | none | failed translations |
| `loki_vl_proxy_coalesced_total` | counter | none | requests served from coalesced results |
| `loki_vl_proxy_coalesced_saved_total` | counter | none | backend requests saved by coalescing |
| `loki_vl_proxy_uptime_seconds` | gauge | none | process uptime |
| `loki_vl_proxy_active_requests` | gauge | none | current in-flight requests |
| `loki_vl_proxy_circuit_breaker_state` | gauge | none | `0=closed`, `1=open`, `2=half-open` |

Operational notes for these hot paths:

- `query_range` and `labels` benchmarks in CI track both cache-hit and cache-bypass behavior
- multi-tenant read fanout and merged response bodies are capped to keep a single request from exhausting proxy memory
- synthetic tail keeps bounded dedup state so long-running websocket sessions do not grow without limit

### Query-Range Windowing Metrics

These are the primary signals for long-range query performance and backend protection:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loki_vl_proxy_window_cache_hit_total` | counter | none | cached split windows served without backend scan |
| `loki_vl_proxy_window_cache_miss_total` | counter | none | split windows requiring backend scan |
| `loki_vl_proxy_window_fetch_seconds` | histogram | none | backend fetch duration per split window |
| `loki_vl_proxy_window_merge_seconds` | histogram | none | merge duration for split-window responses |
| `loki_vl_proxy_window_count` | histogram | none | split windows per `query_range` request |
| `loki_vl_proxy_window_prefilter_attempt_total` | counter | none | prefilter runs against `/select/logsql/hits` |
| `loki_vl_proxy_window_prefilter_error_total` | counter | none | prefilter failures (proxy safely falls back to full window fanout) |
| `loki_vl_proxy_window_prefilter_kept_total` | counter | none | split windows retained for real log fanout |
| `loki_vl_proxy_window_prefilter_skipped_total` | counter | none | split windows skipped as empty by prefilter |
| `loki_vl_proxy_window_prefilter_hit_ratio` | gauge | none | current prefilter kept/total ratio (0-1) |
| `loki_vl_proxy_window_retry_total` | counter | none | per-window retry attempts after retryable backend failures |
| `loki_vl_proxy_window_degraded_batch_total` | counter | none | batches that were downgraded to lower parallelism |
| `loki_vl_proxy_window_partial_response_total` | counter | none | partial query-range responses returned when slow windows exceed budget |
| `loki_vl_proxy_window_prefilter_duration_seconds` | histogram | none | prefilter latency |
| `loki_vl_proxy_window_adaptive_parallel_current` | gauge | none | current adaptive split-window parallelism |
| `loki_vl_proxy_window_adaptive_latency_ewma_seconds` | gauge | none | adaptive EWMA latency |
| `loki_vl_proxy_window_adaptive_error_ewma` | gauge | none | adaptive EWMA backend error ratio |

### Tenant and Client Metrics

These are the metrics to use when you want to identify the users or tenants actually causing backend load.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loki_vl_proxy_tenant_requests_total` | counter | `system`, `direction`, `tenant`, `endpoint`, `route`, `status` | request volume by tenant |
| `loki_vl_proxy_tenant_request_duration_seconds` | histogram | `system`, `direction`, `tenant`, `endpoint`, `route` | latency by tenant |
| `loki_vl_proxy_client_requests_total` | counter | `system`, `direction`, `client`, `endpoint`, `route` | request volume by client identity |
| `loki_vl_proxy_client_response_bytes_total` | counter | `client` | response bytes by client |
| `loki_vl_proxy_client_status_total` | counter | `system`, `direction`, `client`, `endpoint`, `route`, `status` | final status breakdown by client |
| `loki_vl_proxy_client_inflight_requests` | gauge | `client` | current parallelism by client |
| `loki_vl_proxy_client_request_duration_seconds` | histogram | `system`, `direction`, `client`, `endpoint`, `route` | request latency by client |
| `loki_vl_proxy_client_query_length_chars` | histogram | `system`, `direction`, `client`, `endpoint`, `route` | query size outliers by client |
| `loki_vl_proxy_client_errors_total` | counter | `system`, `direction`, `endpoint`, `route`, `reason` | categorized downstream client errors |

This is one of the main advantages of putting an explicit proxy between the
Grafana Loki datasource and VictoriaLogs: the read path becomes attributable.

Instead of only seeing aggregate datasource traffic, operators can see:

- which Grafana user or trusted client identity is generating load
- which tenant is hot
- which route is expensive for that client or tenant
- which client is producing the largest responses, longest queries, or most bad requests

### Grafana Client Visibility, Offenders, and User Patterns

When `-metrics.trust-proxy-headers=true` is enabled behind a trusted Grafana or
auth proxy, the proxy can turn northbound identity into durable read-path
signals without using raw datasource credentials as the end-user key.

That gives you:

- per-client request rate by route via `loki_vl_proxy_client_requests_total`
- per-client latency by route via `loki_vl_proxy_client_request_duration_seconds`
- per-client response-volume visibility via `loki_vl_proxy_client_response_bytes_total`
- per-client query-size outlier visibility via `loki_vl_proxy_client_query_length_chars`
- per-client bad-request and error clustering via `loki_vl_proxy_client_status_total` and `loki_vl_proxy_client_errors_total`
- per-tenant volume and latency visibility via `loki_vl_proxy_tenant_*`

Those tenant and client identity series are opt-in. Set `-metrics.export-sensitive-labels=true` only on trusted scrape or OTLP paths where exposing identity labels is acceptable.

At log level, the same request can also carry:

- `enduser.id`
- `enduser.name`
- `enduser.source`
- `auth.principal`
- `auth.source`
- `loki.tenant.id`
- `http.route`

That separation matters:

- `enduser.*` answers "which Grafana user or trusted client triggered this?"
- `auth.*` answers "which datasource or auth principal was used on the request path?"
- `loki.tenant.id` answers "which tenant boundary did the request execute in?"

This is what makes offender analysis practical on the read path instead of only
looking at coarse IP-level traffic.

### Northbound and Southbound Auth Boundaries

The same proxy layer also improves trust separation between components.

| Boundary | Main controls | Why it matters operationally |
|---|---|---|
| Grafana or client -> proxy | `-auth.enabled`, `-tls-client-ca-file`, `-tls-require-client-cert`, trusted user headers with `-metrics.trust-proxy-headers` | Lets the proxy require tenant context, optionally require client certs, and attribute read traffic to the actual Grafana user or trusted upstream identity when sensitive metrics export is explicitly enabled. |
| Proxy -> VictoriaLogs | `-backend-basic-auth`, `-forward-authorization`, `-forward-headers` | Lets the lower layer keep its own auth boundary while the proxy preserves full Loki-client compatibility on the northbound side. |
| Proxy -> peer cache | `-peer-auth-token` | Prevents peer-cache reuse from becoming an unauthenticated east-west path when the fleet spans a broader network boundary. |
| Operator -> admin/debug endpoints | `-server.admin-auth-token` | Protects admin and troubleshooting surfaces without weakening the main read path. Non-loopback listeners now require this token before `/debug/queries` or `/debug/pprof` can be enabled. |

When trusted proxy headers are enabled, the proxy also forwards derived context
headers to VictoriaLogs:

- `X-Loki-VL-Client-ID`
- `X-Loki-VL-Client-Source`

That gives the lower layer better context about who is really behind the read
traffic while still preserving datasource compatibility at the Grafana edge.

### Runtime and Process Metrics

The proxy also exports a lightweight built-in set of runtime and process/container health metrics.
App-scoped aliases are emitted with the `loki_vl_proxy_` prefix, while legacy `go_*` and `process_*` families remain for compatibility:

| Metric family | Description |
|---|---|
| `loki_vl_proxy_go_*` (`go_*` compatibility aliases) | Go runtime health |
| `loki_vl_proxy_process_resident_memory_bytes`, `loki_vl_proxy_process_open_fds` (`process_*` compatibility aliases) | process resource usage |
| `loki_vl_proxy_process_cpu_usage_ratio` (`process_cpu_usage_ratio` alias) | CPU pressure split by `mode` |
| `loki_vl_proxy_process_memory_*` (`process_memory_*` aliases) | total, free, available, usage ratio |
| `loki_vl_proxy_process_disk_*_bytes_total` (`process_disk_*_bytes_total` aliases) | disk I/O counters |
| `loki_vl_proxy_process_disk_*_operations_total` | disk read/write operation counters |
| `loki_vl_proxy_process_network_*_bytes_total` (`process_network_*_bytes_total` aliases) | network I/O counters |
| `loki_vl_proxy_process_pressure_*` (`process_pressure_*` aliases) | Linux PSI gauges when available |

Kubernetes notes:
- These runtime/system metrics are read from `/proc` and do not require Kubernetes RBAC permissions.
- PSI metrics (`process_pressure_*`) depend on kernel support and may be absent on nodes without `/proc/pressure/*`.
- On startup, the proxy logs a system-metrics readiness check with missing families and remediation hints instead of failing silently.
- If you mount host `/proc` (`-proc-root=/host/proc`), these metrics will reflect host scope; keep default pod `/proc` for pod/container scope.
- For per-pod attribution in OTLP backends, set `OTEL_SERVICE_INSTANCE_ID` from pod name and `OTEL_SERVICE_NAMESPACE` from pod namespace (the upstream chart now injects these by default).
- CI includes a metric-name guard so new app metrics must stay under the `loki_vl_proxy_*` prefix unless explicitly allowlisted for compatibility.

### PromQL Drilldowns For Slowness And Client Errors

Use these queries to quickly isolate downstream client pain, upstream slowness, and route-specific cache efficiency:

| Goal | Query |
|---|---|
| Downstream p95 latency by route | `histogram_quantile(0.95, sum(rate(loki_vl_proxy_request_duration_seconds_bucket{system="loki",direction="downstream"}[5m])) by (le, endpoint, route))` |
| Upstream p95 latency by route | `histogram_quantile(0.95, sum(rate(loki_vl_proxy_backend_duration_seconds_bucket{system="vl",direction="upstream"}[5m])) by (le, endpoint, route))` |
| Downstream 5xx rate by route | `sum(rate(loki_vl_proxy_requests_total{system="loki",direction="downstream",status=~"5.."}[5m])) by (endpoint, route)` |
| Tenant p99 latency by route | `histogram_quantile(0.99, sum(rate(loki_vl_proxy_tenant_request_duration_seconds_bucket{system="loki",direction="downstream"}[5m])) by (le, tenant, endpoint, route))` |
| Route cache hit ratio | `sum(rate(loki_vl_proxy_cache_hits_by_endpoint{system="loki",direction="downstream"}[5m])) by (endpoint, route) / clamp_min(sum(rate(loki_vl_proxy_cache_hits_by_endpoint{system="loki",direction="downstream"}[5m])) by (endpoint, route) + sum(rate(loki_vl_proxy_cache_misses_by_endpoint{system="loki",direction="downstream"}[5m])) by (endpoint, route), 1)` |
| Client bad_request by route | `sum(rate(loki_vl_proxy_client_errors_total{system="loki",direction="downstream",reason="bad_request"}[5m])) by (endpoint, route)` |

For latency histograms, keep dashboards on `p50`, `p95`, and `p99` rather than averages. Averages hide tail latency incidents. For exact proxy-only overhead, use structured logs (`proxy.overhead_ms`) alongside the latency histograms; subtracting histogram quantiles is not mathematically reliable.

The packaged `Loki-VL-Proxy` dashboard includes an `Operational Resources` section with:

- memory saturation and memory footprint/headroom
- CPU usage split by mode
- disk IOPS up/down and disk throughput up/down
- network up/down
- PSI pressure (cpu/memory/io)
- process RSS and open file descriptors by pod

The top of the dashboard is organized as a left-to-right operator flow:

- `Main Overview - Client -> Proxy -> VictoriaLogs`
- `Client Edge - Request Quality & Shape`
- `Heavy Consumers - Client Load Drivers`
- `Proxy -> VictoriaLogs Query Pipeline`

It also includes a `Query-Range Windowing` section for cache/tuning signals:

- window fetch p50/p95 latency
- window merge p50/p95 latency
- window cache hit ratio
- adaptive window parallelism + EWMA latency/error

It also includes a `Long-Range Resilience KPIs` section for phase tuning:

- prefilter kept/skipped rate
- retry/degraded-batch/partial-response rate
- prefilter hit ratio

Dashboard datasource notes:

- datasource variable regex is intentionally permissive (`/.*/`) so the dashboard works with scrape-backed and OTLP-backed metric datasources without renaming
- key stat panels use explicit zero fallbacks so dashboards remain readable during cold starts and low-traffic windows

### Active Backend E2E Healthchecks

`/ready` confirms backend reachability, but production health should also include synthetic end-to-end probes with real query traffic shape.

Recommended pattern:

1. Probe `/ready` every 15-30s for hard availability.
2. Run a lightweight synthetic `query_range` every 1-5m from inside the cluster.
3. Alert when synthetic query latency or error ratio breaches SLO even if `/ready` is green.

This catches backend partial degradation (slow scans, storage pressure, auth drift) earlier than readiness alone.

## Choosing Client Identity

Per-client metrics and request logs can use trusted upstream identity instead of only remote IP:

```bash
-metrics.trust-proxy-headers=true
```

When enabled, the proxy prefers:

1. Trusted user headers (`X-Grafana-User`, `X-Forwarded-User`, `X-Webauth-User`, `X-Auth-Request-User`)
2. tenant
3. trusted forwarded client IP (`X-Forwarded-For`)
4. remote IP

Datasource/basic-auth credentials are reported separately under `auth.*` and are not used as end-user identity.
Only enable trusted proxy headers when the proxy sits behind a trusted auth proxy or Grafana instance.

## Integration Examples

### OpenTelemetry Collector: scrape `/metrics` and export OTLP

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: loki-vl-proxy
          scrape_interval: 15s
          static_configs:
            - targets: ["loki-vl-proxy:3100"]

processors:
  batch: {}

exporters:
  otlphttp:
    endpoint: https://otel-gateway.example.com
    headers:
      Authorization: Bearer ${OTLP_TOKEN}

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [batch]
      exporters: [otlphttp]
```

### OpenTelemetry Collector: collect JSON logs from container stdout

```yaml
receivers:
  filelog:
    include:
      - /var/log/containers/*loki-vl-proxy*.log
    operators:
      - type: json_parser

processors:
  batch: {}

exporters:
  otlphttp:
    endpoint: https://otel-gateway.example.com

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [otlphttp]
```

### Vector: ship structured JSON logs

```toml
[sources.proxy_logs]
type = "kubernetes_logs"

[transforms.proxy_json]
type = "remap"
inputs = ["proxy_logs"]
source = '''
. = parse_json!(string!(.message))
'''

[sinks.proxy_otlp]
type = "opentelemetry"
inputs = ["proxy_json"]
protocol.type = "http"
protocol.uri = "https://otel-gateway.example.com/v1/logs"
```

### Fluent Bit: tail container logs and keep JSON structure

```ini
[INPUT]
    Name              tail
    Path              /var/log/containers/*loki-vl-proxy*.log
    Parser            docker
    Tag               loki_vl_proxy

[FILTER]
    Name              parser
    Match             loki_vl_proxy
    Key_Name          log
    Parser            json

[OUTPUT]
    Name              opentelemetry
    Match             loki_vl_proxy
    Host              otel-collector
    Port              4318
    Logs_uri          /v1/logs
```

## Recommended Dashboards and Alerts

Start with:

- request rate and error rate by `endpoint`
- backend latency p95/p99 by `endpoint`
- cache hit ratio overall and by `endpoint`
- top `client` by request rate, bytes, and query length
- top `tenant` by request volume and latency
- circuit breaker state
- process RSS and open file descriptors

### Dashboard Catalog

| Dashboard | Source | Primary use |
|---|---|---|
| [`dashboard/loki-vl-proxy.json`](../dashboard/loki-vl-proxy.json) | Prometheus metrics | Service health, SLOs, cache and endpoint latency trends |

#### Metrics Dashboard Setup (Scrape and OTLP Push)

The metrics dashboard includes a `Datasource` variable and works with either metric transport mode:

- Prometheus scrape (`/metrics` + `ServiceMonitor`)
- OTLP push (`-otlp-endpoint=...`) into a Prometheus-compatible backend

Recommended setup:

1. Point `Datasource` to any Prometheus-compatible datasource that contains `loki_vl_proxy_*` metrics.
2. For scrape mode, use the datasource fed by your `ServiceMonitor`/Prometheus scrape pipeline.
3. For OTLP push mode, use the datasource fed by your OTLP metrics pipeline.
4. VictoriaMetrics can be used for both modes when it receives both scrape and OTLP streams.

Transport checklist:

- Scrape mode:
  - `-server.register-instrumentation=true`
  - Helm `serviceMonitor.enabled=true`
- OTLP push mode:
  - `-otlp-endpoint` configured
  - `-server.register-instrumentation=false` (optional, recommended when you want push-only)

Quick validation in Grafana Explore against the selected datasource:

```promql
loki_vl_proxy_uptime_seconds
```

If this query has data, the `Loki-VL-Proxy Metrics` dashboard should populate out of the box.

High-signal alert ideas:

- `5xx` rate rising on query endpoints
- cache hit ratio collapsing
- backend latency p95 breaching SLO
- a single `client` dominating bytes or query length
- circuit breaker opening repeatedly

The packaged alert set and incident procedures live in:

- [`alerting/loki-vl-proxy-prometheusrule.yaml`](../alerting/loki-vl-proxy-prometheusrule.yaml)
- [`docs/runbooks/alerts.md`](runbooks/alerts.md)

## Notes

- OTLP push and Prometheus scrape share the same important proxy metrics and metric names.
- The OTLP export is intentionally lightweight and does not pull in the full OpenTelemetry Go SDK.
- Structured logs are already safe for JSON ingestion; agents can forward them directly or transform them into OTLP logs.
