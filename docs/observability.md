# Observability Guide

Loki-VL-proxy emits the same core operational signals in two forms:

| Signal | Transport | Format | Best use |
|---|---|---|---|
| Metrics | Pull | Prometheus text at `/metrics` | Prometheus, Grafana Agent, Alloy, VictoriaMetrics, kube scraping |
| Metrics | Push | OTLP HTTP JSON to `/v1/metrics` | OpenTelemetry Collector, vendor OTLP gateways |
| Logs | Stream | Structured JSON on stdout/stderr | Fluent Bit, Vector, OpenTelemetry Collector, Docker/Kubernetes log agents |

The intent is parity, not two separate products. Prometheus scrape and OTLP push carry the same proxy-centric metrics with the same names, labels, and units for the important operational paths:

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
| `GET /metrics` | Prometheus text exposition (`-server.register-instrumentation`) |
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
  "service.name": "loki-vl-proxy",
  "service.version": "0.27.25",
  "service.instance.id": "proxy-1",
  "deployment.environment.name": "prod",
  "component": "proxy",
  "http.route": "query_range",
  "http.request.method": "GET",
  "http.response.status_code": 200,
  "event.duration_ms": 42,
  "loki.tenant.id": "team-a",
  "loki.query": "{service_name=\"api\"} |= \"error\"",
  "client.address": "10.0.0.12:51884",
  "enduser.id": "grafana-user@example.com",
  "loki.client.source": "grafana_user",
  "cache.result": "miss",
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
| `service.*` | stable service identity |
| `deployment.environment.name` | environment name |
| `component` | internal subsystem (`proxy`, `disk_cache`, `cache_warmer`, `otlp_metrics`) |
| `http.*` | request semantics |
| `client.address` | remote address |
| `enduser.id` | trusted user/client identity when available |
| `auth.*` | datasource/auth principal context (separate from `enduser.id`) |
| `cache.result` | compatibility cache result (`hit`, `miss`, `bypass`) |
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

These flags shape both OTLP metric exports and structured logs:

| Flag | Meaning |
|---|---|
| `-otel-service-name` | `service.name` |
| `-otel-service-namespace` | `service.namespace` |
| `-otel-service-instance-id` | `service.instance.id` |
| `-deployment-environment` | `deployment.environment.name` |

### Core Proxy Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loki_vl_proxy_requests_total` | counter | `endpoint`, `status` | all proxied requests |
| `loki_vl_proxy_request_duration_seconds` | histogram | `endpoint` | end-to-end request latency |
| `loki_vl_proxy_backend_duration_seconds` | histogram | `endpoint` | upstream VictoriaLogs latency only |
| `loki_vl_proxy_cache_hits_total` | counter | none | global cache hits |
| `loki_vl_proxy_cache_misses_total` | counter | none | global cache misses |
| `loki_vl_proxy_cache_hits_by_endpoint` | counter | `endpoint` | cache hits per endpoint |
| `loki_vl_proxy_cache_misses_by_endpoint` | counter | `endpoint` | cache misses per endpoint |
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

### Tenant and Client Metrics

These are the metrics to use when you want to identify the users or tenants actually causing backend load.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `loki_vl_proxy_tenant_requests_total` | counter | `tenant`, `endpoint`, `status` | request volume by tenant |
| `loki_vl_proxy_tenant_request_duration_seconds` | histogram | `tenant`, `endpoint` | latency by tenant |
| `loki_vl_proxy_client_requests_total` | counter | `client`, `endpoint` | request volume by client identity |
| `loki_vl_proxy_client_response_bytes_total` | counter | `client` | response bytes by client |
| `loki_vl_proxy_client_status_total` | counter | `client`, `endpoint`, `status` | final status breakdown by client |
| `loki_vl_proxy_client_inflight_requests` | gauge | `client` | current parallelism by client |
| `loki_vl_proxy_client_request_duration_seconds` | histogram | `client`, `endpoint` | request latency by client |
| `loki_vl_proxy_client_query_length_chars` | histogram | `client`, `endpoint` | query size outliers by client |
| `loki_vl_proxy_client_errors_total` | counter | `endpoint`, `reason` | categorized 4xx-style client errors |

### Runtime and Process Metrics

The proxy also exports a lightweight built-in set of runtime and process/container health metrics:

| Metric family | Description |
|---|---|
| `go_memstats_*`, `go_goroutines`, `go_gc_cycles_total` | Go runtime health |
| `process_resident_memory_bytes`, `process_open_fds` | process resource usage |
| `process_cpu_usage_ratio` | CPU pressure split by `mode` |
| `process_memory_*` | total, free, available, usage ratio |
| `process_disk_*_bytes_total` | disk I/O counters |
| `process_network_*_bytes_total` | network I/O counters |
| `process_pressure_*` | Linux PSI gauges when available |

Kubernetes notes:
- These runtime/system metrics are read from `/proc` and do not require Kubernetes RBAC permissions.
- PSI metrics (`process_pressure_*`) depend on kernel support and may be absent on nodes without `/proc/pressure/*`.
- On startup, the proxy logs a system-metrics readiness check with missing families and remediation hints instead of failing silently.
- If you mount host `/proc` (`-proc-root=/host/proc`), these metrics will reflect host scope; keep default pod `/proc` for pod/container scope.
- For per-pod attribution in OTLP backends, set `OTEL_SERVICE_INSTANCE_ID` from pod name and `OTEL_SERVICE_NAMESPACE` from pod namespace (the upstream chart now injects these by default).

### PromQL Drilldowns For Slowness And Client Errors

Use these queries to quickly isolate backend slowness vs proxy/client-side failures:

| Goal | Query |
|---|---|
| Backend p95 latency by endpoint | `histogram_quantile(0.95, sum(rate(loki_vl_proxy_backend_duration_seconds_bucket[5m])) by (le, endpoint))` |
| Proxy p99 end-to-end latency by endpoint | `histogram_quantile(0.99, sum(rate(loki_vl_proxy_request_duration_seconds_bucket[5m])) by (le, endpoint))` |
| Tenant p99 latency | `histogram_quantile(0.99, sum(rate(loki_vl_proxy_tenant_request_duration_seconds_bucket[5m])) by (le, tenant, endpoint))` |
| 5xx rate by endpoint | `sum(rate(loki_vl_proxy_requests_total{status=~"5.."}[5m])) by (endpoint)` |
| Client bad_request by client+endpoint | `sum(rate(loki_vl_proxy_client_errors_total{reason="bad_request"}[5m])) by (client, endpoint)` |
| Client rate_limited by client+endpoint | `sum(rate(loki_vl_proxy_client_errors_total{reason="rate_limited"}[5m])) by (client, endpoint)` |

For latency histograms, keep dashboards on `p50`, `p95`, and `p99` rather than averages. Averages hide tail latency incidents.

The packaged `Loki-VL-Proxy` dashboard also includes a `System Resources` section with:

- memory usage ratio
- CPU usage split by mode
- PSI pressure (cpu/memory/io)
- disk and network throughput
- process RSS and open file descriptors

It also includes a `Query-Range Windowing` section for cache/tuning signals:

- window fetch p50/p95 latency
- window merge p50/p95 latency
- window cache hit ratio
- adaptive window parallelism + EWMA latency/error

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
| [`dashboard/loki-vl-proxy-offenders.json`](../dashboard/loki-vl-proxy-offenders.json) | Native VictoriaLogs datasource | Offender triage with built-in `tenant`, `client`, `cluster`, and `env` filters for route/status load and error analysis |

Use the offenders dashboard during Loki/proxy incidents to keep visibility on raw operator/client behavior directly from stored logs.

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
