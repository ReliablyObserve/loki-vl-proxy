---
sidebar_label: Operations Guide
description: "Day-2 operations: health checks, graceful shutdown, multi-tenancy, rate limiting, and production deployment patterns."
---

# Operations Guide

## Deployment

### Minimum Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 50m | 200m |
| Memory | 64Mi | 256Mi |
| Replicas | 1 | 2+ (with PDB) |

The proxy is stateless (except optional disk cache). Scale horizontally without coordination.

Key scaling controls (all tunable via CLI flags):

- `-max-concurrent 100` — per-replica in-flight request cap (excess gets Loki's `429 too many outstanding requests`; the flag is named in the log line); also bounds concurrent backend operations
- `-rate-limit-per-second 50` / `-rate-limit-burst 100` — per-client token bucket (excess gets the same `429 too many outstanding requests`)
- `-cb-fail-threshold 5` / `-cb-open-duration 10s` — backend circuit breaker
- use Grafana refresh policy, ingress shaping, HPA, and cache tuning as complementary levers

### Helm Deployment

```bash
helm install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \
  --version <release> \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set extraArgs.label-style=underscores

# Local chart (development)
helm install loki-vl-proxy ./charts/loki-vl-proxy \
  --set extraArgs.backend=http://victorialogs:9428 \
  --set extraArgs.label-style=underscores
```

For multi-replica fleets with HPA, prefer `peerCache.enabled=true` over static peer lists. The chart creates a headless service and the proxy refreshes DNS-discovered peers automatically, so scaling events do not require manual replica or peer updates.

For Grafana Logs Drilldown pattern discovery, keep the default `extraArgs.patterns-enabled=true` or set it explicitly during rollout if you need to control the surface area:

```yaml
extraArgs:
  backend: http://victorialogs:9428
  label-style: underscores
  patterns-enabled: "true"
```

### Required Configuration

| Flag | Required | Description |
|------|----------|-------------|
| `-backend` | Yes | VictoriaLogs URL |
| `-listen` | No | Listen address (default `:3100`) |
| `-label-style` | No | `underscores` (default) or `passthrough` |

---

### Backend Auth Forwarding

If VictoriaLogs authentication is delegated from upstream clients, you can forward client `Authorization` to backend explicitly:

```bash
-forward-authorization=true
```

Equivalent manual mode:

```bash
-forward-headers=Authorization
```

Use this only in trusted topologies (for example Grafana/auth-proxy -> Loki-VL-proxy -> VictoriaLogs).

---

## Operational Assets

Treat these as one versioned operational package:

| Asset | Canonical source | Purpose |
|------|------------------|---------|
| Grafana operations dashboard | [`dashboard/loki-vl-proxy.json`](../dashboard/loki-vl-proxy.json) | Three-section layout: **Section 1 — SLO/SLI + Health** (8-stat top strip: circuit breaker, active requests, QPS, error %, P99 client latency, P95 backend latency, cache hit ratio, uptime; plus SLI time-series rows). **Section 2 — Client → Proxy → VL + Resources** (client visibility: request rate by route, errors by reason, query length, per-client inflight, latency by route; proxy internals: coalescing, internal ops, response tuple mode, tenant QPS; VL backend: upstream fanout, window count, backend latency, fetch/merge latency, adaptive parallelism; process resources: CPU, memory, goroutines, GC, network, disk I/O, PSI pressure). **Section 3 — Deep Proxy Internals** (cache tiers: T0/L1/L2/L3 hit/miss, sizes, stale hits, backend fallthrough; peer cache fleet: cluster members, hit/miss, write-through, hot read-ahead, error breakdown; query-range windowing: window cache, prefilter efficiency, retries, partial responses, prefilter duration, adaptive parallelism trace; patterns engine: in-memory count/bytes, mining rate, source line pipeline, snapshot hits/reuse, persistence; HTTP connection lifecycle: states, rotation reasons, transitions; tenant deep dive: per-tenant QPS/P99/errors) |
| Alert rules | [`alerting/loki-vl-proxy-prometheusrule.yaml`](../alerting/loki-vl-proxy-prometheusrule.yaml) | PrometheusRule/vmalert-oriented alert set with standardized labels and annotations |
| SRE runbooks | [`docs/runbooks/alerts.md`](runbooks/alerts.md) | Index plus per-alert runbook files referenced directly from alert `runbook_url` |

When using the Helm chart, the runtime templates consume synced copies in `charts/loki-vl-proxy/{dashboards,alerting}`. Keep canonical and chart copies aligned with:

```bash
./scripts/ci/sync_observability_assets.sh sync
./scripts/ci/sync_observability_assets.sh --check
```

`--check` is already enforced in CI to prevent drift.

---

## Preventive Scaling And Deployment

Use the dedicated guide for prevention-oriented operations hardening:

- [`docs/runbooks/deployment-best-practices.md`](runbooks/deployment-best-practices.md)

Critical defaults to reduce incident frequency:

- run at least 2 replicas with PDB enabled
- enable HPA with conservative downscale
- tune cache TTLs differently for query paths vs metadata paths
- monitor backend p95 and proxy p99 histograms, not averages
- add synthetic in-cluster e2e query probes in addition to `/ready`

---

## Multi-Tenancy

Tenant map strategies (inline JSON, a hot-reloaded file, tenant label routing), `-require-tenant-header` and `-auth.enabled`: see [Tenancy and authentication](tenancy-and-auth.md). Tenancy is off by default.

### Who is loading the proxy or VictoriaLogs

Every `slow_request` and `slow_upstream_request` line names its tenant (with the
VictoriaLogs account and project it goes to), its user and Grafana app, and what
it cost VictoriaLogs. `/debug/queries` lists the heaviest callers by
VictoriaLogs time and bytes, and `loki_vl_proxy_tenant_vl_*_total` count each
tenant's VictoriaLogs cost. To name Grafana users set Grafana's
`[dataproxy] send_user_header = true` and `-log-user-header=X-Grafana-User`
(Helm `observability.queryUser.header`). Every identity option, its default and
the spoofing caveat: [Grafana user identity](observability.md#grafana-user-identity)
and [Who is loading the proxy or VictoriaLogs](observability.md#who-is-loading-the-proxy-or-victorialogs).

---

## Health Check Endpoints

The proxy exposes these operational endpoints:

| Endpoint | Purpose | Kubernetes probe |
|----------|---------|-----------------|
| `/alive` | Liveness — confirms the process is running | `livenessProbe` |
| `/ready` | Readiness — confirms the proxy is ready to serve traffic (backend reachable, warm-up complete) | `readinessProbe` |
| `/metrics` | Prometheus metrics scrape. Off by default since v1.56.0: requires `-server.register-instrumentation=true`, and is served on `--metrics-listen` when set (the Helm chart uses `:9091`), otherwise on the main listener | ServiceMonitor / scrape config |

Admin and debug routes (`/admin/cache/flush`, `/debug/pprof/*`, `/debug/queries`) are served on the loopback `--admin-listen` address (default `127.0.0.1:3101`) unless `-server.admin-auth-token` is set, in which case they move to the main listener and require that token. `/admin/cache/flush` exists only when `-server.register-instrumentation=true`; `POST /admin/cache/flush?peers=1` also purges every peer in the ring through the token-protected `POST /_cache/purge` peer endpoint.

If `/ready` stays non-`ok` immediately after a restart, check whether patterns or indexed label-values startup warm is configured — those persistence restores can intentionally hold readiness at `503` until warm-up completes.

---

## Translation Modes

Translation guidance moved to dedicated docs:

- [Translation Modes Guide](translation-modes.md) for mode selection and exact underscore vs dotted behavior
- [Configuration](configuration.md#label-translation) for flag reference
- [Translation Reference](translation-reference.md) for LogQL-to-LogsQL execution mapping

Operational recommendation:

- use `label-style=underscores` (default) when upstream VL stores dotted OTel fields; `passthrough` when VL already stores underscore names
- use `metadata-field-mode=hybrid` for mixed Loki + OTel field workflows
- use `metadata-field-mode=translated` (default) for strict Loki-style field surfaces
- use `metadata-field-mode=native` for OTel-native field-only surfaces

### JSON lines pushed through VictoriaLogs' Loki API

VictoriaLogs' Loki push endpoint (`/insert/loki/api/v1/push`) parses a JSON log line into separate fields at ingest by default. The proxy reads every stored field the way Loki reads structured metadata, so a key of the JSON body becomes visible where Loki needs a parser to see it:

- a label filter before any parser (`{env="production"} | trace_id!=""`, the query Logs Drilldown's fields page sends for a field listed without a parser) also keeps the lines whose body carries the key. On the compose stack this kept 459,595 lines where Loki kept 189,140, and the 38 MB answer was too large for Grafana's Loki plugin process;
- a stream selector matcher, or a grouping, on a label that a body key of the same name shadows reads the body's value (see the open cases in [real-window-compatibility-gaps.md](real-window-compatibility-gaps.md)).

To keep Loki's semantics for these queries, push with JSON message parsing disabled: start VictoriaLogs with `-loki.disableMessageParsing`, or send `disable_message_parsing=1` (or the `VL-Loki-Disable-Message-Parsing: 1` header) on the push requests (VictoriaLogs v1.49.0 and later). The keys then stay in `_msg`, where `| json` reads them as in Loki. The trade-off is query cost: VictoriaLogs unpacks the line at query time for every query that reads a body key, instead of reading a stored field.

`-max-stats-query-series` defaults to 1000, above Loki's `max_query_series` of 500, because the proxy answers faster than Loki; set it to 500 to match Loki. Keep it modest: with a raised cap, a metric query grouped by a per-request id answers one series per id, and Grafana renders hundreds of thousands of series for one panel.

For complete Fields comparisons, match the effective series limits on both
servers, including tenant overrides. A partial-result warning means values
were omitted or the range was cut; Grafana's **Show all N** only expands the
frames already received. It does not retrieve omitted values. The query's
`limit` parameter controls log entries, not metric series.

Query response cache keys include the resolved series limit, so a replica
with a raised or lowered limit never serves an answer cached under the old
limit, including from SSD after restart or a peer with different settings.

The UI log generator sends the original lines with
`disable_message_parsing=1` in both live and backfill mode. This preserves
Loki's distinction between JSON body keys and structured metadata. Existing
rows written with message parsing enabled keep their old fields; restarting
the generator does not change them.

The Loki-compatible profile discovers fields from sampled log lines.
Native and hybrid profiles can also expose fields known to the VictoriaLogs
index. The compatibility stack defaults `PATTERNS_MAX_QUERY_SERIES` to
1,000,000 so the proxy and its reference Loki can compare complete large
answers. In the playground, a smaller override limits Grafana's frame
count and enables partial-result notices on charts that reach the cap.
Opening all high-cardinality fields can exhaust Grafana's datasource plugin
memory even when VictoriaLogs and the proxy are healthy. Cache hits still
return the full cached frame inputs to Grafana.

If Grafana reports `Plugin unavailable`, inspect its datasource plugin logs
and cgroup `memory.events` counter `oom_kill`. A killed child process can
leave Grafana serving `/api/health`; a successful later datasource health
request can also restart the plugin. The UI CI shard captures these counters
before and after its tests and fails on a new kill, replacement or restart.
Its memory peak is the container lifetime high-water, not per-query memory.
Equal effective limits still share answers. Metadata listings and VictoriaLogs
bucket caches remain reusable. During a rolling change, each limit can hold
its own response until normal expiry or eviction; physical cache budgets do
not increase.

---

## Capacity Planning

### Memory

| Component | Memory per Unit |
|-----------|----------------|
| L1 cache | ~50MB per 10k entries |
| L2 disk cache (bbolt) | ~10MB mmap overhead |
| Per active query | ~1-5MB (depends on result size) |
| Singleflight coalescing buffer | Up to 256MB per unique query |
| Base process | ~20MB |

**Formula**: `base(20MB) + cache(entries × 5KB) + concurrent_queries × 3MB`

Default `-cache-max` is `10000` (binary default). The Helm chart ships `50000` to suit light-to-moderate production use. For 50k cache entries and 100 concurrent queries: ~570MB recommended limit.

### CPU

The proxy is CPU-light. Main costs:
- JSON marshaling/unmarshaling (~70% of CPU)
- LogQL→LogsQL translation (~10%)
- Label translation (~5%)
- HTTP overhead (~15%)

**Guideline**: 1 CPU core handles ~2000 req/s.

### Disk Cache

L2 disk cache with bbolt:
- 1 million entries ≈ 2-5GB on disk (gzip compressed)
- Write amplification: ~2x with bbolt
- Use fast SSD (NVMe) for the cache volume
- Set `disk-cache-flush-size=500` and `disk-cache-flush-interval=10s` for batched writes

---

## Performance Tuning

### Cache TTLs

Default TTLs are conservative. Adjust for your query patterns:

```bash
-cache-ttl=120s          # Increase for stable label sets
-cache-max=50000         # Increase for high-cardinality environments
```

| Endpoint | Default TTL | Notes |
|----------|-------------|-------|
| labels, label_values | 5m | `-labels-cache-ttl`; scaled up for longer request windows, capped at 1h |
| detected_fields, detected_field_values, detected_labels | 90s | scaled up for longer request windows, capped at 1h |
| series | 30s | Tier0 compatibility cache |
| query_range, query | 5m final-response cache | requests ending within `-recent-tail-refresh-window` (default `2m`) of now refetch once the entry is older than `-recent-tail-refresh-max-staleness` (default `2s`) |
| index_stats, volume, volume_range | 10s | |

The per-endpoint TTLs above are built in; `-labels-cache-ttl` is the only per-endpoint override. Query-range split windows use `-query-range-history-cache-ttl` / `-query-range-recent-cache-ttl`.

### Concurrency Limits

```bash
-http-max-header-bytes=1048576   # 1MB default
-http-max-body-bytes=10485760    # 10MB default
```

Identical read requests share one run. Requests with the same route, parameters, tenant, credentials and response profile that arrive while the first one runs wait for it, on every cached read route (query, query_range, series, labels, label values, volume, index stats, detected fields and labels, patterns), GET and POST: N identical requests make 1 VictoriaLogs call, whether they succeed or fail. A client that is still waiting never receives another request's cancellation or timeout; it starts again as the new leader.

A failed answer is then held per replica for the time of its class, so a dashboard refreshing against a failing VictoriaLogs asks it about once per second per query instead of on every refresh: `-error-cache-transient-ttl` (default `1s`) for VictoriaLogs 5xx, 429, queue timeouts and refused or reset connections, `-error-cache-data-ttl` (default `5s`) for failures read from the window's data (series or size limits, a JSON parser or unwrap conversion error under a metric). The client's own cancellation, any timeout, partial answers and the proxy's own `429` are never held. Held answers are never pushed to peers, are bounded by `-error-cache-max-entries` and `-error-cache-max-bytes`, and are dropped on a configuration reload. Watch `loki_vl_proxy_error_cache_hits_total{class}` and `loki_vl_proxy_request_flight_saved_total`; the flags are in the [configuration reference](configuration.md).

### Built-In Traffic Guards

All traffic guard controls are tunable via CLI flags (or `extraArgs` in the Helm chart):

| Flag | Default | Description |
|---|---|---|
| `-rate-limit-per-second` | `50` | Per-client request rate (req/s) |
| `-rate-limit-burst` | `100` | Per-client burst allowance |
| `-max-concurrent` | `100` | Per-replica in-flight request cap (excess gets `503` with `Retry-After: 5`); also bounds concurrent backend operations |
| `-cb-fail-threshold` | `5` | Failures within window to open circuit breaker |
| `-cb-open-duration` | `10s` | How long circuit breaker stays open |
| `-cb-window-duration` | `30s` | Failure counting window |
| `-backend-max-concurrent-heavy-queries` | `2` | Heavy VictoriaLogs calls running at once per replica (raw-row metric fetches, stats and hits over `-backend-heavy-query-min-range`); excess waits `-backend-heavy-query-queue-wait` (`20s`), then gets `429` |
| `-backend-max-concurrent-metadata-scans` | `8` | Ceiling of the adaptive limit on long-range VictoriaLogs metadata scans per replica (`/labels`, `/label/{name}/values`, `/series`, `detected_fields` over `-backend-heavy-query-min-range`, and the day bucket scans of the label inventory); selects VictoriaLogs runs for others count against it, and it follows scan latency (`-backend-metadata-scan-latency-tolerance`, `1.5`), backend failures and VictoriaLogs memory (`-backend-metadata-scan-memory-headroom`, `0.4`, read from its `/metrics`) between `-backend-min-concurrent-metadata-scans` (`1`) and this ceiling; excess waits `-backend-heavy-query-queue-wait`, then gets `429`; background warm-ups skip instead of waiting |
| `-metadata-inventory-parallelism` | `4` | Bucket listings in flight per label request from the time-bucketed label inventory (day, hour, 5-minute and minute buckets cached and merged; a refresh reads only its edges). `0` turns the inventory off |

If defaults are too strict or too loose for your workload, tune at the proxy first, then complement with:

- reduced Grafana auto-refresh and retry pressure
- ingress or service-mesh shaping in front of the proxy
- scale out replicas and raise cache effectiveness before pushing more uncached load

---

## Monitoring

See the dedicated [Observability Guide](observability.md) for the full metrics catalog, JSON log schema, OTLP push configuration, and collector/agent integration examples.

### Metrics

The proxy exposes Prometheus metrics at `/metrics`:

Use the [Observability Guide](observability.md) as the canonical catalog for:

- every documented `loki_vl_proxy_*` metric family
- cardinality level (`Low`, `Medium`, `High (capped)`) for each family
- scrape versus OTLP field/label mapping
- the new fanout and proxy-internal operation metrics/log fields

| Metric | Type | Primary dimensions | Description |
|--------|------|--------------------|-------------|
| `loki_vl_proxy_requests_total` | counter | `system`, `direction`, `endpoint`, `route`, `status` | Total requests by downstream Loki route or upstream backend route |
| `loki_vl_proxy_request_duration_seconds` | histogram | `system`, `direction`, `endpoint`, `route` | End-to-end request latency |
| `loki_vl_proxy_backend_duration_seconds` | histogram | `system`, `direction`, `endpoint`, `route` | Upstream-only latency for VictoriaLogs and rules/alerts backends |
| `loki_vl_proxy_cache_hits_by_endpoint` / `loki_vl_proxy_cache_misses_by_endpoint` | counter | `system`, `direction`, `endpoint`, `route` | Cache efficiency by normalized route |
| `loki_vl_proxy_tenant_requests_total` / `loki_vl_proxy_client_requests_total` | counter | tenant/client plus route dimensions | Hot tenants and clients per route |
| `loki_vl_proxy_process_*` | gauges/counters | metric family specific | Runtime, CPU, memory, disk, network, and PSI health |

### Key Ratios to Monitor

- **Route cache hit ratio**: `cache_hits_by_endpoint / (cache_hits_by_endpoint + cache_misses_by_endpoint)` by `endpoint,route` — target >80% on stable metadata paths
- **Downstream error rate**: `requests_total{system="loki",direction="downstream",status=~"5.."}` over total downstream requests — target &lt;1%
- **Upstream latency**: `backend_duration_seconds` by `endpoint,route` — use this to separate VictoriaLogs slowness from proxy-side work
- **End-to-end latency**: `request_duration_seconds{system="loki",direction="downstream"}` by `endpoint,route` — compare with upstream latency and request logs

### OTLP Push

Push metrics to an OTLP collector:

```bash
-otlp-endpoint=http://otel-collector:4318/v1/metrics
-otlp-interval=30s
-otlp-compression=gzip
```

The OTLP exporter reuses the same core proxy metric names that `/metrics` exposes, so dashboards and alert logic can stay aligned across scrape and push modes.

For exact proxy-only overhead on translated paths, use structured request logs with `proxy.overhead_ms`, `proxy.duration_ms`, and `upstream.duration_ms`. The metrics intentionally keep route-aware end-to-end and upstream histograms, while logs carry the per-request decomposition.

---

## Troubleshooting

### No Data in Grafana

1. Check proxy health: `curl http://proxy:3100/ready`
2. Check VL backend: `curl http://vl:9428/health`
3. Check proxy logs for translation errors and execution-limit rejections
4. Verify label-style matches your VL ingestion format
5. Check `/loki/api/v1/labels` for available labels

### Label Names Don't Match

| Symptom | Cause | Fix |
|---------|-------|-----|
| Dots in Grafana labels | `label-style=passthrough` with dotted VL data | Set `label-style=underscores` (the default) |
| Empty label_values for service_name | VL stores `service.name`, query asks `service_name` | Set `label-style=underscores` (the default) |
| Grafana Drilldown "failed to fetch" | Volume/stats endpoint issue | Check proxy logs, ensure VL v1.49+ |

### High Memory Usage

- Reduce `-cache-max` (default 10000)
- Reduce `-http-max-body-bytes`
- Add memory limits in Kubernetes
- Check for singleflight amplification (many unique queries)

### High Latency

- Keep `-response-compression=gzip` for broad Loki/Grafana compatibility; `auto` now behaves the same on the frontend for legacy configs
- Set `-response-compression-min-bytes` around `1024` to avoid wasting CPU on small metadata/control responses
- Increase cache TTLs
- Check VL backend latency via metrics
- Rely on the built-in sharing of identical concurrent reads and on held failures (see above)

### 500 / 429 Errors On Large Queries

Not every `500` means VictoriaLogs is down. Like Loki, the proxy answers every failed backend call with `500` (never `502` or `503`), and built-in execution limits reject oversized work instead of returning truncated data. A client reads Loki's text for the limit's class and never a flag name; the `request error` log line of the rejected request names the flag (`limit_flag`) and its setting (`limit_value`):

- `400` (`bad_data`) with `limit reached while evaluating the query` and `limit_flag=-manual-range-metric-row-limit` in the log line (its `upstream_error` says `manual range metric row limit exceeded`) — raise `-manual-range-metric-row-limit` or narrow the query; it is a client error, as Loki answers a query over one of its limits, so it counts toward `LokiVLProxyHighClientErrors`, not the 5xx alert. A `sum`, `min`, `max`, `avg` or `count` `without (...)` reads a series per stream before the proxy folds them; when those series pass the limit the same message answers `400` (if the limit is below the tenant's effective `max_query_series`, which is `-max-stats-query-series` unless the tenant has its own override, that limit bounds the reads and Loki's series message answers instead; this `400` is the same answer as above), and the folded answer is then held to `max_query_series`
- `400` with `maximum number of series (N) reached for a single query` — Loki's own answer above `max_query_series`; narrow the query or raise `-max-stats-query-series`. The log line for the rejected request names that flag (`limit_flag`). Grafana Logs Drilldown requests instead receive a partial result with the warning `... returning partial results`
- `429` with Loki's `too many outstanding requests`, `Retry-After: 5` and `limit_flag=-max-concurrent` in the log line — the `-max-concurrent` admission cap was reached. The per-client rate limit answers the same text with `Retry-After: 1` and `limit_flag=-rate-limit-per-second`
- `500` with `rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (N vs. LIMIT)` — a response above a buffer limit (`-backend-max-buffered-response-bytes`, `-ordered-json-metric-max-bytes`, `-ordered-json-metric-max-samples`, `-label-values-max-response-bytes`, `-multi-tenant-max-merged-response-bytes`); the log line's `limit_flag` names which, and `upstream_error` says what was too large
- `429` with Loki's `too many outstanding requests` and, in the `request error` log line, `limit_flag=-backend-max-concurrent-heavy-queries` and an `upstream_error` of `heavy VictoriaLogs queries are limited to -backend-max-concurrent-heavy-queries=N` — heavy long-range VictoriaLogs calls waited longer than `-backend-heavy-query-queue-wait` for a slot. Retry, narrow the time range, or raise `-backend-max-concurrent-heavy-queries` together with VictoriaLogs memory (see [Heavy VictoriaLogs Query Admission](configuration.md#heavy-victorialogs-query-admission))
- `429` with Loki's `too many outstanding requests` and, in the log line, `limit_flag=-backend-max-concurrent-metadata-scans` and an `upstream_error` of `long-range VictoriaLogs metadata scans are limited to at most -backend-max-concurrent-metadata-scans=N per replica (adaptive limit L, ...)` — a long-range `/labels`, `/label/{name}/values`, `/series` or `detected_fields` request waited longer than `-backend-heavy-query-queue-wait` for one of its long scans: VictoriaLogs was running as many long selects as the adaptive limit (for this replica or others), concurrent scans had slowed down, a scan failed, VictoriaLogs' memory in use left less than `-backend-metadata-scan-memory-headroom` free, or a listing no replica has measured yet was waiting for VictoriaLogs to run no other select (the logged message says which). The buckets the request did scan stay cached, so a retry continues where it stopped. The backend is saturated: give VictoriaLogs memory or cores, or narrow the range; raising the ceiling does not help while the memory gate or VictoriaLogs' own load is what closed. The same 429 whose logged message ends in `the scan was stopped to keep VictoriaLogs' memory inside the headroom` means the scan had started and VictoriaLogs' memory in use then passed the brake mark (a third of the way into `-backend-metadata-scan-memory-headroom`); the proxy stopped it before VictoriaLogs ran out of memory.
- `500` with `Internal Server Error` and, in the `request error` log line, `plan_defect=true`, `plan_route` (the VictoriaLogs route that refused the call), `logsql_hash` and VictoriaLogs' message as `upstream_error` — VictoriaLogs refused the query the proxy wrote for a request it had already checked as Loki does. The query is valid for Loki, so this is a proxy defect, not a user error and not an outage: `loki_vl_proxy_plan_defects_total{endpoint,plan_route}` counts them. Report the `logsql_hash`, the route and the LogQL of the request; retrying does not help. These answers count toward `LokiVLProxyHighErrorRate` and `LokiVLProxyCriticalErrorRate`, not `LokiVLProxyHighClientErrors`.
`line_format` and binary-expression evaluation limits return `400`. See [Fixed Execution Limits](configuration.md#fixed-execution-limits).

### Circuit Breaker Tripping

The circuit breaker opens after `-cb-fail-threshold` backend transport failures (connection errors) within `-cb-window-duration`. HTTP error responses from VictoriaLogs and timeouts do not count, because they prove the backend is reachable. Check:
- VL backend health and logs
- Network connectivity between proxy and VL
- VL resource usage (CPU/memory/disk)

---

## Backup & Recovery

The proxy is stateless. Only the optional disk cache needs backup:

- **L1 cache**: In-memory, rebuilds on restart
- **L2 disk cache**: bbolt file at `-disk-cache-path`. Can be deleted safely — will be repopulated.
- **Configuration**: All config is CLI flags / env vars. Store in Helm values or ConfigMap.

---

## Scaling

### Horizontal Scaling

```yaml
horizontalPodAutoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

### Pod Disruption Budget

```yaml
podDisruptionBudget:
  enabled: true
  minAvailable: 1
```

### Multi-Zone Deployment

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: loki-vl-proxy
```
