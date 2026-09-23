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

- `-max-concurrent 100` — per-replica in-flight request cap (excess gets `503`); also bounds concurrent backend operations
- `-rate-limit-per-second 50` / `-rate-limit-burst 100` — per-client token bucket
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

### Tenant Mapping Strategies

The proxy maps `X-Scope-OrgID` headers to VictoriaLogs tenant IDs. Three strategies are available depending on deployment size and dynamism.

#### 1. Inline JSON (`-tenant-map`)

Best for small, static tenant maps. The entire map is provided directly as a CLI flag or env var value:

```bash
-tenant-map='{"team-a":{"account_id":"1","project_id":"0"},"team-b":{"account_id":"2","project_id":"0"}}'
```

This requires a proxy restart to update. (A map supplied through the `TENANT_MAP` environment variable is re-read on SIGHUP when no `-tenant-map-file` is set.)

#### 2. File-based (`-tenant-map-file`)

Best for Kubernetes environments where tenant maps are mounted as ConfigMaps. The proxy hot-reloads the file on SIGHUP and also polls for mtime changes on the configured interval:

```bash
-tenant-map-file=/etc/proxy/tenants.yaml
-tenant-map-reload-interval=30s
```

The default reload interval is `30s`. To trigger an immediate reload without restarting the proxy:

```bash
kill -HUP <pid>
```

In Helm, configure a lifecycle hook to send SIGHUP on ConfigMap updates:

```yaml
lifecycle:
  postStart:
    exec:
      command: ["/bin/sh", "-c", "kill -HUP 1"]
```

Polling every `30s` means changes are picked up automatically even without an explicit signal, which suits ConfigMap-mounted files that are updated by an external controller.

#### 3. Label-based (`-tenant-label`)

Scopes each request by a stream field instead of VictoriaLogs `AccountID`/`ProjectID` headers. Useful when the VictoriaLogs default tenant (0:0) holds data for several tenants distinguished by a stream label such as `tenant`:

```bash
-tenant-label=tenant
```

When set, the client still sends `X-Scope-OrgID`. For an org ID that is not in the tenant map, not a default-tenant alias (`0`, `fake`, `default`) and not `*`, the proxy adds a VictoriaLogs `extra_stream_filters` constraint `{"tenant":"<orgID>"}` to backend queries; request parameters cannot override it. The configured field must be a VictoriaLogs **stream field** (part of `_stream_fields` at ingestion). Explicit tenant-map entries take priority, and an unmapped `X-Scope-OrgID: *` is still rejected with `403` unless `-tenant.allow-global=true`.

---

### `-require-tenant-header` Flag

`-require-tenant-header=true` enforces that every request carries an `X-Scope-OrgID` header (returns HTTP 401 if missing) without enabling full auth. This is useful for catching misconfigured clients in multi-tenant setups without a full auth proxy.

`-auth.enabled=true` has the same effect on requests without the header (`401`). Neither flag authenticates the header value; put an authenticating proxy in front of the proxy when tenants must not be able to choose their own `X-Scope-OrgID`.

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

The proxy uses singleflight to coalesce identical concurrent queries. N identical requests → 1 backend request.

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
- Rely on built-in singleflight coalescing for identical concurrent reads

### 502 / 503 Errors On Large Queries

Not every `502` or `503` means VictoriaLogs is down. Built-in execution limits reject oversized work instead of returning truncated data:

- `502` with `manual range metric row limit exceeded` — raise `-manual-range-metric-row-limit` or narrow the query
- `400` with `maximum number of series (N) reached for a single query` — Loki's own answer above `max_query_series`; narrow the query or raise `-max-stats-query-series`. The log line for the rejected request names that flag. Grafana Logs Drilldown requests instead receive a partial result with the warning `... returning partial results`
- `503` with `too many concurrent queries` — the `-max-concurrent` admission cap was reached
- `429` with `too many outstanding requests: heavy VictoriaLogs queries are limited to -backend-max-concurrent-heavy-queries=N` — heavy long-range VictoriaLogs calls waited longer than `-backend-heavy-query-queue-wait` for a slot. Retry, narrow the time range, or raise `-backend-max-concurrent-heavy-queries` together with VictoriaLogs memory (see [Heavy VictoriaLogs Query Admission](configuration.md#heavy-victorialogs-query-admission))

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