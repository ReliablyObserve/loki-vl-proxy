# Operations Guide

## Deployment

### Minimum Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 50m | 200m |
| Memory | 64Mi | 256Mi |
| Replicas | 1 | 2+ (with PDB) |

The proxy is stateless (except optional disk cache). Scale horizontally without coordination.

Current implementation note:

- per-client rate limiting, global concurrent-query protection, and the backend circuit breaker are built in with fixed defaults today
- the current CLI does not expose direct tuning flags for those controls
- use Grafana refresh policy, ingress shaping, HPA, and cache tuning as the primary operator levers

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
| `-label-style` | No | `passthrough` (default) or `underscores` |

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
| Grafana operations dashboard | [`dashboard/loki-vl-proxy.json`](../dashboard/loki-vl-proxy.json) | Explicit `Client-Side Loki`, `Proxy Internal`, and `Backend-Side VictoriaLogs` row groups with route-aware RED signals, cache/fanout tuning, and operational resources |
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

## Translation Modes

Translation guidance moved to dedicated docs:

- [Translation Modes Guide](translation-modes.md) for mode selection and exact underscore vs dotted behavior
- [Configuration](configuration.md#label-translation) for flag reference
- [Translation Reference](translation-reference.md) for LogQL-to-LogsQL execution mapping

Operational recommendation:

- use `label-style=underscores` when upstream VL stores dotted OTel fields
- use `metadata-field-mode=hybrid` for mixed Loki + OTel field workflows
- use `metadata-field-mode=translated` for strict Loki-style field surfaces
- use `metadata-field-mode=native` for OTel-native field-only surfaces

---

## Capacity Planning

### Memory

| Component | Memory per Unit |
|-----------|----------------|
| L1 cache (default 10k entries) | ~50MB |
| L2 disk cache (bbolt) | ~10MB mmap overhead |
| Per active query | ~1-5MB (depends on result size) |
| Singleflight coalescing buffer | Up to 256MB per unique query |
| Base process | ~20MB |

**Formula**: `base(20MB) + cache(entries × 5KB) + concurrent_queries × 3MB`

For 10k cache entries and 100 concurrent queries: ~370MB recommended limit.

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

| Endpoint | Default TTL | Recommendation |
|----------|-------------|----------------|
| labels | 60s | 120-300s if label set is stable |
| label_values | 60s | 60-120s |
| series | 30s | 30-60s |
| detected_fields | 30s | 30-60s |
| query_range | 10s | 5-30s depending on freshness needs |
| query | 10s | 5-30s |

### Concurrency Limits

```bash
-http-max-header-bytes=1048576   # 1MB default
-http-max-body-bytes=10485760    # 10MB default
```

The proxy uses singleflight to coalesce identical concurrent queries. N identical requests → 1 backend request.

### Built-In Traffic Guards

The current code uses these built-in defaults:

- per-client rate limit: `50 req/s`
- per-client burst: `100`
- global concurrent backend queries: `100`
- circuit breaker: open after `5` failures, remain open for `10s`

These values are not exposed as CLI or Helm flags today. If they are too strict or too loose for your workload, mitigate at the surrounding layers:

- reduce Grafana auto-refresh and retry pressure
- add ingress or service-mesh shaping in front of the proxy
- scale out replicas and raise cache effectiveness before pushing more uncached load through the same pods

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
3. Check proxy logs for translation errors
4. Verify label-style matches your VL ingestion format
5. Check `/loki/api/v1/labels` for available labels

If `/ready` stays non-`ok` immediately after a restart, also check whether patterns or indexed label-values startup warm is configured. Those persistence restores can intentionally hold readiness at `503` until warm-up completes.

### Label Names Don't Match

| Symptom | Cause | Fix |
|---------|-------|-----|
| Dots in Grafana labels | `label-style=passthrough` with dotted VL data | Set `label-style=underscores` |
| Empty label_values for service_name | VL stores `service.name`, query asks `service_name` | Set `label-style=underscores` |
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

### Circuit Breaker Tripping

The circuit breaker opens after consecutive backend 5xx responses. Check:
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
