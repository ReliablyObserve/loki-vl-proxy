# Operations Guide

## Deployment

### Minimum Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 50m | 200m |
| Memory | 64Mi | 256Mi |
| Replicas | 1 | 2+ (with PDB) |

The proxy is stateless (except optional disk cache). Scale horizontally without coordination.

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

### Required Configuration

| Flag | Required | Description |
|------|----------|-------------|
| `-backend` | Yes | VictoriaLogs URL |
| `-listen` | No | Listen address (default `:3100`) |
| `-label-style` | No | `passthrough` (default) or `underscores` |

---

## Operational Assets

Treat these as one versioned operational package:

| Asset | Canonical source | Purpose |
|------|------------------|---------|
| Grafana dashboard | [`dashboard/loki-vl-proxy.json`](../dashboard/loki-vl-proxy.json) | SLO and troubleshooting views for request, cache, tenant, and backend signals |
| Alert rules | [`alerting/loki-vl-proxy-prometheusrule.yaml`](../alerting/loki-vl-proxy-prometheusrule.yaml) | PrometheusRule/vmalert-oriented alert set with standardized labels and annotations |
| SRE runbooks | [`docs/runbooks/alerts.md`](runbooks/alerts.md) | Incident playbooks referenced directly from alert `runbook_url` |

When using the Helm chart, the runtime templates consume synced copies in `charts/loki-vl-proxy/{dashboards,alerting}`. Keep canonical and chart copies aligned with:

```bash
./scripts/ci/sync_observability_assets.sh sync
./scripts/ci/sync_observability_assets.sh --check
```

`--check` is already enforced in CI to prevent drift.

---

## Label Translation

### When to Use Each Mode

| VL Ingestion | Pipeline | Label Style | Result |
|---|---|---|---|
| OTLP direct → VL | None | `underscores` | `service.name` → `service_name` in Loki |
| Vector → VL | Vector normalizes dots→underscores | `passthrough` | Already underscore, no translation needed |
| FluentBit → VL | FluentBit preserves dots | `underscores` | Dots translated to underscores |
| Loki push → VL | Any | `passthrough` | Labels already Loki-compatible |
| ES bulk → VL | Any | Depends on field names | Use `field-mapping` for custom |

### Custom Field Mapping

For non-standard VL field names:

```bash
-field-mapping '[
  {"vl_field": "my_app_trace_id", "loki_label": "traceID"},
  {"vl_field": "internal.request.id", "loki_label": "request_id"}
]'
```

Custom mappings override automatic translation for both query and response directions.

### Known OTel Semantic Fields

The proxy has built-in reverse mappings for 50+ OTel semantic convention fields:

- **Service**: `service_name`, `service_namespace`, `service_version`, `service_instance_id`
- **Kubernetes**: `k8s_pod_name`, `k8s_namespace_name`, `k8s_node_name`, `k8s_container_name`, `k8s_deployment_name`, `k8s_daemonset_name`, `k8s_statefulset_name`, `k8s_replicaset_name`, `k8s_job_name`, `k8s_cronjob_name`, `k8s_cluster_name`
- **Cloud**: `cloud_provider`, `cloud_platform`, `cloud_region`, `cloud_availability_zone`, `cloud_account_id`
- **Host**: `host_name`, `host_id`, `host_type`, `host_arch`
- **Process**: `process_pid`, `process_executable_name`, `process_runtime_name`, `process_runtime_version`
- **Container**: `container_id`, `container_name`, `container_runtime`, `container_image_name`, `container_image_tag`
- **Network**: `net_host_name`, `net_host_port`, `net_peer_name`, `net_peer_port`
- **OS**: `os_type`, `os_version`
- **Log**: `log_file_path`, `log_file_name`, `log_iostream`
- **Telemetry**: `telemetry_sdk_name`, `telemetry_sdk_language`, `telemetry_sdk_version`
- **Deployment**: `deployment_environment`, `deployment_environment_name`

Fields NOT in this table pass through as-is in queries. The response direction uses generic regex sanitization for any dotted field.

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

### Rate Limiting

Default: 50 req/s per client IP, burst 100. Adjust:

```yaml
# In Helm values
extraArgs:
  rate-per-second: "100"
  rate-burst: "200"
```

### Circuit Breaker

Opens after 5 consecutive backend failures, stays open for 10 seconds:

```yaml
extraArgs:
  cb-fail-threshold: "10"     # more tolerant
  cb-open-duration: "30s"     # longer backoff
```

---

## Monitoring

See the dedicated [Observability Guide](observability.md) for the full metrics catalog, JSON log schema, OTLP push configuration, and collector/agent integration examples.

### Metrics

The proxy exposes Prometheus metrics at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `loki_vl_proxy_requests_total{endpoint,status}` | counter | Total requests by endpoint and HTTP status |
| `loki_vl_proxy_request_duration_seconds{endpoint}` | histogram | Request latency by endpoint |
| `loki_vl_proxy_cache_hits_total` | counter | L1 cache hits |
| `loki_vl_proxy_cache_misses_total` | counter | L1 cache misses |
| `loki_vl_proxy_translations_total` | counter | LogQL→LogsQL translations |
| `loki_vl_proxy_translation_errors_total` | counter | Failed translations |
| `loki_vl_proxy_uptime_seconds` | gauge | Process uptime |

### Key Ratios to Monitor

- **Cache hit ratio**: `cache_hits / (cache_hits + cache_misses)` — target >80%
- **Error rate**: `requests_total{status=~"5.."} / requests_total` — target <1%
- **P99 latency**: from histogram — target <2s for queries, <100ms for labels

### OTLP Push

Push metrics to an OTLP collector:

```bash
-otlp-endpoint=http://otel-collector:4318/v1/metrics
-otlp-interval=30s
-otlp-compression=gzip
```

The OTLP exporter reuses the same core proxy metric names that `/metrics` exposes, so dashboards and alert logic can stay aligned across scrape and push modes.

---

## Troubleshooting

### No Data in Grafana

1. Check proxy health: `curl http://proxy:3100/ready`
2. Check VL backend: `curl http://vl:9428/health`
3. Check proxy logs for translation errors
4. Verify label-style matches your VL ingestion format
5. Check `/loki/api/v1/labels` for available labels

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

- Enable `-response-gzip` (default: on)
- Increase cache TTLs
- Check VL backend latency via metrics
- Enable singleflight (on by default) to coalesce identical queries

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
