# API Reference

## Loki-Compatible Endpoints

| Loki Endpoint | Status | VL Backend | Cached | Tests |
|---|---|---|---|---|
| `POST /loki/api/v1/query_range` (logs) | Implemented | `/select/logsql/query` | 10s | 6 |
| `POST /loki/api/v1/query_range` (metrics) | Implemented | `/select/logsql/stats_query_range` | 10s | 1 |
| `GET /loki/api/v1/query` | Implemented | `/select/logsql/query` or `stats_query` | 10s | 1 |
| `GET /loki/api/v1/labels` | Implemented | `/select/logsql/stream_field_names` with fallback to `/select/logsql/field_names` | 60s | 3 |
| `GET /loki/api/v1/label/{name}/values` | Implemented | `/select/logsql/stream_field_values` with fallback to `/select/logsql/field_values` | 60s | 3 |
| `GET /loki/api/v1/series` | Implemented | `/select/logsql/streams` | 30s | 2 |
| `GET /loki/api/v1/index/stats` | Implemented | `/select/logsql/hits` | - | 2 |
| `GET /loki/api/v1/index/volume` | Implemented | `/select/logsql/hits` (field grouping) | - | 2 |
| `GET /loki/api/v1/index/volume_range` | Implemented | `/select/logsql/hits` (step) | - | 2 |
| `GET /loki/api/v1/detected_fields` | Implemented | `/select/logsql/field_names` | 30s | 1 |
| `GET /loki/api/v1/detected_field/{name}/values` | Implemented | `/select/logsql/field_values` | - | 1 |
| `GET /loki/api/v1/detected_labels` | Implemented | `/select/logsql/field_names` | - | 1 |
| `GET /loki/api/v1/patterns` | Implemented | `/select/logsql/query` + pattern extraction | - | 3 |
| `GET /loki/api/v1/format_query` | Implemented | - (passthrough) | - | 1 |
| `WS /loki/api/v1/tail` | Implemented | `/select/logsql/tail` (WebSocket->NDJSON) | - | 2 |

### Drilldown Field Shaping

For Grafana Logs Drilldown and Explore compatibility:

- Query results still use canonical Loki 2-tuples: `[timestamp, line]`.
- Stream labels stay Loki-compatible on the `stream` object.
- Label APIs prefer VictoriaLogs stream metadata so parsed fields do not leak into Loki label pickers when the backend supports the stream-only endpoints.
- Parsed fields and structured metadata are surfaced through `detected_fields` and `detected_field/{name}/values`.
- With `-metadata-field-mode=hybrid` (the default), field-oriented APIs expose both native VictoriaLogs dotted names and translated Loki aliases when they differ, for example `service.name` and `service_name`.
- Synthetic compatibility labels such as `service_name` and `detected_level` stay available on the stream and label APIs.

## Delete Endpoint (Exception)

| Endpoint | Method | VL Backend |
|---|---|---|
| `/loki/api/v1/delete` | POST | `/select/logsql/delete` |

The delete endpoint is the only write operation exposed. It includes strict safeguards:

- **Confirmation header**: Requires `X-Delete-Confirmation: true`
- **Query required**: Must target specific streams (no wildcards `{}` or `*`)
- **Time range required**: Both `start` and `end` parameters mandatory
- **Time range limit**: Maximum 30 days per delete operation
- **Tenant scoping**: Deletes scoped to the requesting tenant's data
- **Audit logging**: All delete operations logged at WARN level with tenant, query, time range, client IP

### Example

```bash
curl -X POST 'http://proxy:3100/loki/api/v1/delete' \
  -H 'X-Delete-Confirmation: true' \
  -H 'X-Scope-OrgID: team-alpha' \
  -d 'query={app="nginx",env="staging"}&start=1704067200&end=1704153600'
```

## Write Endpoint (Blocked)

| Endpoint | Response |
|---|---|
| `POST /loki/api/v1/push` | 405 Method Not Allowed |

This is a read-only proxy. Log ingestion should go directly to VictoriaLogs.

## Alerting and Config Compatibility

| Endpoint | Response | Purpose |
|---|---|---|
| `GET /loki/api/v1/rules` | Legacy Loki YAML rules view when `-ruler-backend` is configured, otherwise empty YAML rules map | Loki/Grafana compatibility |
| `GET /loki/api/v1/rules/{namespace}` | Legacy Loki YAML rules view filtered to a namespace when `-ruler-backend` is configured | Loki compatibility |
| `GET /loki/api/v1/rules/{namespace}/{group}` | Legacy Loki YAML single-rule-group view when `-ruler-backend` is configured | Loki compatibility |
| `GET /api/prom/rules` | Legacy Loki YAML alias for `/loki/api/v1/rules` when `-ruler-backend` is configured, otherwise empty YAML rules map | Loki/Grafana compatibility |
| `GET /api/prom/rules/{namespace}` | Legacy Loki YAML alias for namespace-filtered rules | Loki compatibility |
| `GET /api/prom/rules/{namespace}/{group}` | Legacy Loki YAML alias for single-rule-group lookups | Loki compatibility |
| `GET /prometheus/api/v1/rules` | Prometheus-style JSON passthrough when `-ruler-backend` is configured, otherwise empty rules JSON stub | Grafana alerting compatibility |
| `GET /loki/api/v1/alerts` | JSON passthrough when `-alerts-backend` or `-ruler-backend` is configured, otherwise empty alerts | Loki/Grafana compatibility |
| `GET /api/prom/alerts` | JSON alias for `/loki/api/v1/alerts` | Loki/Grafana compatibility |
| `GET /prometheus/api/v1/alerts` | Prometheus-style JSON passthrough when `-alerts-backend` or `-ruler-backend` is configured | Grafana alerting compatibility |
| `GET /config` | YAML stub | Configuration endpoint |

Read-path alerting compatibility follows Loki-facing routes and query parameters, including legacy YAML responses on the classic Loki rules endpoints. Write-path ruler APIs are still not implemented. If you configure a backend such as `vmalert`, the proxy forwards tenant context using VictoriaLogs-style `AccountID` and `ProjectID` headers after applying the normal tenant mapping logic.

Query endpoints also support Loki-style explicit multi-tenant headers such as `X-Scope-OrgID: team-a|team-b`. The proxy fans those requests out per tenant, merges the Loki-shaped responses, and injects synthetic `__tenant_id__` labels in merged results. Leading-selector `__tenant_id__` matchers such as `{app="api",__tenant_id__="team-b"}` narrow the fanout set before backend requests are issued. `/tail`, delete, and write endpoints remain single-tenant.

`/loki/api/v1/drilldown-limits` is a bootstrap/capability endpoint for Grafana Logs Drilldown and does not require a tenant header, even when `-auth.enabled=true`.

## Infrastructure Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /ready` | Readiness probe (checks VL `/health` + circuit breaker) |
| `GET /loki/api/v1/status/buildinfo` | Fake Loki 2.9.0 build info for Grafana detection |
| `GET /metrics` | Prometheus text exposition (`-server.register-instrumentation`) |
| `GET /debug/queries` | Query analytics, disabled by default (`-server.enable-query-analytics`) |
| `GET /debug/pprof/` | Go profiling, disabled by default (`-server.enable-pprof`) |

## Observability

### Metrics (Prometheus)

```
# Request tracking
loki_vl_proxy_requests_total{endpoint, status}
loki_vl_proxy_request_duration_seconds{endpoint}  (histogram)

# Per-tenant tracking
loki_vl_proxy_tenant_requests_total{tenant, endpoint, status}
loki_vl_proxy_tenant_request_duration_seconds{tenant, endpoint}  (histogram)

# Per-client tracking
loki_vl_proxy_client_requests_total{client, endpoint}
loki_vl_proxy_client_response_bytes_total{client}
loki_vl_proxy_client_status_total{client, endpoint, status}
loki_vl_proxy_client_inflight_requests{client}
loki_vl_proxy_client_request_duration_seconds{client, endpoint}  (histogram)
loki_vl_proxy_client_query_length_chars{client, endpoint}  (histogram)

# Client error breakdown
loki_vl_proxy_client_errors_total{endpoint, reason}

# Cache efficiency
loki_vl_proxy_cache_hits_total
loki_vl_proxy_cache_misses_total

# Fleet peer-cache visibility
loki_vl_proxy_peer_cache_peers
loki_vl_proxy_peer_cache_cluster_members
loki_vl_proxy_peer_cache_hits_total
loki_vl_proxy_peer_cache_misses_total
loki_vl_proxy_peer_cache_errors_total

# Translation tracking
loki_vl_proxy_translations_total
loki_vl_proxy_translation_errors_total

# Circuit breaker
loki_vl_proxy_circuit_breaker_state  (0=closed, 1=open, 2=half-open)

# System
loki_vl_proxy_uptime_seconds
```

### Request Logs

Structured JSON to stdout:

```json
{"time":"2026-04-04T10:30:00Z","level":"INFO","msg":"request","endpoint":"query_range","method":"GET","status":200,"duration_ms":42,"tenant":"team-alpha","query":"{app=\"nginx\"} |= \"error\"","client":"10.0.1.42:5678","client_id":"alice@example.com","client_source":"grafana_user"}
```
