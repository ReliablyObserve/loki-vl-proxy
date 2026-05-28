---
sidebar_label: API Reference
description: Complete list of Loki-compatible HTTP endpoints exposed by loki-vl-proxy with request/response formats.
---

# API Reference

## Loki-Compatible Endpoints

| Loki Endpoint | Status | VL Backend | Cached | Tests |
|---|---|---|---|---|
| `GET/POST /loki/api/v1/query_range` (logs) | Implemented | `/select/logsql/query` | 10s | 6+ (1) |
| `GET/POST /loki/api/v1/query_range` (metrics) | Implemented | `/select/logsql/stats_query_range` | 10s | 1+ (1) |
| `GET/POST /loki/api/v1/query` | Implemented | `/select/logsql/query` or `stats_query` | 10s | 1+ (1) |
| `GET /loki/api/v1/labels` | Implemented | `/select/logsql/stream_field_names` with fallback to `/select/logsql/field_names` | 60s | 3 |
| `GET /loki/api/v1/label/{name}/values` | Implemented | `field_names` (candidate resolution, capped to 1h) → `stream_field_names` (endpoint gate) → `stream_field_values` if stream-indexed, else `field_values` | 60s | 3 |
| `GET /loki/api/v1/series` | Implemented | `/select/logsql/streams` | 30s | 2 |
| `GET /loki/api/v1/index/stats` | Implemented | `/select/logsql/hits` | 10s | 2 |
| `GET /loki/api/v1/index/volume` | Implemented | `/select/logsql/hits` (field grouping) | 10s | 2 |
| `GET /loki/api/v1/index/volume_range` | Implemented | `/select/logsql/hits` (step) | 10s | 2 |
| `GET /loki/api/v1/detected_fields` | Implemented | `/select/logsql/field_names` | 30s | 1 |
| `GET /loki/api/v1/detected_field/{name}/values` | Implemented | `/select/logsql/field_values` | 30s | 1 |
| `GET /loki/api/v1/detected_labels` | Implemented | `/select/logsql/field_names` | 30s | 1 |
| `GET /loki/api/v1/patterns` | Implemented (toggleable) | `/select/logsql/query` + Drain-like token clustering | `100y` (effectively persistent) | 4 |
| `GET /loki/api/v1/format_query` | Implemented | - (passthrough) | - | 1 |
| `WS /loki/api/v1/tail` | Implemented | `/select/logsql/tail` (WebSocket->NDJSON) | - | 2 |

**(1)** Test counts shown are baseline per-endpoint counts. Additional coverage from `missing_ops_compat_test.go` adds cross-cutting e2e compatibility tests for `unpack`, `unwrap duration()/bytes()`, `offset`, `label_replace()`, and pattern match line filters across query and query_range endpoints.

### Drilldown Field Shaping

For Grafana Logs Drilldown and Explore compatibility:

- Query results use canonical Loki 2-tuples `[timestamp, line]` by default.
- When `X-Loki-Response-Encoding-Flags: categorize-labels` is present and `-emit-structured-metadata=true`, query results emit Loki 3-tuples `[timestamp, line, metadata]`.
- The 3rd tuple object follows Loki keys only: `structuredMetadata` and/or `parsed` (no snake_case alias keys).
- `structuredMetadata` and `parsed` are Loki metadata objects (`{"name":"value", ...}`), matching Loki query/tail response shape.
- Stream labels stay Loki-compatible on the `stream` object.
- Label APIs prefer VictoriaLogs stream metadata so parsed fields do not leak into Loki label pickers when the backend supports the stream-only endpoints.
- `-extra-label-fields` extends label-facing APIs (`/labels`, `/label/{name}/values`) with explicit VL fields and improves custom dot/underscore alias resolution.
- Optional indexed browse mode for label values (`-label-values-indexed-cache=true`) supports hotset-first responses and optional `offset`/`search` (`search` or `q`) on `GET /loki/api/v1/label/{name}/values`.
- Patterns API can be explicitly gated via `-patterns-enabled` (default `true`) to match deployments that do not expose Drilldown pattern discovery.
- Patterns responses are clamped to `1000` entries per request and can be persisted/restored with `-patterns-persist-*` flags.
- Indexed label-values cache snapshots can be persisted to disk (`-label-values-index-persist-path`) and restored at startup.
- On stale/missing disk snapshot, startup can warm from peer cache before serving (`-label-values-index-startup-stale-threshold`, `-label-values-index-startup-peer-warm-timeout`).
- Peer cache payload fetches (`/_cache/get`) support `zstd` or `gzip` response compression for lower network latency/cost on large cache objects.
- `GET /_cache/has?keys=k1,k2,...` is a lightweight batch peer endpoint that returns key presence and remaining TTL without transferring values. Used during startup warmup so instances can discover which peer has the freshest copy of each label window before fetching.
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

This is a read-only proxy. Log ingestion should go directly to VictoriaLogs-side ingestion paths, for example:

- `vlagent` pipelines targeting VictoriaLogs ([docs](https://docs.victoriametrics.com/victorialogs/data-ingestion/))
- Loki-push-compatible ingestion endpoints handled on the VictoriaLogs side
- OTLP log ingestion into VictoriaLogs
- native JSON / OTel-shaped log ingestion into VictoriaLogs

After ingestion, data is queryable through the proxy's Loki-compatible read API.

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
| `GET /config/tenant/v1/limits` | YAML tenant-limits compatibility view generated by the proxy and optionally overridden by published-limit flags | Grafana / Logs Drilldown bootstrap compatibility |
| `GET /config` | YAML stub | Configuration endpoint |
| `GET /loki/api/v1/drilldown-limits` | Bootstrap/capability endpoint for Grafana Logs Drilldown | Datasource capability probing |

Behavior and scope notes for these endpoints live in:
- [Configuration](configuration.md) for flags, tenant fanout behavior, and backend mapping
- [Known Issues](KNOWN_ISSUES.md) for intentional compatibility boundaries

Write-surface boundary for rules and alerts:

- These routes expose read compatibility only (Loki YAML views and Prometheus-style JSON views).
- Rule and alert write/lifecycle operations remain on [`vmalert`](https://docs.victoriametrics.com/vmalert/) and VictoriaLogs backend systems ([VictoriaLogs docs](https://docs.victoriametrics.com/victorialogs/)).
- The proxy does not implement Loki ruler write APIs.

Tenant limits notes:

- `/config/tenant/v1/limits` is single-tenant only; multi-tenant `X-Scope-OrgID: a|b` returns `400`
- published fields are filtered by `-tenant-limits-allow-publish`
- `-tenant-default-limits` and `-tenant-limits` only override the published compatibility payload; they are not backend quota enforcement

## Error Response Format

All error responses from the proxy use the standard Loki JSON error envelope:

```json
{"status": "error", "errorType": "<type>", "error": "<message>"}
```

| `errorType` | HTTP Status | Source | When |
|---|---|---|---|
| `"parse error"` | 400 | `translator.ParseError` | Invalid LogQL syntax that cannot be parsed |
| `"bad_data"` | 400 | `translator.UnsupportedError` | Valid LogQL with no LogsQL equivalent (e.g. unsupported function) |
| `"execution"` | 500 | Backend or proxy runtime error | VictoriaLogs error, fanout failure, or unexpected proxy failure |

**`parse error`** is returned when the query string cannot be parsed at all. Grafana and LogQL clients display this as a syntax error.

**`bad_data`** is returned when the query is syntactically valid LogQL but uses a construct the proxy cannot translate to a VictoriaLogs equivalent. The `error` field includes the unsupported function or operator name where applicable.

**Query-length violations** return HTTP 400 with a plain `error` field (no `errorType`): `"query length X exceeds limit Y"`.

## Infrastructure Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /alive`, `GET /livez` | Liveness probe — returns 200 when proxy process is healthy |
| `GET /health`, `GET /healthz` | Health probe — returns 200 when healthy |
| `GET /ready` | Readiness probe (checks VL `/health` + circuit breaker) |
| `GET /loki/api/v1/status/buildinfo` | Returns Loki `3.7.1` build info — version ≥ 3.0.0 is required for Grafana to send `X-Loki-Response-Encoding-Flags: categorize-labels`, which enables `structuredMetadata` in `query_range` responses |
| `GET /metrics` | Prometheus text exposition (`-server.register-instrumentation`); low-cardinality by default unless `-metrics.export-sensitive-labels=true` |
| `POST /admin/cache/flush` | Flush all caches (requires `-admin-auth-token`) |
| `GET /debug/queries` | Query analytics, disabled by default (`-server.enable-query-analytics`) |
| `GET /debug/pprof/` | Go profiling, disabled by default (`-server.enable-pprof`) |

### Peer Cache Endpoints

These endpoints are registered when peer cache is enabled. Protected by `-peer-auth-token` or source-IP peer membership.

| Endpoint | Purpose |
|---|---|
| `GET /_cache/get?key=…` | Fetch a cached entry from this peer |
| `POST /_cache/set?key=…&ttl_ms=…` | Push a cache entry to this peer (write-through) |
| `GET /_cache/hot?limit=…` | Return top-N hot cache keys for read-ahead |
| `GET /_cache/has?key=…` | Check whether a key exists in this peer's cache |
| `GET /_cache/peers` | Return the current peer list |

## Observability

Observability details are maintained in [Observability Guide](observability.md), including:

- metrics and labels exposed on `/metrics`
- OTLP push configuration
- structured request log shape
- recommended dashboards and alert signals