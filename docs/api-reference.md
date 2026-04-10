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

- Query results use canonical Loki 2-tuples `[timestamp, line]` by default.
- When `X-Loki-Response-Encoding-Flags: categorize-labels` is present and `-emit-structured-metadata=true`, query results emit Loki 3-tuples `[timestamp, line, metadata]`.
- The 3rd tuple object follows Loki keys only: `structuredMetadata` and/or `parsed` (no snake_case alias keys).
- `structuredMetadata` and `parsed` are Loki-style label-pair arrays (`[{name,value}, ...]`), not free-form maps.
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
| `GET /config` | YAML stub | Configuration endpoint |
| `GET /loki/api/v1/drilldown-limits` | Bootstrap/capability endpoint for Grafana Logs Drilldown | Datasource capability probing |

Behavior and scope notes for these endpoints live in:
- [Configuration](configuration.md) for flags, tenant fanout behavior, and backend mapping
- [Known Issues](KNOWN_ISSUES.md) for intentional compatibility boundaries

Write-surface boundary for rules and alerts:

- These routes expose read compatibility only (Loki YAML views and Prometheus-style JSON views).
- Rule and alert write/lifecycle operations remain on [`vmalert`](https://docs.victoriametrics.com/vmalert/) and VictoriaLogs backend systems ([VictoriaLogs docs](https://docs.victoriametrics.com/victorialogs/)).
- The proxy does not implement Loki ruler write APIs.

## Infrastructure Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /ready` | Readiness probe (checks VL `/health` + circuit breaker) |
| `GET /loki/api/v1/status/buildinfo` | Fake Loki 2.9.0 build info for Grafana detection |
| `GET /metrics` | Prometheus text exposition (`-server.register-instrumentation`) |
| `GET /debug/queries` | Query analytics, disabled by default (`-server.enable-query-analytics`) |
| `GET /debug/pprof/` | Go profiling, disabled by default (`-server.enable-pprof`) |

## Observability

Observability details are maintained in [Observability Guide](observability.md), including:

- metrics and labels exposed on `/metrics`
- OTLP push configuration
- structured request log shape
- recommended dashboards and alert signals
