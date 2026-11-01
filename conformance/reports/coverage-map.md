---
sidebar_label: Coverage Map
description: Which Loki endpoints the proxy implements, how each one is served by VictoriaLogs, which clients depend on it, and what evidence proves it.
---

# Coverage Map

Generated from the conformance registry by `conformance/scripts/coverage_map.py`. Do not edit by hand.

Loki surface: **v3.7.7**, extracted from the Loki source.

The proxy serves Loki's read API; write endpoints such as push are not supported (ingest goes to VictoriaLogs directly). Optional deletion management is outside read API scores: its opt-in handler does not implement VictoriaLogs deletion. The alerting-rules read API remains in scope, including `/api/prom/rules*` for vmalert. The deprecated read aliases `/api/prom/label`, `/api/prom/label/{name}/values`, `/api/prom/query`, `/api/prom/series` and `/api/prom/tail` are unsupported and excluded from totals, gaps and compatibility scores (owner 2026-10-31); use the corresponding `/loki/api/v1` routes. No other write endpoint is planned.

## Read API scores

| Score | Value | Meaning |
|---|---|---|
| Implementation | 25/27 | endpoints the proxy routes |
| Differential evidence | 19/27 | endpoints with tests comparing the proxy against Loki |
| Proven | 12/27 | endpoints whose registry state is `proven` |
| VictoriaLogs native share | 6/25 | implemented endpoints answered natively |

Counts exclude optional deletion management as well as out-of-scope writes and deprecated aliases.
A route or a linked test is not a full compatibility claim.

## Read endpoints

| Endpoint | Methods | State | Served by | Consumers | Tests | vs Loki | Implemented in |
|---|---|---|---|---|---:|---:|---|
| `/api/prom/rules` | GET | partial | proxy-side | api | 2 | 0 | internal/proxy/alerting.go:95 |
| `/api/prom/rules/{namespace}` | GET | partial | proxy-side | api | 1 | 0 | internal/proxy/alerting.go:95 |
| `/api/prom/rules/{namespace}/{groupName}` | GET | partial | proxy-side | api | 4 | 0 | internal/proxy/alerting.go:95 |
| `/loki/api/v1/detected_field/{name}/values` | GET, POST | partial | VictoriaLogs native | drilldown, api | 21 | 4 | internal/proxy/label_handlers.go:783 |
| `/loki/api/v1/detected_fields` | GET, POST | proven | proxy-side | explore, drilldown, api | 40 | 13 | internal/proxy/label_handlers.go:711 |
| `/loki/api/v1/detected_labels` | GET, POST | proven | proxy-side | drilldown, api | 21 | 7 | internal/proxy/patterns.go:1367 |
| `/loki/api/v1/drilldown-limits` | GET | partial | proxy-side | api | 8 | 2 | internal/proxy/patterns.go:1256 |
| `/loki/api/v1/format_query` | GET, POST | proven | proxy-side | explore, api | 3 | 1 | internal/proxy/patterns.go:1246 |
| `/loki/api/v1/index/stats` | GET, POST | proven | VictoriaLogs native | explore, datasource, api | 20 | 7 | internal/proxy/label_handlers.go:546 |
| `/loki/api/v1/index/volume` | GET, POST | proven | proxy-side | drilldown, api | 29 | 10 | internal/proxy/volume.go:192 |
| `/loki/api/v1/index/volume_range` | GET, POST | proven | proxy-side | drilldown, api | 21 | 4 | internal/proxy/volume.go:200 |
| `/loki/api/v1/label` | GET, POST | proven | VictoriaLogs native | explore, drilldown, datasource, api | 3 | 1 | internal/proxy/label_handlers.go:24 |
| `/loki/api/v1/label/{name}/values` | GET, POST | partial | VictoriaLogs native | explore, drilldown, datasource, api | 50 | 15 | internal/proxy/label_handlers.go:124 |
| `/loki/api/v1/labels` | GET, POST | proven | VictoriaLogs native | explore, drilldown, datasource, api | 84 | 18 | internal/proxy/label_handlers.go:24 |
| `/loki/api/v1/patterns` | GET, POST | proven | hybrid | drilldown, api | 23 | 3 | internal/proxy/patterns.go:108 |
| `/loki/api/v1/query` | GET, POST | proven | hybrid | explore, drilldown, api | 104 | 40 | internal/proxy/proxy.go:2721 |
| `/loki/api/v1/query_range` | GET, POST | proven | hybrid | explore, drilldown, api | 217 | 64 | internal/proxy/proxy.go:2301 |
| `/loki/api/v1/rules` | GET | partial | proxy-side | api | 10 | 2 | internal/proxy/alerting.go:95 |
| `/loki/api/v1/rules/{namespace}` | GET | partial | proxy-side | api | 4 | 0 | internal/proxy/alerting.go:95 |
| `/loki/api/v1/rules/{namespace}/{groupName}` | GET | partial | proxy-side | api | 4 | 0 | internal/proxy/alerting.go:95 |
| `/loki/api/v1/series` | GET, POST | proven | hybrid | explore, datasource, api | 39 | 13 | internal/proxy/label_handlers.go:426 |
| `/loki/api/v1/status/buildinfo` | GET | partial | proxy-side | datasource, api | 8 | 4 | internal/proxy/alerting.go:128 |
| `/loki/api/v1/tail` | GET, POST | partial | VictoriaLogs native | explore, api | 14 | 4 | internal/proxy/tail.go:26 |
| `/prometheus/api/v1/alerts` | GET | partial | proxy-side | api | 5 | 0 | internal/proxy/alerting.go:111 |
| `/prometheus/api/v1/rules` | GET | partial | proxy-side | api | 5 | 1 | internal/proxy/alerting.go:95 |
| `/loki/api/v1/cache/generation_numbers` | GET | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/index/shards` | GET, POST | gap (optional) | not implemented | api | 0 | 0 | — |

Optional API gaps remain included in the totals and scores:

- `/loki/api/v1/index/shards`: Optional planner compatibility only: no proxy route or differential proof. This endpoint is not a blocker for normal pinned Grafana Explore, Logs Drilldown or peer cache lookup. It remains a declared read API gap and stays in totals and compatibility scores.

## Optional deletion management

Not included in the read API denominator. `/loki/api/v1/delete` is disabled
by default. Its opt-in handler has request safeguards but forwards to an
unsupported VictoriaLogs path; it does not implement VL's asynchronous deletion API.
See [Known Issues](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/docs/KNOWN_ISSUES.md).

## Proxy-side work

Where the proxy computes a result instead of passing a VictoriaLogs answer through.

| Endpoint | Proxy-side work |
|---|---|
| `/loki/api/v1/patterns` | ensureSyntheticServiceName |
| `/loki/api/v1/query` | binaryEvaluationContext, buildBoundedBareParserMetric, collectRangeMetricSamples, ensureDetectedLevel, ensureSyntheticServiceName, orderedJSONMetric |
| `/loki/api/v1/query_range` | binaryEvaluationContext, buildBoundedBareParserMetric, collectRangeMetricSamples, ensureDetectedLevel, ensureSyntheticServiceName, orderedJSONMetric |
| `/loki/api/v1/series` | ensureSyntheticServiceName |

## VictoriaLogs surface in use

- Endpoints: `/select/logsql/delete`, `/select/logsql/field_names`, `/select/logsql/field_values`, `/select/logsql/hits`, `/select/logsql/query`, `/select/logsql/stats_query`, `/select/logsql/stats_query_range`, `/select/logsql/stream_field_names`, `/select/logsql/stream_field_values`, `/select/logsql/streams`, `/select/logsql/tail`
- Pipes: `coalesce`, `drop`, `extract`, `field_names`, `field_values`, `filter`, `format`, `json_array_len`, `keep`, `len`, `limit`, `math`, `pack_json`, `replace`, `running`, `sort`, `stats by`, `stats count`, `top`, `uniq`, `unpack_json`, `unpack_logfmt`, `unroll`
- Stats functions: `avg`, `count()`, `count_uniq`, `histogram`, `max`, `min`, `quantile`, `rate`, `rate_sum`, `row_any`, `sum`, `sum_len`, `uniq_values`, `values`

| Version-gated capability | Since | Used in |
|---|---|---:|
| DensePatternWindowing | v1.50 | 0 |
| PipeCoalesce | v1.51 | 2 |
