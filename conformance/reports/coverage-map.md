---
sidebar_label: Coverage Map
description: Which Loki endpoints the proxy implements, how each one is served by VictoriaLogs, which clients depend on it, and what evidence proves it.
---

# Coverage Map

Generated from the conformance registry by `conformance/scripts/coverage_map.py`. Do not edit by hand.

Loki surface: **v3.7.7**, extracted from the Loki source.

The proxy serves Loki's read API. Write endpoints (push) are not supported: ingest goes to VictoriaLogs directly.

## Scores

| Score | Value | Meaning |
|---|---|---|
| Implementation | 23/33 | endpoints the proxy routes |
| Differential evidence | 18/33 | endpoints with tests comparing the proxy against Loki |
| Proven | 12/33 | endpoints whose registry state is `proven` |
| VictoriaLogs native share | 6/23 | implemented endpoints answered natively |

## Endpoints

| Endpoint | State | Served by | Consumers | Tests | vs Loki | Implemented in |
|---|---|---|---|---:|---:|---|
| `/api/prom/rules` | partial | proxy-side | api | 2 | 0 | internal/proxy/alerting.go:94 |
| `/api/prom/rules/{namespace}` | partial | proxy-side | api | 1 | 0 | internal/proxy/alerting.go:94 |
| `/loki/api/v1/delete` | partial | VictoriaLogs native | api | 3 | 0 | internal/proxy/patterns.go:1431 |
| `/loki/api/v1/detected_field/{name}/values` | partial | VictoriaLogs native | drilldown, api | 18 | 4 | internal/proxy/label_handlers.go:764 |
| `/loki/api/v1/detected_fields` | proven | proxy-side | explore, drilldown, api | 37 | 13 | internal/proxy/label_handlers.go:691 |
| `/loki/api/v1/detected_labels` | proven | proxy-side | drilldown, api | 17 | 6 | internal/proxy/patterns.go:1367 |
| `/loki/api/v1/drilldown-limits` | partial | proxy-side | api | 8 | 2 | internal/proxy/patterns.go:1256 |
| `/loki/api/v1/format_query` | proven | proxy-side | explore, api | 3 | 1 | internal/proxy/patterns.go:1246 |
| `/loki/api/v1/index/stats` | proven | VictoriaLogs native | explore, datasource, api | 18 | 7 | internal/proxy/label_handlers.go:525 |
| `/loki/api/v1/index/volume` | proven | proxy-side | drilldown, api | 26 | 9 | internal/proxy/volume.go:192 |
| `/loki/api/v1/index/volume_range` | proven | proxy-side | drilldown, api | 19 | 4 | internal/proxy/volume.go:200 |
| `/loki/api/v1/label/{name}/values` | partial | VictoriaLogs native | explore, drilldown, datasource, api | 45 | 14 | internal/proxy/label_handlers.go:102 |
| `/loki/api/v1/labels` | proven | VictoriaLogs native | explore, drilldown, datasource, api | 72 | 16 | internal/proxy/label_handlers.go:24 |
| `/loki/api/v1/patterns` | proven | hybrid | drilldown, api | 20 | 3 | internal/proxy/patterns.go:108 |
| `/loki/api/v1/query` | proven | hybrid | explore, drilldown, api | 72 | 27 | internal/proxy/proxy.go:2532 |
| `/loki/api/v1/query_range` | proven | hybrid | explore, drilldown, api | 169 | 50 | internal/proxy/proxy.go:2172 |
| `/loki/api/v1/rules` | partial | proxy-side | api | 9 | 2 | internal/proxy/alerting.go:94 |
| `/loki/api/v1/rules/{namespace}` | partial | proxy-side | api | 4 | 0 | internal/proxy/alerting.go:94 |
| `/loki/api/v1/series` | proven | hybrid | explore, datasource, api | 35 | 12 | internal/proxy/label_handlers.go:404 |
| `/loki/api/v1/status/buildinfo` | partial | proxy-side | datasource, api | 8 | 4 | internal/proxy/alerting.go:127 |
| `/loki/api/v1/tail` | proven | VictoriaLogs native | explore, api | 13 | 4 | internal/proxy/tail.go:26 |
| `/prometheus/api/v1/alerts` | partial | proxy-side | api | 4 | 0 | internal/proxy/alerting.go:110 |
| `/prometheus/api/v1/rules` | partial | proxy-side | api | 5 | 1 | internal/proxy/alerting.go:94 |
| `/api/prom/label` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/label/{name}/values` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/query` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/rules/{namespace}/{groupName}` | gap | not implemented | api | 4 | 0 | — |
| `/api/prom/series` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/tail` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/cache/generation_numbers` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/index/shards` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/label` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/rules/{namespace}/{groupName}` | gap | not implemented | api | 4 | 0 | — |

## Proxy-side work

Where the proxy computes a result instead of passing a VictoriaLogs answer through.

| Endpoint | Proxy-side work |
|---|---|
| `/loki/api/v1/patterns` | ensureSyntheticServiceName |
| `/loki/api/v1/query` | binaryEvaluationContext, buildBoundedBareParserMetric, collectRangeMetricSamples, ensureSyntheticServiceName, orderedJSONMetric |
| `/loki/api/v1/query_range` | binaryEvaluationContext, buildBoundedBareParserMetric, collectRangeMetricSamples, ensureSyntheticServiceName, orderedJSONMetric |
| `/loki/api/v1/series` | ensureSyntheticServiceName |

## VictoriaLogs surface in use

- Endpoints: `/select/logsql/delete`, `/select/logsql/field_names`, `/select/logsql/field_values`, `/select/logsql/hits`, `/select/logsql/query`, `/select/logsql/stats_query`, `/select/logsql/stats_query_range`, `/select/logsql/stream_field_names`, `/select/logsql/stream_field_values`, `/select/logsql/streams`, `/select/logsql/tail`
- Pipes: `coalesce`, `drop`, `extract`, `field_names`, `field_values`, `filter`, `format`, `json_array_len`, `keep`, `len`, `limit`, `math`, `pack_json`, `replace`, `running`, `sort`, `stats by`, `stats count`, `top`, `uniq`, `unpack_json`, `unpack_logfmt`, `unroll`
- Stats functions: `avg`, `count()`, `count_uniq`, `histogram`, `max`, `min`, `quantile`, `rate`, `rate_sum`, `row_any`, `sum`, `sum_len`, `uniq_values`, `values`

| Version-gated capability | Since | Used in |
|---|---|---:|
| DensePatternWindowing | v1.50 | 0 |
| PipeCoalesce | v1.51 | 2 |
