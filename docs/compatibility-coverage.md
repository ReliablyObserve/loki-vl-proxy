---
sidebar_label: Coverage Map
description: Which Loki endpoints the proxy implements, how each one is served by VictoriaLogs, which clients depend on it, and what evidence proves it.
---

# Coverage Map

Generated from the conformance registry by `conformance/scripts/coverage_map.py`. Do not edit by hand.

Loki surface: **v3.7.7**, extracted from the Loki source.

## Scores

| Score | Value | Meaning |
|---|---|---|
| Implementation | 15/35 | endpoints the proxy routes |
| Differential evidence | 18/35 | endpoints with tests comparing the proxy against Loki |
| Proven | 12/35 | endpoints whose registry state is `proven` |
| VictoriaLogs native share | 6/15 | implemented endpoints answered natively |

## Endpoints

| Endpoint | State | Served by | Consumers | Tests | vs Loki | Implemented in |
|---|---|---|---|---:|---:|---|
| `/loki/api/v1/delete` | partial | VictoriaLogs native | api | 5 | 0 | internal/proxy/patterns.go:1434 |
| `/loki/api/v1/detected_field/{name}/values` | partial | VictoriaLogs native | drilldown, api | 0 | 0 | internal/proxy/label_handlers.go:633 |
| `/loki/api/v1/detected_fields` | proven | proxy-side | explore, drilldown, api | 29 | 9 | internal/proxy/label_handlers.go:560 |
| `/loki/api/v1/detected_labels` | proven | proxy-side | drilldown, api | 16 | 4 | internal/proxy/patterns.go:1370 |
| `/loki/api/v1/format_query` | proven | proxy-side | explore, api | 5 | 1 | internal/proxy/patterns.go:1241 |
| `/loki/api/v1/index/stats` | proven | VictoriaLogs native | explore, datasource, api | 19 | 5 | internal/proxy/label_handlers.go:404 |
| `/loki/api/v1/index/volume` | proven | proxy-side | drilldown, api | 26 | 7 | internal/proxy/volume.go:190 |
| `/loki/api/v1/index/volume_range` | proven | proxy-side | drilldown, api | 22 | 4 | internal/proxy/volume.go:198 |
| `/loki/api/v1/label/{name}/values` | partial | VictoriaLogs native | explore, drilldown, datasource, api | 0 | 0 | internal/proxy/label_handlers.go:97 |
| `/loki/api/v1/labels` | proven | VictoriaLogs native | explore, drilldown, datasource, api | 57 | 9 | internal/proxy/label_handlers.go:22 |
| `/loki/api/v1/patterns` | proven | hybrid | drilldown, api | 22 | 3 | internal/proxy/patterns.go:107 |
| `/loki/api/v1/query` | proven | hybrid | explore, drilldown, api | 131 | 34 | internal/proxy/proxy.go:2329 |
| `/loki/api/v1/query_range` | proven | hybrid | explore, drilldown, api | 120 | 30 | internal/proxy/proxy.go:2034 |
| `/loki/api/v1/series` | proven | hybrid | explore, datasource, api | 29 | 8 | internal/proxy/label_handlers.go:283 |
| `/loki/api/v1/tail` | proven | VictoriaLogs native | explore, api | 14 | 3 | internal/proxy/tail.go:21 |
| `/api/prom/label` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/label/{name}/values` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/push` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/query` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/rules` | gap | not implemented | api | 6 | 0 | — |
| `/api/prom/rules/{namespace}` | gap | not implemented | api | 2 | 0 | — |
| `/api/prom/rules/{namespace}/{groupName}` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/series` | gap | not implemented | api | 0 | 0 | — |
| `/api/prom/tail` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/cache/generation_numbers` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/drilldown-limits` | gap | not implemented | api | 9 | 1 | — |
| `/loki/api/v1/index/shards` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/label` | gap | not implemented | api | 62 | 11 | — |
| `/loki/api/v1/push` | gap | not implemented | api | 33 | 29 | — |
| `/loki/api/v1/rules` | gap | not implemented | api | 10 | 2 | — |
| `/loki/api/v1/rules/{namespace}` | gap | not implemented | api | 2 | 0 | — |
| `/loki/api/v1/rules/{namespace}/{groupName}` | gap | not implemented | api | 0 | 0 | — |
| `/loki/api/v1/status/buildinfo` | gap | not implemented | datasource, api | 10 | 4 | — |
| `/prometheus/api/v1/alerts` | gap | not implemented | api | 4 | 0 | — |
| `/prometheus/api/v1/rules` | gap | not implemented | api | 5 | 1 | — |

## Proxy-side work

Where the proxy computes a result instead of passing a VictoriaLogs answer through.

| Endpoint | Proxy-side work |
|---|---|
| `/loki/api/v1/patterns` | ensureDetectedLevel, ensureSyntheticServiceName |
| `/loki/api/v1/query` | binaryEvaluationContext, buildBoundedBareParserMetric, collectRangeMetricSamples, ensureSyntheticServiceName, orderedJSONMetric |
| `/loki/api/v1/query_range` | buildBoundedBareParserMetric, collectRangeMetricSamples, ensureDetectedLevel, ensureSyntheticServiceName, orderedJSONMetric, zerofillStatsMatrix |
| `/loki/api/v1/series` | ensureSyntheticServiceName |

## VictoriaLogs surface in use

- Endpoints: `/select/logsql/delete`, `/select/logsql/field_names`, `/select/logsql/field_values`, `/select/logsql/hits`, `/select/logsql/query`, `/select/logsql/stats_query`, `/select/logsql/stats_query_range`, `/select/logsql/stream_field_names`, `/select/logsql/stream_field_values`, `/select/logsql/streams`, `/select/logsql/tail`
- Pipes: `coalesce`, `drop`, `extract`, `field_names`, `field_values`, `filter`, `format`, `json_array_len`, `keep`, `len`, `limit`, `math`, `pack_json`, `replace`, `running`, `sort`, `stats by`, `top`, `uniq`, `unpack_json`, `unpack_logfmt`, `unroll`
- Stats functions: `avg`, `count()`, `count_uniq`, `histogram`, `max`, `min`, `quantile`, `rate`, `rate_sum`, `row_any`, `sum`, `sum_len`, `uniq_values`, `values`

| Version-gated capability | Since | Used in |
|---|---|---:|
| DensePatternWindowing | v1.50 | 0 |
| FieldIPv4Range | v1.45 | 1 |
| MetadataSubstring | v1.49 | 0 |
| StatsRateSum | v1.44 | 0 |
