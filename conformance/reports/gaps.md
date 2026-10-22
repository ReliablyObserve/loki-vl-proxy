# Open gaps

Generated from the registry by `conformance/scripts/gaps.py`. Do not edit by hand.

Priority = consumer weight (explore/drilldown 3, datasource 2, api 1), plus 3 when no test
compares the endpoint against Loki, plus 2 when the proxy computes the result itself.

| Priority | Endpoint | Consumers | Served by | Tests | vs Loki | Missing |
|---:|---|---|---|---:|---:|---|
| 12 | `/loki/api/v1/label/{name}/values` | explore, drilldown, datasource, api | native_vl | 0 | 0 | no test compares it against Loki |
| 9 | `/loki/api/v1/detected_fields` | explore, drilldown, api | proxy_side | 29 | 9 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 7 | `/loki/api/v1/detected_field/{name}/values` | drilldown, api | native_vl | 0 | 0 | no test compares it against Loki; no test wired to the registry |
| 6 | `/loki/api/v1/index/volume_range` | drilldown, api | proxy_side | 22 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume` | drilldown, api | proxy_side | 26 | 7 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/format_query` | explore, api | proxy_side | 5 | 1 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/detected_labels` | drilldown, api | proxy_side | 16 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 4 | `/prometheus/api/v1/alerts` | api | not_implemented | 4 | 0 | not implemented; no test compares it against Loki |
| 4 | `/loki/api/v1/rules/{namespace}/{groupName}` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki |
| 4 | `/loki/api/v1/rules/{namespace}` | api | not_implemented | 2 | 0 | not implemented; no test compares it against Loki |
| 4 | `/loki/api/v1/patterns` | drilldown, api | hybrid | 22 | 3 | no test wired to the registry |
| 4 | `/loki/api/v1/index/shards` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/delete` | api | native_vl | 5 | 0 | no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/cache/generation_numbers` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/tail` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/series` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/rules/{namespace}/{groupName}` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki |
| 4 | `/api/prom/rules/{namespace}` | api | not_implemented | 2 | 0 | not implemented; no test compares it against Loki |
| 4 | `/api/prom/rules` | api | not_implemented | 6 | 0 | not implemented; no test compares it against Loki |
| 4 | `/api/prom/query` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/push` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/label/{name}/values` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/label` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 3 | `/loki/api/v1/status/buildinfo` | datasource, api | not_implemented | 10 | 4 | not implemented; no test wired to the registry |
| 1 | `/prometheus/api/v1/rules` | api | not_implemented | 5 | 1 | not implemented |
| 1 | `/loki/api/v1/rules` | api | not_implemented | 10 | 2 | not implemented |
| 1 | `/loki/api/v1/push` | api | not_implemented | 33 | 29 | not implemented; no test wired to the registry |
| 1 | `/loki/api/v1/label` | api | not_implemented | 62 | 11 | not implemented; no test wired to the registry |
| 1 | `/loki/api/v1/drilldown-limits` | api | not_implemented | 9 | 1 | not implemented |

## Error surface

Every dynamic message is a risk: it can carry VictoriaLogs or proxy-internal text that Loki
would never emit. Each needs a registry entry stating Loki's own text.

| Status | errorType | proxy call sites | literal messages | dynamic |
|---|---|---:|---:|---:|
| 400 | bad_data | 64 | 20 | 34 |
| 401 | — | 3 | 2 | 0 |
| 403 | — | 2 | 2 | 0 |
| 404 | not_found | 1 | 1 | 0 |
| 413 | — | 1 | 1 | 0 |
| 500 | internal | 6 | 2 | 2 |
| 502 | unavailable | 11 | 6 | 4 |
| 503 | timeout | 5 | 1 | 3 |

## Behaviour tracks

Semantics, severity, identity and data-quality behaviour the proxy must reproduce.
`cases` counts the edge cases named in the item; `wired` counts tests declaring them.

| Track | Item | Cases named | Wired | State |
|---|---|---:|---:|---|
| limits | `backend-admission-and-heavy-query-queueing` — Heavy backend work is admitted, queued, then refused like Loki's scheduler | 15 | 7 | proven |
| resource_control | `backend-deadlines-and-cancellation` — Work the client has given up on stops in the backend too | 6 | 2 | proven |
| data_quality | `data-density-and-chart-quality` — Chart density, zero-fill and high-cardinality behaviour | 6 | 0 | gap |
| data_quality | `data-probing-and-freshness` — Probes the proxy runs, and their cost and staleness | 9 | 0 | gap |
| limits | `heavy-metric-fetch-bounds` — Metric evaluation reads bounded work from VictoriaLogs, or refuses early | 10 | 3 | partial |
| semantics | `loki-compatible-profile` — The Loki-compatible profile holds requests and responses to Loki's contract | 14 | 7 | partial |
| identity | `metric-series-identity` — A metric series is named by the stream, its metadata and its parsed labels | 6 | 13 | partial |
| semantics | `numeric-and-response-formatting` — Timestamps, number formatting and empty shapes | 5 | 0 | gap |
| limits | `operator-configurable-limits` — Every bound on work is an operator flag, documented from one source | 13 | 8 | proven |
| identity | `parsed-label-series-identity` — Labels a parser extracts are part of a metric series | 6 | 4 | partial |
| semantics | `parser-error-and-label-collision` — Parser errors, __error__ and _extracted collisions | 23 | 10 | partial |
| limits | `series-limits-and-partial-results` — Series limits: error, or partial result with a warning | 9 | 10 | partial |
| identity | `service-name-derivation` — service_name follows Loki's discovery order | 9 | 0 | partial |
| severity | `severity-detected-level-derivation` — detected_level is derived on the read path | 10 | 6 | partial |
| severity | `severity-exposure-surfaces` — Where detected_level and level must appear | 7 | 3 | partial |
| semantics | `tenant-label-isolation` — Tenant-label mode confines every request to its org | 4 | 4 | proven |
| limits | `tenant-query-limits` — Loki's per-tenant query limits, enforced as published | 13 | 3 | proven |
| semantics | `window-bounds-and-step-alignment` — Range windows, bucket edges and step alignment | 8 | 6 | partial |

## LogQL surface (v3.7.7)

64/72 constructs are referenced in proxy code. Not referenced anywhere:

- `--keep-empty` (operator, OpKeepEmpty)
- `__first_over_time_ts__` (range_function, OpRangeTypeFirstWithTimestamp)
- `__last_over_time_ts__` (range_function, OpRangeTypeLastWithTimestamp)
- `__quantile_sketch_over_time__` (range_function, OpRangeTypeQuantileSketch)
- `--strict` (operator, OpStrict)
- `approx_topk` (operator, OpTypeApproxTopK)
- `__count_min_sketch__` (operator, OpTypeCountMinSketch)
- `variants` (operator, OpVariants)

## VictoriaLogs reuse opportunities

VictoriaLogs already provides these, and the proxy does not emit them. Each is a
chance to move work off the proxy and onto the backend.

| LogQL | VictoriaLogs | Available since |
|---|---|---|
| `avg` | stats-avg | v1.40.0 |
| `avg_over_time` | stats-avg | v1.40.0 |
| `bottomk` | pipe-sort-topk | v1.40.0 |
| `count` | stats-count-uniq | v1.40.0 |
| `first_over_time` | stats-row-min | v1.40.0 |
| `label_replace` | pipe-replace-regexp | v1.40.0 |
| `last_over_time` | stats-row-max | v1.40.0 |
| `stddev` | stats-stddev | v1.49.0 |
| `stddev_over_time` | stats-stddev | v1.49.0 |
| `stdvar` | stats-stddev | v1.49.0 |
| `stdvar_over_time` | stats-stddev | v1.49.0 |
| `unpack` | pipe-unpack | v1.40.0 |

LogQL constructs the proxy supports with no VictoriaLogs equivalent (23): `!=`, `%`, `*`, `+`, `-`, `/`, `<`, `<=`, `==`, `>`, `>=`, `^`, `and`, `group_left`, `group_right`, `ignoring`, `offset`, `on`, `or`, `rate_counter`, `unless`, `vector`, `|`


## Slower than Loki on first load

Registry items whose measured shapes the proxy answers slower than Loki on a
cold request (bench/ab runs; details in [performance.md](performance.md)). Warm is the
p50 of the runs after the first, on the worst cold row; `also slower warm` counts the rows
slower warm too (— when no row comes from a run that can say). Rows Loki's results cache
answered (by its own counters or the identical-answer heuristic) are excluded.

| registry item | slower shape×range | worst proxy cold | Loki cold there | proxy warm there | Loki warm there | also slower warm |
|---|---|---|---|---|---|---|
| `parser-error-and-label-collision` | 13 | 22.48s (X1 json filter level_extracted, by level, 24h) | 4.70s | 2.63s | 0.04s | — |
| `semantics/extracted-suffix-collision` | 4 | 22.48s (X1 json filter level_extracted, by level, 24h) | 4.70s | 2.63s | 0.04s | — |
| `semantics/json-extracted-metric-pushdown` | 3 | 22.48s (X1 json filter level_extracted, by level, 24h) | 4.70s | 2.63s | 0.04s | — |
| `semantics/unwrap-conversion-error` | 7 | 13.13s (E3 logfmt latency_ms rate, production, 7d) | 0.01s | 13.44s | 0.00s | — |
| `semantics/json-metric-detected-level-derivation` | 8 | 11.46s (DLV2 json error filter, detected_level, 6h) | 1.31s | 11.51s | 0.04s | — |
| `severity-detected-level-derivation` | 8 | 11.46s (DLV2 json error filter, detected_level, 6h) | 1.31s | 11.51s | 0.04s | — |
| `explore-logs-volume-json` | 3 | 8.05s (A explore volume, 2 filters, drop, 24h) | 4.78s | 0.37s | 0.06s | — |
| `semantics/json-filter-pushdown-translated-label` | 3 | 8.05s (A explore volume, 2 filters, drop, 24h) | 4.78s | 0.37s | 0.06s | — |
| `semantics/json-filter-pushdown-underscore-label` | 6 | 8.05s (A explore volume, 2 filters, drop, 24h) | 4.78s | 0.37s | 0.06s | — |
| `parsed-label-series-identity` | 3 | 7.93s (F field breakdown service_version (explore), 24h) | 5.04s | 0.40s | 0.07s | — |
| `semantics/json-label-spelling-probe` | 3 | 7.93s (F field breakdown service_version (explore), 24h) | 5.04s | 0.40s | 0.07s | — |
| `loki-compatible-profile` | 16 | 0.51s (E7 json count by level_extracted, 1h) | 0.06s | 0.08s | 0.01s | — |
| `profiles/structured-metadata-label-collision` | 1 | 0.51s (E7 json count by level_extracted, 1h) | 0.06s | 0.08s | 0.01s | — |
| `profiles/stage-field-exposure` | 15 | 0.38s (SF5 logs, regexp capture, 6h) | 0.04s | 0.15s | 0.04s | — |
| `metric-series-identity` | 1 | 0.66s (W3 sum without pod, rate, 24h) | 0.41s | 0.68s | 0.01s | — |
| `semantics/unwrap-sum-without-collapses` | 1 | 0.66s (W3 sum without pod, rate, 24h) | 0.41s | 0.68s | 0.01s | — |
| `semantics/pattern-line-filter-anchoring` | 3 | 0.28s (A7 nginx exclude ends on a literal, log query, 24h) | 0.04s | 0.02s | 0.02s | — |
| `semantics/line-filter-or-alternatives` | 2 | 0.22s (OR2 logs, negated or alternatives, 6h) | 0.10s | 0.08s | 0.07s | — |
| `semantics/unwrap-duration-seconds` | 1 | 0.34s (D1 logfmt ttl duration_seconds rate by service, 24h) | 0.22s | 0.34s | 0.01s | — |
| `backend-admission-and-heavy-query-queueing` | 1 | 0.12s (drilldown landing volume, 7d) | 0.03s | 0.10s | 0.00s | — |
| `parser-logfmt` | 1 | 0.14s (logs | logfmt, 1h) | 0.08s | 0.14s | 0.08s | — |
