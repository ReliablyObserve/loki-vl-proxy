# Open gaps

Generated from the registry by `conformance/scripts/gaps.py`. Do not edit by hand.

Priority = consumer weight (explore/drilldown 3, datasource 2, api 1), plus 3 when no test
compares the endpoint against Loki, plus 2 when the proxy computes the result itself.

| Priority | Endpoint | Consumers | Served by | Tests | vs Loki | Missing |
|---:|---|---|---|---:|---:|---|
| 12 | `/loki/api/v1/label/{name}/values` | explore, drilldown, datasource, api | native_vl | 0 | 0 | no test compares it against Loki |
| 9 | `/loki/api/v1/detected_fields` | explore, drilldown, api | proxy_side | 29 | 9 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 7 | `/loki/api/v1/query` | explore, drilldown, api | hybrid | 131 | 34 | no test wired to the registry |
| 7 | `/loki/api/v1/detected_field/{name}/values` | drilldown, api | native_vl | 0 | 0 | no test compares it against Loki; no test wired to the registry |
| 6 | `/loki/api/v1/series` | explore, datasource, api | hybrid | 29 | 8 | no test wired to the registry |
| 6 | `/loki/api/v1/index/volume_range` | drilldown, api | proxy_side | 22 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume` | drilldown, api | proxy_side | 26 | 7 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/stats` | explore, datasource, api | native_vl | 19 | 5 | no test wired to the registry |
| 6 | `/loki/api/v1/format_query` | explore, api | proxy_side | 5 | 1 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/detected_labels` | drilldown, api | proxy_side | 16 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 4 | `/prometheus/api/v1/alerts` | api | not_implemented | 4 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/tail` | explore, api | native_vl | 14 | 3 | no test wired to the registry |
| 4 | `/loki/api/v1/rules/{namespace}/{groupName}` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/rules/{namespace}` | api | not_implemented | 2 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/patterns` | drilldown, api | hybrid | 22 | 3 | no test wired to the registry |
| 4 | `/loki/api/v1/index/shards` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/delete` | api | native_vl | 5 | 0 | no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/cache/generation_numbers` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/tail` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/series` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/rules/{namespace}/{groupName}` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/rules/{namespace}` | api | not_implemented | 2 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/rules` | api | not_implemented | 6 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/query` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/push` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/label/{name}/values` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/label` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 3 | `/loki/api/v1/status/buildinfo` | datasource, api | not_implemented | 10 | 4 | not implemented; no test wired to the registry |
| 1 | `/prometheus/api/v1/rules` | api | not_implemented | 5 | 1 | not implemented; no test wired to the registry |
| 1 | `/loki/api/v1/rules` | api | not_implemented | 10 | 2 | not implemented; no test wired to the registry |
| 1 | `/loki/api/v1/push` | api | not_implemented | 33 | 29 | not implemented; no test wired to the registry |
| 1 | `/loki/api/v1/label` | api | not_implemented | 62 | 11 | not implemented; no test wired to the registry |
| 1 | `/loki/api/v1/drilldown-limits` | api | not_implemented | 9 | 1 | not implemented; no test wired to the registry |

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
| limits | `backend-admission-and-heavy-query-queueing` — Heavy backend work is admitted, queued, then refused like Loki's scheduler | 10 | 2 | proven |
| resource_control | `backend-deadlines-and-cancellation` — Work the client has given up on stops in the backend too | 4 | 1 | proven |
| data_quality | `data-density-and-chart-quality` — Chart density, zero-fill and high-cardinality behaviour | 6 | 0 | gap |
| data_quality | `data-probing-and-freshness` — Probes the proxy runs, and their cost and staleness | 4 | 0 | gap |
| limits | `heavy-metric-fetch-bounds` — Metric evaluation reads bounded work from VictoriaLogs, or refuses early | 9 | 3 | partial |
| semantics | `numeric-and-response-formatting` — Timestamps, number formatting and empty shapes | 5 | 0 | gap |
| limits | `operator-configurable-limits` — Every bound on work is an operator flag, documented from one source | 11 | 4 | proven |
| semantics | `parser-error-and-label-collision` — Parser errors, __error__ and _extracted collisions | 7 | 0 | gap |
| limits | `series-limits-and-partial-results` — Series limits: error, or partial result with a warning | 7 | 3 | partial |
| identity | `service-name-derivation` — service_name follows Loki's discovery order | 8 | 0 | partial |
| severity | `severity-detected-level-derivation` — detected_level is derived on the read path | 8 | 4 | partial |
| severity | `severity-exposure-surfaces` — Where detected_level and level must appear | 6 | 3 | partial |
| semantics | `window-bounds-and-step-alignment` — Range windows, bucket edges and step alignment | 7 | 1 | gap |

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

