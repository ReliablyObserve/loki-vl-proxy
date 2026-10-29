# Open gaps

Generated from the registry by `conformance/scripts/gaps.py`. Do not edit by hand.

Priority = consumer weight (explore/drilldown 3, datasource 2, api 1), plus 3 when no test
compares the endpoint against Loki, plus 2 when the proxy computes the result itself.
An endpoint the owner named a priority for is placed by that first: `first` above all
others (and always listed), `last` below all others; the score orders within each group.

The proxy serves Loki's read API; write endpoints such as push are not supported (ingest goes to VictoriaLogs directly); delete may be supported, and the alerting-rules API may come last; no other write endpoint is planned.

| Priority | Endpoint | Consumers | Served by | Tests | vs Loki | Missing |
|---:|---|---|---|---:|---:|---|
| 4 | `/loki/api/v1/tail` | explore, api | native_vl | 13 | 4 | owner priority: Routed (handleTail) and compared against Loki in 4 test files, but not yet Loki-identical. Missing: start and limit history replay (Loki sends the last limit entries since start before live lines, pkg/loghttp/tail.go:70-94; open case semantics/tail-history-start-limit); limit and delay_for validation (limit must be positive, delay_for above 5 is refused, tail.go:101-103 and params.go:27-36); a multi-tenant tail request, which the proxy refuses (docs/KNOWN_ISSUES.md) with no registry row yet pinning Loki's answer. |
| 4 | `/api/prom/tail` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry; owner priority: Not routed: the proxy answers 404. Loki serves /api/prom/tail with the same TailHandler as /loki/api/v1/tail (pkg/loki/modules.go:718-719), so it needs the same behaviour and the same frames, and shares the missing items of loki_api_v1_tail (history replay for start and limit, limit and delay_for validation, multi-tenant answer). |
| 9 | `/loki/api/v1/detected_fields` | explore, drilldown, api | proxy_side | 37 | 13 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume_range` | drilldown, api | proxy_side | 19 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume` | drilldown, api | proxy_side | 26 | 9 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/format_query` | explore, api | proxy_side | 3 | 1 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/detected_labels` | drilldown, api | proxy_side | 17 | 6 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 5 | `/loki/api/v1/status/buildinfo` | datasource, api | proxy_side | 8 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 4 | `/loki/api/v1/patterns` | drilldown, api | hybrid | 20 | 3 | no test wired to the registry |
| 4 | `/loki/api/v1/label` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/index/shards` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/loki/api/v1/delete` | api | native_vl | 3 | 0 | no test compares it against Loki |
| 4 | `/loki/api/v1/cache/generation_numbers` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/series` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/query` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/label/{name}/values` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 4 | `/api/prom/label` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 3 | `/loki/api/v1/drilldown-limits` | api | proxy_side | 8 | 2 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/prometheus/api/v1/alerts` | api | proxy_side | 4 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/rules/{namespace}` | api | proxy_side | 4 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/api/prom/rules/{namespace}` | api | proxy_side | 1 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/api/prom/rules` | api | proxy_side | 2 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 4 | `/loki/api/v1/rules/{namespace}/{groupName}` | api | not_implemented | 4 | 0 | not implemented; no test compares it against Loki |
| 4 | `/api/prom/rules/{namespace}/{groupName}` | api | not_implemented | 4 | 0 | not implemented; no test compares it against Loki |
| 3 | `/prometheus/api/v1/rules` | api | proxy_side | 5 | 1 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 3 | `/loki/api/v1/rules` | api | proxy_side | 9 | 2 | proxy-side: justify in the registry or push down to VictoriaLogs |

## Error surface

Every dynamic message is a risk: it can carry VictoriaLogs or proxy-internal text that Loki
would never emit. Each needs a registry entry stating Loki's own text.

| Status | errorType | proxy call sites | literal messages | dynamic |
|---|---|---:|---:|---:|
| 400 | bad_data | 86 | 25 | 65 |
| 401 | bad_data | 3 | 2 | 0 |
| 403 | bad_data | 2 | 2 | 0 |
| 404 | not_found | 1 | 1 | 0 |
| 408 | bad_data | 2 | 0 | 2 |
| 413 | bad_data | 1 | 1 | 0 |
| 429 | bad_data | 1 | 0 | 1 |
| 500 | internal | 6 | 2 | 6 |
| 502 | unavailable | 13 | 8 | 10 |
| 503 | timeout | 1 | 1 | 0 |
| set at run time | — | 69 | 4 | 67 |

## Behaviour tracks

Semantics, severity, identity and data-quality behaviour the proxy must reproduce.
`cases` counts the edge cases named in the item; `wired` counts tests declaring them.

| Track | Item | Cases named | Wired | State |
|---|---|---:|---:|---|
| limits | `backend-admission-and-heavy-query-queueing` — Heavy backend work is admitted, queued, then refused like Loki's scheduler | 15 | 8 | proven |
| resource_control | `backend-deadlines-and-cancellation` — Work the client has given up on stops in the backend too | 8 | 5 | proven |
| data_quality | `data-density-and-chart-quality` — Chart density, zero-fill and high-cardinality behaviour | 6 | 0 | gap |
| data_quality | `data-probing-and-freshness` — Probes the proxy runs, and their cost and staleness | 11 | 0 | gap |
| limits | `heavy-metric-fetch-bounds` — Metric evaluation reads bounded work from VictoriaLogs, or refuses early | 10 | 3 | partial |
| semantics | `loki-compatible-profile` — The Loki-compatible profile holds requests and responses to Loki's contract | 14 | 7 | partial |
| identity | `metric-series-identity` — A metric series is named by the stream, its metadata and its parsed labels | 6 | 29 | partial |
| semantics | `numeric-and-response-formatting` — Timestamps, number formatting and empty shapes | 5 | 1 | gap |
| limits | `operator-configurable-limits` — Every bound on work is an operator flag, documented from one source | 13 | 8 | proven |
| identity | `parsed-label-series-identity` — Labels a parser extracts are part of a metric series | 6 | 4 | partial |
| semantics | `parser-error-and-label-collision` — Parser errors, __error__ and _extracted collisions | 23 | 10 | partial |
| resource_control | `peer-cache-fleet-recovery` — With a ring owner down or every cache lost, the fleet answers like Loki from VictoriaLogs | 2 | 1 | partial |
| resource_control | `peer-cache-label-values-index` — A replica that lost its disk restores the label-values index from its peers | 1 | 1 | partial |
| resource_control | `peer-cache-metadata` — Label and detected-field answers are read from the ring owner and the disk tier | 4 | 1 | partial |
| resource_control | `peer-cache-patterns` — Patterns mined on one replica are served by every replica and restored on a replica that lost its disk | 3 | 1 | partial |
| resource_control | `peer-cache-query-range-windows` — Log query windows computed by their ring owner are read by the other replicas | 2 | 1 | partial |
| resource_control | `peer-cache-replication` — Every peer-cache key has a backup replica, so losing one replica's disk or the owner costs no recompute | 3 | 4 | partial |
| resource_control | `peer-cache-stats-extent` — Stats extent buckets read on one replica are served by every replica without VictoriaLogs | 3 | 1 | partial |
| limits | `series-limits-and-partial-results` — Series limits: error, or partial result with a warning | 9 | 11 | partial |
| identity | `service-name-derivation` — service_name follows Loki's discovery order | 9 | 0 | partial |
| severity | `severity-detected-level-derivation` — detected_level is derived on the read path | 10 | 6 | partial |
| severity | `severity-exposure-surfaces` — Where detected_level and level must appear | 7 | 3 | partial |
| resource_control | `shutdown-releases-background-work` — Shutting a proxy down stops every goroutine it started | 2 | 2 | unknown |
| semantics | `tenant-label-isolation` — Tenant-label mode confines every request to its org | 4 | 4 | proven |
| semantics | `tenant-numeric-routing` — A numeric org reaches VictoriaLogs only in its canonical spelling, and strict routing serves only mapped orgs | 4 | 9 | proven |
| limits | `tenant-query-limits` — Loki's per-tenant query limits, enforced as published | 13 | 3 | proven |
| semantics | `window-bounds-and-step-alignment` — Range windows, bucket edges and step alignment | 8 | 15 | partial |

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
| `parser-error-and-label-collision` | 13 | 13.13s (E3 logfmt latency_ms rate, production, 7d) | 0.01s | 13.44s | 0.00s | — |
| `semantics/unwrap-conversion-error` | 8 | 13.13s (E3 logfmt latency_ms rate, production, 7d) | 0.01s | 13.44s | 0.00s | — |
| `semantics/extracted-suffix-collision` | 5 | 10.62s (X1 json filter level_extracted, by level, 24h) | 3.48s | 0.05s | 0.05s | — |
| `semantics/json-extracted-metric-pushdown` | 4 | 10.62s (X1 json filter level_extracted, by level, 24h) | 3.48s | 0.05s | 0.05s | — |
| `semantics/json-metric-detected-level-derivation` | 10 | 6.78s (DLV2 json error filter, detected_level, 6h) | 0.88s | 6.64s | 0.03s | — |
| `severity-detected-level-derivation` | 25 | 6.78s (DLV2 json error filter, detected_level, 6h) | 0.88s | 6.64s | 0.03s | — |
| `explore-logs-volume-json` | 3 | 4.33s (A explore volume, 2 filters, drop, 24h) | 2.61s | 0.02s | 0.09s | — |
| `semantics/json-filter-pushdown-translated-label` | 3 | 4.33s (A explore volume, 2 filters, drop, 24h) | 2.61s | 0.02s | 0.09s | — |
| `semantics/json-filter-pushdown-underscore-label` | 4 | 4.33s (A explore volume, 2 filters, drop, 24h) | 2.61s | 0.02s | 0.09s | — |
| `severity/detected-level-filter-json-level` | 15 | 0.66s (LF4 logs, detected_level regexp with unknown, 6h) | 0.17s | 0.12s | 0.16s | — |
| `drilldown-service-landing` | 3 | 0.63s (LF12 drilldown logs volume by level, level pill, 24h) | 0.23s | 0.01s | 0.01s | — |
| `loki-compatible-profile` | 4 | 0.26s (E7 json count by level_extracted, 1h) | 0.05s | 0.04s | 0.01s | — |
| `profiles/structured-metadata-label-collision` | 1 | 0.26s (E7 json count by level_extracted, 1h) | 0.05s | 0.04s | 0.01s | — |
| `semantics/pattern-line-filter-anchoring` | 2 | 0.16s (A1 nginx include GET, log query, 24h) | 0.03s | 0.01s | 0.02s | — |
| `parser-json` | 2 | 0.38s (LF15 drilldown field breakdown, level filter, 24h) | 0.24s | 0.01s | 0.01s | — |
| `semantics/detected-level-filter-before-parser-pushdown` | 2 | 0.38s (LF15 drilldown field breakdown, level filter, 24h) | 0.24s | 0.01s | 0.01s | — |
| `operator-drop` | 2 | 0.11s (LF29 logs, drop detected_level, 6h) | 0.01s | 0.03s | 0.01s | — |
| `severity-exposure-surfaces` | 3 | 0.11s (LF29 logs, drop detected_level, 6h) | 0.01s | 0.03s | 0.01s | — |
| `severity/detected-level-stage-side-effects` | 2 | 0.11s (LF29 logs, drop detected_level, 6h) | 0.01s | 0.03s | 0.01s | — |
| `backend-admission-and-heavy-query-queueing` | 1 | 0.12s (drilldown landing volume, 7d) | 0.03s | 0.10s | 0.00s | — |
| `metric-series-identity` | 2 | 0.21s (G8 bytes() of a number, 24h) | 0.12s | 0.01s | 0.01s | — |
| `semantics/unwrap-gate-parsefloat-divergences` | 2 | 0.21s (G8 bytes() of a number, 24h) | 0.12s | 0.01s | 0.01s | — |
| `profiles/stage-field-exposure` | 3 | 0.14s (SF5 logs, regexp capture, 3h) | 0.06s | 0.05s | 0.06s | — |
| `parsed-label-series-identity` | 1 | 0.25s (F field breakdown service_version (explore), 1h) | 0.17s | 0.02s | 0.09s | — |
| `semantics/json-label-spelling-probe` | 1 | 0.25s (F field breakdown service_version (explore), 1h) | 0.17s | 0.02s | 0.09s | — |
| `semantics/unwrap-duration-seconds` | 1 | 0.22s (D1 logfmt ttl duration_seconds rate by service, 24h) | 0.14s | 0.01s | 0.01s | — |
