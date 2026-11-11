# Open gaps

Generated from the registry by `conformance/scripts/gaps.py`. Do not edit by hand.

Priority = consumer weight (explore/drilldown 3, datasource 2, api 1), plus 3 when no test
compares the endpoint against Loki, plus 2 when the proxy computes the result itself.
An endpoint the owner named a priority for is placed by that first: `first` above all
others (and always listed), `last` after normal gaps, `optional` after those; the score orders within each group.
Optional gaps remain in the declared API inventory and compatibility scores.

The proxy serves Loki's read API; write endpoints such as push are not supported (ingest goes to VictoriaLogs directly). Optional deletion management is outside read API scores: its opt-in handler does not implement VictoriaLogs deletion. The alerting-rules read API remains in scope, including `/api/prom/rules*` for vmalert. The deprecated read aliases `/api/prom/label`, `/api/prom/label/{name}/values`, `/api/prom/query`, `/api/prom/series` and `/api/prom/tail` are unsupported and excluded from totals, gaps and compatibility scores (owner 2026-10-31); use the corresponding `/loki/api/v1` routes. No other write endpoint is planned.

| Priority | Endpoint | Consumers | Served by | Tests | vs Loki | Missing |
|---:|---|---|---|---:|---:|---|
| 4 | `/loki/api/v1/tail` | explore, api | native_vl | 14 | 4 | owner priority: Routed (handleTail) and compared against Loki in 4 test files, but not yet Loki-identical. Missing: start and limit history replay (Loki sends the last limit entries since start before live lines, pkg/loghttp/tail.go:70-94; open case semantics/tail-history-start-limit); limit and delay_for validation (limit must be positive, delay_for above 5 is refused, tail.go:101-103 and params.go:27-36); a multi-tenant tail request, which the proxy refuses (docs/KNOWN_ISSUES.md) with no registry row yet pinning Loki's answer. |
| 9 | `/loki/api/v1/detected_fields` | explore, drilldown, api | proxy_side | 40 | 13 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume_range` | drilldown, api | proxy_side | 21 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume` | drilldown, api | proxy_side | 29 | 10 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/format_query` | explore, api | proxy_side | 3 | 1 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/detected_labels` | drilldown, api | proxy_side | 21 | 7 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 5 | `/loki/api/v1/status/buildinfo` | datasource, api | proxy_side | 8 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 4 | `/loki/api/v1/cache/generation_numbers` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 3 | `/loki/api/v1/drilldown-limits` | api | proxy_side | 8 | 2 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/prometheus/api/v1/alerts` | api | proxy_side | 5 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/rules/{namespace}/{groupName}` | api | proxy_side | 4 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/rules/{namespace}` | api | proxy_side | 4 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/api/prom/rules/{namespace}/{groupName}` | api | proxy_side | 4 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/api/prom/rules/{namespace}` | api | proxy_side | 1 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/api/prom/rules` | api | proxy_side | 2 | 0 | no test compares it against Loki; proxy-side: justify in the registry or push down to VictoriaLogs |
| 3 | `/prometheus/api/v1/rules` | api | proxy_side | 5 | 1 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 3 | `/loki/api/v1/rules` | api | proxy_side | 10 | 2 | proxy-side: justify in the registry or push down to VictoriaLogs |
| optional | `/loki/api/v1/index/shards` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry; optional: Optional planner compatibility only: no proxy route or differential proof. This endpoint is not a blocker for normal pinned Grafana Explore, Logs Drilldown or peer cache lookup. It remains a declared read API gap and stays in totals and compatibility scores. |

## Optional deletion management

Outside the read API score and ranking: the opt-in `/loki/api/v1/delete` handler
does not implement VictoriaLogs deletion. It is not a proven working delete API.

## Error surface

Every place the proxy writes an error to a client (`conformance/scripts/sync_errors.py`). A literal
message is text the proxy wrote; an error passed through the error layer (`lokiClientError`) reaches
the client only as Loki's text or Loki's fixed text for its status; a message formatted at run time
can carry text Loki would never emit, an error's own text most of all. The query routes answer in
Loki's query-frontend shape: the status and the text, `text/plain`, no errorType.

| Status | call sites | literal message | through the error layer | formatted at run time | of those, an error's own text | no body |
|---|---:|---:|---:|---:|---:|---:|
| 400 | 87 | 28 | 0 | 59 | 43 | 0 |
| 401 | 4 | 4 | 0 | 0 | 0 | 0 |
| 403 | 2 | 2 | 0 | 0 | 0 | 0 |
| 404 | 1 | 1 | 0 | 0 | 0 | 0 |
| 405 | 4 | 3 | 0 | 0 | 0 | 1 |
| 413 | 1 | 1 | 0 | 0 | 0 | 0 |
| 429 | 1 | 1 | 0 | 0 | 0 | 0 |
| 500 | 6 | 6 | 0 | 0 | 0 | 0 |
| set at run time | 89 | 1 | 86 | 2 | 1 | 0 |

The registry holds one item per status (12 under `conformance/registry/loki/errors/`), not one per message: no check yet compares a formatted message with Loki's text.

The error-layer gate (`TestErrorLayer_NoVictoriaLogsLeak`, every route through every VictoriaLogs failure mode) lists 40 route × failure-mode cells that still leak (43 cell × case pairs, ceiling 43) in `conformance/registry/error-layer-known-leaks.json`; the list may only shrink. Sites outside those routes and modes are not driven by the gate.

## Behaviour tracks

Semantics, severity, identity and data-quality behaviour the proxy must reproduce.
`cases` counts the edge cases named in the item; `wired` counts tests declaring them.

| Track | Item | Cases named | Wired | State |
|---|---|---:|---:|---|
| architecture | `architecture-syntax-package-layering` — The LogQL and LogsQL syntax packages depend on nothing but the standard library | 3 | 67 | unknown |
| limits | `backend-admission-and-heavy-query-queueing` — Heavy backend work is admitted, queued, then refused like Loki's scheduler | 15 | 8 | proven |
| resource_control | `backend-deadlines-and-cancellation` — Work the client has given up on stops in the backend too | 11 | 8 | proven |
| data_quality | `cache-response-key-versions` — An answer cached before a semantics fix is never served after it | 4 | 3 | unknown |
| data_quality | `data-density-and-chart-quality` — Chart density, zero-fill and high-cardinality behaviour | 6 | 0 | gap |
| data_quality | `data-probing-and-freshness` — Probes the proxy runs, and their cost and staleness | 11 | 1 | gap |
| limits | `heavy-metric-fetch-bounds` — Metric evaluation reads bounded work from VictoriaLogs, or refuses early | 10 | 6 | partial |
| semantics | `loki-compatible-profile` — The Loki-compatible profile holds requests and responses to Loki's contract | 14 | 7 | partial |
| identity | `metric-series-identity` — A metric series is named by the stream, its metadata and its parsed labels | 6 | 37 | partial |
| semantics | `numeric-and-response-formatting` — Timestamps, number formatting and empty shapes | 5 | 1 | gap |
| observability | `observability-query-flow` — Every client request reports how many parses, translations, cache lookups and VictoriaLogs calls it caused, and what each took | 6 | 3 | unknown |
| observability | `observability-slow-query-log` — A slow request and a slow VictoriaLogs call are counted and logged with their queries and steps | 5 | 2 | unknown |
| limits | `operator-configurable-limits` — Every bound on work is an operator flag, documented from one source | 13 | 10 | proven |
| identity | `parsed-label-series-identity` — Labels a parser extracts are part of a metric series | 6 | 4 | partial |
| semantics | `parser-error-and-label-collision` — Parser errors, __error__ and _extracted collisions | 23 | 13 | partial |
| semantics | `patterns-loki-mining` — /loki/api/v1/patterns answers the patterns Loki's pattern ingester mines, from the lines VictoriaLogs holds | 7 | 6 | partial |
| resource_control | `peer-cache-fleet-recovery` — With a ring owner down or every cache lost, the fleet answers like Loki from VictoriaLogs | 2 | 1 | partial |
| resource_control | `peer-cache-label-values-index` — A replica that lost its disk restores the label-values index from its peers | 1 | 1 | partial |
| resource_control | `peer-cache-metadata` — Label and detected-field answers are read from the ring owner and the disk tier | 4 | 1 | partial |
| resource_control | `peer-cache-patterns` — Patterns mined on one replica are served by every replica and restored on a replica that lost its disk | 5 | 4 | partial |
| resource_control | `peer-cache-query-range-windows` — Log query windows computed by their ring owner are read by the other replicas | 2 | 1 | partial |
| resource_control | `peer-cache-query-replies` — Completed query replies computed on one replica answer the same request on every replica | 2 | 1 | partial |
| resource_control | `peer-cache-replication` — Every peer-cache key has a backup replica, so losing one replica's disk or the owner costs no recompute | 3 | 4 | partial |
| resource_control | `peer-cache-stats-extent` — Stats extent buckets read on one replica are served by every replica without VictoriaLogs | 3 | 1 | partial |
| observability | `query-stats-envelope` — Every query and query_range answer carries Loki's data.stats object, filled only with measurements of that request | 11 | 3 | unknown |
| limits | `series-limits-and-partial-results` — Series limits: error, or partial result with a warning | 9 | 12 | partial |
| identity | `service-name-derivation` — service_name follows Loki's discovery order | 9 | 0 | partial |
| severity | `severity-detected-level-derivation` — detected_level is derived on the read path | 10 | 14 | partial |
| severity | `severity-exposure-surfaces` — Where detected_level and level must appear | 7 | 5 | partial |
| resource_control | `shutdown-releases-background-work` — Shutting a proxy down stops every goroutine it started | 2 | 2 | unknown |
| semantics | `tenant-label-isolation` — Tenant-label mode confines every request to its org | 4 | 4 | proven |
| semantics | `tenant-numeric-routing` — A numeric org reaches VictoriaLogs only in its canonical spelling, and strict routing serves only mapped orgs | 4 | 10 | proven |
| limits | `tenant-query-limits` — Loki's per-tenant query limits, enforced as published | 13 | 3 | proven |
| semantics | `window-bounds-and-step-alignment` — Range windows, bucket edges and step alignment | 11 | 26 | partial |

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

VictoriaLogs constructs that would serve a LogQL construct and that the proxy emits nowhere
(`conformance/scripts/link_ast.py`; what the proxy emits is a scan of the LogsQL the translator
and the proxy build, refreshed by `sync_vl_surface.py --offline`). A construct the proxy emits for
other queries is not listed even where it computes this LogQL construct itself, for example
topk and bottomk per step, label_replace, or a binary operator between two metrics; telling
those apart needs translating each construct.

| LogQL | VictoriaLogs | Available since | Covers |
|---|---|---|---|
| `and` | pipe-join | v1.40.0 | part: join inner keeps the rows with a match on the join fields; Loki's and matches whole label sets (or on/ignoring) per step |
| `or` | pipe-union | v1.40.0 | part: union appends the rows of a second query; Loki's or keeps a right-side series only when no left-side series has its labels |

LogQL constructs the proxy supports with no VictoriaLogs construct to serve them (7): `group_left`, `group_right`, `ignoring`, `on`, `rate_counter`, `unless`, `vector`

Not a backend construct: `offset` (the proxy shifts the window it sends to VictoriaLogs, internal/proxy/query_translation.go extractLogQLOffset); `|` (pipeline syntax).


## Slower than Loki on first load

Registry items whose measured shapes the proxy answers slower than Loki on a
cold request (bench/ab runs; details in [performance.md](performance.md)). Warm is the
p50 of the runs after the first, on the worst cold row; `also slower warm` counts the rows
slower warm too (— when no row comes from a run that can say). Rows Loki's results cache
answered (by its own counters or the identical-answer heuristic) are excluded, and so are rows the
newest weekly run did not re-measure (listed apart in performance.md).

| registry item | slower shape×range | worst proxy cold | Loki cold there | proxy warm there | Loki warm there | also slower warm |
|---|---|---|---|---|---|---|
| `parser-error-and-label-collision` | 6 | 4.50s (X1 json filter level_extracted, by level, 24h) | 3.50s | 0.05s | 0.03s | — |
| `quality/json-label-probe-gates` | 5 | 4.50s (X1 json filter level_extracted, by level, 24h) | 3.50s | 0.05s | 0.03s | — |
| `semantics/extracted-suffix-collision` | 5 | 4.50s (X1 json filter level_extracted, by level, 24h) | 3.50s | 0.05s | 0.03s | — |
| `semantics/json-extracted-metric-pushdown` | 4 | 4.50s (X1 json filter level_extracted, by level, 24h) | 3.50s | 0.05s | 0.03s | — |
| `drilldown/patterns-full-range-counts` | 2 | 1.18s (DP2 patterns of an environment, 1h) | 0.21s | 0.12s | 0.25s | — |
| `drilldown/patterns-one-series-per-level` | 2 | 1.18s (DP2 patterns of an environment, 1h) | 0.21s | 0.12s | 0.25s | — |
| `quality/query-range-settled-steps` | 1 | 0.96s (E2 json duration_ms p95 by service, 24h) | 0.76s | 0.01s | 0.01s | — |
| `semantics/unwrap-conversion-error` | 1 | 0.96s (E2 json duration_ms p95 by service, 24h) | 0.76s | 0.01s | 0.01s | — |
| `explore-logs-volume-json` | 1 | 0.27s (A explore volume, 2 filters, drop, 1h) | 0.16s | 0.03s | 0.03s | — |
| `parsed-label-series-identity` | 1 | 0.27s (F field breakdown service_version (explore), 1h) | 0.15s | 0.03s | 0.03s | — |
| `semantics/json-filter-pushdown-translated-label` | 1 | 0.27s (A explore volume, 2 filters, drop, 1h) | 0.16s | 0.03s | 0.03s | — |
| `semantics/json-filter-pushdown-underscore-label` | 2 | 0.27s (A explore volume, 2 filters, drop, 1h) | 0.16s | 0.03s | 0.03s | — |
| `semantics/json-label-spelling-probe` | 1 | 0.27s (F field breakdown service_version (explore), 1h) | 0.15s | 0.03s | 0.03s | — |
| `drilldown-service-landing` | 1 | 0.13s (LF9 drilldown logs, level pill, logfmt service, 24h) | 0.03s | 0.01s | 0.12s | — |
| `severity-detected-level-derivation` | 1 | 0.13s (LF9 drilldown logs, level pill, logfmt service, 24h) | 0.03s | 0.01s | 0.12s | — |
| `severity/detected-level-filter-json-level` | 1 | 0.13s (LF9 drilldown logs, level pill, logfmt service, 24h) | 0.03s | 0.01s | 0.12s | — |
| `semantics/ip-filter-json-stats-pushdown` | 1 | 0.28s (IP5 count, json ip=ip(range), 24h) | 0.19s | 0.03s | 0.01s | — |
| `semantics/ip-label-filter-missing-label` | 1 | 0.28s (IP5 count, json ip=ip(range), 24h) | 0.19s | 0.03s | 0.01s | — |
| `semantics/pattern-line-filter-anchoring` | 1 | 0.18s (A5 drilldown exclude, volume by detected_level, 24h) | 0.11s | 0.01s | 0.01s | — |
| `loki-compatible-profile` | 1 | 0.10s (E7 json count by level_extracted, 1h) | 0.04s | 0.04s | 0.01s | — |
| `profiles/structured-metadata-label-collision` | 1 | 0.10s (E7 json count by level_extracted, 1h) | 0.04s | 0.04s | 0.01s | — |
