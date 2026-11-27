# Open gaps

Generated from the registry by `conformance/scripts/gaps.py`. Do not edit by hand.

Priority = consumer weight (explore/drilldown 3, datasource 2, api 1), plus 3 when no test
compares the endpoint against Loki, plus 2 when the proxy computes the result itself.
An endpoint the owner named a priority for is placed by that first: `first` above all
others (and always listed), `last` after normal gaps, `optional` after those; the score orders within each group.
Optional gaps remain in the declared API inventory and compatibility scores.

The proxy serves Loki's read API; write endpoints such as push are not supported (ingest goes to VictoriaLogs directly). Optional deletion management is outside read API scores: its opt-in handler runs Loki's create and cancel as VictoriaLogs delete tasks, and answers Loki's 500 for the list and for queries a VictoriaLogs filter cannot select. The alerting-rules read API remains in scope, including `/api/prom/rules*` for vmalert. The deprecated read aliases `/api/prom/label`, `/api/prom/label/{name}/values`, `/api/prom/query`, `/api/prom/series` and `/api/prom/tail` are unsupported and excluded from totals, gaps and compatibility scores (owner 2026-10-31); use the corresponding `/loki/api/v1` routes. No other write endpoint is planned.

| Priority | Endpoint | Consumers | Served by | Tests | vs Loki | Missing |
|---:|---|---|---|---:|---:|---|
| 4 | `/loki/api/v1/tail` | explore, api | native_vl | 14 | 4 | owner priority: Routed (handleTail) and compared against Loki in 4 test files, but not yet Loki-identical. Missing: start and limit history replay (Loki sends the last limit entries since start before live lines; open case semantics/tail-history-start-limit); a multi-tenant tail request, which the proxy refuses (docs/KNOWN_ISSUES.md) with no registry row yet pinning Loki's answer. Not missing: limit and delay_for validation. The proxy answers Loki's recorded status, content type and text for limit (abc, -1 and 0 are refused; 100000 is accepted on the upgrade path), delay_for (abc, -1, 10; 0 and 5 are accepted), start and end, with and without the websocket upgrade headers (rows r1138-r1143 and r1436-r1448 of internal/proxy/testdata/loki_request_param_answers.jsonl, replayed byte for byte by TestRequestParamAnswers; none is a known difference), and the live e2e tail/bad_limit and tail/delay_for_over_max rows (test/e2e-compat/error_body_like_loki_test.go) compare it with Loki. Registry: semantics/error-request-validation-parity, msg-delay-for. |
| 9 | `/loki/api/v1/detected_fields` | explore, drilldown, api | proxy_side | 40 | 13 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume_range` | drilldown, api | proxy_side | 21 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/index/volume` | drilldown, api | proxy_side | 29 | 10 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/format_query` | explore, api | proxy_side | 3 | 1 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 6 | `/loki/api/v1/detected_labels` | drilldown, api | proxy_side | 21 | 7 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 5 | `/loki/api/v1/status/buildinfo` | datasource, api | proxy_side | 8 | 4 | no test wired to the registry; proxy-side: justify in the registry or push down to VictoriaLogs |
| 4 | `/loki/api/v1/cache/generation_numbers` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry |
| 3 | `/loki/api/v1/drilldown-limits` | api | proxy_side | 8 | 2 | proxy-side: justify in the registry or push down to VictoriaLogs |
| 4 | `/prometheus/api/v1/alerts` | api | native_vl | 5 | 0 | no test compares it against Loki |
| 4 | `/loki/api/v1/rules/{namespace}/{groupName}` | api | native_vl | 4 | 0 | no test compares it against Loki |
| 4 | `/loki/api/v1/rules/{namespace}` | api | native_vl | 4 | 0 | no test compares it against Loki |
| 4 | `/api/prom/rules/{namespace}/{groupName}` | api | native_vl | 4 | 0 | no test compares it against Loki |
| 4 | `/api/prom/rules/{namespace}` | api | native_vl | 1 | 0 | no test compares it against Loki |
| 4 | `/api/prom/rules` | api | native_vl | 2 | 0 | no test compares it against Loki |
| optional | `/loki/api/v1/index/shards` | api | not_implemented | 0 | 0 | not implemented; no test compares it against Loki; no test wired to the registry; optional: Optional planner compatibility only: no proxy route or differential proof. This endpoint is not a blocker for normal pinned Grafana Explore, Logs Drilldown or peer cache lookup. It remains a declared read API gap and stays in totals and compatibility scores. |

## Optional deletion management

Outside the read API score and ranking: the opt-in `/loki/api/v1/delete` handler
runs Loki's create and cancel as VictoriaLogs delete tasks with Loki's answers; the list and
queries a VictoriaLogs filter cannot select answer Loki's 500 (semantics/delete-api-via-victorialogs).

## Error surface

Every place the proxy writes an error to a client (`conformance/scripts/sync_errors.py`). A literal
message is text the proxy wrote; an error passed through the error layer (`lokiClientError`) reaches
the client only as Loki's text or Loki's fixed text for its status; a message formatted at run time
can carry text Loki would never emit, an error's own text most of all. The query routes answer in
Loki's query-frontend shape: the status and the text, `text/plain`, no errorType.

| Status | call sites | literal message | through the error layer | formatted at run time | of those, an error's own text | no body |
|---|---:|---:|---:|---:|---:|---:|
| 400 | 16 | 2 | 0 | 14 | 7 | 0 |
| 401 | 4 | 4 | 0 | 0 | 0 | 0 |
| 403 | 2 | 2 | 0 | 0 | 0 | 0 |
| 404 | 1 | 1 | 0 | 0 | 0 | 0 |
| 405 | 3 | 3 | 0 | 0 | 0 | 0 |
| 429 | 1 | 1 | 0 | 0 | 0 | 0 |
| 500 | 6 | 6 | 0 | 0 | 0 | 0 |
| set at run time | 194 | 1 | 190 | 2 | 1 | 1 |

The registry holds one item per status (12) and one per Loki error message (56) under `conformance/registry/loki/errors/`; the messages are checked against recorded Loki answers, route by route and error kind by error kind, in the generated [error compatibility coverage](../../docs/error-compatibility-coverage.md).

The error-layer gate (`TestErrorLayer_NoVictoriaLogsLeak`, every route through every VictoriaLogs failure mode) lists 0 route × failure-mode cells that still leak (0 cell × case pairs, ceiling 0) in `conformance/registry/error-layer-known-leaks.json`; the list may only shrink. Sites outside those routes and modes are not driven by the gate.

## Behaviour tracks

Semantics, severity, identity and data-quality behaviour the proxy must reproduce.
`cases` counts the edge cases named in the item; `wired` counts tests declaring them.

| Track | Item | Cases named | Wired | State |
|---|---|---:|---:|---|
| architecture | `architecture-syntax-package-layering` — The LogQL and LogsQL syntax packages depend on nothing but the standard library | 3 | 108 | unknown |
| limits | `backend-admission-and-heavy-query-queueing` — Heavy backend work is admitted, queued, then refused like Loki's scheduler | 15 | 8 | proven |
| resource_control | `backend-deadlines-and-cancellation` — Work the client has given up on stops in the backend too | 11 | 11 | proven |
| semantics | `backend-url-path-prefix` — A backend URL with a path keeps that path on every request the proxy sends | 6 | 1 | unknown |
| semantics | `backend-version-source` — The operator can name where the VictoriaLogs release comes from, and the probes check it | 3 | 2 | unknown |
| resource_control | `cache-answer-ttl-shorten-only` — An operator can shorten the cache TTL of each Loki answer class, never lengthen it, and replicas or restarts with other settings never serve each other's entries | 11 | 2 | partial |
| observability | `cache-eviction-signals` — The cache says why entries leave a tier or never enter it, how old and how reused they were, and which TTL each answer class has | 8 | 4 | partial |
| semantics | `cache-key-credential-digest` — Credentials enter cache keys only as keyed digests | 13 | 5 | unknown |
| data_quality | `cache-response-key-versions` — An answer cached before a semantics fix is never served after it | 6 | 13 | unknown |
| resource_control | `cache-restart-handover` — A replica restarted gracefully starts with its previous memory hot set, never serving an entry past its expiry or for another build | 11 | 18 | partial |
| performance | `cache-reuse-retention` — Under capacity pressure the memory cache keeps entries that are read again, and can hand evicted reused entries to the disk cache, without changing a TTL or an answer | 7 | 3 | partial |
| observability | `cache-sizing-signals` — The cache says whether a larger budget or a longer TTL would answer more, how large the working set is, how long until a key is asked again, and its byte hit ratio per answer class | 7 | 3 | partial |
| semantics | `client-authentication` — Caller authentication is off by default; on, every route but the probes, /metrics and the peer routes needs a Basic or bearer credential | 11 | 5 | unknown |
| semantics | `credential-passthrough-as-configured` — Credentials Grafana sends reach VictoriaLogs only as configured, and never the log or a shared cache key | 2 | 3 | proven |
| data_quality | `data-density-and-chart-quality` — Chart density, zero-fill and high-cardinality behaviour | 6 | 0 | gap |
| data_quality | `data-probing-and-freshness` — Probes the proxy runs, and their cost and staleness | 12 | 3 | gap |
| semantics | `delete-api-requires-token` — The delete API is closed unless the caller presents its own delete token | 5 | 4 | proven |
| resource_control | `error-coalescing-and-error-cache` — Identical requests share one run, and failed answers are held for their class's TTL | 14 | 0 | proven |
| semantics | `field-mapping-label-names` — 'The operator chooses the names a mapped field shows in answers: one per field, its own, its aliases or both' | 23 | 10 | unknown |
| limits | `heavy-metric-fetch-bounds` — Metric evaluation reads bounded work from VictoriaLogs, or refuses early | 10 | 7 | partial |
| semantics | `instrumentation-reveals-no-secrets` — /metrics and /config reveal no secret and no more than Loki's | 2 | 2 | proven |
| resource_control | `json-metric-unparsed-line-lookup-once` — A `| json` metric looks up a line Loki's parser rejects once, and one spelling sends one set of VictoriaLogs requests | 3 | 1 | partial |
| semantics | `loki-compatible-profile` — The Loki-compatible profile holds requests and responses to Loki's contract | 14 | 7 | partial |
| semantics | `loki-error-phases` — Loki's error phases and which metric queries fail on a pipeline error | 8 | 0 | unknown |
| identity | `metric-series-identity` — A metric series is named by the stream, its metadata and its parsed labels | 6 | 53 | partial |
| semantics | `numeric-and-response-formatting` — Timestamps, number formatting and empty shapes | 5 | 1 | gap |
| observability | `observability-metrics-histograms-and-labels` — Duration histograms tell a slow request from a timeout, and the proxy's endpoint label survives a scrape | 7 | 4 | unknown |
| observability | `observability-query-flow` — Every client request reports how many parses, translations, cache lookups and VictoriaLogs calls it caused, and what each took | 7 | 7 | unknown |
| observability | `observability-slow-query-log` — A slow request and a slow VictoriaLogs call are counted and logged with their queries and steps | 8 | 5 | unknown |
| limits | `operator-configurable-limits` — Every bound on work is an operator flag, documented from one source | 13 | 11 | proven |
| identity | `parsed-label-series-identity` — Labels a parser extracts are part of a metric series | 6 | 5 | partial |
| semantics | `parser-error-and-label-collision` — Parser errors, __error__ and _extracted collisions | 23 | 15 | partial |
| semantics | `patterns-loki-mining` — /loki/api/v1/patterns answers the patterns Loki's pattern ingester mines, from the lines VictoriaLogs holds | 9 | 9 | partial |
| resource_control | `peer-cache-fleet-recovery` — With a ring owner down or every cache lost, the fleet answers like Loki from VictoriaLogs | 2 | 4 | partial |
| resource_control | `peer-cache-label-values-index` — A replica that lost its disk restores the label-values index from its peers | 1 | 1 | partial |
| resource_control | `peer-cache-metadata` — Label and detected-field answers are read from the ring owner and the disk tier | 4 | 1 | partial |
| resource_control | `peer-cache-patterns` — Patterns mined on one replica are served by every replica and restored on a replica that lost its disk | 5 | 6 | partial |
| resource_control | `peer-cache-push-replay` — Pushes a replica cannot take while its breaker is open are buffered and replayed, so the fleet heals to full replication | 4 | 6 | unknown |
| resource_control | `peer-cache-query-range-windows` — Log query windows computed by their ring owner are read by the other replicas | 2 | 1 | partial |
| resource_control | `peer-cache-query-replies` — Completed query replies computed on one replica answer the same request on every replica | 2 | 1 | partial |
| resource_control | `peer-cache-read-hedge` — A peer-cache read never waits on a paused or hung replica longer than the read hedge delay | 3 | 3 | partial |
| resource_control | `peer-cache-replication` — Every peer-cache key has a backup replica, so losing one replica's disk or the owner costs no recompute | 3 | 8 | partial |
| resource_control | `peer-cache-stats-extent` — Stats extent buckets read on one replica are served by every replica without VictoriaLogs | 3 | 1 | partial |
| semantics | `query-complexity-limits` — Operator limits on query size and on the proxy's own planning time | 8 | 10 | unknown |
| observability | `query-stats-envelope` — Every query, query_range, volume and volume_range answer carries Loki's whole data.stats object, filled with measurements of that request in Loki's accounting | 14 | 8 | unknown |
| semantics | `release-image-pinned-by-digest` — The chart can pin the released image by digest, and each release publishes that digest as a file | 4 | 1 | unknown |
| semantics | `secrets-outside-process-arguments` — The proxy's secrets need no process argument, and never reach logs or admin answers | 4 | 3 | unknown |
| limits | `series-limits-and-partial-results` — Series limits: error, or partial result with a warning | 13 | 13 | partial |
| identity | `service-name-derivation` — service_name follows Loki's discovery order | 20 | 1 | partial |
| severity | `severity-detected-level-derivation` — detected_level is derived on the read path | 12 | 22 | partial |
| severity | `severity-exposure-surfaces` — Where detected_level and level must appear | 7 | 5 | partial |
| resource_control | `shutdown-releases-background-work` — Shutting a proxy down stops every goroutine it started | 3 | 6 | unknown |
| semantics | `tail-reads-settled-rows` — A live tail reads only rows VictoriaLogs has made searchable, so no line of a stream is skipped | 2 | 3 | unknown |
| semantics | `tenant-auth-follows-loki` — X-Scope-OrgID is admitted and refused as Loki does, before any cache or VictoriaLogs call | 4 | 6 | proven |
| semantics | `tenant-label-isolation` — Tenant-label mode confines every request to its org | 4 | 4 | proven |
| semantics | `tenant-numeric-routing` — A numeric org reaches VictoriaLogs only in its canonical spelling, and strict routing serves only mapped orgs | 4 | 15 | proven |
| limits | `tenant-query-limits` — Loki's per-tenant query limits, enforced as published | 16 | 6 | proven |
| semantics | `window-bounds-and-step-alignment` — Range windows, bucket edges and step alignment | 11 | 35 | partial |

## LogQL surface (v3.7.7)

68/68 constructs are referenced in proxy code. Not referenced anywhere:


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

LogQL constructs the proxy supports with no VictoriaLogs construct to serve them (11): `--keep-empty`, `--strict`, `approx_topk`, `group_left`, `group_right`, `ignoring`, `on`, `rate_counter`, `unless`, `variants`, `vector`

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
| `explore-logs-volume-json` | 2 | 6.83s (A explore volume, 2 filters, drop, 24h) | 4.79s | 0.02s | 0.10s | — |
| `parser-error-and-label-collision` | 10 | 6.83s (A explore volume, 2 filters, drop, 24h) | 4.79s | 0.02s | 0.10s | — |
| `quality/probe-regexp-parse-cost` | 9 | 6.83s (A explore volume, 2 filters, drop, 24h) | 4.79s | 0.02s | 0.10s | — |
| `semantics/json-filter-pushdown-translated-label` | 2 | 6.83s (A explore volume, 2 filters, drop, 24h) | 4.79s | 0.02s | 0.10s | — |
| `semantics/json-filter-pushdown-underscore-label` | 3 | 6.83s (A explore volume, 2 filters, drop, 24h) | 4.79s | 0.02s | 0.10s | — |
| `quality/json-label-probe-gates` | 5 | 6.27s (X1 json filter level_extracted, by level, 24h) | 4.76s | 0.01s | 0.04s | — |
| `semantics/extracted-suffix-collision` | 5 | 6.27s (X1 json filter level_extracted, by level, 24h) | 4.76s | 0.01s | 0.04s | — |
| `semantics/json-extracted-metric-pushdown` | 5 | 6.27s (X1 json filter level_extracted, by level, 24h) | 4.76s | 0.01s | 0.04s | — |
| `drilldown/patterns-full-range-counts` | 3 | 1.31s (DP2 patterns of an environment, 1h) | 0.28s | 0.11s | 0.28s | — |
| `drilldown/patterns-one-series-per-level` | 3 | 1.31s (DP2 patterns of an environment, 1h) | 0.28s | 0.11s | 0.28s | — |
| `semantics/json-error-filter-metric-split-pushdown` | 1 | 1.45s (J3 json no-error count by parsed method, 24h) | 0.96s | 0.01s | 0.02s | — |
| `semantics/ip-filter-json-stats-pushdown` | 3 | 1.06s (IP4 count, json ip=ip(CIDR), 24h) | 0.65s | 0.01s | 0.01s | — |
| `semantics/ip-label-filter-missing-label` | 3 | 1.06s (IP4 count, json ip=ip(CIDR), 24h) | 0.65s | 0.01s | 0.01s | — |
| `operator-keep` | 2 | 1.42s (K6 count by kept key, json, 24h) | 1.06s | 0.01s | 0.01s | — |
| `semantics/keep-leaves-labels-loki-drops` | 2 | 1.42s (K6 count by kept key, json, 24h) | 1.06s | 0.01s | 0.01s | — |
| `quality/query-range-settled-steps` | 3 | 1.28s (E2 json duration_ms p95 by service, 24h) | 0.96s | 0.01s | 0.01s | — |
| `semantics/unwrap-conversion-error` | 2 | 1.28s (E2 json duration_ms p95 by service, 24h) | 0.96s | 0.01s | 0.01s | — |
| `semantics/json-detected-level-error-split` | 1 | 1.19s (DLV7 plain json, detected_level, 30s range (shorter than the 24h step), 24h) | 0.87s | 0.01s | 0.01s | — |
| `semantics/json-metric-detected-level-derivation` | 2 | 1.19s (DLV7 plain json, detected_level, 30s range (shorter than the 24h step), 24h) | 0.87s | 0.01s | 0.01s | — |
| `semantics/json-metric-step-wider-than-range` | 1 | 1.19s (DLV7 plain json, detected_level, 30s range (shorter than the 24h step), 24h) | 0.87s | 0.01s | 0.01s | — |
| `severity-detected-level-derivation` | 7 | 1.19s (DLV7 plain json, detected_level, 30s range (shorter than the 24h step), 24h) | 0.87s | 0.01s | 0.01s | — |
| `parsed-label-series-identity` | 1 | 0.36s (F field breakdown service_version (explore), 1h) | 0.21s | 0.02s | 0.05s | — |
| `semantics/json-label-spelling-probe` | 1 | 0.36s (F field breakdown service_version (explore), 1h) | 0.21s | 0.02s | 0.05s | — |
| `metric-series-identity` | 2 | 0.55s (W3 sum without pod, rate, 24h) | 0.41s | 0.01s | 0.01s | — |
| `semantics/unwrap-sum-without-collapses` | 1 | 0.55s (W3 sum without pod, rate, 24h) | 0.41s | 0.01s | 0.01s | — |
| `parser-logfmt` | 2 | 0.35s (LF20 logfmt count by detected_level, 24h) | 0.25s | 0.01s | 0.01s | — |
| `parser-json` | 1 | 0.34s (LF16 drilldown label breakdown, level filter, 24h) | 0.23s | 0.02s | 0.01s | — |
| `semantics/detected-level-filter-before-parser-pushdown` | 1 | 0.34s (LF16 drilldown label breakdown, level filter, 24h) | 0.23s | 0.02s | 0.01s | — |
| `drilldown-service-landing` | 2 | 0.13s (LF10 drilldown logs, level pill multi-select, json service, 1h) | 0.03s | 0.07s | 0.03s | — |
| `severity/detected-level-filter-json-level` | 4 | 0.13s (LF10 drilldown logs, level pill multi-select, json service, 1h) | 0.03s | 0.07s | 0.03s | — |
| `quality/unwrap-logfmt-narrowed-unpack` | 1 | 0.29s (G8 bytes() of a number, 24h) | 0.20s | 0.01s | 0.01s | — |
| `semantics/unwrap-gate-parsefloat-divergences` | 1 | 0.29s (G8 bytes() of a number, 24h) | 0.20s | 0.01s | 0.01s | — |
| `semantics/pattern-line-filter-anchoring` | 1 | 0.24s (A5 drilldown exclude, volume by detected_level, 24h) | 0.19s | 0.01s | 0.01s | — |
