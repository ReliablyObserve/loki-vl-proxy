---
sidebar_label: Real-window compatibility gaps
description: Measured compatibility findings using isolated populated fixtures.
# CommonMark, not MDX: the generated section sits between HTML comment markers (conformance/scripts/perf_evidence.py).
mdx:
  format: md
---

# Real-window compatibility findings

**Current state lives in the conformance registry, not on this page.** The reports below are generated from it on every change and checked in CI:

- [Coverage map](compatibility-coverage.md): every Loki endpoint, how it is served and what evidence proves it.
- [Ranked differences from Loki](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/parity-gaps.md): every known difference, open or fixed, ranked by user impact.
- [Open gaps](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/gaps.md): endpoints, the error surface, behaviour tracks, the LogQL surface and VictoriaLogs reuse opportunities.
- [Performance evidence](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/performance.md): every measured shape the proxy answers slower than Loki.
- [Quality and performance vs Loki (current)](https://github.com/ReliablyObserve/loki-vl-proxy#quality-and-performance-vs-loki-current): the dated summary of the numbers above, with the open differences and the shapes that are still slower.

## Quality and performance vs Loki (current)

<!-- quality-summary:start -->

Tested against Loki v3.7.9, the release the compatibility stack and CI pin. Timings were measured on 2026-11-08 at commit `e4a743f675` on the project's A/B stack: the same data, the same hardware, the latest valid weekly run of each of 39 query-shape sets over 1h to 24h. A pair is one query shape over one time range; "comparable" means both sides answered 200 and Loki's results cache did not answer the first request. Faster and slower mean beyond 25% and 50 ms; everything else is on par. A `topk` or `bottomk` answer is compared by its ranked points and their sum, not by its series count.

Loki profile shown: `loki-defaults`, Loki with its own defaults (align_queries_with_step off, max_cache_freshness_per_query 10m); proxy builds run with their defaults. The `aligned` profile (the stack's Loki (align_queries_with_step on, max_cache_freshness_per_query 0s)) is measured too: the distribution table below shows both profiles, the rest of this section the one shown. The proxy serves Loki's read API; write endpoints such as push are not supported (ingest goes to VictoriaLogs directly). Optional deletion management is outside read API scores: its opt-in handler does not implement VictoriaLogs deletion. The alerting-rules read API remains in scope, including `/api/prom/rules*` for vmalert. The deprecated read aliases `/api/prom/label`, `/api/prom/label/{name}/values`, `/api/prom/query`, `/api/prom/series` and `/api/prom/tail` are unsupported and excluded from totals, gaps and compatibility scores (owner 2026-10-31); use the corresponding `/loki/api/v1` routes. No other write endpoint is planned.

**🟢 Where the proxy is better than Loki**

- First request: **352 of 560** comparable pairs are faster (63%).
- Drilldown and Explore pages: **83 of 117** faster (71%); median first request **0.07 s** against Loki's **0.29 s**.
- Metric queries: **238 of 360** faster (66%); median first request **0.05 s** against **0.17 s**.
- Label and label-value calls: median repeat **0.00 s** against Loki's **0.04 s**, over 36 pairs (36 of them answered by Loki's results cache).
- Share of rows below Loki, first request: **62.9%** (352 of 560 rows); repeat: **27.4%** (208 of 759). The target is most queries below Loki (above 50%).

**🟡 Where it is on par**

- First request: **202 of 560** pairs are within noise (36%).
- Log queries: **47 of 75** on par; metric queries: **120 of 360**; Drilldown and Explore pages: **33 of 117**.
- Rows close to Loki (within the same noise): **202** on the first request, **550** on a repeat.
- Repeat of the same query, median p50: Drilldown and Explore pages **0.01 s** against **0.05 s**; metric queries **0.01 s** against **0.02 s**; log queries **0.01 s** against **0.06 s**.
- Answers: **713 of 779** pairs return the same result as Loki (92%).
- `sum_over_time`, `avg_over_time`, `max_over_time` and `min_over_time` over an unwrapped field answer Loki's window on `query_range` for a range longer or shorter than the step (some outer aggregations of them are still listed as open differences).
- An unwrap metric moved forward by a refresh or a re-run reads from VictoriaLogs only the buckets it has not read before.
- A zero divisor of `/` and `%` is NaN, like Loki.
- Binary vector matching raises Loki's duplicate-series error text at the first step where Loki's join fails.
- Eligible shared cache keys use owner plus one backup by default (`-peer-replication-factor=2`); RF=1 keeps only the owner. Nonowner RAM shadows are bounded to 30s and 10% of L1 capacity without extra disk copies. Three-node tests cover pooled capacity, disk refill and owner loss.

**🔴 Where it is worse**

- First request: **6 of 560** pairs are slower (1%): log queries **0 of 75**, Drilldown and Explore pages **1 of 117**, metric queries **2 of 360**.
- Between 1× and 2× Loki: **3** rows on the first request, **1** on a repeat; above 2× Loki: **3** on the first request and **0** on a repeat (the 2026-11-08 distribution).
- **2 shape, range and mode rows are on the above-2× list for the `loki-defaults` profile** (more than 2× Loki and more than 50 ms slower): **2** on the first request, **0** on a repeat, **0** on a refresh. Each is a defect being fixed: a row joins the list after two consecutive weekly runs above 2× and leaves it after two within 2×, so it can hold more or fewer rows than the day's distribution above; a change that adds or worsens a row is flagged. 29 of 108 covered registry items are locked at the 2× bound.
- In the run shown, **1** of those rows measured above 2× Loki and **1** within 2×.
- Largest gaps on that list in the run shown, first request: `LS8 json count by level_extracted, collision` over 6h, **0.30 s** against **0.12 s**.
- Answers: **51 of 779** pairs return a different result from Loki, and **15** could not be compared.

**Distribution versus Loki in the run shown, both Loki profiles**

Rows compared: both sides answered 200 and, on a first request, Loki's results cache did not answer. Bands are those of `bench/ab/over2x.py`: beyond 25% and 50 ms, and above 2× Loki also more than 50 ms behind. The `aligned` profile runs the range-metric shapes only (the queries its setting changes).

| Loki profile | Request | Rows compared | 🟢 Below Loki | 🟡 Close to Loki | 🟠 1–2× Loki | 🔴 Above 2× Loki | Not judged: Loki not 200 / proxy not 200 / first request from Loki's cache |
|---|---|---:|---:|---:|---:|---:|---|
| `loki-defaults` | first | 560 | 352 (62.9%) | 202 (36.1%) | 3 (0.5%) | **3 (0.5%)** | 20 / 0 / 199 |
| `loki-defaults` | repeat | 759 | 208 (27.4%) | 550 (72.5%) | 1 (0.1%) | **0 (0.0%)** | 20 / 0 / — |
| `aligned` | first | 440 | 308 (70.0%) | 128 (29.1%) | 3 (0.7%) | **1 (0.2%)** | 19 / 0 / 83 |
| `aligned` | repeat | 523 | 19 (3.6%) | 504 (96.4%) | 0 (0.0%) | **0 (0.0%)** | 19 / 0 / — |

**By workload in the run shown**

First request and repeat of the comparable pairs of the `loki-defaults` run, as p50 · p90 · slowest pair: the median hides the heavy queries, the p90 and the slowest show them. The counts are of pairs, with the rule above (🟢 faster, 🟡 on par, 🔴 slower). A repeat is the recorded p50 of the repeated request, which may be a cache hit on either side.

| Workload | Pairs | 🟢 / 🟡 / 🔴 | First request: proxy | Loki | Repeat p50: proxy | Loki |
|---|---:|:---:|---:|---:|---:|---:|
| Metadata and label browser | 8 | 3 / 2 / 3 | **0.06 · 0.97 · 0.97 s** | 0.09 · 0.43 · 0.43 s | 🟢 **0.02 s** | 0.10 s |
| Drilldown and Explore pages | 117 | 83 / 33 / 1 | **0.07 · 0.52 · 3.34 s** | 0.29 · 2.83 · 7.13 s | 🟡 **0.01 s** | 0.05 s |
| Log queries | 75 | 28 / 47 / 0 | **0.01 · 0.06 · 0.15 s** | 0.06 · 0.23 · 1.09 s | 🟡 **0.01 s** | 0.06 s |
| Metric queries | 360 | 238 / 120 / 2 | **0.05 · 0.52 · 3.27 s** | 0.17 · 2.86 · 31.32 s | 🟡 **0.01 s** | 0.02 s |

**By time range, first request**

How each side grows with the range a user picks: p50 · p90 · slowest pair of each workload and range.

| Workload | Range | Pairs | 🟢 / 🟡 / 🔴 | Proxy | Loki |
|---|---|---:|:---:|---:|---:|
| Drilldown and Explore pages | 1h | 40 | 24 / 16 / 0 | **0.03 · 0.06 · 0.24 s** | 0.14 · 0.19 · 0.52 s |
| Drilldown and Explore pages | 6h | 36 | 26 / 10 / 0 | **0.08 · 0.38 · 0.58 s** | 0.50 · 0.91 · 1.92 s |
| Drilldown and Explore pages | 24h | 39 | 32 / 6 / 1 | **0.28 · 1.38 · 3.34 s** | 2.67 · 3.47 · 7.13 s |
| Log queries | 1h | 30 | 4 / 26 / 0 | **0.03 · 0.06 · 0.07 s** | 0.06 · 0.10 · 0.17 s |
| Log queries | 3h | 7 | 5 / 2 / 0 | **0.01 · 0.01 · 0.01 s** | 0.07 · 0.10 · 0.10 s |
| Log queries | 6h | 19 | 8 / 11 / 0 | **0.01 · 0.07 · 0.11 s** | 0.06 · 0.27 · 0.28 s |
| Log queries | 24h | 19 | 11 / 8 / 0 | **0.01 · 0.10 · 0.15 s** | 0.08 · 1.08 · 1.09 s |
| Metric queries | 1h | 128 | 65 / 63 / 0 | **0.02 · 0.07 · 0.18 s** | 0.09 · 0.21 · 2.82 s |
| Metric queries | 6h | 99 | 76 / 22 / 1 | **0.09 · 0.31 · 0.89 s** | 0.50 · 1.07 · 11.04 s |
| Metric queries | 24h | 97 | 86 / 10 / 1 | **0.31 · 1.18 · 3.27 s** | 2.55 · 4.07 · 22.03 s |

**The heaviest queries for Loki in the run shown**

The 10 comparable pairs Loki takes longest to answer on a first request: the queries a user waits for. The multiple is Loki's time over the proxy's, or the reverse when the proxy is slower.

| Query shape | Range | Workload | Proxy | Loki | Proxy vs Loki |
|---|---|---|---:|---:|---|
| `F1 sum by over a ratio of two per-stream metrics, 20s step` | 24h-20s | Metric queries | **2.48 s** | 31.32 s | 🟢 12.6× faster |
| `B2 sum by over a ratio of two per-stream metrics` | 24h | Metric queries | **2.39 s** | 22.03 s | 🟢 9.2× faster |
| `F3 avg by service of rate, 20s step` | 24h-20s | Metric queries | **1.20 s** | 11.34 s | 🟢 9.5× faster |
| `B2 sum by over a ratio of two per-stream metrics` | 6h | Metric queries | **0.63 s** | 11.04 s | 🟢 17.5× faster |
| `F2 count by service of count_over_time, 20s step` | 24h-20s | Metric queries | **0.01 s** ‡ | 8.71 s | 🟢 1451.2× faster |
| `F4 sum of max by service,app, 20s step` | 24h-20s | Metric queries | **2.70 s** | 8.34 s | 🟢 3.1× faster |
| `DB1 label breakdown pod` | 24h | Drilldown and Explore pages | **0.70 s** | 7.13 s | 🟢 10.2× faster |
| `SF9 volume, logfmt label_format rename` | 24h | Metric queries | **1.80 s** | 5.12 s | 🟢 2.8× faster |
| `G10 logfmt latency_ms sum_over_time, production` | 24h | Metric queries | **0.99 s** | 4.78 s | 🟢 4.8× faster |
| `E3 logfmt latency_ms rate, production` | 24h | Metric queries | **0.96 s** | 4.71 s | 🟢 4.9× faster |

‡ The proxy answered a first request over 6h or more in under 50 ms: probably from its own cache, warmed by an earlier set of the weekly run that sent the same query. The run records Loki's cache counters but not the proxy's, so this row is not a cold proxy.

**Where the proxy loses most in the run shown**

| Query shape | Range | Workload | Proxy | Loki | Proxy vs Loki |
|---|---|---|---:|---:|---|
| `DP1 patterns of a plain-text service` | 1h | Metadata and label browser | **0.21 s** | 0.02 s | 🔴 13.1× slower |
| `LS8 json count by level_extracted, collision` | 6h | Metric queries | **0.30 s** | 0.12 s | 🔴 2.5× slower |
| `DP2 patterns of an environment` | 1h | Metadata and label browser | **0.97 s** | 0.43 s | 🔴 2.3× slower |
| `A5 drilldown exclude, volume by detected_level` | 24h | Drilldown and Explore pages | **0.16 s** | 0.10 s | 🔴 1.7× slower |
| `DP2 patterns of an environment` | 24h | Metadata and label browser | **0.24 s** | 0.15 s | 🔴 1.6× slower |

Label browser and Drilldown metadata calls: every pair both sides answered with 200, including the first requests Loki's results cache answered (counted in the third column; a Loki time near zero there is its cache). Medians.

| Call | Pairs | Loki cache-answered | First request: proxy | Loki | Repeat: proxy | Loki |
|---|---:|---:|---:|---:|---:|---:|
| Drilldown detected labels and fields | 15 | 15 | 🟡 **0.03 s** | 0.03 s | 🟡 **0.00 s** | 0.02 s |
| Label values | 18 | 18 | 🟡 **0.00 s** | 0.05 s | 🟡 **0.00 s** | 0.05 s |
| Labels (label browser) | 18 | 18 | 🟡 **0.00 s** | 0.02 s | 🟡 **0.00 s** | 0.02 s |
| Volume (Drilldown landing) | 9 | 1 | 🟡 **0.18 s** | 0.15 s | 🟢 **0.01 s** | 0.08 s |

**Quality of the Loki read API**

| | Now |
|---|---|
| Loki endpoints the proxy routes | **25 of 27** (93%) |
| Endpoints with tests that compare the proxy against Loki | **19 of 27** (70%) |
| Endpoints whose registry state is `proven` | **12 of 27** (44%) |
| Routed endpoints answered natively by VictoriaLogs | 6 of 25 |
| Behaviour and feature items | 6 proven, 21 partial, 3 gaps |
| Known differences from Loki (ranked) | **107**: 84 open, 13 documented, 7 owner-kept, 3 upstream |
| Open differences visible in Grafana (registry impact) | **58 of 84**: 44 Explore, 13 Logs Drilldown, 1 Grafana datasource; 12 API only, 14 edge cases |

**Readiness by client**

The Loki endpoints each client calls (registry `consumers`), with the same evidence as above, and the open differences filed as visible to that client.

| Client | Loki endpoints it calls | Routed | Compared with Loki in tests | Proven | Open differences it shows |
|---|---:|---:|---:|---:|---:|
| Grafana Explore | 10 | 10 (100%) | 10 (100%) | 8 (80%) | 44 |
| Logs Drilldown | 11 | 11 (100%) | 11 (100%) | 9 (82%) | 13 |
| Grafana Loki datasource | 6 | 6 (100%) | 6 (100%) | 4 (67%) | 1 |
| Loki API clients | 27 | 25 (93%) | 19 (70%) | 12 (44%) | 12 |

Read routes not implemented: `/loki/api/v1/cache/generation_numbers`, `/loki/api/v1/index/shards` (optional planner gap, low priority).

These are registry evidence counts, not a full runtime compatibility percentage. Optional deletion management is excluded from the read totals; deprecated aliases and write-only APIs are outside this score.

The 5 open differences with the most impact (all explore-visible): `parsed-identity/bare-parser-metric-duplicates-extracted` (bare JSON metrics cannot distinguish equal-valued metadata from ingested body fields), `semantics/parse-error-labels-on-log-lines` (A line a parser rejects carries __error__ and __error_details__ in Loki's log answer), `semantics/tail-history-start-limit` (native tail ignores start and limit: Grafana Live shows no history where Loki shows its last 100 lines), `semantics/unwrap-per-series-identity-raw-rows` (A function taken per series (avg, max, min over time, rate_counter) under sum or sum by uses the stream as the series), `semantics/binary-ignoring-matching` (`+ ignoring(label)` between two sums joins the wrong series). Differences kept on purpose are listed as documented or owner-kept.

Details, the per-workload tables and the method: [performance summary](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/performance.md#summary-versus-loki) · [performance guide](performance.md#current-position-versus-loki) · [ranked differences](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/parity-gaps.md) · [coverage map](compatibility-coverage.md) · [Known Issues](KNOWN_ISSUES.md).

<!-- quality-summary:end -->

This page records the populated-window measurement that corrected the exhaustive helper's timestamps, the fixes that followed it, and the limits found then. It is a historical record from the v1.67.0 integration: the pass counts below were measured then, and the "Remaining limits" may since have been fixed or changed; the current state is in the reports above. Versions named below are the ones measured at the time.

These findings cover populated-query regressions and resource guards following the v1.67.0 security integration. They do not establish full LogQL parity. Each correction has a focused regression; the remaining limits below define the scope of the compatibility claim.

## Timestamp defect

The exhaustive helper sent millisecond integers to Loki, which interprets integers as nanoseconds. Loki therefore queried 1970 while the proxy queried current data. RFC3339Nano timestamps now address the same populated window. A literal canary also requires both freshly ingested lines from both backends. See the [Loki timestamp contract](https://grafana.com/docs/loki/latest/reference/loki-http-api/#timestamps).

## Isolated baseline

A fresh Compose project with Loki 3.7.7, VictoriaLogs 1.50.0 and a pre-release build of these corrections, without the UI generator, produced **283/284 query checks** and **64/70 error checks**. The query failure was implicit many-to-one matching. The six error failures were four invalid IP cases and a duplicated parser-error case.

The default `test/e2e-compat/docker-compose.yml` pins Loki 3.7.9 and VictoriaLogs v1.53.0, the same Loki release as the `docker-compose.review.yml` override; `LOKI_IMAGE` selects another Loki release.

The earlier 255/284 query result came from a long-running UI generator stack with repeated ingestion. Its reference timeouts and empty-result findings were not isolated reproductions. A later deterministic test reproduced the regexp capture/filter failure after the stored-field inventory was warmed: a query-created `http_method` capture was incorrectly rewritten to a stored `http.method` field. Query-local capture names now remain independent of that inventory.

The exhaustive checks compare status, result type and non-emptiness. Strict quantile canaries demonstrated wrong grouping and values even when those checks passed. They are insufficient proof of labels, samples or timestamps.

## Corrections

- IP line filters reject invalid single addresses, prefixes, ranges and unsupported operators. Substring filters preserve literal regex metacharacters, including text resembling `ip(...)`.
- Grouped quantiles retain `by (labels)` and `by ()`, interpolate over trailing windows and include samples at the evaluation timestamp. The exact adapter uses bounded raw samples; it does not establish native quantile performance.
- Binary matching checks cardinality independently at each timestamp. Empty grouping modifiers retain their meaning, disjoint streams do not conflict, and set operations retain their many-to-many exemption. Original operands execute through the scoped query handlers, preserving trailing windows, error propagation, extraction aliases and comparison semantics.
- Additive ungrouped sums reduce all streams instead of leaking intermediate backend groups. Regexp captures remain visible as parsed fields in ordinary and categorized log responses.
- Tumbling range aggregations (range equal to step) served from VictoriaLogs `stats_query_range` buckets are relabelled to Loki's evaluation timestamps. VictoriaLogs labels a bucket by its start and covers `[T, T+step)`; Loki's sample at `T` covers `(T-range, T]`. The proxy fetches from `start-range` and moves each bucket forward by one range, instead of trimming the bucket Loki's first sample needs. It covers `rate` and `bytes_rate` in any aggregation, and ungrouped `sum(count_over_time(...))` and `sum(bytes_over_time(...))`.
- Sliding range aggregations (`count_over_time`, `rate`, `bytes_over_time`, `bytes_rate` with a range different from the step) served from `stats_query_range` buckets sum buckets of `gcd(step, range)` anchored to the request start and shifted by one nanosecond through the `offset` argument, so each bucket covers `(T, T+bucket]` and every window `(t-range, t]` is an exact union of buckets, including lines on window edges and unaligned starts. A step whose window holds no line is absent, as in Loki, for every client including Drilldown. Buckets below 1 ms and stats responses over the byte limit use the raw-sample evaluator, which applies the same `(t-range, t]` boundaries to these functions for range and instant queries.
- Bare parser range aggregations without an outer aggregation (`count_over_time({...} | logfmt [90s])`, also `regexp`, `pattern` and `json` with parameters) use the same anchored `gcd(step, range)` buckets: `stats_query_range` grouped by stream for `count_over_time`, `rate`, `bytes_over_time` and `bytes_rate` with a range greater than or equal to the step (byte sums add a `count()` presence column, so a window holding only empty lines is a zero sample), VictoriaLogs `/hits` with the same `offset` argument for sliding `count_over_time` and `rate` when stream label fields are declared, and bucket values for `sum_over_time`, `max_over_time` and `min_over_time` over `unwrap`. Tumbling queries on this path are no longer merged into one unlabelled series. Bare `| json` without parameters keeps the ordered JSON evaluator.
- Binary expressions joining two vector operands evaluate both on the step-aligned grid, as Loki's query frontend does when `align_queries_with_step` is enabled. The proxy moves every metric `query_range` request onto that grid by default (`-align-queries-with-step`); a Loki with its defaults floors the start to the step too, in its metric splitter (`split_queries_by_interval`, 1h by default), and rounds the end up. Operands with different ranges take different execution paths (sliding evaluation versus tumbling buckets), and an unaligned start previously left no common timestamps to join.
- `unwrap duration(...)` and `unwrap bytes(...)` over bare parsers convert unit strings such as `15ms` and `1024B` instead of dropping every sample.
- Range operands grouped `by (level)` keep Loki's `level` key, so `on(level)` matching works for range queries as it already did for instant queries.
- Selector regexps (`{app=~"api"}`) match the whole label value, as Prometheus compiles them (`^(?s:…)$`). VictoriaLogs' `field:~` is a substring match, so `{app=~"api"}` selected `api-gateway` and `{namespace=~"prod"}` selected `production` where Loki returns nothing; results of such queries shrink to Loki's. Label filters after a parser (`| status=~"5.."`) match what Loki's simplified filter matches: a literal or literal alternation compares whole values, `api.*` and a factored alternation such as `api|api-gateway` compare by substring, as in Loki. Line filters keep their substring semantics.
- Scalar results carry second timestamps and all sample values use Loki's fixed-point rendering (`1234000`, not `1.234e+06`).

- Metric queries over `| regexp` and `| pattern` keep the labels those stages extract in the series identity, as Loki does, so an existing query now returns one series per captured value instead of one per stream (a minute of one generator app: 450 series instead of 30, same total). `| json` and `| logfmt` are unchanged by default; `-exact-parser-series-identity` opts in.

These corrections reject previously accepted invalid queries and change incorrect numeric results. They do not imply unchanged behavior for all clients. IP validation is eager: Loki can bypass invalid pipeline construction for historical empty ranges, while the proxy rejects the invalid expression.

## Ordered parser error state

VictoriaLogs JSON unpacking does not produce Loki error labels. Filtering those labels after pushdown can lose malformed lines or include lines that Loki rejects. Stage order and aggregation hints affect the result:

| Pipeline inside a metric | Measured Loki behavior |
| --- | --- |
| JSON with surviving malformed input | Pipeline error in ordinary grouped/raw evaluation |
| JSON then empty-error filter | Valid parsed samples only |
| JSON then nonempty-error filter | Pipeline error |
| JSON then drop error | Valid and malformed samples accepted |
| JSON, nonempty-error filter, then drop error | Malformed samples accepted |
| Drop error before nonempty-error filter | Empty result |
| Drop error details only | Error state survives |

Aggregation can suppress parsing or retain error labels. A blanket syntax rejection or malformed-JSON preflight does not implement this contract. The original strict canary passed 24/24 Loki checks and only 5/24 proxy checks. The ordered evaluator now preserves error state, aggregation hints, drop/keep ordering, structured-metadata collisions and Loki string decoding for eligible count/rate/byte metrics. Subsequent canaries also check both window boundaries and the proven native-aggregation optimization.

Eligibility is deliberately limited to supported bare or sum-wrapped JSON count/rate/byte pipelines. Explicit JSON extraction, mixed parsers, unwrap, compound or numeric predicates and non-sum wrappers retain their existing paths. Passing this coverage does not claim those paths have identical parser-error semantics.

Sources: [Loki parser](https://github.com/grafana/loki/blob/v3.7.7/pkg/logql/log/parser.go), [parser hints](https://github.com/grafana/loki/blob/v3.7.7/pkg/logql/log/parser_hints.go), [evaluator](https://github.com/grafana/loki/blob/v3.7.7/pkg/logql/evaluator.go), [VictoriaLogs JSON unpacking](https://github.com/VictoriaMetrics/VictoriaLogs/blob/v1.50.0/lib/logstorage/pipe_unpack_json.go).

## Resource and operational findings

Raw metric collectors request one extra row and reject overflow rather than returning a successful partial calculation. A final LogsQL limit pipe avoids VictoriaLogs' implicit timestamp sort from the HTTP `limit` argument; complete samples are sorted locally. See the [VictoriaLogs query contract](https://docs.victoriametrics.com/victorialogs/querying/#querying-logs).

| Guard | Setting | Client-visible result |
| --- | --- | --- |
| Raw rows per manual metric call | `-manual-range-metric-row-limit` (default 1,000,000) | HTTP 400, `manual range metric row limit exceeded` naming the flag |
| Metric query series | `-max-stats-query-series` (default 500) | HTTP 400, Loki's `maximum number of series (N) reached for a single query`; Logs Drilldown gets a partial result with the `... returning partial results` warning |

Every metric path applies the series limit the same way: raw and ordered-parser evaluators, stats and hits buckets, sliding windows, binary expressions and unions, multi-tenant merges, range and instant. Logs Drilldown's partial result holds the busiest series by total value, where Loki keeps the first series it encounters. Logs Drilldown label and field breakdowns take the same path: every series up to the limit with Loki's values, and over it the busiest series, ranked by VictoriaLogs in a bounded second call, with Loki's warning.

Binary evaluation limits nesting to 64 and child evaluations to 1,024. It shares budgets across children: 256 MiB of captured response data, two million decoded arrays, one million constructed output samples and 64 MiB of label-processing work. Individual encoded results are capped at 64 MiB. These are conservative work limits: repeated labels and nested intermediate results consume budget, so a valid large expression can now fail explicitly.

The release UI pass also found a local Compose configuration mismatch: a 1 GiB VL cap retained settings intended for the repository's 5 GiB profile. Wide Fields queries OOM-killed VL, after which the dropdown displayed no options. Restoring the repository memory/restart settings restored all seven label ranges. Independently, a broad bare-parser unwrap query exposed excessive proxy output and required resource guards on that separate path. A backend failure can cause the two ingestion targets to diverge; comparisons after that failure require a fresh paired fixture.

## Remaining limits

Valid label `ip()` syntax and exact matching of all IPv6/non-octet CIDR/range forms need further compatibility work; argument validation does not fix approximate translation. Regexp capture collisions with existing labels and learned aliases when only an alternate stored spelling exists remain separate limits. The grouped quantile canary uses a shared aligned evaluation axis; arbitrary frontend range alignment is not established by it. Native tumbling results are on the step-aligned grid, Loki's grid now that metric `query_range` bounds are aligned by default (with `-align-queries-with-step=false` they stay on it where Loki with its splitting off evaluates at `start+k*step`), and a bucket boundary is inclusive at its start where Loki's window is inclusive at its end. Grouped tumbling `count_over_time` and `bytes_over_time`, served by the Drilldown hits and hybrid paths, are not relabelled, so their labels mark bucket starts while ungrouped sums use Loki's evaluation timestamps. Bare parser range aggregations served from buckets label each series with its stream labels (or the declared fields on the `/hits` route), while Loki also adds every parsed label, such as `level` from `| logfmt`; their values match Loki when parsing adds no labels. A uniform offset is applied before binary alignment, which differs from Loki only when the offset is not a multiple of the step. Wide raw-sample workloads remain subject to explicit resource limits.

The existing label sanitizer normalizes a stored `__name__` label to `_name`.
Direct-ingestion probes that require Loki's reserved metric-name identity are
therefore outside the demonstrated grouping parity. Metric `label_format`
identity rewrites also remain a separate compatibility limit. Empty `without()`
reduction is tested independently of these unsupported input transformations;
this work does not change the global label-normalization contract.

## Reproduce

Use a fresh isolated Compose project for each parity shard. Do not enable the UI generator or repeatedly ingest shared fixtures into the same volumes. Wait for Loki and the proxy to report ready.

```sh
cd test/e2e-compat
docker compose down -v && docker compose up -d && ../../scripts/ci/wait_e2e_stack.sh 180
cd ../..
go test -v -tags=e2e ./test/e2e-compat -run '^TestLogQL_Exhaustive_' -count=1
```

Set `LOKI_URL`, `PROXY_URL` and `VL_URL` only when the stack is published on non-default ports.

Unique-fixture `TestHardeningLive_` canaries are selected by the existing security regression CI script. Record failures separately from passes; HTTP 200 or a nonempty chart is insufficient proof of parity.
