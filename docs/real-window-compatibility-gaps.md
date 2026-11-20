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

<!-- verified-numbers:start -->

Verified 2026-11-20 on `7f6f98961833` (Loki 3.7.10, VictoriaLogs v1.49.0 and v1.54.0), 53 check(s) red; see the [daily summary](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/registry/generated/verification/summary.json).

- Registry: 146 open, 169 fixed, 15 documented, 7 owner-kept, 3 upstream; open by impact: 64 explore-visible, 14 drilldown-visible, 1 grafana-datasource, 21 api-only, 46 edge.
- Registry cases sent to Loki and the proxy, per VictoriaLogs version: v1.49.0: 177 match, 98 differ on equal data, 28 not comparable on that day's data, 80 blocked; v1.54.0: 180 match, 96 differ on equal data, 28 not comparable on that day's data, 79 blocked.
- Errors: 241 of 248 recorded Loki answers match, 7 known differences, 0 known leaks; 0 of 0 rows answered as recorded by the live Loki.
- Grammar sweep of 25824 queries, re-verified live by `query-corpus.yaml` on 2026-11-20 (failure): 383 VictoriaLogs rejects the translator text, 32 accepted, translation fails, 24 error text differs, 23 proxy accepts, Loki rejects, 17 tree fact differs: binary operation, 12 error text differs, rejected at translation, 10 proxy rejects, Loki accepts, 4 tree fact differs: offsets, 4 tree fact differs: outermost operation, 3 tree fact differs: has a log pipeline.
- Performance, weekly full run verified 2026-11-16 (Loki 3.7.10): 525 comparable shape x range pairs, 353 faster, 153 on par, 19 slower; 6 rows above 2x Loki.
- Performance, daily smoke verified 2026-11-20: 217 rows, 0 above 2x Loki, 4 weekly ratchet rows lost.
- Cache reuse, verified 2026-11-20: registry cases on v1.49.0: 245 repeats served from the cache, 1 not (2 VictoriaLogs calls on the repeats); registry cases on v1.54.0: 245 repeats served from the cache, 1 not (2 VictoriaLogs calls on the repeats); perf smoke: 12 of 217 rows repeat without calling VictoriaLogs.

<!-- verified-numbers:end -->

## Quality and performance vs Loki (current)

<!-- quality-summary:start -->

Tested against Loki v3.7.10, the release the compatibility stack and CI pin. Timings were measured on 2026-11-16 at commit `5b5c7965fc` on the project's A/B stack: the same data, the same hardware, the latest valid weekly run of each of 39 query-shape sets over 1h to 24h. A pair is one query shape over one time range; "comparable" means both sides answered 200 and Loki's results cache did not answer the first request. Faster and slower mean beyond 25% and 50 ms; everything else is on par. A `topk` or `bottomk` answer is compared by its ranked points and their sum, not by its series count.

Loki profile shown: `loki-defaults`, Loki with its own defaults (align_queries_with_step off, max_cache_freshness_per_query 10m); proxy builds run with their defaults. The `aligned` profile (the stack's Loki (align_queries_with_step on, max_cache_freshness_per_query 0s)) is measured too: the distribution table below shows both profiles, the rest of this section the one shown. The proxy serves Loki's read API; write endpoints such as push are not supported (ingest goes to VictoriaLogs directly). Optional deletion management is outside read API scores: its opt-in handler runs Loki's create and cancel as VictoriaLogs delete tasks, and answers Loki's 500 for the list and for queries a VictoriaLogs filter cannot select. The alerting-rules read API remains in scope, including `/api/prom/rules*` for vmalert. The deprecated read aliases `/api/prom/label`, `/api/prom/label/{name}/values`, `/api/prom/query`, `/api/prom/series` and `/api/prom/tail` are unsupported and excluded from totals, gaps and compatibility scores (owner 2026-10-31); use the corresponding `/loki/api/v1` routes. No other write endpoint is planned.

**🟢 Where the proxy is better than Loki**

- First request: **365 of 570** comparable pairs are faster (64%).
- Drilldown and Explore pages: **85 of 118** faster (72%); median first request **0.13 s** against Loki's **0.38 s**.
- Metric queries: **239 of 366** faster (65%); median first request **0.10 s** against **0.27 s**.
- Label and label-value calls: median repeat **0.01 s** against Loki's **0.05 s**, over 36 pairs (36 of them answered by Loki's results cache).
- Share of rows below Loki, first request: **64.3%** (365 of 568 rows); repeat: **24.4%** (187 of 765). The target is most queries below Loki (above 50%).

**🟡 Where it is on par**

- First request: **183 of 570** pairs are within noise (32%).
- Log queries: **39 of 79** on par; metric queries: **117 of 366**; Drilldown and Explore pages: **25 of 118**.
- Rows close to Loki (within the same noise): **183** on the first request, **568** on a repeat.
- Repeat of the same query, median p50: Drilldown and Explore pages **0.03 s** against **0.05 s**; metric queries **0.02 s** against **0.02 s**; log queries **0.01 s** against **0.08 s**.
- Answers: **719 of 785** pairs return the same result as Loki (92%).
- `sum_over_time`, `avg_over_time`, `max_over_time` and `min_over_time` over an unwrapped field answer Loki's window on `query_range` for a range longer or shorter than the step (some outer aggregations of them are still listed as open differences).
- An unwrap metric moved forward by a refresh or a re-run reads from VictoriaLogs only the buckets it has not read before.
- A zero divisor of `/` and `%` is NaN, like Loki.
- Binary vector matching raises Loki's duplicate-series error text at the first step where Loki's join fails.
- Eligible shared cache keys use owner plus one backup by default (`-peer-replication-factor=2`); RF=1 keeps only the owner. Nonowner RAM shadows are bounded to 30s and 10% of L1 capacity without extra disk copies. Three-node tests cover pooled capacity, disk refill and owner loss.

**🔴 Where it is worse**

- First request: **22 of 570** pairs are slower (4%): log queries **1 of 79**, Drilldown and Explore pages **8 of 118**, metric queries **10 of 366**.
- Between 1× and 2× Loki: **14** rows on the first request, **4** on a repeat; above 2× Loki: **6** on the first request and **6** on a repeat (the 2026-11-16 distribution).
- **2 shape, range and mode rows are on the above-2× list for the `loki-defaults` profile** (more than 2× Loki and more than 50 ms slower): **2** on the first request, **0** on a repeat, **0** on a refresh. Each is a defect being fixed: a row joins the list after two consecutive weekly runs above 2× and leaves it after two within 2×, so it can hold more or fewer rows than the day's distribution above; a change that adds or worsens a row is flagged. 29 of 110 covered registry items are locked at the 2× bound.
- **2** listed first requests are first asks (`DP1 patterns of a plain-text service` over 1h, `DP2 patterns of an environment` over 1h): the first request for a selector whose pattern state the proxy has not built yet, which Loki answers from the state its pattern ingester built at push time. They are held to an absolute bound instead of Loki's time (0.6 s, 1.6 s), and leave the list on the next weekly run; their repeats keep the 2× bound.
- Largest gaps on that list in the run shown, first request: `LS8 json count by level_extracted, collision` over 6h, **0.56 s** against **0.17 s**; `E7 json count by level_extracted` over 1h, **0.14 s** against **0.06 s**.
- Answers: **51 of 785** pairs return a different result from Loki, and **15** could not be compared.

**Distribution versus Loki in the run shown, both Loki profiles**

Rows compared: both sides answered 200 and, on a first request, Loki's results cache did not answer. Bands are those of `bench/ab/over2x.py`: beyond 25% and 50 ms, and above 2× Loki also more than 50 ms behind. The `aligned` profile runs the range-metric shapes only (the queries its setting changes).

| Loki profile | Request | Rows compared | 🟢 Below Loki | 🟡 Close to Loki | 🟠 1–2× Loki | 🔴 Above 2× Loki | Not judged: Loki not 200 / proxy not 200 / first request from Loki's cache |
|---|---|---:|---:|---:|---:|---:|---|
| `loki-defaults` | first | 568 | 365 (64.3%) | 183 (32.2%) | 14 (2.5%) | **6 (1.1%)** | 20 / 0 / 195 |
| `loki-defaults` | repeat | 765 | 187 (24.4%) | 568 (74.2%) | 4 (0.5%) | **6 (0.8%)** | 20 / 0 / — |
| `aligned` | first | 447 | 312 (69.8%) | 120 (26.8%) | 13 (2.9%) | **2 (0.4%)** | 19 / 0 / 82 |
| `aligned` | repeat | 529 | 22 (4.2%) | 500 (94.5%) | 2 (0.4%) | **5 (0.9%)** | 19 / 0 / — |

**By workload in the run shown**

First request and repeat of the comparable pairs of the `loki-defaults` run, as p50 · p90 · slowest pair: the median hides the heavy queries, the p90 and the slowest show them. The counts are of pairs, with the rule above (🟢 faster, 🟡 on par, 🔴 slower). A repeat is the recorded p50 of the repeated request, which may be a cache hit on either side.

| Workload | Pairs | 🟢 / 🟡 / 🔴 | First request: proxy | Loki | Repeat p50: proxy | Loki |
|---|---:|:---:|---:|---:|---:|---:|
| Metadata and label browser | 7 | 2 / 2 / 3 | **0.28 · 1.62 · 1.62 s** | 0.10 · 0.38 · 0.38 s | 🟢 **0.03 s** | 0.10 s |
| Drilldown and Explore pages | 118 | 85 / 25 / 8 | **0.12 · 1.06 · 6.73 s** | 0.35 · 4.96 · 9.85 s | 🟡 **0.03 s** | 0.05 s |
| Log queries | 79 | 39 / 39 / 1 | **0.02 · 0.08 · 0.24 s** | 0.08 · 0.42 · 1.58 s | 🟢 **0.01 s** | 0.08 s |
| Metric queries | 366 | 239 / 117 / 10 | **0.10 · 1.04 · 6.52 s** | 0.27 · 4.65 · 44.02 s | 🟡 **0.02 s** | 0.02 s |

**By time range, first request**

How each side grows with the range a user picks: p50 · p90 · slowest pair of each workload and range.

| Workload | Range | Pairs | 🟢 / 🟡 / 🔴 | Proxy | Loki |
|---|---|---:|:---:|---:|---:|
| Drilldown and Explore pages | 1h | 40 | 26 / 12 / 2 | **0.04 · 0.11 · 0.43 s** | 0.24 · 0.33 · 0.64 s |
| Drilldown and Explore pages | 6h | 36 | 27 / 8 / 1 | **0.16 · 0.76 · 1.09 s** | 0.93 · 1.41 · 2.82 s |
| Drilldown and Explore pages | 24h | 40 | 31 / 4 / 5 | **0.50 · 2.46 · 6.73 s** | 4.16 · 5.53 · 9.85 s |
| Log queries | 1h | 31 | 4 / 26 / 1 | **0.05 · 0.08 · 0.09 s** | 0.07 · 0.11 · 0.15 s |
| Log queries | 3h | 8 | 8 / 0 / 0 | **0.01 · 0.01 · 0.01 s** | 0.08 · 0.14 · 0.14 s |
| Log queries | 6h | 20 | 13 / 7 / 0 | **0.01 · 0.04 · 0.08 s** | 0.08 · 0.41 · 0.50 s |
| Log queries | 24h | 20 | 14 / 6 / 0 | **0.01 · 0.06 · 0.24 s** | 0.11 · 1.50 · 1.58 s |
| Metric queries | 1h | 130 | 69 / 57 / 4 | **0.03 · 0.12 · 0.36 s** | 0.13 · 0.30 · 3.98 s |
| Metric queries | 6h | 101 | 78 / 20 / 3 | **0.17 · 0.62 · 1.78 s** | 0.66 · 1.60 · 14.84 s |
| Metric queries | 24h | 99 | 83 / 13 / 3 | **0.63 · 2.49 · 6.52 s** | 3.35 · 6.55 · 31.26 s |

**The heaviest queries for Loki in the run shown**

The 10 comparable pairs Loki takes longest to answer on a first request: the queries a user waits for. The multiple is Loki's time over the proxy's, or the reverse when the proxy is slower.

| Query shape | Range | Workload | Proxy | Loki | Proxy vs Loki |
|---|---|---|---:|---:|---|
| `F1 sum by over a ratio of two per-stream metrics, 20s step` | 24h-20s | Metric queries | **4.14 s** | 44.02 s | 🟢 10.6× faster |
| `B2 sum by over a ratio of two per-stream metrics` | 24h | Metric queries | **3.81 s** | 31.26 s | 🟢 8.2× faster |
| `F3 avg by service of rate, 20s step` | 24h-20s | Metric queries | **1.82 s** | 16.32 s | 🟢 9.0× faster |
| `B2 sum by over a ratio of two per-stream metrics` | 6h | Metric queries | **1.05 s** | 14.84 s | 🟢 14.1× faster |
| `F2 count by service of count_over_time, 20s step` | 24h-20s | Metric queries | **0.01 s** ‡ | 12.39 s | 🟢 1376.4× faster |
| `F4 sum of max by service,app, 20s step` | 24h-20s | Metric queries | **4.43 s** | 12.37 s | 🟢 2.8× faster |
| `DB1 label breakdown pod` | 24h | Drilldown and Explore pages | **1.33 s** | 9.85 s | 🟢 7.4× faster |
| `SF9 volume, logfmt label_format rename` | 24h | Metric queries | **3.58 s** | 8.38 s | 🟢 2.3× faster |
| `E3 logfmt latency_ms rate, production` | 24h | Metric queries | **1.86 s** | 8.27 s | 🟢 4.4× faster |
| `logfmt volume` | 24h | Metric queries | **1.46 s** | 8.07 s | 🟢 5.5× faster |

‡ The proxy answered a first request over 6h or more in under 50 ms: probably from its own cache, warmed by an earlier set of the weekly run that sent the same query. The run records Loki's cache counters but not the proxy's, so this row is not a cold proxy.

**Where the proxy loses most in the run shown**

| Query shape | Range | Workload | Proxy | Loki | Proxy vs Loki |
|---|---|---|---:|---:|---|
| `DP1 patterns of a plain-text service` | 1h | Metadata and label browser | **0.33 s** | 0.02 s | 🔴 14.5× slower |
| `DP2 patterns of an environment` | 1h | Metadata and label browser | **1.62 s** | 0.27 s | 🔴 6.0× slower |
| `LF29 logs, drop detected_level` | 1h | Log queries | **0.08 s** | 0.01 s | 🔴 5.5× slower |
| `LS8 json count by level_extracted, collision` | 1h | Metric queries | **0.14 s** | 0.04 s | 🔴 3.4× slower |
| `LS8 json count by level_extracted, collision` | 6h | Metric queries | **0.56 s** | 0.17 s | 🔴 3.3× slower |

Label browser and Drilldown metadata calls: every pair both sides answered with 200, including the first requests Loki's results cache answered (counted in the third column; a Loki time near zero there is its cache). Medians.

| Call | Pairs | Loki cache-answered | First request: proxy | Loki | Repeat: proxy | Loki |
|---|---:|---:|---:|---:|---:|---:|
| Drilldown detected labels and fields | 15 | 15 | 🟡 **0.06 s** | 0.03 s | 🟡 **0.01 s** | 0.02 s |
| Label values | 18 | 18 | 🟡 **0.01 s** | 0.05 s | 🟡 **0.01 s** | 0.05 s |
| Labels (label browser) | 18 | 18 | 🟡 **0.00 s** | 0.01 s | 🟡 **0.01 s** | 0.01 s |
| Volume (Drilldown landing) | 9 | 2 | 🔴 **0.28 s** | 0.11 s | 🟢 **0.02 s** | 0.10 s |

**Quality of the Loki read API**

| | Now |
|---|---|
| Loki endpoints the proxy routes | **25 of 27** (93%) |
| Endpoints with tests that compare the proxy against Loki | **19 of 27** (70%) |
| Endpoints whose registry state is `proven` | **12 of 27** (44%) |
| Routed endpoints answered natively by VictoriaLogs | 14 of 25 |
| Behaviour and feature items | 11 proven, 23 partial, 3 gaps |
| Known differences from Loki (ranked) | **174**: 149 open, 15 documented, 7 owner-kept, 3 upstream |
| Open differences visible in Grafana (registry impact) | **82 of 149**: 64 Explore, 17 Logs Drilldown, 1 Grafana datasource; 21 API only, 46 edge cases |

**Readiness by client**

The Loki endpoints each client calls (registry `consumers`), with the same evidence as above, and the open differences filed as visible to that client.

| Client | Loki endpoints it calls | Routed | Compared with Loki in tests | Proven | Open differences it shows |
|---|---:|---:|---:|---:|---:|
| Grafana Explore | 10 | 10 (100%) | 10 (100%) | 8 (80%) | 64 |
| Logs Drilldown | 11 | 11 (100%) | 11 (100%) | 9 (82%) | 17 |
| Grafana Loki datasource | 6 | 6 (100%) | 6 (100%) | 4 (67%) | 1 |
| Loki API clients | 27 | 25 (93%) | 19 (70%) | 12 (44%) | 21 |

Read routes not implemented: `/loki/api/v1/cache/generation_numbers`, `/loki/api/v1/index/shards` (optional planner gap, low priority).

These are registry evidence counts, not a full runtime compatibility percentage. Optional deletion management is excluded from the read totals; deprecated aliases and write-only APIs are outside this score.

The 5 open differences with the most impact (all explore-visible): `parsed-identity/bare-parser-metric-duplicates-extracted` (bare JSON metrics cannot distinguish equal-valued metadata from ingested body fields), `semantics/parse-error-labels-on-log-lines` (A line a parser rejects carries __error__ and __error_details__ in Loki's log answer), `semantics/tail-history-start-limit` (native tail ignores start and limit: Grafana Live shows no history where Loki shows its last 100 lines), `semantics/unwrap-per-series-identity-raw-rows` (A function taken per series (avg, max, min, stddev, stdvar over time, rate_counter) under sum or sum by uses the stream as the series (open, partly fixed - avg to stdvar on query and on every range and step)), `semantics/without-fold-detected-level-unknown` (sum, max and topk without over an unwrap function after an extraction list label a series detected_level="unknown" where Loki keeps the stream's level). Differences kept on purpose are listed as documented or owner-kept.

Details, the per-workload tables and the method: [performance summary](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/performance.md#summary-versus-loki) · [performance guide](performance.md#current-position-versus-loki) · [ranked differences](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/parity-gaps.md) · [coverage map](compatibility-coverage.md) · [Known Issues](KNOWN_ISSUES.md).

<!-- quality-summary:end -->

This page records the populated-window measurement that corrected the exhaustive helper's timestamps, the fixes that followed it, and the limits found then. It is a historical record from the v1.67.0 integration: the pass counts below were measured then, and the "Remaining limits" may since have been fixed or changed; the current state is in the reports above. Versions named below are the ones measured at the time.

These findings cover populated-query regressions and resource guards following the v1.67.0 security integration. They do not establish full LogQL parity. Each correction has a focused regression; the remaining limits below define the scope of the compatibility claim.

## Timestamp defect

The exhaustive helper sent millisecond integers to Loki, which interprets integers as nanoseconds. Loki therefore queried 1970 while the proxy queried current data. RFC3339Nano timestamps now address the same populated window. A literal canary also requires both freshly ingested lines from both backends. See the [Loki timestamp contract](https://grafana.com/docs/loki/latest/reference/loki-http-api/#timestamps).

## Isolated baseline

A fresh Compose project with Loki 3.7.7, VictoriaLogs 1.50.0 and a pre-release build of these corrections, without the UI generator, produced **283/284 query checks** and **64/70 error checks**. The query failure was implicit many-to-one matching. The six error failures were four invalid IP cases and a duplicated parser-error case.

The default `test/e2e-compat/docker-compose.yml` pins Loki 3.7.9 and VictoriaLogs v1.54.0, the same Loki release as the `docker-compose.review.yml` override; `LOKI_IMAGE` selects another Loki release.

The earlier 255/284 query result came from a long-running UI generator stack with repeated ingestion. Its reference timeouts and empty-result findings were not isolated reproductions. A later deterministic test reproduced the regexp capture/filter failure after the stored-field inventory was warmed: a query-created `http_method` capture was incorrectly rewritten to a stored `http.method` field. Query-local capture names now remain independent of that inventory.

The exhaustive checks compare status, result type and non-emptiness. Strict quantile canaries demonstrated wrong grouping and values even when those checks passed. They are insufficient proof of labels, samples or timestamps.

## Corrections

- IP line filters reject invalid single addresses, prefixes, ranges and unsupported operators, and IPv4 ones keep exactly Loki's lines. Substring filters preserve literal regex metacharacters, including text resembling `ip(...)`.
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

Sources: the answers of a live Loki to the canary checks, and [VictoriaLogs JSON unpacking](https://github.com/VictoriaMetrics/VictoriaLogs/blob/v1.50.0/lib/logstorage/pipe_unpack_json.go).

## Resource and operational findings

Raw metric collectors request one extra row and reject overflow rather than returning a successful partial calculation. A final LogsQL limit pipe avoids VictoriaLogs' implicit timestamp sort from the HTTP `limit` argument; complete samples are sorted locally. See the [VictoriaLogs query contract](https://docs.victoriametrics.com/victorialogs/querying/#querying-logs).

| Guard | Setting | Client-visible result |
| --- | --- | --- |
| Raw rows per manual metric call | `-manual-range-metric-row-limit` (default 1,000,000) | HTTP 400, Loki's `limit reached while evaluating the query`; the log line names the flag |
| Metric query series | `-max-stats-query-series` (default 1000) | HTTP 400, Loki's `maximum number of series (N) reached for a single query`; Logs Drilldown gets a partial result with the `... returning partial results` warning |

Every metric path applies the series limit the same way: raw and ordered-parser evaluators, stats and hits buckets, sliding windows, binary expressions and unions, multi-tenant merges, range and instant. Logs Drilldown's partial result holds the busiest series by total value, where Loki keeps the first series it encounters. Logs Drilldown label and field breakdowns take the same path: every series up to the limit with Loki's values, and over it the busiest series, ranked by VictoriaLogs in a bounded second call, with Loki's warning.

Binary evaluation limits nesting to 64 and child evaluations to 1,024. It shares budgets across children: 256 MiB of captured response data, two million decoded arrays, one million constructed output samples and 64 MiB of label-processing work. Individual encoded results are capped at 64 MiB. These are conservative work limits: repeated labels and nested intermediate results consume budget, so a valid large expression can now fail explicitly.

The release UI pass also found a local Compose configuration mismatch: a 1 GiB VL cap retained settings intended for the repository's 5 GiB profile. Wide Fields queries OOM-killed VL, after which the dropdown displayed no options. Restoring the repository memory/restart settings restored all seven label ranges. Independently, a broad bare-parser unwrap query exposed excessive proxy output and required resource guards on that separate path. A backend failure can cause the two ingestion targets to diverge; comparisons after that failure require a fresh paired fixture.

## Remaining limits

Label `ip()` filters and exact matching of IPv6 line filters need further compatibility work. Regexp capture collisions with existing labels and learned aliases when only an alternate stored spelling exists remain separate limits. The grouped quantile canary uses a shared aligned evaluation axis; arbitrary frontend range alignment is not established by it. Native tumbling results are on the step-aligned grid, Loki's grid now that metric `query_range` bounds are aligned by default (with `-align-queries-with-step=false` they stay on it where Loki with its splitting off evaluates at `start+k*step`), and a bucket boundary is inclusive at its start where Loki's window is inclusive at its end. Grouped tumbling `count_over_time` and `bytes_over_time`, served by the Drilldown hits and hybrid paths, are not relabelled, so their labels mark bucket starts while ungrouped sums use Loki's evaluation timestamps. Bare parser range aggregations served from buckets label each series with its stream labels (or the declared fields on the `/hits` route), while Loki also adds every parsed label, such as `level` from `| logfmt`; their values match Loki when parsing adds no labels. A uniform offset is applied before binary alignment, which differs from Loki only when the offset is not a multiple of the step. Wide raw-sample workloads remain subject to explicit resource limits.

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
