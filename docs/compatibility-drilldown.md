---
sidebar_label: Logs Drilldown Compatibility
description: Grafana Logs Drilldown plugin compatibility — patterns, detected_fields, label filters, and metric queries.
---

# Logs Drilldown Compatibility

This track measures compatibility with the Grafana Logs Drilldown app, not generic Loki clients.

## Scope

- Grafana datasource resource endpoints consumed by the app
- Service selection, service-detail log volume, fields, labels, and field values
- Log frame expectations that affect labels, level coloring, and field visibility

## CI And Score

- Workflow: `compat-drilldown.yaml`
- Score test: `TestDrilldownTrackScore`
- Runtime coverage: pinned Grafana runtime plus current-family and previous-family Grafana smoke on PRs, with the fuller Grafana matrix kept for scheduled/manual runs
- Version matrix: source-contract checks across the current Drilldown family and one family behind

The Drilldown matrix is also a moving window. We support the current app family and one family behind, with the contract list sliding forward as upstream releases move. We do not keep an open-ended tail of older app families.

## Version Matrix

### Grafana runtime profiles

| Grafana version | Coverage path | Version-specific focus |
|---|---|---|
| `13.2.1` | PR/main pinned runtime + scheduled/manual runtime matrix | Full Drilldown runtime score; current pinned build; React 19 |
| `12.4.10` | PR/main previous-family smoke + scheduled/manual runtime matrix | datasource catalog, base Drilldown resource contracts, explicit `2.x` runtime-family assertions |
| `12.4.1` | Scheduled and manual runtime matrix | datasource catalog, base Drilldown resource contracts |
| `11.6.6` | Scheduled and manual runtime matrix | datasource catalog, base Drilldown resource contracts, explicit `1.x` runtime-family assertions |

### Logs Drilldown app versions

| Logs Drilldown version | Coverage path | Version-specific focus |
|---|---|---|
| `2.5.2` | PR/main pinned runtime + scheduled/manual contract matrix | Current pinned contract; label/field filter inputs located by placeholder; patterns tab requires patterns-autodetect as Grafana default |
| `2.2.0`–`2.5.1` | Scheduled and manual contract matrix | Mixed parser expression in `MIXED_FORMAT_EXPR` |
| `2.1.0`–`2.1.5` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `2.0.4` | Scheduled and manual contract matrix | Previous pinned contract; patterns tab requires patterns-autodetect as Grafana default |
| `2.0.3` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels, patterns |
| `2.0.2` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `2.0.1` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `2.0.0` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `1.0.41` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.40` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.39` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.38` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.37` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.36` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.35` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.34` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |

## Runtime Detection And Version Coupling

Proxy-side Drilldown detection is based on deterministic request signals:

- `X-Query-Tags: Source=grafana-lokiexplore-app` identifies Drilldown-origin resource calls
- `User-Agent: Grafana/<version>` provides Grafana runtime version family

Important limit:

- exact Drilldown app semver is not emitted on the datasource HTTP request path by default

Because of that, version-specific behavior should be gated by:

1. explicit request source tag,
2. Grafana runtime family (`12.x`, `13.x`),
3. compatibility matrix contract version bands (`1.0.x`, `2.x`), validated in CI.

## Label And Field Breakdowns

Grafana Logs Drilldown draws its label and field breakdowns with one metric
range query per label or field, for example

```
sum(count_over_time({env="production" ,pod != ""} [5m])) by (pod)
sum by (user_id) (count_over_time({env="production"} | json user_id="[\"user_id\"]" | drop __error__, __error_details__ | user_id!="" [5m]))
```

with `step` equal to the range (`$__auto`). The plugin neither caps nor
samples the series it receives; it sorts them. Loki answers these queries like
any other: every series with exact values, and above `max_query_series` a
partial result with the warning `maximum number of series (N) reached for a
single query; returning partial results` for Drilldown and a `400` for every
other client (`JoinSampleVector` in `pkg/logql/engine.go` and the
`seriesLimiter` in `pkg/querier/queryrange/limits.go`, v3.7.7).

The proxy answers them the same way, from VictoriaLogs `stats_query_range`
buckets relabelled onto Loki's evaluation timestamps (the sample at `t` covers `(t-range, t]`):

- Every series up to the tenant's effective `max_query_series`
  (`-tenant-limits` → `-tenant-default-limits` → `-max-stats-query-series` →
  Loki's default of 500), with the same values Loki returns.
- Under the limit that is one `stats_query_range` call, for Drilldown and
  every other client alike.
- Every other client gets Loki's `400` as soon as the response holds more
  series than the limit.
- A Drilldown single-field breakdown over the limit is asked again with an
  `in()` subquery that ranks the field's values by line count in the request's
  time range and keeps the `limit + 1` busiest. VictoriaLogs reads the lines
  twice for that call (the ranking and the buckets), but its response holds at
  most `limit + 1` series however many values the field has; the proxy
  remembers the breakdown for five minutes and ranks straight away. Drilldown
  gets the `limit` busiest series and Loki's warning. Loki keeps the first
  series it meets instead of the busiest, so for series of equal volume the
  kept set can differ; each kept series is exact. A breakdown that cannot be
  ranked this way (the underscore label style's dotted fields, which group by
  both spellings) is read whole and capped the same way.
- Drilldown breakdowns share `-stats-query-range-concurrency` slots; a slot is
  free again after `-stats-query-range-inter-query-delay-ms`, which does not
  delay the answer.
- A response larger than `-backend-max-buffered-response-bytes` returns `502`
  naming that flag; nothing is truncated or sampled silently.

Sliding range metrics (`count_over_time`, `rate`, `bytes_over_time`, `bytes_rate` with a
range different from the step, plus `topk`/`bottomk` over them) follow Loki for
every client, including Drilldown-tagged requests: a step whose window
`(t-range, t]` holds no log line is absent, not `0`. Each window is summed from
`stats_query_range` buckets of `gcd(step, range)` whose edges are anchored to
the request start with the `offset` argument (VictoriaLogs v1.45+) and shifted
by one nanosecond, so a line on a window edge counts where Loki counts it. The
bucket count has no budget because VictoriaLogs returns only non-empty buckets.
The raw-sample evaluator answers with the same `(t-range, t]` boundaries when a
bucket would be below 1 ms, when the stats response exceeds its byte limit, or
when an unaligned grid meets a backend known to be older than v1.45 (an
undetected version keeps the bucket path: such a backend ignores the `offset`
argument at worst). On such older backends an
epoch-aligned grid still uses buckets without the one-nanosecond shift, so a
line exactly on a window edge counts in the neighbouring window.

Routing does not depend on the client: Explore, Drilldown, dashboard panels
and direct API clients reach the same exact stats path. Only the answer above
the series limit differs, as it does in Loki.

When VictoriaLogs fails the direct `stats_query_range` call for a Grafana-sourced
request, the proxy mirrors Loki's partial-results behaviour: HTTP 200 with
`warnings`, a `Warning` header and `X-Proxy-Upstream-Status` /
`X-Proxy-Upstream-Error` for operators. Non-Grafana clients receive the upstream
error status.

## Long-Range Histograms And Grafana querySplitting

For ranges ≥ 24h, Grafana's Loki datasource splits every metric range query into
24h chunks before sending them to the proxy. The split logic lives in
`public/app/plugins/datasource/loki/metricTimeSplitting.ts -> splitTimeRange()` and
fires for any client built on the Loki datasource — Drilldown, Explore, and
dashboard panels. The proxy must produce a chunk response shape that Grafana's
in-browser `mergeFrames + closestIdx + splice` algorithm
(`public/app/plugins/datasource/loki/mergeResponses.ts`) can glue back into a
coherent timeline. This section pins the contract.

### What Grafana sends

`splitTimeRange(start, end, step, oneDayMs)` produces:

1. `floor(range / aligned_day)` chunks of `aligned_day - step` length (aligned to step boundary).
2. One residual chunk of `(range mod aligned_day)` length, which is often **smaller than `step`** (e.g. for an exactly-24h request the residual is 0–120 s wide when step is 120 s; for 25 h it is ~1 h).

Chunks are dispatched **newest-first** because `runSplitGroupedQueries` recurses with `partition[totalRequests - 1]` first.

### What the proxy must return per chunk

Each chunk sub-request must produce a Loki matrix with:

- Samples on Loki's evaluation timestamps inside the chunk's `start..end`
  window, and nothing outside it.
- The same series Loki returns for the chunk. **One-bucket series, where every
  value lands on the same timestamp, are stacked by `mergeFrames` into a tall
  edge spike** — the 2026-06 incident root cause.

### How the proxy enforces this

- The proxy returns an **empty Loki matrix** (with response header
  `X-Proxy-Drilldown-Path: hits-leftover-suppressed`) when the request is
  Drilldown-tagged (`X-Query-Tags: Source=grafana-lokiexplore-app`) AND
  `end - start < step` (`isQuerySplitResidual`). The check runs at the
  `query_range` entry for metric expressions (range/vector aggregations,
  binary, opaque metric and literal expressions; log queries are never
  blanked) and again at the stats entry. The residual chunk from
  `splitTimeRange` falls into this case and the chart loses less than one step
  of width at the edge instead of showing a spike.
- Explore, dashboard panels and other Grafana clients are not suppressed. They
  rely on per-chunk axis trimming, which keeps every response inside the
  chunk's `start..end` window, so their merged frames stay spike-free without
  an empty chunk.

### Acceptable irreducible bump (25h–47h cases)

For ranges like 25h the proxy cannot safely suppress the 1h leftover chunk
because users genuinely query 1h ranges in Drilldown / Explore. The merged
frame can therefore show a modest right-edge bump (≤ 65% of nonzero buckets
fall into the rightmost bin) from the leftover chunk's own series.
This is the price of preserving short-range queries; e2e thresholds reflect
the trade-off and document it explicitly.

### Do Not Regress

The following invariants are pinned by `internal/proxy/drilldown_regression_lock_test.go`
and `test/e2e-compat/drilldown_chunked_merge_lock_test.go`. Each is named
`TestLock_*` / `TestE2ELock_*` and breaking any of them fails CI:

1. Drilldown label and field breakdowns hold every series up to the tenant's
   series limit with Loki's values, from one `stats_query_range` call and no
   `/hits` call; above the limit Drilldown gets the busiest series with Loki's
   warning and other clients get Loki's `400`
   (`internal/proxy/drilldown_breakdown_exact_test.go`).
2. Residual chunks (`end - start < step`) from Drilldown-tagged requests are
   suppressed and the response carries `X-Proxy-Drilldown-Path: hits-leftover-suppressed`;
   other Grafana and non-Grafana callers with the same shape are served normally.
3. Grafana mergeFrames simulation on the live VL stack must produce a merged
   frame whose rightmost bin holds < 65 % of nonzero timestamps for 24h, 25h,
   2d, and 7d ranges, for both Drilldown and dashboard sources.

## Drilldown Capability Profiles

| Drilldown version family | Capability profile | Proxy handling focus |
|---|---|---|
| `2.x` | `drilldown-v2` | detected-level defaults, modern service-detail scenes, patterns and field-value drill flows |
| `1.0.x` | `drilldown-v1` | legacy service buckets, filtered detected-fields path, prior labels/field rendering behavior |

These profiles are matrix-level compatibility profiles (contract and CI guidance). Runtime request handling must stay Loki-compatible and should not depend on guessed app build strings.

## Known Issues

### Drilldown 2.0.4 through 2.5.2: Patterns Tab Initialization

Drilldown 2.0.4 through 2.5.2 contain a bug in `ServiceScene.subscribeToLokiConfig()` where `void 0 === null` (always `false`) prevents re-enabling the Patterns tab after it was disabled. Concretely:

- If the Grafana default datasource has `pattern_ingester_enabled=false` in `/loki/api/v1/drilldown-limits`, `$patternsData` is set to `null`.
- Switching to a datasource where `pattern_ingester_enabled=true` does NOT re-show the tab because the `void 0 === null` guard treats `null` as "already initialized".

**Workaround**: Configure the patterns-autodetect proxy variant as the Grafana default datasource. Since `pattern_ingester_enabled=true` is returned on first load, `$patternsData` is correctly initialized and the Patterns tab appears.

In the e2e-compat compose stack, `loki-vl-proxy-patterns-autodetect` is set as `isDefault: true` in `grafana-datasources.yaml` for this reason.

## Release Watchlist

Potential next family move:

- current: `2.x` (pinned: `2.5.2`)
- next expected family to evaluate: `3.0.x` when released

Promotion criteria for a new family:

1. add versions to matrix manifest,
2. verify `TestDrilldownTrackScore` and `TestDrilldown_RuntimeFamilyContracts` on pinned + smoke runtimes,
3. confirm no regressions in patterns, labels/fields, and service detail flows.

## Contracts We Enforce

- `index/volume` must expose real `service_name` buckets
- `index/volume_range` must expose non-empty `detected_level` series names
- `detected_fields` must show parsed fields like `method`, `path`, `status`, `duration_ms`
- `detected_fields` must not leak indexed labels like `app`, `cluster`, or `namespace`
- `detected_fields` must suppress high-cardinality terminal timestamp fields (`timestamp_end`, `observed_timestamp_end`) so Drilldown field discovery does not trigger expensive backend stats paths that can flap into intermittent no-data responses
- In hybrid field mode, `detected_fields` may expose both native dotted fields and translated aliases such as `service.name` and `service_name`
- `labels` and `label/{name}/values` should stay stream-shaped; they should prefer VictoriaLogs stream metadata endpoints and only fall back to generic field endpoints for older backend versions
- `detected_fields`, `detected_labels`, and `detected_field/{name}/values` should prefer native VictoriaLogs metadata lookups where they map cleanly, then fall back to bounded raw-log sampling for parsed and derived fields
- Alias resolution must keep exact native matches working, allow unique translated aliases to resolve automatically, and avoid silently choosing the wrong native field when multiple dotted names collapse to the same Loki-safe alias
- Label-value resources for additional filters such as `cluster` must return real values
- Unknown label and detected-field lookups should keep a success payload shape instead of flipping into transport errors
- `patterns` must return non-empty grouped pattern payloads with sample buckets for Drilldown
- Multi-tenant Drilldown queries with repeated `var-levels=detected_level|=|...` selections must stay valid and return logs instead of backend parse errors
- _(v1.17.1)_ When `detected_level` is synthesized in metric results, the raw `level` label is removed from those same results to prevent Drilldown from showing both labels simultaneously — the include button for `detected_level` values must work correctly without `level` duplication
- _(v1.17.1)_ Nested JSON objects in the log body (e.g., `service={"name":"api-gateway"}`) must be excluded from the `detected_fields` field breakdown; exposing them previously broke the field breakdown view when users clicked on such a field

## Edge Cases Covered

- Mixed parser query path: `| json ... | logfmt | drop __error__, __error_details__`
- Labels object parsing in returned log frames
- App-level field suppression for `detected_level`, `level`, and `level_extracted`
- High-cardinality terminal timestamp keys (`timestamp_end`, `observed_timestamp_end`) are excluded from Drilldown detected-field responses while regular parsed fields stay visible
- `1.x` service-selection buckets, detected-fields filtering, and labels field parsing stay explicit in the source-contract checks
- `2.x` detected-level default columns, field-values breakdown scenes, and additional label-tab wiring stay explicit in the source-contract checks
- Grafana runtime `11.x` explicitly asserts `1.x`-style service buckets, filtered detected fields, and extra label values at runtime
- Grafana runtime `12.x` explicitly asserts `2.x`-style detected-level series, field-value breakdowns, and extra label values at runtime
- Grafana runtime `13.x` uses the same `2.x`-style contract as `12.x` — added to `RuntimeFamilyContracts` in v1.15.0
- Service-detail field breakdowns and additional label filters
- Multi-tenant Drilldown log views filtered by `cluster` plus multiple selected `detected_level` values
- Multi-tenant Grafana resource calls with `__tenant_id__!~...` and `__tenant_id__="missing"` keep the correct narrowed or empty-success behavior
- Native field-value discovery for indexed metadata such as `service.name`, with parser-stage stripping before the backend lookup
- Fallback scanning for parsed-only fields such as `method` when no safe native metadata path exists
- Patterns grouping across repeated request shapes
- _(v1.17.1)_ `detected_level`/`level` metric deduplication: raw `level` label removed from metric results when `detected_level` is synthesized, fixing the Drilldown include button for `detected_level` filter selections
- _(v1.17.1)_ Nested JSON object field suppression: `service={"name":"..."}` and similar compound body fields are excluded from the field breakdown to prevent broken Drilldown field-click behavior