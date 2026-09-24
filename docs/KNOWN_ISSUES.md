---
sidebar_label: Known Issues
description: Known differences between loki-vl-proxy and native Loki, and workarounds where available.
---

# Known Differences and Known Issues

Last updated against `main`.

This project is a Loki-compatible read proxy for VictoriaLogs. It is not a claim
that VictoriaLogs is natively Loki or that every Loki behavior is reproduced in
the backend itself. This page tracks the differences, scope boundaries, and
operational caveats that still matter in the current codebase.

## Intentional Scope Boundaries

| Area | Current state |
|---|---|
| Write path | `POST /loki/api/v1/push` stays blocked. The proxy is read-focused. Log ingestion should go directly to VictoriaLogs-side ingestion paths. |
| Delete path | Not supported. `POST /loki/api/v1/delete` is registered and checks a confirmation header, time range, tenant scope and audit logging, but it forwards to `/select/logsql/delete`, which VictoriaLogs rejects as an unsupported path. VictoriaLogs deletion uses the asynchronous `/delete/run_task` API, which the proxy does not implement. See [Security hardening migration](security-hardening-migration.md#remaining-delete-api-gap). |
| Rules and alerts lifecycle | Read compatibility is exposed through Loki YAML and Prometheus-style JSON views when `-ruler-backend` / `-alerts-backend` is configured. Rule writes and alert lifecycle changes remain outside the proxy. |
| Browser-origin tailing | `/loki/api/v1/tail` rejects browser `Origin` headers unless allowlisted with `-tail.allowed-origins`. |
| Multi-tenant tailing | Tail remains intentionally single-tenant. Loki-style multi-tenant tail fanout is not supported there. |

## Current Behavioral Differences

| Area | What to expect |
|---|---|
| Label vs field surfaces | With `-label-style=underscores` and `-metadata-field-mode=hybrid`, label APIs remain Loki-safe (`service_name`) while field-oriented APIs can expose both `service.name` and `service_name`. This is expected compatibility behavior, not duplicate data corruption. |
| Grafana dotted-field builder UX | Grafana builder paths can still tokenize dotted field names awkwardly even when the generated query executes correctly. For click-to-filter flows, underscore aliases are the safer UI path. |
| Parsed-only field freshness | `detected_fields` and `detected_field/{name}/values` prefer native VictoriaLogs metadata when possible, but parsed-only or very new fields can still fall back to bounded sampling. That means freshness can differ from indexed metadata. |
| Multi-tenant Drilldown aggregation | Some Drilldown-oriented field and label surfaces still use approximate merged cardinality across tenants. Query fanout works, but merged browse surfaces are not perfect set-theory replicas of native Loki multitenancy. |
| Wildcard tenant shorthand | `X-Scope-OrgID: *` is not a Loki-compatible all-tenants shorthand. An unmapped `*` returns HTTP 403 in both native and label-routing tenant modes unless `-tenant.allow-global=true` is set; an explicit tenant-map entry for `*` still takes precedence. |
| Metric series of `| json` / `| logfmt` pipelines | Loki names a metric series with the stream labels plus every label the pipeline extracted, so `count_over_time({app="x"} \| logfmt [1m])` returns one series per distinct set of parsed values (450 on a 30-stream minute of the e2e generator, 1050 for `\| json` on a busier app) where the proxy returns one per stream (30). The key set of those two parsers is known only once a line is read, so matching Loki means evaluating the query from rows instead of pushing it down to VictoriaLogs stats, which the proxy does not do by default on wide ranges. `-exact-parser-series-identity=true` opts in per deployment. `| regexp` and `| pattern` name their captures in the query, so those labels are always part of the series identity and are grouped in VictoriaLogs. |
| Patterns surface | `/loki/api/v1/patterns` is optional (`-patterns-enabled`) and responses are clamped to `1000` patterns per request. |
| `count_values()` aggregation | Not translatable. VictoriaLogs has no equivalent function that groups by metric values. Queries using `count_values` return HTTP 400 (`bad_data`). |
| Implicit many-to-one binary matching | Vector-vector operations where several series on one side match one series on the other without `group_left`/`group_right` are rejected with HTTP 500 and Loki's `multiple matches for labels` error. Cardinality is checked independently at each timestamp. |
| Data-dependent pipeline errors | Loki reports some invalid pipeline stages only when it builds the pipeline for a time range it actually queries: invalid `\| json`/`\| logfmt` extraction expressions and `label_format`/`line_format` templates answer HTTP 400 over a recent range but HTTP 200 with an empty result over a range with no data to read. The proxy rejects them with Loki's 400 message regardless of the range. Parse-time errors (`\| pattern`, `\| regexp`, selectors, subqueries, `topk(0, …)`) are 400 on both. |
| `ip()` line filter validation | Invalid addresses, prefixes, ranges and operators other than `\|=`/`!=` are rejected when the query is parsed. Loki can skip building an invalid pipeline for an empty historical range; the proxy rejects the expression regardless of data. |
| `ip()` matching precision | The filter is translated to VictoriaLogs regular expressions. Exact matching for all IPv6, non-octet CIDR and range forms is not established, and label `ip()` filters need further compatibility work. |
| Grafana-sourced stats errors | When VictoriaLogs fails a `stats_query_range` request from Grafana (Drilldown, Explore or dashboards), the proxy returns HTTP 200 with Loki-style `warnings` and a `Warning` header instead of the upstream error. Non-Grafana clients receive the error status. |
| Grafana query-split residual chunk | For Drilldown-tagged metric range requests shorter than one step (the trailing chunk of Grafana's 24h query splitting), the proxy returns an empty matrix with `X-Proxy-Drilldown-Path: hits-leftover-suppressed`. |
| Drilldown breakdown over the series limit | Above the tenant's `max_query_series` a Logs Drilldown label or field breakdown gets that many series with Loki's partial-result warning, as in Loki, but the proxy keeps the busiest series (ranked by VictoriaLogs) where Loki keeps the first series it meets. For series of equal volume the kept set can differ from Loki's; every kept series has Loki's values. |
| Volume bytes and structured metadata | `/index/volume` and `/index/volume_range` report bytes like Loki, but the proxy counts exact log line bytes (VictoriaLogs `sum_len(_msg)`). Loki's volume is its chunk size estimate, which also counts structured metadata: 8 bytes for each structured-metadata name/value pair on every line (one `detected_level` pair per line when `discover_log_levels` is on) plus the metadata names and values once per chunk. Loki also splits a chunk's size across buckets by the share of the chunk's time span that falls into each bucket. For data still in the ingester, expect Loki to be higher by about 8 bytes per line per metadata pair, give or take about one line per stream per bucket. Once chunks are flushed, Loki rounds each chunk to whole KiB and adds a chunk once for every TSDB index file that lists it, so Loki can report a multiple of the stored bytes until its index files are compacted. Labels, timestamps, result type and ordering match Loki for single-tenant requests. |
| Volume extensions and multi-tenant volume | `targetLabels=detected_level` returns per-level volumes from the log lines (lines without a level form a `detected_level=""` volume); Loki keeps `detected_level` in structured metadata and returns no volume for it. Multi-tenant volume requests return each tenant's volumes with a `__tenant_id__` label, up to `limit` per tenant, in tenant order; Loki sums volumes of the same name across tenants, applies `limit` once and orders by volume. |
| Metadata default lookback | `/labels`, `/label/{name}/values` and `/series` requests without `start`/`end` are bounded to the last 12h (`-metadata-default-lookback`; `0` disables). |
| Log stream `detected_level` | Log query responses, tail, `/patterns` and `/detected_fields` derive `detected_level` from the stored row with Loki's rules (stored `detected_level`, stream and other level fields, `severity_number`, then the JSON, logfmt and keyword reading of the line, `unknown` otherwise), in the stream labels by default and as structured metadata with `categorize-labels`. Remaining differences come from what VictoriaLogs stores: a JSON line that VictoriaLogs unpacked into fields (Loki push API without `disable_message_parsing=1`) has lost its key order and `null` values, so a nested level key listed before a top-level one, or a keyword that appeared only as a `null` key, can resolve differently. Rows written with a JSON key as the message field (`_msg_field`) keep their other keys as stored fields, so an unknown level word such as `notice` is kept where Loki would scan the line. A level field created at query time by `\| json`, `\| logfmt` or `\| label_format` is read like a stored field, and `\| drop` or `\| keep` stages that remove stored level fields change the derived value, while Loki's is fixed at ingest. With the default encoding only `level` and `detected_level` join the stream labels, not other structured metadata. An OTLP exporter that sends a `severity_text` identical to VictoriaLogs' form of the number (for example `Info2`) gets the level of the number. A stream selector matcher on `detected_level` treats the label as absent, as Loki does for structured metadata, unless `-stream-fields` lists `detected_level`; a `detected_level` label pushed as a Loki stream label is an index label in Loki, so list it there to select on it. `by (detected_level)` metric grouping derives the same value, computed by VictoriaLogs over the same stats query, so a grouped metric query and a log response agree. `\| detected_level` filters and volume by `detected_level` still do not read the line: they use the stored `detected_level` or `level` field. |
| `unwrap` conversion errors | When `\| unwrap` meets a value that is not a number (for example `sum_over_time({app="a"} \| logfmt \| unwrap msg [5m])` over text), Loki fails the whole query with HTTP 400 `pipeline error: 'SampleExtractionErr' for series: ...` unless the query drops those samples with `\| __error__=""`. VictoriaLogs skips non-numeric values in its stats functions, so the proxy returns HTTP 200 with the numeric samples only (often an empty result). Reproducing Loki's data-dependent error would need an extra scan of every unwrapped field per window. Add `\| __error__=""` to get the same result from both. |
| OTel attribute translation in upstream queries | By default (`-translate-otel-attributes=true`), the LogQL→LogsQL translator rewrites known OTel semantic convention labels from underscore to dotted form (e.g., `k8s_container_name` → `k8s.container.name`). Deployments that store these fields with underscores (Vector, Promtail, Fluent-bit via Elasticsearch bulk ingest) should set `-translate-otel-attributes=false`. |

## Translation and Performance Caveats

Some compatibility behavior is implemented in the proxy rather than delegated to
native VictoriaLogs primitives. That keeps the Loki-facing contract usable, but
it also means latency, CPU cost, and observability differ from a native Loki
backend or a pure VictoriaLogs query path.

This especially matters for:

- parser and filter compatibility stages
- some response shaping and label/field alias resolution
- parts of binary compatibility behavior
- formatting helpers such as `line_format` / `label_format`
- unwrap helper compatibility such as duration and byte parsing

Treat those paths as supported compatibility work, not as zero-cost backend
equivalents.

### Hot+Cold response merging

When both hot (VictoriaLogs) and cold (Victoria Lakehouse) backends return results,
the proxy merges them in a streaming fashion. Backward-direction queries use a
bounded ring buffer (`maxRingSize=5000` entries) for the reverse pass rather than
materializing the full cold response. Very large time ranges hitting both backends
will still see higher proxy memory usage than hot-only queries, but the ring buffer
caps the worst case.

## Operational Caveats

| Area | Current state |
|---|---|
| Patterns persistence | If `-patterns-persist-path` is configured and not writable, startup fails fast. Without persistence, the endpoint still works, but warm state is lost on restart. |
| Label-values persistence | If `-label-values-index-persist-path` is configured and not writable, startup fails fast. Without persistence, indexed browse state is rebuilt after restart. |
| Startup warm readiness | When patterns or label-values startup warm is configured, readiness can remain `503` until disk restore or peer warm completes. |
| Older VictoriaLogs metadata paths | Newer VictoriaLogs versions let the proxy prefer stream-only metadata APIs. Older versions may fall back to broader field APIs, which can change how strictly stream-shaped some browse endpoints feel. |
| Large body fields | Very large body fields can still be dropped on the VictoriaLogs side. Track the upstream issue: [VictoriaLogs issue #91](https://github.com/VictoriaMetrics/victorialogs-datasource/issues/91). |
| Optional tenant header | By default the proxy accepts requests without `X-Scope-OrgID` and routes them to the default tenant. Use `-require-tenant-header` (or `-auth.enabled`) to reject requests that omit the header with HTTP 401. |
| Execution limits | Bounded work limits reject oversized queries instead of truncating them: raw metric scans beyond `-manual-range-metric-row-limit` (default 1,000,000 rows) return an explicit error (HTTP 502 on the manual range-metric path) instead of a partial result; binary expressions are limited to 64 nesting levels, 1,024 child evaluations and shared memory/sample budgets; `line_format` to 64 KiB per line and 16 MiB per response (HTTP 400). See [Security hardening migration](security-hardening-migration.md#execution-and-storage-limits). A rejected query does not mean the logs are absent. |
| Metric series caps | A metric query above `-max-stats-query-series` (default 500, Loki's own default `max_query_series`) fails with Loki's HTTP 400 `maximum number of series (N) reached for a single query` on every path, range and instant. Grafana Logs Drilldown receives a partial result with Loki's `... returning partial results` warning; it holds the busiest series by total count, where Loki keeps the first series it encounters. |
| Multi-tenant fanout concurrency | When `X-Scope-OrgID` contains multiple tenants, the proxy fans out sub-requests in parallel (goroutine per tenant). Latency equals the slowest tenant, not the sum. Very high fan-out (10+ tenants) may increase backend load proportionally. |


## What Is No Longer an Open Gap

These are not current open issues in this codebase:

- read-path query and label fanout across multiple tenants
- Grafana Logs Drilldown contract coverage as a tracked compatibility product
- Loki-compatible `/loki/api/v1/patterns` support with persistence and peer warm
- route-aware proxy, cache, and upstream request telemetry
- prefixed app metrics under `loki_vl_proxy_*` with CI guard coverage
- `label_replace()` — fully implemented in translator with proxy-side post-processing (v1.21.0)
- `label_join()` — fully implemented in translator with proxy-side post-processing (v1.21.0)
- `group()` — implemented: inner metric translated normally, proxy normalises all matrix values to `1` (v1.21.0)
- bare label matcher malformed VL output — queries like `app="value"` (missing braces) now return a descriptive HTTP 400 instead of silently producing double-quoted VL syntax (v1.20.0)
- `detected_level` on log responses — derived per row from stored fields and the line with Loki's rules, including `unknown`, normalised values and structured metadata placement under `categorize-labels` (Unreleased; replaces the v1.20.0 JSON/logfmt inference)
- circuit breaker sliding window — failure counting uses a 30-second sliding window; sporadic slow-query resets no longer open the breaker (v1.18.0)
- deterministic log stream ordering for multi-window queries — streams and per-stream values now sorted stably before response emission (v1.21.1)
- `offset` directive — fully implemented: proxy strips the offset clause and shifts `start`/`end` (or `time` for instant queries) backward by the offset duration before backend dispatch
- `| drop field=value` matcher semantics — proxy now conditionally removes a field only when its value matches, via proxy-side post-processing (`ParseDropConditions` + `applyDropConditions`); previously the value predicate was silently ignored and the field was always dropped (v1.36.1)
- structuredMetadata vs parsedFields classification — proxy correctly classifies structured metadata fields by comparing against `_msg` JSON content; previously some structured metadata fields were misclassified as parsed fields (v1.36.0)
- `| keep field=value` matcher form on stream labels — proxy now applies keep conditions to stream labels (not just structured metadata / parsed fields); mirrors the existing `| drop field=value` stream label path (v1.51.0)
- Parallel multi-tenant fanout — sub-requests dispatched via goroutine-per-tenant with `sync.WaitGroup`; latency equals slowest tenant, not sum (v1.37.1)
- Streaming backward hot+cold merge — cold reverse pass uses bounded ring buffer (`maxRingSize=5000`) with early termination instead of full body buffering (v1.37.1)
- `absent_over_time()` — fully implemented (v1.35.0); translates to `stats count()` with empty-series emission
- `sort` / `sort_desc` outer aggregations — fixed in v1.35.0; sort by metric value across series now works correctly
- Cold storage backend routing (Victoria Lakehouse) — implemented v1.28.0; time-boundary split between hot VL and cold Lakehouse

## Related Docs

- [Real-window compatibility findings](real-window-compatibility-gaps.md) — measured parity results and remaining LogQL limits
- [Compatibility Matrix](compatibility-matrix.md)
- [Loki Compatibility](compatibility-loki.md)
- [Logs Drilldown Compatibility](compatibility-drilldown.md)
- [VictoriaLogs Compatibility](compatibility-victorialogs.md)
- [Translation Modes](translation-modes.md)
- [API Reference](api-reference.md)
