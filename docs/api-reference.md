---
sidebar_label: API Reference
description: Routed Loki read APIs, evidence coverage, unsupported routes and request/response formats.
# CommonMark supports the generated HTML comment markers.
mdx:
  format: md
---

# API Reference

## API implementation and evidence

<!-- api-status:start -->
The proxy serves Loki's read API; write endpoints such as push are not supported (ingest goes to VictoriaLogs directly). Optional deletion management is outside read API scores: its opt-in handler does not implement VictoriaLogs deletion. The alerting-rules read API remains in scope, including `/api/prom/rules*` for vmalert. The deprecated read aliases `/api/prom/label`, `/api/prom/label/{name}/values`, `/api/prom/query`, `/api/prom/series` and `/api/prom/tail` are unsupported and excluded from totals, gaps and compatibility scores (owner 2026-10-31); use the corresponding `/loki/api/v1` routes. No other write endpoint is planned.

| Read API signal | Registry evidence |
|---|---|
| Routed endpoints | 25/27 |
| Endpoints with Loki differential tests | 19/27 |
| Endpoints declared `proven` | 12/27 |

Routing, linked tests and declared proof are different signals; none means
every LogQL shape or every backend version is compatible. See the
[coverage map](compatibility-coverage.md) and [ranked differences](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/conformance/reports/parity-gaps.md).

Read routes without an implementation:

| Route | Current scope |
|---|---|
| `/loki/api/v1/cache/generation_numbers` | Not implemented |
| `/loki/api/v1/index/shards` | Optional planner gap; low priority, counted in scores |

`index/shards` is not required by the pinned Grafana/Drilldown request paths
or by proxy peer-cache lookup.

`cache/generation_numbers` is not implemented; manual cache flush and
eviction do not provide a distributed deletion generation.

Rules reads under `/loki/api/v1/rules` and `/api/prom/rules`, including namespace
and group lookups, are already routed. Configure `-ruler-backend` for vmalert;
routing these reads does not implement rule writes or prove full ruler parity.
<!-- api-status:end -->

## Query and metadata routes

The table describes routed handlers, not exhaustive Loki parity. Current test
counts and registry states are in the generated [coverage map](compatibility-coverage.md).
Conditional and unsupported APIs are listed separately below.

| Loki Endpoint | Status | VL Backend | Cached |
|---|---|---|---|
| `GET/POST /loki/api/v1/query_range` (logs) | Routed | `/select/logsql/query` | 5m (2) |
| `GET/POST /loki/api/v1/query_range` (metrics) | Routed | `/select/logsql/stats_query_range` | 5m (2) |
| `GET/POST /loki/api/v1/query` | Routed | `/select/logsql/query` or `stats_query` | 5m (2) |
| `GET/POST /loki/api/v1/labels` | Routed | `/select/logsql/stream_field_names` with fallback to `/select/logsql/field_names` | 5m (3) |
| `GET/POST /loki/api/v1/label` | Routed, singular labels alias | Same labels handler and metadata cache as `/labels`; outer GET cache remains path-specific | 5m (3) |
| `GET/POST /loki/api/v1/label/{name}/values` | Routed | `field_names` (label alias resolution) → `stream_field_values` when the backend supports stream metadata endpoints, else `field_values` (also used when `stream_field_values` returns a 4xx) | 5m (3) |
| `GET/POST /loki/api/v1/series` | Routed | `/select/logsql/streams` | 30s |
| `GET/POST /loki/api/v1/index/stats` | Routed | `/select/logsql/query` (`stats count(), count_uniq_hash(_stream_id)`) | 10s |
| `GET/POST /loki/api/v1/index/volume` | Routed (4) | `/select/logsql/stats_query` with `sum_len(_msg)` | 10s |
| `GET/POST /loki/api/v1/index/volume_range` | Routed (4) | `/select/logsql/stats_query_range` with `sum_len(_msg)` | 10s |
| `GET/POST /loki/api/v1/detected_fields` | Routed | `/select/logsql/field_names` | 90s (3) |
| `GET/POST /loki/api/v1/detected_field/{name}/values` | Routed | `/select/logsql/field_values` | 90s (3) |
| `GET/POST /loki/api/v1/detected_labels` | Routed | `stream_field_names` + `field_values` on VictoriaLogs v1.50+, otherwise `/select/logsql/streams`; bounded log scan fallback | 90s (3) |
| `GET/POST /loki/api/v1/patterns` | Routed (toggleable; empty `data` when `-patterns-enabled=false`) | `/select/logsql/query` + Drain-like token clustering | `100y` (effectively persistent) |
| `GET/POST /loki/api/v1/format_query` | Routed | - (passthrough) | - |
| `WS /loki/api/v1/tail` | Routed | `/select/logsql/tail` (WebSocket->NDJSON) | - |

**(2)** Final-response cache. Requests ending within `-recent-tail-refresh-window` (default `2m`) of now are refetched once the cached entry is older than `-recent-tail-refresh-max-staleness` (default `2s`).

**(3)** Base TTL for request windows up to 1h (`-labels-cache-ttl` for labels and label values); longer windows scale it up, capped at 1h. Like Loki, `/labels` and `/label/{name}/values` return every label name or value with data anywhere in the requested `start`–`end` range on the first response in the Loki-compatible browse mode. The inventory covers the full range, reusing settled cached buckets and reading recent or due buckets from VictoriaLogs; each backend request need not rescan the entire window. Optional indexed browse extensions can return a bounded page instead. A window without data returns an empty list (no synthetic `service_name`); empty answers are cached for only 30 seconds (or the higher of `-disk-cache-min-ttl` and `-peer-write-through-min-ttl`), through the same memory, disk and peer write paths as other answers, so they replace an earlier non-empty answer for the same request. If VictoriaLogs fails, the last cached answer for the same request is served when one exists, marked with the `X-Proxy-Stale-Response: true` and `Cache-Control: no-store` headers and never stored in the compatibility-edge or multi-tenant merge caches (an expired empty list is never used as that answer); otherwise the error is returned, never a partial list. When a client omits both `start` and `end` on `/labels`, `/label/{name}/values` or `/series`, the proxy bounds the backend lookup to `-metadata-default-lookback` (default `12h`).

**(4)** Volume is reported in bytes, as in Loki. The proxy sums the length of the matching log lines (`sum_len(_msg)` in VictoriaLogs), so its values are exact line bytes; Loki adds structured-metadata bytes on top (see [Known Issues](KNOWN_ISSUES.md)). The rest follows Loki: `aggregateBy=series` (default) names a result by the `targetLabels`, or by the selector's label names when `targetLabels` is empty; `aggregateBy=labels` returns one `{label=""}` result per label name; every `targetLabels` entry must be present on a stream (a stream without a service label counts as `service_name="unknown_service"`); `limit` (default `100`) keeps the largest volumes of each bucket, ties broken by name. An instant volume is stamped at `end`. A `volume_range` bucket is stamped at its end minus 1ms, the last bucket at `end`, and only buckets with data are returned. `volume_range` answers a `vector` when every series has a single sample. Invalid `aggregateBy` or `limit` values return HTTP 400. VictoriaLogs reads the log lines to measure them, so a volume request costs more than a `count()` over the same window. See Known Issues for `detected_level` volumes and multi-tenant merging.

### Drilldown Field Shaping

For Grafana Logs Drilldown and Explore compatibility:

- Query results use canonical Loki 2-tuples `[timestamp, line]` by default.
- When `X-Loki-Response-Encoding-Flags: categorize-labels` is present and `-emit-structured-metadata=true`, query results emit Loki 3-tuples `[timestamp, line, metadata]`.
- The 3rd tuple object follows Loki keys only: `structuredMetadata` and/or `parsed` (no snake_case alias keys).
- `structuredMetadata` and `parsed` are Loki metadata objects (`{"name":"value", ...}`), matching Loki query/tail response shape.
- Stream labels stay Loki-compatible on the `stream` object.
- Label APIs prefer VictoriaLogs stream metadata so parsed fields do not leak into Loki label pickers when the backend supports the stream-only endpoints.
- `-extra-label-fields` extends label-facing APIs (`/labels`, `/label/{name}/values`) with explicit VL fields and improves custom dot/underscore alias resolution.
- Optional indexed browse mode for label values (`-label-values-indexed-cache=true`) supports hotset-first responses and optional `limit`/`offset`/`search` (`search` or `q`) on `GET /loki/api/v1/label/{name}/values`. Loki ignores those parameters; `-label-browse-extensions` (default `auto`: off in the Loki-compatible profile `-label-style=underscores -metadata-field-mode=translated` unless this flag is on) decides, and `search` on `/labels` likewise.
- With `-logql-dotted-names=reject` (the `auto` default in the Loki-compatible profile) a dotted name anywhere in LogQL is Loki's 400 parse error (`parse error at line L, col C: syntax error: unexpected .` plus the expected-token list Loki prints in that position); `accept` (the `auto` default in the hybrid and native metadata modes) translates it to the dotted VictoriaLogs field.
- Patterns API can be explicitly gated via `-patterns-enabled` (default `true`) to match deployments that do not expose Drilldown pattern discovery.
- Patterns responses are clamped to `1000` entries per request and can be persisted/restored with `-patterns-persist-*` flags.
- Indexed label-values cache snapshots can be persisted to disk (`-label-values-index-persist-path`) and restored at startup.
- On stale/missing disk snapshot, startup can warm from peer cache before serving (`-label-values-index-startup-stale-threshold`, `-label-values-index-startup-peer-warm-timeout`).
- Peer cache payload fetches (`/_cache/get`) support `zstd` or `gzip` response compression for lower network latency/cost on large cache objects.
- `GET /_cache/has?keys=k1,k2,...` is a lightweight batch peer endpoint that returns key presence and remaining TTL without transferring values. Used during startup warmup so instances can discover which peer has the freshest copy of each label window before fetching.
- `/loki/api/v1/tail` accepts at most 4 KiB per client WebSocket message and closes with code `1009` when exceeded; server log frames are not limited by this.
- Parsed fields and structured metadata are surfaced through `detected_fields` and `detected_field/{name}/values`.
- With `-metadata-field-mode=translated` (the default), field-oriented APIs expose Loki-style aliases only. With `-metadata-field-mode=hybrid`, they expose both native VictoriaLogs dotted names and translated Loki aliases when they differ, for example `service.name` and `service_name`.
- Synthetic compatibility labels such as `service_name` and `detected_level` stay available on the stream and label APIs.
### Query statistics (`data.stats`)

Every `query`, `query_range`, `index/volume` and `index/volume_range` answer
carries Loki's `data.stats` object with all five sections, `summary`,
`querier`, `ingester`, `cache` and `index`, with Loki's keys, key order and
value types (recorded from Loki 3.7.9 in
`internal/proxy/testdata/loki_query_stats_object.json`). Grafana builds the
query inspector's Stats ("Summary:" and "Ingester:" rows) and the Logs
Drilldown footer's "Total bytes processed" from them. `ingester` and `index`
measure Loki's own components; the proxy has none, so they are always in
Loki's zero form (as Loki writes them for a results-cache hit).

`-query-stats-mode` chooses how the numbers are filled:

- `loki` (the default) emulates Loki's numbers for the same query, as close
  as the proxy can get without asking VictoriaLogs for more. The rules were
  read off Loki's answers on the test stack and are pinned by
  `internal/proxy/testdata/loki_query_stats_splits.json`.
- `victorialogs` reports what the proxy measured for the request: the rows
  VictoriaLogs returned and their line bytes, a metric answer's own counts,
  the VictoriaLogs calls as `splits`, and nothing processed on a cache hit.

No VictoriaLogs request is added for the statistics, except with
`-query-stats-exact-metric-bytes` or `-query-stats-exact-log-lines` (below).

Loki's accounting (`-query-stats-mode=loki`):

| Field | Source |
|---|---|
| `summary.execTime` | The request's time in the proxy, in seconds. Every request reports its own, cache hits included. |
| `summary.queueTime` | Time the request waited for heavy-query admission (`-backend-heavy-query-*`), in seconds. |
| `summary.splits`, `summary.shards` | Loki's pieces. A range query is split at 1h (Loki's default `split_queries_by_interval`), aligned to the hour, after aligning start and end down to the step. A metric query runs every piece: `splits` = the pieces when there are more than one, else 0; `shards` = the pieces. An instant query splits its range into ceil(range / 1h) pieces. A log query reads its pieces from the newest (backward) or the oldest (forward) and stops after the piece in which its limit was filled: `splits` and `shards` = the pieces it read. `volume_range`: `splits` = the step buckets the window touches; `volume`: 1. |
| `summary.totalLinesProcessed`, `totalPostFilterLines` | Log query: the lines of the pieces Loki reads, which it reads whole. When the panel's count answer of the same expression is held (Explore and Logs Drilldown send one with the logs query), they come from it, else they are estimated from the answer: its entries spread over the time they cover, scaled to the pieces read and capped at the entries times the pieces read (exact when the limit was not filled). When the limit is filled, Loki's own figure depends on how its chunks are laid out (a head block is read whole, a compressed block until the limit, a flushed chunk whole per piece), which the proxy cannot see; it reports whole pieces, a figure between the entries returned and the window's lines. With `-query-stats-exact-log-lines` (default off), a log query cache miss that fills its limit, or a filtered one, gets the lines of the pieces read from one VictoriaLogs count of its stream selector over them (a timestamp-only count, one per log query miss: on a fleet, one more VictoriaLogs query per Explore or Logs Drilldown logs panel refresh that misses the cache). A filtered log query with no count of its bare selector reports the post-filter lines as its lines. Metric query: the lines its answer counts over Loki's window `(start - range, end]` (`count_over_time`; `rate` times its range; on sliding windows the sum times step/range, an estimate). Volume: 0. |
| `summary.totalBytesProcessed` | Those lines times the mean line size: for a log query the mean of the lines it returned; for a metric query the mean the selector's (or the tenant's) log answers showed on the same replica, kept an hour in a bounded table (4,096 entries); `bytes_over_time` and `bytes_rate` count their own bytes. With `-query-stats-exact-metric-bytes`, a metric query's bytes are the line bytes of its stream selector over its window, read from VictoriaLogs with one volume request sent next to the query (about six times the VictoriaLogs time of a count; cached like volume answers). Structured-metadata bytes, which Loki adds, are not counted. Volume: 0. |
| `summary.bytesProcessedPerSecond`, `linesProcessedPerSecond` | The two totals over `execTime`; on a replayed cache hit, over the stored computation's time. |
| `summary.totalEntriesReturned` | Entries of a streams answer, series of a matrix or vector answer. |
| `summary.subqueries`, `totalStructuredMetadataBytesProcessed` | 0: VictoriaLogs has no subqueries and keeps no structured-metadata byte count. |
| `querier.querierExecTime` | The time the request spent in VictoriaLogs calls, in seconds (0 for volume answers, as Loki's index answers them). The rest of `querier`, `ingester` and `index` is Loki's zero form. |
| `cache.result` | The request's reads and stores in the response caches (final answers, log windows, settled steps): `entriesRequested`, `entriesFound`, `entriesStored`, `bytesReceived`, `bytesSent`, `requests`, `downloadTime` (ns); `queryLengthServed` the window (ns) of a whole metric answer found. |
| `cache.instantMetricResult` | The same for the response cache of instant queries. |
| `cache.statsResult`, `cache.volumeResult` | Reads of the stats extent cache and of volume segments. |
| `cache.chunk`, `index`, `seriesResult`, `labelResult` | 0: no such read in a query request. |

A response-cache hit reports what Loki reports for the same request with its
default results cache: Loki computes a non-empty log query and an instant
query on every request, so the proxy's hit of one replays the stored
computation's lines, bytes, rates, splits and querier time with the hit's
own `execTime` (a log repeat's `shards` are the pieces between its oldest
and newest entries: Loki serves the empty pieces from its results cache);
an empty log answer and a metric `query_range` are served from Loki's
results cache, so their hits report no lines or bytes processed, the entry
found and the pieces as `splits`. A metric
answer partly served from cached steps or extents reports no derived lines
(Loki counts only the part it computed).

The panel's count answer is kept for this (`qs-totals:` entries, 5 minutes,
in memory and pushed to the key's peer replicas; keyed by tenant org,
routing and auth scope and the expression) when it holds every line of the
expression: a tumbling `count_over_time` range answer of a log expression
without a parser. A log query reads it from memory only (never waiting on a
peer) and uses it when the answer's whole steps inside the pieces read span
at least 90% of them; the uncovered edge is left out, not estimated.

Known differences in Loki's accounting, measured on the test stack:

- Loki reads part of a piece already flushed to its store when a forward
  query starts there, where the proxy counts the piece whole.
- An estimated log line count is off by up to a few percent, more for a
  small limit (the entries cover only seconds).
- Bytes are within about 10% (no structured-metadata bytes; a metric query's
  bytes estimated from the mean line size).

When Grafana loads a long range in pieces (query splitting, ranges over one
day), it sends one request per piece and its Stats tab keeps only "Summary:
total bytes processed" (summed over the pieces) and "Summary: exec time"
(observed with Grafana 13.3.0); each piece reports its own figures by the
rules above.

## Delete Endpoint (Exception)

| Endpoint | Method | VL Backend |
|---|---|---|
| `/loki/api/v1/delete` | POST, PUT (Loki's create) | `/select/logsql/delete` |

The delete endpoint is the only write route, and it is registered only with `-delete-api-enabled=true`; by default every method on the path answers `404` (`404 page not found`), as Loki does with deletion disabled. When enabled it is **not functional against current VictoriaLogs**: the handler forwards to `/select/logsql/delete`, which VictoriaLogs rejects as an unsupported path (observed on v1.52.0; VictoriaLogs deletion uses the asynchronous `/delete/run_task` API), and the proxy answers it as Loki answers a failed call (`500`, never the backend's status). GET (list) and DELETE (cancel), which Loki also serves on this path, are not implemented and answer a bodyless `405` (an open gap). Do not rely on it for deletion; see [Security hardening migration](security-hardening-migration.md#remaining-delete-api-gap).

Requests are still validated before forwarding:

- **Delete token**: `-delete-api-enabled=true` requires `-delete-api-auth-token`, and every request must carry it in `X-Delete-Token` or `Authorization: Bearer`; without it the answer is `401 delete authentication required`, before any other check (Loki itself has no authentication on this route). It is the delete API's own credential: the admin token does not open the route, and enabling deletes leaves the admin listener as it is
- **Tenant**: checked as Loki checks it (see [Tenant header](#tenant-header-x-scope-orgid))
- **Confirmation header**: Requires `X-Delete-Confirmation: true`
- **Query required**: Must target specific streams (no wildcards `{}` or `*`)
- **Time range required**: Both `start` and `end` parameters mandatory
- **Time range limit**: Maximum 30 days per delete operation
- **Tenant scoping**: Deletes scoped to the requesting tenant's data
- **Audit logging**: All delete operations logged at WARN level with tenant, query, time range, client IP

### Example

```bash
curl -X POST 'http://proxy:3100/loki/api/v1/delete' \
  -H "X-Delete-Token: $DELETE_API_AUTH_TOKEN" \
  -H 'X-Delete-Confirmation: true' \
  -H 'X-Scope-OrgID: team-alpha' \
  -d 'query={app="nginx",env="staging"}&start=1704067200&end=1704153600'
```

## Write Endpoint (Blocked)

| Endpoint | Response |
|---|---|
| `POST /loki/api/v1/push` | 405 Method Not Allowed |

This is a read-only proxy. Log ingestion should go directly to VictoriaLogs-side ingestion paths, for example:

- `vlagent` pipelines targeting VictoriaLogs ([docs](https://docs.victoriametrics.com/victorialogs/data-ingestion/))
- Loki-push-compatible ingestion endpoints handled on the VictoriaLogs side
- OTLP log ingestion into VictoriaLogs
- native JSON / OTel-shaped log ingestion into VictoriaLogs

After ingestion, data is queryable through the proxy's Loki-compatible read API.

## Alerting and Config Compatibility

With `-tenant-label`, the rules and alerts routes below answer an org only with the rules and alerts whose `labels` carry `<tenant-label>=<org>` (rule groups left without a rule are dropped; an org with none gets the usual empty answer, and a named group it has no rule in is `404`). A rule or alert without that label is shown to no org. Alerts nested in a kept rule are filtered the same way, and a tenant-label value holding a template (`{{`) matches no org, so use literal tenant-label values on rules: a templated one hides the rule on these routes. A kept rule group is reduced to its name, file, interval, limit, evaluation times and rules, and warnings, infos and the group paging token are dropped; use one tenant per rule group. A mapped org is sent to the backend with its mapping's tenant headers instead and gets no proxy filtering, so its view depends on the ruler/alerts backend scoping by the forwarded `AccountID`/`ProjectID`; vmalert and the Loki ruler do not, so a mapped org sees the backend's global view. `-tenant.allow-global=true` keeps the unfiltered view. A backend error is answered with its status and a generic body, and an answer the proxy cannot attribute is refused with `500`.

| Endpoint | Response | Purpose |
|---|---|---|
| `GET /loki/api/v1/rules` | Legacy Loki YAML rules view when `-ruler-backend` is configured, otherwise empty YAML rules map | Loki/Grafana compatibility |
| `GET /loki/api/v1/rules/{namespace}` | Legacy Loki YAML rules view filtered to a namespace when `-ruler-backend` is configured | Loki compatibility |
| `GET /loki/api/v1/rules/{namespace}/{group}` | Legacy Loki YAML single-rule-group view when `-ruler-backend` is configured | Loki compatibility |
| `GET /api/prom/rules` | Legacy Loki YAML alias for `/loki/api/v1/rules` when `-ruler-backend` is configured, otherwise empty YAML rules map | Loki/Grafana compatibility |
| `GET /api/prom/rules/{namespace}` | Legacy Loki YAML alias for namespace-filtered rules | Loki compatibility |
| `GET /api/prom/rules/{namespace}/{group}` | Legacy Loki YAML alias for single-rule-group lookups | Loki compatibility |
| `GET /prometheus/api/v1/rules` | Prometheus-style JSON passthrough when `-ruler-backend` is configured, otherwise empty rules JSON stub | Grafana alerting compatibility |
| `GET /loki/api/v1/alerts` | JSON passthrough when `-alerts-backend` or `-ruler-backend` is configured, otherwise empty alerts | Loki/Grafana compatibility |
| `GET /api/prom/alerts` | JSON alias for `/loki/api/v1/alerts` | Loki/Grafana compatibility |
| `GET /prometheus/api/v1/alerts` | Prometheus-style JSON passthrough when `-alerts-backend` or `-ruler-backend` is configured | Grafana alerting compatibility |
| `GET /config/tenant/v1/limits` | YAML view of the tenant's Loki limits, the values its queries run under | Grafana / Logs Drilldown bootstrap compatibility |
| `GET /config` | YAML stub | Configuration endpoint |
| `GET /loki/api/v1/drilldown-limits` | Bootstrap/capability endpoint for Grafana Logs Drilldown | Datasource capability probing |

Behavior and scope notes for these endpoints live in:
- [Configuration](configuration.md) for flags, tenant fanout behavior, and backend mapping
- [Known Issues](KNOWN_ISSUES.md) for intentional compatibility boundaries

Write-surface boundary for rules and alerts:

- These routes expose read compatibility only (Loki YAML views and Prometheus-style JSON views).
- Rule and alert write/lifecycle operations remain on [`vmalert`](https://docs.victoriametrics.com/vmalert/) and VictoriaLogs backend systems ([VictoriaLogs docs](https://docs.victoriametrics.com/victorialogs/)).
- The proxy does not implement Loki ruler write APIs.

Tenant limits notes:

- `/config/tenant/v1/limits` is single-tenant only; multi-tenant `X-Scope-OrgID: a|b` returns Loki's `401 multiple org IDs present`
- `/loki/api/v1/drilldown-limits` answers `a|b` with the limits the proxy enforces on such a request (its tenants combined as Loki combines them)
- published fields are filtered by `-tenant-limits-allow-publish`
- `-tenant-default-limits` and `-tenant-limits` set Loki's query limits per tenant: `max_query_series`, `max_entries_limit_per_query`, `max_query_length`, `max_query_lookback`, `max_query_range` and `query_timeout` are enforced exactly as published (see [Published tenant limits](configuration.md#published-tenant-limits-compatibility-surface))

## Tenant header (X-Scope-OrgID)

Tenancy is off by default: the proxy is Loki's single-tenant mode (`auth_enabled: false`), the header is ignored on every route and every request reads the default tenant (recorded from the e2e stack's Loki in `testdata/tenant_noauth_loki_answers.jsonl`; see [Tenancy and authentication](tenancy-and-auth.md)). With tenancy on, the proxy admits and refuses `X-Scope-OrgID` as Loki 3.7.9 does with `auth_enabled: true`, before the response cache or any VictoriaLogs call. The answers below are recorded from Loki and replayed against the proxy in `internal/proxy/tenant_auth_parity_test.go`.

| Header | Read routes | `/loki/api/v1/tail` | `/loki/api/v1/delete` |
|---|---|---|---|
| none, empty or blank | `401` `no org id` + newline | the same | the same |
| `.` or `..` | `400` `tenant ID is '.' or '..'` | `401`, text + newline | `400`, text + newline |
| a byte outside `A-Z a-z 0-9 ! ' ( ) * - . : _` | `400` `tenant ID '<id>' contains unsupported character '<byte>'` | `401`, text + newline | `400`, text + newline |
| more than 150 bytes | `400` `tenant ID is too long: max 150 characters` | `401`, text + newline | `400`, text + newline |
| `a\|b` (several distinct tenants) | served, each series or stream with `__tenant_id__` | `400` `multiple org IDs present` | `400` `multiple org IDs present` + newline |

`query_range`, `detected_fields` and `detected_field/{name}/values` put Loki's querier prefix `rpc error: code = Code(400) desc = ` before the `400` texts. Characters are checked before length, and each member of `a|b` in header order (members are not trimmed, so `a | b` is refused for its space); delete checks only the first member before refusing more than one. `a|a` is the tenant `a`, and `a|` is a multi-tenant request over `a`. The ruler views answer in their JSON: `/loki/api/v1/rules` and `/api/prom/rules` `500` `no org id` for any refused or multi-tenant header, `/prometheus/api/v1/rules` and `/prometheus/api/v1/alerts` `400` `too many org ids found` for a multi-tenant header and `500` `no valid org id found` for a refused ID. `Authorization` plays no part in the tenant check.

A tenant Loki accepts can still be refused by the proxy's own routing (`403 unknown tenant` for an unmapped name or a non-canonical number, `*` without `-tenant.allow-global`, tenant-label alias rules): VictoriaLogs has numeric accounts only, and serving an unmapped name from the default tenant would expose that tenant's data. See [Tenant resolution order](tenancy-and-auth.md#tenant-resolution-order-tenancy-on).

## Error Response Format

Error responses are Loki's, byte for byte in shape (Loki v3.7.9). Every query, metadata and tail-upgrade route answers with the status, `Content-Type: text/plain; charset=utf-8`, `X-Content-Type-Options: nosniff` and the message alone, with no JSON envelope and no trailing newline (`pkg/util/server/error.go` `WriteError`):

```text
HTTP/1.1 400 Bad Request
Content-Type: text/plain; charset=utf-8
X-Content-Type-Options: nosniff

the query time range exceeds the limit (query length: 800h0m0s, limit: 721h0m0s)
```

Where Loki itself writes another shape, the proxy does the same:

| Routes | Error body |
|---|---|
| `/prometheus/api/v1/rules` | the ruler's Prometheus JSON `{"status":"error","data":null,"errorType":"bad_data\|server_error","error":"<message>"}` (`pkg/ruler/base/api.go` `respondError`, `:106-123`) with no `Content-Type` of its own, so Go sniffs `text/plain; charset=utf-8`, and no `nosniff`; an unknown `type` is `400` `not supported value "xyz"`; every other failure is `500` |
| `/prometheus/api/v1/alerts` | the same JSON, with `Content-Type: application/json`, which `PrometheusAlerts` sets before it asks the ruler (`:286`) |
| `/loki/api/v1/alerts`, `/api/prom/alerts` | proxy aliases of the alerts API, answered in the alerts shape above (they are not Loki routes) |
| `/loki/api/v1/rules`, `/api/prom/rules` and their namespace and group forms | Go's `http.Error`: `text/plain; charset=utf-8`, `nosniff`, the message and a newline; a group that does not exist is `404` `group does not exist` (`:572`); a path that cannot be parsed is `500` in the ruler's JSON (`:523`, `:564`) |
| `/loki/api/v1/delete` (with `-delete-api-enabled`) | the same `http.Error` shape; PUT and POST are the create request (`pkg/loki/modules.go:1936`); GET (list, `:1937`) and DELETE (cancel, `:1938`) are not implemented and answer a bodyless `405` (open gap) |
| `/loki/api/v1/tail` after the upgrade | a close frame `1011` with the message, as Loki's `TailHandler` |

The status codes below follow Loki's Prometheus-style API handler and are the statuses of the message above; there is no `errorType` field on the wire:

| HTTP Status | Prometheus `errorType` (not sent) | Typical causes |
|---|---|---|
| 400 | `bad_data` | LogQL parse errors, unsupported constructs, invalid parameters, query-length violations (`query length X exceeds limit Y`), `line_format` / binary-expression evaluation limits, multi-tenant fanout above 64 tenants, a multi-tenant request whose query a tenant rejected |
| 401, 403, 413 and other 4xx | `bad_data` | missing `X-Scope-OrgID` with `-auth.enabled` or `-require-tenant-header` (401 `no org id`, as Loki), a tenant ID Loki refuses (400 with Loki's text, see [Tenant header](#tenant-header-x-scope-orgid)), unknown tenant, or unmapped wildcard `X-Scope-OrgID: *` (and, with `-tenant-label`, an unmapped default alias or empty org) without `-tenant.allow-global` (403), merged multi-tenant response above 32 MiB (413) |
| 404 | `not_found` | rules lookups with no matching rule group |
| 406 | `not_acceptable` | Loki status mapping |
| 429 | `bad_data` | a full query queue: `too many outstanding requests`, Loki's text (`pkg/queue/queue.go:21`), for the proxy's heavy-query and metadata-scan queues, for a VictoriaLogs `429`, and for VictoriaLogs' `503` when a query waited `-search.maxQueueDuration` for a slot |
| 499 | `canceled` | client canceled the request: `the request was cancelled by the client`, Loki's text (`pkg/util/server/error.go:26`) |
| 500 | `internal` | any failed backend call: VictoriaLogs (or the ruler, or the cold tier) refused or reset the connection, answered an error status (VictoriaLogs' `400`/`422` for a query it could not execute, `500`, a front proxy's `502`/`503`), or sent a truncated or malformed body. The text is `Internal Server Error`; Loki returns its storage error's own text there (`pkg/util/server/error.go:124-128`), and the proxy never forwards the backend's. Also proxy evaluation errors, including implicit many-to-one / multiple-match vector joins, an open circuit breaker, and the proxy's limits that name their flag (`manual metric response exceeds N bytes`, `-backend-max-buffered-response-bytes`, `-ordered-json-metric-max-bytes`) |
| 400 | `bad_data` | invalid LogQL, out-of-range parameters, a query VictoriaLogs rejects as unparsable (`failed to parse the log query`, Loki's `logqlmodel.ErrParse` text), `maximum number of series (N) reached for a single query` (as Loki answers above `max_query_series`), `manual range metric row limit exceeded (N); narrow the query or increase -manual-range-metric-row-limit` whenever a raw-row or stats-row metric evaluation passes `-manual-range-metric-row-limit` (Loki answers a query over one of its limits with 400, `logqlmodel.ErrLimit`; it has no equivalent flag). A `without` aggregation whose series before the fold pass that limit gets it too, unless the limit is below the tenant's effective `max_query_series` (then the series limit message answers) |
| 504 | `timeout` | backend or window timeouts, including a timeout on one tenant of a multi-tenant request: `request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed`, Loki's text (`pkg/util/server/error.go:27`) |

See [Fixed Execution Limits](configuration.md#fixed-execution-limits) for the limit values.

Errors answered through the proxy's error layer carry no backend text: not VictoriaLogs' message, status or LogsQL, not a transport error with the backend's host and port, not a Go decoder error. The routes that do not yet (the Logs Drilldown stats partial-results reply, a ruler or alerts backend that answers 200 with a malformed or truncated body, native tail close frames after the stream started) are listed in `conformance/registry/error-layer-known-leaks.json`. The proxy's `request error` log line carries the full error as `upstream_error`, with VictoriaLogs' message redacted (potential secrets, label selectors, long quoted literals and long hex strings replaced, truncated to 500 characters; `-debug-log-raw-queries=true` disables this redaction).

A few responses are produced before the Loki error writer: per-client rate limiting (`429`, `rate limit exceeded`) and the `-max-concurrent` admission cap (`503`, `too many concurrent queries`), both as the message alone in `text/plain`, and blocked writes (`405`, `/loki/api/v1/push`): the one proxy-only answer that stays JSON (`{"error":"write operations are not supported..."}`), since Loki has no read-only push answer to match.

### Partial Results

Some responses are `200` with incomplete data:

- With `-query-range-partial-responses=true`, log `query_range` responses may stop at the first failed window after retryable backend errors and set `X-Loki-VL-Partial-Response: true`.

## Infrastructure Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /alive`, `GET /livez` | Liveness probe — returns 200 when proxy process is healthy |
| `GET /health`, `GET /healthz` | Health probe — returns 200 when healthy |
| `GET /ready` | Readiness probe (checks VL `/health` + circuit breaker) |
| `GET /loki/api/v1/status/buildinfo` | Returns Loki `3.7.1` build info — version ≥ 3.0.0 is required for Grafana to send `X-Loki-Response-Encoding-Flags: categorize-labels`, which enables `structuredMetadata` in `query_range` responses |
| `GET /metrics` | Prometheus text exposition. Off by default: requires `-server.register-instrumentation=true`; served on `--metrics-listen` when set (the Helm chart uses `:9091`), otherwise on the main listener. Low-cardinality by default unless `-metrics.export-sensitive-labels=true` |
| `POST /admin/cache/flush` | Flush this instance's caches (Tier0, L0 hot index, L1 memory and L2 disk); `?peers=1` also purges every peer in the ring. Registered when `-server.register-instrumentation=true` |
| `GET /debug/queries` | Query analytics, disabled by default (`-server.enable-query-analytics`) |
| `GET /debug/pprof/` | Go profiling, disabled by default (`-server.enable-pprof`; also requires `-server.register-instrumentation=true`) |

Admin and debug routes are served on the loopback `--admin-listen` address (default `127.0.0.1:3101`) when `-server.admin-auth-token` is empty. When the token is set they are served on the main listener and require it as `X-Admin-Token: <token>` or `Authorization: Bearer <token>`.

### Peer Cache Endpoints

These endpoints are registered on the main listener when peer cache is enabled. They require the shared `-peer-auth-token` in the `X-Peer-Token` header; source-IP peer membership is accepted instead only with `-peer-insecure-ip-allowlist=true`.

| Endpoint | Purpose |
|---|---|
| `GET /_cache/get?key=…` | Fetch a cached entry from this peer |
| `POST /_cache/set?key=…&ttl_ms=…` | Push a cache entry to this peer (write-through) |
| `GET /_cache/hot?limit=…` | Return top-N hot cache keys for read-ahead |
| `GET /_cache/has?keys=k1,k2,…` | Batch check which keys exist in this peer's cache, with remaining TTL |
| `GET /_cache/peers` | Return the current peer list |
| `POST /_cache/purge` | Purge this peer's local caches (fanout target of `/admin/cache/flush?peers=1`; peers do not re-fan-out) |

## Observability

Observability details are maintained in [Observability Guide](observability.md), including:

- metrics and labels exposed on `/metrics`
- OTLP push configuration
- structured request log shape
- recommended dashboards and alert signals
