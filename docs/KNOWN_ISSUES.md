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
| Delete path | `POST /loki/api/v1/delete` is the only write exception and is guarded by confirmation header, time-range checks, tenant scoping, and audit logging. |
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
| Wildcard tenant shorthand | `X-Scope-OrgID: *` is a proxy convenience for global/default routing. It is not a Loki-compatible all-tenants shorthand. |
| Patterns surface | `/loki/api/v1/patterns` is optional (`-patterns-enabled`) and responses are clamped to `1000` patterns per request. |
| `count_values()` aggregation | Not translatable. VictoriaLogs has no equivalent function that groups by metric values. Queries using `count_values` return a descriptive error. |
| Log stream ordering above split interval | For queries spanning more than one windowing interval, log entries within each stream are sorted ascending by timestamp; however Grafana may display them in the requested `direction` based on the overall response. This is stable as of v1.21.1. |
| OTel attribute translation in upstream queries | By default (`-translate-otel-attributes=true`), the LogQL→LogsQL translator rewrites known OTel semantic convention labels from underscore to dotted form (e.g., `k8s_container_name` → `k8s.container.name`). Deployments that store these fields with underscores (Vector, Promtail, Fluent-bit via Elasticsearch bulk ingest) should set `-translate-otel-attributes=false`. |

## Translation and Performance Caveats

Some compatibility behavior is implemented in the proxy rather than delegated to
native VictoriaLogs primitives. That keeps the Loki-facing contract usable, but
it also means latency, CPU cost, and observability differ from a native Loki
backend or a pure VictoriaLogs query path.

This especially matters for:

- parser and filter compatibility stages
- some response shaping and label/field alias resolution
- parts of binary and subquery compatibility behavior
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
- `detected_level` inference — proxy infers level from JSON/logfmt `_msg` content when not present in stream labels (v1.20.0)
- circuit breaker sliding window — failure counting uses a 30-second sliding window; sporadic slow-query resets no longer open the breaker (v1.18.0)
- deterministic log stream ordering for multi-window queries — streams and per-stream values now sorted stably before response emission (v1.21.1)
- `offset` directive — fully implemented: proxy strips the offset clause and shifts `start`/`end` (or `time` for instant queries) backward by the offset duration before backend dispatch
- `| drop field=value` matcher semantics — proxy now conditionally removes a field only when its value matches, via proxy-side post-processing (`ParseDropConditions` + `applyDropConditions`); previously the value predicate was silently ignored and the field was always dropped (v3.7.1)
- structuredMetadata vs parsedFields classification — proxy correctly classifies structured metadata fields by comparing against `_msg` JSON content; previously some structured metadata fields were misclassified as parsed fields (v3.7.1)
- exhaustive parity test coverage — 555+ LogQL parity cases with all 14 previously tracked `proxy_bug` and `proxy_strict` KnownGaps resolved (v3.7.1)
- `| keep field=value` matcher form on stream labels — proxy now applies keep conditions to stream labels (not just structured metadata / parsed fields); mirrors the existing `| drop field=value` stream label path (v1.50.1)
- Parallel multi-tenant fanout — sub-requests dispatched via goroutine-per-tenant with `sync.WaitGroup`; latency equals slowest tenant, not sum (v1.43.0)
- Streaming backward hot+cold merge — cold reverse pass uses bounded ring buffer (`maxRingSize=5000`) with early termination instead of full body buffering (v1.43.0)
- `absent_over_time()` — fully implemented (v1.35.0); translates to `stats count()` with empty-series emission
- `sort` / `sort_desc` outer aggregations — fixed in v1.35.0; sort by metric value across series now works correctly
- Cold storage backend routing (Victoria Lakehouse) — implemented v1.28.0; time-boundary split between hot VL and cold Lakehouse

## Related Docs

- [Compatibility Matrix](compatibility-matrix.md)
- [Loki Compatibility](compatibility-loki.md)
- [Logs Drilldown Compatibility](compatibility-drilldown.md)
- [VictoriaLogs Compatibility](compatibility-victorialogs.md)
- [Translation Modes](translation-modes.md)
- [API Reference](api-reference.md)
