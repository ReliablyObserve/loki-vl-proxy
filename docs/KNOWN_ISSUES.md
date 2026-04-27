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
| `offset` directive | Silently stripped from queries; results do not reflect time shifting. Implementation requires parsing offset value and adjusting start/end parameters before backend dispatch. |
| `label_replace()` function | Not implemented in the translator or proxy. Queries using `label_replace` will fail with a translation error. |

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

## Operational Caveats

| Area | Current state |
|---|---|
| Patterns persistence | If `-patterns-persist-path` is configured and not writable, startup fails fast. Without persistence, the endpoint still works, but warm state is lost on restart. |
| Label-values persistence | If `-label-values-index-persist-path` is configured and not writable, startup fails fast. Without persistence, indexed browse state is rebuilt after restart. |
| Startup warm readiness | When patterns or label-values startup warm is configured, readiness can remain `503` until disk restore or peer warm completes. |
| Older VictoriaLogs metadata paths | Newer VictoriaLogs versions let the proxy prefer stream-only metadata APIs. Older versions may fall back to broader field APIs, which can change how strictly stream-shaped some browse endpoints feel. |
| Large body fields | Very large body fields can still be dropped on the VictoriaLogs side. Track the upstream issue: [VictoriaLogs issue #91](https://github.com/VictoriaMetrics/victorialogs-datasource/issues/91). |

## What Is No Longer an Open Gap

These are not current open issues in this codebase:

- read-path query and label fanout across multiple tenants
- Grafana Logs Drilldown contract coverage as a tracked compatibility product
- Loki-compatible `/loki/api/v1/patterns` support with persistence and peer warm
- route-aware proxy, cache, and upstream request telemetry
- prefixed app metrics under `loki_vl_proxy_*` with CI guard coverage

## Related Docs

- [Compatibility Matrix](compatibility-matrix.md)
- [Loki Compatibility](compatibility-loki.md)
- [Logs Drilldown Compatibility](compatibility-drilldown.md)
- [VictoriaLogs Compatibility](compatibility-victorialogs.md)
- [Translation Modes](translation-modes.md)
- [API Reference](api-reference.md)
