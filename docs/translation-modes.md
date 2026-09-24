---
sidebar_label: Translation Modes
description: "Label-style modes: passthrough, underscores, and hybrid — when to use each and how they affect Grafana queries."
---

# Translation Modes Guide

This guide explains how field and label translation behaves across proxy surfaces, and how to choose a mode based on whether you want a more Loki-like experience or more OTel-native dotted semantics.

## Translation Knobs

| Flag | Purpose | Typical values |
|---|---|---|
| `-label-style` | Controls label-name translation between VL and Loki label surfaces | `passthrough`, `underscores` |
| `-metadata-field-mode` | Controls field exposure for field-oriented APIs and structured metadata payloads | `native`, `translated`, `hybrid` |
| `-emit-structured-metadata` | Enables 3-tuple metadata responses for explicit `categorize-labels` requests (default: `true`) | `false`, `true` |
| `-field-mapping` | Custom mapping between VL field name and Loki label name | JSON mappings |
| `-extra-label-fields` | Explicitly extends label-facing APIs and dot/underscore alias resolution for custom fields | comma-separated VL field names |

### OTel Attribute Translation

**Flag:** `-translate-otel-attributes` (default: `true`, env `TRANSLATE_OTEL_ATTRIBUTES`)

Controls whether the LogQL→LogsQL translator rewrites the built-in list of known OTel semantic convention labels from underscore to dotted form in upstream queries to VictoriaLogs.

- `true` (default): Labels like `k8s_container_name` are rewritten to `k8s.container.name` in the upstream LogsQL query. Correct when VictoriaLogs stores these fields in dotted OTel form.
- `false`: The built-in `knownUnderscoreToDot` rewrite is skipped, so known OTel labels reach VL with their underscore form intact. Use this when your ingest pipeline (Vector, Promtail, Fluent-bit) stores OTel attributes with underscores in VictoriaLogs.

**Scope.** This flag gates only the built-in OTel rewrite layer. The other parts of `ToVL()` keep working in both modes:

| Layer | Affected by `-translate-otel-attributes=false`? |
|---|---|
| Explicit `-field-mapping` rules (`lokiToVL`) | No — operator intent always wins |
| `detected_level` → `level` alias | No |
| Built-in `knownUnderscoreToDot` semconv rewrite | Yes — disabled when flag is `false` |
| Runtime-learned aliases from VL field inventory | No — reflects what VL actually stores |
| Unknown labels (passthrough) | No |

The flag also gates `ResolveLabelCandidates` and the `resolveTargetLabelFields` underscore-fallback, so the dotted form is not added as a candidate or fallback when OTel translation is disabled. This flag only affects the **query direction** (Loki→VL); response-side label translation is controlled by `-label-style` and `-metadata-field-mode`.

## Surfaces Affected

| Surface | Uses `label-style` | Uses `metadata-field-mode` |
|---|---|---|
| Stream labels in query/query_range result (`stream: {...}`) | Yes | No |
| Label APIs (`/labels`, `/label/<name>/values`) | Yes | No |
| Field APIs (`/detected_fields`, `/detected_field/<name>/values`) | No | Yes |
| Structured metadata in 3-tuples (`categorize-labels`) | No | Yes |

## What Stays Underscore vs Dotted

Assume VL stores OTel dotted fields like `service.name`.

### With `-label-style=underscores`

| Surface | Output key |
|---|---|
| Stream labels | `service_name` |
| Label APIs | `service_name` |
| Query input (recommended) | `{service_name="..."}` (translated to VL `service.name`) |

`service_name` itself is derived as Loki assigns it: a `service_name` field, else OTel `service.name`, else the first non-empty field of Loki's `discover_service_name` list (`service`, `app`, `application`, `app_name`, `name`, `app_kubernetes_io_name`, `container`, `container_name`, `k8s_container_name`, `component`, `workload`, `job`, `k8s_job_name`, dotted OTel forms included), else `unknown_service`. A `{service_name<op>"v"}` selector is translated to a VictoriaLogs field filter that selects a row only when that derived value satisfies the matcher. `by (service_name)` in a metric query groups by the same derived value, computed in VictoriaLogs before the stats pipe (the `coalesce` pipe on v1.51+, `format` pipes before), and `/label/service_name/values` lists its distinct values. A `| service_name=…` label filter stage follows Loki's label-filter rules, which simplify a regexp before matching: `=~"gateway"` is an equality check, `=~"check.*"` a substring check, and only what Loki cannot simplify stays an anchored match (a `{service_name=~"…"}` selector is always anchored, as Prometheus matchers are).

Two limits are worth knowing. The filter repeats the matcher value once per priority position, so a selector with a long alternation (a Drilldown regexp over 30 services) reaches ~15 KB; VictoriaLogs and vmauth accept it, but a reverse proxy with a small header buffer in front of the proxy may reject the GET, so raise that buffer or let clients POST the query. And Loki derives the name from stream labels only. VictoriaLogs does not record which fields were stream labels at ingest, so a source field stored as an ordinary field (structured metadata, or a `service` key of a JSON line that was unpacked at ingest) also takes part in the derivation, and a row can then carry a different `service_name` in a log response, where the proxy reads stream fields only.

### Field-oriented and metadata behavior by `-metadata-field-mode`

| Mode | `detected_fields` / `detected_field/*` | 3-tuple metadata (`categorize-labels`) |
|---|---|---|
| `native` | only `service.name` | only `service.name` |
| `translated` | only `service_name` | only `service_name` |
| `hybrid` | both `service.name` and `service_name` | both `service.name` and `service_name` |

Notes:
- The default is `translated` (Loki-compatible names only). Use `hybrid` when users need both Loki-style and OTel-style workflows — it is not the default but is recommended for OTel data.
- Label surfaces remain Loki-compatible (underscore) when `label-style=underscores` regardless of metadata mode.

## Grafana Datasource Behavior

When Grafana Explore/Drilldown builds field filters from Event Details, it may emit dotted field expressions such as:

```logql
{service_name="otel-collector"} | k8s.cluster.name = `us-east-1`
```

Compatibility behavior:
- In the Loki-compatible profile (`-label-style=underscores -metadata-field-mode=translated`, the default) Grafana only ever sees underscore names, so it builds `k8s_cluster_name = ...`, and a dotted name typed into a query gets Loki's own parse error (`parse error at line 1, col 43: syntax error: unexpected .`), with the same line, column and expected-token list Loki reports in every LogQL position, on every endpoint, before any VictoriaLogs call.
- With `-metadata-field-mode=hybrid` or `native` (or `-label-style=passthrough`) the proxy exposes dotted names, so dotted filters are accepted and translated to VL-native dotted field matching. That is an extension of those modes; Loki rejects dotted names.
- Underscore aliases for known OTel fields are accepted in every mode (`k8s_cluster_name = ...`) and resolve to the same dotted VL field, and a filtered log volume on them is pushed down to VictoriaLogs stats.
- Stream label outputs remain Loki-safe (underscore keys) when `-label-style=underscores`.
- Field-oriented and metadata surfaces follow `-metadata-field-mode` (`native`, `translated`, `hybrid`).
- `-extra-label-fields` can be used to make custom dotted VL fields reliably visible/resolvable through `/labels`, `/label/<name>/values`, and `targetLabels` in volume APIs.

Caveat for Grafana Loki datasource builder:
- The builder UI can tokenize dotted keys (for example `host.id`) into `host` `.` `id` controls even when the generated LogQL query executes correctly.
- For stable click-to-filter workflows from Event Details, prefer underscore aliases in the UI (`-label-style=underscores`, `-metadata-field-mode=translated`) while VL remains dotted internally.
- Code mode accepts dotted expressions only in the hybrid and native modes.

## Mode Profiles

### Loki-First Profile

Use this when you want the most Loki-like label and field experience.

```bash
-label-style=underscores
-metadata-field-mode=translated
-emit-structured-metadata=true
```

Outcome:
- label surfaces are underscore-only
- field APIs and 3-tuple metadata expose underscore aliases only
- LogQL follows Loki's grammar: a dotted name is Loki's 400 parse error
- `/labels` and `/label/{name}/values` ignore `limit`, `offset` and `search` as Loki does, unless `-label-values-indexed-cache=true` opts into the indexed browse window

### Balanced Compatibility Profile (Recommended)

Use this when Loki label compatibility is required but OTel dotted correlation is also needed.

```bash
-label-style=underscores
-metadata-field-mode=hybrid
-emit-structured-metadata=true
```

Outcome:
- label surfaces are underscore-only
- field APIs and 3-tuple metadata expose both dotted and underscore keys
- dotted names are accepted in queries (an extension of this mode; Loki rejects them)

### OTel-Native Field Profile

Use this when teams primarily use dotted OTel semantics for field exploration/correlation.

```bash
-label-style=underscores
-metadata-field-mode=native
-emit-structured-metadata=true
```

Outcome:
- label surfaces still remain Loki-compatible underscore keys
- field APIs and 3-tuple metadata expose dotted names only

### Passthrough Profile (Only for already-underscore data)

Use this when ingestion already normalizes labels to underscore names upstream.

```bash
-label-style=passthrough
-metadata-field-mode=translated
```

Outcome:
- proxy does not alter label names
- avoid this when upstream stores dotted labels and you need Loki query ergonomics

## Custom Mapping (`-field-mapping`)

Custom mappings override automatic translation and apply in both directions.

```bash
-field-mapping='[
  {"vl_field":"my_trace_id","loki_label":"traceID"},
  {"vl_field":"internal.request.id","loki_label":"request_id"}
]'
```

Use mapping when your VL schema does not follow common OTel naming or you need stable alias names for dashboards/alerts.

## Recommended Decision Path

1. If VL stores dotted OTel fields, start with `label-style=underscores`.
2. Choose `metadata-field-mode` based on consumer needs:
   - `translated` for Loki-only field UX
   - `hybrid` for mixed Loki + OTel workflows
   - `native` for OTel-native field UX
3. Enable `-emit-structured-metadata=true` when clients need metadata in 3-tuple responses via `categorize-labels`.
4. Add `-field-mapping` only for non-standard schema cases.
5. Add `-extra-label-fields` for custom fields you want consistently visible on label-facing APIs and Grafana builder workflows.

## Related

- [Configuration](configuration.md)
- [Translation Reference](translation-reference.md)
- [Compatibility Drilldown](compatibility-drilldown.md)
- [Known Issues](KNOWN_ISSUES.md)