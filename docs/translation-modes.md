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

### Field-oriented and metadata behavior by `-metadata-field-mode`

| Mode | `detected_fields` / `detected_field/*` | 3-tuple metadata (`categorize-labels`) |
|---|---|---|
| `native` | only `service.name` | only `service.name` |
| `translated` | only `service_name` | only `service_name` |
| `hybrid` | both `service.name` and `service_name` | both `service.name` and `service_name` |

Notes:
- `hybrid` is the default and best when users need both Loki-style and OTel-style workflows.
- Label surfaces remain Loki-compatible (underscore) when `label-style=underscores` regardless of metadata mode.

## Grafana Datasource Behavior

When Grafana Explore/Drilldown builds field filters from Event Details, it may emit dotted field expressions such as:

```logql
{service_name="otel-collector"} | k8s.cluster.name = `us-east-1`
```

Compatibility behavior:
- Dotted filters are accepted and translated to VL-native dotted field matching.
- Underscore aliases for known OTel fields are also accepted (`k8s_cluster_name = ...`) and resolve to the same dotted VL field.
- Stream label outputs remain Loki-safe (underscore keys) when `-label-style=underscores`.
- Field-oriented and metadata surfaces follow `-metadata-field-mode` (`native`, `translated`, `hybrid`).
- `-extra-label-fields` can be used to make custom dotted VL fields reliably visible/resolvable through `/labels`, `/label/<name>/values`, and `targetLabels` in volume APIs.

Caveat for Grafana Loki datasource builder:
- The builder UI can tokenize dotted keys (for example `host.id`) into `host` `.` `id` controls even when the generated LogQL query executes correctly.
- For stable click-to-filter workflows from Event Details, prefer underscore aliases in the UI (`-label-style=underscores`, `-metadata-field-mode=translated`) while VL remains dotted internally.
- Code mode remains valid for dotted expressions when your workflow requires native dotted fields.

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