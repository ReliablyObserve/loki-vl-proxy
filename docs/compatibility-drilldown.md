# Logs Drilldown Compatibility

This track measures compatibility with the Grafana Logs Drilldown app, not generic Loki clients.

## Scope

- Grafana datasource resource endpoints consumed by the app
- Service selection, service-detail log volume, fields, labels, and field values
- Log frame expectations that affect labels, level coloring, and field visibility

## CI And Score

- Workflow: `compat-drilldown.yaml`
- Score test: `TestDrilldownTrackScore`
- Runtime coverage: pinned Grafana runtime on PRs plus previous-family Grafana smoke on PRs, with the fuller Grafana matrix kept for scheduled/manual runs
- Version matrix: source-contract checks across the current Drilldown family and one family behind

The Drilldown matrix is also a moving window. We support the current app family and one family behind, with the contract list sliding forward as upstream releases move. We do not keep an open-ended tail of older app families.

## Version Matrix

| Logs Drilldown version | Coverage path | Version-specific focus |
|---|---|---|
| `2.0.1` | PR/main pinned runtime + scheduled/manual contract matrix | Current pinned contract |
| `2.0.0` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `1.0.41` | PR/main previous-family Grafana smoke + scheduled/manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.40` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.39` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.38` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.37` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.36` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.35` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
| `1.0.34` | Scheduled and manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |

## Contracts We Enforce

- `index/volume` must expose real `service_name` buckets
- `index/volume_range` must expose non-empty `detected_level` series names
- `detected_fields` must show parsed fields like `method`, `path`, `status`, `duration_ms`
- `detected_fields` must not leak indexed labels like `app`, `cluster`, or `namespace`
- In hybrid field mode, `detected_fields` may expose both native dotted fields and translated aliases such as `service.name` and `service_name`
- `detected_fields`, `detected_labels`, and `detected_field/{name}/values` should prefer native VictoriaLogs metadata lookups where they map cleanly, then fall back to bounded raw-log sampling for parsed and derived fields
- Label-value resources for additional filters such as `cluster` must return real values
- `patterns` must return non-empty grouped pattern payloads with sample buckets for Drilldown
- Multi-tenant Drilldown queries with repeated `var-levels=detected_level|=|...` selections must stay valid and return logs instead of backend parse errors

## Edge Cases Covered

- Mixed parser query path: `| json ... | logfmt | drop __error__, __error_details__`
- Labels object parsing in returned log frames
- App-level field suppression for `detected_level`, `level`, and `level_extracted`
- `1.x` service-selection buckets, detected-fields filtering, and labels field parsing stay explicit in the source-contract checks
- `2.x` detected-level default columns, field-values breakdown scenes, and additional label-tab wiring stay explicit in the source-contract checks
- Service-detail field breakdowns and additional label filters
- Multi-tenant Drilldown log views filtered by `cluster` plus multiple selected `detected_level` values
- Native field-value discovery for indexed metadata such as `service.name`, with parser-stage stripping before the backend lookup
- Fallback scanning for parsed-only fields such as `method` when no safe native metadata path exists
- Patterns grouping across repeated request shapes
