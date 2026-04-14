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
| `12.4.2` | PR/main pinned runtime + scheduled/manual runtime matrix | full Drilldown runtime score on the pinned current build |
| `12.4.1` | PR/main current-family smoke + scheduled/manual runtime matrix | datasource catalog, base Drilldown resource contracts, explicit `2.x` runtime-family assertions |
| `11.6.6` | PR/main previous-family smoke + scheduled/manual runtime matrix | datasource catalog, base Drilldown resource contracts, explicit `1.x` runtime-family assertions |

### Logs Drilldown app versions

| Logs Drilldown version | Coverage path | Version-specific focus |
|---|---|---|
| `2.0.3` | PR/main pinned runtime + scheduled/manual contract matrix | Current pinned contract |
| `2.0.2` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `2.0.1` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `2.0.0` | Scheduled and manual contract matrix | `detected_level` coloring, service-detail panels |
| `1.0.41` | PR/main previous-family Grafana smoke + scheduled/manual contract matrix | Service buckets, detected-fields filtering, labels field parsing |
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
2. Grafana runtime family (`11.x`, `12.x`),
3. compatibility matrix contract version bands (`1.0.x`, `2.0.x`), validated in CI.

## Drilldown Capability Profiles

| Drilldown version family | Capability profile | Proxy handling focus |
|---|---|---|
| `2.0.x` | `drilldown-v2` | detected-level defaults, modern service-detail scenes, patterns and field-value drill flows |
| `1.0.x` | `drilldown-v1` | legacy service buckets, filtered detected-fields path, prior labels/field rendering behavior |

These profiles are matrix-level compatibility profiles (contract and CI guidance). Runtime request handling must stay Loki-compatible and should not depend on guessed app build strings.

## Release Watchlist

Potential next family move:

- current: `2.0.x`
- next expected family to evaluate: `2.1.x` (then `3.0.x` when released)

Promotion criteria for a new family:

1. add versions to matrix manifest,
2. verify `TestDrilldownTrackScore` and `TestDrilldown_RuntimeFamilyContracts` on pinned + smoke runtimes,
3. confirm no regressions in patterns, labels/fields, and service detail flows.

## Contracts We Enforce

- `index/volume` must expose real `service_name` buckets
- `index/volume_range` must expose non-empty `detected_level` series names
- `detected_fields` must show parsed fields like `method`, `path`, `status`, `duration_ms`
- `detected_fields` must not leak indexed labels like `app`, `cluster`, or `namespace`
- In hybrid field mode, `detected_fields` may expose both native dotted fields and translated aliases such as `service.name` and `service_name`
- `labels` and `label/{name}/values` should stay stream-shaped; they should prefer VictoriaLogs stream metadata endpoints and only fall back to generic field endpoints for older backend versions
- `detected_fields`, `detected_labels`, and `detected_field/{name}/values` should prefer native VictoriaLogs metadata lookups where they map cleanly, then fall back to bounded raw-log sampling for parsed and derived fields
- Alias resolution must keep exact native matches working, allow unique translated aliases to resolve automatically, and avoid silently choosing the wrong native field when multiple dotted names collapse to the same Loki-safe alias
- Label-value resources for additional filters such as `cluster` must return real values
- Unknown label and detected-field lookups should keep a success payload shape instead of flipping into transport errors
- `patterns` must return non-empty grouped pattern payloads with sample buckets for Drilldown
- Multi-tenant Drilldown queries with repeated `var-levels=detected_level|=|...` selections must stay valid and return logs instead of backend parse errors

## Edge Cases Covered

- Mixed parser query path: `| json ... | logfmt | drop __error__, __error_details__`
- Labels object parsing in returned log frames
- App-level field suppression for `detected_level`, `level`, and `level_extracted`
- `1.x` service-selection buckets, detected-fields filtering, and labels field parsing stay explicit in the source-contract checks
- `2.x` detected-level default columns, field-values breakdown scenes, and additional label-tab wiring stay explicit in the source-contract checks
- Grafana runtime `11.x` explicitly asserts `1.x`-style service buckets, filtered detected fields, and extra label values at runtime
- Grafana runtime `12.x` explicitly asserts `2.x`-style detected-level series, field-value breakdowns, and extra label values at runtime
- Service-detail field breakdowns and additional label filters
- Multi-tenant Drilldown log views filtered by `cluster` plus multiple selected `detected_level` values
- Multi-tenant Grafana resource calls with `__tenant_id__!~...` and `__tenant_id__="missing"` keep the correct narrowed or empty-success behavior
- Native field-value discovery for indexed metadata such as `service.name`, with parser-stage stripping before the backend lookup
- Fallback scanning for parsed-only fields such as `method` when no safe native metadata path exists
- Patterns grouping across repeated request shapes
