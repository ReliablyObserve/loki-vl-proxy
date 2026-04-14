# VictoriaLogs Compatibility

This track measures whether the proxy keeps VictoriaLogs behavior usable and stable while exposing Loki-compatible semantics on top.

## Scope

- Translation to VictoriaLogs query and metadata endpoints
- `_stream` parsing and synthetic label construction
- `field_names`, `field_values`, `hits`, and `stats_query_range` integration
- Service-name derivation, detected field values, and volume endpoints backed by VictoriaLogs

## CI And Score

- Workflow: `compat-vl.yaml`
- Score test: `TestVLTrackScore`
- Runtime matrix: real VictoriaLogs images from the `v1.3x.x` through `v1.5x.x` bands

VictoriaLogs is intentionally broader than the Loki support window. We support `v1.3x.x` through `v1.5x.x` because backend upgrades often lag behind proxy upgrades during migrations. When the backend support band moves forward, the oldest decade-band drops out.

## Version Matrix

| VictoriaLogs version | Coverage path | Version-specific focus |
|---|---|---|
| `v1.50.0` | PR and main CI pinned runtime | Current pinned backend |
| `v1.49.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.48.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.47.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.46.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.45.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.44.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.43.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.42.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.41.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.40.0` | Scheduled and manual matrix | Structured metadata shaping, volume endpoints |
| `v1.39.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.38.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.37.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.36.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.35.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.34.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.33.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.32.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.31.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |
| `v1.30.0` | Scheduled and manual matrix | `_stream` parsing, field names and values |

## Edge Cases Covered

- Service name derived from labels when VictoriaLogs does not carry a native `service_name`
- `detected_fields` and `detected_field/<name>/values` derived from VictoriaLogs field content
- Loki `index/stats`, `index/volume`, and `index/volume_range` backed by VictoriaLogs `hits` and stats endpoints
- Raw VictoriaLogs fields mapped into parsed fields or structured metadata without polluting stream labels

## Runtime Capability Profiles

The proxy passively detects backend version from upstream response headers and selects a capability profile. This keeps one binary safe across mixed backend versions while enabling newer LogSQL optimizations where available.

| VictoriaLogs version family | Capability profile | Stream metadata endpoints fast path (`stream_field_*`) | Metadata substring filter (`q` + `filter=substring`) | Dense patterns windowing profile |
|---|---|---|---|---|
| `v1.50.x+` | `vl-v1.50-plus` | enabled | enabled | enabled |
| `v1.49.x` | `vl-v1.49-plus` | enabled | enabled | disabled (conservative profile) |
| `v1.30.x` to `v1.48.x` | `vl-v1.30-plus` | enabled | disabled | disabled (conservative profile) |
| `< v1.30.0` | `legacy-pre-v1.30` | disabled (fallback to generic `field_*`) | disabled | disabled |
| unknown (before first upstream response) | `unknown` | enabled (optimistic default) | disabled (safe default) | disabled (safe default) |

Current code gates:

- pattern extraction window density for `/loki/api/v1/patterns`
- stream-metadata-first label inventory/value lookups (`/select/logsql/stream_field_names`, `/select/logsql/stream_field_values`)
- metadata search fan-in via `q` + `filter=substring` on `field_*` and `stream_field_*` endpoints

As new LogSQL backend features land, this table and the capability derivation in proxy code should be updated together, with explicit tests per profile.

## Feature Capability Matrix (v1.30.0 To v1.50.0)

The table below tracks changelog-relevant LogSQL and metadata behavior between `v1.30.0` and `v1.50.0`, and how the proxy should treat each band.

| Version band | Backend capability signals | Limitations / risks to account for | Proxy handling policy |
|---|---|---|---|
| `v1.30.x` to `v1.33.x` | baseline stable `field_*`, `stream_field_*`, `hits`, and core query endpoints | older query-path edge cases; use conservative behavior for expensive pattern extraction | keep stream metadata fast-path enabled; keep conservative (non-dense) pattern windowing |
| `v1.34.x` to `v1.48.x` | improved cluster query behavior and partial-response handling in VictoriaLogs changelog line | still treat dense pattern extraction as opt-in to avoid long-range overload in mixed backends | same runtime profile as `vl-v1.30-plus`; prefer conservative pattern sampling |
| `v1.49.x` | adds `filter=substring` support for `field_names` / `field_values` and stream metadata browse endpoints | older versions do not support this parameter reliably | gate substring server-side filtering by backend capability; keep fallback filtering in proxy for older versions |
| `v1.50.x+` | parser/query fixes and latest metadata/query behavior; current pinned target | none specific beyond normal backend saturation limits | enable `vl-v1.50-plus` profile, including dense patterns windowing and newest metadata behavior |
| `< v1.30.0` | legacy behavior only | higher compatibility risk for modern drilldown/explore contracts | block startup by default (`backend-min-version`), allow override with `backend-allow-unsupported-version=true` |

### Capability Profile Guidance

- `vl-v1.50-plus`: use for latest backend capabilities and densest safe pattern windowing.
- `vl-v1.30-plus`: compatibility-first profile across transitional fleets.
- `legacy-pre-v1.30`: fallback-only profile, intended for temporary migrations.

When adding a new profile or changing a gate, update three places in the same PR:

- runtime derivation in `internal/proxy/proxy.go`
- capability metadata in `test/e2e-compat/compatibility-matrix.json`
- this document section
