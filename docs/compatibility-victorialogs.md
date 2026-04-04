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
- Runtime matrix: real VictoriaLogs images from the `v1.3x.x` and `v1.4x.x` bands

VictoriaLogs is intentionally broader than the Loki support window. We support `v1.3x.x` and `v1.4x.x` because backend upgrades often lag behind proxy upgrades during migrations. When the backend support band moves forward, the oldest decade-band drops out.

## Version Matrix

| VictoriaLogs version | Coverage path | Version-specific focus |
|---|---|---|
| `v1.49.0` | PR and main CI pinned runtime | Current pinned backend |
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
