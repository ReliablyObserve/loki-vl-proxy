# Loki Compatibility

This track measures how closely the proxy behaves like Loki on Loki-facing APIs and LogQL behavior.

## Scope

- `/labels`, `/label/<name>/values`, `/series`, `/query`, `/query_range`
- LogQL parser, filter, metric, and OTel label compatibility
- Synthetic compatibility labels the proxy must expose to Loki clients, such as `service_name`

## CI And Score

- Workflow: `compat-loki.yaml`
- Score test: `TestLokiTrackScore`
- Runtime matrix: real Loki images
- Support window: current Loki minor family plus one minor family behind

The Loki matrix is a moving window. When a new Loki minor becomes current, the matrix advances to that family and the immediately previous minor family. Older minors drop out of support.

## Version Matrix

| Loki version | Coverage path | Version-specific focus |
|---|---|---|
| `3.7.1` | PR and main CI pinned runtime | Primary supported reference |
| `3.7.0` | Scheduled and manual matrix | `detected_level` metric grouping, OTel label parity |
| `3.6.10` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.9` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.8` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.7` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.6` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.5` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.4` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.3` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.2` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.1` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |
| `3.6.0` | Scheduled and manual matrix | Range query matrix shape, detected fields stability |

## Edge Cases Covered

- JSON and logfmt parser chains followed by field filters
- `detected_level` grouped metric queries used by Grafana log volume panels
- OTel dotted and underscore label parity through the underscore proxy
- Series and label-value parity for labels synthesized by the proxy
