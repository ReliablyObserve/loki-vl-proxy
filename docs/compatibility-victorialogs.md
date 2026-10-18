---
sidebar_label: VictoriaLogs Compatibility
description: How loki-vl-proxy integrates with VictoriaLogs versions, endpoints, and query features.
---

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
- Runtime matrix: real VictoriaLogs images, every supported minor at its latest patch that has a published image (`v1.49.0`, `v1.50.0`, `v1.51.1`, `v1.52.0`, `v1.53.0`). Each version runs the score test, the version-sensitive checks and the range-metric table compared with Loki (`TestCompat_Unwrap*`, `TestRangeMetric*`, `TestGrafanaClickout_*`)

## Support Policy

Supported and tested versions are also listed in the [README](https://github.com/ReliablyObserve/Loki-VL-proxy#supported-versions). The rules:

- **Fully supported:** `v1.49.0`, the latest `v1.4x` release, and every `v1.5x` release. `v1.48.x` and older are not supported.
- **Tested:** every supported minor at its latest patch that has a published Docker image (`v1.49.0`, `v1.50.0`, `v1.51.1`, `v1.52.0`, `v1.53.0`; `v1.52.1` has no image, so `v1.52.0`). The pull-request CI image is pinned to `v1.52.0`; moving the pin is a separate change with its own proof.
- **Floor:** `-backend-min-version` defaults to `v1.49.0`. Startup fails when the detected backend is older, with an error naming the flag. With an explicit older `-backend-min-version` (for example a Helm value still carrying `v1.30.0`) or `-backend-allow-unsupported-version=true` the proxy starts, logs a warning on every start (`backend version is below the supported VictoriaLogs lines`, whatever the flag says), and the version is unsupported and untested: the proxy keeps no fallbacks for features older than `v1.49.0`, for example the `stats_query_range` `offset` argument (`v1.45`). A backend whose version cannot be read only warns unless `-backend-version-strict=true`, and then behaves as `v1.49.0`. See [Configuration](configuration.md).
- **Moving the floor or adding a version:** a new minor joins `matrix_versions`; when support for the oldest supported minor ends, the floor moves. In one change: update `matrix_versions`, `support_window` and the capability profiles in `test/e2e-compat/compatibility-matrix.json`, the supported-versions table in the README, the `-backend-min-version` default (flag, Go fallback, Helm value), `logsql.MinSupportedMinor`, and the pinned image. `TestVictoriaLogsSupportPolicy` fails until these agree: the floor is the lowest matrix version and the only `v1.4x` entry, every `v1.5x` minor from `v1.50` up is present once, and the pin is a tested version.

## Version Matrix

| VictoriaLogs version | Coverage path | Version-specific focus |
|---|---|---|
| `v1.53.0` | Scheduled and manual matrix (tested) | Newest tested backend. `/internal/force_merge`, `/internal/force_flush`, `/internal/log_new_streams` and `/internal/partition/*` require `POST` (the proxy does not call them). Not the PR CI pin; moving the pin is a separate change |
| `v1.52.0` | PR and main CI pinned runtime | Current pinned backend. Distroless image (no shell): compose health probes must exec the binary. `json_array_concat` pipe. Bare filter pipes starting with a non-word token or `not` are accepted again. Parse errors echo the query before the reason |
| `v1.51.1` | Scheduled and manual matrix (tested) | Cluster upgrade bridge: `vlstorage` v1.51.1 accepts `vlselect` v1.38.0 through v1.51.0; same LogsQL surface as v1.51.0 |
| `v1.50.0` | Scheduled and manual matrix (tested) | Stream fields in the column index (`field_values` fast path), dense pattern windowing |
| `v1.49.0` | Scheduled and manual matrix (tested); supported floor | Structured metadata shaping, volume endpoints |

## Edge Cases Covered

- Service name derived from labels when VictoriaLogs does not carry a native `service_name`
- `detected_fields` and `detected_field/<name>/values` derived from VictoriaLogs field content
- Loki `index/stats` backed by one VictoriaLogs `stats count(), count_uniq_hash(_stream_id)` row over the window (entries and streams as Loki counts them; `bytes` is an estimate, VictoriaLogs has no chunk accounting); `index/volume` and `index/volume_range` backed by VictoriaLogs `stats_query` / `stats_query_range` with `sum_len(_msg)` (bytes)
- Raw VictoriaLogs fields mapped into parsed fields or structured metadata without polluting stream labels

## Runtime Capability Profiles

The proxy passively detects backend version from upstream response headers and selects a capability profile. This keeps one binary safe across mixed backend versions while enabling newer LogSQL optimizations where available.

| VictoriaLogs version family | Capability profile | Stream metadata endpoints fast path (`stream_field_*`) | Metadata substring filter (`q` + `filter=substring`) | Dense patterns windowing profile |
|---|---|---|---|---|
| `v1.50.x+` | `vl-v1.50-plus` | enabled | enabled | enabled |
| `v1.49.x` | `vl-v1.49-plus` | enabled | enabled | disabled (conservative profile) |
| `v1.30.x` to `v1.48.x` (unsupported) | `vl-v1.30-plus` | enabled | enabled (always forwarded) | disabled (conservative profile) |
| `< v1.30.0` | `legacy-pre-v1.30` | disabled (fallback to generic `field_*`) | enabled (always forwarded) | disabled |
| unknown (before first upstream response) | `unknown` | enabled (optimistic default) | enabled (always forwarded) | disabled (safe default) |

Current code gates:

- pattern extraction window density for `/loki/api/v1/patterns`
- stream-metadata-first label inventory/value lookups (`/select/logsql/stream_field_names`, `/select/logsql/stream_field_values`)

As new LogSQL backend features land, this table and the capability derivation in proxy code should be updated together, with explicit tests per profile.

## Feature Capability Matrix (v1.49.0 To v1.53.0)

The table below tracks changelog-relevant LogSQL and metadata behavior between `v1.49.0` and `v1.53.0`, and how the proxy should treat each band.

| Version band | Backend capability signals | Limitations / risks to account for | Proxy handling policy |
|---|---|---|---|
| `v1.49.x` | `filter=substring` support for `field_names` / `field_values` and stream metadata browse endpoints; `stats_query_range` `offset` argument (since `v1.45`) | none on the supported floor | `q` + `filter=substring` is always forwarded and stats buckets are always anchored with `offset`; the proxy-side search window also serves the browse index and offset/limit paging |
| `v1.50.x` | parser/query fixes and latest metadata/query behavior | none specific beyond normal backend saturation limits | enable `vl-v1.50-plus` profile, including dense patterns windowing and newest metadata behavior |
| `v1.51.x` | `coalesce` pipe; `limit`/`offset` after `stats` on `stats_query`; `unpack_json` accepts JSON with leading spaces; `stats_query`/`stats_query_range` answer 502 when a storage node is unavailable | a filter pipe without the `filter` prefix is rejected unless it starts with `field_name:` | the translator emits `| filter` for every filter that follows a non-filter pipe |
| `v1.52.x` | distroless image; `json_array_concat` pipe; filters starting with a non-word token or `not` are accepted without the prefix again; `-search.maxQueueDuration` is honoured for queued requests; current pinned target | no shell in the image; parse errors echo the query before the reason (`cannot parse query arg [<query>]: <reason>`) | same `vl-v1.50-plus` profile; compose health probes exec `/victoria-logs-prod -version`; the upstream error classifier reads both parse-error layouts, so invalid queries stay Loki `400 bad_data` |
| `v1.53.x` | `/internal/force_merge`, `/internal/force_flush`, `/internal/log_new_streams` and `/internal/partition/*` require `POST`; `/delete/run_task` is `POST` only; Unix socket listener | none for the proxy: it uses only the select endpoints | same `vl-v1.50-plus` profile; newest tested version, not yet the PR CI pin |
| `< v1.49.0` | not supported | outside the supported versions; modern drilldown/explore contracts are not tested there, and the proxy keeps no fallbacks for them (for example `v1.44` and older ignore the `stats_query_range` `offset` argument) | block startup by default (`backend-min-version`), allow override with `backend-allow-unsupported-version=true` |

### Capability Profile Guidance

- `vl-v1.50-plus`: use for latest backend capabilities and densest safe pattern windowing.
- `vl-v1.49-plus`: the floor profile.
- `vl-v1.30-plus`: base profile for every version with the stream metadata endpoints; no supported version maps to it (the floor is `v1.49.0`).
- `legacy-pre-v1.30`: fallback-only profile for versions without them.

Profile names describe where a capability starts, not the support floor, so the floor moving does not rename them. A detected version below the floor logs a startup warning (`backend version is below the supported VictoriaLogs lines`) whatever `-backend-min-version` says; such a version is unsupported and untested.

When adding a new profile or changing a gate, update three places in the same PR:

- runtime derivation in `internal/proxy/backend.go`
- capability metadata in `test/e2e-compat/compatibility-matrix.json`
- this document section

## Backend Compression

**`backend-compression=auto` loopback detection**: When the VL backend URL resolves to a loopback address (`localhost`, `127.0.0.1`, `::1`), `auto` mode selects `identity` (no compression) to avoid the overhead of compressing and decompressing data on the same host. For remote backends, `auto` selects gzip. You can override with an explicit value (`gzip`, `zstd`, `none`).