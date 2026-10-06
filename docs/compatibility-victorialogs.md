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
- Runtime matrix: real VictoriaLogs images, the latest 3 releases of each supported line (`v1.47.0`, `v1.48.0`, `v1.49.0` and `v1.51.1`, `v1.52.0`, `v1.53.0`)

## Support Policy

VictoriaLogs support is by minor line, not by individual release:

- **Fully supported:** the latest line and the previous one. Today that is `v1.5x.x` and `v1.4x.x` (`v1.40.0` and newer). `v1.3x.x` and older are not supported.
- **Tested:** at most the latest 3 releases of each supported line, using the latest patch of each minor. Versions in a supported line that fall outside the tested three (`v1.40`-`v1.46` and `v1.50.x`) are supported through the capability gates below, which fall back for every feature introduced after `v1.40.0`, but they are not run in CI.
- **Floor:** `-backend-min-version` defaults to `v1.40.0`, the first minor of the oldest supported line. Startup fails when the detected backend is older, with an error naming the flag. With an explicit older `-backend-min-version` (for example a Helm value still carrying `v1.30.0`) or `-backend-allow-unsupported-version=true` the proxy starts, logs a warning on every start (`backend version is below the supported VictoriaLogs lines`, whatever the flag says), and the version is unsupported and untested. A backend whose version cannot be read only warns unless `-backend-version-strict=true`. See [Configuration](configuration.md).
- **Moving the window:** when a new line appears (for example `v1.6x`), it becomes current, the previous current line becomes previous, and the oldest line drops out. In one change: update `matrix_versions`, `support_window` and the capability profiles in `test/e2e-compat/compatibility-matrix.json`, the `-backend-min-version` default, `logsql.MinSupportedMinor`, and the pinned image. `TestVictoriaLogsSupportPolicy` fails until these agree and enforces "at most 3 per line, two adjacent lines".

## Version Matrix

| VictoriaLogs version | Coverage path | Version-specific focus |
|---|---|---|
| `v1.53.0` | Scheduled and manual matrix (tested) | Newest tested backend. `/internal/force_merge`, `/internal/force_flush`, `/internal/log_new_streams` and `/internal/partition/*` require `POST` (the proxy does not call them). Not the PR CI pin; moving the pin is a separate change |
| `v1.52.0` | PR and main CI pinned runtime | Current pinned backend. Distroless image (no shell): compose health probes must exec the binary. `json_array_concat` pipe. Bare filter pipes starting with a non-word token or `not` are accepted again. Parse errors echo the query before the reason |
| `v1.51.1` | Scheduled and manual matrix (tested) | Cluster upgrade bridge: `vlstorage` v1.51.1 accepts `vlselect` v1.38.0 through v1.51.0; same LogsQL surface as v1.51.0 |
| `v1.49.0` | Scheduled and manual matrix (tested) | Structured metadata shaping, volume endpoints |
| `v1.48.0` | Scheduled and manual matrix (tested) | Structured metadata shaping, volume endpoints |
| `v1.47.0` | Scheduled and manual matrix (tested) | Structured metadata shaping, volume endpoints |

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
| `v1.30.x` to `v1.48.x` | `vl-v1.30-plus` | enabled | disabled | disabled (conservative profile) |
| `< v1.30.0` | `legacy-pre-v1.30` | disabled (fallback to generic `field_*`) | disabled | disabled |
| unknown (before first upstream response) | `unknown` | enabled (optimistic default) | disabled (safe default) | disabled (safe default) |

Current code gates:

- pattern extraction window density for `/loki/api/v1/patterns`
- stream-metadata-first label inventory/value lookups (`/select/logsql/stream_field_names`, `/select/logsql/stream_field_values`)
- metadata search fan-in via `q` + `filter=substring` on `field_*` and `stream_field_*` endpoints

As new LogSQL backend features land, this table and the capability derivation in proxy code should be updated together, with explicit tests per profile.

## Feature Capability Matrix (v1.40.0 To v1.53.0)

The table below tracks changelog-relevant LogSQL and metadata behavior between `v1.40.0` and `v1.53.0`, and how the proxy should treat each band.

| Version band | Backend capability signals | Limitations / risks to account for | Proxy handling policy |
|---|---|---|---|
| `v1.40.x` to `v1.48.x` | improved cluster query behavior and partial-response handling in VictoriaLogs changelog line | still treat dense pattern extraction as opt-in to avoid long-range overload in mixed backends | same runtime profile as `vl-v1.30-plus`; prefer conservative pattern sampling |
| `v1.49.x` | adds `filter=substring` support for `field_names` / `field_values` and stream metadata browse endpoints | older versions do not support this parameter reliably | gate substring server-side filtering by backend capability; keep fallback filtering in proxy for older versions |
| `v1.50.x` | parser/query fixes and latest metadata/query behavior | none specific beyond normal backend saturation limits | enable `vl-v1.50-plus` profile, including dense patterns windowing and newest metadata behavior |
| `v1.51.x` | `coalesce` pipe; `limit`/`offset` after `stats` on `stats_query`; `unpack_json` accepts JSON with leading spaces; `stats_query`/`stats_query_range` answer 502 when a storage node is unavailable | a filter pipe without the `filter` prefix is rejected unless it starts with `field_name:` | the translator emits `| filter` for every filter that follows a non-filter pipe |
| `v1.52.x` | distroless image; `json_array_concat` pipe; filters starting with a non-word token or `not` are accepted without the prefix again; `-search.maxQueueDuration` is honoured for queued requests; current pinned target | no shell in the image; parse errors echo the query before the reason (`cannot parse query arg [<query>]: <reason>`) | same `vl-v1.50-plus` profile; compose health probes exec `/victoria-logs-prod -version`; the upstream error classifier reads both parse-error layouts, so invalid queries stay Loki `400 bad_data` |
| `v1.53.x` | `/internal/force_merge`, `/internal/force_flush`, `/internal/log_new_streams` and `/internal/partition/*` require `POST`; `/delete/run_task` is `POST` only; Unix socket listener | none for the proxy: it uses only the select endpoints | same `vl-v1.50-plus` profile; newest tested version, not yet the PR CI pin |
| `< v1.40.0` | not supported (`v1.3x` and older) | outside the supported lines; modern drilldown/explore contracts are not tested there | block startup by default (`backend-min-version`), allow override with `backend-allow-unsupported-version=true` |

### Capability Profile Guidance

- `vl-v1.50-plus`: use for latest backend capabilities and densest safe pattern windowing.
- `vl-v1.49-plus`: adds the metadata substring filter.
- `vl-v1.30-plus`: base profile for every version with the stream metadata endpoints; the supported part of it is `v1.40.0` to `v1.48.x`.
- `legacy-pre-v1.30`: fallback-only profile for versions without them.

Profile names describe where a capability starts, not the support floor, so the floor (`v1.40.0`) moving does not rename them. A detected version below the floor logs a startup warning (`backend version is below the supported VictoriaLogs lines`) whatever `-backend-min-version` says; such a version is unsupported and untested.

When adding a new profile or changing a gate, update three places in the same PR:

- runtime derivation in `internal/proxy/backend.go`
- capability metadata in `test/e2e-compat/compatibility-matrix.json`
- this document section

## Backend Compression

**`backend-compression=auto` loopback detection**: When the VL backend URL resolves to a loopback address (`localhost`, `127.0.0.1`, `::1`), `auto` mode selects `identity` (no compression) to avoid the overhead of compressing and decompressing data on the same host. For remote backends, `auto` selects gzip. You can override with an explicit value (`gzip`, `zstd`, `none`).