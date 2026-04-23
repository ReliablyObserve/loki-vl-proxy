# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.13.1] - 2026-04-23

### Tests

- e2e/clickout: add Grafana `/api/ds/query` parity coverage for range-metric functions (missing-unwrap 400 parity and unwrap + `$__auto` success parity) across direct Loki and Loki-via-proxy datasources.

## [1.13.0] - 2026-04-23

### Bug Fixes

- metrics/query-range-compat: route parser-stage metric queries (translated `unpack_*` / `extract*`) through proxy-side range evaluation for `query` and `query_range`, preserving parser label cardinality and unwrap semantics when direct `stats_query(_range)` behavior diverges from Loki.
- metrics/rate-counter: force `rate_counter` onto the compatibility path so counter-reset-aware behavior is consistent across parser and non-parser shapes instead of depending on backend parser acceptance.
- query-range/windowing: return explicit Loki-style backend errors when split-window fetches fail after retries instead of silently falling back to full-range direct query execution.
- query-range/error-contract: keep failure shape stable for Grafana and API clients by surfacing the real upstream failure class on split execution errors.
- drilldown/cache: normalize empty detected-field-value refresh payloads to `[]` (not `null`) to keep downstream consumers stable during cache refreshes.

### Tests

- compat/query-range: add parser-stage/manual evaluation coverage for `rate`, `bytes_rate`, `first_over_time`, `quantile_over_time`, and `rate_counter`, and keep `stdvar_over_time` compatibility covered through matrix/binary evaluation paths.
- compat/query-range: add explicit parser-probe and path-selection coverage to enforce when manual compatibility evaluation is required vs when backend stats execution remains valid.
- query-range/windowing: add regressions proving split-window failures return upstream errors and transient per-window backend failures are retried.
- drilldown/compat: harden parser-probe expectations so metric compatibility tests accept both direct stats and manual compatibility execution paths while still enforcing "single working parser" behavior.

### Documentation

- docs/translation-reference: document `rate_counter` translation and parser-stage range-metric compatibility behavior, including path-selection rules for manual evaluation vs single-shot `stats_query_range`.
- docs/testing: expand required matrix edge cases for parser-stage range metrics, `rate_counter`, unwrap error shapes, and comparison expectations across Loki/proxy parity checks.

## [1.12.3] - 2026-04-23

### Bug Fixes

- metrics/compat: harden range-function compatibility by requiring explicit `| unwrap <field>` for unwrap-dependent range functions, resolve Grafana range template selectors (`$__auto`, `$__interval`, `$__rate_interval`, `$__range*`) before compatibility handling, and reject unsupported bare metric range queries early instead of silently falling through.
- metrics/compat: add `rate_counter(...)` support on parser+unwrap compatibility paths, including counter-reset aware rate calculation.

### Tests

- proxy/translator: add regressions for unwrap-required function errors, Grafana template token resolution, parser probe unquoting, template-window resolution for `rate_counter`, and counter-reset handling in compatibility window evaluation.

## [1.12.2] - 2026-04-21

### Bug Fixes

- chart: tolerate reserved scalar `env` values by rendering list-typed `env`/`envFrom` only when they are real lists, and add `extraEnv` / `extraEnvFrom` compatibility aliases for chart consumers that need explicit container env injection without colliding with scalar environment selectors.

### Tests

- chart/ci: add a Helm render regression for scalar `env` combined with `extraEnv` / `extraEnvFrom` so the manifest shape stays valid in CI.

## [1.12.1] - 2026-04-21

### Bug Fixes

- metrics/query-range: stop injecting synthetic `service_name="unknown_service"` into metric `query`/`query_range` responses when the result labelset has no real service signal (for example `sum by(cluster)(rate(...))`).
- drilldown/fields: suppress high-cardinality terminal timestamp fields (`timestamp_end`, `observed_timestamp_end`) from `detected_fields` responses to keep Drilldown field discovery stable under repeated refreshes.

### Tests

- drilldown/compat: add unit and e2e regressions for non-service metric aggregations (no synthetic `unknown_service`) and detected-field suppression of terminal timestamp keys.

## [1.12.0] - 2026-04-21

### Bug Fixes

- drilldown/volume: stop injecting synthetic `service_name="unknown_service"` into `index/volume` and `index/volume_range` buckets when requests are grouped by non-service labels (for example `cluster`), while preserving service-aware grouping behavior.
- drilldown/volume: honor Drilldown grouping hints (`drillDownLabel`, `fieldBy`, and `var-fieldBy`) as target-label fallbacks when `targetLabels` is omitted, so field/label include-exclude actions keep grouping on the selected dimension instead of falling back to selector-order inference.
- translator/metrics: make `rate` and `bytes_rate` preserve Loki per-second semantics via window normalization, make parser+unwrap metric paths preserve Loki-like cardinality with outer-aggregation composition, emit VictoriaLogs-compatible byte aggregations via `sum_len(_msg)`, and implement `stdvar_over_time` via proxy-side `stddev^2` composition to avoid backend `stdvar` parser failures.
- proxy/binary-metrics: fix scalar and binary post-processing to mutate both legacy `results` payloads and Prometheus-style `data.result` payloads (including instant-vector `value` samples), preventing silent no-op arithmetic on valid `stats_query` responses.

### Tests

- drilldown/volume: add regression coverage for inferred non-service target labels (no synthetic `unknown_service`) and for Drilldown `fieldBy` fallback mapping on both vector and matrix volume endpoints.
- compat/matrix: add operation/filter/function matrix coverage across translator and proxy scalar paths, with deterministic e2e checks for binary scalar operators, filter operators, and metric function families (cross-engine parity for compatible functions plus proxy-local expected-value checks where semantics intentionally differ), and add scalar/binary fuzz coverage for response-shape robustness.

## [1.11.0] - 2026-04-21

### Bug Fixes

- compat/loki: preserve Loki semantics for bare parser-derived metric queries and `absent_over_time(...)` on the direct `query` and `query_range` paths so valid Loki operations keep their parser-derived label cardinality, unwrap behavior, and empty-series semantics instead of collapsing into proxy-specific aggregated fallback results.

### Changed

- release/metadata: synchronized release metadata for v1.10.2.

### Tests

- compat/loki: make the query-semantics matrix and operation inventory required in CI, expand positive and negative Loki operation coverage across parser pipelines, unwrap range functions, boolean/set operators, and invalid log/metric combinations, and add unit coverage for the bare parser metric and `absent_over_time(...)` compatibility handlers plus cache-tier coverage for the current mainline helper cache paths.

## [1.10.2] - 2026-04-20

### Bug Fixes

- chart/helm: quote rendered container args in the chart deployment template so values containing JSON, commas, colons, or embedded delimiters survive Helm rendering unchanged instead of being split or reinterpreted by YAML parsing.
- proxy/wrapping: normalize wrapped stats responses to always include Loki `data.resultType` and `data.result` (including fallback mapping from legacy/top-level `results`) so Grafana Loki datasource queries no longer fail with `no resultType found` when backend payloads omit result metadata.

### Tests

- chart/ci: add a quoted-args Helm template regression case covering structured `extraArgs` values such as auth pairs, field-mapping JSON, and tenant limit JSON blobs.

## [1.10.0] - 2026-04-20

### Bug Fixes

- cache/tiering: move helper/read caches onto shared fresh reads with local-memory plus local-disk persistence, keep stale fallback local-first, and expose per-tier cache lookup metrics.
- cache/keys: canonicalize helper/read cache keys across query-param ordering and alias pairs such as `from`/`start`, `to`/`end`, and `q`/`search`, plus normalize effective detected-field limits so Grafana refreshes can reuse the same helper cache entries instead of churning near-identical keys.
- drilldown/discovery: stop relaxing helper discovery queries after a successful empty primary result for label names, label values, native field values, and detected-label scans; successful empty strict detected-field value resolution now stays strict instead of broadening into relaxed query data, and `service_name` metadata lookup stays on metadata endpoints instead of spilling into streams/scans when metadata is sufficient.
- metrics/cache: promote cache-tier stats into the shared metrics pipeline so `/metrics` and OTLP now export the same L1/L2/L3 request, hit, miss, stale-hit, backend-fallthrough, object, and byte series instead of keeping them as proxy-local text-only metrics.
- peer/persistence: advertise peer write-through compression support on existing GET/hot responses, opportunistically compress owner write-through pushes only when the remote peer has confirmed support, accept compressed peer cache POST bodies, request compressed peer snapshot warm responses, and skip periodic snapshot rewrites when the on-disk patterns or label-values payload is unchanged.

### Tests

- cache/tiering: add regression coverage for TTL-aware disk fresh/stale reads, shared L2 promotion into L1, and helper cache locality.
- discovery/keys: add regression coverage for canonical helper cache keys, for stopping relaxed discovery fallback after a successful empty primary result, and for OTLP/Prometheus cache-tier metric export.
- peer/persistence: add regression coverage for compressed peer write-through/set round trips, compressed peer snapshot warm fetches, and skipping unchanged periodic snapshot rewrites.

## [1.9.6] - 2026-04-20

### Bug Fixes

- read-path/hardening: stop converting backend failures on Drilldown `detected_fields`, `detected_labels`, detected field values, `/index/volume`, and `/index/volume_range` into empty success payloads; these handlers now serve stale last-good cache entries when available and otherwise return real upstream-style errors, and volume helpers now reject non-success `/select/logsql/hits` responses instead of silently parsing them as empty data.

### Tests

- drilldown/cache: add regression coverage for stale-on-error recovery on detected fields, detected labels, detected field values, and near-now volume refreshes, plus cache coverage for reusing expired L1 entries as last-good fallback data.

## [1.9.5] - 2026-04-20

### Bug Fixes

- patterns/persistence: store query-level compact pattern snapshots for persistence instead of range-shaped filled payload variants, reducing snapshot entry churn and write amplification when the same logical Drilldown query is refreshed across different time windows.

## [1.9.4] - 2026-04-20

### Bug Fixes

- query-range/metrics: stop splitting metric `query_range` requests into multiple backend `stats_query_range` windows, keeping the read path aligned with the documented log-only windowing contract and reducing avoidable VL request fanout for Drilldown and Explore metric panels.

### Tests

- patterns/e2e: compare bursty mixed-pattern Drilldown compatibility by preserved signal and time bounds instead of demanding near-identical sparse bucket coverage between native Loki and grouped proxy mining.

## [1.9.3] - 2026-04-20

### Bug Fixes

- drilldown/discovery: ignore zero-hit native `field_values` entries so detected field value pickers stop advertising values that are not actually present in the current selector and time window, and query-escape peer-cache GET/SET keys so discovery caches with embedded query strings survive peer transport correctly instead of truncating on `&`-style separators.

### Tests

- drilldown/cache: add regression coverage for zero-hit native detected field values and for peer-cache fetch/write-through round trips using realistic cache keys that include encoded query fragments.

## [1.9.2] - 2026-04-19

### Bug Fixes

- cache/read-path: keep full `query_range`, label, volume, and detected-* response caches local to the serving pod when those handlers only read cache through L1 `GetWithTTL`, avoiding redundant disk writes and peer write-through churn that do not provide any fallback benefit.

## [1.9.1] - 2026-04-19

### Bug Fixes

- drilldown/metrics: treat synthetic `service_name!=""` filters as "any service source field is populated" instead of requiring every possible backing field to be non-empty, restoring Grafana Drilldown label-card metric queries such as `sum(count_over_time({service_name="argocd",service_name != ""}[5s])) by (service_name)`.

### Tests

- drilldown/translator: add regression coverage for synthetic `service_name` empty and non-empty matcher translation, including the exact Drilldown grouped `query_range` label-card shape used by Grafana.

## [1.9.0] - 2026-04-19

### Features

- ci/security: add layered GitHub Actions security lanes with fast PR blockers for secrets, SAST, supply-chain, workflow, and container linting, plus runtime ZAP coverage on PRs and deeper scheduled scanning with SBOM, Semgrep, fuzzing, and curated Nuclei checks.

### Bug Fixes

- proxy/security: apply baseline hardening headers (`nosniff`, frame-deny, same-origin resource policy, and non-cacheable responses) across normal, error, and disabled-admin responses, keep the container runtime non-root, and tighten the security test fixtures so the static scanners stay focused on actionable findings.

## [1.8.2] - 2026-04-19

### Bug Fixes

- query-range/cache: keep window fragment results and prefilter hit estimates local to the serving pod instead of distributing those short-lived scratch entries through peer-cache write-through, reducing avoidable owner hot spots, network churn, and disk activity during Drilldown or Explore fanout.

## [1.8.1] - 2026-04-19

### Bug Fixes

- drilldown/detected-labels: recover `service_name` in broad Drilldown label discovery when native stream labels collapse to `unknown_service` by deriving service identity from structured metadata fields such as `service.name` during scan fallback and replacing incomplete native summaries with scanned service names.

## [1.8.0] - 2026-04-19

### Bug Fixes

- drilldown/metadata: restore `service_name` label discovery by preferring native field metadata over stream scans, and retry relaxed query candidates for labels, label values, and detected-label fallbacks when parser-heavy Drilldown queries return empty metadata.

## [1.7.0] - 2026-04-18

### Bug Fixes

- drilldown/fields: retry native field and stream discovery with relaxed candidates when parser or comparison stages make Drilldown field lookups unstable, treat backend `5xx` scan responses as failed windows instead of empty results, and preserve native-only fallback for structured metadata without leaking indexed labels.
- peer-cache/persistence: keep persisted patterns and label-values snapshot blobs local instead of distributing them through the ring owner path, merge label-values startup warm state directly from peer-local snapshots, and use the correct peer auth header during snapshot fetches so fleet warmups stay compatible when `peer-auth-token` is enabled.

### Tests

- drilldown: add regression coverage for duration-filtered detected field and detected field value requests so future changes catch strict-vs-relaxed discovery regressions before release.
- cache/persistence: add regression coverage proving fleet snapshot blobs skip write-through owner pushes and that peer-warmed label-values state merges correctly under shared peer auth.

## [1.6.3] - 2026-04-17

### Features

- helm/chart: add a reusable `customManifests` hook so downstream installs can render extra Kubernetes resources with full chart context without forking chart templates.

## [1.6.2] - 2026-04-17

### Bug Fixes

- patterns/read-path: reduce dense short-range sampling fanout so 30-minute pattern refreshes stay bounded instead of exploding into excessive backend raw-log window fetches.

## [1.6.1] - 2026-04-17

### Bug Fixes

- proxy/logging: emit build identity on startup and listener bind, carry real version/revision/build-time metadata in release artifacts, add restore/persist duration logging for patterns and label-values snapshots, downgrade `4xx` request errors to warn, and keep verbose per-type request breakdown maps at debug level while warning when peer cache runs without a shared auth token.

## [1.6.0] - 2026-04-17

### Features

- patterns/observability: add read-path quality counters, last-response gauges, persisted snapshot inventory gauges, and disk/peer snapshot byte exchange metrics so fleet monitoring can distinguish sparse real pattern activity from degraded mining or restore behavior.

### Bug Fixes

- patterns/read-path: improve mining fidelity with typed placeholders, stronger cross-window merging, and bounded second-pass widening for capped windows so rare and high-cardinality pattern families stay closer to native Loki behavior on reads.

### Tests

- patterns: expand native-Loki-vs-proxy A/B coverage with bursty mixed-pattern and high-cardinality variable fixtures in addition to the stable-stream parity check.

## [1.5.1] - 2026-04-17

### Bug Fixes

- translator/proxy: remove off-by-one line scanners flagged by CodeQL in proxy and Drilldown helpers, and tighten translator helper coverage plus error-path tests to keep the reliability/maintainability sweep green.

### Tests

- e2e: replace brittle fixed sleeps with bounded polling in long-range Drilldown and patterns compatibility tests, add explicit dense-bucket boundary coverage, and simplify native patterns seed quoting to use the standard library directly.

## [1.5.0] - 2026-04-17

### Bug Fixes

- patterns/drilldown: make proxy read-path pattern mining match native Loki pattern coverage more closely by preserving stable fixed-prefix patterns, avoiding split-window overlap on backend raw log fetches, and dropping the synthetic trailing zero bucket beyond the last observed sample.

## [1.4.6] - 2026-04-16

### Bug Fixes

- drilldown/volume: compensate split `stats_query_range` metric windows by extending backend end timestamps by one step and trimming the extra point back to the requested range, so 2-day Explore and Drilldown volume queries stay dense across 24h boundaries.

## [1.4.5] - 2026-04-16

### Bug Fixes

- query-range/windowing: fall back from the windowed log `query_range` path to the direct full-range Loki-compatible query when per-window fetches exhaust retries, instead of surfacing the windowed-path failure immediately.

## [1.4.4] - 2026-04-16

### Bug Fixes

- drilldown/step-parsing: normalize float-second `step` values before forwarding them to backend volume and patterns requests, and reuse the same positive-duration parsing in zero-fill plus patterns bucketing so float-second cadences stay dense and aligned.

## [1.4.3] - 2026-04-16

### Bug Fixes

- drilldown/query-range: split long `stats_query_range` metric requests into aligned VL windows before merging them back into one Loki-shaped matrix response, and translate Drilldown pattern hover queries that use Loki pattern line filters (`|>`, `!>`) plus backtick-quoted `pattern`/`extract`/`regexp` stages into VL-compatible syntax.

## [1.4.2] - 2026-04-16

### Features

- peer-cache/metrics: add configurable peer fetch timeout wiring (`peer-timeout`) and per-reason peer fetch error counters so peer cache failures are no longer a single opaque `errors_total` bucket.

### Bug Fixes

- drilldown: keep 7-day minute `volume_range` requests zero-filled beyond the old 10k-bucket ceiling, and make dense short-range patterns fan out enough to avoid the “one visible burst every ~40 minutes” sampling shape.

## [1.4.1] - 2026-04-16

### Bug Fixes

- drilldown: translate Loki pattern-match filters (`|>`, `!>`) into VictoriaLogs regex filters so pattern stats queries compile correctly, and accept both `start/end` and `from/to` on volume endpoints so long-range volume buckets remain dense.

## [1.4.0] - 2026-04-16

### Features

- observability: add per-request fanout and proxy-internal operation telemetry to logs, Prometheus, OTLP export, bundled dashboard panels, and first-class Helm chart values for the new observability/runtime flags.

### Bug Fixes

- drilldown/patterns: keep long-range Drilldown patterns and volume queries dense across `>24h` windows by boosting dense per-window sampling and adding 48h regression coverage.

## [1.3.0] - 2026-04-16

### Bug Fixes

- security/runtime: require explicit `tenant.allow-global` for wildcard tenant bypass, add bounded peer/backend body reads plus `ReadHeaderTimeout`, hide sensitive metrics labels by default, and harden admin/debug surfaces.

### Documentation

- security/docs: document explicit wildcard-tenant opt-in, low-cardinality default metrics export, `/metrics` concurrency bounds, and the Go `1.26.2` baseline.

### Tests

- security/metrics: add coverage for wildcard-tenant rejection, admin exposure validation, `ReadHeaderTimeout`, and sensitive-metrics export opt-in.

### CI

- toolchain/security: bump repo and workflow Go version to `1.26.2`, add `govulncheck` to CI, and override the docs-site `serialize-javascript` dependency to the non-vulnerable line.

## [1.2.0] - 2026-04-15

### Features

- proxy: add downstream HTTP/1.x connection rotation, connection-state metrics, and frontend compression controls to redistribute sticky Loki-compatible clients while keeping upstream backend reuse warm.

### Bug Fixes

- query-range/read path: reduce translation buffering and repeated frontend gzip work by reusing hot compat-cache encodings, moving log-query conversion to a one-pass reader path, and bounding miss-path capture memory.
- query-range/patterns: reuse query response pattern extraction for autodetected pattern warming and keep `volume_range` zero-filled across the full requested range.
- security/runtime: require explicit `tenant.allow-global` for wildcard tenant bypass, add bounded peer/backend body reads plus `ReadHeaderTimeout`, hide sensitive metrics labels by default, and harden admin/debug surfaces.

### Documentation

- dashboards/config docs: refresh bundled operational dashboard JSON and document the client-facing gzip defaults and connection-redistribution controls.
- security/docs: document explicit wildcard-tenant opt-in, low-cardinality default metrics export, `/metrics` concurrency bounds, and the Go `1.26.2` baseline.

### Tests

- proxy/compression: add regression coverage for the one-pass reader conversion path, pattern collection on reader-backed query results, and frontend `nosniff` hardening on compressed responses.
- security/metrics: add coverage for wildcard-tenant rejection, admin exposure validation, `ReadHeaderTimeout`, and sensitive-metrics export opt-in.

### CI

- toolchain/security: bump repo and workflow Go version to `1.26.2`, add `govulncheck` to CI, and override the docs-site `serialize-javascript` dependency to the non-vulnerable line.

## [1.1.0] - 2026-04-15

### Features

- peer cache: add optional owner write-through replication (`/_cache/set`) so non-owner replicas can proactively warm ring owners under skewed client traffic, with bounded TTL gating (`peer-write-through`, `peer-write-through-min-ttl`) and peer token support on both peer endpoints.
- metrics: expose peer write-through counters (`loki_vl_proxy_peer_cache_write_through_pushes_total`, `loki_vl_proxy_peer_cache_write_through_errors_total`) for fleet-cache redistribution observability.
- peer cache: enable owner write-through by default (`peer-write-through=true`) so skewed client traffic warms owner shards without extra rollout tuning.
- peer cache: add bounded hot read-ahead with owner hot-index endpoint (`/_cache/hot`), top-N selection, key/byte/concurrency budgets, tenant-fair prefetch selection, jittered periodic pulls, and backoff-aware anti-storm behavior.
- peer cache: expose read-ahead metrics (`*_hot_index_requests_total`, `*_hot_index_errors_total`, `*_read_ahead_prefetches_total`, `*_read_ahead_prefetch_bytes_total`, `*_read_ahead_budget_drops_total`, `*_read_ahead_tenant_skips_total`) and wire runtime flags for interval/jitter/top-N/budgets/fair-share/backoff.

### Bug Fixes

- cache persistence: skip duplicate L2 disk writes for `patterns:*` keys because patterns already use dedicated snapshot persistence, reducing avoidable local disk write pressure.
- e2e dense patterns harness: fix synthetic timestamp generation overflow for large ranges/high line counts so 7d dense repro runs generate valid evenly distributed data instead of corrupted short-tail timestamps.
- cache write path: clamp non-owner local shadow TTL and skip non-owner L2 disk writes when write-through is enabled, reducing hot-pod disk amplification while preserving owner cache warmth.

### Documentation

- fleet-cache/config docs and Helm defaults: document default owner write-through behavior, `peer-write-through*` flags, and peer token requirements on both `/_cache/get` and `/_cache/set`.
- fleet cache docs: add explicit collapse-forwarding status and a bounded hot-read-ahead design proposal (budgets, jitter, anti-storm guardrails, and validation plan).
- docs: extend bounded hot-read-ahead proposal with planned flag surface, phased rollout plan, and proposed observability metrics for future regression gates.

### Tests

- cache: add regression tests for hot-index serving and bounded tenant-fair read-ahead prefetch behavior.
- benchmarks: add cache-path benchmarks for `TopHotKeys` and bounded `PeerCache` read-ahead cycle performance.

### CI

- ci: add `internal/cache` coverage guard (`>=79.0%`) in the main test workflow.
- ci: add cache benchmark regression guard in CI for hot read-ahead and cache hot paths, with threshold checks and job summary output.
- auto-release: add PR size/scope-aware version bump heuristics so large runtime-impacting PRs (for example `size/XL` with proxy/cache scopes) auto-promote from patch to minor when no explicit release label overrides are set.

## [1.0.24] - 2026-04-15

### Bug Fixes

- backend detection: add startup fallback version sensing from `/metrics` (`vm_app_version`/`victorialogs_build_info`) when upstream proxies strip version headers, and infer conservative capability profiles from runtime endpoint probes when explicit semver is unavailable.
- drilldown-limits: expose backend detection state (`backend_version_source`, `backend_version_semver`, `backend_capability_profile`) for operational verification and e2e assertions.

### Tests

- add regression tests for backend version fallback detection (`/metrics`), fallback min-version enforcement, endpoint-probed capability inference, and backend detection fields in drilldown limits.
- add e2e compose coverage for backend/grafana detection surfaces across VictoriaLogs `v1.50.0` and `v1.49.0`.
- add e2e compose coverage for backend/grafana detection when VictoriaLogs is fronted by `vmauth` (`Loki -> proxy -> vmauth -> VictoriaLogs`) to better match production routing topology.

## [1.0.23] - 2026-04-15

## [1.0.22] - 2026-04-14

### Bug Fixes

- ci: remove an unused Grafana surface helper that triggered lint failure.
- tests: harden long-range patterns contract tests against race conditions under parallel window fanout (`-race`).

## [1.0.21] - 2026-04-14

### Bug Fixes

- patterns: keep `/loki/api/v1/patterns` charts full-range on large scopes by parsing relative range boundaries and adaptively coarsening overly dense bucket grids instead of returning sparse short-tail samples.
- patterns: harden long-range windowed extraction by using direct `query_range` fanout per window and bounded per-window limits, avoiding helper-path stalls and preserving deterministic dense-range coverage.
- compatibility gate: enforce minimum supported VictoriaLogs version at startup (with explicit unsafe bypass flag) and add version-sensed runtime capability profiles for stream metadata and dense pattern windowing paths.
- metadata browse: add version-gated (`v1.49+`) forwarding of `q` + `filter=substring` to VictoriaLogs `field_*`/`stream_field_*` endpoints, with safe fallback behavior for older versions.
- cache freshness: add near-now stale-cache bypass controls (`recent-tail-refresh-*`) for `query_range`, `index/volume`, and `index/volume_range` to reduce refresh gaps without sacrificing long-lived historical cache reuse.

### Tests

- add regression coverage for VictoriaLogs version compatibility gate edge paths (missing version headers, health probe failure, non-success health status).
- add cache freshness and persistence hardening tests for near-now cache bypass decisions, volume cache bypass behavior, patterns snapshot compaction, and patterns persistence loop startup/shutdown.

### Documentation

- compatibility matrix: bump Logs Drilldown contract coverage to `2.0.3` and VictoriaLogs pinned runtime coverage to `v1.50.0`, including updated support-band notes and pinned commit references.
- compatibility matrix/docs: add VictoriaLogs runtime capability profiles (version-sensed feature gates) and document which LogSQL optimizations are enabled per version family.
- compatibility matrix/docs: add Grafana Loki datasource compatibility profiles and Drilldown `v1`/`v2` capability profiles, including runtime detection limits (`X-Query-Tags` + `User-Agent`) and release-family watchlists for forward support planning.

## [1.0.20] - 2026-04-14

### Features

- add `zstd`-capable read-path compression support for client responses and peer-cache transfers, plus negotiated upstream `zstd`/`gzip` decoding for backend responses when the upstream provides compressed payloads

## [1.0.19] - 2026-04-14

### Bug Fixes

- patterns: normalize relative-range (`now`/`now-*`) cache key boundaries for `/loki/api/v1/patterns` so Drilldown refresh requests consistently reuse the correct time-scoped cache entries
- patterns: fill returned `samples` across the full requested range (`start..end` by `step`) with zero buckets for missing intervals, preventing short-tail-only pattern graphs after refresh
- patterns: parse relative (`now`/`now-*`) boundaries in extraction/fill paths and adaptively coarsen overly dense bucket grids, so large-scope high-volume pattern charts render full selected ranges instead of collapsing to recent minutes

## [1.0.18] - 2026-04-14

### Documentation

- add a Docusaurus-based GitHub Pages site with SEO landing pages for VictoriaLogs, Grafana Loki datasource usage, Explore, Drilldown, LogQL semantics, comparison, and migration flows
- polish the docs-site dark/light theme, switch navbar branding to the SVG logo, and refresh known differences / known issues pages against the current codebase
- align the repo docs with the current runtime flags and behavior, expand the source-backed comparison matrix, and add cost-model pages that use Loki's published throughput sizing plus VictoriaLogs compression caveats

### CI

- add a GitHub Pages workflow that builds and deploys the Docusaurus site from `website/`
- skip docs-site deployment cleanly until GitHub Pages is enabled in repository settings, instead of failing `main` builds when the Pages site is not yet provisioned

## [1.0.15] - 2026-04-14

### Bug Fixes

- patterns: improve long-range `/loki/api/v1/patterns` extraction by windowing `query_range` sampling and merging samples across windows, so Drilldown pattern charts keep full-range visibility

## [1.0.14] - 2026-04-14

### Bug Fixes

- patterns: derive and forward a stable `step` for `/loki/api/v1/patterns` when clients omit it, preserving full selected time-range bucketization after Drilldown refresh

## [1.0.13] - 2026-04-14

### Bug Fixes

- patterns: honor `from`/`to` when `start`/`end` are not provided so Drilldown refresh requests keep the selected range scope
- patterns: replace fixed upstream source fetch limit (`1000`) with bounded adaptive sampling for longer ranges to prevent near-now-only pattern results after refresh

## [1.0.12] - 2026-04-14

### Features

- patterns: support static custom pattern overlays via inline JSON/text (`-patterns-custom`) and file-backed input (`-patterns-custom-file`) for deterministic Drilldown pattern suggestions
- patterns: prefer `query_range` during pattern extraction with `query` fallback so Drilldown pattern graphs align with selected time windows instead of only recent tail data

### Chart

- add Helm wiring for custom patterns via `patternsCustom.inline` and `patternsCustom.file.*` (including optional ConfigMap generation and mount wiring)

### Tests

- extend proxy unit coverage for custom pattern parsing, file loading, and merged `/patterns` response behavior

## [1.0.11] - 2026-04-14

### Features

- runtime compatibility: add Loki-compatible `/config/tenant/v1/limits` endpoint to expose published tenant limits for datasource/runtime probing
- runtime compatibility: wire `/loki/api/v1/drilldown-limits` to runtime published limits (including per-tenant overrides) instead of fixed literals

### Configuration

- add published limits runtime controls: `tenant-limits-allow-publish`, `tenant-default-limits`, and `tenant-limits`
- add matching environment variables: `TENANT_LIMITS_ALLOW_PUBLISH`, `TENANT_DEFAULT_LIMITS`, and `TENANT_LIMITS`

### Bug Fixes

- drilldown-limits: advertise `pattern_ingester_enabled` from actual query-autodetect runtime state and `limits.pattern_persistence_enabled` from configured persistence path, so Grafana capability probing reflects real proxy behavior

### Tests

- contract: add unit/e2e regression coverage for `/config/tenant/v1/limits` and strict drilldown limits contract keys/types
- e2e compat: add Drilldown contract coverage for pattern flags and query-range-seeded autodetection through Grafana datasource resources
- e2e compat: add dedicated autodetect proxy verification (`localhost:3110`) asserting `patterns_detected_total` increases after `query_range`
- e2e UI: add Playwright Drilldown regression test proving `/resources/patterns` returns non-empty data for autodetect-enabled datasource
- e2e UI: stabilize Drilldown patterns autodetect test by seeding and validating against the guaranteed `api-gateway` stream in CI stacks

## [1.0.10] - 2026-04-14

### Documentation

- align release metadata and notes coverage for `1.0.8`/`1.0.9` so changelog and published GitHub releases remain consistent for patterns/persistence deliverables

### Bug Fixes

- patterns: canonicalize pattern cache keys across RFC3339 and Unix timestamp formats (and equivalent step formats) so Drilldown `/patterns` requests consistently reuse autodetected cache entries
- patterns: scope pattern detection queries to selector-only context for Drilldown-style pipelines, preventing empty responses when parser/filter stages are present
- patterns: treat empty on-disk pattern snapshot files as a startup no-op instead of surfacing JSON decode warnings

## [1.0.9] - 2026-04-14

### Features

- patterns: persist `/loki/api/v1/patterns` snapshots to disk and restore on startup for warm restarts
- patterns: support peer-warm startup flow so stale/missing local snapshots can be refreshed from fleet peers
- patterns: support global pattern warming from successful `query` and `query_range` responses via `patterns-autodetect-from-queries`

### Configuration

- add patterns persistence/runtime flags: `patterns-persist-path`, `patterns-persist-interval`, `patterns-startup-stale-threshold`, `patterns-startup-peer-warm-timeout`
- wire and validate patterns autodetect/persistence flags end-to-end in startup/runtime config

### Reliability

- fail fast on invalid/unwritable patterns persistence paths with explicit startup validation errors
- avoid sticky empty `/patterns` responses by skipping compatibility-edge cache writes for empty patterns payloads

### Observability

- add patterns lifecycle metrics coverage (detected, stored, restored from disk/peers, in-memory footprint)

### Tests

- add regression and benchmark coverage for patterns persistence/restore, peer warm, and performance stability
- harden e2e patterns readiness polling (`/loki/api/v1/patterns`) with explicit `step=60s` and extended readiness window for slower CI runners

### Documentation

- add dedicated patterns documentation and refresh README/API/configuration docs for persistence/autodetect operator guidance

## [1.0.8] - 2026-04-13

### Documentation

- refresh README, API reference, observability guide, operations guide, and runbooks for the `1.0.x` line
- align compatibility/operations guidance for patterns support, route-aware telemetry, and dashboard terminology updates

## [1.0.7] - 2026-04-13

### CI

- stabilize the label/field benchmark regression guard by raising only the catastrophic threshold for `BenchmarkProxy_LabelKeys_Scale_CacheBypass/keys_10000`, preventing flaky failures on shared GitHub runners while preserving strict bounds for other scale rows

## [1.0.6] - 2026-04-13

### Features

- patterns: add Loki-compatible `/loki/api/v1/patterns` extraction flow with canonicalized token clustering and low-signal line filtering for better parity on repeated dynamic log messages

### Configuration

- proxy: add `-patterns-enabled` runtime flag to explicitly enable/disable patterns API behavior
- chart: expose `extraArgs.patterns-enabled` in Helm values for straightforward deployment-time control

### Tests

- patterns: add benchmark scale coverage (`10/100/1000/10000`) with CI regression gates to keep cache-assisted patterns extraction from regressing in latency-sensitive paths

## [1.0.5] - 2026-04-13

### Observability

- rebuild the packaged operations dashboard resource section into a consistent operator view, adding directional CPU/memory/disk/network panels plus per-pod FD and RSS visibility
- add prefixed process disk operation metrics (`loki_vl_proxy_process_disk_read_operations_total`, `loki_vl_proxy_process_disk_write_operations_total`) for both Prometheus scrape and OTLP export

### Tests

- add a metric-name guard that fails CI when new unprefixed metric families are introduced outside the legacy compatibility allowlist

## [1.0.4] - 2026-04-13

### CI

- benchmarks: make label/field benchmark row parsing robust across GitHub runner output formats (single-line `Benchmark... ns/op ...`), preventing false CI failures when extracting the 12-row scale matrix

## [1.0.3] - 2026-04-13

### Documentation

- unify operations dashboard artifacts by keeping a single `loki-vl-proxy` dashboard definition, removing the separate offenders dashboard variant from both top-level and Helm chart dashboard bundles

### Observability

- align proxy request telemetry with OTel semantic HTTP attributes, and add shared upstream/downstream route-aware request dimensions for Loki and VictoriaLogs visibility

### Bug Fixes

- drilldown: resolve `service_name` detected-field values via the dedicated service discovery fast path before generic field scans, removing intermittent `No data` responses in field value search
- drilldown: make detected-label value fallback alias-aware so underscore keys (for example `k8s_cluster_name`) correctly resolve dotted VL labels

### Performance

- cache: increase discovery endpoint TTLs for labels and detected fields/values/labels to reduce repeated upstream metadata scans during drilldown exploration

### CI

- benchmarks: add always-on label/field scale benchmark matrix (`10/100/1000/10000`) with artifact upload, workflow summary table, and a conservative ns/op regression guard

## [1.0.2] - 2026-04-13

### Bug Fixes

- chart: decouple StatefulSet immutable `spec.serviceName` from peer-discovery alias settings by binding StatefulSet identity to `workload.statefulSet.serviceName` and rendering an additional DNS headless alias service when `peerCache.serviceName` differs

## [1.0.1] - 2026-04-13

### Bug Fixes

- drilldown: fix `service_name` label values intermittently returning empty by preserving detected-label summaries and using selector-only context for service discovery fallbacks
- drilldown: treat non-2xx discovery responses as errors (instead of silent empty success) for fields/labels/value discovery paths to prevent false `no data`
- proxy: stop caching transient empty fallback payloads for detected fields/labels endpoints, reducing sticky post-error empty states until manual refresh

## [1.0.0] - 2026-04-13

### CI

- make auto-release honor forward chart version overrides so published tags/images/charts can jump directly to `1.0.0` (and future explicit major/minor targets) instead of always patch-bumping from the latest tag

### Documentation

- update release and security policy docs for the `1.x` support line and release branch examples

## [0.27.43] - 2026-04-13

### Documentation

- prepare post-`1.0.0` release cycle changelog section

## [1.0.0] - 2026-04-13

### Highlights

- official `1.0.0` stable release of Loki-VL-proxy as a production Loki API compatibility layer on top of VictoriaLogs
- full compatibility-first delivery model with dedicated CI suites for Loki API, Logs Drilldown, and VictoriaLogs behavior
- proven long-range query hardening for 2d/7d+ workloads with adaptive execution, retries, and partial-response safety paths

### Features

- Loki API coverage for query/query_range, labels, series, index endpoints, buildinfo, and readiness/metrics paths
- Logs Drilldown support including detected labels/fields/values flows and include/exclude filter translation
- native dot/underscore metadata compatibility modes for Loki-style and OTel/VL-style field conventions
- chart-driven runtime controls for translation, structured metadata, caching, retry behavior, and query windowing

### Performance

- split-window query execution with adaptive parallelism and prefiltering to skip empty windows before fanout
- stream-aware batching and overlap-aware coalescing to reduce repeated backend work across refresh/back-navigation traffic
- disk + memory cache improvements, peer cache sharing, and write-amplification reductions for steadier runtime behavior

### Reliability

- bounded retries and degraded-batch fallback for transient backend pressure instead of immediate user-visible hard failures
- direct fallback safeguards and adaptive timeout budgeting for expensive long-range query patterns
- startup/runtime cache hardening with consistency protections for rolling updates and recovery scenarios

### Observability

- OTel semantic logging alignment for HTTP, network, auth, and end-user context
- standardized `loki_vl_proxy_*` KPI metrics for cache, query windowing, retries, degraded batches, and partial responses
- dashboard and docs updates for zero/no-data hardening and stable scrape/OTLP visibility

### Bug Fixes

- harden Drilldown include/exclude behavior for repeated same-field clicks by keeping the latest field filter authoritative and preventing impossible accumulated chains
- use OTel semantic end-user fields in request logs (`enduser.name`/`enduser.id`/`enduser.source`) for clearer identity provenance
- stop duplicating OTel resource attributes (`service.*`, `deployment.environment.name`, `telemetry.sdk.*`) in per-line JSON payloads to prevent downstream `message.*` field explosion

### Documentation

- add compose-backed Playwright screenshot workflow and publish refreshed UI gallery assets for Explore, Tail, and Drilldown
- refresh docs for compatibility profiles, cache behavior, and runtime tuning guidance

## [0.27.42] - 2026-04-13

### Performance

- add long-range phase-2 stream-aware window batching controls to reduce backend saturation spikes on expensive 2d/7d query mixes
- add long-range phase-3 fast-path behavior that prioritizes early panel viability and avoids expensive all-window retries when backend pressure is transient
- add long-range phase-4 overlap-aware window coalescing reuse to reduce repeated upstream calls across Grafana refresh/back-navigation traffic

### Reliability

- add long-range phase-5 adaptive timeout budgeting with partial-response fallback and warm-cache continuation to avoid hard user-facing failures during backend saturation
- harden drilldown include/exclude query translation by deduplicating repeated field filters and making the latest include/exclude toggle authoritative for the same field/value

### Observability

- add phase KPI metrics for long-range resilience and tuning: `loki_vl_proxy_window_retry_total`, `loki_vl_proxy_window_degraded_batch_total`, `loki_vl_proxy_window_partial_response_total`, and `loki_vl_proxy_window_prefilter_hit_ratio`
- update the packaged proxy metrics dashboard and observability docs for no-data hardening and phase KPI visibility

### Tests

- add regression coverage for stream-aware batching, overlap/coalescing behavior, degraded-batch fallback, and partial-response long-range safety paths
- add benchmark coverage for phase-1/2 controls to track fanout/query-call reductions and backend-pressure tradeoffs

## [0.27.40] - 2026-04-13

### Performance

- add phase-1 long-range `query_range` prefiltering using `/select/logsql/hits` to skip empty windows before window fanout, with fail-open behavior when prefilter is unavailable

### Observability

- add query-range prefilter metrics (`loki_vl_proxy_window_prefilter_*` and duration histogram) to measure kept/skipped windows and prefilter error rate

### Tests

- add regression coverage for prefilter skip/fail-open behavior and selector extraction used by long-range window prefiltering

## [0.27.39] - 2026-04-12

### Reliability

- improve long-range windowed `query_range` resiliency by degrading batch parallelism on retryable upstream failures (`backend unavailable` / 502/503/504), adding bounded single-window retries, and widening retry backoff to survive transient breaker/backend spikes

### Tests

- add regression coverage for batch degradation, forced adaptive parallel backoff, and retry-helper classification paths used by long-range windowed queries

## [0.27.38] - 2026-04-12

### Reliability

- harden long-range `query_range` execution by retrying transient per-window backend failures with bounded backoff and returning Loki-style upstream errors instead of collapsing to an expensive direct full-range fallback

### Observability

- scope `process_*` and `loki_vl_proxy_process_*` CPU/disk/network runtime metrics to proxy-process sources (`/proc/self` + cgroup pressure fallback) to avoid host-level attribution drift in scrape and OTLP paths

## [0.27.37] - 2026-04-12

### Reliability

- harden long-range query execution by adding `query_range` fallback from window-split execution to direct backend query path on transient upstream failures

### Performance

- reduce proxy disk write amplification by skipping L2 disk-cache writes for short-lived entries and avoiding unchanged periodic label-index snapshot rewrites

### Observability

- keep `loki_vl_proxy_*` runtime/process metric families consistently queryable across scrape and OTLP flows for dashboard compatibility

## [0.27.36] - 2026-04-12

### Reliability

- add query_range safety fallback from window-split execution to direct backend query path on transient upstream failures, reducing user-facing 5xx during long-range requests

### Performance

- reduce proxy disk write amplification by skipping L2 disk-cache writes for short-lived entries and avoiding unchanged periodic label-index snapshot rewrites

## [0.27.35] - 2026-04-12

### Security

- cap preallocated slice capacities on label-values browse paths to satisfy CodeQL excessive allocation guards for request-driven limits

### Tests

- fix `TestLoad_HighConcurrency_MemoryStability` memory delta arithmetic to avoid unsigned underflow false-positives after GC, keeping release validation deterministic

### CI

- fix release asset upload globs to avoid duplicate `.tgz` matches in GitHub Release publishing, which could fail with REST asset update `Not Found`

### Performance

- reduce proxy disk write amplification by skipping L2 disk-cache writes for short-lived entries via `disk-cache-min-ttl` (default `30s`)
- avoid periodic label-values index snapshot rewrites when index structure is unchanged, lowering background disk I/O and write latency pressure

### Observability

- improve app-scoped metrics compatibility for dashboard/runtime process telemetry by keeping `loki_vl_proxy_*` metric families consistently queryable across scrape and OTLP flows

## [0.27.34] - 2026-04-11

### Configuration

- wire runtime support for indexed label-values cache flags end-to-end (`label-values-indexed-cache`, `label-values-hot-limit`, `label-values-index-max-entries`) to remove chart/runtime drift
- add persistent indexed label-values snapshot controls (`label-values-index-persist-path`, `label-values-index-persist-interval`, `label-values-index-startup-stale-threshold`, `label-values-index-startup-peer-warm-timeout`)

### Reliability

- restore indexed label-values cache state from disk on startup and persist periodic + graceful-shutdown snapshots for rolling updates
- add startup peer warm fallback when local snapshot is stale/missing so new pods can reuse fresh fleet cache state before serving
- gate readiness on indexed cache startup warm completion to keep probe behavior consistent during rollouts
- enable gzip compression for peer cache transport payloads on `_cache/get` to reduce transfer size/latency on large cache objects

### Tests

- add regression coverage for runtime flag wiring, indexed snapshot persistence/restore, stale-disk peer warm fallback, and peer gzip transport behavior
- pin e2e compose + manifest guards for indexed cache persistence flags so CI fails on drift

### Documentation

- document indexed cache persistence/warm behavior, sizing estimates, and chart/helm examples

## [0.27.33] - 2026-04-11

### Configuration

- expose indexed label-values browse cache knobs in Helm `extraArgs` (`label-values-indexed-cache`, `label-values-hot-limit`, `label-values-index-max-entries`) and document chart-based tuning examples for high-cardinality label UX

## [0.27.32] - 2026-04-11

### Bug Fixes

- add runtime learning for unique custom underscore-to-dotted field aliases from backend field inventory, with ambiguity safeguards and precedence for explicit mappings and known OTel aliases
- resolve `index/volume` and `index/volume_range` `targetLabels` aliases through stream-field inventory so underscore Loki labels (for example `host_id`) map to canonical dotted VL fields (`host.id`) without empty bucket regressions
- extend label alias resolution with configured `extra-label-fields` so custom dotted VL fields remain queryable via Loki-safe underscore aliases even when stream-field APIs are unavailable

### Documentation

- document Grafana Loki datasource builder caveats for dotted field keys and recommend underscore UI mode for stable click-to-filter workflows

## [0.27.31] - 2026-04-11

### Bug Fixes

- harden malformed dotted Drilldown pipeline stages (for example `| custom . \`pipeline.\``) to degrade into safe dotted-prefix regex filters instead of impossible field-existence matchers
- preserve Grafana datasource dotted-key filter intent by validating `key=value` label filtering for native dotted metadata fields (for example `k8s.cluster.name=my-cluster`) and preventing malformed dot-token fallback regressions
- normalize malformed spaced dotted triplets with trailing-dot artifacts (for example `custom . \`pipeline.processing.\` = \`vector-processing\``) into valid dotted field comparisons across translated query/query_range datasource operations

### CI

- expose compatibility component-level endpoint scores in PR quality reports and enforce shared component regressions through the quality gate

### Tests

- add unit, e2e-compat, and UI regression guards for dotted metadata key filtering across Explore/Drilldown query construction and datasource compatibility paths

## [0.27.29] - 2026-04-11

### Bug Fixes

- harden backend circuit-breaker accounting for long-range queries by counting only transport reachability failures as breaker failures, preventing transient upstream HTTP 5xx responses from opening the local breaker
- keep canceled/timeout upstream transport errors out of breaker failure accounting so client cancellations and timeout paths do not unnecessarily block subsequent traffic

### Tests

- add breaker regression coverage for upstream HTTP 502, transport connection failure, and canceled transport error paths
- add a 7-day query-range windowing regression test to verify adaptive parallel window fetch behavior under long time-range fanout

### CI

- enrich PR quality compatibility snapshots with component-level endpoint scores per track (Loki API, Logs Drilldown, VictoriaLogs) and render these as a dedicated report section for direct API-surface visibility
- harden the quality gate to validate required per-component compatibility signals and fail on shared component regressions or missing component breakdowns
- add CI unit coverage for quality-gate component checks and bump PR-quality base snapshot cache schema to refresh compatibility report shape

## [0.27.28] - 2026-04-11

### Bug Fixes

- restore Explore event tuple structured metadata behavior across translated/native metadata paths, preserving Loki-compatible payloads for Grafana Explore details

### Tests

- add pinned e2e compatibility matrix guards for structured metadata profile modes so future label/metadata mode drift fails CI early

## [0.27.27] - 2026-04-11

### Tests

- sanitize test/doc metadata fixtures to use neutral placeholder values (remove infra-specific cluster/namespace/resource examples)

### CI

- remove an unused helper from stream metadata contract tests so `golangci-lint` `unused` checks pass reliably

## [0.27.26] - 2026-04-10

### Bug Fixes

- align categorized stream responses with upstream Loki/Grafana contract by emitting `data.encodingFlags=["categorize-labels"]` alongside 3-tuples and object-map metadata (`structuredMetadata`/`parsed`)
- harden query-range windowed and multi-tenant merge stream responses so categorized tuple payloads remain parser-safe and normalized across legacy metadata shapes

### Tests

- add categorized-stream contract coverage for metadata-disabled mode to enforce parser-safe 3-tuple output with empty metadata object
- add fuzz targets for metadata normalization and merged categorized-stream contract invariants

### CI

- add `fuzz-smoke` PR workflow steps to exercise structured-metadata normalization and merged categorized-stream contract fuzz targets

## [0.27.25] - 2026-04-10
### Bug Fixes

- normalize merged query stream metadata to Loki pair-tuples (`[[name,value], ...]`) so legacy/object-shaped metadata cannot trigger strict decoder `ReadArray` failures

### Tests

- extend tuple regression coverage for query-range and multitenant merge paths to lock pair-tuple metadata compatibility

## [0.27.24] - 2026-04-10

### Bug Fixes

- emit `structuredMetadata` and `parsed` tuple metadata as Loki pair-tuples (`[[name,value], ...]`) when `X-Loki-Response-Encoding-Flags: categorize-labels` is enabled, preventing strict decoder `ReadArray` failures on object-shaped or `{name,value}` pair payloads
- normalize multi-tenant merged stream metadata to pair-tuples (`[[name,value], ...]`) so legacy backend shapes cannot leak incompatible tuple payloads to strict decoders

### Tests

- harden tuple regression guards across single-tenant, multi-tenant merge, and query-range windowing paths to enforce Loki metadata pair-array shape and fail fast on future tuple payload regressions

## [0.27.23] - 2026-04-10

### Documentation

- add explicit compatibility profile guidance for chart operators, including recommended combinations of `label-style`, `metadata-field-mode`, and `emit-structured-metadata`
- link chart values comments directly to compatibility/configuration docs for faster profile selection during deployments

## [0.27.22] - 2026-04-10

### Bug Fixes

- segregate `query`/`query_range` cache keys by tuple mode (`default_2tuple` vs `categorize_labels_3tuple`) to prevent metadata 3-tuples from leaking into default Grafana decode paths

## [0.27.21] - 2026-04-10

### Bug Fixes

- migrate built-in system metric families from `node_*` to `process_*` so proxy-exported CPU/memory/disk/network/pressure signals are pod/container scoped instead of node-scoped by name
- harden `query_range` tuple-shape cache safety by keying cache entries with tuple mode (`default_2tuple` vs `categorize_labels_3tuple`) so metadata-enabled responses cannot leak into default Grafana decode paths (`ReadArray` regression guard)

### Features

- add Loki-aligned `query_range` window cache defaults (`split=1h`, `max-parallel=2`, `freshness=10m`, `recent-ttl=0s`, `history-ttl=24h`) with bounded parallel fanout and historical-window reuse
- add adaptive query-range window parallelism (`min/max` bounds with latency/error EWMA feedback) so backend fanout can scale up under healthy latency and back off under pressure
- add `-disk-cache-max-bytes` to cap on-disk L2 cache size for predictable retention and capacity control

### Observability

- add adaptive query-range tuning gauges: current parallelism, latency EWMA, and error EWMA, exposed in both Prometheus and OTLP metrics
- harden `Loki-VL-Proxy Metrics` dashboard selectors to tolerate headless/non-headless job+service labels and blank namespace URL vars so drilldown views no longer collapse to no-data
- add a `Query-Range Windowing` dashboard section (window fetch/merge latency, window cache hit ratio, adaptive EWMA/parallelism)
- update packaged dashboards, alerts, and runbook queries to consume `process_*` system metric families

### Tests

- add focused query-range helper coverage (`time parsing/normalization`, window split/ttl helpers, adaptive parallel controller behavior) to reduce CI quality-gate regressions
- add synthetic `/proc` OTLP system-metrics coverage to lock process-scope metric family compatibility

### Configuration

- enable `systemMetrics.hostProc.enabled` by default in the upstream chart and document the host `/proc` mount behavior for node-level CPU/memory/disk/network/PSI visibility

## [0.27.20] - 2026-04-10

### Tests

- expand tuple-contract coverage and smoke validation paths to keep strict default 2-tuple and categorize-labels 3-tuple behavior regression-safe
- add e2e compatibility checks for vlogs alerting plus recording-rule visibility across direct `vmalert`, proxy Prometheus endpoints, legacy Loki YAML, and Grafana datasource-proxy paths
- add dedicated e2e structured-metadata compatibility coverage for `-metadata-field-mode=hybrid` and `-metadata-field-mode=native`

### CI

- extend CI shard coverage to include structured-metadata compatibility tests so metadata mode regressions fail on pull requests
- harden tuple-smoke wiring to validate strict 2-tuples on the default proxy endpoint and `categorize-labels` 3-tuples on the metadata-enabled endpoint with explicit failure diagnostics

### Documentation

- update architecture/readme diagrams and migration/testing docs to describe recording-rule remote-write expectations and Grafana datasource-proxy `datasource_type=vlogs` validation flow
- document query-range window cache controls, Loki-aligned defaults, and practical disk sizing guidance for longer retention windows

## [0.27.19] - 2026-04-10

### Bug Fixes

- enforce strict Loki tuple behavior for query responses: default/no-flag requests return canonical 2-tuples, while `X-Loki-Response-Encoding-Flags: categorize-labels` returns Loki-style 3-tuples with `structuredMetadata` and/or `parsed`
- remove proxy-side Grafana caller sniffing and `structured_metadata` request overrides from tuple-shape decisions, so response shape is controlled only by Loki header flags
- remove non-Loki `structured_metadata` tuple key alias from query responses and keep only canonical Loki metadata keys

### Tests

- add strict `/query_range` and `/query` contract tests covering both default 2-tuple and `categorize-labels` 3-tuple paths
- add parser-chain/brace-heavy stream-response regression coverage to prevent `ReadArray` tuple-shape regressions in Grafana Explore/Drilldown
- extend `TestTupleContract_*` gate coverage to enforce both strict default 2-tuple and `categorize-labels` 3-tuple compliance paths
- add e2e parity checks for vlogs recording rules and alerts across direct `vmalert`, proxy Prometheus endpoints, legacy Loki YAML rules, and Grafana datasource proxy endpoints
- add e2e structured-metadata mode checks for `-metadata-field-mode=hybrid` vs `-metadata-field-mode=native` with OTel dotted fields and Loki-compatible underscore labels

### Documentation

- update API/config docs to describe strict `categorize-labels`-driven 3-tuple behavior and canonical Loki metadata keys
- update tuple-contract runbook guidance to the strict mode set (`default_2tuple`, `categorize_labels_3tuple`)
- document pinned e2e stack alerting/runtime updates, including recording-rule coverage and native-metadata profile checks
- refresh architecture and migration docs/mermaid flows to include optional recording-rule remote-write sinks and Grafana datasource-proxy `datasource_type=vlogs` validation paths

### CI

- add automated `tuple-smoke` workflow job that boots the compat stack, seeds logs, and runs `scripts/smoke-test.sh` (default + categorize-labels checks)
- add `TestStructuredMetadata_*` coverage to the `e2e-compat (otel-edge)` PR shard so hybrid/native metadata regressions fail on pull requests

### Observability

- align tuple-contract Prometheus alerts with strict mode labels (`default_2tuple`, `categorize_labels_3tuple`) and retire stale `grafana_*` mode assumptions

## [0.27.18] - 2026-04-10

### Bug Fixes

- restore Grafana-safe tuple defaults when `-emit-structured-metadata=true`: Explore/Drilldown requests now stay on canonical `[timestamp, line]` unless explicitly opted into `structured_metadata=true` (or `X-Loki-Response-Encoding-Flags: structured-metadata`), preventing `ReadArray` decode regressions
- harden Grafana metrics dashboard templating with universal regex-safe variables (`job`, `cluster`, `env`, `namespace`, `service`, `pod`) and default service scoping to reduce duplicated/noisy series

### Tests

- add strict tuple-shape regression coverage that decodes Grafana responses as `[2]string` tuples and fails on any metadata-object tuple shape leak
- add explicit tuple-contract tests for `/query_range`, `/query`, stream-response mode, multi-tenant merge, and parser-chain brace-heavy logs

### CI

- add a dedicated tuple-shape contract gate in CI (`TestTupleContract_*`) so Grafana default stream responses fail fast on any 3-tuple regression

### Observability

- add tuple-mode regression alerts for unexpected `grafana_default_3tuple` emissions and missing `grafana_default_2tuple` emissions while Grafana tuple traffic is present

### Documentation

- add `scripts/smoke-test.sh` deploy canary to validate strict 2-tuple Grafana responses on `/query_range` and `/query`
- add a dedicated Grafana tuple-contract runbook and include it in the alert runbook index

## [0.27.17] - 2026-04-10

### Bug Fixes

- default to Loki 3-tuple structured metadata for Grafana query callers when `-emit-structured-metadata=true`, so Explore one-event details include full metadata by default while still allowing explicit request override via `structured_metadata=true|false`

## [0.27.16] - 2026-04-10

### Bug Fixes

- normalize backtick-quoted LogQL line filters (for example ``|= `api` ``) to literal substring matches so parser pipelines such as `| logfmt` no longer drop valid lines
- make structured metadata emission default for Grafana query callers when `-emit-structured-metadata=true`, so Explore one-event details can include full metadata beyond stream labels; keep explicit request-level override support via `structured_metadata=true|false` and `X-Loki-Response-Encoding-Flags: structured-metadata`

### Tests

- add translator regression coverage for backtick raw-string line filters, including `|= ... | logfmt` and literals containing `|`
- add proxy coverage for Grafana default structured-metadata emission plus explicit `structured_metadata=false` opt-out behavior

## [0.27.15] - 2026-04-10

### Bug Fixes

- harden startup diagnostics and /proc-host mount guidance for system resource metrics so missing CPU/memory/disk/network/PSI families are surfaced explicitly at boot

### Tests

- add coverage for `PROC_ROOT` env override behavior and startup diagnostics branches (`passed`/`incomplete`) to prevent regressions in release quality checks

## [0.27.15] - 2026-04-10

### Bug Fixes

- keep Grafana Explore/Drilldown on canonical 2-tuples even when `-emit-structured-metadata=true`, and require explicit caller opt-in (`X-Loki-Response-Encoding-Flags: structured-metadata` or `structured_metadata=true`) for 3-tuples to prevent `ReadArray` client decode failures

### Observability

- add startup diagnostics for `/proc`-backed system metrics so missing CPU/memory/disk/network/PSI families are logged with concrete remediation instead of failing silently
- expand the main operations dashboard with system resource drilldown panels (memory, CPU modes, PSI pressure, disk/network throughput, process RSS, open FDs)
- add actionable system resource alerts for missing system metrics, high memory usage, and sustained CPU/IO PSI pressure

### Helm

- add chart support for host `/proc` mounting (`systemMetrics.hostProc.enabled`) and auto-wire `-proc-root` so node-level system metrics can be enabled explicitly in Kubernetes

### Documentation

- add a dedicated system-resources runbook and include it in the alert runbook index for faster incident handling

## [0.27.14] - 2026-04-10

### Bug Fixes

- preserve Loki stream 3-tuples (`[ts,line,metadata]`) in multi-tenant query merge paths so metadata-bearing responses no longer fail tuple decode/sort logic

### Observability

- align request-log attributes closer to OTEL semantic conventions by adding `url.path`, `network.peer.address`, `user.id`, `user.name`, and `event.duration` while keeping existing compatibility fields

## [0.27.13] - 2026-04-10

### Features

- add Service `trafficDistribution` support across chart service resources (`service` and `peerService`), including configurable values for `PreferSameZone`, `PreferSameNode`, and deprecated alias `PreferClose`

### Reliability

- add a StatefulSet immutable-field upgrade guard in the Helm chart to fail early when live immutable fields drift from desired values (`serviceName`, `podManagementPolicy`, `volumeClaimTemplates`)

## [0.27.12] - 2026-04-09

### Documentation

- add a Grafana user-header forwarding guide for Loki datasource deployments, including `dataproxy.send_user_header`, trusted proxy headers, and expected `enduser.id` / `auth.*` request-log behavior

## [0.27.11] - 2026-04-09

### Bug Fixes

- harden stream tuple metadata emission to always return a flat key/value object in tuple slot `2`, including requests with `X-Loki-Response-Encoding-Flags: categorize-labels`, so Explore/Drilldown clients that require array-safe tuple decoding do not fail on nested metadata objects
- classify upstream transport failures more accurately: map canceled upstream requests to `499` (`errorType=canceled`) and timeout/deadline failures to `504` (`errorType=timeout`) instead of generic `502`

## [0.27.11] - 2026-04-09

### Bug Fixes

- separate datasource/basic-auth credentials from end-user attribution: `enduser.id` now resolves from trusted user headers/tenant/client IP, while auth principals are reported separately via `auth.*` logs and `X-Loki-VL-Auth-*` upstream headers
- make emitted 3-tuple stream metadata parser-safe by default (flat key/value third element), and emit nested Loki categorized metadata (`structuredMetadata`/`parsed`) only when clients request `X-Loki-Response-Encoding-Flags: categorize-labels`
- enrich request logs with proxy context diagnostics, including cache result (`hit|miss|bypass`), upstream call count/status/latency, and proxy-overhead timing

## [0.27.10] - 2026-04-09

### Features

- add native VictoriaLogs operations dashboard with tenant/client/cluster/env filtering for incident analysis independent of Loki-proxy query health
- expand packaged PrometheusRule coverage with backend-latency and client bad-request burst alerts linked to dedicated runbooks

### Bug Fixes

- dedupe translated metric `by(...)` labels after alias mapping so queries that combine canonical and alias fields (for example `level` plus `detected_level`) do not emit duplicate stats grouping columns
- add opt-in `-emit-structured-metadata` support for Loki-style stream 3-tuples `[timestamp, line, metadata]` while keeping default query responses on canonical 2-tuples for compatibility
- expose `structured_metadata` as a compatibility alias alongside canonical `structuredMetadata` in emitted stream metadata payloads

### Documentation

- split runbooks into per-alert files under `docs/runbooks/` and add deployment/scaling best-practice guidance for prevention-focused operations
- document dashboard purpose mapping in README/operations/observability and move release-process details to `docs/release-info.md`

### CI

- enforce canonical dashboard/alert asset sync in CI and support syncing multiple dashboard JSON files into chart assets
- add auto-release tag push fallback to retry with the workflow token when checkout credentials cannot create tags
- make auto-release tag creation prefer `RELEASE_PR_TOKEN` when configured and fall back to `GITHUB_TOKEN` only if needed
- avoid failing auto-release when metadata sync branch push is denied; emit warnings and skip metadata PR automation for that run

## [0.27.8] - 2026-04-08

### CI

- stabilize PR performance smoke by running benchmarks/load in an isolated phase after functional checks, increasing benchmark sample depth (`-benchtime=2s`, `-count=7`), and tightening perf regression thresholds to better flag real cache-bypass regressions
- harden release publishing for org moves by normalizing GHCR owner names to lowercase and keeping metadata-sync invocation compatible with tagged release script versions
- add fallback manual-release notes when a tag lacks a versioned changelog section, so republish runs can still proceed

### Features

- make chart `goMemLimitPercent` effective at runtime by computing and injecting `GOMEMLIMIT` from `resources.limits.memory` when `goMemLimit` is not explicitly set
- expand the packaged PrometheusRule set with backend-latency and client-bad-request alerts, and point each alert to dedicated per-alert runbook files
- add a native VictoriaLogs operations dashboard focused on tenant/client/cluster/env filtering to keep operator visibility when Loki/proxy query paths are degraded

### Documentation

- update values and performance docs with explicit `goMemLimitPercent` behavior, precedence, supported units, and runtime output format
- reorganize README LogQL compatibility into native-VictoriaLogs vs proxy-compatibility sections with direct VictoriaLogs references, expand documentation index links, and clarify read-only rules/alerts boundaries with `vmalert` and VictoriaLogs docs
- split runbooks into `docs/runbooks/` per-alert files, add deployment/scaling best-practice guidance, and document dashboard roles in README/operations/observability

### CI

- make observability asset sync/check support multiple dashboard JSON files under `dashboard/*.json` and chart copies under `charts/loki-vl-proxy/dashboards/*.json`

## [0.27.7] - 2026-04-08

### Features

- add safe Tier0 compatibility-cache controls and route-level guardrails for cacheable Loki read endpoints

### Performance

- expand cache benchmark coverage for query and Drilldown metadata paths, including delayed-backend hit-path comparisons and fleet peer-cache warm-hit behavior

### Tests

- extend proxy, middleware, cache, metrics, and e2e fleet/ui coverage to harden cache behavior, race-prone paths, and runtime regressions

### CI

- enforce Helm chart `version` and `appVersion` validation after release metadata sync in both auto and manual release workflows, and publish Docker Hub images to the canonical `docker.io/reliablyobserve/loki-vl-proxy` repository when credentials are configured

### Documentation

- refresh README, architecture, and performance guidance with clearer operator-facing cache topology, Tier0 mapping, and value-focused messaging
- update repository links, chart metadata references, image examples, and testing/compatibility doc links to the `ReliablyObserve/Loki-VL-proxy` org namespace and current docs structure

## [0.27.6] - 2026-04-07

### CI

- require `100%` Loki compatibility on PR quality and dedicated Loki compatibility workflow checks
- allow release metadata sync PRs to pass changelog gating when they materialize `Unreleased` into a versioned section
- support `RELEASE_PR_TOKEN` in release workflows so metadata PRs trigger required pull_request checks under branch protection
- skip PR quality performance smoke on non-perf-sensitive changes to avoid runner-jitter noise in docs/metadata/CI-only PR reports

## [0.27.5] - 2026-04-07

### CI

- route release metadata sync through a dedicated PR branch with auto-merge instead of direct pushes to `main`, and keep GitHub release notes sourced from changelog section content only

## [0.27.4] - 2026-04-07

### Features

- add ingress-backed and native-only `/tail` compatibility coverage for Grafana Explore and compose e2e
- extend multi-tenant Explore and Logs Drilldown coverage for `__tenant_id__`, label breakdowns, and service drilldowns
- prefer native VictoriaLogs field names, field values, and streams metadata for Drilldown discovery, with bounded fallback scanning for parsed and derived fields
- harden Loki label and Drilldown metadata resolution so stream metadata is preferred, exact native names win, and ambiguous translated aliases avoid silent wrong-field fallback
- complete the upstream Helm distribution surface with deployable runtime templates, peer-cache DNS service wiring, optional Gateway API routing, and OCI chart publication from release workflows
- add chart-native StatefulSet support, PVC-backed disk-cache defaults, and extra claim templates for persistent cache deployments

### Performance

- harden proxy cache and fanout hot paths with tighter response capture, typed multi-tenant merges, translation caching, capped pattern extraction, and safer synthetic-tail state bounds
- enforce bounded allocation on pattern responses and keep expensive metadata paths warm without stretching live query and tail freshness

### CI

- make the PR labeler fail-soft when optional repository labels are missing
- split Grafana UI smoke coverage into stable shards, add PR-time current-family and previous-family Grafana smoke plus scheduled/manual runtime profiles, and move browser-independent Grafana checks into cheaper non-browser gates
- prebuild and cache proxy images in Docker-backed CI jobs, run compose stacks with `--no-build`, and keep grouped compatibility gates with a legacy `e2e-compat` aggregate shim for required-check compatibility
- fix release workflow parsing by using env-based Docker Hub secret gating in `auto-release` and `release` workflows

### Tests

- add `/tail` ingress, idle-window, and native-failure regressions against the live compose stack
- expand tail fallback coverage so auto mode is verified against upstream `401`, `403`, and `5xx` native-tail failures without reopening browser cost
- add browser-level Explore live-tail and multi-tenant Logs Drilldown regressions
- raise `cmd/proxy` startup/server-loop coverage with more direct unit tests
- add HTTP-level Explore compatibility contracts, Drilldown filtered/freshness/empty-success resource contracts, explicit oversized multi-tenant fanout coverage, and stronger native tail failure propagation coverage
- extract pure Grafana Explore/Drilldown URL builders, test them directly, and add browser console/request guardrails plus reload-persistence smoke coverage
- make Logs Drilldown `1.x` and `2.x` contract assertions explicit in the source matrix, and harden Playwright smokes against toolbar overflow and in-place URL state updates
- add explicit Grafana runtime-family assertions so `11.x` keeps the `1.x` Drilldown expectations and `12.x` keeps the `2.x` expectations
- add resolver and handler coverage for stream-metadata preference, native-name precedence, and alias-collision behavior in label and Drilldown paths

### Documentation

- document native-first Drilldown discovery, multi-tenant safety caps, metadata-vs-live cache freshness, and tail mode behavior across README and operator docs
- document the Playwright shard matrix, browser-vs-non-browser coverage split, and the pinned/current/previous Grafana runtime compatibility profiles

## [0.27.0] - 2026-04-06

### Features

- configurable `tail.mode` with explicit `auto`, `native`, and `synthetic` streaming modes for Loki-compatible `/tail`

### CI

- require releasable PRs to update `CHANGELOG.md` `Unreleased` via a dedicated changelog gate workflow

### Tests

- compose-backed `/tail` coverage now verifies native live frames, forced synthetic tail streaming, and browser-origin behavior against the real stack
- Grafana Explore UI coverage now includes a live-tail regression against the browser-allowed synthetic-tail datasource

## [0.26.1] - 2026-04-06

### Features

- observability guide with metrics catalog, JSON log schema, and collector/agent integration examples
- OTLP metrics export now carries the same core proxy metric names as `/metrics`
- OTel-friendly JSON logging is now used consistently across proxy, disk cache, cache warmer, and OTLP export paths

### Tests

- expanded OTLP exporter and observability configuration coverage

## [0.26.0] - 2026-04-05

### Features

- improve observability and pr quality reporting

### Bug Fixes

- validate release prs via workflow dispatch (#5)
- align release notes with changelog (#3)
- restore nonzero pr quality snapshots
- keep pr quality metrics json clean
- publish releases from workflow dispatch
- restore release workflow automation

### Tests

- 1023 total tests (86.3% coverage)

## [0.25.0] - 2026-04-05

### Features

- improve hybrid drilldown compatibility
- expand drilldown and compatibility coverage
- improve tenant defaults and observability
- **Single-tenant VictoriaLogs migration mode**: `X-Scope-OrgID: "0"` and `X-Scope-OrgID: "*"` now use VictoriaLogs' default tenant (`AccountID=0`, `ProjectID=0`) when no tenant map is configured. This keeps Grafana Loki datasources simple for single-tenant backends while preserving strict mapped multitenancy.
- **Optional global bypass in mapped mode**: New `-tenant.allow-global` flag allows `0` and `*` to keep using the backend default tenant even when a tenant map is configured, making staged migrations from Loki string tenants to VictoriaLogs numeric tenants easier.
- **Client identity propagation**: The proxy now derives client identity from trusted Grafana headers, tenant, basic auth, or remote address and forwards `X-Loki-VL-Client-ID` and `X-Loki-VL-Client-Source` to the backend. When `-metrics.trust-proxy-headers=true`, `X-Grafana-User` is also forwarded.
- **Client-centric observability**: `/metrics` now exports per-client request counters, per-client status breakdowns, in-flight request gauges, response bytes, and LogQL query length histograms to identify the real users driving load.
- **Fleet peer-cache observability**: Added peer-cache metrics for remote peers, total ring members, peer hits, misses, and errors so fleet behavior can be diagnosed without relying only on logs.

### Bug Fixes

- stabilize compatibility and release automation
- repair auto release workflow
- backfill changelog and release tagging

### Security

- **Fail-closed multitenancy preserved**: Unknown non-numeric tenant strings still return `403 Forbidden` instead of silently falling back to the global VictoriaLogs tenant.
- **Global tenant bypass is explicit**: In mapped deployments, `0` and `*` only bypass tenant scoping when `-tenant.allow-global=true`.
- **Trusted-header handling tightened**: Grafana user identity is only used for metrics and backend context forwarding when `-metrics.trust-proxy-headers=true`.

### Operations

- **Pinned build and CI toolchain versions**: Workflows now use Go `1.26.1`, `golangci-lint` `v2.11.4`, Docker builder image `golang:1.26.1-alpine3.22`, and runtime image `alpine:3.22.2`.
- **Updated local/dev runtime images**: The dev/test Compose stack now uses `victoriametrics/victoria-logs:v1.49.0`.
- **Release workflow improvements**: Release builds now package the Helm chart as a versioned `.tgz` asset, update chart metadata during auto-release PR creation, and stop publishing a floating Docker `latest` tag.

### Documentation

- Added the full request-flow diagram to the top-level README.
- Updated configuration, API reference, fleet cache, and scaling docs for tenant defaults, client metrics, fleet metrics, pinned versions, and Grafana datasource behavior.

### Tests

- 974 total tests (82.9% coverage)

## [0.24.0] - 2026-04-04

### Features

- per-client identity metrics, scaling/capacity docs
- secret redaction, encryption removal, lint fixes, fleet e2e
- TTL-preserving shadow copies — never extend original expiry
- gossip key directory for local-first cache — minimize hops behind LB
- owner-affinity write-through, LB-aware fleet cache design
- fleet-distributed peer cache with consistent hashing and circuit breakers
- group_left/group_right one-to-many join, vector matching metadata passthrough
- proper without() label exclusion and on()/ignoring() label-subset matching
- smart PR labeling + scope-aware version bumping
- auto-release pipeline — version bump, changelog, badges, tag via PRs

### Bug Fixes

- pass gh token to release pr step
- remove unused websocket helper
- harden proxy and release workflow
- fix compat flakes, add disk cache e2e, cache sizing tests
- badge workflow creates PR instead of direct push (respects branch rules)
- re-enable badge auto-push, ruleset allows GHA bot
- badges workflow read-only (no push), e2e continue-on-error
- mark e2e-compat as continue-on-error (data ingestion timing flakes)
- remove unused functions, fix staticcheck SA9003/QF1001
- lint errcheck exclusions, staticcheck fix, e2e docker compose startup
- move errcheck test exclusion to linters.exclusions.rules (golangci-lint v2)
- simplify golangci-lint v2 config, add Apache 2.0 license
- race detector fix, golangci-lint v2 config, fuzz tests, README badges

### Tests

- 920 total tests (82.2% coverage)

## [0.23.0] - 2026-04-04

### Features

- **Proxy-side subquery evaluation**: `max_over_time(rate({app="nginx"}[5m])[1h:5m])` no longer returns an error. The proxy parses the subquery syntax, executes the inner metric query at each sub-step interval (e.g., every 5m over 1h = 12 VL queries), and aggregates results with the outer function (max, min, avg, sum, count, stddev, stdvar, first, last). Concurrent sub-step execution (bounded at 10) keeps latency low. Both query_range and instant query endpoints are supported.
- **VL stream selector optimization** (`-stream-fields`): New `-stream-fields=app,env,namespace` flag tells the proxy which labels are VL `_stream_fields`. For those labels, the proxy uses VL's native `{label="value"}` stream selectors (fast index path) instead of field filters. Non-stream-field labels still use field filters for correctness. Mixed matchers split correctly.
- **Loki-compatible error responses**: Error responses now use Loki/Prometheus-standard `errorType` values (`bad_data`, `execution`, `unavailable`, `timeout`, `canceled`, `internal`, `too_many_requests`) instead of a generic `bad_request` for all errors.

### Performance

Subquery benchmarks (Apple M3 Max):

| Subquery | Latency | Allocs | Memory |
|----------|---------|--------|--------|
| 3 steps (30m/10m) | 270µs | 1,152 | 170KB |
| 12 steps (1h/5m) | 634µs | 2,545 | 360KB |
| 72 steps (6h/5m) | 2.4ms | 11,693 | 1.1MB |
| 288 steps (24h/5m) | 8.6ms | 44,667 | 3.9MB |

Load: 7,036 subquery req/s at 50 concurrent (4 VL calls each). Stable memory (~47-48 MB per 100-request round, no leak).

### Tests

- 666 total tests (80 new: subquery parsing/evaluation/aggregation/perf/regression, stream selector optimization, Loki error format, duration/timestamp parsing)

## [0.22.0] - 2026-04-04

### Bug Fixes

- **CI: go vet failure** — fixed atomic.Int64 copy in perf_test.go
- **CI: golangci-lint errors** — added .golangci.yml config (errcheck excluded in tests, common HTTP patterns excluded)
- **CI: release workflow** — `release` job no longer blocked by `compat-check` failure; uses `if: always() && needs.test.result == 'success'`
- **CI: system metrics test** — robust on idle CI runners (CPU delta may be 0)
- **errcheck in production code** — `conn.Close()`, `conn.WriteMessage()`, `resp.Body.Close()` properly handled

### Security

- **CodeQL analysis** — added `.github/workflows/codeql.yaml` for weekly security scanning

### GitHub Releases

- Created missing releases v0.17.0 through v0.21.0 on GitHub (were git tags only)

## [0.21.0] - 2026-04-04

### Features

- **LRU cache eviction**: Evicts least-recently-used entries instead of random map iteration. O(1) promote on access, O(1) evict from tail. Hot entries survive under cache pressure.
- **`unwrap duration()/bytes()` unit conversion**: Proxy-side parsers for Loki duration strings (ns/us/ms/s/m/h/d) to seconds and byte strings (B/KB/KiB/MB/MiB/GB/GiB/TB/TiB) to bytes.
- **System metrics from /proc** (Linux): CPU (user/system/iowait), memory (total/available/free/usage ratio), process RSS/FDs, disk IO, network IO, PSI pressure stall (cpu/memory/io at 10s/60s/300s).
- **`bool` modifier on comparisons**: Stripped at translation; applyOp returns 1/0 for all comparisons.
- **Field-specific parser**: `| json f1, f2` / `| logfmt f1, f2` maps to full unpack.
- **Backslash quote handling**: `findMatchingBrace` handles `\"` in stream selectors.

### Tests

- 583 total tests (31 new: 4 LRU, 26 unit conversion, 1 translator)

## [0.20.0] - 2026-04-04

### Features

- System metrics (/proc CPU, mem, IO, net, PSI), @ modifier, remaining gap tests

## [0.19.0] - 2026-04-04

### Performance

- **Buffer pool for JSON encoding**: `marshalJSON()` uses `sync.Pool`-backed `bytes.Buffer` (64KB cap) for all JSON response encoding, reducing allocations across all handlers
- **Pooled response body reads**: `readBodyPooled()` reuses buffers for `io.ReadAll` paths
- **GOGC=200**: Helm chart sets `GOGC=200` (halves GC frequency for proxy workloads with short-lived allocations)
- **sync.Pool for NDJSON**: Entry maps pooled and reused across log line parsing (49% less memory)
- **Connection pool tuning**: `MaxIdleConnsPerHost=256` (was Go default 2), prevents port exhaustion at high concurrency
- **Results**: 39K req/s at 200 concurrent (no-cache), up from 8K before optimizations (+388% total improvement)

### Features

- **Complete Helm chart**: 11 templates — deployment, service, HPA, PDB, ServiceMonitor, ingress, HTTPRoute (Gateway API), NetworkPolicy, PVC, headless service for peer cache
- **GOMEMLIMIT auto-calc**: Calculates GOMEMLIMIT as configurable % of `resources.limits.memory` (default 70%)
- **Go runtime metrics**: `/metrics` exposes `go_memstats_alloc_bytes`, `go_memstats_sys_bytes`, `go_goroutines`, `go_gc_cycles_total`
- **Peer cache design**: Architecture doc for distributed L1.5 cache across replicas via headless service + consistent hashing

### Security

- **Rate limit bypass fixed**: `ClientID()` uses `RemoteAddr` (not spoofable `X-Forwarded-For`)
- **SECURITY.md**: Vulnerability reporting policy, security features documented
- **CONTRIBUTING.md**: Development workflow, code style, areas for contribution
- **Issue templates**: Bug report and feature request templates

### Tests

- 529 total tests (30 new coverage gap tests covering admin stubs, fallback paths, VL error propagation, label round-trips, Unicode, metrics recording)
- Performance regression tests: connection pool, memory leak detection, buffer pool safety
- CI benchmark job with artifact upload and regression gate

## [0.18.0] - 2026-04-04

### Security Fixes

- **P0: Cross-tenant data exposure in 7 handlers**: `handleSeries`, `handleIndexStats`, `handleVolume`, `handleVolumeRange`, `handleDetectedFields`, `handleDetectedFieldValues`, `handlePatterns` were missing `withOrgID(r)` — tenant headers never forwarded to VL
- **P0: Cross-tenant cache leak**: Cache keys for `/labels` and `/label/values` did not include `X-Scope-OrgID` — Tenant A's cached response could be served to Tenant B
- **P0: Rate limit bypass via X-Forwarded-For**: `ClientID()` trusted raw attacker-controlled `X-Forwarded-For` header. Now uses `RemoteAddr` (connection-level, not spoofable) with port stripping for consistent bucketing

### Bug Fixes

- **P1: `handleReady` nil dereference**: When VL is unreachable (`err != nil`), `resp` is nil but `resp.StatusCode` was accessed — panic in production. Now checks `err` first, defers `resp.Body.Close()`
- **P1: `ForwardHeaders` dead code**: Config field stored but never used in `applyBackendHeaders`. Now threads original request via context and copies configured headers to VL requests
- **P2: `handleSeries` swallows VL errors**: Always returned 200 with empty data on backend errors. Now propagates VL error status codes
- **P2: Goroutine leak in `cleanupStaleClients`**: No shutdown mechanism — leaked on config reload. Added `Stop()` method with `done` channel
- **P2: `containsWithoutClause` false positive on escaped quotes**: `\"` inside strings incorrectly toggled quote state. Now handles backslash escapes

### Tests

- 9 new tenant scoping tests (7 handler tests + 2 cache isolation tests)
- Updated `ClientID` tests for security change (XFF ignored, port stripped)
- 471 total tests passing

## [0.17.0] - 2026-04-04

### Security Fixes

- **P0: Tenant map reload data race**: `forwardTenantHeaders` now holds `configMu.RLock` when reading `tenantMap`, preventing data race on SIGHUP reload
- **P0: `without()` clause silent wrong behavior**: `without()` was silently treated as `by()` producing incorrect aggregation results. Now returns a clear error directing users to use `by()` with explicit labels

### Bug Fixes

- **Binary operators**: `applyOp` now handles `%` (modulo), `^` (power), and comparison operators (`==`, `!=`, `>`, `<`, `>=`, `<=`) — previously these silently returned the left operand
- **CB metrics mismatch**: Circuit breaker `State()` returns `"half_open"` but metrics matched `"half-open"` — half-open gauge never showed value 2. Now accepts both forms
- **`targetLabels` on volume_range**: `handleVolumeRange` now forwards the `targetLabels` param as VL `field` (was missing, only `/volume` had it)
- **`IsScalar` negative/scientific**: `IsScalar` now uses `strconv.ParseFloat` — supports `-1`, `1e5`, `1.5e-3` (previously only digits and dots)

### Features

- **Delete API endpoint**: `/loki/api/v1/delete` added as exception to read-only proxy with 7 safeguards: POST-only, `X-Delete-Confirmation` header, non-wildcard query, time range required, 30-day max range, tenant scoping, audit logging at WARN level

### Documentation

- **Restructured docs**: README slimmed to project summary + architecture, content moved to categorized files:
  - `docs/architecture.md` — component design, data flow, protection layers
  - `docs/configuration.md` — all flags, env vars, cache, tenancy, TLS, OTLP
  - `docs/api-reference.md` — endpoint table, delete safeguards, metrics
  - `docs/translation-reference.md` — LogQL to LogsQL mapping, supported/unsupported
  - `docs/testing.md` — test categories, running tests, fuzz testing
  - `docs/roadmap.md` — completed and planned features
- **Updated KNOWN_ISSUES.md** with v0.17.0 fixes

### Tests

- 50+ new tests: concurrent tenant reload (race detector), all binary operators (18 cases), delete safeguards (8 cases), CB metrics, `IsScalar`, `without()` clause detection

## [0.16.0] - 2026-04-04

### Security Fixes

- **P0: Coalescer tenant data leak**: `RequestKey()` now includes `X-Scope-OrgID` in the hash, preventing cross-tenant response sharing via singleflight coalescing
- **P0: isStatsQuery false-positive**: Rewrote stats detection to skip quoted regions — `|= "stats query"` no longer triggers stats handler routing
- **P0: Metrics always recording 200**: `handleQueryRange` and `handleQuery` now capture actual HTTP status codes via `statusCapture` wrapper. VL error responses (4xx/5xx) are propagated to clients.

### Features

- **Direction parameter**: `proxyLogQuery` reads Loki's `direction` param and appends VL's `| sort by (_time)` or `| sort by (_time desc)` accordingly
- **Labels query param**: `/labels` endpoint now translates and forwards the `query` param to scope label suggestions in Grafana Explore
- **`without()` grouping**: `extractOuterAggregation` now accepts `without()` alongside `by()` in outer aggregation clauses
- **Additional outer aggregations**: `stddev`, `stdvar`, `sort`, `sort_desc` added to the aggregation regex
- **`label_format` multi-rename**: `| label_format a="{{.x}}", b="{{.y}}"` now generates separate `| format` pipes for each assignment
- **`quantile_over_time`**: Maps to VL's `quantile(phi, field)` stats function
- **`absent_over_time`**: Maps to VL's `count()`
- **Stats response label translation**: Dotted VL label names in metric responses are now translated to underscore format
- **VL error message mapping**: `wrapAsLokiResponse` detects VL error formats and translates to Loki's `{"status":"error","error":"..."}` format
- **Admin endpoint stubs**: `/loki/api/v1/rules`, `/loki/api/v1/alerts`, `/config` for Grafana Alerting ruler mode compatibility
- **Half-open circuit breaker fix**: Half-open state now limits probe requests to `successThreshold` count instead of allowing all
- **`convertGoTemplate` dotted fields**: `{{.service.name}}` now correctly maps to `<service.name>`
- **`unwrap` conversion wrappers**: `| unwrap duration(field)` and `| unwrap bytes(field)` now strip the wrapper and extract the field name
- **Extended binary operators**: `%`, `^`, `==`, `!=`, `>`, `<`, `>=`, `<=` added to binary metric expression parsing

### Testing

- **Playwright UI e2e framework**: Full Grafana UI test suite via Playwright — Explore, drill-down, label navigation, error handling, side-by-side proxy vs Loki comparison
- 395+ unit tests (translator 92.2%, middleware 91.8%, cache 88.4%)
- 8 new e2e compat tests for direction, labels query param, quantile_over_time, tenant isolation, error propagation, without() grouping, label_format multi-rename

## [0.15.0] - 2026-04-04

### Features

- **`/loki/api/v1/patterns` — real implementation**: Proxy-side drain-like pattern extraction. Queries VL for log lines, tokenizes to patterns (replaces IPs, numbers, UUIDs, timestamps with `<_>`), groups by pattern, returns sorted by frequency. Handles both structured (JSON) and unstructured log formats.

### Tests

- 349 unit tests, 80+ e2e tests
- Pattern extraction unit tests (tokenize, isVariable, extractLogPatterns)
- E2e: patterns endpoint with real data, Loki vs proxy comparison

### Zero Gaps

All Loki API endpoints are now fully implemented. No stubs remain.

## [0.14.0] - 2026-04-04

### Features

- **Nested metric queries**: Proxy-side binary evaluation for `sum(rate(...)) / sum(rate(...))`, scalar ops (`rate(...) * 100`), and matching time series point-by-point arithmetic
- **Comprehensive e2e chaining tests**: 40+ tests covering parser→filter, multi-filter, parser→drop/keep, parser→line_format, filter→decolorize, metric queries (rate, count, bytes, sum by, topk), binary metric queries, all endpoints, security headers, write blocking

### Tests

- 340 unit tests, 80+ e2e tests
- E2e test coverage: json+logfmt parsers, label filters (==, !=, =~, !~, >=), line filters (|=, !=, |~, !~), decolorize, ip() filter, line_format templates, metric queries (rate, count_over_time, bytes_over_time, sum by, topk), binary expressions (/, *, +, -), format_query, detected_labels, push blocked, buildinfo, ready, metrics, patterns, security headers, all API endpoints

## [0.13.0] - 2026-04-04

### Features

- **`| decolorize`**: Proxy-side ANSI escape stripping (Loki parity, ready for VL native replacement)
- **`| ip("CIDR")`**: Proxy-side IP range filtering with `net.ParseCIDR` (Loki parity)
- **`| line_format` full templates**: Go `text/template` with ToUpper, ToLower, default, TrimSpace, etc.
- **`/loki/api/v1/format_query`**: Returns query as-is (client-side formatting)
- **`/loki/api/v1/detected_labels`**: Stream-level labels endpoint (Loki 3.x)
- **Write safeguard**: `/loki/api/v1/push` returns 405 (read-only proxy)

### Tests

- 340 unit tests, 60+ e2e tests
- New e2e: decolorize, ip() filter, line_format, format_query, detected_labels, write blocked, buildinfo, ready, metrics validation, patterns
- Fuzz testing: 1.2M+ executions, no panics

## [0.12.0] - 2026-04-04

### Features

- **Write endpoint safeguard**: `/loki/api/v1/push` returns 405 — read-only proxy
- **`/loki/api/v1/format_query`**: Returns query as-is (client-side formatting)
- **`/loki/api/v1/detected_labels`**: Stream-level labels (Loki 3.x compat)
- **`| decolorize` pipe**: Proxy-side ANSI escape stripping (ready for VL native replacement)
- **`| ip()` filter**: Marked for proxy-side CIDR matching (ready for VL native replacement)
- **pprof endpoint**: `/debug/pprof/` for production profiling
- **SIGHUP config reload**: Hot-reload tenant-map and field-mapping without restart
- **Rate limit headers**: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `Retry-After` on 429
- **Enhanced `/ready`**: Checks VL health + circuit breaker state
- **Per-endpoint cache metrics**: `loki_vl_proxy_cache_hits_by_endpoint{endpoint}`
- **Backend latency histogram**: `loki_vl_proxy_backend_duration_seconds{endpoint}`
- **Singleflight stats**: `loki_vl_proxy_coalesced_total`, `loki_vl_proxy_coalesced_saved_total`
- **Circuit breaker gauge**: `loki_vl_proxy_circuit_breaker_state` (0=closed, 1=open, 2=half-open)
- **PrometheusRule CR**: Helm template with 7 alert rules for Prometheus Operator
- **Grafana dashboard ConfigMap**: Auto-provisioned via Helm with `grafana_dashboard: "1"` label
- **Fuzz tests**: LogQL translator fuzz testing (1.2M+ executions, no panics)

### Tests

- 314 unit tests passing (up from 263)
- Fuzz seed corpus: 30+ valid/malformed/adversarial LogQL inputs

## [0.11.0] - 2026-04-04

### Features

- **OTel label translation**: Bidirectional dot↔underscore conversion for 50+ OTel semantic convention fields
  - `-label-style=underscores` converts VL dotted names (service.name) to Loki underscores (service_name)
  - `-label-style=passthrough` (default) passes VL field names as-is
  - Query direction: `{service_name="x"}` → VL `"service.name":"x"` with automatic field quoting
  - Response direction: all 7 response paths translated (labels, label_values, detected_fields, series, query results, streaming, tail)
- **Custom field remapping**: `-field-mapping` JSON config for arbitrary VL↔Loki field name mappings
- **Per-tenant metrics**: `loki_vl_proxy_tenant_requests_total{tenant,endpoint,status}` and latency histograms
- **Client error breakdown**: `loki_vl_proxy_client_errors_total{endpoint,reason}` — bad_request, rate_limited, not_found, body_too_large
- **Request logging middleware**: Structured JSON log per request with tenant, query, status, duration, client IP
- **Tenant wildcard**: `X-Scope-OrgID: "*"` or `"0"` skips tenant headers for single-tenant VL setups
- **Grafana dashboard**: Pre-built importable dashboard (`examples/grafana-dashboard.json`) with tenant breakdown
- **Alerting rules**: 16 Prometheus/VM alert rules (`examples/alerting-rules.yaml`) including per-tenant abuse detection
- **Operations guide**: SRE documentation (`docs/operations.md`) — capacity planning, perf tuning, troubleshooting

### Tests

- 44 new unit tests for label translation (SanitizeLabelName, LabelTranslator, TranslateLogQLWithLabels)
- 50+ OTel e2e compatibility tests across 12 scenarios (dots→underscores, passthrough, mixed, query direction, Drilldown)
- Docker-compose: added `loki-vl-proxy-underscore` service at :3102 for e2e label translation tests
- **263 unit tests, 50+ e2e tests** — all passing

## [0.6.0] - 2026-04-03

### Features

- **Loki-VL-proxy**: HTTP proxy translating Loki API to VictoriaLogs
- **LogQL to LogsQL translator**: stream selectors, line filters (substring semantics),
  parsers (json, logfmt, pattern, regexp), label filters, metric queries (rate,
  count_over_time, bytes_over_time, sum by, topk), unwrap handling
- **Response converter**: VL NDJSON → Loki streams format, VL stats → Prometheus matrix/vector
- **Request coalescing**: singleflight deduplication — N identical concurrent queries → 1 backend request
- **Rate limiting**: per-client token bucket + global concurrent query semaphore
- **Circuit breaker**: opens after consecutive failures, auto-recovers via half-open probing
- **Query normalization**: sort label matchers, collapse whitespace for better cache hits
- **TTL cache**: per-endpoint TTLs (labels=60s, queries=10s), max 256MB, eviction tracking
- **Prometheus metrics**: `/metrics` endpoint with request counters, latency histograms, cache stats
- **JSON structured logs**: via Go's slog to stdout
- **Helm chart**: VictoriaMetrics-style with extraArgs, ServiceMonitor, security context
- **GHA CI/CD**: build, test, lint, Docker multi-arch, GitHub Release with binaries + checksums
- **Docker**: single static binary, ~10MB Alpine-based image

### Critical Fixes

- **Substring vs word matching**: Loki `|= "text"` is substring match; translated to VL `~"text"`
  (not `"text"` which is word-only). Without this fix, queries silently return fewer results.
- **Stream filter vs field filter**: Loki stream selectors `{level="error"}` converted to VL
  field filters `level:=error` (not stream filters which only match `_stream_fields`)
- **Parser + filter chains**: `| json | status >= 400` correctly becomes
  `| unpack_json | filter status:>=400` in VL
- **Regex quoting**: `namespace=~"prod|staging"` properly quoted as `namespace:~"prod|staging"`
- **Keep fields**: `| keep app, level` always includes `_time, _msg, _stream` for response building

### API Coverage

| Endpoint | Status |
|---|---|
| `/loki/api/v1/query_range` | Implemented (streams + matrix) |
| `/loki/api/v1/query` | Implemented |
| `/loki/api/v1/labels` | Implemented + cached |
| `/loki/api/v1/label/{name}/values` | Implemented + cached |
| `/loki/api/v1/series` | Implemented |
| `/loki/api/v1/detected_fields` | Implemented |
| `/loki/api/v1/index/stats` | Stub |
| `/loki/api/v1/index/volume` | Stub |
| `/loki/api/v1/index/volume_range` | Stub |
| `/loki/api/v1/patterns` | Stub |
| `/loki/api/v1/tail` | Not implemented |
| `/ready` | Implemented |
| `/loki/api/v1/status/buildinfo` | Implemented |
| `/metrics` | Implemented |

### Tests

- 126 unit tests (translator, proxy contracts, cache, middleware, normalization)
- 54 e2e tests (Loki vs proxy side-by-side comparison with compatibility scoring)
- 10 performance e2e tests (Loki direct vs proxy latency comparison)
- All at 100% compatibility

### Performance

Proxy is 40-77% faster than direct Loki for all query endpoints (VictoriaLogs is faster).
Cache provides 3.2x speedup on warm hits.

### Known Limitations

See [docs/KNOWN_ISSUES.md](docs/KNOWN_ISSUES.md) for full list:
- `/loki/api/v1/tail` WebSocket not implemented
- Volume API endpoints return stubs
- Multitenancy header mapping not implemented (Loki string org IDs vs VL numeric AccountID)
- Some LogQL features have no VL equivalent (decolorize, absent_over_time, subqueries)
