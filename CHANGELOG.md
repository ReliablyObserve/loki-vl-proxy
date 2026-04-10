# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- harden `Loki-VL-Proxy Metrics` dashboard selectors to tolerate headless/non-headless job+service labels and blank namespace URL vars so sand drilldowns no longer collapse to no-data
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
- harden Grafana sand metrics dashboard templating with universal regex-safe variables (`job`, `cluster`, `env`, `namespace`, `service`, `pod`) and default service scoping to reduce duplicated/noisy series

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

- add native VictoriaLogs offenders dashboard with tenant/client/cluster/env filtering for incident analysis independent of Loki-proxy query health
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
- add a native VictoriaLogs offenders dashboard focused on tenant/client/cluster/env filtering to keep operator visibility when Loki/proxy query paths are degraded

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
