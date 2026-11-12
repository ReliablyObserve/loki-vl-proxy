---
sidebar_label: Testing Guide
description: Unit, integration, and E2E test architecture. Covers parity tests, fuzz tests, and the query semantics matrix.
---

# Testing

## Quick Start

```bash
# Unit tests
go test ./...

# Dedicated tuple-shape contract gate used in CI
go test ./internal/proxy -run '^TestTupleContract_' -count=1

# With race detector
go test -race ./...

# E2E compatibility tests (requires Docker Compose).
# The stack is started from test/e2e-compat; the Go tests run from the repository root.
(cd test/e2e-compat && docker compose up -d --build && ../../scripts/ci/wait_e2e_stack.sh 180)
# Example: the `semantics` group; copy the other group patterns from .github/workflows/ci.yaml
go test -v -tags=e2e -timeout=300s -count=1 \
  -run '^(TestSetup_IngestLogs|TestQuerySemanticsMatrixManifest|TestQuerySemanticsOperationsInventory|TestQuerySemanticsMatrix|TestGrafanaClickout_.*|TestMissingOps_.*|TestRangeMetricCompatibility.*|TestLogQL_Exhaustive_.*|TestPipeline_.*|TestEdge_DropErrorInstantQueryReturnsAggregatedResult|TestEdge_DetectedFieldsAfterJsonDropPipelineIncludesJsonFields)$' \
  ./test/e2e-compat/

# Manifest-driven Loki query semantics parity + inventory
go test -v -tags=e2e -run '^TestQuerySemantics' ./test/e2e-compat/

# Track-specific scores
go test -v -tags=e2e -run '^TestLokiTrackScore$' ./test/e2e-compat/
go test -v -tags=e2e -run '^TestDrilldownTrackScore$' ./test/e2e-compat/
go test -v -tags=e2e -run '^TestVLTrackScore$' ./test/e2e-compat/

# Reset the stack before switching to another group
(cd test/e2e-compat && docker compose down -v)

# Playwright UI tests: start the stack with the log generator profile, as CI does
(cd test/e2e-compat && docker compose --profile ui up -d --build && ../../scripts/ci/wait_e2e_stack.sh 180)
cd test/e2e-ui
npm ci && npx playwright install chromium
npm test

# Run the same shards used in CI
npx playwright test tests/datasource.spec.ts
npx playwright test --grep @explore-core
npx playwright test --grep @explore-tail
npx playwright test --grep @drilldown-core
npx playwright test --grep @drilldown-mt
npx playwright test --grep @explore-ops
npx playwright test --grep @explore-mt
npx playwright test --grep @regression
npx playwright test --grep @comprehensive-ui
cd ../..

# macOS fallback (run from the repository root): the same UI tests inside Linux Playwright.
# Specs default to 127.0.0.1, which is the container itself, so point every URL at the host.
docker run --rm \
  -v "$(pwd)/test/e2e-ui:/work" \
  -w /work \
  -e GRAFANA_URL=http://host.docker.internal:3002 \
  -e LOKI_URL=http://host.docker.internal:13101 \
  -e VL_URL=http://host.docker.internal:19428 \
  -e PROXY_NATIVE_METADATA_URL=http://host.docker.internal:13106 \
  mcr.microsoft.com/playwright:v1.59.1-noble \
  /bin/bash -lc "npm ci && npx playwright test --grep @drilldown-core"

# Build binary
go build -o loki-vl-proxy ./cmd/proxy

# Post-deploy tuple contract canary (validates default 2-tuple + categorize-labels 3-tuple; expects recent log data)
PROXY_URL=http://127.0.0.1:3100 ./scripts/smoke-test.sh

# The same canary against the compose stack, as the tuple-smoke CI job runs it
go test -v -tags=e2e -run '^TestSetup_IngestLogs$' ./test/e2e-compat/
PROXY_URL=http://127.0.0.1:13100 PROXY_URL_CATEGORIZED=http://127.0.0.1:13102 \
  SMOKE_QUERY='{app="e2e-test"}' ./scripts/smoke-test.sh
```

For a local run that keeps memory and time down, use the capped short mode:

```bash
scripts/test-short.sh                                   # GOMEMLIMIT=3GiB, GOMAXPROCS=4, -short -p 1
scripts/test-short.sh -run 'TestLabel' ./internal/proxy # narrow it with go test flags
```

`-short` skips the unit tests that take five seconds or more; CI runs them (it
does not pass `-short`). Run `-race` locally only on the tests you touched
(`go test -race -run '<tests>' ./internal/...`); CI runs it on every package.

CI runs each `e2e-compat` group on a fresh stack. Running several groups against one stack re-ingests the fixtures (VictoriaLogs keeps duplicate rows, Loki drops them), which shows up as false parity diffs, so run `docker compose down -v` between groups. Do not run the Go parity groups with `--profile ui`: the continuous log generator changes cardinality while the comparison runs.

The UI specs read these environment variables: `GRAFANA_URL` (default `http://127.0.0.1:3002`, used as the Playwright `baseURL`), `LOKI_URL` (default `http://127.0.0.1:13101`), `VL_URL` (default `http://127.0.0.1:19428`), `PROXY_NATIVE_METADATA_URL` (default `http://127.0.0.1:13106`), plus `PLAYWRIGHT_EXECUTABLE_PATH`, `CI`, `WORKERS` and `HEADED` in `playwright.config.ts`. CI also exports `PROXY_URL`, but no spec reads it.

## Security Validation

The repository now has a dedicated security lane in CI. These are the closest local equivalents when changing auth, tenant isolation, Dockerfile hardening, or workflow security.

```bash
# secret scanning
docker run --rm -v "$PWD:/repo" -w /repo \
  ghcr.io/gitleaks/gitleaks:v8.28.0 \
  detect --source . --report-format sarif --report-path gitleaks.sarif --exit-code 1

# Go SAST (exclusions and their rationale live in .github/workflows/security-pr.yaml)
go install github.com/securego/gosec/v2/cmd/gosec@v2.29.0
"$(go env GOPATH)/bin/gosec" \
  -exclude=G104,G108,G115,G118,G301,G302,G304,G306,G402,G404,G704,G705 \
  -exclude-generated \
  -exclude-dir=bench \
  ./...

# filesystem vuln/misconfig/secret scan
docker run --rm -v "$PWD:/repo" -w /repo \
  aquasec/trivy:0.71.0 \
  fs . \
  --ignorefile .trivyignore.yaml \
  --scanners vuln,misconfig,secret \
  --severity HIGH,CRITICAL \
  --ignore-unfixed \
  --exit-code 1 \
  --skip-version-check

# workflow + Dockerfile linting
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7 -color
docker run --rm -i -v "$PWD/.hadolint.yaml:/root/.config/hadolint.yaml:ro" \
  hadolint/hadolint:v2.12.0 < Dockerfile

# supply-chain posture
docker run --rm \
  -e GITHUB_AUTH_TOKEN="${GITHUB_TOKEN}" \
  ghcr.io/ossf/scorecard:v5.6.0@sha256:c0a21a1c33a2d8819937d0a8a62e6f4189c800639abd9f0bfbe61c90c71fae90 \
  --repo="github.com/ReliablyObserve/Loki-VL-proxy" \
  --format json \
  --show-details > scorecard.json
python3 scripts/ci/check_scorecard.py scorecard.json \
  --min-overall 5.0 \
  --require-check Dangerous-Workflow=10 \
  --require-check Binary-Artifacts=10 \
  --require-check CI-Tests=8 \
  --require-check SAST=7

# repo-specific runtime checks against the e2e-compat stack
# (the scan scripts default to port 3100; the compose proxy listens on 13100)
./scripts/ci/run_security_regressions.sh
PROXY_BASE_URL=http://127.0.0.1:13100 ./scripts/ci/run_zap_scan.sh baseline

# heavy lane only (scheduled/manual Security Heavy workflow)
PROXY_BASE_URL=http://127.0.0.1:13100 ./scripts/ci/run_zap_scan.sh active
PROXY_BASE_URL=http://127.0.0.1:13100 ./scripts/ci/run_nuclei_scan.sh
```

The pull-request `Security / runtime` job runs the security regressions and the ZAP baseline only. The scheduled heavy lane adds longer fuzzing, Trivy image scanning, SBOM generation, broader `Semgrep`, an OWASP ZAP active scan, and the curated `nuclei` scan.

Local ZAP baseline runs may still report `10049 Non-Storable Content` on intentional `404` discovery paths such as `/` or disabled `/debug/*` URLs. That output is expected visibility noise unless it points at a real user-facing route.

## CI Job Inventory

Test, security and quality jobs defined under `.github/workflows/` (release, docs-site, labeler and badge workflows are not listed). Unless noted, a job runs on pull requests, pushes to `main`, and manual dispatch.

| Workflow | Job | What it runs |
|---|---|---|
| `ci.yaml` | `test` (fan-in of the `test-shard (1..16, rest)` matrix) | observability asset sync check, CI script unit tests, `go build`, `go vet`, `govulncheck` v1.8.0, `go test ./... -race` with coverage, `internal/cache` coverage guard (79%); shards 1-12 split the top-level tests of `internal/proxy` and `internal/querycorpus` (`scripts/ci/test_shards.py`, weights in `scripts/ci/test-shard-weights.tsv`), `rest` runs the other packages, and `test` fails unless every test ran exactly once |
| `ci.yaml` | `lint` | `gofmt -s` check and `golangci-lint` v2.14.0 |
| `ci.yaml` | `tuple-contract` | `go test ./internal/proxy -run '^TestTupleContract_'` |
| `ci.yaml` | `tuple-smoke` | compose stack, `TestSetup_IngestLogs`, then `scripts/smoke-test.sh` |
| `ci.yaml` | `fuzz-smoke` | short (12-20s) fuzz runs for `internal/proxy`, `internal/translator`, `internal/cache` and `internal/rulesmigrate` targets |
| `ci.yaml` | `race-stress` (aggregates the `race-stress-shard` matrix: 3 proxy shards + `other`) | `-race -count=1` over `internal/proxy`, `internal/cache`, `internal/middleware`, `internal/observability`, plus targeted concurrency regressions |
| `ci.yaml` | `stress-tests` | `-tags=stress -race` translation, coalescer and range-metric tests in `internal/proxy` |
| `ci.yaml` | `memory-leak-tests` | `-tags=memleak -race -run 'TestMemLeak_'` in `internal/proxy` |
| `ci.yaml` | `bench` | `internal/proxy` benchmarks (`-count=3`), label/field, patterns, cache and stats-translation scale benchmarks with regression thresholds, `TestLoad*` load tests |
| `ci.yaml` | `docker` | image build and binary smoke run |
| `ci.yaml` | `image` | builds the proxy image once (`docker/build-push-action`, `type=docker` tar uploaded as the `proxy-image` artifact); `tuple-smoke`, `e2e-compat`, `e2e-fleet` and `e2e-ui` download and `docker load` it instead of building it themselves |
| `ci.yaml` | `helm` | `helm lint`, template regressions, `scripts/ci/validate_helm_flags.sh` |
| `ci.yaml` | `e2e-compat (core, drilldown, otel-edge, tail-multitenancy, semantics)` | one fresh compose stack per group, `go test -tags=e2e -run '<group pattern>' ./test/e2e-compat/` |
| `ci.yaml` | `e2e-compat` | aggregates the five grouped jobs into one result |
| `ci.yaml` | `e2e-fleet` | `test/e2e-fleet` 3-proxy stack, `TestFleetSmoke_QueryRangeWarmHitIncrementsCacheMetrics` only |
| `ci.yaml` | `e2e-ui (5 shards)` | compose stack with `--profile ui`, one group of Playwright tags per job (see [CI Shards](#ci-shards)) |
| `compat-loki.yaml` | `loki-pinned` | Loki 3.7.9, VictoriaLogs v1.53.0, Grafana 13.3.0: `TestLokiTrackScore` (must be 100%), `TestQuerySemantics*`, `TestLabelCache_*` |
| `compat-loki.yaml` | `loki-matrix` | weekly: the same checks for every `stack.loki.matrix_versions` entry |
| `compat-drilldown.yaml` | `drilldown-pinned-runtime` | Loki 3.7.9, VictoriaLogs v1.53.0, Grafana 13.3.0: `TestDrilldownTrackScore` |
| `compat-drilldown.yaml` | `drilldown-grafana-pr-matrix` | runtime profiles with `run_on_pr` (Grafana 13.3.0 `current_smoke`, 12.4.14 `previous_smoke`) |
| `compat-drilldown.yaml` | `drilldown-contract-matrix`, `drilldown-grafana-runtime-matrix` | weekly/manual: Drilldown app contract checks per version and every Grafana runtime profile |
| `compat-vl.yaml` | `vl-pinned` | Loki 3.7.9, VictoriaLogs v1.53.0, Grafana 13.3.0: `TestVLTrackScore` |
| `compat-vl.yaml` | `vl-matrix` | weekly/manual, for every `stack.victorialogs.matrix_versions` entry: `TestVLTrackScore`, the version-sensitive checks (`TestCompat_DetectedLevelQueryRange`, `TestCompat_RejectedQueryStatusParity`) and the range-metric table compared with Loki (`TestCompat_Unwrap*`, `TestRangeMetric*`, `TestGrafanaClickout_*`) |
| `security-pr.yaml` | `Security / static` | Gitleaks v8.28.0, gosec v2.29.0, Trivy 0.71.0 filesystem scan, actionlint 1.7.7, hadolint v2.12.0, OpenSSF Scorecard v5.6.0 guardrails |
| `security-pr.yaml` | `Security / runtime` | compose stack, `scripts/ci/run_security_regressions.sh`, ZAP baseline |
| `security-heavy.yaml` | `Heavy / image and sbom`, `Heavy / semgrep`, `Heavy / fuzz`, `Heavy / dast` | scheduled/manual only: Trivy image scan and SBOM, Semgrep 1.161.0, 2-minute fuzzing, ZAP active scan and curated nuclei |
| `codeql.yaml` | `analyze` | CodeQL Go analysis (also weekly) |
| `scorecard.yaml` | `analysis` | pushes to `main`, weekly and manual: OpenSSF Scorecard through `ossf/scorecard-action`, published to the public Scorecard API for the README badge; the result is kept as an artifact |
| `e2e-pipeline.yml` | `e2e-pipeline` | `test/e2e-pipeline` stack, `go test -tags=e2e ./test/e2e-pipeline/` |
| `changelog-pr.yaml` | `changelog` | pull requests only: `scripts/ci/check_changelog_pr.py` changelog gate |
| `pr-quality-report.yaml` | `report` (waits for `snapshot-tests (tree shard)`, `snapshot-compat (tree)`, `head`, `base` and `perf`) | pull requests only: test count, coverage, compatibility, benchmark and load deltas against the base branch; the unit tests of both trees run as shards through `scripts/ci/go_test_unit.sh` (the base tree's only when its saved count is missing), the compatibility scores in a job per tree |
| `visual-smoke.yaml`, `visual-smoke-cleanup.yaml` | `visual-plan`, `visual-smoke-shard (N)`, `visual-smoke` (fan-in, the check name), `visual-publish`, `visual-comment`, `cleanup` | pull requests only: Grafana (Explore, Logs Drilldown) through the base build, the PR build and Loki; a fixed core set plus the pages near the changed code (`bench/visual/plan.py`), one sticky comment with the montages, fails on a base-vs-PR data difference that Loki does not settle in the PR's favour (label `visual-change-expected` accepts an intended one), a panel that went empty or an error only the PR shows; a PR that fixes a registry case gets a "Fix proof" section with the captures of that exact case (`fixes` in `bench/visual/spec.json`; `plan.py --check` requires one for every fixed explore-, drilldown-visible or grafana-datasource case, or `visual: none` with a reason in its gap block) (see `bench/visual/README.md`); the capture runs in parallel shards, one stack each, about a dozen captures per shard, joined by the `visual-smoke` job |

## Test Coverage by Category

Exact counts move often. Treat the categories below as the stable map of what is covered; CI and PR reporting publish current counts and deltas.

| Category | Tests | What they verify |
|---|---|---|
| Loki API contracts | 30 | Exact response JSON structure per Loki spec |
| LogQL translation (basic) | 30 | Stream selectors, line filters, parsers, label filters |
| LogQL translation (advanced) | 22 | Metric queries, unwrap, topk, sum by, complex pipelines |
| Query normalization | 8 | Canonicalization for cache keys |
| Cache behavior | 6 | Hit/miss/TTL/eviction/protection |
| Multitenancy | 4 | String->int mapping, numeric passthrough, unmapped default |
| WebSocket tail | 4+ | Query validation, origin policy, native live frames, synthetic live streaming |
| Disk cache (L2) | 12 | Set/get, TTL, compression, persistence, stats |
| OTLP pusher | 4 | Push, custom headers, error handling, payload structure |
| Hardening | 4 | Query length limit, limit sanitization, security headers |
| Middleware | 12 | Coalescing, rate limiting, circuit breaker |
| Security CI static | Dedicated workflow | gitleaks, gosec, Trivy, actionlint, hadolint, Scorecard |
| Security CI runtime | Dedicated workflow | custom regressions and ZAP baseline on PRs; ZAP active scan and curated nuclei in the scheduled heavy lane |
| Critical fixes | 30+ | Data race, binary operators, delete safeguards, without() |
| Benchmarks | 10+ | Translation hot paths, Tier0 response-cache hits, and warm fleet shadow-copy reads |
| E2E basic (Loki vs proxy) | 11 | Side-by-side API response comparison |
| E2E complex (real-world) | 31 | Multi-label, chained filters, parsers, cross-service |
| E2E edge cases (VL issues) | 12 | Large bodies, dotted labels, unicode, multiline |
| Fuzz testing | 1.2M+ executions | No panics found |

## Recent Regression Guards

Recent PRs added targeted guards in areas that were previously flaky in live Grafana workflows:

- `internal/translator/labels_translate_test.go` verifies that repeated same-field include/exclude filters are all kept in order, as Loki applies them; `TestRepeatedLabelFiltersApplyAllLikeLoki` holds the translation to Loki's recorded answers row by row (the `process` command of the Loki oracle), and `TestIPLabelFilterMatchesLoki` does the same for `ip()` label filters over 67,584 pattern and value cases.
- `internal/proxy/request_logger_semconv_test.go` verifies request logs use end-user semantic fields (`enduser.*`) without falling back to legacy `user.*`.
- `internal/observability/logger_test.go` verifies resource identity fields are not duplicated into per-line JSON payloads (prevents downstream `message.service.*` / `message.telemetry.sdk.*` field explosion).
- `internal/metrics/procenv_test.go` + related `otlp_test.go` / `system_test.go` locking guards keep `-race` CI deterministic when tests manipulate proc-path globals alongside OTLP pusher goroutines.
- Tenant header and caller credentials: `internal/proxy/tenant_auth_parity_test.go` replays 664 requests recorded from Loki 3.7.9 with `auth_enabled: true` (`testdata/tenant_auth_loki_answers.jsonl`; re-record with `LVP_ORACLE_RECORD_TENANT_AUTH=<loki url> go test ./internal/proxy -run TestRecordTenantAuthLokiAnswers` against a Loki started with `testdata/tenant_auth_loki_config.yaml`) and matches status, Content-Type and body byte for byte; `TestTenantNoAuth_HeaderIgnoredLikeLoki` replays 616 GETs recorded read-only from the e2e stack's Loki (`auth_enabled: false`, `testdata/tenant_noauth_loki_answers.jsonl`; `LVP_ORACLE_RECORD_TENANT_NOAUTH=http://127.0.0.1:13101 go test ./internal/proxy -run TestRecordTenantNoAuthLokiAnswers`) against a proxy with tenancy turned off and checks the header is ignored and never reaches VictoriaLogs (`TestTenantHeader_NumericPassthroughRoutesTenancyOffIgnores` holds the default configuration to numeric routing as before); `tenant_isolation_leak_test.go` runs two replicas with memory, disk and peer caches over a tenant-scoped VictoriaLogs emulator (`vlemu.Tenants`) and proves no read route or tail answers one tenant with another's data; `FuzzTenantHeader` (fuzz-smoke) holds any header to Loki's tenant rules; `credential_passthrough_test.go` checks each credential Grafana's Loki data source can send reaches VictoriaLogs only as configured and never the log or the disk cache; `instrumentation_secrets_test.go` scrapes `/metrics` and `/config` for configured secrets; `delete_route_test.go` holds the delete API behind its own token, separate from the admin token. The live comparison with an auth-enabled Loki is `test/e2e-compat/tenant_auth_loki_test.go` (needs `LOKI_AUTH_URL`; not in a CI group because the stack's Loki runs with `auth_enabled: false`).

## Test Files

| File | Focus |
|---|---|
| `internal/proxy/proxy_test.go` | Loki API contract tests, response format validation |
| `internal/proxy/gaps_test.go` | Feature gap coverage tests |
| `internal/proxy/hardening_test.go` | Security and input validation |
| `cmd/proxy/main_test.go` | Global HTTP wrapper hardening, compression, and not-found edge-path protections |
| `internal/proxy/tenant_test.go` | Multitenancy routing |
| `internal/proxy/critical_fixes_test.go` | Data race, binary ops, delete endpoint, CB metrics |
| `internal/translator/translator_test.go` | LogQL translation unit tests |
| `internal/translator/advanced_test.go` | Complex metric query translation |
| `internal/translator/coverage_test.go` | Edge case coverage |
| `internal/translator/fuzz_test.go` | Fuzz testing harness |
| `internal/translator/fixes_test.go` | IsScalar, without() clause tests |
| `internal/cache/cache_test.go` | L1 cache behavior |
| `internal/cache/cache_bench_test.go` | L1/L3 benchmarks including hot-index extraction (`TopHotKeys`) and bounded read-ahead cycle cost |
| `internal/cache/peer_test.go` | L3 peer cache behavior, distribution, 3-peer shadow-copy efficiency, hot-index serving, and tenant-fair bounded read-ahead prefetch |
| `internal/cache/disk_test.go` | L2 disk cache |
| `internal/middleware/middleware_test.go` | Rate limiter, circuit breaker |
| `scripts/ci/run_security_regressions.sh` | Repo-specific auth, tenant, cache, and hardening smoke gates used by CI |
| `scripts/ci/run_zap_scan.sh` | ZAP baseline/active scan wrapper |
| `scripts/ci/run_nuclei_scan.sh` | Curated nuclei HTTP security checks |
| `test/e2e-compat/` | Docker-based Loki vs proxy comparison |
| `test/e2e-compat/drilldown_compat_test.go` | Grafana Logs Drilldown resource contracts via Grafana datasource proxy |
| `test/e2e-compat/explore_contract_test.go` | HTTP-level Explore contracts for line filters, parsers, direction, metric shape, `label_format`, invalid-query handling |
| `internal/logql/label_filter_number_literal_loki_test.go` | Loki's lexer on a 53,000-query grid (number, duration and bytes literals, keywords, single characters, NUL, a binary operator after a label filter, literals in metric and line filter positions): status and error text equal Loki's recorded answers (`LVP_RUN_ORACLES=1` records them again) |
| `test/e2e-compat/label_filter_number_literal_test.go` | The same literals against live Loki: status and bytes of the 400 |
| `test/e2e-compat/query_semantics_matrix_test.go` | Manifest-driven Loki query semantics parity against the real Loki Compose oracle |
| `test/e2e-compat/query-semantics-matrix.json` | Single source of truth for valid/invalid query combinations and edge-case expectations |
| `test/e2e-compat/grafana_surface_test.go` | Grafana datasource catalog, datasource health, proxy bootstrap/control-plane surface |
| `test/e2e-compat/features_test.go` | Live Grafana-facing edge cases including multi-tenant `__tenant_id__`, long-lived tail sessions, and Drilldown level-filter regressions |
| `test/e2e-ui/tests/url-state.spec.ts` | Pure URL/state builder tests for Explore and Logs Drilldown reloadable state |
| `test/e2e-compat/missing_ops_compat_test.go` | Edge-case parity coverage: unpack (test-data gap), `\|>` pattern match, unwrap duration/bytes, label_replace (nested sum gap) |
| `test/e2e-ui/tests/explore-operations.spec.ts` | Explore Loki operations browser smoke (11 tests, `@explore-ops`) |
| `test/e2e-ui/tests/explore-comprehensive-ui.spec.ts` | Explore UI coverage (15 tests, `@comprehensive-ui`): page load, editor, query execution, results panel, empty results, label filters, `unwrap`; timings recorded as annotations |
| `test/e2e-ui/tests/performance-baseline.spec.ts` | Performance baseline measurements (7 tests, `@performance`, not run in CI): page load, query response, log row expansion, label selector, filter changes |
| `test/e2e-ui/` | Playwright browser smoke tests for datasource UI, Explore, and Logs Drilldown with console/request guardrails |

## Playwright UI Matrix

The browser suite now keeps only browser-only smoke paths. Query parity, Drilldown resource contracts, datasource bootstrap, and most tail protocol coverage live in `test/e2e-compat` or lower-level Go tests so CI does not keep paying Chromium cost for them.

## Compose Screenshot Workflow

The repository includes a direct Playwright capture script for documentation screenshots:

```bash
cd test/e2e-compat
docker compose up -d --build
../../scripts/ci/wait_e2e_stack.sh 180

cd ../e2e-ui
npm ci
npx playwright install chromium
# The script defaults to ports 3100/9428; point it at the compose ports
PROXY_QUERY_URL=http://127.0.0.1:13100 \
VL_INSERT_URL='http://127.0.0.1:19428/insert/jsonline?_stream_fields=app,service_name,level,detected_level' \
npm run capture:screenshots
```

Output directory:

- `docs/images/ui/explore-main.png`
- `docs/images/ui/explore-details.png`
- `docs/images/ui/drilldown-main.png`
- `docs/images/ui/drilldown-service.png`
- `docs/images/ui/explore-tail-multitenant.png`

The capture script writes a fresh seed batch and continues background log ingestion while taking screenshots, so Explore range queries, live tail, and Drilldown screenshots include visible active data.

Optional overrides:

- `SCREENSHOT_FROM` (default `now-5m`)
- `SCREENSHOT_TO` (default `now`)
- `SCREENSHOT_OUT_DIR` (default `../../docs/images/ui`, relative to the working directory)
- `GRAFANA_URL` (default `http://127.0.0.1:3002`)
- `PROXY_QUERY_URL` (default `http://127.0.0.1:3100`; the compose proxy is `http://127.0.0.1:13100`)
- `VL_INSERT_URL` (default `http://127.0.0.1:9428/insert/jsonline?_stream_fields=app,service_name,level,detected_level`; compose VictoriaLogs is on port `19428`)
- `PLAYWRIGHT_EXECUTABLE_PATH` (optional system Chrome/Chromium binary)

CI prefers the runner's existing Chrome/Chromium binary for these shards and falls back to `npx playwright install chromium` only when no system browser is available. That removes the repeated `apt` dependency install from the common GitHub-hosted path.

### CI Shards

| Shard | Commands | Primary focus |
|---|---|---|
| `explore-core` | `npx playwright test --grep @explore-core` | default Explore browser smoke plus API-level proxy-vs-Loki metric parity (`explore-parity.spec.ts`) |
| `regression-datasource` | `npx playwright test --grep @regression`, then `npx playwright test tests/datasource.spec.ts` | `@regression`: API-level parity register (`explore-regression.spec.ts`): log selectors, line filters, parsers and pipelines compared line-for-line on an uncapped window; grouped metric queries compared by series set after Loki's range path has warmed; the `-max-stats-query-series` cap; content checks. Then the Grafana datasource settings smoke |
| `drilldown-core-tail` | `npx playwright test --grep '@drilldown-core\|@explore-tail'` | `@drilldown-core`: Explore detail-panel smoke and single-tenant Logs Drilldown smoke. `@explore-tail`: browser-only multi-tenant (`__tenant_id__` exact and negative regex) plus live-tail recovery |
| `comprehensive-drilldown-mt` | `npx playwright test --grep '@comprehensive-ui\|@drilldown-mt'` | `@comprehensive-ui`: Explore UI coverage (`explore-comprehensive-ui.spec.ts`): page load, editor, query execution, results panel, empty results, filters; timings recorded as annotations. `@drilldown-mt`: multi-tenant Logs Drilldown landing/service/fields plus URL filter-reload persistence |
| `ops-mt` | `npx playwright test --grep '@explore-ops\|@explore-mt'` | `@explore-ops`: Loki operations parity: parsers (json, logfmt), formatting (line_format, label_format, keep/drop), metric queries (count_over_time, rate, unwrap), line filters (regex, negative), aggregations (topk). `@explore-mt`: multi-tenant Explore coverage |

Each shard pays for a stack start, so the former one-tag shards are grouped; the five together select exactly the tests the nine selected before, each once. `explore-core` stays the longest shard.

Specs whose tags match no shard do not run in CI: `explore-click-interactions.spec.ts` (`@click-interactions`), `performance-baseline.spec.ts` (`@performance`), `drilldown-loki-vs-proxy-compare.spec.ts` (`@compare`), and the `@drilldown-cache` groups of `drilldown-cache-regression.spec.ts` other than the labels-sidebar group, which also carries `@drilldown-core`.

## Performance Testing

### Comprehensive UI Coverage & Performance Baselines

Two Playwright suites cover Explore UI behavior and browser-side timings. For proxy and backend read-path benchmarks, see [benchmarks.md](benchmarks.md) and `bench/README.md`.

#### Comprehensive UI Tests
- **File**: `test/e2e-ui/tests/explore-comprehensive-ui.spec.ts` (`@comprehensive-ui`, CI shard `explore-comprehensive`)
- **Tests**: 15 cases covering:
  - Page load and query editor rendering
  - Query execution (stream selector, `sum(rate(...))` metric, json parser, `avg_over_time ... unwrap` metric)
  - Results panel (logs and graph), empty results, a label filter stage (`| level="error"`)
  - Every test asserts no Grafana error toasts, console errors or failed datasource requests
  - Timings are recorded as Playwright annotations (`timing-ms`), not asserted

#### Performance Baseline Tests
- **File**: `test/e2e-ui/tests/performance-baseline.spec.ts` (`@performance`, 7 tests, not run in CI)
- **Thresholds** (the `THRESHOLDS` constant in the spec):
  - **Explore page load**: &lt;3000 ms (asserted)
  - **Simple metric query response**: &lt;5000 ms (asserted)
  - **JSON parsed logs query**: &lt;5000 ms (asserted)
  - **Log entry expansion**: &lt;500 ms (asserted only when the expand button is visible)
  - **Label selector open**: &lt;1000 ms (asserted only when the selector is visible)
  - **Rapid filter application**: recorded against 5000 ms, not asserted on its own
  - **Performance Report**: prints every recorded result and fails if any recorded result exceeded its threshold

#### Running Performance Tests

```bash
cd test/e2e-ui

# Run comprehensive UI tests
npx playwright test tests/explore-comprehensive-ui.spec.ts

# Run performance baseline
npx playwright test tests/performance-baseline.spec.ts

# Run both with one line per test
npx playwright test --grep "@comprehensive-ui|@performance" --reporter=list

# Generate HTML report, then open it
npx playwright test tests/performance-baseline.spec.ts --reporter=html
npm run report
```

#### Performance Trends

To track performance over time:

```bash
# Create baseline
npx playwright test tests/performance-baseline.spec.ts --reporter=list > baseline-$(date +%Y-%m-%d).txt

# Compare against current
npx playwright test tests/performance-baseline.spec.ts --reporter=list > current-$(date +%Y-%m-%d).txt
diff -u baseline-*.txt current-*.txt
```

## E2E Compatibility Matrix

The repo now keeps two different matrix files in `test/e2e-compat`:

- `compatibility-matrix.json` for runtime/version coverage
- `query-semantics-matrix.json` for manifest-driven Loki query/operator parity

They solve different problems and should evolve independently.

The Docker-backed `test/e2e-compat` suite now runs as five functional PR shards instead of one monolithic job. Each shard builds the stack, waits on explicit HTTP readiness checks, and runs only its own test family.

| Shard | Primary scope |
|---|---|
| `e2e-compat (core)` | `TestCompat_*`, `TestExtended_*`, `TestChaining_*`, `TestAlertingCompat_*`, Explore HTTP contracts, `TestLokiFunctions_*`, datasource catalog/health, proxy compatibility surface, pinned matrix vs compose check |
| `e2e-compat (drilldown)` | `TestDrilldown_*` contracts including runtime-family checks, index statistics/volume, Drilldown limits, bounded 24h–7d chunked-merge replay, cluster/level filter features, Loki/Drilldown/VL track scores |
| `e2e-compat (otel-edge)` | OTel label translation, structured metadata, underscore-proxy surfaces, label dedup/translation, `TestEdge_*`, `TestComplex_*` |
| `e2e-compat (tail-multitenancy)` | multi-tenant behavior, tail transport semantics, admin/analytics endpoints, security headers, metrics, gzip and unsupported-codec fallback parity, response-shape and edge checks |
| `e2e-compat (semantics)` | query semantics matrix and operations inventory, `TestOperationsMatrix_*`, `TestLogQL_Exhaustive_*`, `TestPipeline_*`, range metric compatibility, Grafana clickout parity, `TestMissingOps_*` |

The group patterns are anchored regexes in `.github/workflows/ci.yaml`.
`TestHardeningLive_*` runs in `Security / runtime` through
`scripts/ci/run_security_regressions.sh`, and `TestLabelCache_*` runs in
`loki-pinned`. The workflow-aware guard
[`test_e2e_selection.py`](../scripts/ci/tests/test_e2e_selection.py) follows
these selectors and the Grafana runtime manifest. Every top-level compatibility
test must match an executable selector or have an exact-name exemption with a
reason. A new test cannot silently inherit a manual-only family prefix.
The exemptions distinguish local fixture/transport helpers from manual latency,
dense-pattern and high-cardinality memory harnesses; selector reachability does
not prove that a test passed or that a runtime fixture was nonempty.

The chunked-merge replay seeds 721 records over 60h in a private stream, within
the stack's 72h retention. Its 24h, 25h, 48h and 7d requested windows exercise
Grafana's newest-first splitting and frame merging without a UI generator. A
missing fixture fails the check rather than skipping the test. The 7d request
exercises empty older chunks; it does not claim seven days of retained data.

Stack startup now uses [`wait_e2e_stack.sh`](../scripts/ci/wait_e2e_stack.sh) instead of `docker compose --wait` or fixed sleeps. That avoids false failures from services without Docker healthchecks and lets UI and compat jobs share the same readiness logic.

The GitHub-hosted Docker jobs now also prebuild the proxy image once per job through BuildKit cache and start compose stacks with `--no-build`. That keeps the grouped compat shards and UI shards parallel without paying the full Docker rebuild cost every time a stack starts inside the same job.

Compose-backed fleet cache smoke runs on pull requests and post-merge `main` in CI (`e2e-fleet`). The job runs only `TestFleetSmoke_QueryRangeWarmHitIncrementsCacheMetrics` against `test/e2e-fleet/docker-compose.yml`; the other `TestFleet_*`, `TestFleetSmoke_*` and `TestPeerDiscovery_*` tests in `test/e2e-fleet` are run locally.
Tuple smoke contract canary also runs automatically in CI (`tuple-smoke`) by seeding e2e data then executing `scripts/smoke-test.sh`.

## Query Plan Shadow

The proxy is moving from a string translator to a typed query plan (`internal/queryplan`: parse once, build a logical plan, lower it to VictoriaLogs operators in `internal/queryplan/lower`, write them with `internal/queryplan/physical`). A construct is served from the plan once it is native and both shadows show 0 mismatches; the others stay on the translator, and two shadows hold the plan to it:

- **Corpus shadow (CI).** `TestPlanShadow` (`internal/querycorpus`) lowers every query of the plan corpus under the 12 translator option profiles (VictoriaLogs v1.49 and v1.53, label style, stream fields, stream labels, the detected_level body scan, keepLine) and compares the lowered operators with the translator's, both read by the LogsQL parser. For every native row it also asserts that `physical.Encode(lower.Plan(...))` is the translator's text byte for byte: that is the text a flipped construct serves, so a difference of one letter case fails the gate. Mismatches must stay 0; the native count may only rise (`conformance/registry/query-plan.json`, re-recorded with `LVP_QUERY_PLAN_UPDATE=1`). Generated grids add shapes the corpus lacks: line, pattern and label filters, json and logfmt parsers (flags, field lists, aliases) with drop and keep stages, regexp and pattern parsers and decolorize (`extractGrid`, with `TestExtractSpellingClass` for the spellings that differ in bytes only), line_format and label_format (`formatGrid`, with `TestFormatStagesMeanWhatLokiMeans` holding the lowered text to the answers the Loki oracle recorded), and label filters after a parser (`labelFilterParsedGrid`: parsed-key names, aliases, stream labels, `name_extracted` labels, trees, placement among other stages, about 42,000 queries), each held to the translator's text byte for byte (`TestLabelFilterParsedGridShadow`), vector aggregations (`sum`, `count`, `min`, `max`, `avg`, `stddev`, `stdvar` with every `by` and `without` spelling over the five range functions, and their blank-padded spellings: 9,396 queries × 12 profiles, `TestVectorAggGridShadow`, `TestVectorAggBlankPaddingShadow`), range aggregations (`count_over_time`, `bytes_over_time`, `absent_over_time`, `rate` and `bytes_rate` over them, with every range and offset spelling, including the sub-second and compound ranges the division of a rate by the range's seconds reads: 13,818 queries × 12 profiles), and the spellings a quoted value can take (every escape kind, raw control characters, backquotes) in each position of a filter-only query, with fuzz targets in fuzz-smoke (`FuzzPatternLineFilterLowering`, `FuzzLabelFilterLowering`, `FuzzParserLowering`, `FuzzLabelFilterAfterParserLowering`, `FuzzEscapedSpellingLowering`, `FuzzRangeAggLowering`, `FuzzVectorAggLowering`). The translator copies the client's spelling of a regexp line filter and of an exact stream-field matcher, so a value written there with an escape is left to it (`LogSelect escaped spelling`); the grids hold every other native row to the translator's text byte for byte. The VictoriaLogs oracle tests (`LVP_RUN_ORACLES=1`) have VictoriaLogs' own parser read the lowered range aggregations (`TestRangeAggVLOracle`) and vector aggregations (`TestVectorAggVLOracle`) on v1.49.0 and v1.53.0 and compare them with the translator's; `TestVectorAggGroupingMatchesLoki` holds the aggregation and the `by` or `without` labels the lowering writes to Loki's own syntax tree (test/oracle/loki). These oracle tests are run by hand for now (not wired into CI).
- **Unwrap aggregations (CI).** `unwrapGrid` (`internal/querycorpus`, about 11,500 queries × 12 profiles, about 8 seconds, `TestUnwrapGridShadow`) holds range and vector aggregations over an unwrapped label to the translator's text byte for byte: every function (`sum`, `avg`, `min`, `max`, `first`, `last`, `stddev`, `stdvar`, `quantile` with the spellings of its parameter, `rate`, `rate_counter`) with every conversion (none, `bytes()`, `duration()`, `duration_seconds()`) and unwrapped labels named like functions, selectors, line filters and label filters before and after the unwrap, every range and offset spelling, every grouping spelling on the range and on the vector aggregation, the spellings of the unwrap stage the translator reads from the query text, and quoted `| unwrap`, `| json` and `| stats` text. Mutations of the lowering (a dropped rule of the series identity, the squaring of `stdvar`, the `detected_level` grouping, the spelling screen) each fail the grid. `FuzzUnwrapLowering` (`internal/queryplan/lower`) does the same on fuzzed values. `BenchmarkUnwrapQueries*` measure the translator, the lowering and the plan alone on the corpus's unwrap aggregations.
- **Parser metrics (CI).** `parserMetricGrid` (`internal/querycorpus`, about 12,500 queries × 12 profiles, about 5 seconds, `TestParserMetricGridShadow`) holds range and vector aggregations over a log query with a `json` or `logfmt` parser to the translator's text byte for byte: the parsers with flags, extraction lists and aliases, in front of and behind each other, label filters behind them (parsed keys with and without underscores, a stream label, the error labels), every function that reads rows and every function over an unwrapped label with its conversions, grouped by labels the translator resolves by text on the range and on the seven vector aggregations, ranges and offsets, quoted text, and the parsers and stages that stay with the translator. Mutations of the series identity of a parser query, the label guard, the rate filter, the narrowed logfmt unpack and the stream value restore each fail it. `BenchmarkParserMetricQueries*` measure the translator, the lowering and the plan alone on the corpus's parser aggregations.
- **Binary operations and label_replace (CI).** `TestBinOpGridShadow` (`internal/querycorpus`) holds the lowering of binary operations and `label_replace` calls to the translator's text byte for byte on a generated grid (every operator, operands of every kind, numbers in several spellings, `bool`, every vector matching clause in several spellings, operator and blank spellings, quoted text the translator's scans read as syntax, `label_replace` nested, with argument spellings and around delegated shapes: 1,497 queries × 12 profiles); `FuzzBinOpLowering` runs in fuzz-smoke. `TestBinOpLoweringMatchesLoki` (`LVP_RUN_ORACLES=1`, by hand) holds the operator, the vector matching, the operands and the `label_replace` arguments of every lowered grid row to Loki's own syntax tree (test/oracle/loki `facts`).
- **Rankings (CI).** The proxy evaluates `topk`, `bottomk`, `sort` and `sort_desc` itself (it translates the operand alone and ranks each step of the answer), so the shadow holds the lowering of a ranking to the proxy's route, not to the translator's text for the whole query: `proxy.PostAggregationPhysical` gives the reference (a `RankStep` per ranking and a `SeriesAgg` per `sum`, `avg`, `min`, `max` or `count` layer between rankings over the operand's operators, and the operand's request), and a native row is equal to it as operators and byte for byte as the operand's request. These rows are counted apart as `native_proxy_evaluated` in `query-plan.json`. `TestRankStepGridShadow` (`internal/querycorpus`, 3,136 queries × 12 profiles, about 2 seconds) and `TestRankGridShadow` (5,731 queries × 12 profiles, about 3 seconds) hold it on generated grids: every ranking name in several letter cases and blank placements, `k` in several spellings, `by` and `without` clauses in both positions, range aggregations with selectors, line filters, parsers, label filters and unwrapped values, vector aggregations and nests of them, `label_replace` around either, rankings over rankings with `sum`, `avg`, `min`, `max` and `count` layers between them, quoted text the translator's scans read as syntax, and the operands and layers that stay with the string translator. Mutations of the clause, the layers, the deepest-ranking choice, the sort-with-clause rule and each spelling screen fail them or the unit tests. `TestEvaluatedOperandSpanIsTheProxysOperand` holds the operand the lowering screens to the text the proxy slices; `TestRankLoweringAllocatesNoMoreThanTheProxyRoute` checks the allocations of every lowered shape against the proxy's route (the decision and the operand's translation), with and without stream labels; `FuzzProxyEvaluatedLowering` runs in fuzz-smoke; `BenchmarkRankRoute*` and `BenchmarkRankLowered*` measure the route and the plan (see [translation reference](translation-reference.md)). `TestRankMatchesLoki` (`LVP_RUN_ORACLES=1`, by hand) holds the outermost ranking, its `k` and its clause of every lowered grid row Loki accepts to Loki's own syntax tree (test/oracle/loki `facts`); the levels below the outermost are not in the facts.
- **Stddev and stdvar (CI).** The proxy folds `stddev` and `stdvar` itself too (it translates the operand alone and takes the population deviation or variance of each group's series at each step), so the shadow holds the lowering of a stddev or stdvar root, and of the `sum`, `avg`, `min`, `max` and `count` layers above a ranking, to the proxy's route in the same way: a `SeriesAgg` over the operand's operators, and the operand's request byte for byte, counted as `native_proxy_evaluated`. `TestSeriesAggGridShadow` (`internal/querycorpus`, 8,341 queries × 12 profiles, about 4 seconds) holds it on a generated grid: both operators in every spelling of the `by` and `without` clause (both positions), operands of every kind (range aggregations with parsers, label filters and unwrapped values, vector aggregations and nests, `label_replace`, rankings, another stddev), the layers `sum`, `avg`, `min`, `max` and `count` above, rankings and `label_replace` around, letter case, blanks, comments, outer parentheses and quoted text the proxy's scans read as syntax (a `| json` inside a line filter makes the proxy rewrite a stddev by a label as the parsed-label form, which then stays with the translator). A stddev, stdvar or ranking below the one level the proxy evaluates, or under a layer it does not take (`sum(stddev by (app) (..))`, `topk(2, stddev by (app) (..))`, `stddev(topk(3, ..))`), is lowered to the chain Loki evaluates, and the shadow records it as a known divergence (`semantics/stddev-stdvar-nested-or-binary`, whose `corpus.dimensions.shapes` are corpus rows; `TestNestedEvaluationCaseRowsAreInTheCorpus`), recognised by the shape of the logical plan, not of the lowering. Mutations of the operator, the clause, `without`, the parsed-label test (in the plan and in the screen), the layers, the label_replace rule, the outer parentheses and the shadow's own classification each fail the grid or the unit tests. `TestEvaluatedOperandSpanIsTheProxysOperand` holds the slice to the proxy's operand for these roots too; `TestRankLoweringAllocatesNoMoreThanTheProxyRoute` checks the allocations of every lowered shape against the route; `FuzzProxyEvaluatedLowering` covers stddev and stdvar roots; `BenchmarkSeriesAgg*` measure the route and the plan. `TestSeriesAggMatchesLoki` (`LVP_RUN_ORACLES=1`, by hand) holds the aggregation and clause of every lowered grid row Loki accepts to Loki's own syntax tree (test/oracle/loki `facts`); the levels below the outermost are not in the facts.
- **Live shadow (opt-in).** `-translate-shadow-sample` (0..1, default 0) compares the two on that share of real translation-cache misses, in the background, with no effect on answers, caches or VictoriaLogs requests; results are counted in `loki_vl_proxy_lowering_shadow_total{result,construct}` (see [observability](observability.md)). A query served from the plan (`result="flipped"`) has nothing to compare; the constructs still on the translator are compared as before. The live shadow and the plan share one mapping from the translator's options to the lowering's profile (`loweringProfile`). It covers the queries the translator translates, and, compared with the proxy's route instead, the rankings, `stddev` and `stdvar` roots the post-aggregation handlers evaluate (see the live shadow of the roots the proxy evaluates, below).
- **VictoriaLogs oracle (PR quality report).** Every corpus row the plan lowers is sent to VictoriaLogs v1.49 and v1.53 in both forms, and the gate fails when VictoriaLogs reads the plan's filter differently from the translator's. Where the translator is the wrong one, the row is listed in `internal/querycorpus/testdata/known-typed-divergences.json` with the registry case that records it (for example `semantics/translator-reads-quoted-text-as-syntax`): it is reported, not failed, and a listed row that reads alike again fails as stale. A class the generated rows keep producing in new spellings is recognised by content instead (`translatorMisreadsQuotedText`: a label filter value holding `detected_level` or `service_name`), in the oracle pairing and the plan shadow alike.
- **Route parity (unit).** `TestQueryPlanLowering_RouteParity` (`internal/proxy`) sends the same log queries (selectors, line filters, pattern filters, label filters with `ip()` and repeated filters, and queries the plan does not cover) through the proxy with `-query-plan-lowering` on and off, under seven translator profiles, against a fake VictoriaLogs, and asserts identical client answers and identical upstream requests; it also asserts which queries were served from the plan.
- **Metric stages (CI).** `metricStagesGrid` (`internal/querycorpus`, about 60,000 queries × 12 profiles, about 25 seconds, `TestMetricStagesGridShadowPart1` to `Part4`, a quarter of the grid each so CI can run them on different shards) holds range and vector aggregations over a log query with `drop`, `keep`, `decolorize`, `line_format`, `label_format` and the `regexp` and `pattern` parsers to the translator's text byte for byte: every 32nd query of the parser, extract, format and parsed label filter grids inside the five row functions and, rotating, the five vector aggregations with `by`, `without` and derived-label clauses and an unwrapped label (`LVP_METRIC_STAGES_STRIDE=1` runs every query of the source grids, about 985,000 in about 7 minutes), plus hand-written shapes: a trailing `keep`, `keep` and `drop` of the level fields, a stream label read after a parser, stages after the `unwrap`, and quoted text the translator reads as a parser, an unwrap, a `without` clause, a `bool` modifier or a stats pipe in each stage that holds a value (including a drop of matchers, which prints no pipe). The unit tests of `internal/queryplan/lower` (`TestPlanLowersMetricStages`, `TestPlanDelegatesMetricStages`, `TestPlanRegexpParserResolvesNoLabels`, `TestTrailingKeep`, `TestAnyClientString`) and `FuzzMetricStagesLowering` (fuzz-smoke; about 4,700 execs per second on four workers) hold the same shapes; mutations of the drop and keep builders, the keep-identity delegation, the rate filter conditions and the quoted-text reader each fail the grid or the unit tests. `BenchmarkMetricStageQueries*` measure the translator, the lowering and the plan alone on the corpus's stage aggregations.
- **Nested vector aggregations (CI).** `TestVectorAggNestedGridShadow` (`internal/querycorpus`, about 10 s) holds the lowering of a vector aggregation over other ones to the translator's text byte for byte on a generated grid (every outer and inner `sum`, `avg`, `min`, `max` and `count` with the clauses each level can take in both positions, the five range functions, three and four levels, selectors, line filters and ranges, json and logfmt operands, renamed, derived and repeated labels, quoted text the translator's scans read as syntax, binary operations and `label_replace` around a nest, and the shapes that stay delegated: `stddev` and `stdvar` at any level, rankings and binaries under a nest, unwrap operands, `without ()`: 6,905 queries × 12 profiles); `FuzzNestedVectorAggLowering` (two to four levels) runs in fuzz-smoke. The same grid is read by the VictoriaLogs parser (`TestVectorAggVLOracle`) and by Loki's syntax tree (`TestVectorAggGroupingMatchesLoki`: the outer operation; the labels a nest keeps are not in Loki's tree, so they are held by the translator's own nest tests and the shared `translate.LevelKeeps`), by hand with `LVP_RUN_ORACLES=1`.
- **Binary operation helpers and bool/without (CI).** `TestBinOpLoweringAllocatesNoMoreThanTheTranslator` and `TestBinOpMatchingHelpersAllocate` (`internal/queryplan/lower`) hold the lowered binary operations to the translator's allocation count on numbers, two operands, `on` and `ignoring` (490 shape checks with and without stream labels) and the shared helpers to their allocation counts. `TestBinOpMoreGridShadow` (`internal/querycorpus`) holds the `bool` and `without`-operand spellings and fails if a `bool` row is lowered: the translator drops `bool`, which is not Loki's reading. The `without` rows are held to the proxy's binary route (below).
- **Range call spelled with a blank (CI).** `TestRangeCallBlankSpellingShadow` (`internal/querycorpus`, 5,416 padded queries × 9 profiles, about 1 second) pads a range aggregation at the top of a query, as an operand of a binary operation and as the first argument of `label_replace` with a blank, tab, newline or two blanks at every place two tokens meet, and holds the lowering to the translator's text byte for byte: a blank before the function's parenthesis (`rate ({app="a"}[5m])`) is a phrase search to the translator and the text screen leaves it to the translator (`RangeAgg spelling`), and the grid asserts the screen turns away only those queries (the unscreened lowering differs from the translator on each). `TestRangeCallBlankSpellingMatchesLoki` (`LVP_RUN_ORACLES=1`) records that Loki accepts every spelling and reads the same function; `FuzzRangeAggLowering` and `FuzzBinOpLowering` pad the call at every place, and the VictoriaLogs oracle's typed-selector pairs are made on what the proxy sends: a screened spelling makes no pair (`TestTypedPairLeavesBlankBeforeRangeCall`), a `quantile_over_time` phi above 1 is clamped to 1 as the handlers do, `rate_counter` (a marker the proxy answers itself) makes none, and a ranking the proxy evaluates is paired with the operand request the proxy sends (`proxy.PostAggregationPhysical`, `TestTypedPairsAreWhatTheProxySends`). The open difference is `semantics/translator-reads-blank-before-range-call-as-text`.
- **LogQL Unicode characters and termination (CI).** `TestUnicodeRunesMatchLoki` (`internal/logql`, 19,370 queries, about 70 milliseconds) holds the parser to Loki's recorded answers for the characters of every Unicode category (edges of the blocks) in 150 positions: label names, numbers, durations, ranges, offsets, name lists, function arguments, stage names, strings, backquotes and comments. It counts and bounds four limits (white space the scanner still skips, a stage that is a bare name, a character that starts no token as a label name, 206 messages that differ in text only: white space, ASCII symbols, a lone `!`, a quote); the other 16,839 answers have Loki's status and text. Recorded with the Loki oracle (`LVP_RUN_ORACLES=1`). `FuzzParseTerminates` (fuzz-smoke) runs 20 entry points on any input and fails past 200 ms or a mebibyte per input byte; `TestEntryPointsTerminate`, `TestEveryDecimalDigitTerminates` (first, middle and last digit of every Nd block, 22 positions) and `TestHangingQueriesAreRejectedLikeLoki` (the 80 probe queries with Loki's answers) are its deterministic form. `TestRejectedQueryNeverReachesVictoriaLogs` (`internal/proxy`) sends the queries to every route that takes one and asserts Loki's 400 and zero requests to the fake VictoriaLogs; `TestCompat_LogQLUnicodeRunesLikeLoki` (`test/e2e-compat`, by hand: `go test -v -tags=e2e -run '^TestCompat_LogQLUnicodeRunesLikeLoki$' ./test/e2e-compat/`) compares 12 of them with live Loki.
- **Parser labels (CI).** `TestParserLabelsGridShadow` (`internal/querycorpus`, about 10 s) holds the lowering of the metric queries whose labels a parser extracts to the translator's text byte for byte on a generated grid (grouping and unwrapped labels with an underscore behind json, logfmt, regexp and pattern, the names a query defines itself, `_extracted` labels, a `without` fold over every extraction list, alias, capture and flag the translator follows or does not, a trailing `keep` with the stages that end it, matchers and error labels, the narrowed logfmt unpack, label filters and drops after the parser, quoted text the translator reads as syntax: 30,182 queries × 12 profiles; under the race detector or coverage, which the CI shards run with, one in 24 of the generated queries and every hand-written shape: 140 s down to 5.5 s under `-race`, `LVP_PARSER_LABELS_STRIDE=1` takes them all); `TestParserLabelsGridCatchesMutations` fails five wrong lowerings; `TestParserLabelsLoweringAllocatesNoMoreThanTheTranslator` holds the allocations of the lowering to the translator's for the shapes this step lowers (the ones above it are delegated, `parser label cost`); `FuzzParserLabelsLowering` runs in fuzz-smoke; `TestParserLabelsVLOracle` (`LVP_RUN_ORACLES=1`, by hand) has VictoriaLogs' parser read the lowered grid.
- **Control bytes in a label filter value (CI).** The string translator marks the names of `<name>_extracted` filters with a `\x02` byte in the text of their stage and reads the first pair of `\x02` bytes of every other stage as such marks, so a label filter value behind a parser that holds two of them (`o="\x02\x00\x02"`) is cut out of the query it writes (`o:=""`). `TestMarkerBytesInLabelValueMeanWhatLokiMeans` (`internal/queryplan/lower`) holds the plan to Loki's recorded answers over 32 rows (four values, `=` and `!=`, a row that holds the value, an empty label, no label and another value; `LVP_RUN_ORACLES=1` records them again) with a model of how VictoriaLogs reads the filter in front of the first unpack, and fails when the translator stops differing. `translatorReadsStageMarker` recognises the shape by the decoded values (in `internal/queryplan/lower` and in `internal/querycorpus`, where the plan shadow counts the case's request as a known divergence), and `FuzzLabelFilterAfterParserLowering` leaves it out (seed `control-chars-in-label-value`); the open difference is `semantics/translator-reads-marker-bytes-in-quoted-value`.
- **Keyword label names (CI).** A label Loki accepts and LogsQL reads as a keyword (`not`, `in`, `options`, `contains_all`, `and` and `or` after a label function drops a leading underscore) is quoted by the lowering, where the string translator writes it bare and VictoriaLogs rejects the query. `internal/logsql/testdata/keyword-field-names-vl.jsonl` holds VictoriaLogs' own verdicts (v1.49.0 and v1.53.0, 1,166 queries: every such word bare and quoted at every place a filter or a pipe names a field, and the words VictoriaLogs reads bare, which stay bare); `TestParseKeywordFieldNamesMatchVictoriaLogs` holds the proxy's LogsQL parser to them (it accepts everything VictoriaLogs accepts), `TestQuoteKeywordField` and `TestKeywordFieldName` the quoting, and `TestPlanQuotesKeywordFieldNames` (`internal/queryplan/lower`) the lowered text of each served shape against the translator's text with the name quoted (`lower.QuoteKeywordFieldText`, which the corpus shadow and the live shadow apply to the translator's text too). The lowering fuzz targets (`FuzzParserLowering`, `FuzzExtractLowering`, `FuzzLabelFilterLowering`, `FuzzLabelFilterAfterParserLowering`) apply it as well and leave out a query the translator cannot write (a json expression for a key called `or` or `and`, a label name that holds `detected_level` or `service_name`). On the Loki side, `TestKeywordLabelNamesMatchLoki` (`internal/logql`, 1,456 queries) holds the proxy to Loki's recorded answers (acceptance and text) for the 15 words of Loki's lexer in every place a label name stands, and `TestLabelsKeepCaseAroundFunctionWords` the case of `Ip` and `Label_replace`. The response cache key carries a version for the queries whose reading changed (`keywordLabelResponseKeyVersion`, `internal/proxy`): `TestKeywordLabelResponseKeyVersionFencesTheChangedReadings` pins which queries it fences (and that an unaffected query costs no allocation), `TestKeywordLabelFenceMovesOnlyAffectedKeys` that it moves no other key of the registry and matrix corpus, and `TestKeywordLabelFenceDoesNotServeAnOldCachedAnswer` that an answer cached under the old key is not served. `TestCompat_KeywordLabelNamesLikeLoki` (`test/e2e-compat`, group semantics) compares the proxy with Loki on `{in="x"}`, `| in="x"` and `| not="x"` over a fixture ingested alike into both, and the 400 and its text for a selector with `of` and for `| keep OF`.
- **Outbound guard and the VictoriaLogs clone round trip (CI).** Every query the proxy sends passes `guardOutboundQuery` (`internal/proxy`; `TestEveryBackendSenderGuardsItsQuery` holds every function that sends a query to it), which runs `logsql.GuardReservedFieldCase`: a filter on `_time`, `_stream` or `_stream_id` in another letter case becomes a copy into a scratch field, a filter on it and its delete, because VictoriaLogs prints a quoted `"_TIME"` bare when it clones a query and panics on its own print (VictoriaMetrics/VictoriaLogs#1942). `TestGuardReservedFieldCase` and `FuzzOutboundGuard` (`internal/logsql`, fuzz-smoke) pin the rewrite, its idempotence and the untouched text of every other query with no allocation; `TestOutboundGuardOverTheCorpus` (`internal/querycorpus`) holds the corpus texts of every profile, the lowering's and the translator's, byte for byte unless the query names such a field; `TestOutboundGuardOnTheServedPath` follows a log query, an unwrap metric and an instant query through the handlers. The VictoriaLogs oracle's `roundtrip` command runs a text through the steps of the `/select/logsql/query` handler (parse at a timestamp, the request's `_time` filter, the sort and limit pipes, `GetLastNResultsQuery`) and every clone on the way with the panic recovered; `TestPlanTextsSurviveVictoriaLogsClone` (`internal/queryplan/lower`) holds every text the proxy sends for a grid of reserved and keyword names in 35 positions, unwrap included, to the recorded v1.49.0 and v1.53.0 verdicts (`testdata/vl-clone-roundtrip.jsonl.gz`, `LVP_RUN_ORACLES=1` records them again), and `TestReservedFieldCaseMatchesLoki` holds the answers of the texts sent for unwraps and label filters on `_TIME` and `_Stream_ID` to Loki's (the Loki oracle `process`, which also reports whether the selector selects the stream, and the VictoriaLogs oracle `store`). `TestCompat_ReservedFieldNameCaseLikeLoki` (`test/e2e-compat`, group semantics) compares them with Loki on the stack.
- **Live shadow of the roots the proxy evaluates (opt-in).** The post-aggregation handlers translate only the operand of a ranking, `stddev` or `stdvar` (and of the `sum`, `avg`, `min`, `max` and `count` layers above a ranking), so the translation's live shadow never sees the whole query. On the same `-translate-shadow-sample` share of their requests, both handlers (`query` and `query_range`) hand the whole query and the operand's request they have just sent to a background comparison (`shadowProxyEvaluated`, `internal/proxy`): the lowering of the whole query against the reference the corpus shadow uses (one `RankStep` or `SeriesAgg` per level the proxy evaluates over the decoded operand, and the operand's request byte for byte), counted as `result="native_proxy_evaluated"` when equal, `mismatch` when not, `not_lowered` with the construct of a delegation, and `known_divergence` when the plan holds a level the route drops (`semantics/stddev-stdvar-nested-or-binary`); the constructs are `Rank`, `VectorAgg stddev`, `VectorAgg stdvar` and `VectorAgg` (a layer over a ranking). It reuses the shadow's two slots, 100 ms budget, panic recovery and shutdown, translates nothing again, and reads the tenant's stream label names without starting a refresh. `TestLoweringShadow_ProxyEvaluatedRootsAreHeldToTheRoute` (both endpoints, three label profiles), `TestLoweringShadow_ProxyEvaluatedMismatchIsCounted`, `TestLoweringShadow_ProxyEvaluatedOffAtSampleZero`, `TestLoweringShadow_ProxyEvaluatedChangesNoAnswer` (answers and VictoriaLogs requests with the shadow on equal those with it off) `TestLoweringShadow_ProxyEvaluatedShutdownLeavesNoGoroutines` and `TestLoweringShadow_ProxyEvaluatedReadsStreamLabelsWithoutRefresh` hold it; `TestLoweringShadow_ProxyEvaluatedSkipAllocatesNothing` and `BenchmarkShadowProxyEvaluatedSkipped` hold a request the sample skips to no allocation.
- **Stddev and stdvar by a parsed label (CI).** For a `stddev` or `stdvar` by labels of a query with a parser the proxy translates the series-level form of `max by (labels) (operand)` and cuts the text (the last stats pipe dropped, then the value's alias removed, or a rate summed by the first grouping); the plan builds the same form (`buildVectorAgg`, `buildNestedVector`) and makes the same cut on its typed pipes (`parsedLabelOperand`, `internal/queryplan/lower`). The shadow holds it to the proxy's route like the other stddev roots (`native_proxy_evaluated`). `TestSeriesAggGridShadow` adds `parsedLabelFoldGrid` (4,876 more queries, 8,341 in all, about 4 seconds: `json`, `logfmt`, `regexp`, `pattern` and `unpack` with flags, extraction lists, label filters, drops, keeps and formats after them, every range function, nests of `sum`, `avg`, `min`, `max` and `count`, groupings by parsed, stream, derived and dotted labels and by case variants of `_time`, `_stream` and `_stream_id`, both clause positions, and the shapes left to the translator); mutations of the cut (the first stats pipe instead of the last, the alias kept, no rate branch, the rate summed by the last grouping, the text-versus-plan gate dropped, a `without` clause rewritten; `TestRouteOperandCutDeclines` holds where the cut declines) each fail it or the unit tests (`TestPlanRebuildsParsedLabelOperand`, `TestPlanDelegatesStddevShapes`). `TestRankLoweringAllocatesNoMoreThanTheProxyRoute` measures these shapes against the route's own rewrite (`proxy.PostAggregationOperand`: the `max by` translation and the cut); a sum with no clause over a rate allocates more and is left to the translator (`VectorAgg stddev parsed label cost`). `FuzzProxyEvaluatedLowering` generates `regexp` and `pattern` operands and parsed groupings too; `BenchmarkSeriesAggParsedLabel*` measure the route and the plan on these shapes; `TestSeriesAggMatchesLoki` holds their aggregation and clause to Loki's own syntax tree (`LVP_RUN_ORACLES=1`). The live shadow's handler test holds `json`, `logfmt`, `regexp` and `pattern` rows to the route on both endpoints.
- **Reserved field names as labels (CI).** A Loki label called `_time`, `_stream`, `_stream_id` or `_msg` is a tag of the stream to VictoriaLogs, and the lowering filters the tag (`{_time="a"}`; a comparison reads the tag's value out of `_stream`), where the string translator writes a field filter VictoriaLogs rejects or reads as its own time, stream or line filter; `_TIME`, `_Stream` and `_STREAM_ID` are the stream filter in a selector and a copy into a scratch field for a label filter. `TestReservedFieldLabelsMatchLoki` (`internal/queryplan/lower`) holds, for 49 queries over 15 streams, the lines Loki's pipeline keeps (the Loki oracle, `process`, which also evaluates the selector's matchers) equal to the rows VictoriaLogs returns for the lowered text (the VictoriaLogs oracle, `store`: the rows stored as the Loki push API stores them, v1.49.0 and v1.53.0), with the translator's text and its answer recorded beside them (`testdata/reserved-field-labels.jsonl`; `LVP_RUN_ORACLES=1` records them again). `TestParseReservedFieldNamesMatchVictoriaLogs` (`internal/logsql`, 352 recorded VictoriaLogs verdicts) pins which spellings VictoriaLogs reads as its own filters, `TestReadsReservedField` the queries the shadows leave out (`lower.ReadsReservedField`: the corpus shadow counts them as a known divergence, the live shadow as the translator's error, and the lowering fuzz targets skip them), `TestReservedFieldAfterAStageIsDelegated` the label filters left to the translator after a parser, drop, keep or label_format, and `TestReservedFieldLabelServedFromThePlan` (`internal/proxy`) the request VictoriaLogs gets and the stream labels of the answer. `reservedFieldLabelResponseKeyVersion` fences the cached answers (`TestReservedFieldLabelResponseKeyVersionFencesTheChangedReadings`, `TestReservedFieldLabelFenceMovesOnlyAffectedKeys`, `TestReservedFieldLabelFenceDoesNotServeAnOldCachedAnswer`). `TestPlanTextsSurviveVictoriaLogsClone` (`internal/queryplan/lower`) holds every text the lowering writes for these names, and for the words LogsQL reads as keywords, under three profiles to VictoriaLogs' clone of a query: the VictoriaLogs oracle's `roundtrip` command runs each text through the steps of the `/select/logsql/query` handler (parse at a timestamp, the request's `_time` filter, the sort and limit pipes, `GetLastNResultsQuery`) and checks that every clone (print, parse again) reads the query back, with the verdicts of v1.49.0 and v1.53.0 recorded in `testdata/vl-clone-roundtrip.jsonl` next to a spelling grid that records the one form VictoriaLogs accepts and cannot read back, a filter on a quoted name in another case. `TestCompat_ReservedFieldLabelsLikeLoki` (`test/e2e-compat`, group semantics) pushes streams labelled `_time`, `_stream_id`, `_TIME` and `_msg` alike to Loki and to VictoriaLogs' Loki push API and compares the proxy with Loki on 14 selector matchers, label filters and comparisons.
- **Route rule of metric queries (CI).** `lower.Route` (`internal/queryplan/lower`) decides, from the plan and the request, how the handlers answer a range aggregation (`count_over_time`, `bytes_over_time`, `rate`, `bytes_rate`, `absent_over_time` over line and label filters, and `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time` over an unwrapped label, with or without a `by` clause of plain labels) and `sum`, `avg`, `min`, `max` and `count` levels over one (over the unwrap functions too, which the lowering hands back typed stats for): one `stats_query` for an instant request; for a range request one `stats_query_range` bucket per window when the range equals the step, buckets of gcd(step, range) when it is longer, a window phase filter and a bucket per step when it is shorter; whether the level presence probe runs first, whether the series keep their stream identity, and the empty-input guard of an ungrouped instant answer; `absent_over_time` keeps no series (one ungrouped count, no probe), and a function over an unwrapped label also counts the rows its conversion rejects and, for a range under the step, is read from VictoriaLogs' own step buckets. `TestPlanRouteParity` (`internal/proxy`, `CI=1`, about 15 seconds) predicts each such run from the rule (status, route steps, and each VictoriaLogs call by its route shape: endpoint, parameters, the query's filters and its route pipes from the window phase filter or the first stats pipe on; the pipes between, which the handlers build from the lowering's text, are not compared, and the corpus shadow holds the lowering's own text) and requires it to equal the handlers' run: 5,706 of 24,970 runs native, 0 mismatches, the route digests unchanged. `without` clauses are decided too (`WithoutFold`: the proxy folds the series rows, VictoriaLogs folds a range equal to the step by cutting the excluded labels out of the stream text, or a sliding window of a single `sum without` reads the cut buckets), the harness writing the folded text with the handlers' builders and so refusing a fold the handlers would not push down; `TestMetricRouteWithoutEdges` holds 36 `without` shapes the corpus lacks (derived labels, underscores, pattern, label and `_msg` filters, levels above) to the handlers and pins each fold per profile. `TestMetricRouteRangeFunctionEdges` holds 53 shapes the corpus lacks (every conversion, translated and derived labels, filters before and after the unwrap, groupings, ranges that divide the span and ones that do not) to the handlers, or to the name of their delegation. `TestMetricRouteVectorUnwrapEdges` does the same for the vector aggregations over those functions (every operator and function, the extreme the handlers push into the buckets of a sliding window, the sum of all lines against the sum of each series, `without` clauses, derived grouping labels). `TestMetricRouteCatchesAWrongDecision` changes one decision at a time (the fold included) and requires the harness to see it; `TestRouteDecidesTheWindows`, `TestRouteTextIsTheLowering`, `TestRouteAllocatesNoMoreThanTheLowering` and `FuzzRoute` (fuzz-smoke) hold the rule itself. Parsers, offsets, the levels the proxy evaluates, quantile, stddev, stdvar, first, last and rate_counter over an unwrapped label, an unwrap rate, a stage after the unwrap, a vector aggregation over absent_over_time, over a nest of levels or grouped by the line `_msg` (each named `<kind> route unwrap <what>`), and the labels called `_time`, `_stream`, `_stream_id` or `_msg` (the handlers still send the translator's filter there) are delegated by name (`<kind> route ...`).
- **Binary operations with a `without` operand (CI).** Loki applies a `without` clause to the aggregation it is written on; the string translator's text for `sum without (pod) (..) / 2` drops every clause and folds the whole operation by the first one (`semantics/translator-folds-binary-by-operand-without`), a text the proxy never sends: its binary route reads the query again and runs each operand that is not a number as a query of its own, folded by its own clause. The plan lowers each operand alone (the fold on the operand) and the shadows hold such a root to that route (`proxy.BinaryOperandsPhysical`: the operator, the vector matching and each operand's request byte for byte, counted as `native_proxy_evaluated`), never to the translator's text, which `checkBinaryRoute` rejects as a lowering. `TestBinOpMoreGridShadow` (3,159 queries × 12 profiles, about 7 seconds) covers a clause on either side and both (head and tail form, every aggregation, label list spellings, different lists), numbers on either side, `on`, `ignoring`, `group_left` and `group_right`, unwrapped and parser operands, nested aggregations and operations, `bool`, set operators and `label_replace` around (delegated: `LabelReplace operand BinOp`); `without ()` is lowered as Loki reads it (every series of the operand kept, a fold by no label) and recorded as the known divergence `semantics/binary-operand-without-empty-collapses`, because the translator reads the clause as none and writes the operand ungrouped, which reaches VictoriaLogs as one series on a range query whose step equals its range (`TestEmptyWithoutOperandsKeepEverySeries` holds the lowering to the same operand under a labelled clause; a `without ()` in a nest or over an unwrapped label stays with the translator as `BinOp operand without empty nest` and `... unwrap`, and on its own, under a ranking, `stddev` or `label_replace`, and in the route rule it is still delegated). `TestBinaryRouteCheckCatchesWrongLowerings` holds the check to failing the translator's reading, a dropped or changed fold, swapped operands, another operator, a dropped matching and another request; `TestBinaryRouteLoweringAllocatesNoMoreThanTheRoute` (4,288 shapes, with and without stream labels) holds the lowering to the route's allocations; `FuzzBinOpLowering` draws the clauses (head, tail, nested, empty) and holds each operand to the translator's text for it alone (an empty clause to the lowering of the same query with a label in it); `BenchmarkBinaryWithout*` measure the route and the plan, `BenchmarkBinaryEmptyWithout*` and `BenchmarkBinOpEmptyWithout*` the `without ()` operands; the live shadow counts these roots as `not_lowered` `BinOp` (`TestLoweringShadow_BinaryWithWithoutOperandIsNotCompared`). `TestBinOpLoweringMatchesLoki` (`LVP_RUN_ORACLES=1`) holds the clause of every vector aggregation level of each operand and each folded operand to Loki's syntax tree (test/oracle/loki `facts`, which now reports the operands' clauses); it found that Loki rejects a label list ending in a comma (`without (pod,)`), which the proxy's parser accepts (`semantics/label-list-trailing-comma`, open).
- **Binary operations with a `vector(n)` operand (CI).** Loki reads `vector(n)` as one series without labels with the value n at every step, a vector that takes part in vector matching (`rate(..) + on() vector(0)`); the string translator's text for a binary operation with one searches for the call as a phrase or drops the operand (`semantics/translator-reads-vector-operand-as-text`), a text the proxy never sends: `evaluateBinaryLogQLOperand` answers the operand from the constant and asks VictoriaLogs nothing. The plan lowers the operand, next to a range or vector aggregation, to `physical.VectorOp` (its own form in `physical.Canon`, never equal to the number n) and the shadows hold such a root to the proxy's binary route (`proxy.BinaryOperandsPhysical`, counted as `native_proxy_evaluated`). `TestBinaryOperandsPhysicalReadsVectorLiteralLikeTheHandler` (`internal/proxy`) pins the route's description to the handler: for each operand text, instant and range, the route says VectorOp exactly when the handler answers a one-series constant with no request to VictoriaLogs, with the same value, and translates nothing for it; `FuzzVectorLiteralReadings` (fuzz-smoke) holds the lowering's reading of the operand text (`lower.VectorLiteral`, the spelling screen) to the route's on any text. A spelling the route sends on as a query (`vector (1)`, `semantics/vector-call-blank-before-parenthesis-as-query`) is turned away by `screenVectorOperands` (`BinOp operand spelling`); a binary operation of constants only (`BinOp constant operands`), the set operators, `bool`, `without ()` and an operand that is itself a binary operation stay delegated by name. `TestPlanLowersVectorLiteralOperands` and `TestPlanDelegatesVectorLiteralOperands` (`internal/queryplan/lower`) hold either side, a number next to it, every operator class, `on`, `ignoring`, `group_left` and `group_right`, integer, decimal and exponent values; `FuzzBinOpLowering` draws the call in four spellings. `TestVectorOperandLoweringMatchesLoki` (`LVP_RUN_ORACLES=1`, 720 lowered rows) holds the operator, bool, matching and operand kinds to Loki's syntax tree (a VectorExpr, not a number) and the proxy's validation to Loki's on the numbers a `vector(n)` takes (Loki rejects a sign, a hexadecimal integer, `Inf` and `NaN`, as the proxy does); `BenchmarkBinaryVector*` (`internal/proxy`) and `BenchmarkBinOpVectorQueries*` measure the route and the plan on these rows; the live shadow counts these roots as `not_lowered` `BinOp` (`TestLoweringShadow_BinaryWithVectorOperandIsNotCompared`).
- **Label lists (CI).** A label list is read as Loki's grammar reads it: names separated by commas, with none before the first, between two commas or after the last, and two names need a comma between them; the empty list of `by`, `without` and `on` and white space around names stay accepted, a stream selector may end in one comma, and an empty or comma-ended `| drop` or `| keep` is a syntax error. `TestLabelListCommasMatchLoki` (`internal/logql`, 916 queries: a trailing, doubled or leading comma, a lone comma, an empty list, white space and line breaks, and two names with no comma, in every list of by, without, on, ignoring, group_left, group_right, json, logfmt, drop, keep and label_format) holds the status and the text to Loki's recorded answers (the Loki oracle `check` command; `LVP_RUN_ORACLES=1` records them again). `TestRejectedQueryNeverReachesVictoriaLogs` (`internal/proxy`) drives such queries through every route that takes a query and counts the requests that reach VictoriaLogs (none), and `TestCompatEdgeDoesNotServeAnAnswerCachedBeforeTheRuneFix` that an answer an older binary cached for one is not served. `TestCompat_LabelListTrailingCommaLikeLoki` (`test/e2e-compat`, group core) compares the status and the body with Loki on query_range and query.
- **Filters on detected_level (CI).** A label filter on `detected_level` that no parser, drop, keep or formatting stage precedes is written as the derivation of Loki's ingest-time value plus the filter (`translate.BuildDetectedLevelFilter`, `JoinDetectedLevelFilters`), and the plan builds the same filter as LogsQL nodes from the same tables (`internal/queryplan/lower/detected_level_ast.go`; `TestDetectedLevelNodesPrintTheBuildersText` and `FuzzDetectedLevelNodes` hold every node to the builders' text). `TestDetectedLevelGridShadow` (`internal/querycorpus`, about 25 s) holds it to the translator's text byte for byte under every profile on a generated grid of 6,362 queries (70,074 rows): every operator against the level spellings in any case, `unknown`, other words, empty values in both quotings, escapes, regexps that compile and one that does not, comparisons and `ip()`, spacing and parentheses, several filters with line, pattern, label, `service_name` and presence filters between them, the stages before and after, and the log queries under range and vector aggregations, groupings by `detected_level`, `level` and `service_name`, a ranking, an unwrap, a binary operation and `label_replace`. `FuzzDetectedLevelLowering` (`internal/queryplan/lower`, fuzz-smoke) does the same on any value and operator; `TestDetectedLevelLoweringAllocatesNoMoreThanTheTranslator` bounds the allocations cold (the plans' memo emptied) and warm; `TestPlanMatchesLokiOracle` holds the label, operator and value the plan reads to Loki's tree (`test/oracle/loki facts`, label filters added); and the screen of the flip must turn a filter-only log query with one away until the plan serves it (`TestFlipScreenAdmitsEveryNativeRow`, `TestQueryPlanLowering_LeavesDetectedLevelFiltersToTheTranslator`).

Served from the plan today: log queries with a stream selector, line filters, pattern filters and label filters and no parser stage (`LogSelect`). `-query-plan-lowering=false` is the rollback.

## Query Semantics Matrix

`TestQuerySemanticsMatrix` reads [`query-semantics-matrix.json`](../test/e2e-compat/query-semantics-matrix.json) and executes each case against:

- real Loki in the Compose stack
- Loki-VL-proxy backed by VictoriaLogs
- the required `compat-loki` CI workflow for pull-request enforcement

`TestQuerySemanticsOperationsInventory` reads [`query-semantics-operations.json`](../test/e2e-compat/query-semantics-operations.json) and enforces:

- every tracked Loki-facing operation references one or more live matrix cases
- every live matrix case is tracked by the operation inventory

Its `depth_paths` section is an index into those same fixtures, not another
query corpus. It tracks selector/parser/field-filter, range-function, and outer-
aggregation paths separately for JSON, logfmt, regexp, and pattern parsing.
The optional `related_tests` entries link existing specialized tests to those
same paths. Those tests retain distinct assertions (such as exact parser-error
text, series limits, or bounded VL reads) without copying their queries into
the depth-path inventory. The matrix is the canonical query catalog; the
operations inventory indexes its IDs and registry links.
`TestQuerySemanticsMatrixDepthPaths` checks the stage order, matrix case IDs,
parser registry items, related registry cases, and any retained evidence path;
`TestQuerySemanticsMatrixCoversParserMetricDepths` fails when any parser is
missing at one of the three depths. Every path runs through the live Loki-versus-
proxy matrix test. The retained parser-error capture is supplemental JSON
error/nesting evidence; it is not presented as a saved response for the other
parser paths. The deeper parser suite adds live cases for `rate` and
`count_over_time`, error-filter states, grouped and nested aggregations, and
both instant and range APIs; its retained evidence is described in the
[parser-matrix README](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/test/e2e-compat/testdata/parser-error-matrix/README.md).
The 117-case semantics manifest and 132-case parser capture cover different
contracts; their counts should not be summed as a full Cartesian compatibility
space.

The manifest also holds 50 existing latency scenarios. All 50 refer directly
to quality case IDs and use the same LogQL. `TestParityLatency` and the focused `BenchmarkParity_*`
helpers resolve queries from this catalog. This moves the former standalone
performance query list into one place without adding a performance gate to the
quality matrix. Visual proof stays in its separate workflow.
The PR Quality Report compares catalog counts between base and PR without
starting tests. Its live matrix row comes from the source-verified pinned Loki
CI job log and reports observed passes, failures, and skips separately; a
missing or stale job is not counted as a pass. When a test-only PR skips the
expensive quality snapshot, the trusted evidence publisher reads the manifest
and operations JSON at the exact base and head commits, verifies their Git blob
identities, and applies the same catalog counter. It does this only if the
existing `loki-pinned` job was selected; it never executes PR code or starts
another test or benchmark for the comment.

Each manifest entry declares:

- query family
- endpoint shape: `query` or `query_range`
- expected outcome: `success`, `client_error`, or `server_error`
- expected `resultType` for valid queries
- an explicit comparison contract; `metric_points` checks every series label set,
  sample timestamp and numeric value. `log_entries` compares each stream's labels,
  nanosecond timestamp, line and optional structured metadata as a multiset,
  retaining duplicate entries while ignoring response order

The two range-ranking latency cases (`topk` and `bottomk`) use `ranked_values`:
every step must return the same count and multiset of numeric samples. Loki's
map iteration can select different label identities among equal values at the
rank cutoff, so exact series-label comparison would be flaky. The dedicated
`TestCompat_TopkRangePerStep` also checks label identity where the cutoff has
no tie, and is linked to `semantics/topk-bottomk-range-series` in the registry.

All 45 log-result cases use `log_entries`, including all 24 log latency scenarios
promoted to quality cases without duplicating their query strings. The comparison
rejects malformed values, missing or extra entries, changed labels, timestamps,
lines or metadata. The manifest validator rejects the former line-count-only
mode, so later cases cannot silently weaken this contract. It does not claim
response ordering parity.

The 42 `metric_points` cases reuse the sliding-range tests' decoder and point
comparison. Label identities use canonical JSON; duplicate series or timestamps,
malformed samples and missing/extra points fail. Timestamp coordinates retain
their decimal precision. Values use relative tolerance `1e-6` and absolute
tolerance `1e-9`, with explicit NaN and signed-infinity handling. Series order
is outside this sample-map comparison, including the sort cases.
The strict top-k case selects the three existing fixture apps with distinct
counts (10, 6 and 5); unrelated fixture apps can tie at the cutoff and Loki
permits different series choices for a tie. Dedicated ranked-result tests
cover ties separately.

Both backends receive the same request parameters, anchored to the ingested
fixture rather than advancing wall clocks. Metric answers and expected HTTP
error bodies are retained in the existing `matrix_response` CI logs. Expected
error cases check status and available structured categories; they do not claim
complete error-message equivalence.

Three dynamic-parser unwrap cases remain `any_non_empty`: they check availability
under the default parser profile and name the existing accepted
[`parsed-identity/dynamic-key-parsers`](../conformance/registry/cases/parsed-identity/dynamic-key-parsers.yaml)
deviation. They are not counted as strict metric parity. The conformance gate
rejects missing, unknown or unaccepted deviation references and forbids applying
one to a strict comparison. This keeps the default behavior separate from the
`-exact-parser-series-identity` opt-in.

The shared sample comparator lives in `test/metricparity`; its 57 pure cases
run with ordinary `go test ./...`, including malformed/duplicate samples and
numeric/timestamp edge cases. Both the live semantic matrix and current-handler
replay use that same implementation.

`TestOrderedJSONRecordedLokiContracts` replays the existing nested-parser
capture through current registered handlers and the existing fixture-based VL
emulator without running services. It covers 56 complete metric answers,
44 pipeline-error diagnostic contracts outside the selected failing series,
and 32 syntax-error status/category contracts. Syntax-text differences remain
owned by the existing open gap. This extends the regular unit suite; it does
not replace live parity. The
[retained fixture documentation](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/test/e2e-compat/testdata/parser-error-matrix/README.md)
lists the source identities, comparison limits and commands.

This is where the repo makes tricky edge cases explicit instead of relying on scattered ad hoc tests. Representative cases include:

- valid parser pipelines like `| json`, `| logfmt`, `| regexp`, and `| pattern`
- valid parser regex and numeric filters
- grouped metric queries such as `sum by(level)(count_over_time(...))`
- parser-inside-range metric queries such as `count_over_time({selector} | json | status >= 500 [5m])`
- byte-oriented parser metrics such as `bytes_over_time(...)` and `bytes_rate(...)`
- bare `unwrap` metrics such as `sum/avg/max/min/first/last/stddev/stdvar/quantile_over_time(... | unwrap field [5m])`
- `absent_over_time(...)` semantics for missing selectors
- instant post-aggregations such as `topk`, `bottomk`, `sort`, and `sort_desc`
- scalar and vector binary operations over valid metric expressions, including scalar `bool` comparison and vector `or` / `unless`
- invalid forms like `sum by(job) ({selector})`
- invalid forms like `topk(2, {selector})` and `sort({selector})`
- invalid forms like `rate({selector})` without a range

Additional required parity edge cases for parser-stage metric compatibility:

- `rate_counter(... | unwrap <field> [window])` must keep reset-aware behavior and match Loki outcome class while preserving parser-derived labels
- parser-stage range metrics must preserve exact metric label-set parity (not only result counts) against Loki
- unwrap-required range functions without `unwrap` must fail with Loki-style invalid-aggregation errors
- parser-probe compatibility fallback must be deterministic (single working parser path), so repeated runs do not switch between incompatible query plans

Proxy-only Grafana helper behavior such as synthetic labels, Drilldown fields, stale-on-error helpers, and detected-label recovery stays outside this matrix and belongs in the dedicated proxy contract tests.

Valid Loki behavior is not an accepted exclusion class. If a query works in real Loki and diverges in the proxy, it should be fixed and added to the required matrix or tracked immediately as a parity bug with a dedicated regression target.

Detailed docs are part of the gate now. When the manifest grows into a new LogQL family, update:

- `docs/compatibility-loki.md`
- `docs/compatibility-matrix.md`
- `docs/testing.md`
- `README.md`

### `datasource` shard

| Test | Purpose |
|---|---|
| `datasource health check succeeds` | Grafana can Save & Test the proxy datasource |

Moved out of Playwright:
`test/e2e-compat/grafana_surface_test.go` now covers datasource catalog, direct datasource health, `/ready`, `/buildinfo`, `/rules`, `/alerts`, and direct Loki Drilldown bootstrap.

### `explore-core` shard

| Test | Purpose |
|---|---|
| `basic log query returns results without errors` | baseline Explore log query |
| `sum by (level) rate returns the same series set on both datasources` | API-level proxy-vs-Loki metric parity through Grafana's datasource proxy (`explore-parity.spec.ts`) |
| `exact windows and formatted rows stay visible: <datasource>` | one test per datasource (proxy, interact proxy, Loki) in `security-hardening-visibility.spec.ts` |
| `buildExploreUrl encodes the datasource pane state` | pure URL/state coverage (`url-state.spec.ts`) |

Moved out of Playwright:
`internal/proxy/proxy_test.go`, `internal/proxy/gaps_test.go`, and `test/e2e-compat/chaining_test.go` cover query translation, response shape, parser pipelines, line filters, direction handling, and metric-query parity faster than the browser can.
`test/e2e-compat/explore_contract_test.go` now adds the browser-removed HTTP contracts for line filters, `json`, `logfmt`, `direction=forward`, metric matrices, `label_format`, and invalid-query `4xx` handling.

### `explore-tail` shard

| Test | Purpose |
|---|---|
| `multi-tenant query respects __tenant_id__ filter in Explore` | tenant narrowing in Explore |
| `multi-tenant negative regex excludes fake tenant in Explore` | tenant negative-regex narrowing stays browser-visible |
| `live tail works through the browser-allowed synthetic datasource` | browser-safe synthetic live tail |
| `native-tail datasource can hand off to ingress live tail` | failure recovery after native-tail path breaks |

Moved out of Playwright:
`test/e2e-compat/features_test.go` and `internal/proxy/*tail*test.go` cover tenant-header fanout, websocket protocol behavior, fallback selection, origin policy, and native-tail failure semantics without Chromium.

### `drilldown-core` shard

| Test | Purpose |
|---|---|
| `clicking a log row expands details without error` | log row expansion path |
| `label filter drill-down for app label` | label filter action from Explore logs |
| `buildLogsDrilldownUrl` and `buildServiceDrilldownUrl` state tests | pure URL/state coverage without launching Chromium |
| `proxy shows service buckets on landing page` | Logs Drilldown landing volumes |
| `service drilldown field filter survives reload from URL state` | Drilldown URL state persists across reloads |
| `patterns are visible in drilldown for autodetected datasource` | Patterns tab with the patterns-autodetect proxy |
| `proxy drilldown-limits payload is a superset of Loki's` and the `Patterns tab is hidden/shown ...` tests | Patterns tab gate driven by `drilldown-limits` (`drilldown-limits-gate.spec.ts`) |
| `labels visible — <range> range` | Drilldown fields labels sidebar per time range (`drilldown-cache-regression.spec.ts`) |

Moved out of Playwright:
`test/e2e-compat/drilldown_compat_test.go` now owns detected-fields contracts, dotted metadata exposure, filtered labels/fields resource behavior, parsed-field freshness, unknown field/label empty-success behavior, Grafana datasource resource parity, and multi-tenant Drilldown resource behavior including regex and no-match tenant filters.

### `drilldown-multitenant` shard

| Test | Purpose |
|---|---|
| `multi-tenant landing shows service buckets without browser errors` | multi-tenant landing-page browser smoke |
| `multi-tenant service drilldown loads without browser errors` | multi-tenant service logs browser smoke |
| `multi-tenant service field view loads detected fields without browser errors` | multi-tenant service fields browser smoke |
| `multi-tenant service filter survives reload from URL state` | multi-tenant URL state keeps `__tenant_id__` filter after reload |
| `multi-tenant service fields tab shows non-zero cardinality badges` | fields tab cardinality badges |
| `multi-tenant drilldown label filter scopes logs to selected tenant` | tenant label filter narrows logs |
| `multi-tenant drilldown with missing tenant shows empty result not error` | unknown tenant renders an empty result |

## Compatibility Tracks

The repo now keeps four separate compatibility tracks/contracts:

| Track | Local score test | Matrix coverage |
|---|---|---|
| Loki | `TestLokiTrackScore` | Loki `3.6.x` and `3.7.x` |
| Logs Drilldown | `TestDrilldownTrackScore` | Logs Drilldown `1.0.x` and `2.x` families |
| Grafana Loki datasource | `TestGrafanaDatasourceCatalogAndHealth` | Grafana runtime `13.x` (current) and `12.x` (previous) families |
| VictoriaLogs | `TestVLTrackScore` | VictoriaLogs `v1.49.0` (the latest `v1.4x` release) and every `v1.5x` minor at its latest patch |

The default local stack (`test/e2e-compat/docker-compose.yml`) is pinned to:

- Loki `3.7.9`
- VictoriaLogs `v1.53.0`
- vmalert `v1.138.0` and vmauth `v1.138.0`
- VictoriaMetrics `v1.119.0` (remote-write target for vmalert recording rules and scrape store for the stack)
- Grafana `13.3.0` with `GF_PLUGINS_PREINSTALL=victoriametrics-logs-datasource@0.33.1,grafana-lokiexplore-app@3.1.0` (overridable through `GRAFANA_PLUGINS_PREINSTALL`)
- Logs Drilldown contract `3.1.0` from `grafana/logs-drilldown` commit `3b8466c7e48ea4e9128e10015bf8fd5cc35d49cb`

`TestPinnedCompatibilityMatrixMatchesCompose` fails when the Loki, VictoriaLogs or Grafana image defaults in compose drift from the pinned versions in `compatibility-matrix.json`, so bump both together. The same test checks that compose preinstalls `grafana-lokiexplore-app@<stack.logs_drilldown_contract.pinned_version>` and that a runtime profile's `drilldown_plugin_version` is a tracked Drilldown version. The VictoriaLogs datasource pin is not checked; keep it aligned by hand.

Field-surface defaults in the pinned stack:

- `-label-style=underscores` is the binary default, so labels stay Loki-compatible
- `-metadata-field-mode=translated` is the binary default: field APIs expose Loki-compatible translated names only; the main proxy (`loki-vl-proxy`, port 13100) runs this mode
- `-metadata-field-mode=translated` (the Loki-compatible profile, with `-label-style=underscores`) exposes Loki's sanitized names only and answers a dotted name in LogQL with Loki's parse error; `loki-vl-proxy` (13100), `loki-vl-proxy-underscore` (13102, behind the Grafana `Loki (via VL proxy)` datasources) and `loki-vl-proxy-patterns-autodetect` (13110, the Logs Drilldown default datasource) run it
- `-metadata-field-mode=hybrid` exposes both native dotted names and translated aliases and accepts dotted names in queries; `loki-vl-proxy-otel-hybrid` (13111, Grafana `Loki (via VL proxy OTel hybrid)`) runs it
- Dedicated `loki-vl-proxy-native-metadata` (`native`), `loki-vl-proxy-translated-metadata` (`translated`) and `loki-vl-proxy-no-metadata` (`-emit-structured-metadata=false`) variants cover the other structured-metadata exposure modes; `TestCompat_OptionMatrixAgainstStack` walks every option combination in-process against the stack

Alerting and recording-rule parity coverage:

- `TestAlertingCompat_PrometheusRulesAndAlerts` validates alerting + recording rules from `vmalert` are visible through proxy Prometheus paths
- `TestAlertingCompat_GrafanaDatasourceRulesAndAlertsParity` validates the same rule/alert payload through Grafana datasource proxy endpoints
- `TestAlertingCompat_LegacyLokiRulesYAML` validates Loki legacy YAML formatting includes both `alert` and `record` entries

Support window policy:

- Loki: current minor family plus one minor behind
- Grafana runtime: pinned current family gets the fuller Drilldown runtime contract, and pull requests also run smaller current-family and previous-family smoke profiles; the full runtime matrix stays on scheduled/manual coverage
- Logs Drilldown: current family plus one family behind
- VictoriaLogs: `v1.49.0` (the latest `v1.4x` release) plus every `v1.5x` release, each supported minor tested at its latest patch with a published image; `v1.48.x` and older are unsupported (see [VictoriaLogs compatibility](compatibility-victorialogs.md#support-policy))

Grafana runtime profiles from the manifest:

- `13.3.0` (`full`) runs `TestDrilldownTrackScore` and `TestDrilldown_RuntimeFamilyContracts` on scheduled and manual compatibility checks
- `13.3.0` (`current_smoke`) runs `TestGrafanaDatasourceCatalogAndHealth`, `TestDrilldown_GrafanaResourceContracts` and `TestDrilldown_RuntimeFamilyContracts` on pull requests
- `12.4.14` (`previous_smoke`) runs the same smoke tests on pull requests against the previous family, with Logs Drilldown `2.6.0` (`drilldown_plugin_version`) because Drilldown `3.x` requires Grafana `>= 13.1`

Grafana `13.3.0` Explore leaves the auto-run from the URL on `Cancel`; the e2e-ui `runQuery` helper clears it and still requires the explicit run to send and answer a query (see [Grafana datasource compatibility](compatibility-grafana-datasource.md#grafana-1330-explore-auto-run-stays-on-cancel)).

The Grafana Loki datasource contract tracks `13.3.0`, `13.2.5`, `13.2.3`, `13.2.1`, `13.0.1`, `12.4.14`, `12.4.12`, `12.4.10`, `12.4.2` and `12.4.1`.

Logs Drilldown family assertions are explicit in the contract matrix:

- `1.0.x` checks service-selection volume buckets, detected-fields filtering, and labels-field parsing behavior
- `2.x` and `3.x` check detected-level default columns, field-values breakdown scenes, and additional label-tab wiring
- `3.1.0` and later also check that the logs footer reads the `Summary: total bytes processed` frame stat
- the `drilldown` e2e-compat group also checks the query statistics against Loki (`TestDrilldown_QueryStatsLikeLoki`): the `data.stats` shape of log, metric, instant and `volume_range` answers over 5m and 1h equals Loki's; in the default `-query-stats-mode=loki` the splits and shards equal Loki's and lines and bytes processed are within 10% and 25% of Loki's (the tolerances of an estimate against Loki's own numbers); each refresh reports its own `execTime`, a log or instant refresh answered from the proxy's cache replays the computation, and a metric repeat reports a results-cache hit with Loki's splits; Grafana lists the same "Summary:" and "Ingester:" frame stats for the proxy as for Loki (`TestDrilldown_QueryStatsEnvelope`). The emulation rules are also pinned by unit tests against Loki answers recorded from the stack (`internal/proxy/testdata/loki_query_stats_splits.json`, `TestLokiStatsEmulation_MatchesRecordedLokiAnswers`)

The stack is version-parameterized through compose environment variables:

```bash
LOKI_IMAGE=grafana/loki:3.6.10 \
VICTORIALOGS_IMAGE=victoriametrics/victoria-logs:v1.49.0 \
GRAFANA_IMAGE=grafana/grafana:12.4.14 \
GRAFANA_PLUGINS_PREINSTALL=victoriametrics-logs-datasource@0.33.1,grafana-lokiexplore-app@2.6.0 \
docker compose -f test/e2e-compat/docker-compose.yml up -d --build
```

GitHub Actions uses the same manifest as the source of truth. The compatibility workflows load their version matrices from `test/e2e-compat/compatibility-matrix.json` instead of duplicating version lists in workflow YAML.

Product pull requests get the `pr-quality-report.yaml` measurements. The sticky
PR Quality Report puts actionable changes first: selected workload improvements
and regressions, new Loki answer differences, new or worsened above-2× Loki rows,
visual failures, failing checks and unavailable comparisons. Detailed tables sit
inside collapsed sections, with links to the original workflow runs.

`pr-evidence-summary.yaml` refreshes that comment when existing CI, PR Quality
Report, Perf A/B, Visual smoke, pinned compatibility or Security PR workflows complete. It reads their artifacts
and job results; it runs no tests, benchmarks, profiling or compose stacks.
Rendering and publication use trusted default-branch code. Artifact contents are
bounded data, never extracted or executed. Current PR head, tested merge, base
commit and workflow attempt identify each source. Missing, expired or stale
artifacts contribute no measurements; the publisher rechecks revisions before
updating the comment.

Synthetic handler microbenchmarks reuse two existing sources. The quality
snapshots pair QueryRange and Labels cache-hit/bypass samples on one runner;
the summary counts comparable cases and regressed/improved/stable metric axes
using the existing percentage, absolute and low-baseline guards. Significant
examples appear above the collapsed details; the quality gate is unchanged.
The CI `benchmarks` artifact adds observed handler cases for series, detected
fields/values, multi-tenant requests, label cardinality and patterns. Those
samples have no paired base in that artifact and never receive regression or
speedup deltas. Their medians, repeat counts and CPU settings remain separate
from the paired quality run. The producing job's checkout must match the PR
head or tested merge; missing or stale identity contributes no measurements.
Both sources measure synthetic elapsed time and allocations, not whole-process
CPU, RSS, physical I/O or production request latency. The report starts no
additional benchmark runs. Raw samples remain in the linked existing CI run.

The collapsed sections include test/subtest outcomes and coverage, selected
smoke contracts, real-query base/PR/Loki latency and answer comparisons, visual
proofs, and candidate-only RF1/RF2 fleet evidence. Fleet process CPU, sampled RSS,
container statistics and logical cache occupancy retain their original scope;
they are not a main-versus-PR resource comparison. Physical SSD throughput/IOPS,
unmeasured evictions and unavailable layer statistics remain explicitly
unmeasured. Synthetic handler `ns/op` is elapsed time; `B/op` and `allocs/op`
describe allocation traffic, not retained RAM or process CPU.

API quality uses the declared Loki read API inventory, with base/PR counts for
implemented read endpoints, linked Loki differential tests, declared proven read endpoints,
behavior/translation states, known gap states and error-layer exceptions.
Write-only endpoints do not enter the read endpoint denominator. Optional
deletion management is listed separately, including its GET administration
method; POST query transports remain reads. Snapshots with different endpoint
scopes have no endpoint count delta.
These are checked-in contract declarations, not a runtime compatibility
percentage. Registry surface versions remain visible separately from runtime
stack versions. The small smoke scores contribute candidate-only component
detail: both collector invocations can share the candidate Docker image, so
their scores are not a base-versus-PR runtime comparison. Actual revision
runtime comparisons come from the existing A/B job, which builds separate
base and PR binaries and queries Loki.

The existing quality test runs retain package-qualified full test/subtest names
and outcomes, plus statement-weighted package coverage. Comparisons identify
newly failing/skipped, removed, recovered and new passing cases; counts of
top-level tests and child cases stay separate. Failed runs retain the observed
outcomes and exit code and fail the quality gate. Their incomplete populations
cannot imply removed tests or absence-based improvements, and partial coverage
is not compared as a complete profile. Cached base snapshots retain these fields;
older snapshots without them have no exact-case comparison.

Existing completed API, semantic, E2E and Grafana job logs supply broader
candidate execution scopes. Their checkout SHA must match the current tested
head/merge revision. The report shows observed pass/fail/skip counts and logged
stack versions; Playwright invocations, flakes, interruptions and tests not run
remain explicit. Expired, unrecognized or stale logs are unavailable. Profiles
are not pooled into one compatibility score, and candidate-only scope counts
are not presented as main-versus-PR regressions. Reading these results starts no
additional tests or measurement workloads.

An unavailable base test/coverage snapshot is not zero and produces no delta.
Passing 11/11 smoke contracts does not mean full Loki compatibility or that the
entire configured version matrix ran. Internal-package test outcomes come from
the existing `unit-results` artifact (retained for one day); overall coverage
comes from the existing whole-suite coverage profile.

The existing measurement job collects test count, named outcomes and coverage from the same Go test pass, uses a shallow checkout plus explicit base-SHA fetch, and reports medians of 7 benchmark samples (`-count=7 -benchtime=2s -cpu=1` with `GOMAXPROCS=1`) and 3 high-concurrency load-test runs (`scripts/ci/collect_quality_metrics.sh`). The base commit's outcomes and coverage are cached per base SHA and collector/parser version (`actions/cache`, read back through `QUALITY_TESTS_SNAPSHOT`): a later run against the same base reuses them. Synthetic elapsed timings, load-test throughput and smoke scores are measured on the same runner; they do not establish whole-stack CPU or RAM savings.
The collector runs test/coverage, compatibility scores, benchmark medians, and load metrics in parallel with bounded fallbacks so a single slow signal does not block the whole report gate.
The PR quality workflow now skips benchmark/load perf smoke when no perf-sensitive files changed, so docs/metadata-only PRs do not report noisy runner jitter as fake regressions.
The quality gate compares base/head with relative and absolute regression thresholds and ignores low-baseline noise, so tiny shared-runner jitter does not fail required checks.
The high-concurrency load threshold remains strict locally (`>10k req/s`) and uses a CI floor (`>5k req/s`) on shared race-enabled runners to avoid flaky non-regression failures.
Selected Loki smoke contracts must all pass on PR quality and the dedicated pinned Loki compatibility workflow; these are not a percentage of the full Loki API surface.
Release automation also materializes `CHANGELOG.md` `Unreleased` into the new version section and uses that same section as the GitHub release notes body, then syncs README tests/coverage/Go LOC badges on `main`.
For reliable metadata PR auto-merge under branch protection, set repository secret `RELEASE_PR_TOKEN` (PAT/App token with repo scope) so release-created metadata PRs trigger required `pull_request` checks.

Required-check note:

- The repo exposes the grouped compat jobs directly: `e2e-compat (core)`, `e2e-compat (drilldown)`, `e2e-compat (otel-edge)`, `e2e-compat (tail-multitenancy)`, and `e2e-compat (semantics)`.
- The umbrella `e2e-compat` job depends on all five groups and fails when any group fails, so one required check covers every group.

That report is part of the required PR gate. It is still a smoke signal rather than a full benchmark lab run, but it now blocks obvious regressions in coverage, compatibility, and the tracked performance signals.

See [compatibility-matrix.md](compatibility-matrix.md), [compatibility-loki.md](compatibility-loki.md), [compatibility-drilldown.md](compatibility-drilldown.md), [compatibility-victorialogs.md](compatibility-victorialogs.md), [compatibility-matrix.json](../test/e2e-compat/compatibility-matrix.json), and [query-semantics-matrix.json](../test/e2e-compat/query-semantics-matrix.json).

## Running Specific Tests

```bash
# Run tests matching a pattern
go test ./internal/proxy/ -run "TestCritical" -v

# Run with race detector
go test ./internal/proxy/ -run "TestCritical_TenantMap" -race

# Fuzz testing
go test ./internal/translator/ -fuzz FuzzTranslateLogQL -fuzztime=60s

# Benchmarks
go test ./internal/translator/ -bench . -benchmem
go test ./internal/cache/ -bench . -benchmem
```
