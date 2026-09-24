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
  gcr.io/openssf/scorecard:stable \
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
| `ci.yaml` | `test` | observability asset sync check, CI script unit tests, `go build`, `go vet`, `govulncheck` v1.8.0, `go test ./... -race` with coverage, `internal/cache` coverage guard (79%) |
| `ci.yaml` | `lint` | `gofmt -s` check and `golangci-lint` v2.13.2 |
| `ci.yaml` | `tuple-contract` | `go test ./internal/proxy -run '^TestTupleContract_'` |
| `ci.yaml` | `tuple-smoke` | compose stack, `TestSetup_IngestLogs`, then `scripts/smoke-test.sh` |
| `ci.yaml` | `fuzz-smoke` | short (12-20s) fuzz runs for `internal/proxy`, `internal/translator`, `internal/cache` and `internal/rulesmigrate` targets |
| `ci.yaml` | `race-stress` | `-race -count=1` over `internal/proxy`, `internal/cache`, `internal/middleware`, `internal/observability`, plus targeted concurrency regressions |
| `ci.yaml` | `stress-tests` | `-tags=stress -race` translation, coalescer and range-metric tests in `internal/proxy` |
| `ci.yaml` | `memory-leak-tests` | `-tags=memleak -race -run 'TestMemLeak_'` in `internal/proxy` |
| `ci.yaml` | `bench` | `internal/proxy` benchmarks (`-count=3`), label/field, patterns, cache and stats-translation scale benchmarks with regression thresholds, `TestLoad*` load tests |
| `ci.yaml` | `docker` | image build and binary smoke run |
| `ci.yaml` | `helm` | `helm lint`, template regressions, `scripts/ci/validate_helm_flags.sh` |
| `ci.yaml` | `e2e-compat (core, drilldown, otel-edge, tail-multitenancy, semantics)` | one fresh compose stack per group, `go test -tags=e2e -run '<group pattern>' ./test/e2e-compat/` |
| `ci.yaml` | `e2e-compat` | aggregates the five grouped jobs into one result |
| `ci.yaml` | `e2e-fleet` | `test/e2e-fleet` 3-proxy stack, `TestFleetSmoke_QueryRangeWarmHitIncrementsCacheMetrics` only |
| `ci.yaml` | `e2e-ui (9 shards)` | compose stack with `--profile ui`, one Playwright shard per job (see [CI Shards](#ci-shards)) |
| `compat-loki.yaml` | `loki-pinned` | Loki 3.7.7, VictoriaLogs v1.52.0, Grafana 13.2.1: `TestLokiTrackScore` (must be 100%), `TestQuerySemantics*`, `TestLabelCache_*` |
| `compat-loki.yaml` | `loki-matrix` | weekly: the same checks for every `stack.loki.matrix_versions` entry |
| `compat-drilldown.yaml` | `drilldown-pinned-runtime` | Loki 3.7.7, VictoriaLogs v1.52.0, Grafana 13.2.1: `TestDrilldownTrackScore` |
| `compat-drilldown.yaml` | `drilldown-grafana-pr-matrix` | runtime profiles with `run_on_pr` (Grafana 13.2.1 `current_smoke`, 12.4.10 `previous_smoke`) |
| `compat-drilldown.yaml` | `drilldown-contract-matrix`, `drilldown-grafana-runtime-matrix` | weekly/manual: Drilldown app contract checks per version and every Grafana runtime profile |
| `compat-vl.yaml` | `vl-pinned` | Loki 3.7.7, VictoriaLogs v1.52.0, Grafana 13.2.1: `TestVLTrackScore` |
| `compat-vl.yaml` | `vl-matrix` | weekly/manual: `TestVLTrackScore` for every `stack.victorialogs.matrix_versions` entry |
| `security-pr.yaml` | `Security / static` | Gitleaks v8.28.0, gosec v2.29.0, Trivy 0.71.0 filesystem scan, actionlint 1.7.7, hadolint v2.12.0, OpenSSF Scorecard guardrails |
| `security-pr.yaml` | `Security / runtime` | compose stack, `scripts/ci/run_security_regressions.sh`, ZAP baseline |
| `security-heavy.yaml` | `Heavy / image and sbom`, `Heavy / semgrep`, `Heavy / fuzz`, `Heavy / dast` | scheduled/manual only: Trivy image scan and SBOM, Semgrep 1.161.0, 2-minute fuzzing, ZAP active scan and curated nuclei |
| `codeql.yaml` | `analyze` | CodeQL Go analysis (also weekly) |
| `e2e-pipeline.yml` | `e2e-pipeline` | `test/e2e-pipeline` stack, `go test -tags=e2e ./test/e2e-pipeline/` |
| `vl-ast-coverage.yml` | `check-coverage` | weekly, manual, and on PRs touching `internal/logsql/**` or its manifest: `scripts/check-vl-ast-coverage.py` |
| `changelog-pr.yaml` | `changelog` | pull requests only: `scripts/ci/check_changelog_pr.py` changelog gate |
| `pr-quality-report.yaml` | `report` | pull requests only: test count, coverage, compatibility, benchmark and load deltas against the base branch |

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

- `internal/translator/labels_translate_test.go` now verifies repeated same-field include/exclude interactions keep the latest equality/regex action while preserving valid range pairs on the same field.
- `internal/proxy/request_logger_semconv_test.go` verifies request logs use end-user semantic fields (`enduser.*`) without falling back to legacy `user.*`.
- `internal/observability/logger_test.go` verifies resource identity fields are not duplicated into per-line JSON payloads (prevents downstream `message.service.*` / `message.telemetry.sdk.*` field explosion).
- `internal/metrics/procenv_test.go` + related `otlp_test.go` / `system_test.go` locking guards keep `-race` CI deterministic when tests manipulate proc-path globals alongside OTLP pusher goroutines.

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

| Shard | Command | Primary focus |
|---|---|---|
| `datasource` | `npx playwright test tests/datasource.spec.ts` | Grafana datasource settings smoke |
| `explore-core` | `npx playwright test --grep @explore-core` | default Explore browser smoke plus API-level proxy-vs-Loki metric parity (`explore-parity.spec.ts`) |
| `explore-tail` | `npx playwright test --grep @explore-tail` | browser-only multi-tenant (`__tenant_id__` exact and negative regex) plus live-tail recovery |
| `drilldown-core` | `npx playwright test --grep @drilldown-core` | Explore detail-panel smoke and single-tenant Logs Drilldown smoke |
| `drilldown-multitenant` | `npx playwright test --grep @drilldown-mt` | multi-tenant Logs Drilldown landing/service/fields plus URL filter-reload persistence |
| `explore-ops` | `npx playwright test --grep @explore-ops` | Loki operations parity: parsers (json, logfmt), formatting (line_format, label_format, keep/drop), metric queries (count_over_time, rate, unwrap), line filters (regex, negative), aggregations (topk) |
| `explore-mt` | `npx playwright test --grep @explore-mt` | multi-tenant Explore coverage |
| `explore-regression` | `npx playwright test --grep @regression` | API-level parity register (`explore-regression.spec.ts`): log selectors, line filters, parsers and pipelines compared line-for-line on an uncapped window; grouped metric queries compared by series set after Loki's range path has warmed; the `-max-stats-query-series` cap; content checks |
| `explore-comprehensive` | `npx playwright test --grep @comprehensive-ui` | Explore UI coverage (`explore-comprehensive-ui.spec.ts`): page load, editor, query execution, results panel, empty results, filters; timings recorded as annotations |

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
| `e2e-compat (drilldown)` | `TestDrilldown_*` contracts including runtime-family checks, Drilldown cluster/level filter features, Loki/Drilldown/VL track scores |
| `e2e-compat (otel-edge)` | OTel label translation, structured metadata, underscore-proxy surfaces, label dedup/translation, `TestEdge_*`, `TestComplex_*` |
| `e2e-compat (tail-multitenancy)` | multi-tenant behavior, tail transport semantics, admin/analytics endpoints, security headers, metrics, gzip, response-shape and edge checks |
| `e2e-compat (semantics)` | query semantics matrix and operations inventory, `TestLogQL_Exhaustive_*`, `TestPipeline_*`, range metric compatibility, Grafana clickout parity, `TestMissingOps_*` |

The group patterns are anchored regexes in `.github/workflows/ci.yaml`. Tests outside every pattern do not run in these jobs: `TestHardeningLive_*` runs in `Security / runtime` (through `scripts/ci/run_security_regressions.sh`), `TestLabelCache_*` runs in `loki-pinned`, while `TestOperationsMatrix_*`, `TestPerf_*`, `TestParityLatency`, `TestPatternsDenseRepro_*`, `TestDense*`, `TestE2ELock_*`, `TestProxy_DrilldownLimits_*`, `TestRangeMetric_UnwrapResponseSizeGuard`, `TestFeature_IndexStats_ReturnsRealData`, `TestFeature_IndexVolume_ReturnsPrometheusFormat`, `TestFeature_IndexVolumeRange_ReturnsMatrix`, `TestFeature_ZstdCompression` and `TestFeature_GzipAndZstdBodiesMatch` are not matched by any CI pattern and run only when invoked locally.

Stack startup now uses [`wait_e2e_stack.sh`](../scripts/ci/wait_e2e_stack.sh) instead of `docker compose --wait` or fixed sleeps. That avoids false failures from services without Docker healthchecks and lets UI and compat jobs share the same readiness logic.

The GitHub-hosted Docker jobs now also prebuild the proxy image once per job through BuildKit cache and start compose stacks with `--no-build`. That keeps the grouped compat shards and UI shards parallel without paying the full Docker rebuild cost every time a stack starts inside the same job.

Compose-backed fleet cache smoke runs on pull requests and post-merge `main` in CI (`e2e-fleet`). The job runs only `TestFleetSmoke_QueryRangeWarmHitIncrementsCacheMetrics` against `test/e2e-fleet/docker-compose.yml`; the other `TestFleet_*`, `TestFleetSmoke_*` and `TestPeerDiscovery_*` tests in `test/e2e-fleet` are run locally.
Tuple smoke contract canary also runs automatically in CI (`tuple-smoke`) by seeding e2e data then executing `scripts/smoke-test.sh`.

## Query Semantics Matrix

`TestQuerySemanticsMatrix` reads [`query-semantics-matrix.json`](../test/e2e-compat/query-semantics-matrix.json) and executes each case against:

- real Loki in the Compose stack
- Loki-VL-proxy backed by VictoriaLogs
- the required `compat-loki` CI workflow for pull-request enforcement

`TestQuerySemanticsOperationsInventory` reads [`query-semantics-operations.json`](../test/e2e-compat/query-semantics-operations.json) and enforces:

- every tracked Loki-facing operation references one or more live matrix cases
- every live matrix case is tracked by the operation inventory

Each manifest entry declares:

- query family
- endpoint shape: `query` or `query_range`
- expected outcome: `success`, `client_error`, or `server_error`
- expected `resultType` for valid queries
- exact comparison mode such as line-count parity, series-count parity, or exact metric-label-set parity

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
| VictoriaLogs | `TestVLTrackScore` | VictoriaLogs `v1.3x.x` through `v1.5x.x` transition band |

The default local stack (`test/e2e-compat/docker-compose.yml`) is pinned to:

- Loki `3.7.7`
- VictoriaLogs `v1.52.0`
- vmalert `v1.138.0` and vmauth `v1.138.0`
- VictoriaMetrics `v1.119.0` (remote-write target for vmalert recording rules and scrape store for the stack)
- Grafana `13.2.1` with `GF_PLUGINS_PREINSTALL=victoriametrics-logs-datasource@0.32.0,grafana-lokiexplore-app@2.5.2`
- Logs Drilldown contract `2.5.2` from `grafana/logs-drilldown` commit `7587eede6a3b0b26feb74281997d1f780cfd58c2`

`TestPinnedCompatibilityMatrixMatchesCompose` fails when the Loki, VictoriaLogs or Grafana image defaults in compose drift from the pinned versions in `compatibility-matrix.json`, so bump both together. The Grafana plugin pins are not checked by that test; keep `GF_PLUGINS_PREINSTALL` and `stack.logs_drilldown_contract.pinned_version` aligned by hand.

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
- VictoriaLogs: `v1.3x.x` through `v1.5x.x` (transition band)

Grafana runtime profiles from the manifest:

- `13.2.1` (`full`) runs `TestDrilldownTrackScore` and `TestDrilldown_RuntimeFamilyContracts` on scheduled and manual compatibility checks
- `13.2.1` (`current_smoke`) runs `TestGrafanaDatasourceCatalogAndHealth`, `TestDrilldown_GrafanaResourceContracts` and `TestDrilldown_RuntimeFamilyContracts` on pull requests
- `12.4.10` (`previous_smoke`) runs the same smoke tests on pull requests against the previous family

The Grafana Loki datasource contract tracks `13.2.1`, `13.0.1`, `12.4.10`, `12.4.2` and `12.4.1`.

Logs Drilldown family assertions are explicit in the contract matrix:

- `1.0.x` checks service-selection volume buckets, detected-fields filtering, and labels-field parsing behavior
- `2.x` checks detected-level default columns, field-values breakdown scenes, and additional label-tab wiring

The stack is version-parameterized through compose environment variables:

```bash
LOKI_IMAGE=grafana/loki:3.6.10 \
VICTORIALOGS_IMAGE=victoriametrics/victoria-logs:v1.48.0 \
GRAFANA_IMAGE=grafana/grafana:12.4.2 \
docker compose -f test/e2e-compat/docker-compose.yml up -d --build
```

GitHub Actions uses the same manifest as the source of truth. The compatibility workflows load their version matrices from `test/e2e-compat/compatibility-matrix.json` instead of duplicating version lists in workflow YAML.

Pull requests also get a dedicated `pr-quality-report.yaml` workflow. It compares the PR branch against the base branch and posts a sticky PR comment with:

- total test count delta
- coverage delta
- Loki / Logs Drilldown / VictoriaLogs compatibility deltas
- sampled benchmark and load-test deltas

The report job now collects test count and coverage from the same Go test pass, uses a shallow checkout plus explicit base-SHA fetch, and reports medians of 7 benchmark samples (`-count=7 -benchtime=2s -cpu=1` with `GOMAXPROCS=1`) and 3 high-concurrency load-test runs (`scripts/ci/collect_quality_metrics.sh`).
The collector runs test/coverage, compatibility scores, benchmark medians, and load metrics in parallel with bounded fallbacks so a single slow signal does not block the whole report gate.
The PR quality workflow now skips benchmark/load perf smoke when no perf-sensitive files changed, so docs/metadata-only PRs do not report noisy runner jitter as fake regressions.
The quality gate compares base/head with relative and absolute regression thresholds and ignores low-baseline noise, so tiny shared-runner jitter does not fail required checks.
The high-concurrency load threshold remains strict locally (`>10k req/s`) and uses a CI floor (`>5k req/s`) on shared race-enabled runners to avoid flaky non-regression failures.
Loki compatibility is additionally enforced as a hard floor at `100%` on PR quality and on the dedicated Loki compatibility workflow.
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