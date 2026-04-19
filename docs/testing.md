# Testing

## Quick Start

```bash
# Unit tests
go test ./...

# Dedicated tuple-shape contract gate used in CI
go test ./internal/proxy -run '^TestTupleContract_' -count=1

# With race detector
go test -race ./...

# E2E compatibility tests (requires Docker Compose)
cd test/e2e-compat
docker compose up -d --build
../../scripts/ci/wait_e2e_stack.sh 180
go test -v -tags=e2e -timeout=180s ./test/e2e-compat/

# Track-specific scores
go test -v -tags=e2e -run '^TestLokiTrackScore$' ./test/e2e-compat/
go test -v -tags=e2e -run '^TestDrilldownTrackScore$' ./test/e2e-compat/
go test -v -tags=e2e -run '^TestVLTrackScore$' ./test/e2e-compat/

# Playwright UI tests (browser-only Grafana smoke flows)
cd test/e2e-ui
npm ci && npx playwright install chromium
npm test

# Generate local UI screenshots (Explore/Drilldown)
npm run capture:screenshots

# Run the same shards used in CI
npx playwright test tests/datasource.spec.ts
npx playwright test --grep @explore-core
npx playwright test --grep @explore-tail
npx playwright test --grep @drilldown-core
npx playwright test --grep @drilldown-mt

# macOS fallback: run the same UI tests inside Linux Playwright
docker run --rm \
  -v "$(pwd)/test/e2e-ui:/work" \
  -w /work \
  -e GRAFANA_URL=http://host.docker.internal:3002 \
  -e PROXY_URL=http://host.docker.internal:3100 \
  mcr.microsoft.com/playwright:v1.59.1-noble \
  /bin/bash -lc "npm ci && npx playwright test --grep @drilldown-core"

# Build binary
go build -o loki-vl-proxy ./cmd/proxy

# Post-deploy tuple contract canary (validates default 2-tuple + categorize-labels 3-tuple; expects recent log data)
PROXY_URL=http://127.0.0.1:3100 ./scripts/smoke-test.sh
```

## Security Validation

The repository now has a dedicated security lane in CI. These are the closest local equivalents when changing auth, tenant isolation, Dockerfile hardening, or workflow security.

```bash
# secret scanning
docker run --rm -v "$PWD:/repo" -w /repo \
  ghcr.io/gitleaks/gitleaks:v8.28.0 \
  detect --source . --report-format sarif --report-path gitleaks.sarif --exit-code 1

# Go SAST
go install github.com/securego/gosec/v2/cmd/gosec@v2.22.7
"$(go env GOPATH)/bin/gosec" \
  -exclude=G104,G108,G115,G301,G302,G304,G306,G402,G404 \
  -exclude-generated \
  ./...

# filesystem vuln/misconfig/secret scan
docker run --rm -v "$PWD:/repo" -w /repo \
  aquasec/trivy:0.69.3 \
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

# repo-specific runtime checks
./scripts/ci/run_security_regressions.sh
./scripts/ci/run_zap_scan.sh baseline
./scripts/ci/run_nuclei_scan.sh
```

The scheduled heavy lane also runs longer fuzzing, image scanning, SBOM generation, broader `Semgrep`, and an OWASP ZAP active scan.

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
| Security CI runtime | Dedicated workflow | custom regressions, ZAP baseline, curated nuclei |
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
| `test/e2e-compat/grafana_surface_test.go` | Grafana datasource catalog, datasource health, proxy bootstrap/control-plane surface |
| `test/e2e-compat/features_test.go` | Live Grafana-facing edge cases including multi-tenant `__tenant_id__`, long-lived tail sessions, and Drilldown level-filter regressions |
| `test/e2e-ui/tests/url-state.spec.ts` | Pure URL/state builder tests for Explore and Logs Drilldown reloadable state |
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
- `SCREENSHOT_OUT_DIR` (default `../../docs/images/ui`)
- `GRAFANA_URL` (default `http://127.0.0.1:3002`)

CI prefers the runner's existing Chrome/Chromium binary for these shards and falls back to `npx playwright install chromium` only when no system browser is available. That removes the repeated `apt` dependency install from the common GitHub-hosted path.

### CI Shards

| Shard | Command | Primary focus |
|---|---|---|
| `datasource` | `npx playwright test tests/datasource.spec.ts` | Grafana datasource settings smoke |
| `explore-core` | `npx playwright test --grep @explore-core` | one default Explore browser smoke |
| `explore-tail` | `npx playwright test --grep @explore-tail` | browser-only multi-tenant (`__tenant_id__` exact and negative regex) plus live-tail recovery |
| `drilldown-core` | `npx playwright test --grep @drilldown-core` | Explore detail-panel smoke and single-tenant Logs Drilldown smoke |
| `drilldown-multitenant` | `npx playwright test --grep @drilldown-mt` | multi-tenant Logs Drilldown landing/service/fields plus URL filter-reload persistence |

## E2E Compatibility Matrix

The Docker-backed `test/e2e-compat` suite now runs as four functional PR shards instead of one monolithic job. Each shard builds the stack, waits on explicit HTTP readiness checks, and runs only its own test family.

| Shard | Primary scope |
|---|---|
| `e2e-compat (core)` | Loki/VL surface parity, alerting, chaining, Explore HTTP contracts, control-plane endpoints |
| `e2e-compat (drilldown)` | Drilldown contracts, Drilldown runtime-family checks, track-score summaries |
| `e2e-compat (otel-edge)` | OTel label translation, complex queries, edge-case payloads and parser behavior |
| `e2e-compat (tail-multitenancy)` | multi-tenant behavior, tail transport semantics, response/security edge checks |

Stack startup now uses [`wait_e2e_stack.sh`](../scripts/ci/wait_e2e_stack.sh) instead of `docker compose --wait` or fixed sleeps. That avoids false failures from services without Docker healthchecks and lets UI and compat jobs share the same readiness logic.

The GitHub-hosted Docker jobs now also prebuild the proxy image once per job through BuildKit cache and start compose stacks with `--no-build`. That keeps the grouped compat shards and UI shards parallel without paying the full Docker rebuild cost every time a stack starts inside the same job.

Compose-backed fleet cache smoke now runs on pull requests and post-merge `main` in CI (`e2e-fleet`), using the dedicated `TestFleetSmoke_*` suite.
Tuple smoke contract canary also runs automatically in CI (`tuple-smoke`) by seeding e2e data then executing `scripts/smoke-test.sh`.

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

Moved out of Playwright:
`internal/proxy/proxy_test.go`, `internal/proxy/gaps_test.go`, and `test/e2e-compat/chaining_test.go` cover query translation, response shape, parser pipelines, line filters, direction handling, and metric-query parity faster than the browser can.
`test/e2e-compat/explore_contract_test.go` now adds the browser-removed HTTP contracts for line filters, `json`, `logfmt`, `direction=forward`, metric matrices, `label_format`, and invalid-query `4xx` handling.

### `explore-tail` shard

| Test | Purpose |
|---|---|
| `multi-tenant query respects __tenant_id__ filter in Explore` | tenant narrowing in Explore |
| `multi-tenant negative regex excludes fake tenant in Explore` | tenant negative-regex narrowing stays browser-visible |
| `live tail works through the browser-allowed synthetic datasource` | browser-safe synthetic live tail |
| `native-tail failure can recover through ingress live tail` | failure recovery after native-tail path breaks |

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

Moved out of Playwright:
`test/e2e-compat/drilldown_compat_test.go` now owns detected-fields contracts, dotted metadata exposure, filtered labels/fields resource behavior, parsed-field freshness, unknown field/label empty-success behavior, Grafana datasource resource parity, and multi-tenant Drilldown resource behavior including regex and no-match tenant filters.

### `drilldown-multitenant` shard

| Test | Purpose |
|---|---|
| `multi-tenant landing shows service buckets without browser errors` | multi-tenant landing-page browser smoke |
| `multi-tenant service drilldown loads without browser errors` | multi-tenant service logs browser smoke |
| `multi-tenant service field view loads detected fields without browser errors` | multi-tenant service fields browser smoke |
| `multi-tenant service filter survives reload from URL state` | multi-tenant URL state keeps `__tenant_id__` filter after reload |

## Compatibility Tracks

The repo now keeps four separate compatibility tracks/contracts:

| Track | Local score test | Matrix coverage |
|---|---|---|
| Loki | `TestLokiTrackScore` | Loki `3.6.x` and `3.7.x` |
| Logs Drilldown | `TestDrilldownTrackScore` | Logs Drilldown `1.0.x` and `2.0.x` families |
| Grafana Loki datasource | `TestGrafanaDatasourceCatalogAndHealth` | Grafana runtime `11.x` and `12.x` families |
| VictoriaLogs | `TestVLTrackScore` | VictoriaLogs `v1.3x.x` through `v1.5x.x` transition band |

The default local stack is pinned to:

- Loki `3.7.1`
- VictoriaLogs `v1.50.0`
- vmalert `v1.138.0` (with local VictoriaMetrics remote-write target for recording-rule evaluation)
- Grafana `12.4.2`
- Logs Drilldown contract `2.0.3` from `grafana/logs-drilldown` commit `c22fae24f533a36ec2173933d2c303804cb7e814`

Field-surface defaults in the pinned stack:

- Labels remain Loki-compatible when `-label-style=underscores` is used
- `-metadata-field-mode=hybrid` is the default, so field APIs expose both native dotted names and translated aliases
- If you need a stricter Loki-only field surface for a focused test, run the proxy with `-metadata-field-mode=translated`
- The compose matrix includes a dedicated native-metadata proxy profile (`-metadata-field-mode=native`) so CI verifies both hybrid and native structured-metadata exposure

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

- `12.4.2` runs the full Drilldown runtime score on scheduled and manual compatibility checks
- `12.4.1` runs a smaller current-family datasource-plus-Drilldown smoke profile on pull requests, and it must include the runtime-family contract checks
- `11.6.6` runs a smaller previous-family datasource-plus-Drilldown smoke profile on pull requests, and it must include the runtime-family contract checks

Logs Drilldown family assertions are explicit in the contract matrix:

- `1.0.x` checks service-selection volume buckets, detected-fields filtering, and labels-field parsing behavior
- `2.0.x` checks detected-level default columns, field-values breakdown scenes, and additional label-tab wiring

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

The report job now collects test count and coverage from the same Go test pass, uses a shallow checkout plus explicit base-SHA fetch, and uses 3-sample benchmark medians to keep the PR gate faster without dropping the tracked signals.
The collector runs test/coverage, compatibility scores, benchmark medians, and load metrics in parallel with bounded fallbacks so a single slow signal does not block the whole report gate.
The PR quality workflow now skips benchmark/load perf smoke when no perf-sensitive files changed, so docs/metadata-only PRs do not report noisy runner jitter as fake regressions.
The quality gate compares base/head with relative and absolute regression thresholds and ignores low-baseline noise, so tiny shared-runner jitter does not fail required checks.
The high-concurrency load threshold remains strict locally (`>10k req/s`) and uses a CI floor (`>5k req/s`) on shared race-enabled runners to avoid flaky non-regression failures.
Loki compatibility is additionally enforced as a hard floor at `100%` on PR quality and on the dedicated Loki compatibility workflow.
Release automation also materializes `CHANGELOG.md` `Unreleased` into the new version section and uses that same section as the GitHub release notes body, then syncs README tests/coverage/Go LOC badges on `main`.
For reliable metadata PR auto-merge under branch protection, set repository secret `RELEASE_PR_TOKEN` (PAT/App token with repo scope) so release-created metadata PRs trigger required `pull_request` checks.

Required-check note:

- The repo now exposes the grouped compat jobs directly: `e2e-compat (core)`, `e2e-compat (drilldown)`, `e2e-compat (otel-edge)`, and `e2e-compat (tail-multitenancy)`.
- The legacy umbrella `e2e-compat` job remains as a compatibility shim until branch protection is updated to require the grouped checks directly.

That report is part of the required PR gate. It is still a smoke signal rather than a full benchmark lab run, but it now blocks obvious regressions in coverage, compatibility, and the tracked performance signals.

See [compatibility-matrix.md](compatibility-matrix.md), [compatibility-loki.md](compatibility-loki.md), [compatibility-drilldown.md](compatibility-drilldown.md), [compatibility-victorialogs.md](compatibility-victorialogs.md), and [compatibility-matrix.json](../test/e2e-compat/compatibility-matrix.json).

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
