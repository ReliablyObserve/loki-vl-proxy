# Testing

## Quick Start

```bash
# Unit tests
go test ./...

# With race detector
go test -race ./...

# E2E compatibility tests (requires docker-compose)
cd test/e2e-compat
docker-compose up -d --build
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
```

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
| Critical fixes | 30+ | Data race, binary operators, delete safeguards, without() |
| Benchmarks | 10 | Translation ~5us, cache hit 42ns |
| E2E basic (Loki vs proxy) | 11 | Side-by-side API response comparison |
| E2E complex (real-world) | 31 | Multi-label, chained filters, parsers, cross-service |
| E2E edge cases (VL issues) | 12 | Large bodies, dotted labels, unicode, multiline |
| Fuzz testing | 1.2M+ executions | No panics found |

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
| `internal/cache/disk_test.go` | L2 disk cache |
| `internal/middleware/middleware_test.go` | Rate limiter, circuit breaker |
| `test/e2e-compat/` | Docker-based Loki vs proxy comparison |
| `test/e2e-compat/drilldown_compat_test.go` | Grafana Logs Drilldown resource contracts via Grafana datasource proxy |
| `test/e2e-compat/explore_contract_test.go` | HTTP-level Explore contracts for line filters, parsers, direction, metric shape, `label_format`, invalid-query handling |
| `test/e2e-compat/grafana_surface_test.go` | Grafana datasource catalog, datasource health, proxy bootstrap/control-plane surface |
| `test/e2e-compat/features_test.go` | Live Grafana-facing edge cases including multi-tenant `__tenant_id__`, long-lived tail sessions, and Drilldown level-filter regressions |
| `test/e2e-ui/tests/url-state.spec.ts` | Pure URL/state builder tests for Explore and Logs Drilldown reloadable state |
| `test/e2e-ui/` | Playwright browser smoke tests for datasource UI, Explore, and Logs Drilldown with console/request guardrails |

## Playwright UI Matrix

The browser suite now keeps only browser-only smoke paths. Query parity, Drilldown resource contracts, datasource bootstrap, and most tail protocol coverage live in `test/e2e-compat` or lower-level Go tests so CI does not keep paying Chromium cost for them.

CI prefers the runner's existing Chrome/Chromium binary for these shards and falls back to `npx playwright install chromium` only when no system browser is available. That removes the repeated `apt` dependency install from the common GitHub-hosted path.

### CI Shards

| Shard | Command | Primary focus |
|---|---|---|
| `datasource` | `npx playwright test tests/datasource.spec.ts` | Grafana datasource settings smoke |
| `explore-core` | `npx playwright test --grep @explore-core` | one default Explore browser smoke |
| `explore-tail` | `npx playwright test --grep @explore-tail` | browser-only multi-tenant and live-tail recovery |
| `drilldown-core` | `npx playwright test --grep @drilldown-core` | Explore detail-panel smoke and single-tenant Logs Drilldown smoke |
| `drilldown-multitenant` | `npx playwright test --grep @drilldown-mt` | one multi-tenant Logs Drilldown service smoke |

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
| `multi-tenant service drilldown loads without browser errors` | multi-tenant service browser smoke |

## Compatibility Tracks

The repo now keeps three separate compatibility tracks:

| Track | Local score test | Matrix coverage |
|---|---|---|
| Loki | `TestLokiTrackScore` | Loki `3.6.x` and `3.7.x` |
| Logs Drilldown | `TestDrilldownTrackScore` | Logs Drilldown `1.0.x` and `2.0.x` families |
| VictoriaLogs | `TestVLTrackScore` | VictoriaLogs `v1.3x.x` and `v1.4x.x` bands |

The default local stack is pinned to:

- Loki `3.7.1`
- VictoriaLogs `v1.49.0`
- Grafana `12.4.2`
- Logs Drilldown contract `2.0.1` from `grafana/logs-drilldown` commit `4463f56047de75da95251086d1906fb902ad53a7`

Field-surface defaults in the pinned stack:

- Labels remain Loki-compatible when `-label-style=underscores` is used
- `-metadata-field-mode=hybrid` is the default, so field APIs expose both native dotted names and translated aliases
- If you need a stricter Loki-only field surface for a focused test, run the proxy with `-metadata-field-mode=translated`

Support window policy:

- Loki: current minor family plus one minor behind
- Grafana runtime: pinned current family gets the fuller Drilldown runtime contract, and pull requests also run smaller current-family and previous-family smoke profiles; the full runtime matrix stays on scheduled/manual coverage
- Logs Drilldown: current family plus one family behind
- VictoriaLogs: `v1.3x.x` and `v1.4x.x`

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
The quality gate compares base/head with relative and absolute regression thresholds and ignores low-baseline noise, so tiny shared-runner jitter does not fail required checks.
The high-concurrency load threshold remains strict locally (`>10k req/s`) and uses a CI floor (`>5k req/s`) on shared race-enabled runners to avoid flaky non-regression failures.

Required-check note:

- The repo now exposes the grouped compat jobs directly: `e2e-compat (core)`, `e2e-compat (drilldown)`, `e2e-compat (otel-edge)`, and `e2e-compat (tail-multitenancy)`.
- The legacy umbrella `e2e-compat` job remains as a compatibility shim until branch protection is updated to require the grouped checks directly.

That report is part of the required PR gate. It is still a smoke signal rather than a full benchmark lab run, but it now blocks obvious regressions in coverage, compatibility, and the tracked performance signals.

See [compatibility-matrix.md](/tmp/Loki-VL-proxy/docs/compatibility-matrix.md), [compatibility-loki.md](/tmp/Loki-VL-proxy/docs/compatibility-loki.md), [compatibility-drilldown.md](/tmp/Loki-VL-proxy/docs/compatibility-drilldown.md), [compatibility-victorialogs.md](/tmp/Loki-VL-proxy/docs/compatibility-victorialogs.md), and [compatibility-matrix.json](/tmp/Loki-VL-proxy/test/e2e-compat/compatibility-matrix.json).

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
