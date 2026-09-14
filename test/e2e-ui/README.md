# Playwright Grafana UI E2E Tests

Validates the Loki-VL-proxy through Grafana's UI: datasource settings smoke, Explore smoke, Logs Drilldown smoke, multi-tenant browser flows, live-tail recovery, and API-level proxy-vs-Loki parity checks driven through Grafana.

## Prerequisites

```bash
# Start the full e2e stack with the log generator profile (as CI does)
cd ../e2e-compat
docker compose --profile ui up -d --build
../../scripts/ci/wait_e2e_stack.sh 180
```

## Run Tests

```bash
cd test/e2e-ui
npm ci
npx playwright install chromium
npm test
```

`npm test` runs every spec, including the ones that no CI shard selects.

### Environment Variables

| Variable | Default | Used by |
|------|---------|----------|
| `GRAFANA_URL` | `http://127.0.0.1:3002` | Playwright `baseURL` in `playwright.config.ts` and `capture-screenshots.mjs` |
| `LOKI_URL` | `http://127.0.0.1:13101` | `security-hardening-visibility.spec.ts`, `drilldown-loki-vs-proxy-compare.spec.ts` |
| `VL_URL` | `http://127.0.0.1:19428` | ingest helpers in `explore.spec.ts`, `logs-drilldown.spec.ts`, `security-hardening-visibility.spec.ts` |
| `PROXY_NATIVE_METADATA_URL` | `http://127.0.0.1:13106` | `drilldown-loki-vs-proxy-compare.spec.ts` |
| `PLAYWRIGHT_EXECUTABLE_PATH` | unset | use an installed Chrome/Chromium instead of the Playwright download |
| `CI` | unset | when set: 1 worker, 1 retry, `forbidOnly` |
| `WORKERS` | `4` | local worker count when `CI` is unset |
| `HEADED` | unset | run the browser headed |

CI also exports `PROXY_URL=http://127.0.0.1:13100`, but no spec reads it.

## Capture UI Screenshots

Generate docs-ready screenshots directly from the local compose stack:

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

Default output path (`SCREENSHOT_OUT_DIR`, default `../../docs/images/ui` relative to the working directory):

- `docs/images/ui/explore-main.png`
- `docs/images/ui/explore-details.png`
- `docs/images/ui/drilldown-main.png`
- `docs/images/ui/drilldown-service.png`
- `docs/images/ui/explore-tail-multitenant.png`

Script overrides:

| Variable | Default |
|------|---------|
| `GRAFANA_URL` | `http://127.0.0.1:3002` |
| `PROXY_QUERY_URL` | `http://127.0.0.1:3100` (compose proxy: `http://127.0.0.1:13100`) |
| `VL_INSERT_URL` | `http://127.0.0.1:9428/insert/jsonline?_stream_fields=app,service_name,level,detected_level` (compose VictoriaLogs: port `19428`) |
| `SCREENSHOT_OUT_DIR` | `../../docs/images/ui` |
| `SCREENSHOT_FROM` | `now-5m` |
| `SCREENSHOT_TO` | `now` |
| `PLAYWRIGHT_EXECUTABLE_PATH` | unset |

For example, widen the time window:

```bash
SCREENSHOT_FROM=now-15m SCREENSHOT_TO=now \
PROXY_QUERY_URL=http://127.0.0.1:13100 \
VL_INSERT_URL='http://127.0.0.1:19428/insert/jsonline?_stream_fields=app,service_name,level,detected_level' \
npm run capture:screenshots
```

The script seeds fresh logs and keeps writing background logs while capturing, so screenshots include live data in Explore range, Explore live tail, and Drilldown service views.

If local Chromium cannot start on macOS, run the same suite inside Linux Playwright (from `test/e2e-ui`). Inside the container `127.0.0.1` is the container itself, so point every URL at the host:

```bash
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  -e GRAFANA_URL=http://host.docker.internal:3002 \
  -e LOKI_URL=http://host.docker.internal:13101 \
  -e VL_URL=http://host.docker.internal:19428 \
  -e PROXY_NATIVE_METADATA_URL=http://host.docker.internal:13106 \
  mcr.microsoft.com/playwright:v1.59.1-noble \
  /bin/bash -lc 'npm ci && npx playwright test --grep @drilldown-core'
```

## Test Suites

| File | Tags | Coverage |
|------|------|----------|
| `datasource.spec.ts` | (selected by file path) | Grafana datasource Save & Test smoke |
| `explore.spec.ts` | `@explore-core`, `@explore-tail` | default Explore smoke plus multi-tenant exact/negative `__tenant_id__` flows and live-tail browser flows |
| `explore-parity.spec.ts` | `@explore-core` | API-level `sum by (level)` rate series-set parity between the proxy and Loki datasources |
| `security-hardening-visibility.spec.ts` | `@explore-core` | exact windows and formatted rows stay visible for the proxy, interact proxy, and Loki datasources |
| `url-state.spec.ts` | `@explore-core`, `@drilldown-core` | pure URL/state builder tests for reloadable Explore and Drilldown URLs |
| `drilldown.spec.ts` | `@drilldown-core` | Explore detail-panel and label filter smoke |
| `drilldown-limits-gate.spec.ts` | `@drilldown-core` | Patterns tab gate driven by `drilldown-limits` |
| `logs-drilldown.spec.ts` | `@drilldown-core`, `@drilldown-mt` | Logs Drilldown landing/service/patterns smoke plus multi-tenant landing, service, fields, filter, missing-tenant, and URL reload persistence |
| `drilldown-cache-regression.spec.ts` | `@drilldown-cache`; labels-sidebar group also `@drilldown-core` | Drilldown fields tab per time range: no errors, labels sidebar, no right-edge gap, repeated loads |
| `explore-operations.spec.ts` | `@explore-ops` | Loki operations in Explore: parsers, formatting, metric queries, line filters, aggregations |
| `explore-multitenant.spec.ts` | `@explore-mt` | multi-tenant Explore: datasource switching, valid/missing tenant, filter-for-value |
| `explore-regression.spec.ts` | `@regression` | API-level proxy-vs-Loki parity register: selectors, filters, parsers, pipelines, metric queries, series cap, content checks |
| `explore-comprehensive-ui.spec.ts` | `@comprehensive-ui` | Explore UI coverage with error guards; timings recorded as annotations |
| `explore-click-interactions.spec.ts` | `@click-interactions` | log row expansion, content checks, filter buttons, complex queries, timing cases (no CI shard) |
| `performance-baseline.spec.ts` | `@performance` | browser timing thresholds (no CI shard) |
| `drilldown-loki-vs-proxy-compare.spec.ts` | `@compare` | direct Loki vs native-metadata proxy fields/labels comparison across time ranges (no CI shard) |

Most non-browser assertions moved out of Playwright:
- `test/e2e-compat/grafana_surface_test.go` covers datasource catalog, health, and proxy bootstrap/control-plane endpoints
- `test/e2e-compat/explore_contract_test.go` covers HTTP-level Explore contracts for filters, parser pipelines, direction handling, metric matrices, `label_format`, and invalid-query handling
- `test/e2e-compat/drilldown_compat_test.go` covers Grafana datasource resource contracts for Drilldown
- `test/e2e-compat/features_test.go` plus `internal/proxy/*tail*test.go` cover most tail protocol and fallback behavior
- `internal/proxy/proxy_test.go` and `test/e2e-compat/chaining_test.go` cover query parity and translation paths faster than the browser

## CI Shards

The GitHub Actions `e2e-ui` job runs as nine shards:

| Shard | Command | Coverage |
|------|---------|----------|
| `datasource` | `npx playwright test tests/datasource.spec.ts` | datasource settings smoke |
| `explore-core` | `npx playwright test --grep @explore-core` | default Explore smoke, API-level proxy-vs-Loki metric parity (`explore-parity.spec.ts`), security hardening visibility, Explore URL-state |
| `explore-tail` | `npx playwright test --grep @explore-tail` | multi-tenant Explore exact/negative tenant filtering plus browser live-tail recovery |
| `drilldown-core` | `npx playwright test --grep @drilldown-core` | Explore detail-panel smoke, URL-state unit coverage, single-tenant Logs Drilldown smoke, the Patterns-tab gate driven by `drilldown-limits` (`drilldown-limits-gate.spec.ts`), and the Drilldown labels-sidebar cache regression |
| `drilldown-multitenant` | `npx playwright test --grep @drilldown-mt` | multi-tenant Logs Drilldown landing/service/fields smoke plus filter persistence from URL state |
| `explore-ops` | `npx playwright test --grep @explore-ops` | Loki operations parity in Explore: parsers, formatting, metric queries, line filters, aggregations |
| `explore-mt` | `npx playwright test --grep @explore-mt` | multi-tenant Explore coverage |
| `explore-regression` | `npx playwright test --grep @regression` | API-level proxy-vs-Loki parity register: log selectors, filters, parsers, pipelines, grouped metric queries, the series cap, content checks (`explore-regression.spec.ts`) |
| `explore-comprehensive` | `npx playwright test --grep @comprehensive-ui` | Explore UI coverage: page load, editor, query execution, results panel, empty results, filters; timings recorded as annotations (`explore-comprehensive-ui.spec.ts`) |

`@click-interactions`, `@performance`, `@compare`, and the `@drilldown-cache` groups without `@drilldown-core` are not selected by any shard.

Each shard starts the stack with `docker compose --profile ui up -d --no-build`. CI prefers the runner's existing Chrome/Chromium binary and only falls back to `npx playwright install chromium` if no system browser is present. That avoids repeated `apt` dependency downloads on normal GitHub-hosted runners while keeping a safe fallback path.

The CI jobs also prebuild the proxy image once per job and then start the compose stack with `--no-build`, so the nine browser shards keep their parallelism without redoing the proxy Docker build inside the same job.

Run any shard locally with the same command CI uses:

```bash
npx playwright test tests/datasource.spec.ts
npx playwright test --grep @explore-core
npx playwright test --grep @explore-tail
npx playwright test --grep @drilldown-core
npx playwright test --grep @drilldown-mt
npx playwright test --grep @explore-ops
npx playwright test --grep @explore-mt
npx playwright test --grep @regression
npx playwright test --grep @comprehensive-ui
```

## Scenario Matrix

See [`docs/testing.md`](../../docs/testing.md) for the per-test UI matrix with each Playwright scenario mapped to its shard and purpose.

The remaining browser tests now install page guardrails by default:

- unexpected browser `console.error` messages fail the test
- unexpected request failures fail the test
- unexpected `4xx`/`5xx` datasource/runtime responses fail the test

Only the native-tail recovery smoke allows the specific tail/live-websocket failures it intentionally triggers before switching to the ingress datasource.

## Debug

```bash
# Run with browser visible
npm run test:headed

# Step-through debugger
npm run test:debug

# View HTML report
npm run report
```
