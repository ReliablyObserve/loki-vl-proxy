# Playwright Grafana UI E2E Tests

Validates the Loki-VL-proxy through Grafana's UI: datasource settings smoke, Explore smoke, Logs Drilldown smoke, multi-tenant browser flow, and live-tail recovery.

## Prerequisites

```bash
# Start the full e2e stack
cd ../e2e-compat
docker-compose up -d --build

# Wait for Grafana to be ready (~20s)
curl -s http://localhost:3002/api/health | jq .
```

## Run Tests

```bash
cd test/e2e-ui
npm ci
npx playwright install chromium
npm test
```

If local Chromium cannot start on macOS, run the same suite inside Linux Playwright:

```bash
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  -e GRAFANA_URL=http://host.docker.internal:3002 \
  -e PROXY_URL=http://host.docker.internal:3100 \
  mcr.microsoft.com/playwright:v1.59.1-noble \
  /bin/bash -lc 'npm ci && npx playwright test --grep @drilldown-core'
```

## Test Suites

| File | Coverage |
|------|----------|
| `datasource.spec.ts` | Grafana datasource Save & Test smoke |
| `explore.spec.ts` | `@explore-core` default Explore smoke plus `@explore-tail` multi-tenant and live-tail browser flows |
| `drilldown.spec.ts` | `@drilldown-core` Explore detail-panel smoke |
| `logs-drilldown.spec.ts` | `@drilldown-core` Logs Drilldown landing/service smoke plus `@drilldown-mt` one multi-tenant filter smoke |
| `url-state.spec.ts` | pure URL/state builder tests for reloadable Explore and Drilldown URLs |

Most non-browser assertions moved out of Playwright:
- `test/e2e-compat/grafana_surface_test.go` covers datasource catalog, health, and proxy bootstrap/control-plane endpoints
- `test/e2e-compat/explore_contract_test.go` covers HTTP-level Explore contracts for filters, parser pipelines, direction handling, metric matrices, `label_format`, and invalid-query handling
- `test/e2e-compat/drilldown_compat_test.go` covers Grafana datasource resource contracts for Drilldown
- `test/e2e-compat/features_test.go` plus `internal/proxy/*tail*test.go` cover most tail protocol and fallback behavior
- `internal/proxy/proxy_test.go` and `test/e2e-compat/chaining_test.go` cover query parity and translation paths faster than the browser

## CI Shards

The GitHub Actions `e2e-ui` job runs as five shards:

| Shard | Command | Coverage |
|------|---------|----------|
| `datasource` | `npx playwright test tests/datasource.spec.ts` | datasource settings smoke |
| `explore-core` | `npx playwright test --grep @explore-core` | one default Explore smoke |
| `explore-tail` | `npx playwright test --grep @explore-tail` | multi-tenant Explore plus browser live-tail recovery |
| `drilldown-core` | `npx playwright test --grep @drilldown-core` | Explore detail-panel smoke, URL-state unit coverage, and single-tenant Logs Drilldown smoke |
| `drilldown-multitenant` | `npx playwright test --grep @drilldown-mt` | one multi-tenant Logs Drilldown filter smoke |

Run any shard locally with the same command CI uses:

```bash
npx playwright test tests/datasource.spec.ts
npx playwright test --grep @explore-core
npx playwright test --grep @explore-tail
npx playwright test --grep @drilldown-core
npx playwright test --grep @drilldown-mt
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
