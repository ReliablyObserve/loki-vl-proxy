# Playwright Grafana UI E2E Tests

Validates the Loki-VL-proxy through Grafana's UI: datasource setup, Explore, Drilldown, multi-tenant flows, and live-tail recovery.

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
| `datasource.spec.ts` | Datasource health, proxy bootstrap endpoints, rules and alerts compatibility, direct datasource health |
| `explore.spec.ts` | `@explore-core` query parity plus `@explore-tail` multi-tenant and live-tail browser flows |
| `drilldown.spec.ts` | `@drilldown-core` Explore drilldown and error-handling coverage |
| `logs-drilldown.spec.ts` | `@drilldown-core` service-detail flows plus `@drilldown-mt` multi-tenant Logs Drilldown coverage |

## CI Shards

The GitHub Actions `e2e-ui` job runs as five shards:

| Shard | Command | Coverage |
|------|---------|----------|
| `datasource` | `npx playwright test tests/datasource.spec.ts` | datasource setup and proxy bootstrap surface |
| `explore-core` | `npx playwright test --grep @explore-core` | Explore query rendering and proxy-vs-Loki parity |
| `explore-tail` | `npx playwright test --grep @explore-tail` | multi-tenant Explore and live-tail recovery |
| `drilldown-core` | `npx playwright test --grep @drilldown-core` | Explore drilldown and single-tenant Logs Drilldown |
| `drilldown-multitenant` | `npx playwright test --grep @drilldown-mt` | multi-tenant Logs Drilldown filters and level flows |

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

## Debug

```bash
# Run with browser visible
npm run test:headed

# Step-through debugger
npm run test:debug

# View HTML report
npm run report
```
