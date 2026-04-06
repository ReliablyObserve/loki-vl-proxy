# Playwright Grafana UI E2E Tests

Validates the Loki-VL-proxy through Grafana's UI — Explore, drill-down, label navigation, error handling.

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
npm install
npx playwright install chromium
npm test
```

If local Chromium cannot start on macOS, run the exact same suite inside Linux Playwright:

```bash
docker run --rm \
  -v "$(pwd):/work" \
  -w /work \
  -e GRAFANA_URL=http://host.docker.internal:3002 \
  -e PROXY_URL=http://host.docker.internal:3100 \
  mcr.microsoft.com/playwright:v1.59.1-noble \
  /bin/bash -lc 'npm ci && npx playwright test --grep "Grafana Logs Drilldown"'
```

## Test Suites

| File | Coverage |
|------|----------|
| `explore.spec.ts` | Basic queries, filters, parsers, metric queries, side-by-side proxy vs Loki |
| `logs-drilldown.spec.ts` | Service list, service detail drill-down, fields, label values, proxy vs direct Loki |
| `datasource.spec.ts` | Health check, buildinfo, ready, rules/alerts compatibility surface |

## Debug

```bash
# Run with browser visible
npm run test:headed

# Step-through debugger
npm run test:debug

# View HTML report
npm run report
```
