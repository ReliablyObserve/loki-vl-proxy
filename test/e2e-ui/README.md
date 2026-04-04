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

## Test Suites

| File | Coverage |
|------|----------|
| `explore.spec.ts` | Basic queries, filters, parsers, metric queries, side-by-side proxy vs Loki |
| `drilldown.spec.ts` | Log row expansion, label filter drill-down, multi-label navigation, error handling |
| `datasource.spec.ts` | Health check, buildinfo, ready, admin stubs (rules/alerts) |

## Debug

```bash
# Run with browser visible
npm run test:headed

# Step-through debugger
npm run test:debug

# View HTML report
npm run report
```
