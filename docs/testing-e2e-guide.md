---
sidebar_label: E2E Test Infrastructure
description: How to run and extend the E2E compatibility tests against a local Docker Compose stack.
---

# E2E Test Infrastructure Guide

Practical reference for the Loki-VL-proxy end-to-end test suite.

## Quick Start

```bash
# 1. Build the proxy image
cd test/e2e-compat
docker compose up -d --build

# 2. Wait for all services to be ready (timeout 180s)
../../scripts/ci/wait_e2e_stack.sh 180

# 3. Run Go e2e tests
go test -v -tags=e2e -timeout=180s ./test/e2e-compat/

# 4. Run Playwright UI tests
cd ../e2e-ui
npm ci
npx playwright install chromium
npm test
```

## Stack Architecture

The compose stack at `test/e2e-compat/docker-compose.yml` runs:

| Service | Host Port | Purpose |
|---------|-----------|---------|
| Loki 3.7.1 | :3101 | Reference implementation (ground truth) |
| VictoriaLogs v1.50.0 | :9428 | Backend for the proxy |
| vmauth v1.138.0 | (internal) | Auth proxy in front of VictoriaLogs |
| VictoriaMetrics v1.119.0 | (internal) | TSDB for vmalert recording-rule remote write |
| vmalert v1.138.0 | :8880 | Alert/rule backend |
| 10 proxy variants | :3100-:3110 | See [Proxy Variants](#proxy-variants) |
| Grafana 12.4.2 | :3002 | UI with all datasources provisioned |
| tail-ingress (nginx) | :3104 | Nginx reverse proxy for tail WebSocket tests |

## Dual-Write Pattern

`pushStream()` in `test/e2e-compat/testdata.go` sends identical data to both backends:

1. **Loki** -- POST to `lokiURL + "/loki/api/v1/push"` with the standard Loki push JSON format (`{"streams": [{"stream": labels, "values": [[ts, line], ...]}]}`)
2. **VictoriaLogs** -- POST to `vlURL + "/insert/jsonline?_stream_fields=..."` with NDJSON where each line includes `_time`, `_msg`, and all labels as fields

This ensures both backends have byte-identical log content. Tests then query Loki (ground truth) and the proxy (translating layer), comparing responses.

## Adding a Go E2E Test

1. **Add test data** (if needed) -- add a new `pushStream()` call in `testdata.go` inside `ingestRichTestData()` with appropriate labels and log lines.

2. **Create test function** in `test/e2e-compat/` with `//go:build e2e` tag. Call `ingestRichTestData(t)` to seed data.

3. **Query both endpoints** -- hit `lokiURL` (:3101) and `proxyURL` (:3100) with the same LogQL query using `queryRange()`.

4. **Compare responses** -- assert line counts, result types, or use existing comparison helpers.

5. **Run**: `go test -v -tags=e2e -run '^TestMyFeature_Something$' ./test/e2e-compat/`

## Adding a Playwright Test

1. **Create spec** in `test/e2e-ui/tests/`. Tag each test with a shard name in the title (e.g., `@explore-core`).

2. **Tag with shard** -- use an existing tag (`@explore-core`, `@explore-tail`, `@drilldown-core`, `@drilldown-mt`, `@explore-ops`) or create a new one.

3. **Add shard to CI** -- if new tag, add to `matrix.shard` in `.github/workflows/ci.yaml` under `e2e-ui`: `{name: "my-shard", command: "--grep @my-shard"}`.

4. **Run locally**: `cd test/e2e-ui && npx playwright test --grep @my-shard` (or `npm run test:headed` for visible browser).

## Adding a Semantics Matrix Case

1. **Add case** to `test/e2e-compat/query-semantics-matrix.json` with fields: `id`, `family`, `endpoint` (`query` or `query_range`), `query`, `expectation`, `expect_result_type`, `compare`, `require_non_empty`.

2. **Register in operations** -- add the case ID to the appropriate operation in `test/e2e-compat/query-semantics-operations.json`, or create a new operation entry with `name`, `category`, and `cases` array.

3. **Verify**: `go test -v -tags=e2e -run '^TestQuerySemanticsMatrix$' ./test/e2e-compat/`

## Proxy Variants

| Port | Service | Label Style | Metadata Mode | Purpose |
|------|---------|-------------|---------------|---------|
| 3100 | loki-vl-proxy | default | default | Primary proxy, indexed label-values cache |
| 3102 | loki-vl-proxy-underscore | underscores | hybrid | OTel dot-to-underscore, structured metadata |
| 3103 | loki-vl-proxy-tail | underscores | default | Synthetic tail mode, browser origin allowlist |
| 3104 | tail-ingress (nginx) | -- | -- | Reverse proxy in front of tail for WebSocket tests |
| 3105 | loki-vl-proxy-tail-native | underscores | default | Native VL tail mode |
| 3106 | loki-vl-proxy-native-metadata | underscores | native | Native metadata field mode |
| 3107 | loki-vl-proxy-translated-metadata | underscores | translated | Translated-only metadata aliases |
| 3108 | loki-vl-proxy-no-metadata | underscores | translated | Structured metadata emission disabled |
| 3109 | loki-vl-proxy-vmauth | underscores | translated | Backend routed through vmauth |
| 3110 | loki-vl-proxy-patterns-autodetect | default | default | Patterns autodetect from queries |

## CI Integration

### E2E Compat Groups (ci.yaml)

5 parallel groups under `e2e-compat-group`:

| Group | Coverage |
|-------|----------|
| `core` | TestCompat, TestExtended, TestChaining, TestAlerting, Explore HTTP contracts, Loki functions, datasource catalog |
| `drilldown` | TestDrilldown, index stats/volume, track scores |
| `otel-edge` | OTel labels, structured metadata, underscore proxy, label dedup/translation edge cases |
| `tail-multitenancy` | Multitenancy, tail modes, security headers, metrics, gzip, derived fields, concurrent/edge queries |
| `semantics` | Query semantics matrix, operations inventory, range metric compatibility, Grafana clickout |

### Playwright Shards (ci.yaml)

6 parallel shards under `e2e-ui`:

| Shard | Command |
|-------|---------|
| `datasource` | `tests/datasource.spec.ts` |
| `explore-core` | `--grep @explore-core` |
| `explore-tail` | `--grep @explore-tail` |
| `drilldown-core` | `--grep @drilldown-core` |
| `drilldown-multitenant` | `--grep @drilldown-mt` |
| `explore-ops` | `--grep @explore-ops` |

### Weekly Loki Matrix (compat-loki.yaml)

Runs on schedule (`cron: 15 3 * * 1`) against multiple Loki versions from `test/e2e-compat/compatibility-matrix.json`. Enforces 100% Loki compatibility score.

## Debugging

**Grafana UI** -- open http://localhost:3002 (anonymous admin, no login). Three datasources are provisioned: Loki (direct), Proxy, and VictoriaLogs.

**Docker logs**:
```bash
cd test/e2e-compat
docker compose logs proxy              # main proxy
docker compose logs proxy-underscore   # underscore variant
docker compose logs loki               # reference Loki
docker compose logs -f victorialogs    # follow VL logs
```

**Re-run a single test**:
```bash
go test -v -tags=e2e -run '^TestCompat_QueryRange$' ./test/e2e-compat/
```

**Playwright debug**:
```bash
cd test/e2e-ui
npm run test:headed    # visible browser
npm run test:debug     # step-through debugger
npm run report         # view HTML report after run
```

**Stack health check**:
```bash
curl -s http://127.0.0.1:3100/ready    # proxy
curl -s http://127.0.0.1:3101/ready    # loki
curl -s http://127.0.0.1:9428/health   # victorialogs
curl -s http://127.0.0.1:3002/api/health  # grafana
```

## Version Overrides

Set environment variables before `docker compose up` to override image versions:

| Variable | Default |
|----------|---------|
| `LOKI_IMAGE` | `grafana/loki:3.7.1` |
| `VICTORIALOGS_IMAGE` | `victoriametrics/victoria-logs:v1.50.0` |
| `GRAFANA_IMAGE` | `grafana/grafana:12.4.2` |
| `PROXY_IMAGE` | `loki-vl-proxy:e2e-local` |
| `VMAUTH_IMAGE` | `victoriametrics/vmauth:v1.138.0` |
| `VMALERT_IMAGE` | `victoriametrics/vmalert:v1.138.0` |
| `VICTORIAMETRICS_IMAGE` | `victoriametrics/victoria-metrics:v1.119.0` |

```bash
LOKI_IMAGE=grafana/loki:3.6.0 docker compose up -d --build
```