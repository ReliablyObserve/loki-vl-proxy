---
sidebar_label: E2E Test Infrastructure
description: How to run and extend the E2E compatibility tests against a local Docker Compose stack.
---

# E2E Test Infrastructure Guide

Practical reference for the Loki-VL-proxy end-to-end test suite.

## Quick Start

```bash
# 1. Build the proxy image and start the stack (from the repository root)
cd test/e2e-compat
docker compose up -d --build

# 2. Wait for all services to be ready (timeout 180s)
../../scripts/ci/wait_e2e_stack.sh 180

# 3. Run Go e2e tests from the repository root
cd ../..
go test -v -tags=e2e -timeout=300s -count=1 -run '^TestSetup_IngestLogs$|^TestCompat_' ./test/e2e-compat/

# 4. Run Playwright UI tests: restart the stack with the log generator profile first
(cd test/e2e-compat && docker compose down -v && docker compose --profile ui up -d --build && ../../scripts/ci/wait_e2e_stack.sh 180)
cd test/e2e-ui
npm ci
npx playwright install chromium
npm test
```

CI runs each Go test group on its own fresh stack (see [E2E Compat Groups](#e2e-compat-groups-ciyaml)). Running several groups on one stack re-ingests fixtures, so run `docker compose down -v` before switching groups. Keep the `ui` profile off for Go parity runs: the continuous log generator changes cardinality during comparisons.

## Stack Architecture

The compose stack at `test/e2e-compat/docker-compose.yml` runs:

| Service | Host Port | Purpose |
|---------|-----------|---------|
| `loki` (Loki 3.7.1) | 13101 | Reference implementation (ground truth) |
| `victorialogs` (VictoriaLogs v1.50.0) | 19428 | Backend for the proxy |
| `vmauth` (vmauth v1.138.0) | (internal) | Auth proxy in front of VictoriaLogs for `loki-vl-proxy-vmauth` |
| `vmauth-ring` (vmauth v1.138.0) | 13200 | Round-robin load balancer across the three peer-ring proxies |
| `victoriametrics` (VictoriaMetrics v1.119.0) | 18428 | vmalert remote-write target and scrape store for proxy/Loki/VictoriaLogs metrics |
| `vmalert` (vmalert v1.138.0) | 18880 | Alert/rule backend |
| 11 proxy variants | 13100, 13102-13103, 13105-13110, 13150-13151 | See [Proxy Variants](#proxy-variants) |
| `tail-ingress` (nginx 1.27) | 13104 | Nginx reverse proxy for tail WebSocket tests |
| `grafana` (Grafana 13.0.1) | 3002 | UI with all datasources provisioned |
| `log-generator` (profile `ui`) | (none) | Continuous dual-write of multi-service logs to Loki and VictoriaLogs |

Grafana preinstalls `victoriametrics-logs-datasource@0.26.3` and `grafana-lokiexplore-app@2.0.4`.

## Dual-Write Pattern

`pushStream()` in `test/e2e-compat/testdata.go` sends identical data to both backends:

1. **Loki** -- POST to `lokiURL + "/loki/api/v1/push"` with the standard Loki push JSON format (`{"streams": [{"stream": labels, "values": [[ts, line], ...]}]}`)
2. **VictoriaLogs** -- POST to `vlURL + "/insert/jsonline?_stream_fields=..."` with NDJSON where each line includes `_time`, `_msg`, and all labels as fields

This ensures both backends have byte-identical log content. Tests then query Loki (ground truth) and the proxy (translating layer), comparing responses.

The Go tests read their endpoints from environment variables with compose defaults: `LOKI_URL` (`http://localhost:13101`), `PROXY_URL` (`http://localhost:13100`), `PROXY_VMAUTH_URL` (`http://localhost:13109`), `TAIL_PROXY_URL` (`http://localhost:13103`), `TAIL_INGRESS_URL` (`http://localhost:13104`), `TAIL_NATIVE_URL` (`http://localhost:13105`), and `VL_URL` (`http://localhost:19428`).

## Adding a Go E2E Test

1. **Add test data** (if needed) -- add a new `pushStream()` call in `testdata.go` inside `ingestRichTestData()` with appropriate labels and log lines.

2. **Create test function** in `test/e2e-compat/` with `//go:build e2e` tag. Call `ingestRichTestData(t)` to seed data.

3. **Query both endpoints** -- hit `lokiURL` (:13101) and `proxyURL` (:13100) with the same LogQL query, for example with `queryRange()`.

4. **Compare responses** -- assert line counts, result types, or use existing comparison helpers.

5. **Run**: `go test -v -tags=e2e -run '^TestMyFeature_Something$' ./test/e2e-compat/`

6. **Wire it into CI** -- add the test name to one of the group patterns in `.github/workflows/ci.yaml`; tests that match no pattern do not run in CI.

## Adding a Playwright Test

1. **Create spec** in `test/e2e-ui/tests/`. Tag each test (or its `test.describe` title) with a shard tag (e.g., `@explore-core`).

2. **Tag with shard** -- use an existing tag (`@explore-core`, `@explore-tail`, `@drilldown-core`, `@drilldown-mt`, `@explore-ops`, `@explore-mt`, `@regression`, `@comprehensive-ui`) or create a new one.

3. **Add shard to CI** -- if new tag, add to `matrix.shard` in `.github/workflows/ci.yaml` under `e2e-ui`: `{name: "my-shard", command: "--grep @my-shard"}`.

4. **Run locally**: `cd test/e2e-ui && npx playwright test --grep @my-shard` (or `npm run test:headed` for visible browser).

## Adding a Semantics Matrix Case

1. **Add case** to `test/e2e-compat/query-semantics-matrix.json` with fields: `id`, `family`, `endpoint` (`query` or `query_range`), `query`, `expectation`, `expect_result_type`, `compare`, `require_non_empty`.

2. **Register in operations** -- add the case ID to the appropriate operation in `test/e2e-compat/query-semantics-operations.json`, or create a new operation entry with `name`, `category`, and `cases` array.

3. **Verify**: `go test -v -tags=e2e -run '^TestQuerySemanticsMatrix$' ./test/e2e-compat/`

## Proxy Variants

| Port | Service | Label Style | Metadata Mode | Purpose |
|------|---------|-------------|---------------|---------|
| 13100 | loki-vl-proxy | underscores | translated | Primary proxy: indexed label-values cache, L2 disk cache, L3 static peer ring with peer-a/peer-b |
| 13102 | loki-vl-proxy-underscore | underscores | hybrid | OTel dot-to-underscore, structured metadata; backs the Grafana `Loki (via VL proxy)` datasources |
| 13103 | loki-vl-proxy-tail | underscores | translated (default) | Synthetic tail mode, browser origin allowlist |
| 13105 | loki-vl-proxy-tail-native | underscores | translated (default) | Native VL tail mode |
| 13106 | loki-vl-proxy-native-metadata | underscores | native | Native metadata field mode |
| 13107 | loki-vl-proxy-translated-metadata | underscores | translated | Translated-only metadata aliases |
| 13108 | loki-vl-proxy-no-metadata | underscores | translated | Structured metadata emission disabled |
| 13109 | loki-vl-proxy-vmauth | underscores | translated | Backend routed through vmauth |
| 13110 | loki-vl-proxy-patterns-autodetect | underscores | hybrid | Patterns autodetect from queries; Grafana default datasource |
| 13150 | loki-vl-proxy-peer-a | underscores | translated | L3 peer ring member (zone-a) |
| 13151 | loki-vl-proxy-peer-b | underscores | translated | L3 peer ring member (zone-b) |

`tail-ingress` (nginx, port 13104) sits in front of `loki-vl-proxy-tail` for WebSocket ingress tests.

## CI Integration

### E2E Compat Groups (ci.yaml)

5 parallel groups under `e2e-compat-group`, each on a fresh stack; the `e2e-compat` job aggregates them:

| Group | Coverage |
|-------|----------|
| `core` | `TestCompat_*`, `TestExtended_*`, `TestChaining_*`, `TestAlertingCompat_*`, Explore HTTP contracts, `TestLokiFunctions_*`, datasource catalog, pinned matrix vs compose |
| `drilldown` | `TestDrilldown_*`, Drilldown cluster/level filter features, Loki/Drilldown/VL track scores |
| `otel-edge` | OTel labels, structured metadata, underscore proxy, label dedup/translation, `TestEdge_*`, `TestComplex_*` |
| `tail-multitenancy` | Multitenancy, tail modes, security headers, metrics, gzip, derived fields, concurrent/edge queries |
| `semantics` | Query semantics matrix, operations inventory, `TestLogQL_Exhaustive_*`, `TestPipeline_*`, range metric compatibility, Grafana clickout, missing ops |

Some `test/e2e-compat` tests match no group pattern (for example `TestOperationsMatrix_*` and `TestPerf_*`); see [Testing](testing.md#e2e-compatibility-matrix) for the list.

### Playwright Shards (ci.yaml)

9 parallel shards under `e2e-ui`, each starting the stack with `docker compose --profile ui up -d --no-build`:

| Shard | Command |
|-------|---------|
| `datasource` | `tests/datasource.spec.ts` |
| `explore-core` | `--grep @explore-core` |
| `explore-tail` | `--grep @explore-tail` |
| `drilldown-core` | `--grep @drilldown-core` |
| `drilldown-multitenant` | `--grep @drilldown-mt` |
| `explore-ops` | `--grep @explore-ops` |
| `explore-mt` | `--grep @explore-mt` |
| `explore-regression` | `--grep @regression` |
| `explore-comprehensive` | `--grep @comprehensive-ui` |

### Compatibility Workflows

`compat-loki.yaml`, `compat-drilldown.yaml` and `compat-vl.yaml` run pinned score jobs (`loki-pinned`, `drilldown-pinned-runtime`, `vl-pinned`) on pull requests and pushes, and weekly matrices over the versions in `test/e2e-compat/compatibility-matrix.json`. The Loki matrix (`cron: 15 3 * * 1`) enforces a 100% Loki compatibility score.

## Debugging

**Grafana UI** -- open http://localhost:3002 (anonymous admin, no login). Eleven datasources are provisioned from `test/e2e-compat/grafana-datasources.yaml`: `Loki (direct)`, eight proxy-backed Loki datasources (`Loki (via VL proxy)`, multi-tenant, native metadata, live tail, ingress tail, live tail native, patterns autodetect, vmauth), `VictoriaLogs (direct)`, and `VictoriaMetrics`.

**Docker logs** (compose service names):
```bash
cd test/e2e-compat
docker compose logs loki-vl-proxy              # main proxy
docker compose logs loki-vl-proxy-underscore   # underscore variant
docker compose logs loki                       # reference Loki
docker compose logs -f victorialogs            # follow VL logs
```

**Re-run a single test**:
```bash
go test -v -tags=e2e -count=1 -run '^TestCompat_QueryRange_LogQuery$' ./test/e2e-compat/
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
curl -s http://127.0.0.1:13100/ready      # proxy
curl -s http://127.0.0.1:13101/ready      # loki
curl -s http://127.0.0.1:19428/health     # victorialogs
curl -s http://127.0.0.1:3002/api/health  # grafana
```

## Version Overrides

Set environment variables before `docker compose up` to override image versions:

| Variable | Default |
|----------|---------|
| `LOKI_IMAGE` | `grafana/loki:3.7.1` |
| `VICTORIALOGS_IMAGE` | `victoriametrics/victoria-logs:v1.50.0` |
| `GRAFANA_IMAGE` | `grafana/grafana:13.0.1` |
| `PROXY_IMAGE` | `loki-vl-proxy:e2e-local` |
| `VMAUTH_IMAGE` | `victoriametrics/vmauth:v1.138.0` |
| `VMALERT_IMAGE` | `victoriametrics/vmalert:v1.138.0` |
| `VICTORIAMETRICS_IMAGE` | `victoriametrics/victoria-metrics:v1.119.0` |

```bash
LOKI_IMAGE=grafana/loki:3.6.0 docker compose up -d --build
```

`TestPinnedCompatibilityMatrixMatchesCompose` compares the Loki, VictoriaLogs and Grafana image defaults written in `docker-compose.yml` with `compatibility-matrix.json`; runtime overrides do not change what it checks.
