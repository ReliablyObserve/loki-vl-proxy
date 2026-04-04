# Testing

## Quick Start

```bash
# Unit tests (460+ tests)
go test ./...

# With race detector
go test -race ./...

# E2E compatibility tests (requires docker-compose)
cd test/e2e-compat
docker-compose up -d --build
go test -v -tags=e2e -timeout=120s ./test/e2e-compat/

# Track-specific scores
go test -v -tags=e2e -run '^TestLokiTrackScore$' ./test/e2e-compat/
go test -v -tags=e2e -run '^TestDrilldownTrackScore$' ./test/e2e-compat/
go test -v -tags=e2e -run '^TestVLTrackScore$' ./test/e2e-compat/

# Playwright UI tests (Grafana Explore, drill-down, error handling)
cd test/e2e-ui
npm install && npx playwright install chromium
npm test

# macOS fallback: run the same UI tests inside Linux Playwright
docker run --rm \
  -v "$(pwd)/test/e2e-ui:/work" \
  -w /work \
  -e GRAFANA_URL=http://host.docker.internal:3002 \
  mcr.microsoft.com/playwright:v1.52.0-jammy \
  /bin/bash -lc 'npm ci && npx playwright test --grep "Grafana Logs Drilldown"'

# Build binary
go build -o loki-vl-proxy ./cmd/proxy
```

## Test Coverage by Category

| Category | Tests | What they verify |
|---|---|---|
| Loki API contracts | 30 | Exact response JSON structure per Loki spec |
| LogQL translation (basic) | 30 | Stream selectors, line filters, parsers, label filters |
| LogQL translation (advanced) | 22 | Metric queries, unwrap, topk, sum by, complex pipelines |
| Query normalization | 8 | Canonicalization for cache keys |
| Cache behavior | 6 | Hit/miss/TTL/eviction/protection |
| Multitenancy | 4 | String->int mapping, numeric passthrough, unmapped default |
| WebSocket tail | 2 | Query validation, WebSocket frame structure |
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
| `test/e2e-ui/` | Playwright browser tests against Grafana |

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

Support window policy:

- Loki: current minor family plus one minor behind
- Logs Drilldown: current family plus one family behind
- VictoriaLogs: `v1.3x.x` and `v1.4x.x`

The stack is version-parameterized through compose environment variables:

```bash
LOKI_IMAGE=grafana/loki:3.6.10 \
VICTORIALOGS_IMAGE=victoriametrics/victoria-logs:v1.48.0 \
GRAFANA_IMAGE=grafana/grafana:12.4.2 \
docker compose -f test/e2e-compat/docker-compose.yml up -d --build
```

GitHub Actions uses the same manifest as the source of truth. The compatibility workflows load their version matrices from [compatibility-matrix.json](/tmp/Loki-VL-proxy/test/e2e-compat/compatibility-matrix.json) instead of duplicating version lists in workflow YAML.

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
