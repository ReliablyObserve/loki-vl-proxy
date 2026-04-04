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

# Playwright UI tests (Grafana Explore, drill-down, error handling)
cd test/e2e-ui
npm install && npx playwright install chromium
npm test

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
| `test/e2e-ui/` | Playwright browser tests against Grafana |

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
