# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.0] - 2026-04-04

### Features

- **Nested metric queries**: Proxy-side binary evaluation for `sum(rate(...)) / sum(rate(...))`, scalar ops (`rate(...) * 100`), and matching time series point-by-point arithmetic
- **Comprehensive e2e chaining tests**: 40+ tests covering parser→filter, multi-filter, parser→drop/keep, parser→line_format, filter→decolorize, metric queries (rate, count, bytes, sum by, topk), binary metric queries, all endpoints, security headers, write blocking

### Tests

- 340 unit tests, 80+ e2e tests
- E2e test coverage: json+logfmt parsers, label filters (==, !=, =~, !~, >=), line filters (|=, !=, |~, !~), decolorize, ip() filter, line_format templates, metric queries (rate, count_over_time, bytes_over_time, sum by, topk), binary expressions (/, *, +, -), format_query, detected_labels, push blocked, buildinfo, ready, metrics, patterns, security headers, all API endpoints

## [0.13.0] - 2026-04-04

### Features

- **`| decolorize`**: Proxy-side ANSI escape stripping (Loki parity, ready for VL native replacement)
- **`| ip("CIDR")`**: Proxy-side IP range filtering with `net.ParseCIDR` (Loki parity)
- **`| line_format` full templates**: Go `text/template` with ToUpper, ToLower, default, TrimSpace, etc.
- **`/loki/api/v1/format_query`**: Returns query as-is (client-side formatting)
- **`/loki/api/v1/detected_labels`**: Stream-level labels endpoint (Loki 3.x)
- **Write safeguard**: `/loki/api/v1/push` returns 405 (read-only proxy)

### Tests

- 340 unit tests, 60+ e2e tests
- New e2e: decolorize, ip() filter, line_format, format_query, detected_labels, write blocked, buildinfo, ready, metrics validation, patterns
- Fuzz testing: 1.2M+ executions, no panics

## [0.12.0] - 2026-04-04

### Features

- **Write endpoint safeguard**: `/loki/api/v1/push` returns 405 — read-only proxy
- **`/loki/api/v1/format_query`**: Returns query as-is (client-side formatting)
- **`/loki/api/v1/detected_labels`**: Stream-level labels (Loki 3.x compat)
- **`| decolorize` pipe**: Proxy-side ANSI escape stripping (ready for VL native replacement)
- **`| ip()` filter**: Marked for proxy-side CIDR matching (ready for VL native replacement)
- **pprof endpoint**: `/debug/pprof/` for production profiling
- **SIGHUP config reload**: Hot-reload tenant-map and field-mapping without restart
- **Rate limit headers**: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `Retry-After` on 429
- **Enhanced `/ready`**: Checks VL health + circuit breaker state
- **Per-endpoint cache metrics**: `loki_vl_proxy_cache_hits_by_endpoint{endpoint}`
- **Backend latency histogram**: `loki_vl_proxy_backend_duration_seconds{endpoint}`
- **Singleflight stats**: `loki_vl_proxy_coalesced_total`, `loki_vl_proxy_coalesced_saved_total`
- **Circuit breaker gauge**: `loki_vl_proxy_circuit_breaker_state` (0=closed, 1=open, 2=half-open)
- **PrometheusRule CR**: Helm template with 7 alert rules for Prometheus Operator
- **Grafana dashboard ConfigMap**: Auto-provisioned via Helm with `grafana_dashboard: "1"` label
- **Fuzz tests**: LogQL translator fuzz testing (1.2M+ executions, no panics)

### Tests

- 314 unit tests passing (up from 263)
- Fuzz seed corpus: 30+ valid/malformed/adversarial LogQL inputs

## [0.11.0] - 2026-04-04

### Features

- **OTel label translation**: Bidirectional dot↔underscore conversion for 50+ OTel semantic convention fields
  - `-label-style=underscores` converts VL dotted names (service.name) to Loki underscores (service_name)
  - `-label-style=passthrough` (default) passes VL field names as-is
  - Query direction: `{service_name="x"}` → VL `"service.name":"x"` with automatic field quoting
  - Response direction: all 7 response paths translated (labels, label_values, detected_fields, series, query results, streaming, tail)
- **Custom field remapping**: `-field-mapping` JSON config for arbitrary VL↔Loki field name mappings
- **Per-tenant metrics**: `loki_vl_proxy_tenant_requests_total{tenant,endpoint,status}` and latency histograms
- **Client error breakdown**: `loki_vl_proxy_client_errors_total{endpoint,reason}` — bad_request, rate_limited, not_found, body_too_large
- **Request logging middleware**: Structured JSON log per request with tenant, query, status, duration, client IP
- **Tenant wildcard**: `X-Scope-OrgID: "*"` or `"0"` skips tenant headers for single-tenant VL setups
- **Grafana dashboard**: Pre-built importable dashboard (`examples/grafana-dashboard.json`) with tenant breakdown
- **Alerting rules**: 16 Prometheus/VM alert rules (`examples/alerting-rules.yaml`) including per-tenant abuse detection
- **Operations guide**: SRE documentation (`docs/operations.md`) — capacity planning, perf tuning, troubleshooting

### Tests

- 44 new unit tests for label translation (SanitizeLabelName, LabelTranslator, TranslateLogQLWithLabels)
- 50+ OTel e2e compatibility tests across 12 scenarios (dots→underscores, passthrough, mixed, query direction, Drilldown)
- Docker-compose: added `loki-vl-proxy-underscore` service at :3102 for e2e label translation tests
- **263 unit tests, 50+ e2e tests** — all passing

## [0.6.0] - 2026-04-03

### Features

- **Loki-VL-proxy**: HTTP proxy translating Loki API to VictoriaLogs
- **LogQL to LogsQL translator**: stream selectors, line filters (substring semantics),
  parsers (json, logfmt, pattern, regexp), label filters, metric queries (rate,
  count_over_time, bytes_over_time, sum by, topk), unwrap handling
- **Response converter**: VL NDJSON → Loki streams format, VL stats → Prometheus matrix/vector
- **Request coalescing**: singleflight deduplication — N identical concurrent queries → 1 backend request
- **Rate limiting**: per-client token bucket + global concurrent query semaphore
- **Circuit breaker**: opens after consecutive failures, auto-recovers via half-open probing
- **Query normalization**: sort label matchers, collapse whitespace for better cache hits
- **TTL cache**: per-endpoint TTLs (labels=60s, queries=10s), max 256MB, eviction tracking
- **Prometheus metrics**: `/metrics` endpoint with request counters, latency histograms, cache stats
- **JSON structured logs**: via Go's slog to stdout
- **Helm chart**: VictoriaMetrics-style with extraArgs, ServiceMonitor, security context
- **GHA CI/CD**: build, test, lint, Docker multi-arch, GitHub Release with binaries + checksums
- **Docker**: single static binary, ~10MB Alpine-based image

### Critical Fixes

- **Substring vs word matching**: Loki `|= "text"` is substring match; translated to VL `~"text"`
  (not `"text"` which is word-only). Without this fix, queries silently return fewer results.
- **Stream filter vs field filter**: Loki stream selectors `{level="error"}` converted to VL
  field filters `level:=error` (not stream filters which only match `_stream_fields`)
- **Parser + filter chains**: `| json | status >= 400` correctly becomes
  `| unpack_json | filter status:>=400` in VL
- **Regex quoting**: `namespace=~"prod|staging"` properly quoted as `namespace:~"prod|staging"`
- **Keep fields**: `| keep app, level` always includes `_time, _msg, _stream` for response building

### API Coverage

| Endpoint | Status |
|---|---|
| `/loki/api/v1/query_range` | Implemented (streams + matrix) |
| `/loki/api/v1/query` | Implemented |
| `/loki/api/v1/labels` | Implemented + cached |
| `/loki/api/v1/label/{name}/values` | Implemented + cached |
| `/loki/api/v1/series` | Implemented |
| `/loki/api/v1/detected_fields` | Implemented |
| `/loki/api/v1/index/stats` | Stub |
| `/loki/api/v1/index/volume` | Stub |
| `/loki/api/v1/index/volume_range` | Stub |
| `/loki/api/v1/patterns` | Stub |
| `/loki/api/v1/tail` | Not implemented |
| `/ready` | Implemented |
| `/loki/api/v1/status/buildinfo` | Implemented |
| `/metrics` | Implemented |

### Tests

- 126 unit tests (translator, proxy contracts, cache, middleware, normalization)
- 54 e2e tests (Loki vs proxy side-by-side comparison with compatibility scoring)
- 10 performance e2e tests (Loki direct vs proxy latency comparison)
- All at 100% compatibility

### Performance

Proxy is 40-77% faster than direct Loki for all query endpoints (VictoriaLogs is faster).
Cache provides 3.2x speedup on warm hits.

### Known Limitations

See [docs/KNOWN_ISSUES.md](docs/KNOWN_ISSUES.md) for full list:
- `/loki/api/v1/tail` WebSocket not implemented
- Volume API endpoints return stubs
- Multitenancy header mapping not implemented (Loki string org IDs vs VL numeric AccountID)
- Some LogQL features have no VL equivalent (decolorize, absent_over_time, subqueries)
