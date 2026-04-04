# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.19.0] - 2026-04-04

### Performance

- **Buffer pool for JSON encoding**: `marshalJSON()` uses `sync.Pool`-backed `bytes.Buffer` (64KB cap) for all JSON response encoding, reducing allocations across all handlers
- **Pooled response body reads**: `readBodyPooled()` reuses buffers for `io.ReadAll` paths
- **GOGC=200**: Helm chart sets `GOGC=200` (halves GC frequency for proxy workloads with short-lived allocations)
- **sync.Pool for NDJSON**: Entry maps pooled and reused across log line parsing (49% less memory)
- **Connection pool tuning**: `MaxIdleConnsPerHost=256` (was Go default 2), prevents port exhaustion at high concurrency
- **Results**: 39K req/s at 200 concurrent (no-cache), up from 8K before optimizations (+388% total improvement)

### Features

- **Complete Helm chart**: 11 templates — deployment, service, HPA, PDB, ServiceMonitor, ingress, HTTPRoute (Gateway API), NetworkPolicy, PVC, headless service for peer cache
- **GOMEMLIMIT auto-calc**: Calculates GOMEMLIMIT as configurable % of `resources.limits.memory` (default 70%)
- **Go runtime metrics**: `/metrics` exposes `go_memstats_alloc_bytes`, `go_memstats_sys_bytes`, `go_goroutines`, `go_gc_cycles_total`
- **Peer cache design**: Architecture doc for distributed L1.5 cache across replicas via headless service + consistent hashing

### Security

- **Rate limit bypass fixed**: `ClientID()` uses `RemoteAddr` (not spoofable `X-Forwarded-For`)
- **SECURITY.md**: Vulnerability reporting policy, security features documented
- **CONTRIBUTING.md**: Development workflow, code style, areas for contribution
- **Issue templates**: Bug report and feature request templates

### Tests

- 529 total tests (30 new coverage gap tests covering admin stubs, fallback paths, VL error propagation, label round-trips, Unicode, metrics recording)
- Performance regression tests: connection pool, memory leak detection, buffer pool safety
- CI benchmark job with artifact upload and regression gate

## [0.18.0] - 2026-04-04

### Security Fixes

- **P0: Cross-tenant data exposure in 7 handlers**: `handleSeries`, `handleIndexStats`, `handleVolume`, `handleVolumeRange`, `handleDetectedFields`, `handleDetectedFieldValues`, `handlePatterns` were missing `withOrgID(r)` — tenant headers never forwarded to VL
- **P0: Cross-tenant cache leak**: Cache keys for `/labels` and `/label/values` did not include `X-Scope-OrgID` — Tenant A's cached response could be served to Tenant B
- **P0: Rate limit bypass via X-Forwarded-For**: `ClientID()` trusted raw attacker-controlled `X-Forwarded-For` header. Now uses `RemoteAddr` (connection-level, not spoofable) with port stripping for consistent bucketing

### Bug Fixes

- **P1: `handleReady` nil dereference**: When VL is unreachable (`err != nil`), `resp` is nil but `resp.StatusCode` was accessed — panic in production. Now checks `err` first, defers `resp.Body.Close()`
- **P1: `ForwardHeaders` dead code**: Config field stored but never used in `applyBackendHeaders`. Now threads original request via context and copies configured headers to VL requests
- **P2: `handleSeries` swallows VL errors**: Always returned 200 with empty data on backend errors. Now propagates VL error status codes
- **P2: Goroutine leak in `cleanupStaleClients`**: No shutdown mechanism — leaked on config reload. Added `Stop()` method with `done` channel
- **P2: `containsWithoutClause` false positive on escaped quotes**: `\"` inside strings incorrectly toggled quote state. Now handles backslash escapes

### Tests

- 9 new tenant scoping tests (7 handler tests + 2 cache isolation tests)
- Updated `ClientID` tests for security change (XFF ignored, port stripped)
- 471 total tests passing

## [0.17.0] - 2026-04-04

### Security Fixes

- **P0: Tenant map reload data race**: `forwardTenantHeaders` now holds `configMu.RLock` when reading `tenantMap`, preventing data race on SIGHUP reload
- **P0: `without()` clause silent wrong behavior**: `without()` was silently treated as `by()` producing incorrect aggregation results. Now returns a clear error directing users to use `by()` with explicit labels

### Bug Fixes

- **Binary operators**: `applyOp` now handles `%` (modulo), `^` (power), and comparison operators (`==`, `!=`, `>`, `<`, `>=`, `<=`) — previously these silently returned the left operand
- **CB metrics mismatch**: Circuit breaker `State()` returns `"half_open"` but metrics matched `"half-open"` — half-open gauge never showed value 2. Now accepts both forms
- **`targetLabels` on volume_range**: `handleVolumeRange` now forwards the `targetLabels` param as VL `field` (was missing, only `/volume` had it)
- **`IsScalar` negative/scientific**: `IsScalar` now uses `strconv.ParseFloat` — supports `-1`, `1e5`, `1.5e-3` (previously only digits and dots)

### Features

- **Delete API endpoint**: `/loki/api/v1/delete` added as exception to read-only proxy with 7 safeguards: POST-only, `X-Delete-Confirmation` header, non-wildcard query, time range required, 30-day max range, tenant scoping, audit logging at WARN level

### Documentation

- **Restructured docs**: README slimmed to project summary + architecture, content moved to categorized files:
  - `docs/architecture.md` — component design, data flow, protection layers
  - `docs/configuration.md` — all flags, env vars, cache, tenancy, TLS, OTLP
  - `docs/api-reference.md` — endpoint table, delete safeguards, metrics
  - `docs/translation-reference.md` — LogQL to LogsQL mapping, supported/unsupported
  - `docs/testing.md` — test categories, running tests, fuzz testing
  - `docs/roadmap.md` — completed and planned features
- **Updated KNOWN_ISSUES.md** with v0.17.0 fixes

### Tests

- 50+ new tests: concurrent tenant reload (race detector), all binary operators (18 cases), delete safeguards (8 cases), CB metrics, `IsScalar`, `without()` clause detection

## [0.16.0] - 2026-04-04

### Security Fixes

- **P0: Coalescer tenant data leak**: `RequestKey()` now includes `X-Scope-OrgID` in the hash, preventing cross-tenant response sharing via singleflight coalescing
- **P0: isStatsQuery false-positive**: Rewrote stats detection to skip quoted regions — `|= "stats query"` no longer triggers stats handler routing
- **P0: Metrics always recording 200**: `handleQueryRange` and `handleQuery` now capture actual HTTP status codes via `statusCapture` wrapper. VL error responses (4xx/5xx) are propagated to clients.

### Features

- **Direction parameter**: `proxyLogQuery` reads Loki's `direction` param and appends VL's `| sort by (_time)` or `| sort by (_time desc)` accordingly
- **Labels query param**: `/labels` endpoint now translates and forwards the `query` param to scope label suggestions in Grafana Explore
- **`without()` grouping**: `extractOuterAggregation` now accepts `without()` alongside `by()` in outer aggregation clauses
- **Additional outer aggregations**: `stddev`, `stdvar`, `sort`, `sort_desc` added to the aggregation regex
- **`label_format` multi-rename**: `| label_format a="{{.x}}", b="{{.y}}"` now generates separate `| format` pipes for each assignment
- **`quantile_over_time`**: Maps to VL's `quantile(phi, field)` stats function
- **`absent_over_time`**: Maps to VL's `count()`
- **Stats response label translation**: Dotted VL label names in metric responses are now translated to underscore format
- **VL error message mapping**: `wrapAsLokiResponse` detects VL error formats and translates to Loki's `{"status":"error","error":"..."}` format
- **Admin endpoint stubs**: `/loki/api/v1/rules`, `/loki/api/v1/alerts`, `/config` for Grafana Alerting ruler mode compatibility
- **Half-open circuit breaker fix**: Half-open state now limits probe requests to `successThreshold` count instead of allowing all
- **`convertGoTemplate` dotted fields**: `{{.service.name}}` now correctly maps to `<service.name>`
- **`unwrap` conversion wrappers**: `| unwrap duration(field)` and `| unwrap bytes(field)` now strip the wrapper and extract the field name
- **Extended binary operators**: `%`, `^`, `==`, `!=`, `>`, `<`, `>=`, `<=` added to binary metric expression parsing

### Testing

- **Playwright UI e2e framework**: Full Grafana UI test suite via Playwright — Explore, drill-down, label navigation, error handling, side-by-side proxy vs Loki comparison
- 395+ unit tests (translator 92.2%, middleware 91.8%, cache 88.4%)
- 8 new e2e compat tests for direction, labels query param, quantile_over_time, tenant isolation, error propagation, without() grouping, label_format multi-rename

## [0.15.0] - 2026-04-04

### Features

- **`/loki/api/v1/patterns` — real implementation**: Proxy-side drain-like pattern extraction. Queries VL for log lines, tokenizes to patterns (replaces IPs, numbers, UUIDs, timestamps with `<_>`), groups by pattern, returns sorted by frequency. Handles both structured (JSON) and unstructured log formats.

### Tests

- 349 unit tests, 80+ e2e tests
- Pattern extraction unit tests (tokenize, isVariable, extractLogPatterns)
- E2e: patterns endpoint with real data, Loki vs proxy comparison

### Zero Gaps

All Loki API endpoints are now fully implemented. No stubs remain.

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
