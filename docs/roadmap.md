# Roadmap

## Completed

- [x] LogQL -> LogsQL translation (stream selectors, line filters, parsers, metric queries)
- [x] Response format conversion (VL NDJSON -> Loki streams, VL stats -> Prometheus matrix/vector)
- [x] Request coalescing (singleflight: N queries -> 1 backend request)
- [x] Rate limiting (per-client token bucket + global concurrency)
- [x] Circuit breaker (closed->open->half-open with configurable thresholds)
- [x] Query normalization (sort matchers, collapse whitespace for cache keys)
- [x] Tiered cache (per-endpoint TTLs, L1 in-memory, L2 bbolt on-disk)
- [x] Multitenancy (string->int tenant mapping, numeric passthrough, SIGHUP reload)
- [x] WebSocket tail (`/loki/api/v1/tail` -> VL NDJSON streaming)
- [x] L2 disk cache with gzip compression, AES-256 encryption, write-back buffer
- [x] OTLP telemetry push (gzip/zstd compression, TLS)
- [x] HTTP hardening (timeouts, body limits, security headers)
- [x] Index stats, volume, volume_range via VL `/select/logsql/hits`
- [x] Query fingerprinting + analytics (`/debug/queries`)
- [x] Auto-warming cache for top-N queries
- [x] Graceful HTTP server shutdown (SIGTERM/SIGINT)
- [x] Grafana datasource config (maxLines, basic auth, TLS, header forwarding)
- [x] Derived fields (regex extraction for trace linking)
- [x] Chunked streaming (Transfer-Encoding: chunked for large results)
- [x] OTel label translation (bidirectional dot<->underscore for 50+ fields)
- [x] Custom field remapping (`-field-mapping`)
- [x] Per-tenant metrics (request rate, latency, error rate by X-Scope-OrgID)
- [x] Client error breakdown (bad_request, rate_limited, not_found, body_too_large)
- [x] Grafana dashboard & alerting rules (Helm PrometheusRule CR)
- [x] Write safeguard (`/push` blocked with 405)
- [x] `| decolorize` proxy-side ANSI stripping
- [x] `| ip("CIDR")` proxy-side IP range filtering
- [x] `| line_format` full Go templates
- [x] pprof, SIGHUP reload, rate limit headers
- [x] Per-endpoint cache/backend metrics, CB state gauge
- [x] Fuzz testing (1.2M+ executions, no panics)
- [x] Nested binary metric queries (`sum(rate(...)) / sum(rate(...))`)
- [x] `/loki/api/v1/patterns` proxy-side drain-like pattern extraction
- [x] `direction` parameter (forward/backward sort)
- [x] `quantile_over_time()` mapped to VL quantile
- [x] `label_format` multi-rename (comma-separated)
- [x] Extended binary ops (`%`, `^`, `==`, `!=`, `>`, `<`, `>=`, `<=`)
- [x] Admin endpoint stubs (`/rules`, `/alerts`, `/config`)
- [x] Playwright UI e2e tests
- [x] Delete API endpoint with safeguards (confirmation, tenant scoping, audit logging)
- [x] `without()` clause detection and clear error message
- [x] `IsScalar` supports negative and scientific notation
- [x] Circuit breaker half-open metrics fix
- [x] Tenant map reload race condition fix

- [x] `bool` modifier on comparison operators — stripped at translation (applyOp returns 1/0 for all comparisons)
- [x] Field-specific parser `| json field1, field2` / `| logfmt field1, field2` — maps to full unpack (VL extracts all fields)
- [x] Backslash-escaped quotes in stream selectors — findMatchingBrace handles `\"`
- [x] Binary expression detection before metric query (fixes `rate(...) > 0` being misrouted)
- [x] Peer cache design doc + headless service Helm template
- [x] Performance: 39K req/s (buffer pools, sync.Pool, GOGC=200, connection pool)
- [x] Complete Helm chart: 11 templates, GOMEMLIMIT auto-calc, HTTPRoute
- [x] 542 tests, CI bench job, regression gates

## Planned

- [ ] `on()`/`ignoring()`/`group_left()`/`group_right()` vector matching
- [ ] `@` timestamp modifier
- [ ] `unwrap duration()/bytes()` unit conversion post-processing (value conversion, not stripping)
- [ ] Subquery syntax `rate(...)[1h:5m]`
- [ ] LRU cache eviction (replace random eviction)
- [ ] Peer cache Phase 1 implementation (DNS discovery + peer fetch)
- [ ] System metrics in /metrics (CPU, memory, IO, network via /proc)
- [ ] Native VL stream selector optimization for known `_stream_fields`
