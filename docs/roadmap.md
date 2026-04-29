---
sidebar_label: Roadmap
description: Planned features, known gaps, and the contribution priority list for loki-vl-proxy.
---

# Roadmap

## Completed

- [x] LogQL -> LogsQL translation (stream selectors, line filters, parsers, metric queries)
- [x] Response format conversion (VL NDJSON -> Loki streams, VL stats -> Prometheus matrix/vector)
- [x] Request coalescing (singleflight: N queries -> 1 backend request)
- [x] Rate limiting (per-client token bucket + global concurrency)
- [x] Circuit breaker (closed->open->half-open with built-in defaults)
- [x] Query normalization (sort matchers, collapse whitespace for cache keys)
- [x] Tiered cache (per-endpoint TTLs, L1 in-memory, L2 bbolt on-disk)
- [x] Multitenancy (string->int tenant mapping, numeric passthrough, SIGHUP reload)
- [x] WebSocket tail (`/loki/api/v1/tail` -> VL NDJSON streaming)
- [x] L2 disk cache with gzip compression, write-back buffer (encryption via cloud provider)
- [x] OTLP telemetry push (gzip/zstd compression, TLS)
- [x] HTTP hardening (timeouts, body limits, security headers)
- [x] Index stats, volume, volume_range via VL `/select/logsql/hits`
- [x] Query fingerprinting + analytics (`/debug/queries`)
- [x] Graceful HTTP server shutdown (SIGTERM/SIGINT)
- [x] Grafana datasource config (maxLines, basic auth, backend timeout, TLS, header/cookie forwarding, optional listener mTLS)
- [x] Derived fields (regex extraction for trace linking)
- [x] Chunked streaming (Transfer-Encoding: chunked for large results)
- [x] OTel label translation (bidirectional dot&lt;-&gt;underscore for 50+ fields)
- [x] Custom field remapping (`-field-mapping`)
- [x] Per-tenant metrics (request rate, latency, error rate by X-Scope-OrgID)
- [x] Client error breakdown (bad_request, rate_limited, not_found, body_too_large)
- [x] Grafana dashboard & alerting rules (Helm PrometheusRule CR)
- [x] Write safeguard (`/push` blocked with 405)
- [x] `| decolorize` proxy-side ANSI stripping
- [x] `| ip("CIDR")` proxy-side IP range filtering
- [x] `| line_format` full Go templates
- [x] pprof, SIGHUP reload, and rate-limit response headers
- [x] Per-endpoint cache/backend metrics, CB state gauge
- [x] Fuzz testing (1.2M+ executions, no panics)
- [x] Nested binary metric queries (`sum(rate(...)) / sum(rate(...))`)
- [x] `/loki/api/v1/patterns` proxy-side Loki-style Drain tokenizer/clustering extraction
- [x] `direction` parameter (forward/backward sort)
- [x] `quantile_over_time()` mapped to VL quantile
- [x] `label_format` multi-rename (comma-separated)
- [x] Extended binary ops (`%`, `^`, `==`, `!=`, `>`, `<`, `>=`, `<=`)
- [x] Datasource compatibility handlers (`/rules`, `/alerts`, `/config`)
- [x] Playwright UI e2e tests
- [x] `/tail` browser and ops coverage (origin policy, native fallback, ingress recovery, upstream `401`/`403`/`5xx` parity)
- [x] Multi-tenant Explore and Logs Drilldown coverage for `__tenant_id__`, labels, series, and detected field/label browser/resource surfaces
- [x] Delete API endpoint with safeguards (confirmation, tenant scoping, audit logging)
- [x] `without()` clause detection and clear error message
- [x] `IsScalar` supports negative and scientific notation
- [x] Circuit breaker half-open metrics fix
- [x] Tenant map reload race condition fix
- [x] `group()` outer aggregation — inner metric translated normally, proxy normalises all values to `1` (v1.21.0)
- [x] `label_replace()` — proxy post-processing via marker suffix; Prometheus no-match semantics (v1.21.0)
- [x] `label_join()` — proxy post-processing via marker suffix; missing src labels skipped (v1.21.0)
- [x] `count_values()` — returns descriptive error (not translatable to VL; VL has no group-by-value primitive) (v1.21.0)
- [x] Circuit breaker sliding-window failure counting — 30s window replaces consecutive-failure model; sporadic slow-query resets no longer open the breaker (v1.18.0)
- [x] Bare label matcher error — `app="value"` (missing braces) returns HTTP 400 with descriptive Loki-style parse error instead of silently emitting malformed VL syntax (v1.20.0)
- [x] `detected_level` inference from `_msg` content — proxy infers level from JSON/logfmt log body when not present in stream labels; Drilldown volume API gains automatic parser unpacking (v1.20.0)
- [x] Deterministic log stream ordering for multi-window queries — streams sorted by canonical key, per-stream values sorted by timestamp before response emission (v1.21.1)

- [x] `bool` modifier on comparison operators — stripped at translation (applyOp returns 1/0 for all comparisons)
- [x] Field-specific parser `| json field1, field2` / `| logfmt field1, field2` — maps to full unpack (VL extracts all fields)
- [x] Backslash-escaped quotes in stream selectors — findMatchingBrace handles `\"`
- [x] Binary expression detection before metric query (fixes `rate(...) > 0` being misrouted)
- [x] Peer cache design doc + headless service Helm template
- [x] Performance-focused optimization pass (buffer pools, sync.Pool, connection-pool tuning, cache hot-path benchmarks)
- [x] Complete Helm chart: 11 templates, GOMEMLIMIT auto-calc, HTTPRoute
- [x] Broad test suite, CI bench job, and regression gates
- [x] Coverage and quality-gate reinforcement for runtime, middleware, cache, and proxy-path tests
- [x] Tier0 compatibility-edge cache with bounded memory budget, safe GET-only guardrails, and reload invalidation
- [x] Fleet shadow-copy validation for 3-peer cache reuse plus Tier0/fleet micro-benchmarks

## Planned

- [x] `on()`/`ignoring()`/`group_left()`/`group_right()` vector matching (v0.22.0)
- [x] `@` timestamp modifier (v0.19.0)
- [x] `unwrap duration()/bytes()` unit conversion (v0.21.0)
- [x] Subquery syntax `rate(...)[1h:5m]` — proxy-side evaluation (v0.23.0)
- [x] LRU cache eviction (v0.21.0)
- [x] Peer cache Phase 1 implementation (DNS discovery + peer fetch) (v0.24.0)
- [x] System metrics in /metrics (CPU, memory, IO, network via /proc) (v0.19.0)
- [x] Native VL stream selector optimization for known `_stream_fields` (v0.23.0)
- [x] PR quality report workflow with coverage, compatibility, and performance delta comments (v0.26.0)
- [ ] Tighten remaining merged-tenant Drilldown metadata accuracy for field and label cardinality surfaces
- [ ] Convert more upstream Loki, Logs Drilldown, and VictoriaLogs edge cases into regression tests
- [ ] Expand browser-level multi-tenant Explore and Drilldown scenarios where API parity already exists but UI combinations still need live regression coverage
- [x] Add bounded peer hot-read-ahead (top-N hot keys with per-interval key/byte/concurrency budgets, jitter, and tenant fairness) to improve non-owner local hit rates without causing peer traffic storms (v1.0.25)
- [x] Add regression/perf suite for collapse forwarding and hot-read-ahead interactions (owner/non-owner paths, coalescing efficiency, backend offload delta) (v1.0.25)
- [x] Promote compose-backed e2e fleet cache smoke coverage into required GitHub Actions for pull requests and post-merge `main` runs (v0.27.7)