# Pipeline E2E and Internal Stress Tests — Design

**Goal:** Catch OOM regressions, query routing bugs, and internal concurrency races through a live-stack pipeline test and in-process stress tests.

**Architecture:** Two independent test layers — a GHA-integrated live e2e suite that drives real HTTP traffic through the full LogQL→proxy→VL stack, and a Go stress suite tagged `//go:build stress` that hammers internal processing paths concurrently under the race detector.

**Tech Stack:** Go `testing` package, `go test -tags=stress -race`, docker-compose (subset), GHA path-filtered workflow.

---

## Part 1: Live E2E Pipeline Tests

### Location

`test/e2e-pipeline/` — new package, separate from the existing `test/e2e-compat/` suite.

### GHA Workflow

New file `.github/workflows/e2e-pipeline.yml`:
- Trigger: `push` and `pull_request` to `main`, path-filtered to `internal/**`, `test/e2e-pipeline/**`, `docker-compose*.yml`
- Spins up a minimal compose: VictoriaLogs v1.49.0 + single proxy variant (`:3100`) — no Grafana, no 10-variant stack
- Readiness: `scripts/ci/wait_e2e_stack.sh 120`
- Run: `go test -tags=e2e -timeout=180s -v ./test/e2e-pipeline/...`

### Compose Stack

New `test/e2e-pipeline/docker-compose.yml`:
```
victorialogs:  v1.49.0 on :9428
loki-vl-proxy: loki-vl-proxy:ci (built from HEAD in GHA)
```
No Loki, no Grafana — this is a proxy pipeline test, not a parity test.

### Data Injection

10,000 log lines across 5 services: `api-gw`, `auth`, `worker`, `cache`, `db`.

Mix of formats:
- JSON lines with fields: `status` (int), `duration_ms` (float), `trace_id` (string), `level` (string)
- logfmt lines with fields: `method`, `path`, `latency`, `err`

Labels per stream: `service_name`, `namespace="prod"`, `env="e2e"`.

Push pattern: reuse `pushStream()` from `test/e2e-compat/testdata.go` — adapted to target VL directly. Each service gets 2000 lines distributed across 5-minute windows so `query_range` returns meaningful samples.

### Query Battery

Each query is fired at `http://localhost:3100` (the proxy). Assertions: HTTP 200, `status: "success"`, correct `data.resultType`, `data.result` non-empty, no top-level `"error"` key.

| Query | Endpoint | resultType | What it tests |
|-------|----------|-----------|---------------|
| `count_over_time({service_name="api-gw"}[5m])` | `query_range` | `matrix` | OOM path in `proxyStatsQueryRangeDirect` |
| `rate({namespace="prod"}[1m])` | `query_range` | `matrix` | Rate translation + aggregation |
| `sum_over_time({service_name="worker"}\|logfmt\|unwrap latency[5m])` | `query_range` | `matrix` | Unwrap fast-path routing |
| `{service_name=~".+"}` | `series` | — | Stream detection |
| `{service_name="worker"}` | `detected_fields` | — | Field exposure pipeline |
| `{service_name="cache"}` | `patterns` | — | Pattern clustering path |
| `sum by (service_name) (count_over_time({namespace="prod"}[5m]))` | `query_range` | `matrix` | Cross-service aggregation |

### Failure Signals

- HTTP non-200 → routing or translation bug
- Empty `data.result` → data not reaching VL or query mistranslated
- Process OOM (exit 137) → regression in body size limits
- Timeout → coalescer deadlock or slow path regression

---

## Part 2: Internal Stress Tests

### Location

Three new files in `internal/proxy/`:
- `translation_stress_test.go`
- `coalesce_stress_test.go`
- `range_metric_stress_test.go`

All tagged `//go:build stress` — excluded from normal `go test ./...`.

### GHA Integration

New job `stress-tests` added to `.github/workflows/ci.yaml`, runs after the unit test job:

```yaml
- name: Stress tests (race detector)
  run: go test -tags=stress -race -parallel=16 -timeout=120s ./internal/proxy/
```

The `-race` detector is the primary value — it catches data races that unit tests miss.

### translation_stress_test.go

50 goroutines × 200 iterations each. Each goroutine calls `TranslateLogQL` and `TranslateLogQLWithStreamFields` with a rotating set of realistic LogQL queries:
- `sum_over_time({app="x"}|logfmt|unwrap duration_ms[5m])`
- `rate({namespace="prod"}[1m])`
- `label_replace(count_over_time({app="x"}[5m]), "dest", "$1", "src", "(.*)")`
- Binary ops: `rate(...) / rate(...)`

Assertions:
- No panics
- Same input always produces same output (stability check via map)
- No races detected by `-race`

### coalesce_stress_test.go

Two sub-tests:

**Same-key storm:** 50 goroutines fire identical query key concurrently against a mock VL backend. Asserts:
- Upstream backend called ≤ 3 times (singleflight deduplication works)
- All 50 goroutines receive identical response bodies

**Mixed-key load:** 50 goroutines each with a unique query key, fired concurrently. Asserts:
- Each goroutine receives its own response (no cross-contamination)
- No goroutine leaks (all complete within timeout)
- No races under `-race`

Mock VL backend: `httptest.NewServer` with configurable delay (5ms) to simulate realistic upstream latency.

### range_metric_stress_test.go

30 goroutines concurrently exercising `collectRangeMetricHits` and `collectRangeMetricSamples` with in-memory generated stats responses (no real VL needed — use `bytes.NewReader` with realistic JSON payloads).

Response payloads:
- 500-series stats response (~2MB) for `collectRangeMetricHits`
- 10k-line log stream for `collectRangeMetricSamples`

Assertions:
- Result counts are deterministic across goroutines (same input → same count)
- No races under `-race`
- No allocations leaking across goroutine boundaries

---

## File Structure

```
test/e2e-pipeline/
  docker-compose.yml          # minimal VL + proxy
  pipeline_test.go            # test suite: inject + query battery
  testdata.go                 # data injection helpers (adapted from e2e-compat)

internal/proxy/
  translation_stress_test.go
  coalesce_stress_test.go
  range_metric_stress_test.go

.github/workflows/
  e2e-pipeline.yml            # new: live pipeline GHA workflow
  ci.yaml                     # modified: add stress-tests job
```

---

## Success Criteria

- Live e2e: all 7 queries return HTTP 200, non-empty results, correct resultType
- Stress: `go test -tags=stress -race` exits 0 with no race reports
- CI gate: e2e-pipeline workflow required status check on PRs touching `internal/**`
- OOM regression: if `proxyStatsQueryRangeDirect` body limit is removed, the test with 10k lines + aggregation will reproduce the OOM
