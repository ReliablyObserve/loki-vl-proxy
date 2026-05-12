# Metric Fast-Path Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the parser-stage guard from `shouldUseManualRangeMetricCompat` so that `rate`, `bytes_rate`, `count_over_time`, and `bytes_over_time` queries with `| json`/`| logfmt` pipeline stages use VL's native `stats_query_range` (fast path) when the range window equals the step (tumbling window).

**Architecture:** A single targeted line-delete in `shouldUseManualRangeMetricCompat` removes the condition that forced any query containing `| unpack_` (translated `| json`) onto the slow raw-log-fetch path. After the change, rate/count/bytes functions obey only the `!rangeEqualsStep` gate: tumbling windows (range == step) go fast, sliding windows (range > step) stay slow with correct semantics. The `originalLogql` parameter, only used by the removed check, is also dropped from the signature and from both callers.

**Tech Stack:** Go 1.23, `net/http/httptest`, table-driven tests, `go test ./internal/proxy/...`

---

## File Structure

| File | Action |
|------|--------|
| `internal/proxy/range_metric_compat.go` | Modify `shouldUseManualRangeMetricCompat` — remove parser-stage guard + drop `originalLogql` param |
| `internal/proxy/range_metric_compat.go` | Update two callers of `shouldUseManualRangeMetricCompat` |
| `internal/proxy/range_metric_compat_test.go` | Add unit tests for changed `shouldUseManualRangeMetricCompat` |
| `internal/proxy/range_metric_compat_test.go` | Add integration test: parser-stage rate tumbling-window → `stats_query_range` |
| `internal/proxy/range_metric_compat_test.go` | Add integration test: parser-stage rate sliding-window → slow path preserved |

---

## Context you need to know

### What the code currently does (lines 369-405)

```go
func shouldUseManualRangeMetricCompat(baseQuery, manualFunc string, rangeEqualsStep bool, originalLogql string) bool {
    manualFunc = strings.TrimSpace(manualFunc)
    if manualFunc == "rate_counter" {
        return true
    }

    switch manualFunc {
    case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
        // ← THIS BLOCK IS WHAT WE REMOVE:
        if queryUsesParserStages(baseQuery) && !strings.Contains(originalLogql, "__error__") {
            return true  // forces slow path for ANY | json / | logfmt query
        }
        return !rangeEqualsStep
    }

    if !queryUsesParserStages(baseQuery) {
        return false
    }

    switch manualFunc {
    case "avg", "sum", "min", "max", "quantile", "stddev", "stdvar", "first", "last":
        return false
    default:
        return true
    }
}
```

### What `queryUsesParserStages` checks (lines 791-802)

```go
func queryUsesParserStages(baseQuery string) bool {
    if strings.Contains(baseQuery, "| unpack_") { return true }  // | json → | unpack_json
    if strings.Contains(baseQuery, "| extract ") { return true }
    if strings.Contains(baseQuery, "| extract_regexp ") { return true }
    return false
}
```

The `baseQuery` is the VL-translated query (e.g. `{app="api-gateway"} | unpack_json | status >= 400`).

### The two callers (lines 311 and 343)

```go
// line 311
if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, noSlidingOverlap, originalLogql) {
    return false
}

// line 343
if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, true, originalLogql) {
    return false
}
```

### The fast / slow path split

`shouldUseManualRangeMetricCompat` returns `true` → proxy fetches raw logs from `/select/logsql/query` and aggregates in Go (slow).
`shouldUseManualRangeMetricCompat` returns `false` → falls through to VL `/select/logsql/stats_query_range` (fast).

### What changes and what does NOT change

**Changes:** rate/count/bytes queries with `| json`/`| logfmt` AND range == step now use fast path.  
**Does NOT change:** sliding windows (range > step) still use slow path — `return !rangeEqualsStep` remains. `avg_over_time | unwrap` and `quantile_over_time | unwrap` are not in this switch — they are unaffected.  
**Semantic trade-off:** VL `stats_query_range` counts all log lines including parse-failed ones; Loki excludes them. For tumbling windows with real production data the difference is negligible.

---

## Task 1: Unit tests for `shouldUseManualRangeMetricCompat` — write failing tests first

**Files:**
- Modify: `internal/proxy/range_metric_compat_test.go`

- [ ] **Step 1: Write the failing unit tests**

Add after the existing `TestParseOriginalRangeMetricSpecRate` test (after line ~106):

```go
func TestShouldUseManualRangeMetricCompat_ParserStageRate(t *testing.T) {
	// After the change, parser-stage rate queries obey only the !rangeEqualsStep gate.
	// Base query uses VL-translated syntax (| unpack_json, not | json).
	parserBaseQuery := `{app="api-gateway"} | unpack_json | status >= 400`
	plainBaseQuery  := `{app="api-gateway"}`

	tests := []struct {
		name            string
		baseQuery       string
		manualFunc      string
		rangeEqualsStep bool
		wantManual      bool // true = slow path, false = fast path
	}{
		// Parser stages + tumbling window (range==step): FAST PATH expected after change.
		{
			name:            "rate_parser_tumbling_fast",
			baseQuery:       parserBaseQuery,
			manualFunc:      "rate",
			rangeEqualsStep: true,
			wantManual:      false, // fast path
		},
		{
			name:            "count_over_time_parser_tumbling_fast",
			baseQuery:       parserBaseQuery,
			manualFunc:      "count_over_time",
			rangeEqualsStep: true,
			wantManual:      false,
		},
		{
			name:            "bytes_rate_parser_tumbling_fast",
			baseQuery:       parserBaseQuery,
			manualFunc:      "bytes_rate",
			rangeEqualsStep: true,
			wantManual:      false,
		},
		{
			name:            "bytes_over_time_parser_tumbling_fast",
			baseQuery:       parserBaseQuery,
			manualFunc:      "bytes_over_time",
			rangeEqualsStep: true,
			wantManual:      false,
		},
		// Parser stages + sliding window: SLOW PATH preserved.
		{
			name:            "rate_parser_sliding_slow",
			baseQuery:       parserBaseQuery,
			manualFunc:      "rate",
			rangeEqualsStep: false,
			wantManual:      true, // slow path
		},
		{
			name:            "count_over_time_parser_sliding_slow",
			baseQuery:       parserBaseQuery,
			manualFunc:      "count_over_time",
			rangeEqualsStep: false,
			wantManual:      true,
		},
		// No parser stages + tumbling: fast path (unchanged behaviour).
		{
			name:            "rate_no_parser_tumbling_fast",
			baseQuery:       plainBaseQuery,
			manualFunc:      "rate",
			rangeEqualsStep: true,
			wantManual:      false,
		},
		// No parser stages + sliding: slow path (unchanged behaviour).
		{
			name:            "rate_no_parser_sliding_slow",
			baseQuery:       plainBaseQuery,
			manualFunc:      "rate",
			rangeEqualsStep: false,
			wantManual:      true,
		},
		// rate_counter always slow regardless.
		{
			name:            "rate_counter_always_slow",
			baseQuery:       plainBaseQuery,
			manualFunc:      "rate_counter",
			rangeEqualsStep: true,
			wantManual:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldUseManualRangeMetricCompat(tc.baseQuery, tc.manualFunc, tc.rangeEqualsStep)
			if got != tc.wantManual {
				t.Errorf("shouldUseManualRangeMetricCompat(%q, %q, rangeEqualsStep=%v) = %v, want %v",
					tc.baseQuery, tc.manualFunc, tc.rangeEqualsStep, got, tc.wantManual)
			}
		})
	}
}
```

Note: the test calls `shouldUseManualRangeMetricCompat` with 3 parameters (after we remove the 4th).

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /Users/slawomirskowron/.config/superpowers/worktrees/loki-vl-proxy/perf/metric-fast-path
go test ./internal/proxy/... -run TestShouldUseManualRangeMetricCompat_ParserStageRate -v 2>&1 | tail -20
```

Expected: compilation failure ("too many arguments") or test failure on the "parser_tumbling_fast" cases returning `true` instead of `false`.

- [ ] **Step 3: Commit the failing tests**

```bash
git add internal/proxy/range_metric_compat_test.go
git commit -m "test: add failing unit tests for parser-stage fast-path gate removal"
```

---

## Task 2: Remove the parser-stage guard from `shouldUseManualRangeMetricCompat`

**Files:**
- Modify: `internal/proxy/range_metric_compat.go:369-405` — function signature + body
- Modify: `internal/proxy/range_metric_compat.go:311` — first caller
- Modify: `internal/proxy/range_metric_compat.go:343` — second caller

- [ ] **Step 1: Update the function signature and body**

Replace (lines 369-405):

```go
func shouldUseManualRangeMetricCompat(baseQuery, manualFunc string, rangeEqualsStep bool, originalLogql string) bool {
	manualFunc = strings.TrimSpace(manualFunc)
	if manualFunc == "rate_counter" {
		return true
	}

	// For sliding windows (range != step) VL native stats_query_range buckets by the
	// step interval (tumbling windows) while LogQL evaluates each point over [T-range, T].
	// When the data distribution is non-uniform the two diverge. Route to the manual
	// log-fetch path for correct sliding-window semantics regardless of parser stages.
	// When range == step windows are non-overlapping and native VL stats is equivalent.
	switch manualFunc {
	case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
		// Extracting parser stages (| unpack_json, | extract, etc.) require the manual
		// log-fetch path even when range==step: VL native stats may not replicate Loki's
		// __error__ exclusion semantics for lines that fail parsing in metric queries.
		// Exception: when the query explicitly handles __error__ (e.g., | drop __error__),
		// parse failures are already accounted for and VL native stats is semantically correct.
		if queryUsesParserStages(baseQuery) && !strings.Contains(originalLogql, "__error__") {
			return true
		}
		return !rangeEqualsStep
	}

	if !queryUsesParserStages(baseQuery) {
		return false
	}

	// Parser stages present — native VL stats is safe for unwrap-based aggregations
	// (parse failures self-filter via absent fields) and for non-sliding windows.
	switch manualFunc {
	case "avg", "sum", "min", "max", "quantile", "stddev", "stdvar", "first", "last":
		return false
	default:
		return true
	}
}
```

With:

```go
func shouldUseManualRangeMetricCompat(baseQuery, manualFunc string, rangeEqualsStep bool) bool {
	manualFunc = strings.TrimSpace(manualFunc)
	if manualFunc == "rate_counter" {
		return true
	}

	// For sliding windows (range != step) VL native stats_query_range buckets by the
	// step interval (tumbling windows) while LogQL evaluates each point over [T-range, T].
	// When the data distribution is non-uniform the two diverge. Route to the manual
	// log-fetch path for correct sliding-window semantics.
	// When range == step windows are non-overlapping and native VL stats is equivalent,
	// including for queries that use parser stages (| json, | logfmt): VL stats_query_range
	// natively supports inline filter pipelines and parser stages.
	switch manualFunc {
	case "rate", "bytes_rate", "count_over_time", "bytes_over_time":
		return !rangeEqualsStep
	}

	if !queryUsesParserStages(baseQuery) {
		return false
	}

	// Parser stages present — native VL stats is safe for unwrap-based aggregations
	// (parse failures self-filter via absent fields) and for non-sliding windows.
	switch manualFunc {
	case "avg", "sum", "min", "max", "quantile", "stddev", "stdvar", "first", "last":
		return false
	default:
		return true
	}
}
```

- [ ] **Step 2: Update the first caller (line 311 in `handleStatsCompatRange`)**

Replace:
```go
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, noSlidingOverlap, originalLogql) {
		return false
	}
```

With:
```go
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, noSlidingOverlap) {
		return false
	}
```

- [ ] **Step 3: Update the second caller (line 343 in `handleStatsCompatInstant`)**

Replace:
```go
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, true, originalLogql) {
		return false
	}
```

With:
```go
	if !shouldUseManualRangeMetricCompat(spec.BaseQuery, manualFunc, true) {
		return false
	}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

```bash
cd /Users/slawomirskowron/.config/superpowers/worktrees/loki-vl-proxy/perf/metric-fast-path
go test ./internal/proxy/... -run TestShouldUseManualRangeMetricCompat_ParserStageRate -v 2>&1 | tail -20
```

Expected: all 9 subtests PASS.

- [ ] **Step 5: Run the full test suite to catch regressions**

```bash
cd /Users/slawomirskowron/.config/superpowers/worktrees/loki-vl-proxy/perf/metric-fast-path
go test ./internal/proxy/... -count=1 -timeout 120s 2>&1 | tail -20
```

Expected: all tests pass (same count as before — 2484 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/proxy/range_metric_compat.go
git commit -m "perf: remove parser-stage guard from shouldUseManualRangeMetricCompat

Rate, count_over_time, bytes_rate, and bytes_over_time queries with | json
or | logfmt parser stages now use VL native stats_query_range when the
range window equals the step (tumbling window). Previously these were forced
to the manual raw-log-fetch path regardless of the window configuration.

Sliding-window queries (range > step) remain on the slow path: the
!rangeEqualsStep gate is preserved for correct sliding-window semantics.

The originalLogql parameter is dropped from shouldUseManualRangeMetricCompat
as it was only used by the removed __error__ exception check."
```

---

## Task 3: Integration test — parser-stage rate tumbling window routes to `stats_query_range`

**Files:**
- Modify: `internal/proxy/range_metric_compat_test.go`

- [ ] **Step 1: Write the integration test**

Add after the `TestShouldUseManualRangeMetricCompat_ParserStageRate` test:

```go
func TestQueryRange_RateParserStageTumblingUsesStatsQueryRange(t *testing.T) {
	// sum by (app) (rate({app="api-gateway"} | json | status >= 400 [5m])) with step=5m
	// (range == step, tumbling window) must route to VL stats_query_range after the
	// parser-stage guard is removed from shouldUseManualRangeMetricCompat.
	base := time.Unix(1700000000, 0).UTC()
	var statsCalled bool

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			statsCalled = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"app":"api-gateway"},"values":[[1700000300,"0.5"]]}]}}`))
		case "/select/logsql/query":
			// Manual raw-log-fetch MUST NOT be called for tumbling window.
			t.Error("unexpected slow-path /select/logsql/query call for tumbling-window parser-stage rate")
			w.Header().Set("Content-Type", "application/x-ndjson")
		default:
			if r.URL.Path != "/metrics" {
				t.Logf("unhandled path: %s", r.URL.Path)
			}
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	// step=300 == range=[5m]=300 → rangeEqualsStep=true → tumbling window → fast path.
	params.Set("query", `sum by (app) (rate({app="api-gateway"} | json | status >= 400 [5m]))`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "300")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !statsCalled {
		t.Error("expected stats_query_range to be called for tumbling-window parser-stage rate query")
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Status != "success" {
		t.Errorf("expected success, got %q", resp.Status)
	}
	if len(resp.Data.Result) == 0 {
		t.Error("expected at least one series in result")
	}
}

func TestQueryRange_RateParserStageSlidingUsesSlowPath(t *testing.T) {
	// sum by (app) (rate({app="api-gateway"} | json | status >= 400 [5m])) with step=60
	// (range=5m > step=60, sliding window) must still use the slow manual path.
	base := time.Unix(1700000000, 0).UTC()
	var slowPathCalled bool

	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			slowPathCalled = true
			w.Header().Set("Content-Type", "application/x-ndjson")
			// Return one log line so the proxy has something to aggregate.
			_, _ = fmt.Fprintf(w,
				`{"_time":%q,"_msg":"ok","_stream":"{app=\"api-gateway\"}","app":"api-gateway","status":"404"}`+"\n",
				base.Format(time.RFC3339Nano),
			)
		case "/select/logsql/stats_query_range":
			t.Error("stats_query_range must NOT be called for sliding-window rate query")
			w.WriteHeader(http.StatusInternalServerError)
		default:
			if r.URL.Path != "/metrics" {
				t.Logf("unhandled path: %s", r.URL.Path)
			}
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	// step=60 != range=[5m]=300 → rangeEqualsStep=false → sliding window → slow path.
	params.Set("query", `sum by (app) (rate({app="api-gateway"} | json | status >= 400 [5m]))`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !slowPathCalled {
		t.Error("expected slow-path /select/logsql/query for sliding-window parser-stage rate query")
	}
}
```

- [ ] **Step 2: Run the new integration tests**

```bash
cd /Users/slawomirskowron/.config/superpowers/worktrees/loki-vl-proxy/perf/metric-fast-path
go test ./internal/proxy/... -run "TestQueryRange_RateParserStage" -v 2>&1 | tail -30
```

Expected: both tests PASS.

- [ ] **Step 3: Run the full test suite one more time**

```bash
go test ./internal/proxy/... -count=1 -timeout 120s 2>&1 | tail -5
```

Expected: all tests pass, no new failures.

- [ ] **Step 4: Commit**

```bash
git add internal/proxy/range_metric_compat_test.go
git commit -m "test: add integration tests for parser-stage rate tumbling/sliding routing"
```

---

## Task 4: Run e2e parity tests to confirm no regression

**Files:** Read-only — runs existing tests against the live e2e-compat stack.

- [ ] **Step 1: Confirm the e2e-compat stack is running**

```bash
docker ps --format '{{.Names}}' | grep -E "loki|victorialogs|e2e" | sort
```

Expected: containers include `e2e-proxy`, `e2e-loki`, `e2e-victorialogs` (or similar).  
If not running: `cd /path/to/repo && docker compose -f docker-compose.e2e.yml up -d` (check the actual compose file path in the repo).

- [ ] **Step 2: Build and deploy the proxy with the change**

```bash
cd /Users/slawomirskowron/.config/superpowers/worktrees/loki-vl-proxy/perf/metric-fast-path
docker build -t loki-vl-proxy:e2e-local . 2>&1 | tail -5
docker compose -f docker-compose.e2e.yml restart e2e-proxy 2>&1
```

Wait 5 seconds for restart, then verify proxy is healthy:
```bash
curl -s http://localhost:3101/loki/api/v1/labels | head -c 100
```

- [ ] **Step 3: Run the LogQL parity test suite**

```bash
cd /Users/slawomirskowron/.config/superpowers/worktrees/loki-vl-proxy/perf/metric-fast-path
go test -v -tags=e2e ./test/e2e-compat/... -run TestLogQLParity -timeout 120s 2>&1 | tail -30
```

Expected: all 116 parity cases pass (PASS lines, 0 FAIL).

- [ ] **Step 4: Run the full e2e-compat suite**

```bash
go test -v -tags=e2e ./test/e2e-compat/... -timeout 300s 2>&1 | grep -E "^(ok|FAIL|---)" | tail -30
```

Expected: all packages `ok`, no `FAIL`.

- [ ] **Step 5: Document any unexpected failures**

If any tests fail, check whether they were also failing on `main` before this branch:
```bash
git stash
go test -tags=e2e ./test/e2e-compat/... -timeout 300s 2>&1 | grep -E "^(ok|FAIL)" | tail -10
git stash pop
```

If the failure is pre-existing, note it and proceed. If it's new, fix it before continuing.

---

## Task 5: Manual UI verification queries

The following LogQL queries should be run in **Grafana Explore** (localhost:3002) against the proxy datasource to verify the fast path works end-to-end. Use time range `last 1h` and step `$__interval`. The step must equal the range window for the fast path to trigger.

> **Note:** In Grafana's default Explore mode, `step` is auto-calculated as ~1/1000 of the time range. To force tumbling window testing, use a custom time range of exactly 5 minutes with `step=5m` (300s), or use the `$__interval` variable in a dashboard panel with a 5m time range.

### Queries to test (fast path: range == step)

Use step = 5m (300s) for all queries below. In Grafana Explore, set time range to "Last 5 minutes" — the auto-step will be ~3-6 seconds, so set `step` explicitly to `300` in the URL or use a dashboard panel with fixed step.

**1. Rate with JSON filter — previously always slow path, now fast path:**
```
sum by (app) (rate({app="api-gateway"} | json | status >= 400 [5m]))
```
Set step=300. Verify the proxy logs show `stats_query_range` being called (not raw log fetch).

**2. Count over time with logfmt:**
```
sum by (app) (count_over_time({app="payment-service"} | logfmt | level="error" [5m]))
```
Step=300. Should route to stats_query_range.

**3. Bytes rate with JSON:**
```
sum by (app) (bytes_rate({namespace="prod"} | json [5m]))
```
Step=300. Should route to stats_query_range.

**4. Ungrouped rate with JSON (no `by` clause):**
```
sum(rate({app="api-gateway"} | json | status >= 400 [5m]))
```
Step=300. Should route to stats_query_range.

**5. Topk over parser-stage rate (tumbling window):**
```
topk(5, sum by (app) (rate({namespace="prod"} | json | status >= 400 [5m])))
```
Step=300. Inner query routes to stats_query_range; topk is applied in proxy.

### Queries that should STILL use slow path (sliding window)

**6. Rate with JSON — sliding window (intentionally slow):**
```
sum by (app) (rate({app="api-gateway"} | json | status >= 400 [5m]))
```
Set step=60 (60s ≠ 5m). Verify this still produces results (semantically correct sliding-window computation). Compare against Loki — values should be very close but may differ slightly (Loki sliding vs VL tumbling with 60s step).

**7. avg_over_time with unwrap — always slow (accepted limitation):**
```
avg_over_time({app="api-gateway"} | json | unwrap latency_ms [5m]) by (app)
```
Any step. Always uses slow path. Compare result against Loki — should match exactly.

**8. quantile_over_time with unwrap — always slow:**
```
quantile_over_time(0.99, {app="api-gateway"} | json | unwrap latency_ms [5m]) by (app)
```
Any step. Always uses slow path.

### How to verify fast path in proxy logs

```bash
docker logs e2e-proxy --tail 50 2>&1 | grep -E "stats_query_range|logsql/query" | tail -20
```

Fast path: `stats_query_range` appears for the query.
Slow path: `/select/logsql/query` appears with `limit=1000000` (manual raw log fetch).

---

## Self-Review Checklist

**Spec coverage:**
- ✅ Change 1 (remove parser-stage guard): Tasks 1+2
- ✅ Testing strategy (unit + integration): Tasks 1+3
- ✅ Parity test verification: Task 4
- ✅ Manual UI test queries: Task 5
- The design spec describes "Change 2" (ungrouped fast path) and "Change 3" (topk unwrapping) — these are ALREADY implemented in the codebase. The spec was aspirational but these don't require new code. No tasks needed for them.

**Placeholder scan:** No TBDs or TODOs in this plan.

**Type consistency:** `shouldUseManualRangeMetricCompat(string, string, bool)` — consistent across Task 1 test code and Task 2 implementation.
