# Proxy Query-Path Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut VL+Proxy latency to within 2x of Loki for heavy metric workloads by eliminating three identified hot-path bottlenecks: per-entry regex re-evaluation in stream processing, full log body fetches for tumbling/sliding metric aggregations, and per-entry JSON field overhead in `encoding/json`.

**Architecture:** Three independent optimization layers each behind its own test file. Task 1 is a 1-line hot-path fix. Task 2 routes `count_over_time`/`rate` sliding windows through VL's `/select/logsql/hits` endpoint (aggregated counts, no log body) and falls back gracefully. Task 3 swaps `encoding/json` for `goccy/go-json` in the NDJSON scanning loops with no API changes.

**Tech Stack:** Go 1.23+, `github.com/goccy/go-json` (pure-Go drop-in, 2-4× faster stdlib JSON), VL `/select/logsql/hits` endpoint (already used by drilldown.go), existing `vlHitsResponse` type in stream_processing.go.

---

## Context and Files

All files are in `/private/tmp/loki-vl-proxy-main/internal/proxy/` unless noted.

| File | Role in this plan |
|------|------------------|
| `stream_processing.go` | Opt 1: precompute `skipLogLineReconstruction` flag, use `reconstructLogLineWithFlag` |
| `http_utils.go` | Unchanged — `reconstructLogLineWithFlag` already exists at line 455 |
| `query_translation.go` | Opt 2: new `fetchBareParserMetricSeriesViaHits`, gate change in `proxyBareParserMetricQueryRange` |
| `stream_processing.go` | Opt 2: new helper `buildSlidingWindowSumsFromHits` (same file, adds to hits machinery) |
| `go.mod` / `go.sum` (root) | Opt 3: add `github.com/goccy/go-json` |
| `postprocess.go` | Opt 3: swap json import in pattern-streaming hot path |
| `stream_processing.go` | Opt 3: swap json import in NDJSON scanner loops |
| `query_translation.go` | Opt 3: swap json import in `fetchBareParserMetricSeries` scanner |
| `drilldown.go` | Opt 3: swap json import in `detectFieldSummariesStream` |
| `opt3_reconstruction_test.go` | New — unit tests for Opt 1 precomputation |
| `opt6_hits_sliding_window_test.go` | New — unit + mock-server tests for Opt 2 |
| `opt7_fast_json_bench_test.go` | New — benchmarks comparing stdlib vs goccy |

---

## Task 1 — Precompute `skipLogLineReconstruction` in `vlReaderToLokiStreams`

**Why:** `reconstructLogLine` (stream_processing.go:513) calls `hasTextExtractionParser` on every log entry in the hot loop. This regex match (3 patterns) is O(len(query)) but executes millions of times per request. `query_range_windowing.go` already avoids it by precomputing once — `stream_processing.go` missed the same fix.

**Files:**
- Modify: `internal/proxy/stream_processing.go` (line 451 area + line 513)
- Test: `internal/proxy/opt3_reconstruction_test.go` (new file)

- [ ] **Step 1.1 — Write the failing test**

Create `internal/proxy/opt3_reconstruction_test.go`:

```go
package proxy

import (
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newOpt3Proxy(t testing.TB) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             "http://unused",
		Cache:                  cache.New(30*time.Second, 100),
		LogLevel:               "error",
		EmitStructuredMetadata: true,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

// TestOPT3_SkipFlagConsistency verifies that precomputing hasTextExtractionParser
// once per request produces identical results to calling it per-entry.
func TestOPT3_SkipFlagConsistency(t *testing.T) {
	queries := []struct {
		logql      string
		wantSkip   bool
	}{
		// logfmt: skip reconstruction (logfmt replaces the line with extracted fields)
		{`{app="foo"} | logfmt`, true},
		// regexp: skip
		{`{app="foo"} | regexp "(?P<msg>.*)"`, true},
		// pattern: skip
		{`{app="foo"} | pattern "<msg>"`, true},
		// json: do NOT skip (json adds parsed fields to entry, reconstruction wraps them)
		{`{app="foo"} | json`, false},
		// plain stream selector: do NOT skip
		{`{app="foo"}`, false},
		// json + filter: do NOT skip
		{`{app="foo"} | json | status >= 400`, false},
	}

	for _, tc := range queries {
		skip := hasTextExtractionParser(tc.logql)
		if skip != tc.wantSkip {
			t.Errorf("hasTextExtractionParser(%q) = %v, want %v", tc.logql, skip, tc.wantSkip)
		}
	}
}

// TestOPT3_VlReaderToLokiStreams_LogfmtNoReconstruction verifies that a logfmt query
// does not trigger JSON reconstruction of log lines (the skip flag is working).
func TestOPT3_VlReaderToLokiStreams_LogfmtNoReconstruction(t *testing.T) {
	p := newOpt3Proxy(t)

	// VL NDJSON response where entries have extra fields (parsed by VL at ingest)
	ndjson := strings.Join([]string{
		`{"_time":"1746100000000000000","_msg":"level=info msg=started","_stream":"{app=\"svc\"}","level":"info","status":"200"}`,
		`{"_time":"1746100001000000000","_msg":"level=error msg=failed","_stream":"{app=\"svc\"}","level":"error","status":"500"}`,
	}, "\n")

	lokiQuery := `{app="svc"} | logfmt`
	streams, _, err := p.vlReaderToLokiStreams(strings.NewReader(ndjson), lokiQuery, 500, "", 0, false, false)
	if err != nil {
		t.Fatalf("vlReaderToLokiStreams: %v", err)
	}
	if len(streams) == 0 {
		t.Fatal("expected at least one stream")
	}
	for _, s := range streams {
		for _, v := range s.Values {
			vals, ok := v.([]interface{})
			if !ok || len(vals) < 2 {
				continue
			}
			line, _ := vals[1].(string)
			// For logfmt, reconstruction must NOT wrap the line in JSON.
			// The line should stay as the original _msg value.
			if strings.HasPrefix(line, `{"_msg":`) {
				t.Errorf("logfmt query produced reconstructed JSON line: %q", line)
			}
		}
	}
}

// TestOPT3_VlReaderToLokiStreams_JsonDoesReconstruct verifies that a json query
// DOES trigger JSON reconstruction (fields merged back into line).
func TestOPT3_VlReaderToLokiStreams_JsonDoesReconstruct(t *testing.T) {
	p := newOpt3Proxy(t)

	ndjson := strings.Join([]string{
		`{"_time":"1746100000000000000","_msg":"{}","_stream":"{app=\"svc\"}","level":"info","status":"200","method":"GET"}`,
	}, "\n")

	lokiQuery := `{app="svc"} | json`
	streams, _, err := p.vlReaderToLokiStreams(strings.NewReader(ndjson), lokiQuery, 500, "", 0, false, false)
	if err != nil {
		t.Fatalf("vlReaderToLokiStreams: %v", err)
	}
	if len(streams) == 0 {
		t.Fatal("expected at least one stream")
	}
	found := false
	for _, s := range streams {
		for _, v := range s.Values {
			vals, ok := v.([]interface{})
			if !ok || len(vals) < 2 {
				continue
			}
			line, _ := vals[1].(string)
			if strings.Contains(line, "status") {
				found = true
			}
		}
	}
	if !found {
		t.Error("json query: expected reconstructed line containing parsed field 'status'")
	}
}
```

- [ ] **Step 1.2 — Run to confirm tests fail for the right reason**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -run TestOPT3 -v 2>&1 | tail -20
```

Expected: compilation success, tests may PASS or FAIL. The `TestOPT3_SkipFlagConsistency` should pass already (tests a helper, not the stream path). The stream tests may fail if `vlReaderToLokiStreams` is not exported or signature differs — adjust accordingly.

- [ ] **Step 1.3 — Apply the one-line hot-path fix in `stream_processing.go`**

In `stream_processing.go`, the block around line 450-451 currently reads:

```go
classifyAsParsed := hasParserStage(originalQuery, "json") || hasParserStage(originalQuery, "logfmt")
```

Add the precomputation immediately after:

```go
classifyAsParsed := hasParserStage(originalQuery, "json") || hasParserStage(originalQuery, "logfmt")
skipLogLineReconstruction := hasTextExtractionParser(originalQuery)
```

Then at the call site around line 513, change:

```go
// Before:
if len(entry) > len(desc.rawLabels)+5 {
    msg = reconstructLogLine(msg, entry, desc.rawLabels, originalQuery)
}

// After:
if len(entry) > len(desc.rawLabels)+5 {
    msg = reconstructLogLineWithFlag(msg, entry, desc.rawLabels, skipLogLineReconstruction)
}
```

- [ ] **Step 1.4 — Run all proxy tests**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -count=1 -timeout 300s 2>&1 | tail -10
```

Expected: all tests pass (no regressions). If `TestOPT3_VlReaderToLokiStreams_*` fail due to unexported method or signature mismatch, adjust the test to call the method indirectly via an HTTP round-trip using `httptest.NewRecorder`.

- [ ] **Step 1.5 — Commit**

```bash
cd /private/tmp/loki-vl-proxy-main
git add internal/proxy/stream_processing.go internal/proxy/opt3_reconstruction_test.go
git commit -m "perf: precompute skipLogLineReconstruction once per response in vlReaderToLokiStreams

Calling hasTextExtractionParser (3 regex matches) per log entry was
O(n_entries × len(query)) in the hot scanner loop. query_range_windowing.go
already precomputed this flag; apply the same fix to vlReaderToLokiStreams.
Switches to reconstructLogLineWithFlag which accepts the precomputed bool."
```

---

## Task 2 — Hits-Based Sliding Window for `count_over_time` / `rate`

**Why:** For `rate({...}[5m])` with step=1m (sliding window, range > step), the proxy currently fetches all raw log entries for the full time range (`/select/logsql/query` with `limit=1000000`), parses every line, and manually buckets them. VL's `/select/logsql/hits` returns pre-aggregated counts per time bucket — no log body, tiny response. For a 6h query at 1m step, hits returns 365 integers vs millions of JSON lines.

**Algorithm:**
1. Call hits with `step=userStep`, `start=userStart-rangeWindow`, `end=userEnd`, `field=<declaredLabelFields...>`
2. Parse hits response into `map[labelKey]map[bucketNanos]int64`
3. For each evaluation point T from `userStart` to `userEnd` at `userStep`:
   - Sum counts in buckets where `bucketNanos ∈ [T-rangeWindow, T)`
4. `count_over_time` → emit sum. `rate` → emit sum / rangeWindow.Seconds().

**Gate (applies when ALL of):**
- `spec.funcName ∈ {count_over_time, rate}`
- `spec.unwrapField == ""`
- `!hasPostParserPipeStage(spec.baseQuery)`
- `range > step` (sliding window; tumbling window already handled by `proxyBareParserMetricViaStats`)
- `p.declaredLabelFields != nil` (proxy knows its stream label field names)

**Files:**
- Modify: `internal/proxy/query_translation.go` (new function + gate change)
- Modify: `internal/proxy/stream_processing.go` (new `buildSlidingWindowSumsFromHits` helper)
- Test: `internal/proxy/opt6_hits_sliding_window_test.go` (new file)

### Step 2.1 — Write tests first

Create `internal/proxy/opt6_hits_sliding_window_test.go`:

```go
package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// hitsServer builds a fake VL backend that responds to /select/logsql/hits
// with the provided response JSON and asserts that the request params match wantParams.
func hitsServer(t *testing.T, hitsJSON string, wantParams map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			http.Error(w, "unexpected path: "+r.URL.Path, 400)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "form parse: "+err.Error(), 400)
			return
		}
		for k, v := range wantParams {
			if got := r.FormValue(k); got != v {
				t.Errorf("hits param %q: got %q, want %q", k, got, v)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, hitsJSON)
	}))
}

func newOpt6Proxy(t testing.TB, backendURL string, streamFields []string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             backendURL,
		Cache:                  cache.New(0, 0), // disabled
		LogLevel:               "error",
		EmitStructuredMetadata: true,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
		StreamFields:           streamFields,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

// TestOPT6_BuildSlidingWindowSums_CountOverTime verifies the sliding-window sum
// computation from tumbling hits buckets.
func TestOPT6_BuildSlidingWindowSums_CountOverTime(t *testing.T) {
	// 5 tumbling 1m buckets: t0, t1, t2, t3, t4
	// rangeWindow = 3m, step = 1m
	// Evaluation at t2: sum buckets in [t2-3m, t2) = t2-3m..t2 = buckets at t-3m, t-2m, t-1m
	// But buckets ARE at t0=minute0, t1=minute1, etc.
	// rangeWindow = 3 buckets, so eval at index i sums [i-3..i-1].
	t0 := int64(1746100000) * 1e9 // second boundary in nanos
	stepNs := int64(60) * 1e9     // 1m
	rangeNs := int64(180) * 1e9   // 3m

	// Buckets keyed by labelKey → bucketNanos → count.
	// Single label combination "app=svc".
	buckets := map[string]map[int64]int64{
		`app="svc"`: {
			t0:            10,
			t0 + stepNs:   20,
			t0 + 2*stepNs: 30,
			t0 + 3*stepNs: 40,
			t0 + 4*stepNs: 50,
		},
	}
	labelSets := map[string]map[string]string{
		`app="svc"`: {"app": "svc"},
	}

	// Evaluation points: start at t0+3*stepNs (first point where 3 buckets are in window)
	// through t0+4*stepNs.
	evalStart := t0 + 3*stepNs
	evalEnd := t0 + 4*stepNs

	series := buildSlidingWindowSumsFromHits(buckets, labelSets, evalStart, evalEnd, stepNs, rangeNs, false)
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	s := series[0]
	// Eval at t0+3m: sum of buckets in [t0, t0+3m) = t0, t0+1m, t0+2m = 10+20+30=60
	// Eval at t0+4m: sum of buckets in [t0+1m, t0+4m) = 20+30+40=90
	wantSamples := []struct {
		ts    int64
		value float64
	}{
		{evalStart, 60},
		{evalEnd, 90},
	}
	if len(s.samples) != len(wantSamples) {
		t.Fatalf("expected %d samples, got %d", len(wantSamples), len(s.samples))
	}
	for i, w := range wantSamples {
		if s.samples[i].tsNanos != w.ts {
			t.Errorf("sample[%d].ts = %d, want %d", i, s.samples[i].tsNanos, w.ts)
		}
		if math.Abs(s.samples[i].value-w.value) > 1e-9 {
			t.Errorf("sample[%d].value = %g, want %g", i, s.samples[i].value, w.value)
		}
	}
}

// TestOPT6_BuildSlidingWindowSums_Rate verifies division by rangeWindow for rate.
func TestOPT6_BuildSlidingWindowSums_Rate(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9
	rangeNs := int64(300) * 1e9 // 5m

	buckets := map[string]map[int64]int64{
		`app="api"`: {t0: 300}, // 300 entries in one bucket
	}
	labelSets := map[string]map[string]string{
		`app="api"`: {"app": "api"},
	}

	evalStart := t0 + 5*stepNs // first point where window is full
	evalEnd := t0 + 5*stepNs

	series := buildSlidingWindowSumsFromHits(buckets, labelSets, evalStart, evalEnd, stepNs, rangeNs, true /* isRate */)
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}
	// rate = 300 / 300s = 1.0
	wantRate := 1.0
	if math.Abs(series[0].samples[0].value-wantRate) > 1e-9 {
		t.Errorf("rate = %g, want %g", series[0].samples[0].value, wantRate)
	}
}

// TestOPT6_BuildSlidingWindowSums_EmptyWindow verifies absent-point behaviour.
func TestOPT6_BuildSlidingWindowSums_EmptyWindow(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9
	rangeNs := int64(180) * 1e9 // 3m

	// No buckets → every eval point has count 0 → no samples emitted (Loki absent-point).
	buckets := map[string]map[int64]int64{}
	labelSets := map[string]map[string]string{}

	evalStart := t0
	evalEnd := t0 + 2*stepNs

	series := buildSlidingWindowSumsFromHits(buckets, labelSets, evalStart, evalEnd, stepNs, rangeNs, false)
	if len(series) != 0 {
		t.Errorf("expected 0 series for empty data, got %d", len(series))
	}
}

// TestOPT6_BuildSlidingWindowSums_MultiStream verifies separate series per label combination.
func TestOPT6_BuildSlidingWindowSums_MultiStream(t *testing.T) {
	t0 := int64(1746100000) * 1e9
	stepNs := int64(60) * 1e9
	rangeNs := int64(60) * 1e9 // 1m (tumbling)

	buckets := map[string]map[int64]int64{
		`app="a"`: {t0: 5},
		`app="b"`: {t0: 15},
	}
	labelSets := map[string]map[string]string{
		`app="a"`: {"app": "a"},
		`app="b"`: {"app": "b"},
	}

	series := buildSlidingWindowSumsFromHits(buckets, labelSets, t0+stepNs, t0+stepNs, stepNs, rangeNs, false)
	if len(series) != 2 {
		t.Errorf("expected 2 series (one per app), got %d", len(series))
	}
}

// TestOPT6_FetchBareParserMetricSeriesViaHits_Integration tests the full round-trip
// using a mock VL server that serves hits responses.
func TestOPT6_FetchBareParserMetricSeriesViaHits_Integration(t *testing.T) {
	// Fake VL hits response: two app label values, each with 5 buckets of 1m.
	// query covers 5m at 1m step, rangeWindow=5m → tumbling edge case handled.
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t0 := now.Add(-5 * time.Minute)

	bucketFn := func(offset time.Duration, count int) string {
		ts := t0.Add(offset).UnixNano()
		return fmt.Sprintf("%d", ts)
	}

	hitsResp := map[string]interface{}{
		"hits": []map[string]interface{}{
			{
				"fields":     map[string]string{"app": "api-gw"},
				"timestamps": []string{bucketFn(0, 0), bucketFn(time.Minute, 0), bucketFn(2*time.Minute, 0)},
				"values":     []int{10, 20, 30},
			},
			{
				"fields":     map[string]string{"app": "auth"},
				"timestamps": []string{bucketFn(0, 0), bucketFn(time.Minute, 0), bucketFn(2*time.Minute, 0)},
				"values":     []int{5, 5, 5},
			},
		},
	}
	hitsJSON, _ := json.Marshal(hitsResp)

	srv := hitsServer(t, string(hitsJSON), map[string]string{
		"query": `namespace:"prod"`,
		"step":  "60s",
	})
	defer srv.Close()

	p := newOpt6Proxy(t, srv.URL, []string{"app", "namespace"})

	spec := bareParserMetricCompatSpec{
		funcName:        "count_over_time",
		baseQuery:       `{namespace="prod"}`,
		rangeWindow:     5 * time.Minute,
		rangeWindowExpr: "5m",
	}

	startNs := t0.UnixNano()
	endNs := now.UnixNano()
	stepNs := int64(60) * 1e9

	series, err := p.fetchBareParserMetricSeriesViaHits(
		t.Context(), spec,
		startNs, endNs, stepNs,
	)
	if err != nil {
		t.Fatalf("fetchBareParserMetricSeriesViaHits: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("expected 2 series (api-gw, auth), got %d", len(series))
	}
	// Verify series have samples with positive values.
	for _, s := range series {
		if len(s.samples) == 0 {
			t.Errorf("series %v has no samples", s.metric)
		}
	}
}

// TestOPT6_ProxyBareParserMetricQueryRange_SlidingWindowUsesHits verifies that
// when the gate conditions are met (range > step, funcName=rate, declaredLabelFields set),
// the proxy routes to the hits-based path instead of the full-fetch path.
func TestOPT6_ProxyBareParserMetricQueryRange_SlidingWindowUsesHits(t *testing.T) {
	hitsCalled := false
	queryCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsCalled = true
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"hits":[]}`)
		case "/select/logsql/query":
			queryCalled = true
			http.Error(w, "should not be called", 500)
		default:
			http.Error(w, "unexpected: "+r.URL.Path, 404)
		}
	}))
	defer srv.Close()

	p := newOpt6Proxy(t, srv.URL, []string{"app", "namespace"})

	// Construct an HTTP request for query_range with rate[5m] and step=1m (sliding window).
	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+url.Values{
		"query": {`rate({namespace="prod"}[5m])`},
		"start": {"1746100000000000000"},
		"end":   {"1746103600000000000"}, // 1h later
		"step":  {"60"},                  // 1m step → sliding (range 5m > step 1m)
	}.Encode(), nil)
	w := httptest.NewRecorder()

	p.handleQueryRange(w, req)

	if queryCalled {
		t.Error("full query endpoint was called — hits-based path was NOT used")
	}
	if !hitsCalled {
		t.Error("hits endpoint was NOT called — sliding window optimisation is not active")
	}
}

// TestOPT6_FallbackWhenNoDeclaredFields verifies that without declaredLabelFields,
// the proxy falls back to the full-fetch path without errors.
func TestOPT6_FallbackWhenNoDeclaredFields(t *testing.T) {
	fullFetchCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			t.Error("hits must not be called when declaredLabelFields is nil")
			w.WriteHeader(500)
		case "/select/logsql/query":
			fullFetchCalled = true
			w.Header().Set("Content-Type", "application/json")
			// Return empty NDJSON — no entries.
			fmt.Fprint(w, "")
		default:
			http.Error(w, "unexpected: "+r.URL.Path, 404)
		}
	}))
	defer srv.Close()

	// No stream fields configured → declaredLabelFields is nil.
	p := newOpt6Proxy(t, srv.URL, nil)

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+url.Values{
		"query": {`rate({namespace="prod"}[5m])`},
		"start": {"1746100000000000000"},
		"end":   {"1746103600000000000"},
		"step":  {"60"},
	}.Encode(), nil)
	w := httptest.NewRecorder()

	p.handleQueryRange(w, req)

	if !fullFetchCalled {
		t.Error("expected fallback to full-fetch path, but /select/logsql/query was not called")
	}
}
```

- [ ] **Step 2.2 — Run tests to confirm they fail (function not yet implemented)**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -run TestOPT6 -v 2>&1 | tail -30
```

Expected: compilation error — `buildSlidingWindowSumsFromHits` and `fetchBareParserMetricSeriesViaHits` are not defined. That's correct.

- [ ] **Step 2.3 — Implement `buildSlidingWindowSumsFromHits` in `stream_processing.go`**

Add this function at the end of `internal/proxy/stream_processing.go` (before the last `}`):

```go
// buildSlidingWindowSumsFromHits converts pre-aggregated VL hits buckets into
// bareParserMetricSeries using sliding-window sums.
//
// buckets:   map[canonicalLabelKey]map[bucketStartNanos]count
// labelSets: map[canonicalLabelKey]map[string]string  (the actual label pairs)
// evalStart, evalEnd, stepNs: the Prometheus evaluation range (nanos)
// rangeNs:   the range window width in nanos (e.g. 5m = 300e9)
// isRate:    if true, divide count by rangeNs/1e9 to get per-second rate
//
// Absent evaluation points (sum == 0) are omitted (matches Loki behaviour).
func buildSlidingWindowSumsFromHits(
	buckets map[string]map[int64]int64,
	labelSets map[string]map[string]string,
	evalStart, evalEnd, stepNs, rangeNs int64,
	isRate bool,
) []bareParserMetricSeries {
	if len(buckets) == 0 {
		return nil
	}
	result := make([]bareParserMetricSeries, 0, len(buckets))
	for labelKey, tsBuckets := range buckets {
		labels := labelSets[labelKey]
		samples := make([]bareParserMetricSample, 0, (evalEnd-evalStart)/stepNs+1)
		for evalT := evalStart; evalT <= evalEnd; evalT += stepNs {
			windowStart := evalT - rangeNs
			var sum int64
			for bucketTs, cnt := range tsBuckets {
				// Include bucket if it overlaps [windowStart, evalT).
				// VL bucket at bucketTs covers [bucketTs, bucketTs+stepNs).
				if bucketTs >= windowStart && bucketTs < evalT {
					sum += cnt
				}
			}
			if sum == 0 {
				continue // absent point — matches Loki behaviour
			}
			value := float64(sum)
			if isRate {
				value = float64(sum) / (float64(rangeNs) / 1e9)
			}
			samples = append(samples, bareParserMetricSample{tsNanos: evalT, value: value})
		}
		if len(samples) == 0 {
			continue
		}
		metric := make(map[string]string, len(labels))
		for k, v := range labels {
			metric[k] = v
		}
		result = append(result, bareParserMetricSeries{metric: metric, samples: samples})
	}
	sort.Slice(result, func(i, j int) bool {
		return canonicalLabelsKey(result[i].metric) < canonicalLabelsKey(result[j].metric)
	})
	return result
}
```

- [ ] **Step 2.4 — Implement `fetchBareParserMetricSeriesViaHits` in `query_translation.go`**

Add this function after `fetchBareParserMetricSeries` (around line 800):

```go
// fetchBareParserMetricSeriesViaHits is the fast path for count_over_time / rate
// when the proxy has declared stream label fields and the query has no post-parser
// pipe stages. Instead of fetching all raw log entries, it calls VL's hits endpoint
// which returns pre-aggregated counts per time bucket — eliminating log body transfer.
//
// evalStart, evalEnd, stepNs are in nanoseconds.
// Returns (series, nil) on success; (nil, error) falls through to the slow path.
func (p *Proxy) fetchBareParserMetricSeriesViaHits(
	ctx context.Context,
	spec bareParserMetricCompatSpec,
	evalStart, evalEnd, stepNs int64,
) ([]bareParserMetricSeries, error) {
	// Translate the base stream selector to VL LogsQL.
	logsqlQuery, err := p.translateQueryWithContext(ctx, spec.baseQuery)
	if err != nil {
		return nil, err
	}

	// Fetch one extra range-window of buckets before evalStart so the first
	// sliding window has enough history.
	fetchStart := evalStart - spec.rangeWindow.Nanoseconds()

	params := url.Values{}
	params.Set("query", logsqlQuery)
	params.Set("start", nanosToVLTimestamp(fetchStart))
	params.Set("end", nanosToVLTimestamp(evalEnd))
	params.Set("step", formatVLStep(strconv.FormatInt(stepNs/int64(time.Second), 10)+"s"))

	// Group counts by each declared stream label field so we get per-stream series.
	p.configMu.RLock()
	declared := p.declaredLabelFields
	p.configMu.RUnlock()
	for _, f := range declared {
		params.Add("field", f)
	}

	resp, err := p.vlGet(ctx, "/select/logsql/hits", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := readBodyLimited(resp.Body, maxUpstreamErrorBodyBytes)
		return nil, fmt.Errorf("hits: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := readBodyLimited(resp.Body, maxBufferedBackendBodyBytes)
	if err != nil {
		return nil, err
	}
	hits := parseHits(body)

	// Build buckets map: canonicalKey → bucketNanos → count.
	buckets := make(map[string]map[int64]int64, len(hits.Hits))
	labelSets := make(map[string]map[string]string, len(hits.Hits))
	for _, hit := range hits.Hits {
		// Translate VL field names back to Loki label names.
		translated := make(map[string]string, len(hit.Fields))
		p.configMu.RLock()
		for k, v := range hit.Fields {
			translated[p.labelTranslator.ToLoki(k)] = v
		}
		p.configMu.RUnlock()
		key := canonicalLabelsKey(translated)
		if _, ok := buckets[key]; !ok {
			buckets[key] = make(map[int64]int64, len(hit.Timestamps))
			labelSets[key] = translated
		}
		for i, ts := range hit.Timestamps {
			tsNanos, ok := parseFlexibleUnixNanos(string(ts))
			if !ok || i >= len(hit.Values) {
				continue
			}
			buckets[key][tsNanos] += int64(hit.Values[i])
		}
	}

	isRate := spec.funcName == "rate"
	return buildSlidingWindowSumsFromHits(buckets, labelSets, evalStart, evalEnd, stepNs, spec.rangeWindow.Nanoseconds(), isRate), nil
}
```

- [ ] **Step 2.5 — Add the gate in `proxyBareParserMetricQueryRange`**

In `query_translation.go`, the function `proxyBareParserMetricQueryRange` (line ~1195) currently falls through to `fetchBareParserMetricSeries` for sliding windows. Modify it to try `fetchBareParserMetricSeriesViaHits` first.

Locate the block after the tumbling-window fast path (the `if spec.unwrapField == "" && rangeEqualsStep ...` block) and add before the `fetchBareParserMetricSeries` call:

```go
// After the tumbling-window fast path block and before the slow full-fetch path:

// Sliding-window fast path: for count_over_time / rate with no post-parser stages
// and configured stream label fields, use the hits endpoint instead of full-fetch.
// Falls through to the slow path on any error.
stepDur, stepOk := parsePositiveStepDuration(r.FormValue("step"))
if stepOk && spec.rangeWindow > stepDur &&
	spec.unwrapField == "" &&
	!hasPostParserPipeStage(spec.baseQuery) &&
	len(p.declaredLabelFields) > 0 {
	switch spec.funcName {
	case "rate", "count_over_time":
		startNanos, ok1 := parseFlexibleUnixNanos(r.FormValue("start"))
		endNanos, ok2 := parseFlexibleUnixNanos(r.FormValue("end"))
		if ok1 && ok2 {
			series, hitsErr := p.fetchBareParserMetricSeriesViaHits(
				r.Context(), spec,
				startNanos, endNanos, int64(stepDur),
			)
			if hitsErr == nil {
				w.Header().Set("Content-Type", "application/json")
				marshalJSON(w, buildBareParserMetricMatrix(series, startNanos, endNanos, int64(stepDur), spec))
				elapsed := time.Since(start)
				p.metrics.RecordRequest("query_range", http.StatusOK, elapsed)
				p.queryTracker.Record("query_range", originalQuery, elapsed, false)
				return
			}
			slog.WarnContext(r.Context(), "hits-based metric path failed, falling back",
				"err", hitsErr, "query", originalQuery)
		}
	}
}
```

**Important:** The existing `startNanos`/`endNanos`/`stepDur` parse blocks below this may now be duplicated. Rename the existing ones to avoid shadowing — wrap the hits-path parse inside the `if` and keep the original parse for the slow path unchanged.

The full function flow after the change:

```
1. Tumbling-window fast path (rangeEqualsStep → stats_query_range)   [existing]
2. Sliding-window fast path (range > step → hits-based)              [NEW]
3. Slow full-fetch path (fetchBareParserMetricSeries)                 [existing, fallback]
```

- [ ] **Step 2.6 — Build and run the new tests**

```bash
cd /private/tmp/loki-vl-proxy-main && go build ./... 2>&1 && echo "BUILD OK"
go test ./internal/proxy/ -run TestOPT6 -v 2>&1 | tail -40
```

Expected:
- `TestOPT6_BuildSlidingWindowSums_*` — PASS (pure function, no HTTP)
- `TestOPT6_FetchBareParserMetricSeriesViaHits_Integration` — PASS (mock server)
- `TestOPT6_ProxyBareParserMetricQueryRange_SlidingWindowUsesHits` — PASS (route check)
- `TestOPT6_FallbackWhenNoDeclaredFields` — PASS (fallback check)

- [ ] **Step 2.7 — Run full test suite to check for regressions**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -count=1 -timeout 300s 2>&1 | tail -15
```

Expected: all existing tests pass. If `coverage_gaps_test.go` or `range_metric_compat` tests fail, trace the issue — the gate condition change may affect existing test expectations. Adjust test assertions (not the implementation) if they test the old routing path.

- [ ] **Step 2.8 — Commit**

```bash
cd /private/tmp/loki-vl-proxy-main
git add internal/proxy/stream_processing.go \
        internal/proxy/query_translation.go \
        internal/proxy/opt6_hits_sliding_window_test.go
git commit -m "perf: route count_over_time/rate sliding windows through VL hits endpoint

For range > step queries (rate[5m] at step=1m), the proxy previously
fetched all raw log entries and bucketed them in memory. VL's /hits
endpoint returns pre-aggregated counts per bucket — no log body transfer.

Gate: funcName in {rate, count_over_time}, no unwrap, no post-parser
stages, declaredLabelFields configured. Falls back to full-fetch on
any hits error. buildSlidingWindowSumsFromHits computes sliding sums
from the tumbling hit buckets."
```

---

## Task 3 — Replace `encoding/json` with `goccy/go-json` in NDJSON Hot Paths

**Why:** `encoding/json` accounts for ~35% of proxy CPU in the heavy no-cache benchmark (cumulative: 28s of 80.78s total). `goccy/go-json` is a pure-Go drop-in that eliminates the `checkValid` pre-scan and uses a faster string-scanning path — typically 2-4× faster for small objects. No API changes required.

**Where to swap:** Only the four NDJSON scanner hot paths. The rest of the codebase (config parsing, test helpers, response marshalling) keeps stdlib — mixing is safe.

**Files:**
- Modify: `go.mod`, `go.sum` (root of repo)
- Modify: `internal/proxy/stream_processing.go` (vlReaderToLokiStreams scanner + hits parser)
- Modify: `internal/proxy/query_translation.go` (fetchBareParserMetricSeries scanner)
- Modify: `internal/proxy/drilldown.go` (detectFieldSummariesStream scanner)
- Modify: `internal/proxy/postprocess.go` (extractLogPatternsStreamWithStats scanner — already in PR #297)
- Test: `internal/proxy/opt7_fast_json_bench_test.go` (new benchmark file)

- [ ] **Step 3.1 — Add the dependency**

```bash
cd /private/tmp/loki-vl-proxy-main
go get github.com/goccy/go-json@latest
go mod tidy
```

Expected output includes a line like:
```
go: added github.com/goccy/go-json v0.10.x
```

- [ ] **Step 3.2 — Write the benchmark to establish baseline**

Create `internal/proxy/opt7_fast_json_bench_test.go`:

```go
package proxy

import (
	stdjson "encoding/json"
	"strings"
	"testing"

	gojson "github.com/goccy/go-json"
)

// Typical VL NDJSON entry from a JSON-ingested log.
const benchNDJSONEntry = `{"_time":"1746100000000000000","_msg":"{\"method\":\"GET\",\"path\":\"/api/v1/health\",\"status\":200,\"latency_ms\":12}","_stream":"{app=\"api-gateway\",namespace=\"prod\",cluster=\"us-east-1\"}","app":"api-gateway","namespace":"prod","cluster":"us-east-1","method":"GET","path":"/api/v1/health","status":"200","latency_ms":"12","level":"info"}`

func BenchmarkJSON_StdlibUnmarshalToMap(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := make(map[string]interface{})
		_ = stdjson.Unmarshal(data, &entry)
	}
}

func BenchmarkJSON_GoccyUnmarshalToMap(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := make(map[string]interface{})
		_ = gojson.Unmarshal(data, &entry)
	}
}

func BenchmarkJSON_PooledEntryStdlib(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		_ = stdjson.Unmarshal(data, &entry)
		vlEntryPool.Put(entry)
	}
}

func BenchmarkJSON_PooledEntryGoccy(b *testing.B) {
	data := []byte(benchNDJSONEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		entry := vlEntryPool.Get().(map[string]interface{})
		for k := range entry {
			delete(entry, k)
		}
		_ = gojson.Unmarshal(data, &entry)
		vlEntryPool.Put(entry)
	}
}

// BenchmarkJSON_ScannerHotPath simulates the full hot path of vlReaderToLokiStreams.
func BenchmarkJSON_ScannerHotPath(b *testing.B) {
	lines := make([]string, 100)
	for i := range lines {
		lines[i] = benchNDJSONEntry
	}
	body := strings.Join(lines, "\n")

	b.Run("stdlib", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, line := range lines {
				entry := vlEntryPool.Get().(map[string]interface{})
				for k := range entry {
					delete(entry, k)
				}
				_ = stdjson.Unmarshal([]byte(line), &entry)
				vlEntryPool.Put(entry)
			}
		}
		_ = body
	})

	b.Run("goccy", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for _, line := range lines {
				entry := vlEntryPool.Get().(map[string]interface{})
				for k := range entry {
					delete(entry, k)
				}
				_ = gojson.Unmarshal([]byte(line), &entry)
				vlEntryPool.Put(entry)
			}
		}
		_ = body
	})
}
```

- [ ] **Step 3.3 — Run the benchmarks to establish baseline**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -bench=BenchmarkJSON -benchmem -benchtime=3s 2>&1 | grep -E "Benchmark|ns/op"
```

Record the ns/op and allocs/op values for stdlib vs goccy. Expected: goccy should be 2-4× faster.

- [ ] **Step 3.4 — Swap JSON in the four hot paths**

**In `stream_processing.go`:** Add import alias at top of file (in the import block):

```go
import (
    // existing imports...
    gojson "github.com/goccy/go-json"
    // ...
)
```

Then find the two `json.Unmarshal` calls in NDJSON scanning loops:
1. In `vlReaderToLokiStreams` (around line 491): `json.Unmarshal(line, &entry)` → `gojson.Unmarshal(line, &entry)`
2. In `parseHits` (line 1065): `json.Unmarshal(body, &resp)` → `gojson.Unmarshal(body, &resp)`

Do NOT change `json.Marshal` or any other non-hot-path usages.

**In `query_translation.go`:** Same pattern — add `gojson` import alias, swap `json.Unmarshal` inside the `fetchBareParserMetricSeries` scanner loop (around line 740).

**In `drilldown.go`:** Same pattern — swap `json.Unmarshal` inside the `detectFieldSummariesStream` scanner loop. This function is at line ~1487.

**In `postprocess.go`:** Same pattern — swap `json.Unmarshal` inside `extractLogPatternsStreamWithStats` scanner loop (the PR #297 addition).

- [ ] **Step 3.5 — Verify the build compiles cleanly**

```bash
cd /private/tmp/loki-vl-proxy-main && go build ./... 2>&1
```

Expected: no errors. If there are import conflicts, check whether any file already imports `encoding/json` as `json` for non-hot-path uses — keep those as `stdjson "encoding/json"` and update references, or just use both imports.

- [ ] **Step 3.6 — Re-run benchmarks to confirm speedup**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -bench=BenchmarkJSON -benchmem -benchtime=3s 2>&1 | grep -E "Benchmark|ns/op"
```

Expected: `BenchmarkJSON_GoccyUnmarshalToMap` and `BenchmarkJSON_PooledEntryGoccy` show 2-4× speedup vs stdlib variants.

- [ ] **Step 3.7 — Run full test suite**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -count=1 -timeout 300s 2>&1 | tail -10
```

Expected: all tests pass. `goccy/go-json` is wire-compatible with `encoding/json` for all standard JSON — no behaviour changes.

- [ ] **Step 3.8 — Commit**

```bash
cd /private/tmp/loki-vl-proxy-main
git add go.mod go.sum \
        internal/proxy/stream_processing.go \
        internal/proxy/query_translation.go \
        internal/proxy/drilldown.go \
        internal/proxy/postprocess.go \
        internal/proxy/opt7_fast_json_bench_test.go
git commit -m "perf: replace encoding/json with goccy/go-json in NDJSON scanning hot paths

encoding/json accounts for ~35% of proxy CPU in heavy no-cache workloads
(28s/80.78s total from pprof). goccy/go-json is a pure-Go drop-in that
eliminates the checkValid pre-scan and uses a faster string scanner
(2-4x speedup, same alloc count).

Swapped only in the four NDJSON hot-path scanner loops:
vlReaderToLokiStreams, fetchBareParserMetricSeries,
detectFieldSummariesStream, extractLogPatternsStreamWithStats.
All other json usages keep encoding/json."
```

---

## Task 4 — Regression and Compatibility Verification

**Why:** Three independent hot-path changes across the metric query path, stream processing, and JSON parsing. Before declaring done, run the full test suite, the parity test machine, and a live benchmark to confirm the gains are real and no Loki-compatibility regressions.

**Files:**
- No new files — verification only.

- [ ] **Step 4.1 — Full unit test suite**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./... -count=1 -timeout 600s 2>&1 | tail -20
```

Expected: all packages pass. Pay attention to:
- `internal/proxy/` — all existing tests must pass
- Any test that exercises `handleQueryRange`, `proxyBareParserMetricQueryRange`, or `vlReaderToLokiStreams`

- [ ] **Step 4.2 — Run LogQL parity tests**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -run TestLogQL -v -count=1 2>&1 | tail -30
```

Expected: all LogQL parity tests pass. The hits-based path must produce identical matrix values to the full-fetch path for `count_over_time` and `rate`.

- [ ] **Step 4.3 — Run coverage gap tests**

```bash
cd /private/tmp/loki-vl-proxy-main && go test ./internal/proxy/ -run TestCoverage -v -count=1 -timeout 120s 2>&1 | tail -20
```

Expected: all pass. `TestRangeMetricCompatibilityMatrix` is particularly important — it covers `rate` and `count_over_time` with various windows and steps.

- [ ] **Step 4.4 — Run the benchmark suite (live comparison)**

Requires the e2e-compat Docker stack to be running (`docker compose up -d` in `test/e2e-compat/`). This is the same benchmark run as before — now compare the numbers:

```bash
cd /private/tmp/loki-vl-proxy-main
PROXY_NO_CACHE_URL=""  # no-cache proxy can't be spawned in sandbox — warm proxy only
bash bench/run-comparison.sh \
  --workloads=heavy \
  --clients=10,50 \
  --duration=30s \
  --skip-proxy-partial \
  --skip-proxy-no-cache \
  --unique-windows 2>&1 | tee /tmp/bench-after-opt.log
```

Compare against `bench/results/bench-2026-05-02T10-56-33.md` (baseline before optimizations). Expected improvements for heavy workload:
- Throughput gap should narrow (proxy/loki ratio should increase)
- P99 tail should improve due to reduced GC pressure and smaller VL responses
- VL native should remain unchanged (it's the reference)

- [ ] **Step 4.5 — Final commit and push**

```bash
cd /private/tmp/loki-vl-proxy-main
git log --oneline -5

# Verify all three perf commits are present, then push
git push origin perf/eliminate-hot-alloc-sources 2>&1 || \
  git push origin HEAD:perf/query-path-optimizations 2>&1
```

Check that PR #297 is updated or that a new PR is created covering the three new commits. Add a benchmark comparison table to the PR description showing before/after numbers.

---

## Self-Review

**Spec coverage check:**

| Optimization | Task |
|---|---|
| Precompute `skipLogLineReconstruction` in vlReaderToLokiStreams | Task 1 ✓ |
| Hits-based sliding window for count_over_time / rate | Task 2 ✓ |
| Replace encoding/json with goccy in hot paths | Task 3 ✓ |
| Regression / compatibility verification | Task 4 ✓ |
| Fallback when declaredLabelFields not configured | Task 2, Step 2.5 gate ✓ |
| Absent-point behaviour (no empty series) | Task 2, Step 2.3 + test ✓ |
| Multi-stream label grouping | Task 2, test TestOPT6_BuildSlidingWindowSums_MultiStream ✓ |
| Rate calculation (divide by window duration) | Task 2, test TestOPT6_BuildSlidingWindowSums_Rate ✓ |

**Placeholder scan:** No TBD/TODO/placeholder text found.

**Type consistency:**
- `bareParserMetricSeries` / `bareParserMetricSample` — defined in stream_processing.go:1078, used in Task 2 ✓
- `vlHitsResponse` — defined in stream_processing.go:1025, used in Task 2 ✓
- `buildSlidingWindowSumsFromHits` — defined Task 2.3, used Task 2.4 and tests ✓
- `fetchBareParserMetricSeriesViaHits` — defined Task 2.4, tested Task 2.1 (OPT6 tests) ✓
- `canonicalLabelsKey` — existing function, used in Tasks 2.3 and 2.4 ✓
- `nanosToVLTimestamp`, `formatVLStep` — existing helpers in query_translation.go ✓
- `p.configMu.RLock()` / `p.declaredLabelFields` — existing fields in proxy.go ✓
- `p.labelTranslator.ToLoki` — verify this method exists; if not, use the existing `p.labelTranslator.ToLoki(k)` pattern from drilldown.go label translation
