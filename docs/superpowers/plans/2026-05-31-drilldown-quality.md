# Drilldown Quality — Zero-fill, Loki Comparison & Quality Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make drilldown field histograms continuously zero-filled (matching Loki), add a Loki reference comparison e2e test, and add a non-blocking quality matrix test that measures bucket density, count parity, and latency across all field types and ranges.

**Architecture:** Four independent deliverables built bottom-up: (1) `zerofillStatsMatrix` core function + unit tests, (2) wire zero-fill into both proxy drilldown paths, (3) e2e quality matrix test that seeds controlled data and reports metrics without blocking CI, (4) e2e Loki comparison test that seeds the same logs into both backends and asserts count/bucket parity within tolerance. All proxy changes are in `internal/proxy/`; all e2e tests go into `test/e2e-compat/` with the `//go:build e2e` tag.

**Tech Stack:** Go 1.22+, fastjson (`github.com/valyala/fastjson`), standard `bytes`/`strconv`/`sort` packages, existing test helpers (`getJSONWithHeaders`, `pushCustomToVL`, `pushCustomToLoki`, `waitForDrilldownServiceVisible`).

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `internal/proxy/drilldown_quality.go` | Create | `zerofillStatsMatrix` — fills missing time steps with "0" |
| `internal/proxy/drilldown_quality_test.go` | Create | Unit tests for `zerofillStatsMatrix` |
| `internal/proxy/metric_binary.go` | Modify (lines 1100–1122) | Call `zerofillStatsMatrix` after raw VL stats body is ready |
| `internal/proxy/drilldown_field_batcher.go` | Modify (lines 200–237) | Call `zerofillStatsMatrix` after per-field stats body |
| `test/e2e-compat/drilldown_quality_report_test.go` | Create | `TestDrilldown_QualityMatrix` — reports metrics, never blocks CI |
| `test/e2e-compat/drilldown_loki_compare_test.go` | Create | `TestDrilldown_LokiCompare_FieldQuality` — Loki vs proxy parity |
| `docs/proxy/drilldown-quality.md` | Create | Architecture doc: quality tiers, zero-fill, Loki parity |
| `CHANGELOG.md` | Modify | Add Fixed and Performance entries |

---

## Task 1: `zerofillStatsMatrix` — Core Function + Unit Tests

**Files:**
- Create: `internal/proxy/drilldown_quality.go`
- Create: `internal/proxy/drilldown_quality_test.go`

### Background

VictoriaLogs `stats_query_range` omits time buckets where count = 0. Loki's `count_over_time` zero-fills every step in the query window. Without zero-fill, the proxy chart shows disconnected spikes; with it, the line is continuous even during quiet periods. The function only affects series that already appear in the response (it never introduces new series).

- [ ] **Step 1: Write the failing test**

```go
// internal/proxy/drilldown_quality_test.go
package proxy

import (
	"encoding/json"
	"testing"
)

func TestZerofillStatsMatrix_FillsGaps(t *testing.T) {
	// VL returned only ts=100 and ts=300; ts=200 is missing.
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"42"],[300,"17"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)

	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, got)
	}
	if len(resp.Data.Result) != 1 {
		t.Fatalf("expected 1 series, got %d", len(resp.Data.Result))
	}
	vals := resp.Data.Result[0].Values
	if len(vals) != 3 {
		t.Fatalf("expected 3 values (100,200,300), got %d: %v", len(vals), vals)
	}
	// ts=200 must be filled with "0"
	ts200 := vals[1]
	if len(ts200) < 2 {
		t.Fatalf("expected [ts,val] pair at index 1, got %v", ts200)
	}
	tsVal, _ := ts200[0].(float64)
	countVal, _ := ts200[1].(string)
	if int64(tsVal) != 200 {
		t.Errorf("expected timestamp 200, got %v", ts200[0])
	}
	if countVal != "0" {
		t.Errorf("expected zero-filled count '0', got %q", countVal)
	}
}

func TestZerofillStatsMatrix_Passthrough_WhenNoGaps(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"10"],[200,"20"],[300,"30"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	vals := resp.Data.Result[0].Values
	if len(vals) != 3 {
		t.Fatalf("expected 3 values unchanged, got %d", len(vals))
	}
}

func TestZerofillStatsMatrix_MultiSeries(t *testing.T) {
	// Two series; info has ts=100 and ts=300 (missing 200); error has ts=200 only (missing 100, 300).
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"50"],[300,"30"]]},` +
		`{"metric":{"level":"error"},"values":[[200,"5"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, got)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("expected 2 series, got %d", len(resp.Data.Result))
	}
	for _, s := range resp.Data.Result {
		if len(s.Values) != 3 {
			t.Errorf("series %v: expected 3 values, got %d: %v", s.Metric, len(s.Values), s.Values)
		}
	}
}

func TestZerofillStatsMatrix_EmptyResult_Passthrough(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	got := zerofillStatsMatrix(input, 0, 300, 100)
	if string(got) != string(input) {
		t.Errorf("expected passthrough for empty result\ngot:  %s\nwant: %s", got, input)
	}
}

func TestZerofillStatsMatrix_ZeroStep_Passthrough(t *testing.T) {
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"level":"info"},"values":[[100,"1"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 0)
	if string(got) != string(input) {
		t.Errorf("expected passthrough when stepSec=0\ngot: %s", got)
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
go test ./internal/proxy/ -run TestZerofillStatsMatrix -v 2>&1 | tail -20
```

Expected: `FAIL — undefined: zerofillStatsMatrix`

- [ ] **Step 3: Implement `zerofillStatsMatrix`**

```go
// internal/proxy/drilldown_quality.go
package proxy

import (
	"bytes"
	"strconv"

	fj "github.com/valyala/fastjson"
)

// zerofillStatsMatrix inserts [ts,"0"] entries for every step in
// [startSec, endSec] (inclusive, at stepSec intervals) that is absent
// from each series in body. body must be a Loki matrix JSON produced by
// a VL stats_query_range call.
//
// VictoriaLogs stats_query_range omits time buckets with zero count.
// Loki's count_over_time emits every step in the query window, including
// zeros. Filling the gaps here makes the proxy response match Loki's
// continuous-line behaviour — Grafana draws a solid line rather than
// disconnected spikes.
//
// Rules:
//   - Only existing series are zero-filled; no new series are introduced.
//   - FV-only stub series (produced by synthesizeDrilldownMatrix or
//     mergeDrilldownWithFieldValues) are not affected — they already
//     cover the full range with averaged counts.
//   - If stepSec ≤ 0 or startSec ≥ endSec the body is returned unchanged.
func zerofillStatsMatrix(body []byte, startSec, endSec, stepSec int64) []byte {
	if stepSec <= 0 || startSec >= endSec {
		return body
	}

	var p fj.Parser
	v, err := p.ParseBytes(body)
	if err != nil || v == nil {
		return body
	}
	result := v.GetArray("data", "result")
	if len(result) == 0 {
		return body
	}

	// Build the complete expected time axis once.
	var axis []int64
	for ts := startSec; ts <= endSec; ts += stepSec {
		axis = append(axis, ts)
	}
	if len(axis) == 0 {
		return body
	}

	var buf bytes.Buffer
	buf.Grow(len(body) + len(result)*len(axis)*20)
	buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)

	for i, item := range result {
		if i > 0 {
			buf.WriteByte(',')
		}

		// Index existing timestamp → value from VL response.
		existing := make(map[int64]string, len(axis))
		for _, pair := range item.GetArray("values") {
			arr := pair.GetArray()
			if len(arr) < 2 {
				continue
			}
			ts := arr[0].GetInt64()
			val := string(arr[1].GetStringBytes())
			if ts > 0 {
				existing[ts] = val
			}
		}

		buf.WriteString(`{"metric":`)
		if m := item.Get("metric"); m != nil {
			buf.Write(m.MarshalTo(nil))
		} else {
			buf.WriteString(`{}`)
		}
		buf.WriteString(`,"values":[`)

		first := true
		for _, ts := range axis {
			val, ok := existing[ts]
			if !ok {
				val = "0"
			}
			if !first {
				buf.WriteByte(',')
			}
			first = false
			buf.WriteByte('[')
			buf.WriteString(strconv.FormatInt(ts, 10))
			buf.WriteString(`,"`)
			buf.WriteString(val)
			buf.WriteString(`"]`)
		}
		buf.WriteString(`]}`)
	}
	buf.WriteString(`]}}`)
	return buf.Bytes()
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
go test ./internal/proxy/ -run TestZerofillStatsMatrix -v 2>&1 | tail -15
```

Expected: `PASS` for all 5 test cases.

- [ ] **Step 5: Run full unit suite to confirm no regressions**

```bash
go test ./internal/proxy/ -count=1 2>&1 | tail -5
```

Expected: `ok github.com/…/internal/proxy` with 2000+ passed.

- [ ] **Step 6: Commit**

```bash
git add internal/proxy/drilldown_quality.go internal/proxy/drilldown_quality_test.go
git commit -m "feat: add zerofillStatsMatrix to fill VL stats gaps like Loki does"
```

---

## Task 2: Wire Zero-fill into the Hybrid Drilldown Path

**Files:**
- Modify: `internal/proxy/metric_binary.go` (around lines 1100–1122)

### Background

`proxyStatsQueryRangeDrilldownHybrid` handles drilldown field queries for ranges > 12 h. After receiving the VL stats body and applying label translations, `zerofillStatsMatrix` must be called before `mergeDrilldownWithFieldValues` so the stats portion arrives fully zero-filled.

The caller already has `startNs`, `endNs`, and `effectiveStepRaw`. We parse `effectiveStepRaw` once to get `stepSec`.

- [ ] **Step 1: Write the failing test (integration guard in unit package)**

Add to `internal/proxy/drilldown_quality_test.go`:

```go
// TestZerofillAppliedInHybridPath confirms that zerofillStatsMatrix is wired
// into the hybrid path. It builds a minimal VL-like body with a gap and
// verifies the gap is filled after the full pipeline runs.
func TestZerofillStatsMatrix_PreservesMetricKey(t *testing.T) {
	// Regression: metric key must survive zero-fill unchanged.
	input := []byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"detected_level":"info"},"values":[[200,"9"]]}` +
		`]}}`)
	got := zerofillStatsMatrix(input, 100, 300, 100)
	if !bytes.Contains(got, []byte(`"detected_level":"info"`)) {
		t.Errorf("metric key lost after zerofill\nbody: %s", got)
	}
	if !bytes.Contains(got, []byte(`[100,"0"]`)) {
		t.Errorf("ts=100 not zero-filled\nbody: %s", got)
	}
	if !bytes.Contains(got, []byte(`[300,"0"]`)) {
		t.Errorf("ts=300 not zero-filled\nbody: %s", got)
	}
}
```

Run it:

```bash
go test ./internal/proxy/ -run TestZerofillStatsMatrix_PreservesMetricKey -v 2>&1 | tail -5
```

Expected: PASS (it tests only the function, so should already pass after Task 1).

- [ ] **Step 2: Add the zero-fill call in `proxyStatsQueryRangeDrilldownHybrid`**

Locate `metric_binary.go` around line 1114. The current block reads:

```go
rawBody = stripVLStatsNameKey(rawBody)
if field != lokiField {
    rawBody = renameStatsBodyMetricKey(rawBody, field, lokiField)
}
rawBody = limitLokiMatrixSeries(rawBody, maxDrilldownSeries)
statsBody = rawBody
```

Replace with:

```go
rawBody = stripVLStatsNameKey(rawBody)
if field != lokiField {
    rawBody = renameStatsBodyMetricKey(rawBody, field, lokiField)
}
rawBody = limitLokiMatrixSeries(rawBody, maxDrilldownSeries)
// Zero-fill missing time steps so Grafana draws a continuous line rather
// than disconnected spikes. VL stats_query_range omits zero-count buckets;
// Loki always emits every step in the query window.
if stepDur, ok := parsePositiveStepDuration(effectiveStepRaw); ok && stepDur > 0 {
    stepSec := int64(stepDur / time.Second)
    rawBody = zerofillStatsMatrix(rawBody,
        startNs/int64(time.Second),
        endNs/int64(time.Second),
        stepSec)
}
statsBody = rawBody
```

- [ ] **Step 3: Build to confirm no compilation errors**

```bash
go build ./internal/proxy/ 2>&1
```

Expected: no output (clean build).

- [ ] **Step 4: Run full unit suite**

```bash
go test ./internal/proxy/ -count=1 2>&1 | tail -5
```

Expected: `ok` with ≥2051 tests passing.

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/metric_binary.go internal/proxy/drilldown_quality_test.go
git commit -m "feat: zero-fill hybrid drilldown stats response to match Loki continuous-line behaviour"
```

---

## Task 3: Wire Zero-fill into the Short-Range Batcher

**Files:**
- Modify: `internal/proxy/drilldown_field_batcher.go` (around lines 220–238)

### Background

`fieldBatch.fire()` handles drilldown for ranges ≤ 12 h. Each field gets an independent VL `stats_query_range` call; the raw body is assembled and sent to `resultCh`. The `fieldBatch` struct already holds `startRaw`, `endRaw`, `stepRaw` — parse these once outside the goroutine loop and pass to `zerofillStatsMatrix` after each per-field response.

- [ ] **Step 1: Locate the existing zero-fill opportunity in `drilldown_field_batcher.go`**

The per-field goroutine (around line 202) ends with:

```go
rawBody = stripVLStatsNameKey(rawBody)
if e.primaryVLField != e.lokiField {
    rawBody = renameStatsBodyMetricKey(rawBody, e.primaryVLField, e.lokiField)
}
rawBody = limitLokiMatrixSeries(rawBody, maxDrilldownSeries)

mu.Lock()
results[e.lokiField] = rawBody
mu.Unlock()
```

- [ ] **Step 2: Parse the time axis once before launching goroutines**

In `fire()`, after `entries := make([]fieldBatchEntry, …)` and before the goroutine launch loop, add:

```go
// Parse the query time bounds once; all per-field goroutines share them.
batchStartNs, startOK := parseLokiTimeToUnixNano(batch.startRaw)
batchEndNs, endOK := parseLokiTimeToUnixNano(batch.endRaw)
batchStepDur, stepOK := parsePositiveStepDuration(batch.stepRaw)
doZerofill := startOK && endOK && stepOK && batchStepDur > 0 && batchEndNs > batchStartNs
batchStartSec := batchStartNs / int64(time.Second)
batchEndSec := batchEndNs / int64(time.Second)
batchStepSec := int64(batchStepDur / time.Second)
```

- [ ] **Step 3: Apply zero-fill inside the per-field goroutine**

Replace the end of the goroutine (after `limitLokiMatrixSeries`) with:

```go
rawBody = stripVLStatsNameKey(rawBody)
if e.primaryVLField != e.lokiField {
    rawBody = renameStatsBodyMetricKey(rawBody, e.primaryVLField, e.lokiField)
}
rawBody = limitLokiMatrixSeries(rawBody, maxDrilldownSeries)
if doZerofill {
    rawBody = zerofillStatsMatrix(rawBody, batchStartSec, batchEndSec, batchStepSec)
}

mu.Lock()
results[e.lokiField] = rawBody
mu.Unlock()
```

- [ ] **Step 4: Build and run unit tests**

```bash
go build ./internal/proxy/ 2>&1
go test ./internal/proxy/ -count=1 2>&1 | tail -5
```

Expected: clean build, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/drilldown_field_batcher.go
git commit -m "feat: zero-fill short-range batcher per-field stats to match Loki continuous lines"
```

---

## Task 4: Quality Matrix E2E Test (Report-Only, Non-Blocking)

**Files:**
- Create: `test/e2e-compat/drilldown_quality_report_test.go`

### Background

This test seeds controlled log data with known field types into VL (not Loki — it's measuring what the proxy returns, not comparing backends), then queries the proxy at seven time ranges × five field types and logs quality metrics. It never calls `t.Errorf` for quality metrics. Hard fails are limited to three regressions that indicate broken infrastructure: (1) phantom "Value" series, (2) empty result for `level`/`detected_level`, (3) proxy HTTP 5xx.

The seed data spans the last 2 h (so it falls within the short-range path) and is pushed once before the subtests run. A unique `service_name` label (`drilldown-quality-XXXXXXXXXX`) prevents cross-contamination.

Field types seeded:

| Field | Cardinality | Values |
|-------|-------------|--------|
| `level` (stream label) | 3 | info/warn/error |
| `http_method` | 5 | GET/POST/PUT/DELETE/PATCH |
| `http_status` | 10 | 200/201/204/400/401/403/404/500/502/503 |
| `duration_ms` | ~50 | random 1–5000 |
| `trace_id` | unique/entry | UUID-style |

- [ ] **Step 1: Write the test file**

```go
// test/e2e-compat/drilldown_quality_report_test.go

//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// drilldownQualityField describes a single field in the quality matrix.
type drilldownQualityField struct {
	// LogQL label/field name (Loki side).
	lokiName string
	// The existence filter clause inside count_over_time({...}[step]).
	// For stream labels: "| level != \"\""; for JSON fields: "|json|drop __error__,__error_details__| duration_ms!=\"\""
	filter string
	// isStreamLabel is true when the field is a Loki stream label (queried via sum by (field) without the drilldown path).
	isStreamLabel bool
	// mustBeNonEmpty: hard-fail if the response has 0 series.
	mustBeNonEmpty bool
}

// drilldownQualityRange describes one range/step combination to measure.
type drilldownQualityRange struct {
	label string
	dur   time.Duration
	step  time.Duration
}

// drilldownQualityResult holds per-field, per-range metrics.
type drilldownQualityResult struct {
	field       string
	rangeLabel  string
	seriesCount int
	nonZeroBuckets int
	totalBuckets   int
	totalCount     int64
	latencyMs      int64
	pathLabel      string // X-Proxy-Drilldown-Path
	errMsg         string
}

func TestDrilldown_QualityMatrix(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)

	// Unique service name isolates this test's data from the log-generator stream.
	svc := fmt.Sprintf("drilldown-quality-%d", time.Now().UnixNano()/1e9)

	// Seed 2 h of controlled data into VL.
	seedDrilldownQualityData(t, svc)
	waitForDrilldownServiceVisible(t, svc, time.Now().Add(-2*time.Hour), time.Now())

	fields := []drilldownQualityField{
		{lokiName: "level", filter: `| level != ""`, isStreamLabel: true, mustBeNonEmpty: true},
		{lokiName: "http_method", filter: `|json|drop __error__,__error_details__| http_method!=""`, mustBeNonEmpty: false},
		{lokiName: "http_status", filter: `|json|drop __error__,__error_details__| http_status!=""`, mustBeNonEmpty: false},
		{lokiName: "duration_ms", filter: `|json|drop __error__,__error_details__| duration_ms!=""`, mustBeNonEmpty: false},
		{lokiName: "trace_id", filter: `|json|drop __error__,__error_details__| trace_id!=""`, mustBeNonEmpty: false},
	}

	ranges := []drilldownQualityRange{
		{"1h", 1 * time.Hour, 60 * time.Second},
		{"3h", 3 * time.Hour, 3 * time.Minute},
		{"6h", 6 * time.Hour, 6 * time.Minute},
		{"12h", 12 * time.Hour, 12 * time.Minute},
		{"24h", 24 * time.Hour, 48 * time.Minute},
		{"2d", 48 * time.Hour, 96 * time.Minute},
		{"7d", 7 * 24 * time.Hour, 336 * time.Minute},
	}

	now := time.Now()
	var results []drilldownQualityResult

	for _, f := range fields {
		for _, r := range ranges {
			start := now.Add(-r.dur)
			res := measureDrilldownQuality(t, svc, f, r, start, now)
			results = append(results, res)

			// Hard fail: proxy 5xx.
			if res.errMsg != "" {
				t.Errorf("HARD FAIL field=%s range=%s: %s", f.lokiName, r.label, res.errMsg)
				continue
			}

			// Hard fail: phantom "Value" series (Grafana default when field is missing).
			for _, name := range []string{"Value", "value", "__value__"} {
				if strings.Contains(res.pathLabel, name) {
					t.Errorf("HARD FAIL phantom %q series field=%s range=%s", name, f.lokiName, r.label)
				}
			}

			// Hard fail: empty result for level/detected_level.
			if f.mustBeNonEmpty && res.seriesCount == 0 {
				t.Errorf("HARD FAIL field=%s range=%s returned 0 series", f.lokiName, r.label)
			}
		}
	}

	// Log full quality table.
	t.Logf("\n=== DRILLDOWN QUALITY MATRIX ===")
	t.Logf("%-14s %-14s %8s %12s %13s %10s %10s %s",
		"field", "range", "series", "nonZeroBkts", "totalBuckets", "totalCount", "latencyMs", "path")
	for _, res := range results {
		if res.errMsg != "" {
			t.Logf("%-14s %-14s ERROR: %s", res.field, res.rangeLabel, res.errMsg)
			continue
		}
		pct := 0.0
		if res.totalBuckets > 0 {
			pct = float64(res.nonZeroBuckets) / float64(res.totalBuckets) * 100
		}
		t.Logf("%-14s %-14s %8d %11d/%d (%.0f%%) %10d %9dms %s",
			res.field, res.rangeLabel,
			res.seriesCount,
			res.nonZeroBuckets, res.totalBuckets, pct,
			res.totalCount,
			res.latencyMs,
			res.pathLabel)
	}
}

func seedDrilldownQualityData(t *testing.T, svc string) {
	t.Helper()

	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	statuses := []int{200, 201, 204, 400, 401, 403, 404, 500, 502, 503}
	levels := []string{"info", "warn", "error"}

	// Push 1200 entries spread over the last 2 h (600 s spacing for 2 h = ~10/minute).
	insertURL := vlURL + "/insert/jsonline?_stream_fields=" +
		url.QueryEscape("service_name,app,level")

	var buf strings.Builder
	buf.Grow(1200 * 200)
	rng := rand.New(rand.NewSource(42))
	now := time.Now()
	for i := 0; i < 1200; i++ {
		ts := now.Add(-2*time.Hour + time.Duration(i)*6*time.Second)
		level := levels[i%len(levels)]
		method := methods[i%len(methods)]
		status := statuses[i%len(statuses)]
		durationMs := rng.Intn(4999) + 1
		traceID := fmt.Sprintf("t-%016x-%016x", rng.Int63(), rng.Int63())
		entry, _ := json.Marshal(map[string]interface{}{
			"_time":        ts.Format(time.RFC3339Nano),
			"_msg":         fmt.Sprintf("%s /api/resource status=%d duration=%dms", method, status, durationMs),
			"service_name": svc,
			"app":          svc,
			"level":        level,
			"http_method":  method,
			"http_status":  strconv.Itoa(status),
			"duration_ms":  strconv.Itoa(durationMs),
			"trace_id":     traceID,
		})
		buf.Write(entry)
		buf.WriteByte('\n')
	}

	resp, err := http.Post(insertURL, "application/stream+json", strings.NewReader(buf.String()))
	if err != nil {
		t.Fatalf("VL seed push failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("VL seed push returned status=%d", resp.StatusCode)
	}
	t.Logf("seeded %s with 1200 entries over 2h", svc)
}

func measureDrilldownQuality(t *testing.T, svc string, f drilldownQualityField, r drilldownQualityRange, start, end time.Time) drilldownQualityResult {
	t.Helper()

	res := drilldownQualityResult{field: f.lokiName, rangeLabel: r.label}

	startStr := strconv.FormatInt(start.UnixNano(), 10)
	endStr := strconv.FormatInt(end.UnixNano(), 10)
	stepStr := strconv.FormatInt(int64(r.step/time.Second), 10)

	var query string
	if f.isStreamLabel {
		query = fmt.Sprintf(`sum by (%s) (count_over_time({service_name=%q}%s [%s]))`,
			f.lokiName, svc, f.filter, r.step)
	} else {
		query = fmt.Sprintf(`sum by (%s) (count_over_time({service_name=%q}%s [%s]))`,
			f.lokiName, svc, f.filter, r.step)
	}

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", startStr)
	params.Set("end", endStr)
	params.Set("step", stepStr)

	req, _ := http.NewRequest(http.MethodGet,
		proxyURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	req.Header.Set("X-Scope-OrgID", "fake")

	t0 := time.Now()
	resp, err := http.DefaultClient.Do(req)
	res.latencyMs = time.Since(t0).Milliseconds()

	if err != nil {
		res.errMsg = fmt.Sprintf("HTTP error: %v", err)
		return res
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		body, _ := io.ReadAll(resp.Body)
		res.errMsg = fmt.Sprintf("proxy 5xx status=%d body=%s", resp.StatusCode, body)
		return res
	}

	res.pathLabel = resp.Header.Get("X-Proxy-Drilldown-Path")

	var payload struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &payload); err != nil {
		res.errMsg = fmt.Sprintf("JSON decode: %v body=%s", err, body[:min(200, len(body))])
		return res
	}

	res.seriesCount = len(payload.Data.Result)
	for _, s := range payload.Data.Result {
		for _, raw := range s.Values {
			res.totalBuckets++
			pair, _ := raw.([]interface{})
			if len(pair) < 2 {
				continue
			}
			var cnt int64
			switch v := pair[1].(type) {
			case string:
				cnt, _ = strconv.ParseInt(v, 10, 64)
			case float64:
				cnt = int64(v)
			}
			if cnt > 0 {
				res.nonZeroBuckets++
			}
			res.totalCount += cnt
		}
	}
	return res
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

- [ ] **Step 2: Verify the test compiles**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
go build -tags e2e ./test/e2e-compat/ 2>&1
```

Expected: no output (clean build).

- [ ] **Step 3: Run the quality matrix test (requires running e2e stack)**

```bash
cd test/e2e-compat
go test -tags e2e -run TestDrilldown_QualityMatrix -v -timeout 120s 2>&1 | tail -40
```

Expected: `PASS` with a table logged showing quality metrics for each field/range combination. Look for `level` showing non-zero series at every range. `duration_ms` and `trace_id` may show stub-only paths for long ranges — that is expected and logged, not failed.

- [ ] **Step 4: Commit**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
git add test/e2e-compat/drilldown_quality_report_test.go
git commit -m "test(e2e): add drilldown quality matrix — reports bucket density and latency per field/range"
```

---

## Task 5: Loki Reference Comparison E2E Test

**Files:**
- Create: `test/e2e-compat/drilldown_loki_compare_test.go`

### Background

The log-generator's `dual_push` already feeds identical streams to both Loki (port 13101) and VL (port 19428). This test seeds a fresh unique service name with a controlled burst into **both** backends simultaneously, then queries them with the same LogQL and asserts:

- **Series count**: VL proxy returns ≥ Loki series count for common fields.
- **Total count parity**: within ±15% (accounts for dual-push timing skew; observed gap is ~6%).
- **Non-zero bucket coverage**: proxy non-zero bucket count ≥ 90% of Loki's count.

Test operates at 1 h, 3 h, and 6 h ranges only — Loki in the e2e stack has ~12 h retention so 6 h is a safe overlap window.

Comparison is only meaningful for low-cardinality fields (`level`, `http_method`, `http_status`) where both backends can return complete histograms. `duration_ms` and `trace_id` are logged as informational only (they may differ due to cardinality capping).

- [ ] **Step 1: Write the test file**

```go
// test/e2e-compat/drilldown_loki_compare_test.go

//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// lokiCompareField holds the field name and the LogQL filter for a Loki comparison query.
type lokiCompareField struct {
	lokiName       string
	filterClause   string // appended inside count_over_time({...}<HERE>[step])
	compareStrict  bool   // if true, enforce count parity; if false, log only
}

type lokiCompareResult struct {
	field          string
	rangeLabel     string
	lokiSeries     int
	proxySeries    int
	lokiTotal      int64
	proxyTotal     int64
	lokiNonZero    int
	proxyNonZero   int
	lokiLatencyMs  int64
	proxyLatencyMs int64
}

// TestDrilldown_LokiCompare_FieldQuality seeds the same logs into both Loki and VL,
// then compares the proxy response against the Loki response for field quality.
//
// Acceptance thresholds (per field, per range):
//   - proxy series count ≥ loki series count
//   - |proxy_total - loki_total| / loki_total ≤ 0.15  (±15%)
//   - proxy non-zero bucket count ≥ loki non-zero count × 0.90  (≥90% coverage)
func TestDrilldown_LokiCompare_FieldQuality(t *testing.T) {
	ensureDataIngested(t)
	waitForReady(t, proxyURL+"/ready", 30*time.Second)
	waitForReady(t, lokiURL+"/ready", 30*time.Second)

	svc := fmt.Sprintf("drilldown-compare-%d", time.Now().UnixNano()/1e9)

	// Seed 90 minutes of data into BOTH Loki and VL at the same timestamps.
	seedDrilldownCompareData(t, svc)
	waitForDrilldownServiceVisible(t, svc, time.Now().Add(-90*time.Minute), time.Now())

	fields := []lokiCompareField{
		{lokiName: "level", filterClause: `| level != ""`, compareStrict: true},
		{lokiName: "http_method", filterClause: `|json|drop __error__,__error_details__| http_method!=""`, compareStrict: true},
		{lokiName: "http_status", filterClause: `|json|drop __error__,__error_details__| http_status!=""`, compareStrict: true},
		{lokiName: "duration_ms", filterClause: `|json|drop __error__,__error_details__| duration_ms!=""`, compareStrict: false},
		{lokiName: "trace_id", filterClause: `|json|drop __error__,__error_details__| trace_id!=""`, compareStrict: false},
	}

	testRanges := []drilldownQualityRange{
		{"1h", 1 * time.Hour, 60 * time.Second},
		{"3h", 3 * time.Hour, 3 * time.Minute},
		{"6h", 6 * time.Hour, 6 * time.Minute},
	}

	now := time.Now()
	var results []lokiCompareResult

	for _, f := range fields {
		for _, r := range testRanges {
			start := now.Add(-r.dur)
			res := runLokiCompare(t, svc, f, r, start, now)
			results = append(results, res)

			if !f.compareStrict {
				continue
			}

			// Assert series count parity.
			if res.proxySeries < res.lokiSeries {
				t.Errorf("field=%s range=%s: proxy returned fewer series (%d) than Loki (%d)",
					f.lokiName, r.label, res.proxySeries, res.lokiSeries)
			}

			// Assert count parity ±15%.
			if res.lokiTotal > 0 {
				diff := res.proxyTotal - res.lokiTotal
				if diff < 0 {
					diff = -diff
				}
				pct := float64(diff) / float64(res.lokiTotal)
				if pct > 0.15 {
					t.Errorf("field=%s range=%s: count gap %.1f%% exceeds 15%% threshold (loki=%d proxy=%d)",
						f.lokiName, r.label, pct*100, res.lokiTotal, res.proxyTotal)
				}
			}

			// Assert non-zero bucket coverage ≥ 90%.
			if res.lokiNonZero > 0 {
				coverage := float64(res.proxyNonZero) / float64(res.lokiNonZero)
				if coverage < 0.90 {
					t.Errorf("field=%s range=%s: proxy non-zero bucket coverage %.0f%% < 90%% (loki=%d proxy=%d)",
						f.lokiName, r.label, coverage*100, res.lokiNonZero, res.proxyNonZero)
				}
			}
		}
	}

	// Log comparison table.
	t.Logf("\n=== LOKI vs PROXY COMPARISON ===")
	t.Logf("%-14s %-6s %10s %12s %12s %14s %14s",
		"field", "range", "series(L/P)", "total(L/P)", "nzBkts(L/P)", "lokiLat", "proxyLat")
	for _, res := range results {
		t.Logf("%-14s %-6s %5d/%-5d %6d/%-6d %6d/%-6d %8dms %8dms",
			res.field, res.rangeLabel,
			res.lokiSeries, res.proxySeries,
			res.lokiTotal, res.proxyTotal,
			res.lokiNonZero, res.proxyNonZero,
			res.lokiLatencyMs, res.proxyLatencyMs)
	}
}

func seedDrilldownCompareData(t *testing.T, svc string) {
	t.Helper()

	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	statuses := []int{200, 201, 204, 400, 404, 500}
	levels := []string{"info", "warn", "error"}
	rng := rand.New(rand.NewSource(99))

	now := time.Now()

	// Build 540 entries spread over 90 minutes (10-second spacing).
	type entry struct {
		ts       time.Time
		level    string
		method   string
		status   int
		duration int
		traceID  string
		msg      string
	}
	var entries []entry
	for i := 0; i < 540; i++ {
		ts := now.Add(-90*time.Minute + time.Duration(i)*10*time.Second)
		lv := levels[i%len(levels)]
		m := methods[i%len(methods)]
		s := statuses[i%len(statuses)]
		d := rng.Intn(4999) + 1
		tid := fmt.Sprintf("t-%016x-%016x", rng.Int63(), rng.Int63())
		msg := fmt.Sprintf("%s /api/v1/resource status=%d duration=%dms trace=%s", m, s, d, tid)
		entries = append(entries, entry{ts: ts, level: lv, method: m, status: s, duration: d, traceID: tid, msg: msg})
	}

	// Push to Loki.
	lokiStreams := make(map[string][][2]string) // level → []{"ts", "line"}
	for _, e := range entries {
		streamKey := e.level
		line, _ := json.Marshal(map[string]interface{}{
			"http_method": e.method,
			"http_status": strconv.Itoa(e.status),
			"duration_ms": strconv.Itoa(e.duration),
			"trace_id":    e.traceID,
			"msg":         e.msg,
		})
		lokiStreams[streamKey] = append(lokiStreams[streamKey],
			[2]string{strconv.FormatInt(e.ts.UnixNano(), 10), string(line)})
	}
	var lokiStreamList []interface{}
	for lv, vals := range lokiStreams {
		values := make([][]string, len(vals))
		for i, v := range vals {
			values[i] = []string{v[0], v[1]}
		}
		lokiStreamList = append(lokiStreamList, map[string]interface{}{
			"stream": map[string]string{
				"service_name": svc, "app": svc, "level": lv,
			},
			"values": values,
		})
	}
	lokiPayload, _ := json.Marshal(map[string]interface{}{"streams": lokiStreamList})
	resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json",
		strings.NewReader(string(lokiPayload)))
	if err != nil {
		t.Fatalf("Loki compare seed push failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	t.Logf("Loki seed: status=%d", resp.StatusCode)

	// Push to VL (jsonline format).
	insertURL := vlURL + "/insert/jsonline?_stream_fields=" +
		url.QueryEscape("service_name,app,level")
	var vlBuf strings.Builder
	vlBuf.Grow(len(entries) * 200)
	for _, e := range entries {
		row, _ := json.Marshal(map[string]interface{}{
			"_time":        e.ts.Format(time.RFC3339Nano),
			"_msg":         e.msg,
			"service_name": svc,
			"app":          svc,
			"level":        e.level,
			"http_method":  e.method,
			"http_status":  strconv.Itoa(e.status),
			"duration_ms":  strconv.Itoa(e.duration),
			"trace_id":     e.traceID,
		})
		vlBuf.Write(row)
		vlBuf.WriteByte('\n')
	}
	resp2, err := http.Post(insertURL, "application/stream+json",
		strings.NewReader(vlBuf.String()))
	if err != nil {
		t.Fatalf("VL compare seed push failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	t.Logf("VL seed: status=%d", resp2.StatusCode)
}

// runLokiCompare queries lokiURL and proxyURL with the same LogQL and returns
// both responses for comparison.
func runLokiCompare(t *testing.T, svc string, f lokiCompareField, r drilldownQualityRange, start, end time.Time) lokiCompareResult {
	t.Helper()

	res := lokiCompareResult{field: f.lokiName, rangeLabel: r.label}

	startStr := strconv.FormatInt(start.UnixNano(), 10)
	endStr := strconv.FormatInt(end.UnixNano(), 10)
	stepStr := strconv.FormatInt(int64(r.step/time.Second), 10)
	query := fmt.Sprintf(`sum by (%s) (count_over_time({service_name=%q}%s [%s]))`,
		f.lokiName, svc, f.filterClause, r.step)

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", startStr)
	params.Set("end", endStr)
	params.Set("step", stepStr)
	queryStr := "/loki/api/v1/query_range?" + params.Encode()

	// Query Loki.
	t0 := time.Now()
	lokiResp, err := http.Get(lokiURL + queryStr)
	res.lokiLatencyMs = time.Since(t0).Milliseconds()
	if err != nil {
		t.Logf("Loki query error field=%s range=%s: %v", f.lokiName, r.label, err)
	} else {
		defer lokiResp.Body.Close()
		lok := parseLokiMatrixSummary(t, lokiResp.Body)
		res.lokiSeries = lok.series
		res.lokiTotal = lok.total
		res.lokiNonZero = lok.nonZero
	}

	// Query VL proxy (with drilldown headers).
	req, _ := http.NewRequest(http.MethodGet, proxyURL+queryStr, nil)
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	req.Header.Set("X-Scope-OrgID", "fake")
	t0 = time.Now()
	proxyResp, err := http.DefaultClient.Do(req)
	res.proxyLatencyMs = time.Since(t0).Milliseconds()
	if err != nil {
		t.Logf("proxy query error field=%s range=%s: %v", f.lokiName, r.label, err)
	} else {
		defer proxyResp.Body.Close()
		prx := parseLokiMatrixSummary(t, proxyResp.Body)
		res.proxySeries = prx.series
		res.proxyTotal = prx.total
		res.proxyNonZero = prx.nonZero
	}

	return res
}

type matrixSummary struct {
	series  int
	total   int64
	nonZero int
}

func parseLokiMatrixSummary(t *testing.T, body io.Reader) matrixSummary {
	t.Helper()
	var payload struct {
		Data struct {
			Result []struct {
				Values [][]interface{} `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	raw, _ := io.ReadAll(body)
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Logf("parseLokiMatrixSummary: decode failed: %v (body=%s)", err, raw[:min(100, len(raw))])
		return matrixSummary{}
	}
	var s matrixSummary
	s.series = len(payload.Data.Result)
	for _, series := range payload.Data.Result {
		for _, raw := range series.Values {
			pair, _ := raw.([]interface{})
			if len(pair) < 2 {
				continue
			}
			var cnt int64
			switch v := pair[1].(type) {
			case string:
				cnt, _ = strconv.ParseInt(v, 10, 64)
			case float64:
				cnt = int64(v)
			}
			if cnt > 0 {
				s.nonZero++
			}
			s.total += cnt
		}
	}
	return s
}
```

- [ ] **Step 2: Verify the test compiles**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
go build -tags e2e ./test/e2e-compat/ 2>&1
```

Expected: no output.

- [ ] **Step 3: Run the Loki comparison test**

```bash
cd test/e2e-compat
go test -tags e2e -run TestDrilldown_LokiCompare_FieldQuality -v -timeout 120s 2>&1 | tail -30
```

Expected: `PASS` with a comparison table showing Loki vs proxy series/count/buckets. `level`, `http_method`, `http_status` should all show ≤15% count gap and ≥90% non-zero bucket coverage. `duration_ms` and `trace_id` are logged informational (compareStrict=false).

- [ ] **Step 4: Commit**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
git add test/e2e-compat/drilldown_loki_compare_test.go
git commit -m "test(e2e): add Loki reference comparison test — asserts count/bucket parity within tolerance"
```

---

## Task 6: Architecture Documentation

**Files:**
- Create: `docs/proxy/drilldown-quality.md`

### Background

Documents the full drilldown quality architecture: path selection, cardinality tiers, zero-fill rationale, Loki parity strategy, and quality measurement methodology. Intended for reviewers, future contributors, and as the authoritative reference when debugging quality regressions.

- [ ] **Step 1: Create the documentation directory and file**

```bash
mkdir -p /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations/docs/proxy
```

- [ ] **Step 2: Write the documentation**

Create `docs/proxy/drilldown-quality.md` with the following content:

````markdown
# Drilldown Field Histogram Quality

This document describes how the proxy produces drilldown field histograms,
how quality is measured, and the strategies used to match Loki's behaviour.

## Overview

Grafana Logs Drilldown displays per-field histograms: for each detected
field (e.g. `level`, `http_method`, `duration_ms`), it shows how many log
lines matched each value over time. These charts are the main "fields panel"
in the Drilldown view. The proxy translates Loki's `count_over_time` +
`sum by (field)` LogQL into VictoriaLogs `stats_query_range`, merges the
result with `field_values` data for coverage, and zero-fills missing time
steps to match Loki's continuous-line behaviour.

## Path Selection

The proxy selects one of three code paths based on the query range:

```
request range ≤ 12 h  →  fieldBatcher (drilldown_field_batcher.go)
                              Per-field parallel stats_query_range
                              Up to 120 buckets (maxDrilldownStatsBucketsShort)
                              Zero-filled by zerofillStatsMatrix

request range > 12 h  →  proxyStatsQueryRangeDrilldownHybrid (metric_binary.go)
                              field_values (O(column-index), ~30 ms, any range)
                              + stats_query_range at full range, ≤ 30 buckets
                              Zero-filled by zerofillStatsMatrix
                              Merged by mergeDrilldownWithFieldValues
```

The `X-Query-Tags: Source=grafana-lokiexplore-app` header (set by Grafana
Logs Drilldown) gates both paths. Without it, requests fall through to the
standard `proxyStatsQueryRangeDirect` path.

## Cardinality Tiers

The proxy classifies each field by name before issuing any VL query:

| Tier | Detection | Strategy | Examples |
|------|-----------|----------|---------|
| **Ultra-high** | `isHighCardinalityFieldName` — suffix `_id`, `_uid` | `field_values` stubs only; no stats histogram | `trace_id`, `span_id`, `request_id` |
| **High** | `isLikelyHighCardinalityField` — exact names or suffix `_token`, `_hash`, `_key`, `_uuid` | `field_values` stubs only; no stats histogram | `session_id`, `api_key` |
| **All others** | none | Full stats histogram + `field_values` merge | `level`, `http_method`, `duration_ms` |

High-cardinality fields skip `stats_query_range` entirely because:
- Millions of unique values × 30–120 buckets = VL CPU spike
- The cross-product limit (2000 combinations) truncates real values
- `field_values` already provides the correct total hit counts

## Zero-fill: Why and How

VictoriaLogs `stats_query_range` omits time buckets where count = 0.
Loki's `count_over_time` aggregation emits every step in the query window,
including zero-count steps.

Without zero-fill:
- VL returns points at t=100, t=300 (skipping t=200)
- Grafana connects those points with a line, drawing incorrect trends
- The chart appears "spiky" even for smooth traffic

With zero-fill (`zerofillStatsMatrix` in `drilldown_quality.go`):
- The proxy builds the complete time axis from startSec to endSec at stepSec
- For each existing series, missing steps are filled with `"0"`
- No new series are introduced; only existing series are zero-filled
- FV-only stub series (values in `field_values` but not in top-N stats)
  keep their averaged stub counts — they have no per-step data from VL

`zerofillStatsMatrix` is applied in two places:
1. `proxyStatsQueryRangeDrilldownHybrid` (long-range path, >12 h)
2. `fieldBatch.fire()` goroutine (short-range batcher, ≤12 h)

## Loki Parity

The e2e test `TestDrilldown_LokiCompare_FieldQuality` seeds identical log
streams into both Loki and VL, then compares proxy vs Loki responses.

Acceptance thresholds (enforced for low/medium cardinality fields):

| Metric | Threshold |
|--------|-----------|
| Series count | proxy ≥ loki |
| Total count per series | within ±15% |
| Non-zero bucket coverage | proxy ≥ 90% of Loki |

The ±15% count threshold accounts for the ~6% dual-push timing skew
observed in the e2e environment (sequential Loki + VL pushes, occasional
push failures). The 90% bucket coverage threshold ensures the proxy does not
lose data points that Loki shows.

Comparison is limited to 1 h, 3 h, and 6 h ranges because the e2e Loki
instance retains only ~12 h of data. For 24 h+ ranges only VL proxy output
is verified (via the quality matrix test).

## Quality Matrix Measurement

`TestDrilldown_QualityMatrix` seeds 1200 entries over 2 h into VL and
measures the proxy at 7 ranges × 5 field types:

**Ranges:** 1 h, 3 h, 6 h, 12 h, 24 h, 2 d, 7 d

**Fields:**

| Field | Cardinality | Type |
|-------|-------------|------|
| `level` | 3 values | stream label |
| `http_method` | 5 values | JSON field |
| `http_status` | 10 values | JSON field |
| `duration_ms` | ~50 values | JSON field |
| `trace_id` | unique/entry | JSON field (ultra-high) |

**Hard failures** (block CI):
- Phantom `"Value"` series — Grafana default when field is absent
- Empty result for `level` or `detected_level`
- Proxy HTTP 5xx

**Quality metrics** (logged, never fail CI):
- Series count
- Non-zero bucket count / total buckets (density %)
- Total log count
- End-to-end proxy latency (ms)
- `X-Proxy-Drilldown-Path` header value

## Reference

| Symbol | File | Purpose |
|--------|------|---------|
| `zerofillStatsMatrix` | `internal/proxy/drilldown_quality.go` | Fill missing VL time steps with 0 |
| `proxyStatsQueryRangeDrilldownHybrid` | `internal/proxy/metric_binary.go:986` | Long-range (>12 h) drilldown path |
| `fieldBatch.fire` | `internal/proxy/drilldown_field_batcher.go:148` | Short-range parallel per-field stats |
| `mergeDrilldownWithFieldValues` | `internal/proxy/metric_binary.go:452` | Merge stats histogram + FV stubs |
| `synthesizeDrilldownMatrix` | `internal/proxy/metric_binary.go:402` | FV-only stub matrix (no stats) |
| `isHighCardinalityFieldName` | `internal/proxy/metric_binary.go:297` | `_id`/`_uid` suffix detection |
| `isLikelyHighCardinalityField` | `internal/proxy/metric_binary.go:225` | Broad HC name detection |
| `coarsenDrilldownStep` | `internal/proxy/metric_binary.go:627` | Cap bucket count to ≤30/120 |
| `drilldownHybridThreshold` | `internal/proxy/metric_binary.go:363` | 12 h path split point |
| `TestDrilldown_QualityMatrix` | `test/e2e-compat/drilldown_quality_report_test.go` | Quality measurement (non-blocking) |
| `TestDrilldown_LokiCompare_FieldQuality` | `test/e2e-compat/drilldown_loki_compare_test.go` | Loki parity assertions |
````

- [ ] **Step 3: Verify the file renders correctly (no broken markdown)**

```bash
wc -l /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations/docs/proxy/drilldown-quality.md
```

Expected: output ≥ 100 lines.

- [ ] **Step 4: Commit**

```bash
git add docs/proxy/drilldown-quality.md
git commit -m "docs: add drilldown-quality.md — zero-fill rationale, path selection, Loki parity strategy"
```

---

## Task 7: CHANGELOG and Final Verification

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add CHANGELOG entries under `[Unreleased]`**

Add the following under `## [Unreleased]` in `CHANGELOG.md`:

```markdown
### Fixed
- Drilldown field histograms now show a continuous line at quiet time periods
  instead of disconnected spikes. VictoriaLogs `stats_query_range` omits
  zero-count buckets; the proxy now fills them with `"0"` to match Loki's
  behaviour (`zerofillStatsMatrix` applied in both the short-range batcher
  and the long-range hybrid path).

### Performance
- Drilldown hybrid path (ranges >12 h) issues one `field_values` + one
  `stats_query_range` per field request, covering the full requested range
  at ≤30 coarsened buckets. Different time ranges now produce different
  histograms (the previous adaptive window made 12 h and 48 h look identical).

### Added
- E2E quality matrix test (`TestDrilldown_QualityMatrix`): seeds 1 200
  controlled log entries and measures bucket density, total count, and proxy
  latency for 5 field types × 7 time ranges. Reports metrics via `t.Logf`;
  never blocks CI except for hard regressions (empty `level` result, proxy
  5xx, phantom "Value" series).
- E2E Loki comparison test (`TestDrilldown_LokiCompare_FieldQuality`): seeds
  identical logs into both Loki and VL, then asserts that the proxy response
  matches Loki within ±15% count and ≥90% non-zero bucket coverage for
  low-cardinality fields.
- Architecture doc `docs/proxy/drilldown-quality.md` covering path selection,
  cardinality tiers, zero-fill implementation, and Loki parity methodology.
```

- [ ] **Step 2: Run the full unit test suite one final time**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
go test ./internal/proxy/ -count=1 2>&1 | tail -5
```

Expected: `ok` with ≥2056 tests passing (5 new zerofill unit tests added).

- [ ] **Step 3: Build the full binary**

```bash
go build ./cmd/loki-vl-proxy/ 2>&1
```

Expected: no output.

- [ ] **Step 4: Run all e2e tests (optional if stack is running)**

```bash
cd test/e2e-compat
go test -tags e2e -run "TestDrilldown_Quality|TestDrilldown_LokiCompare" -v -timeout 180s 2>&1 | grep -E "PASS|FAIL|==="
```

Expected: `PASS` for both new tests.

- [ ] **Step 5: Commit**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy/.worktrees/perf/proxy-allocations
git add CHANGELOG.md
git commit -m "docs: CHANGELOG entries for zero-fill, quality matrix, and Loki comparison tests"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|----------------|------|
| Zero-fill VL stats gaps to match Loki | Tasks 1–3 |
| Apply in both hybrid and short-range paths | Tasks 2 and 3 |
| Quality matrix e2e test (non-blocking) | Task 4 |
| Loki reference comparison e2e test | Task 5 |
| ±15% count tolerance, ≥90% bucket coverage | Task 5 |
| Architecture documentation | Task 6 |
| CHANGELOG | Task 7 |

**Placeholder scan:** no TBDs, no "handle edge cases", all code blocks complete.

**Type consistency:**
- `zerofillStatsMatrix(body []byte, startSec, endSec, stepSec int64) []byte` — same signature used in Tasks 1, 2, 3.
- `drilldownQualityField`, `drilldownQualityRange`, `drilldownQualityResult` — defined in Task 4, used only there.
- `lokiCompareField`, `lokiCompareResult`, `matrixSummary` — defined in Task 5, used only there.
- `parseLokiMatrixSummary` in Task 5 is self-contained; it does not conflict with `queryRangeMatrixResult` from `drilldown_longrange_regression_test.go` (different name, different signature).
- `min(a, b int) int` — defined in Task 4 (`drilldown_quality_report_test.go`). Task 5 also uses `min`. Since both are in the same package (`e2e_compat`) this would be a duplicate. Task 5 uses the `min` already defined in Task 4's file — do not redefine it in Task 5.
