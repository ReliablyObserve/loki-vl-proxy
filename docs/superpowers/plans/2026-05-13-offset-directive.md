# LogQL `offset` Directive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the LogQL `offset` directive so that `rate({app}[5m] offset 1h)` correctly shifts the query window backward in time instead of silently stripping the clause.

**Architecture:** Extract the offset duration from the LogQL query string before translation, strip the clause, and shift the `start`/`end` (range queries) or `time` (instant queries) form parameters backward by the offset amount on the cloned request. All downstream dispatch paths — fast-path `stats_query_range`, slow-path manual log fetch, bare-parser metric, binary metric — pick up the shifted times from the request form values, so no per-path changes are needed.

**Tech Stack:** Go, `regexp`, existing helpers `parseLokiDuration`, `parseLokiTimeToUnixNano`, `nanosToVLTimestamp` in `internal/proxy/`.

---

## File Map

| File | Change |
|---|---|
| `internal/proxy/query_translation.go` | Add `logqlOffsetRE` regex and `extractLogQLOffset()` |
| `internal/proxy/proxy.go` | Apply offset in `handleQueryRange` and `handleQuery` |
| `internal/proxy/offset_test.go` | New: unit tests for `extractLogQLOffset` |
| `internal/proxy/offset_integration_test.go` | New: integration tests for time shifting in handlers |
| `internal/translator/roadmap_test.go` | Update offset tests: assert stripping now works correctly |
| `docs/translation-reference.md` | Mark `offset` as implemented (line 133) |
| `docs/KNOWN_ISSUES.md` | Remove `offset` from "Current Behavioral Differences", add to "What Is No Longer an Open Gap" |

---

### Task 1: `extractLogQLOffset()` — parse and strip offset from a LogQL string

**Files:**
- Modify: `internal/proxy/query_translation.go` (add near other query-transformation helpers)
- Create: `internal/proxy/offset_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/proxy/offset_test.go`:

```go
package proxy

import (
	"testing"
	"time"
)

func TestExtractLogQLOffset(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantOffset  time.Duration
		wantQuery   string
		wantErr     bool
	}{
		{
			name:       "no offset",
			input:      `rate({app="nginx"}[5m])`,
			wantOffset: 0,
			wantQuery:  `rate({app="nginx"}[5m])`,
		},
		{
			name:       "simple 1h offset",
			input:      `rate({app="nginx"}[5m] offset 1h)`,
			wantOffset: time.Hour,
			wantQuery:  `rate({app="nginx"}[5m])`,
		},
		{
			name:       "30m offset on count_over_time",
			input:      `count_over_time({app="nginx"}[5m] offset 30m)`,
			wantOffset: 30 * time.Minute,
			wantQuery:  `count_over_time({app="nginx"}[5m])`,
		},
		{
			name:       "outer aggregation with offset",
			input:      `sum by (level) (count_over_time({app="api"}[5m] offset 1h))`,
			wantOffset: time.Hour,
			wantQuery:  `sum by (level) (count_over_time({app="api"}[5m]))`,
		},
		{
			name:       "negative offset",
			input:      `rate({app="nginx"}[5m] offset -30m)`,
			wantOffset: -30 * time.Minute,
			wantQuery:  `rate({app="nginx"}[5m])`,
		},
		{
			name:       "1d offset",
			input:      `count_over_time({app="nginx"}[1h] offset 1d)`,
			wantOffset: 24 * time.Hour,
			wantQuery:  `count_over_time({app="nginx"}[1h])`,
		},
		{
			name:    "multiple different offsets error",
			input:   `rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 2h)`,
			wantErr: true,
		},
		{
			name:       "same offset repeated is ok",
			input:      `rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 1h)`,
			wantOffset: time.Hour,
			wantQuery:  `rate({app="a"}[5m]) + rate({app="b"}[5m])`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, stripped, err := extractLogQLOffset(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if offset != tt.wantOffset {
				t.Errorf("offset: got %v, want %v", offset, tt.wantOffset)
			}
			if stripped != tt.wantQuery {
				t.Errorf("stripped query:\n  got  %q\n  want %q", stripped, tt.wantQuery)
			}
		})
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy
go test ./internal/proxy/ -run TestExtractLogQLOffset -v 2>&1 | tail -10
```

Expected: `FAIL — undefined: extractLogQLOffset`

- [ ] **Step 3: Add `logqlOffsetRE` and `extractLogQLOffset` to `query_translation.go`**

Open `internal/proxy/query_translation.go`. Find the `var (` block near the top (around line 13 where other regexp vars are declared). Add:

```go
// logqlOffsetRE matches the "offset <duration>" clause that appears after a
// range window bracket, e.g. "[5m] offset 1h". The capture group is the
// duration string. Supports negative offsets: "[5m] offset -30m".
var logqlOffsetRE = regexp.MustCompile(`\]\s+offset\s+(-?[\w.]+)`)
```

Then, after the last existing standalone function in the file (search for a good spot near other query-string helpers), add:

```go
// extractLogQLOffset finds a LogQL offset modifier (e.g. "[5m] offset 1h"),
// strips all occurrences from the query, and returns the offset duration.
// Returns an error when multiple *different* offset values are present — Loki
// rejects such queries. Zero duration + unchanged query when no offset found.
func extractLogQLOffset(logql string) (time.Duration, string, error) {
	matches := logqlOffsetRE.FindAllStringSubmatch(logql, -1)
	if len(matches) == 0 {
		return 0, logql, nil
	}

	seen := map[string]time.Duration{}
	for _, m := range matches {
		durStr := m[1]
		if _, already := seen[durStr]; !already {
			seen[durStr] = parseLokiDuration(durStr)
		}
	}
	if len(seen) > 1 {
		return 0, logql, fmt.Errorf("found %d offsets while expecting at most 1", len(seen))
	}

	var offset time.Duration
	for _, d := range seen {
		offset = d
	}
	stripped := logqlOffsetRE.ReplaceAllString(logql, "]")
	return offset, strings.TrimSpace(stripped), nil
}
```

- [ ] **Step 4: Run tests — confirm pass**

```bash
go test ./internal/proxy/ -run TestExtractLogQLOffset -v 2>&1 | tail -15
```

Expected: all 8 subtests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/query_translation.go internal/proxy/offset_test.go
git commit -m "feat(offset): add extractLogQLOffset — parse and strip LogQL offset clause"
```

---

### Task 2: Apply offset in `handleQueryRange` — shift start/end on the cloned request

**Files:**
- Modify: `internal/proxy/proxy.go` (lines ~1450–1470 in `handleQueryRange`)
- Create: `internal/proxy/offset_integration_test.go`

- [ ] **Step 1: Write the failing integration test**

Create `internal/proxy/offset_integration_test.go`:

```go
package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestQueryRange_OffsetShiftsTimeWindow(t *testing.T) {
	// rate({app="nginx"}[5m] offset 1h) with start=T end=T+30m step=60
	// The proxy must query VL with start=T-1h end=T+30m-1h (both shifted back 1h).
	base := time.Unix(1700000000, 0).UTC()
	offset := time.Hour

	var gotStart, gotEnd string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			gotStart = r.FormValue("start")
			gotEnd = r.FormValue("end")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		default:
			if r.URL.Path != "/metrics" {
				t.Logf("unhandled: %s", r.URL.Path)
			}
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="nginx"}[5m] offset 1h)`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	wantStart := strconv.FormatInt(base.Add(-offset).Unix(), 10)
	wantEnd := strconv.FormatInt(base.Add(30*time.Minute).Add(-offset).Unix(), 10)

	if gotStart != wantStart {
		t.Errorf("start: got %s want %s (diff %s)", gotStart, wantStart, gotStart)
	}
	// VL extends end by one step; allow up to +step tolerance.
	gotEndNs, _ := strconv.ParseInt(gotEnd, 10, 64)
	wantEndNs, _ := strconv.ParseInt(wantEnd, 10, 64)
	if gotEndNs < wantEndNs || gotEndNs > wantEndNs+60 {
		t.Errorf("end: got %s want ~%s", gotEnd, wantEnd)
	}
}

func TestQueryRange_NoOffsetUnchanged(t *testing.T) {
	// Verify that queries without offset leave start/end untouched.
	base := time.Unix(1700000000, 0).UTC()

	var gotStart string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			_ = r.ParseForm()
			gotStart = r.FormValue("start")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		} else {
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="nginx"}[5m])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	wantStart := strconv.FormatInt(base.Unix(), 10)
	if gotStart != wantStart {
		t.Errorf("start should be unmodified: got %s want %s", gotStart, wantStart)
	}
}

func TestQueryRange_MultipleOffsetReturns400(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", fmt.Sprintf(`rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 2h)`))
	params.Set("start", "1700000000")
	params.Set("end", "1700001800")
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for multiple offsets, got %d: %s", rec.Code, rec.Body.String())
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./internal/proxy/ -run "TestQueryRange_Offset|TestQueryRange_NoOffsetUnchanged|TestQueryRange_MultipleOffset" -v 2>&1 | tail -20
```

Expected: tests run but `TestQueryRange_OffsetShiftsTimeWindow` fails — `gotStart` equals the unshifted value; `TestQueryRange_MultipleOffsetReturns400` gets 200 instead of 400.

- [ ] **Step 3: Apply offset in `handleQueryRange`**

Open `internal/proxy/proxy.go`. Find the section after `r = p.injectAuthFingerprint(r)` (around line 1450) and after `preferWorkingParser`. The block looks like:

```go
logqlQuery = resolveGrafanaRangeTemplateTokens(logqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

if spec, ok := parseBareParserMetricCompatSpec(logqlQuery); ok {
```

Insert between `preferWorkingParser` and `parseBareParserMetricCompatSpec`:

```go
	// Extract and apply LogQL offset: shift start/end backward by the offset so all
	// downstream dispatch paths (stats_query_range, manual log fetch, bare-parser)
	// query the correct historical window. The offset clause is stripped from the
	// query so the translator receives offset-free LogQL.
	{
		offsetDur, strippedQuery, offsetErr := extractLogQLOffset(logqlQuery)
		if offsetErr != nil {
			p.writeError(w, http.StatusBadRequest, offsetErr.Error())
			p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
			return
		}
		logqlQuery = strippedQuery
		if offsetDur != 0 {
			_ = r.ParseForm()
			if startNs, ok := parseLokiTimeToUnixNano(r.FormValue("start")); ok {
				r.Form.Set("start", nanosToVLTimestamp(startNs-offsetDur.Nanoseconds()))
			}
			if endNs, ok := parseLokiTimeToUnixNano(r.FormValue("end")); ok {
				r.Form.Set("end", nanosToVLTimestamp(endNs-offsetDur.Nanoseconds()))
			}
		}
	}
```

- [ ] **Step 4: Run tests — confirm pass**

```bash
go test ./internal/proxy/ -run "TestQueryRange_Offset|TestQueryRange_NoOffsetUnchanged|TestQueryRange_MultipleOffset" -v 2>&1 | tail -20
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Run full proxy test suite — confirm no regressions**

```bash
go test ./internal/proxy/... 2>&1 | tail -5
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/proxy/proxy.go internal/proxy/offset_integration_test.go
git commit -m "feat(offset): apply LogQL offset in handleQueryRange — shift start/end by offset"
```

---

### Task 3: Apply offset in `handleQuery` — shift `time` for instant queries

**Files:**
- Modify: `internal/proxy/proxy.go` (lines ~1609–1632 in `handleQuery`)
- Modify: `internal/proxy/offset_integration_test.go` (add instant query test)

- [ ] **Step 1: Write the failing test**

Add to `internal/proxy/offset_integration_test.go`:

```go
func TestQuery_OffsetShiftsTime(t *testing.T) {
	// sum(count_over_time({app="nginx"}[5m] offset 1h)) at time T
	// The proxy must query VL with time=T-1h.
	evalTime := time.Unix(1700000000, 0).UTC()
	offset := time.Hour

	var gotTime string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			gotTime = r.FormValue("time")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		default:
			if r.URL.Path != "/metrics" {
				t.Logf("unhandled: %s", r.URL.Path)
			}
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `sum(count_over_time({app="nginx"}[5m] offset 1h))`)
	params.Set("time", strconv.FormatInt(evalTime.Unix(), 10))
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	wantTime := strconv.FormatInt(evalTime.Add(-offset).Unix(), 10)
	if gotTime != wantTime {
		t.Errorf("time: got %s want %s", gotTime, wantTime)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./internal/proxy/ -run TestQuery_OffsetShiftsTime -v 2>&1 | tail -10
```

Expected: FAIL — `gotTime` equals the unshifted eval time.

- [ ] **Step 3: Apply offset in `handleQuery`**

Open `internal/proxy/proxy.go`. Find the section in `handleQuery` after `preferWorkingParser` (around line 1610):

```go
	logqlQuery = resolveGrafanaRangeTemplateTokens(logqlQuery, r.FormValue("start"), r.FormValue("end"), r.FormValue("step"))
	logqlQuery = p.preferWorkingParser(r.Context(), logqlQuery, r.FormValue("start"), r.FormValue("end"))

	if spec, ok := parseBareParserMetricCompatSpec(logqlQuery); ok {
```

Insert between `preferWorkingParser` and `parseBareParserMetricCompatSpec`:

```go
	// Extract and apply LogQL offset: shift the evaluation time backward by the
	// offset duration. Instant queries use a single "time" parameter instead of
	// start/end. The offset clause is stripped so the translator gets offset-free LogQL.
	{
		offsetDur, strippedQuery, offsetErr := extractLogQLOffset(logqlQuery)
		if offsetErr != nil {
			p.writeError(w, http.StatusBadRequest, offsetErr.Error())
			p.metrics.RecordRequest("query", http.StatusBadRequest, time.Since(start))
			return
		}
		logqlQuery = strippedQuery
		if offsetDur != 0 {
			_ = r.ParseForm()
			timeParam := r.FormValue("time")
			if timeParam == "" {
				timeParam = r.FormValue("end")
			}
			if tsNs, ok := parseLokiTimeToUnixNano(timeParam); ok {
				r.Form.Set("time", nanosToVLTimestamp(tsNs-offsetDur.Nanoseconds()))
			}
		}
	}
```

- [ ] **Step 4: Run tests — confirm pass**

```bash
go test ./internal/proxy/ -run "TestQuery_OffsetShiftsTime" -v 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 5: Run full proxy test suite**

```bash
go test ./internal/proxy/... 2>&1 | tail -5
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/proxy/proxy.go internal/proxy/offset_integration_test.go
git commit -m "feat(offset): apply LogQL offset in handleQuery — shift eval time for instant queries"
```

---

### Task 4: Update roadmap tests — offset is now implemented

**Files:**
- Modify: `internal/translator/roadmap_test.go` (lines 41–71)

- [ ] **Step 1: Update `TestOffsetModifier_Recognized` to verify the offset is stripped**

Open `internal/translator/roadmap_test.go`. Find `TestOffsetModifier_Recognized` (line 41). The test currently only checks no error is returned. Update it to verify the offset clause is not present in the translated output:

```go
func TestOffsetModifier_Recognized(t *testing.T) {
	tests := []struct {
		name    string
		logql   string
		wantErr bool
	}{
		{
			name:    "rate with offset",
			logql:   `rate({app="nginx"}[5m] offset 1h)`,
			wantErr: false,
		},
		{
			name:    "count with offset",
			logql:   `count_over_time({app="nginx"}[5m] offset 30m)`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The proxy strips the offset before translation; the translator
			// receives offset-free LogQL. Verify translation still succeeds.
			result, err := TranslateLogQL(tt.logql)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err == nil && result == "" {
				t.Error("expected non-empty result")
			}
			// Confirm "offset" keyword is not propagated into VL LogsQL output.
			if strings.Contains(result, " offset ") {
				t.Errorf("translated query still contains 'offset': %s", result)
			}
		})
	}
}
```

Add `"strings"` to the import block if not already present.

- [ ] **Step 2: Run translator tests**

```bash
go test ./internal/translator/... -run TestOffsetModifier_Recognized -v 2>&1 | tail -10
```

Expected: PASS (the test still passes since the translator was already not producing "offset" — it was just silently mangling it, now the proxy strips it cleanly first).

- [ ] **Step 3: Run full test suite**

```bash
go test ./... 2>&1 | tail -5
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add internal/translator/roadmap_test.go
git commit -m "test(offset): strengthen roadmap test — assert offset not propagated to VL output"
```

---

### Task 5: Update documentation

**Files:**
- Modify: `docs/translation-reference.md` (line 133)
- Modify: `docs/KNOWN_ISSUES.md` (line 35 and the "What Is No Longer an Open Gap" section)

- [ ] **Step 1: Update `translation-reference.md`**

Open `docs/translation-reference.md`. Find line 133:
```
| `offset 1h` on range vectors | NOT YET IMPLEMENTED -- time-window shifting gap; offset is silently stripped |
```

Replace with:
```
| `offset 1h` on range vectors | Implemented: the proxy extracts the offset from the LogQL query, strips the clause, and shifts the `start`/`end` (range queries) or `time` (instant queries) backward by the offset duration before dispatching to VictoriaLogs. |
```

- [ ] **Step 2: Update `KNOWN_ISSUES.md`**

Open `docs/KNOWN_ISSUES.md`. Find (line 35):
```
| `offset` directive | Silently stripped from queries; results do not reflect time shifting. Implementation requires parsing the offset value and adjusting `start`/`end` parameters before backend dispatch. See [translation-reference.md](translation-reference.md). |
```

Delete that row entirely.

Then find the "## What Is No Longer an Open Gap" section. Add at the end of the bullet list:

```
- `offset` directive — proxy extracts the offset, strips the clause from the query, and shifts `start`/`end`/`time` backward before dispatch (v1.32.0)
```

- [ ] **Step 3: Verify docs render correctly (spot check)**

```bash
grep -n "offset" docs/KNOWN_ISSUES.md docs/translation-reference.md
```

Expected: no "silently stripped" or "NOT YET IMPLEMENTED" entries for offset.

- [ ] **Step 4: Commit**

```bash
git add docs/translation-reference.md docs/KNOWN_ISSUES.md
git commit -m "docs(offset): mark offset directive as implemented, remove from known gaps"
```

---

### Task 6: CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add entry to `[Unreleased]` section**

Open `CHANGELOG.md`. Find the `## [Unreleased]` section. Add under `### Added` (create the heading if missing):

```markdown
### Added
- `offset` directive support for range vector metric queries. `rate({app}[5m] offset 1h)` and similar expressions now correctly query the historical time window by shifting `start`/`end` (range queries) or `time` (instant queries) backward by the offset duration before dispatching to VictoriaLogs. Multiple conflicting offsets in a single query return HTTP 400, matching Loki behavior.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add offset directive implementation entry"
```

---

## Self-Review Checklist (completed inline)

1. **Spec coverage** — all design requirements covered: extract+strip (`extractLogQLOffset`), range shift (Task 2), instant shift (Task 3), error on multiple offsets (Task 2 test), docs (Task 5), changelog (Task 6). ✓
2. **No placeholders** — all code blocks are complete and runnable. ✓
3. **Type consistency** — `extractLogQLOffset` signature `(string) (time.Duration, string, error)` used consistently across all tasks. `parseLokiTimeToUnixNano`, `nanosToVLTimestamp` used by exact name throughout. ✓
4. **Ambiguity** — negative offsets: shifts time *forward* (subtracting a negative adds to the timestamp). Documented in `extractLogQLOffset` test case. ✓
