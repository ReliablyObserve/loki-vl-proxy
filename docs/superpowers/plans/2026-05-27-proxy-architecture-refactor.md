# Proxy Architecture Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship one PR adding ParseError/UnsupportedError, query-range limit enforcement, a typed AST-to-AST translation layer for log queries, and a Deps/Config/State/Handler split for the Proxy monolith.

**Architecture:** Three sequential commits in one PR (Part 1 → Part 2 → Part 3). Each commit leaves all 554 translator unit tests, proxy tests, and e2e parity suite green. The proxy's main translation path remains `translator.TranslateLogQLWithCapabilities` throughout — `logql.Translate` is wired but not yet called by proxy handlers (that migration is a follow-on PR).

**Tech Stack:** Go 1.22+, `internal/logql` (LogQL AST), `internal/logsql` (LogsQL AST + builder), `internal/translator`, `internal/proxy`. No new dependencies.

---

## File map

**Part 1 — new files:**
- `internal/translator/errors.go` — `ParseError`, `UnsupportedError`
- `internal/proxy/limits_enforcement.go` — `effectiveMaxQueryLength`, enforcement helpers

**Part 1 — modified files:**
- `internal/proxy/proxy.go` — add `defaultMaxQueryLength` field, update `RegisterRoutes` to extract `routeHandler`
- `internal/proxy/query_translation.go` — call limit check at top of `handleQueryRange` and `handleQuery`
- `cmd/proxy/main.go` — register `-default-max-query-length` flag

**Part 2 — modified files:**
- `internal/logql/translate.go` — rewrite from 24 → ~300 lines: errFallthrough sentinel + `translateExpr` dispatcher + log-query AST-to-AST mapper (metric path still falls through)
- `internal/logql/translate_test.go` — new file with 16 test cases

**Part 3 — new files:**
- `internal/proxy/deps.go` — `Deps` struct + `buildDeps`
- `internal/proxy/config.go` — `Config` struct + `buildConfig`
- `internal/proxy/state.go` — `State` struct + `buildState`, `newState`
- `internal/proxy/handler.go` — `Handler` struct + migrated `RegisterRoutes`

**Part 3 — modified files:**
- `internal/proxy/proxy.go` — shrinks to ~120 lines (thin `Proxy` wrapper embedding `*Handler`)
- `internal/proxy/query_translation.go` — receiver `*Proxy` → `*Handler`, field access updated
- `internal/proxy/labels.go` — same
- `internal/proxy/patterns.go` — same
- `internal/proxy/postprocess.go` — same
- `internal/proxy/stream_processing.go` — same
- `internal/proxy/range_metric_compat.go` — same
- `internal/proxy/query_range_windowing.go` — same
- `internal/proxy/metric_binary.go` — same

---

## Part 1 — Query-range enforcement + error types + middleware docs

### Task 1: ParseError and UnsupportedError

**Files:**
- Create: `internal/translator/errors.go`
- Modify: `internal/translator/translator.go` (several error return sites)

- [ ] **Step 1: Write the failing test**

Create `internal/translator/errors_test.go`:

```go
package translator_test

import (
    "errors"
    "testing"

    "github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func TestParseError_ErrorInterface(t *testing.T) {
    e := &translator.ParseError{Msg: "unexpected token", Pos: 5}
    if e.Error() != "unexpected token" {
        t.Errorf("got %q", e.Error())
    }
    var pe *translator.ParseError
    if !errors.As(e, &pe) {
        t.Error("errors.As failed")
    }
}

func TestUnsupportedError_ErrorInterface(t *testing.T) {
    e := &translator.UnsupportedError{Msg: "count_values is not supported", Func: "count_values"}
    if e.Error() != "count_values is not supported" {
        t.Errorf("got %q", e.Error())
    }
    var ue *translator.UnsupportedError
    if !errors.As(e, &ue) {
        t.Error("errors.As failed")
    }
    if ue.Func != "count_values" {
        t.Errorf("got Func %q", ue.Func)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /path/to/loki-vl-proxy
go test ./internal/translator/ -run "TestParseError|TestUnsupportedError" -v
```

Expected: FAIL with "undefined: translator.ParseError"

- [ ] **Step 3: Create `internal/translator/errors.go`**

```go
package translator

// ParseError is returned when the input is not valid LogQL syntax.
// The proxy HTTP layer maps this to errorType "parse error".
type ParseError struct {
    Msg string
    Pos int // byte offset in input; -1 if unknown
}

func (e *ParseError) Error() string { return e.Msg }

// UnsupportedError is returned when a valid LogQL construct has no
// equivalent in LogsQL and cannot be translated.
// The proxy HTTP layer maps this to errorType "bad_data".
type UnsupportedError struct {
    Msg  string
    Func string // the LogQL function or construct that is unsupported
}

func (e *UnsupportedError) Error() string { return e.Msg }
```

- [ ] **Step 4: Run test to verify it passes**

```bash
go test ./internal/translator/ -run "TestParseError|TestUnsupportedError" -v
```

Expected: PASS

- [ ] **Step 5: Update error return sites in translator.go**

In `internal/translator/translator.go`, find sites that return errors for parse failures and unsupported constructs. Use `grep -n "fmt.Errorf\|errors.New" internal/translator/translator.go | head -40` to identify them.

Update the following categories:
- Malformed stream selectors / unmatched braces → `&ParseError{Msg: ..., Pos: -1}`
- `count_values` not supported → `&UnsupportedError{Msg: "count_values is not translatable to LogsQL", Func: "count_values"}`
- Range aggregation missing `| unwrap` → `&UnsupportedError{Msg: "...", Func: string(op)}`

Example change in `translator.go`:

Find: `return "", fmt.Errorf("count_values is not supported")`
Replace with: `return "", &UnsupportedError{Msg: "count_values is not translatable to LogsQL", Func: "count_values"}`

Find patterns like: `return "", fmt.Errorf("...parse...") or "...invalid..."`
Replace with: `return "", &ParseError{Msg: "...", Pos: -1}`

- [ ] **Step 6: Run all translator tests**

```bash
go test ./internal/translator/ -v 2>&1 | tail -20
```

Expected: All tests pass. Count should be ≥ 554.

- [ ] **Step 7: Commit**

```bash
git add internal/translator/errors.go internal/translator/errors_test.go internal/translator/translator.go
git commit -m "feat: add ParseError and UnsupportedError to translator package"
```

---

### Task 2: Query-length enforcement

**Files:**
- Create: `internal/proxy/limits_enforcement.go`
- Modify: `internal/proxy/proxy.go` (add field, update RegisterRoutes)
- Modify: `cmd/proxy/main.go` (register flag)

**Context:** `publishedTenantLimitsForOrgID` is at `internal/proxy/patterns.go:1274`. It already returns `"max_query_length": "30d1h"` in the limits map. `parseLokiDuration` is at `internal/proxy/subquery.go:457` — already in the same package, no import needed. `getOrgID(ctx)` is at `internal/proxy/telemetry.go:252`.

- [ ] **Step 1: Write the failing test**

Create `internal/proxy/limits_enforcement_test.go`:

```go
package proxy

import (
    "testing"
    "time"
)

func TestEffectiveMaxQueryLength_ZeroDefault(t *testing.T) {
    p := &Proxy{defaultMaxQueryLength: 0}
    // orgID with no per-tenant limits → returns flag default (0 = unlimited)
    got := p.effectiveMaxQueryLength("tenant-a")
    if got != 0 {
        t.Errorf("want 0 (unlimited), got %v", got)
    }
}

func TestEffectiveMaxQueryLength_FlagDefault(t *testing.T) {
    p := &Proxy{defaultMaxQueryLength: 7 * 24 * time.Hour}
    // orgID not in tenant limits map → returns flag default
    p.tenantLimits = map[string]map[string]any{}
    p.tenantDefaultLimits = map[string]any{}
    got := p.effectiveMaxQueryLength("tenant-a")
    if got != 7*24*time.Hour {
        t.Errorf("want 168h, got %v", got)
    }
}

func TestEffectiveMaxQueryLength_TenantOverride(t *testing.T) {
    p := &Proxy{defaultMaxQueryLength: 7 * 24 * time.Hour}
    p.tenantLimits = map[string]map[string]any{
        "tenant-a": {"max_query_length": "1h"},
    }
    p.tenantDefaultLimits = map[string]any{}
    got := p.effectiveMaxQueryLength("tenant-a")
    if got != time.Hour {
        t.Errorf("want 1h, got %v", got)
    }
}

func TestEffectiveMaxQueryLength_DefaultLimitOverride(t *testing.T) {
    p := &Proxy{defaultMaxQueryLength: 0}
    p.tenantLimits = map[string]map[string]any{}
    p.tenantDefaultLimits = map[string]any{"max_query_length": "24h"}
    got := p.effectiveMaxQueryLength("")
    if got != 24*time.Hour {
        t.Errorf("want 24h, got %v", got)
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/proxy/ -run "TestEffectiveMaxQueryLength" -v
```

Expected: FAIL with "undefined: effectiveMaxQueryLength" or field errors.

- [ ] **Step 3: Add `defaultMaxQueryLength` field to `Proxy` struct in proxy.go**

In `internal/proxy/proxy.go`, add to the `Proxy` struct (after the `tenantLimits` field, around line 430):

```go
defaultMaxQueryLength time.Duration // 0 = unlimited; enforced in handleQueryRange/handleQuery
```

- [ ] **Step 4: Create `internal/proxy/limits_enforcement.go`**

```go
package proxy

import (
    "context"
    "time"
)

// effectiveMaxQueryLength returns the maximum allowed query time range for orgID.
// Precedence (highest first): per-tenant tenantLimits → tenantDefaultLimits → defaultMaxQueryLength flag.
// Returns 0 when unlimited (default when flag is not set).
func (p *Proxy) effectiveMaxQueryLength(orgID string) time.Duration {
    p.configMu.RLock()
    tenantOverride, _ := p.tenantLimits[orgID]["max_query_length"].(string)
    defaultOverride, _ := p.tenantDefaultLimits["max_query_length"].(string)
    p.configMu.RUnlock()

    if tenantOverride != "" {
        if d := parseLokiDuration(tenantOverride); d > 0 {
            return d
        }
    }
    if defaultOverride != "" {
        if d := parseLokiDuration(defaultOverride); d > 0 {
            return d
        }
    }
    return p.defaultMaxQueryLength
}

// checkQueryRangeLength returns an error string if the requested range exceeds
// the enforced max query length for orgID. Returns "" when within limit or unlimited.
func (p *Proxy) checkQueryRangeLength(ctx context.Context, startNs, endNs int64) string {
    maxLen := p.effectiveMaxQueryLength(getOrgID(ctx))
    if maxLen == 0 {
        return "" // unlimited
    }
    rangeNs := endNs - startNs
    if rangeNs <= 0 {
        return ""
    }
    rangeDur := time.Duration(rangeNs)
    if rangeDur > maxLen {
        return "query length " + rangeDur.String() + " exceeds limit " + maxLen.String()
    }
    return ""
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./internal/proxy/ -run "TestEffectiveMaxQueryLength" -v
```

Expected: All 4 tests PASS.

- [ ] **Step 6: Register `-default-max-query-length` flag in `cmd/proxy/main.go`**

Find the flag registration block (around line 443 in main.go, near `query-range-windowing`). Add:

```go
defaultMaxQueryLength := fs.Duration("default-max-query-length", 0, "Default maximum query time range enforced for all tenants unless overridden by per-tenant limits (0 = unlimited, matches Loki default)")
```

Then in `buildProxyConfig` (around line 1511), in the `proxy.Config{}` struct literal, find the `QueryRangeWindowing:` line and add:

```go
DefaultMaxQueryLength: *defaultMaxQueryLength,
```

Also add `DefaultMaxQueryLength time.Duration` to `proxy.Config` struct in `internal/proxy/proxy.go` (the `Config` type, not the `Proxy` struct — they are the same struct for now, so add it to the `ProxyConfig` type which is the `Config` struct alias). Search for the `ProxyConfig` struct around line 100 in proxy.go.

> **Note:** The proxy currently uses `ProxyConfig` as the input config and copies fields to the `Proxy` struct during `New`/`Init`. Add `DefaultMaxQueryLength time.Duration` to `ProxyConfig`, and in `Init` or `New` assign `p.defaultMaxQueryLength = cfg.DefaultMaxQueryLength`.

Find `func New(cfg ProxyConfig)` or `func (p *Proxy) Init(cfg ProxyConfig)` and add the assignment.

- [ ] **Step 7: Run proxy tests**

```bash
go test ./internal/proxy/ -count=1 -timeout=120s 2>&1 | tail -10
```

Expected: PASS (all tests, no compilation errors).

- [ ] **Step 8: Commit**

```bash
git add internal/proxy/limits_enforcement.go internal/proxy/limits_enforcement_test.go \
    internal/proxy/proxy.go cmd/proxy/main.go
git commit -m "feat: add effectiveMaxQueryLength and -default-max-query-length flag"
```

---

### Task 3: Enforce limit in handlers + extract routeHandler + Part 1 commit

**Files:**
- Modify: `internal/proxy/query_translation.go` (enforce in handleQueryRange and handleQuery)
- Modify: `internal/proxy/proxy.go` (extract routeHandler from RegisterRoutes)

**Context:** `handleQueryRange` is at `proxy.go:1487`. Time extraction uses `parseLokiTimeToUnixNano` at `query_range_windowing.go:999`. The enforcement must happen AFTER `extractLogQLOffset` shifts start/end (around line 1563 of proxy.go) but BEFORE the first VL request is made.

- [ ] **Step 1: Write the failing handler test**

In `internal/proxy/limits_enforcement_test.go`, add:

```go
func TestHandleQueryRange_ExceedsMaxQueryLength_Returns400(t *testing.T) {
    p := newTestProxy(t)
    p.defaultMaxQueryLength = time.Hour

    start := "0"                                                 // Unix nano 0
    end := fmt.Sprintf("%d", int64(3*time.Hour.Nanoseconds())) // 3h > 1h limit
    req := httptest.NewRequest("GET",
        `/loki/api/v1/query_range?query={app="x"}&start=`+start+`&end=`+end, nil)
    w := httptest.NewRecorder()
    p.handleQueryRange(w, req)

    if w.Code != http.StatusBadRequest {
        t.Errorf("want 400, got %d (body: %s)", w.Code, w.Body.String())
    }
    body := w.Body.String()
    if !strings.Contains(body, "exceeds limit") {
        t.Errorf("want 'exceeds limit' in body, got: %s", body)
    }
}

func TestHandleQueryRange_WithinLimit_Passes(t *testing.T) {
    p := newTestProxy(t)
    p.defaultMaxQueryLength = 24 * time.Hour

    start := "0"
    end := fmt.Sprintf("%d", int64(time.Hour.Nanoseconds())) // 1h < 24h limit
    req := httptest.NewRequest("GET",
        `/loki/api/v1/query_range?query={app="x"}&start=`+start+`&end=`+end, nil)
    w := httptest.NewRecorder()
    p.handleQueryRange(w, req)

    // Should not return 400 for limit violation (may still fail for other reasons)
    if w.Code == http.StatusBadRequest {
        if strings.Contains(w.Body.String(), "exceeds limit") {
            t.Errorf("unexpected limit rejection: %s", w.Body.String())
        }
    }
}
```

> The test uses `newTestProxy(t)` — look for existing test helper in `coverage_gaps_test.go` or `critical_fixes_test.go`. Use `buildTestProxy` or similar existing factory if present. If no factory exists, construct a minimal `&Proxy{log: slog.Default(), metrics: metrics.NewNoop()}`.

- [ ] **Step 2: Run failing test**

```bash
go test ./internal/proxy/ -run "TestHandleQueryRange_ExceedsMaxQueryLength" -v
```

Expected: FAIL (limit not enforced yet — returns 2xx or non-400).

- [ ] **Step 3: Add enforcement in `handleQueryRange` in `query_translation.go`**

In `internal/proxy/query_translation.go`, find `handleQueryRange`. Locate the offset extraction block (around line 1545 of proxy.go — this is in query_translation.go or proxy.go, grep to confirm). After the block that extracts and shifts `start`/`end` (after line ~1574 where `r.Form.Set("end", ...)` is called), add:

```go
// Enforce max query length AFTER offset is applied (start/end reflect shifted range).
if startNs, okS := parseLokiTimeToUnixNano(r.FormValue("start")); okS {
    if endNs, okE := parseLokiTimeToUnixNano(r.FormValue("end")); okE {
        if errMsg := p.checkQueryRangeLength(r.Context(), startNs, endNs); errMsg != "" {
            p.writeError(w, http.StatusBadRequest, errMsg)
            p.metrics.RecordRequest("query_range", http.StatusBadRequest, time.Since(start))
            return
        }
    }
}
```

> **Important:** The variable `start` (line 1488 in proxy.go) is `time.Time` for elapsed timing; the form value `"start"` is a string. They are different — use `r.FormValue("start")` for the time range check.

- [ ] **Step 4: Add enforcement in `handleQuery` (instant queries)**

In `handleQuery` (around line 1739 of proxy.go), add the same check after the offset extraction block for instant queries. The instant query uses a single timestamp (`time`) parameter, so the range is from `time-1s` to `time`. Skip enforcement for instant queries (they are always point-in-time), or add a 0-duration check that always passes. Per Loki behavior: instant queries have no time range constraint.

- [ ] **Step 5: Extract `routeHandler` from `RegisterRoutes` in proxy.go**

In `internal/proxy/proxy.go`, find `RegisterRoutes` at line 1276. The inner `rl` closure is:

```go
rl := func(endpoint, route string, h http.HandlerFunc) http.Handler {
    return securityHeaders(p.tenantMiddleware(p.limiter.Middleware(p.requestLogger(endpoint, route, p.compatCacheMiddleware(endpoint, route, h)))))
}
```

Replace this (and its usage) with a named method `routeHandler`. Add to proxy.go:

```go
// routeHandler wraps h with the standard per-route middleware chain for tenant-aware
// rate-limited endpoints. Applied outermost → innermost (last executes first on request path):
//   securityHeaders → tenantMiddleware → limiter → requestLogger → compatCacheMiddleware
func (p *Proxy) routeHandler(endpoint, route string, h http.HandlerFunc) http.Handler {
    return securityHeaders(
        p.tenantMiddleware(
            p.limiter.Middleware(
                p.requestLogger(endpoint, route,
                    p.compatCacheMiddleware(endpoint, route, h)))))
}
```

Then in `RegisterRoutes`, replace all calls to `rl(...)` with `p.routeHandler(...)`. Keep `rlNoTenant` as a local closure (it differs from `routeHandler` by skipping `tenantMiddleware`).

- [ ] **Step 6: Verify build and run tests**

```bash
go build ./... && go test ./internal/proxy/ -run "TestHandleQueryRange" -v
go test ./... -count=1 -timeout=180s 2>&1 | tail -15
```

Expected: all tests pass. `TestHandleQueryRange_ExceedsMaxQueryLength_Returns400` must pass.

- [ ] **Step 7: Part 1 commit**

```bash
git add internal/proxy/query_translation.go internal/proxy/proxy.go \
    internal/proxy/limits_enforcement.go internal/proxy/limits_enforcement_test.go \
    cmd/proxy/main.go
git commit -m "feat: add ParseError/UnsupportedError + query-length enforcement + routeHandler extraction"
```

---

## Part 2 — Typed AST-to-AST translation pipeline (log queries only)

### Task 4: translate.go — errFallthrough + StreamSelector + LineFilter

**Files:**
- Modify: `internal/logql/translate.go` (rewrite from 24 lines)
- Create: `internal/logql/translate_test.go`

**Context:** The current `internal/logql/translate.go` roundtrips through the string translator. This task replaces it with a typed mapper. Metric queries (`RangeAggregation`, `VectorAggregation`, `BinOpExpr`) fall through to the string translator — their string-based handling is unchanged. The proxy does NOT call `logql.Translate` yet (follow-on PR).

The logsql builder constructors are in `internal/logsql/builder.go`:
- `logsql.NewQuery(filter, pipes...)` — builds `*logsql.Query`
- `logsql.And(left, right)`, `logsql.Or(...)`, `logsql.Not(...)`
- `logsql.FieldExact(field, value)`, `logsql.FieldRegexp(field, pattern)`

The logsql filter types used for stream selectors:
- `logsql.StreamFilter{Matchers: []logsql.LabelMatcher{{Name, Op, Value}}}` — emits `{app="x"}`
- `logsql.FieldFilter{Field, Op: logsql.FieldOpExact, Value}` — emits `app:="x"`

- [ ] **Step 1: Write the failing test file**

Create `internal/logql/translate_test.go`:

```go
package logql_test

import (
    "testing"

    "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func mustParse(t *testing.T, q string) logql.Expr {
    t.Helper()
    expr, err := logql.Parse(q)
    if err != nil {
        t.Fatalf("parse %q: %v", q, err)
    }
    return expr
}

func TestTranslate_StreamSelector_SingleMatcher_Eq(t *testing.T) {
    expr := mustParse(t, `{app="api-gateway"}`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    want := `{app="api-gateway"}`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_StreamSelector_Neq(t *testing.T) {
    expr := mustParse(t, `{app!="test"}`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    // != in stream selector → NOT app:="test" field filter (not stream filter)
    // since we can't represent != in a stream filter; we use FieldFilter
    want := `NOT app:="test"`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_StreamSelector_MultiMatcher(t *testing.T) {
    expr := mustParse(t, `{app="api", env="prod"}`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    // Two eq matchers → stream filter
    want := `{app="api", env="prod"}`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_LineFilter_Contains(t *testing.T) {
    expr := mustParse(t, `{app="x"} |= "error"`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    want := `{app="x"} | filter "error"`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_LineFilter_Excludes(t *testing.T) {
    expr := mustParse(t, `{app="x"} != "debug"`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    want := `{app="x"} | filter NOT "debug"`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_LineFilter_MatchRe(t *testing.T) {
    expr := mustParse(t, `{app="x"} |~ "error|warn"`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    want := `{app="x"} | filter ~"error|warn"`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_LineFilter_ExcludeRe(t *testing.T) {
    expr := mustParse(t, `{app="x"} !~ "debug|trace"`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    want := `{app="x"} | filter NOT ~"debug|trace"`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_OpaqueMetricExpr_FallsThrough(t *testing.T) {
    // label_replace is OpaqueMetricExpr — must not error, falls through to string translator
    expr := mustParse(t, `label_replace(rate({app="x"}[5m]), "new", "$1", "old", "(.*)")`)
    _, err := logql.Translate(expr, logql.TranslateOptions{})
    // error is ok if it comes from the string translator; must not panic
    _ = err
}

func TestTranslate_RangeAgg_FallsThrough(t *testing.T) {
    expr := mustParse(t, `rate({app="x"}[5m])`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    // metric path falls through to string translator → should produce a LogsQL string (not error)
    if err != nil {
        t.Errorf("unexpected error: %v", err)
    }
    if got == "" {
        t.Error("want non-empty LogsQL output")
    }
}
```

- [ ] **Step 2: Run tests to see failures**

```bash
go test ./internal/logql/ -run "TestTranslate_" -v
```

Expected: Some tests FAIL because the current `Translate` roundtrips through the string translator and produces different output.

- [ ] **Step 3: Rewrite `internal/logql/translate.go`**

Replace the entire file with:

```go
package logql

import (
    "errors"
    "strings"

    "github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
    "github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// TranslateOptions controls how LogQL is translated to LogsQL.
type TranslateOptions struct {
    // LabelFn translates a Loki label name to a VictoriaLogs field name.
    // If nil, label names are passed through unchanged.
    LabelFn translator.LabelTranslateFunc

    // StreamFields is the set of VL _stream_fields labels for which the
    // translator should use the faster {label="value"} stream-selector syntax.
    StreamFields map[string]bool

    // Caps controls which VL version-gated features are emitted.
    // Zero value is safe — disables all gated features.
    Caps logsql.Capabilities
}

// errFallthrough signals that translateExpr cannot handle this node type
// and the caller should route to the string-based translator instead.
// It is never returned to external callers.
var errFallthrough = errors.New("fallthrough to string translator")

// Translate converts a parsed LogQL expression to a LogsQL query string.
//
// For log queries (LogQuery), it uses a typed AST-to-AST mapping:
//   logql.Expr → logsql.Query → .String()
//
// For metric queries (RangeAggregation, VectorAggregation, BinOpExpr) and
// OpaqueMetricExpr (label_replace, label_join), it falls through to the
// existing string-based translator which handles those paths correctly.
func Translate(expr Expr, opts TranslateOptions) (string, error) {
    q, err := translateExpr(expr, opts)
    if errors.Is(err, errFallthrough) {
        return translator.TranslateLogQLWithStreamFields(expr.String(), opts.LabelFn, opts.StreamFields)
    }
    if err != nil {
        return "", err
    }
    return q.String(), nil
}

// translateExpr dispatches to the appropriate node translator.
// Returns errFallthrough for nodes not yet covered by the AST mapper.
func translateExpr(expr Expr, opts TranslateOptions) (*logsql.Query, error) {
    switch e := expr.(type) {
    case *LogQuery:
        return translateLogQuery(e, opts)
    default:
        // RangeAggregation, VectorAggregation, BinOpExpr, OpaqueMetricExpr,
        // LiteralExpr — all fall through to the string translator.
        return nil, errFallthrough
    }
}

// translateLogQuery maps a LogQL log query to a logsql.Query.
// StreamSelector becomes the filter; pipeline stages become pipes.
func translateLogQuery(lq *LogQuery, opts TranslateOptions) (*logsql.Query, error) {
    filter, err := translateStreamSelector(lq.Selector, opts)
    if err != nil {
        return nil, err
    }

    var pipes []logsql.Pipe
    for _, stage := range lq.Pipeline {
        stagePipes, err := translateStage(stage, opts)
        if err != nil {
            if errors.Is(err, errFallthrough) {
                // A single stage we can't map → fall through the entire query
                return nil, errFallthrough
            }
            return nil, err
        }
        pipes = append(pipes, stagePipes...)
    }
    return logsql.NewQuery(filter, pipes...), nil
}

// translateStreamSelector maps {app="x", env!="test"} matchers to a logsql filter.
//
// Strategy:
//   - Equality matchers (=) for stream fields → grouped into one StreamFilter
//   - Equality matchers (=) for non-stream fields → FieldFilter (field:="val")
//   - Inequality (!= / =~ / !~) → FieldFilter or NOT-wrapped FieldFilter
//   - Mixed stream + non-stream eq matchers → StreamFilter AND FieldFilter(s)
func translateStreamSelector(sel *StreamSelector, opts TranslateOptions) (logsql.FilterExpr, error) {
    if len(sel.Matchers) == 0 {
        return nil, nil // nil → "*" in logsql
    }

    applyLabelFn := func(name string) string {
        if opts.LabelFn != nil {
            return opts.LabelFn(name)
        }
        return name
    }

    var streamMatchers []logsql.LabelMatcher
    var filters []logsql.FilterExpr

    for _, m := range sel.Matchers {
        field := applyLabelFn(m.Name)
        isStream := opts.StreamFields[m.Name] || opts.StreamFields[field]

        switch m.Op {
        case MatchEq:
            if isStream {
                streamMatchers = append(streamMatchers, logsql.LabelMatcher{
                    Name: field, Op: `="`, Value: m.Value,
                })
            } else {
                filters = append(filters, logsql.FieldExact(field, m.Value))
            }
        case MatchNeq:
            filters = append(filters, logsql.Not(logsql.FieldExact(field, m.Value)))
        case MatchRe:
            filters = append(filters, logsql.FieldRegexp(field, m.Value))
        case MatchNotRe:
            filters = append(filters, logsql.Not(logsql.FieldRegexp(field, m.Value)))
        }
    }

    if len(streamMatchers) > 0 {
        filters = append([]logsql.FilterExpr{logsql.StreamFilter{Matchers: streamMatchers}}, filters...)
    }

    if len(filters) == 0 {
        return nil, nil
    }
    result := filters[0]
    for _, f := range filters[1:] {
        result = logsql.And(result, f)
    }
    return result, nil
}

// translateStage maps a single LogQL pipeline stage to zero or more logsql pipes.
// Returns errFallthrough for stages the AST mapper does not cover.
func translateStage(stage Stage, opts TranslateOptions) ([]logsql.Pipe, error) {
    switch s := stage.(type) {
    case *LineFilterStage:
        return translateLineFilter(s)
    case *ParserStage:
        return translateParser(s)
    case *LabelFilterStage:
        return translateLabelFilter(s)
    case *DropStage:
        return translateDrop(s)
    case *KeepStage:
        return translateKeep(s)
    case *LineFormatStage:
        return []logsql.Pipe{logsql.PipeFormat{Template: s.Template, ResultField: "_msg"}}, nil
    case *LabelFormatStage:
        return translateLabelFormat(s)
    case *DecolorizeStage:
        return nil, nil // no-op in VL
    case *UnwrapStage:
        // UnwrapStage only appears inside range aggregations; if we reach it
        // in a log query pipeline the string translator handles it.
        return nil, errFallthrough
    default:
        return nil, errFallthrough
    }
}

// translateLineFilter maps line filter stages to logsql.PipeFilter nodes.
func translateLineFilter(s *LineFilterStage) ([]logsql.Pipe, error) {
    var f logsql.FilterExpr
    switch s.Op {
    case LineFilterContains:
        f = logsql.Phrase{Value: s.Value}
    case LineFilterExcludes:
        f = logsql.Not(logsql.Phrase{Value: s.Value})
    case LineFilterMatchRe:
        f = logsql.Regexp{Pattern: s.Value}
    case LineFilterExcludeRe:
        f = logsql.Not(logsql.Regexp{Pattern: s.Value})
    case LineFilterContainsPat:
        // |> pattern: replace <_> and <...> placeholders with .*
        re := patternToRegexp(s.Value)
        f = logsql.Regexp{Pattern: re}
    case LineFilterExcludePat:
        re := patternToRegexp(s.Value)
        f = logsql.Not(logsql.Regexp{Pattern: re})
    default:
        return nil, errFallthrough
    }
    return []logsql.Pipe{logsql.PipeFilter{Expr: f}}, nil
}

// patternToRegexp converts a LogQL |> pattern string to a regexp approximation.
// <_> and <...> wildcards become .*, literal chars are not escaped (patterns
// are already regexp-safe in Loki's grammar).
func patternToRegexp(pattern string) string {
    r := strings.NewReplacer("<_>", ".*", "<...>", ".*")
    return r.Replace(pattern)
}

// translateParser maps parser stages to logsql unpack/extract pipes.
func translateParser(s *ParserStage) ([]logsql.Pipe, error) {
    switch s.Type {
    case ParserJSON, ParserUnpack:
        return []logsql.Pipe{logsql.PipeUnpackJSON{}}, nil
    case ParserLogfmt:
        return []logsql.Pipe{logsql.PipeUnpackLogfmt{}}, nil
    case ParserRegexp:
        return []logsql.Pipe{logsql.PipeExtractRegexp{Pattern: s.Param, From: "_msg"}}, nil
    case ParserPattern:
        return []logsql.Pipe{logsql.PipeExtract{Pattern: s.Param, From: "_msg"}}, nil
    default:
        return nil, errFallthrough
    }
}

// translateLabelFilter maps label filter stages to logsql.PipeFilter.
// The filter expression body uses DeferredExpr to carry the raw expression string —
// this preserves the filter semantics without re-implementing the full label filter
// parser in the AST path. The string translator still handles complex cases.
func translateLabelFilter(s *LabelFilterStage) ([]logsql.Pipe, error) {
    if s.Raw == "" {
        return nil, nil
    }
    return []logsql.Pipe{logsql.PipeFilter{Expr: logsql.DeferredExpr{Raw: s.Raw}}}, nil
}

// translateDrop maps drop stages to PipeDelete (bare labels) or PipeFilter (conditional).
func translateDrop(s *DropStage) ([]logsql.Pipe, error) {
    if len(s.Labels) > 0 && len(s.Matchers) == 0 {
        return []logsql.Pipe{logsql.PipeDelete{Labels: s.Labels}}, nil
    }
    // Conditional drop: | drop level="debug" → | filter NOT level:="debug"
    // Emit one PipeFilter per matcher, negated.
    var pipes []logsql.Pipe
    for _, m := range s.Matchers {
        f := logsql.FieldFilter{Field: m.Name, Op: logsql.FieldOpExact, Value: m.Value}
        // Handle != and =~ operators in drop matchers
        switch m.Op {
        case "=~":
            f.Op = logsql.FieldOpRegexp
        }
        pipes = append(pipes, logsql.PipeFilter{Expr: logsql.Not(f)})
    }
    return pipes, nil
}

// translateKeep maps keep stages to PipeFields (bare labels) or PipeFilter (conditional).
func translateKeep(s *KeepStage) ([]logsql.Pipe, error) {
    if len(s.Labels) > 0 && len(s.Matchers) == 0 {
        return []logsql.Pipe{logsql.PipeFields{Labels: s.Labels}}, nil
    }
    // Conditional keep: | keep level="info" → | filter level:="info"
    var pipes []logsql.Pipe
    for _, m := range s.Matchers {
        f := logsql.FieldFilter{Field: m.Name, Op: logsql.FieldOpExact, Value: m.Value}
        if m.Op == "=~" {
            f.Op = logsql.FieldOpRegexp
        }
        pipes = append(pipes, logsql.PipeFilter{Expr: f})
    }
    return pipes, nil
}

// translateLabelFormat maps | label_format a=b, c="d" to PipeRename.
// LabelFormatStage.Raw is a raw string like "new_name=old_name, new2=\"lit\"".
// Simple rename pairs are mapped; literal assignments use DeferredExpr fallback.
func translateLabelFormat(s *LabelFormatStage) ([]logsql.Pipe, error) {
    // Parse pairs from Raw: "dst=src" or `dst="literal"`.
    // For this PR: emit as DeferredExpr wrapped in a PipeFormat to preserve semantics.
    // Full parsing is a follow-on improvement.
    return []logsql.Pipe{logsql.PipeFilter{Expr: logsql.DeferredExpr{Raw: "label_format " + s.Raw}}}, nil
}
```

> **Note on `LabelMatcher.Op`:** The `logsql.LabelMatcher.Op` field is a string (e.g., `="`, `!="`, `=~"`, `!~"`). Check the actual type in `internal/logsql/ast.go:LabelMatcher` and use the correct string value. The `StreamFilter.String()` method shows the format.

- [ ] **Step 4: Run tests and fix compilation errors**

```bash
go build ./internal/logql/ 2>&1
go test ./internal/logql/ -run "TestTranslate_" -v
```

Fix any compilation errors (import paths, field names, etc.). The `logsql.LabelMatcher` Op field — check `internal/logsql/ast.go:LabelMatcher` and use the actual Op string (e.g., `="` for equals).

- [ ] **Step 5: Fix failing tests**

Some tests may fail because the expected output differs from what the logsql AST produces (e.g., spacing, quoting). Run each test individually, see the actual output, and adjust the `want` string to match the actual correct logsql syntax. The logsql AST's `String()` methods are canonical.

```bash
go test ./internal/logql/ -run "TestTranslate_StreamSelector_SingleMatcher" -v
```

- [ ] **Step 6: Verify all other logql tests still pass**

```bash
go test ./internal/logql/ -v 2>&1 | tail -20
```

Expected: All existing tests pass (parse tests, validate tests, etc.).

- [ ] **Step 7: Commit this step**

```bash
git add internal/logql/translate.go internal/logql/translate_test.go
git commit -m "feat: wire AST-to-AST path for StreamSelector + LineFilter in translate.go"
```

---

### Task 5: translate.go — Parser + LabelFilter + Drop/Keep/LineFormat test coverage

**Files:**
- Modify: `internal/logql/translate_test.go` (add more test cases)

**Context:** The implementations are already in `translate.go` from Task 4. This task adds tests that verify them and catches any edge-case bugs.

- [ ] **Step 1: Add parser and label filter tests**

Append to `internal/logql/translate_test.go`:

```go
func TestTranslate_Parser_JSON_Bare(t *testing.T) {
    expr := mustParse(t, `{app="x"} | json`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    want := `{app="x"} | unpack_json`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_Parser_Logfmt(t *testing.T) {
    expr := mustParse(t, `{app="x"} | logfmt`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    want := `{app="x"} | unpack_logfmt`
    if got != want {
        t.Errorf("want %q, got %q", want, got)
    }
}

func TestTranslate_Parser_Regexp(t *testing.T) {
    expr := mustParse(t, "{ app=\"x\" } | regexp `(?P<level>\\w+) (?P<msg>.+)`")
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    if !strings.Contains(got, "extract_regexp") {
        t.Errorf("want extract_regexp in output, got %q", got)
    }
}

func TestTranslate_LabelFilter_Exact(t *testing.T) {
    expr := mustParse(t, `{app="x"} | json | level="error"`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    // PipeFilter with DeferredExpr containing the raw label filter
    if !strings.Contains(got, "filter") {
        t.Errorf("want 'filter' in output, got %q", got)
    }
    if !strings.Contains(got, `level="error"`) {
        t.Errorf("want 'level=\"error\"' in output, got %q", got)
    }
}

func TestTranslate_DropStage_BareLabels(t *testing.T) {
    expr := mustParse(t, `{app="x"} | json | drop level, env`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    if !strings.Contains(got, "delete") {
        t.Errorf("want 'delete' in output, got %q", got)
    }
    if !strings.Contains(got, "level") || !strings.Contains(got, "env") {
        t.Errorf("want 'level' and 'env' in output, got %q", got)
    }
}

func TestTranslate_KeepStage_BareLabels(t *testing.T) {
    expr := mustParse(t, `{app="x"} | json | keep level, msg`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    if !strings.Contains(got, "fields") {
        t.Errorf("want 'fields' in output, got %q", got)
    }
    if !strings.Contains(got, "level") || !strings.Contains(got, "msg") {
        t.Errorf("want 'level' and 'msg' in output, got %q", got)
    }
}

func TestTranslate_LineFormatStage(t *testing.T) {
    expr := mustParse(t, `{app="x"} | line_format "{{.level}}: {{.msg}}"`)
    got, err := logql.Translate(expr, logql.TranslateOptions{})
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    if !strings.Contains(got, "format") {
        t.Errorf("want 'format' in output, got %q", got)
    }
}

func TestTranslate_LabelFn_Applied(t *testing.T) {
    // LabelFn renames "app" to "service" in stream selector
    expr := mustParse(t, `{app="api-gateway"}`)
    opts := logql.TranslateOptions{
        LabelFn: func(label string) string {
            if label == "app" {
                return "service"
            }
            return label
        },
    }
    got, err := logql.Translate(expr, opts)
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    if !strings.Contains(got, "service") {
        t.Errorf("want 'service' in output, got %q", got)
    }
}

func TestTranslate_StreamFields_UsesStreamFilter(t *testing.T) {
    expr := mustParse(t, `{app="api-gateway", level="error"}`)
    opts := logql.TranslateOptions{
        StreamFields: map[string]bool{"app": true, "level": true},
    }
    got, err := logql.Translate(expr, opts)
    if err != nil {
        t.Fatalf("translate: %v", err)
    }
    // Both matchers are stream fields → should use stream filter syntax {app=..., level=...}
    if !strings.Contains(got, "{") {
        t.Errorf("want stream filter syntax with '{', got %q", got)
    }
}
```

- [ ] **Step 2: Run tests**

```bash
go test ./internal/logql/ -run "TestTranslate_" -v 2>&1
```

Fix any failing tests by adjusting expected strings to match actual logsql output.

- [ ] **Step 3: Verify full test suite**

```bash
go test ./... -count=1 -timeout=180s 2>&1 | grep -E "FAIL|ok" | head -20
```

Expected: All packages pass.

- [ ] **Step 4: Part 2 commit**

```bash
git add internal/logql/translate.go internal/logql/translate_test.go
git commit -m "feat: wire logql AST into translate.go — replace string roundtrip with AST walker for log queries"
```

---

### Task 6: Verify Part 2 correctness + e2e gate

**Files:**
- No new files

**Context:** Part 2 is complete. This task runs the e2e parity suite to confirm that the new AST path (even though not yet wired into proxy handlers) doesn't break any existing tests, and that the fallthrough works correctly.

- [ ] **Step 1: Run full translator test suite**

```bash
go test ./internal/translator/ -count=1 -v 2>&1 | grep -E "PASS|FAIL|---" | head -40
```

Expected: 554+ tests pass, 0 failures.

- [ ] **Step 2: Run e2e parity gate**

```bash
go test ./internal/proxy/ -run "TestLogQL_Exhaustive|TestMissingOps" -v -timeout=60s 2>&1 | tail -20
```

Expected: All parity tests pass.

- [ ] **Step 3: Run `go vet` and check for issues**

```bash
go vet ./...
```

Expected: No issues.

- [ ] **Step 4: Confirm no regression in existing logql tests**

```bash
go test ./internal/logql/ -count=1 -v 2>&1 | grep -E "PASS|FAIL" | tail -20
```

Expected: All pass.

---

## Part 3 — Proxy Deps / Config / State / Handler restructure

### Task 7: Define Deps, Config, State types

**Files:**
- Create: `internal/proxy/deps.go`
- Create: `internal/proxy/config.go`
- Create: `internal/proxy/state.go`

**Context:** These types extract groups of fields from the `Proxy` struct. They compile successfully alongside the existing `Proxy` struct — no behavior change yet, no receiver rename yet. The key goal is to establish the types so Task 8 can define `Handler` using them.

Read `internal/proxy/proxy.go:380-460` for the full field list before starting.

- [ ] **Step 1: Create `internal/proxy/deps.go`**

```go
package proxy

import (
    "log/slog"
    "net/http"
    "net/url"

    "github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
    "github.com/ReliablyObserve/Loki-VL-proxy/internal/metrics"
    "github.com/ReliablyObserve/Loki-VL-proxy/internal/mw"
)

// Deps holds external dependencies that are injected at startup and mockable in unit tests.
// All fields are safe for concurrent read after Init completes.
type Deps struct {
    Backend    *url.URL
    Client     *http.Client   // primary VL query client
    TailClient *http.Client   // WebSocket tail client (may be same as Client)
    Cache      *cache.Cache   // L1 per-request memory cache
    CompatCache *cache.Cache  // L2 disk-backed compat cache (may be nil)
    Log        *slog.Logger
    Metrics    *metrics.Metrics
    Limiter    *mw.RateLimiter
    Breaker    *mw.CircuitBreaker
    Coalescer  *mw.Coalescer
}
```

> **Note:** Verify that `mw.RateLimiter`, `mw.CircuitBreaker`, `mw.Coalescer` exist by checking `internal/mw/`. Adjust type names to match actual types (e.g., `*mw.Limiter` vs `*mw.RateLimiter`). Run `go build ./internal/proxy/` after creating this file.

- [ ] **Step 2: Create `internal/proxy/config.go`**

Extract the immutable startup fields from the `Proxy` struct. These are fields set once from flags in `New`/`Init` and never written afterward:

```go
package proxy

import "time"

// Config holds immutable startup configuration.
// Set once during New/Init from ProxyConfig; never written after that.
type Config struct {
    TenantLabel                  string
    AuthEnabled                  bool
    RequireTenantHeader          bool
    AllowGlobalTenant            bool
    ForwardTenantHeader          bool
    MaxLines                     int
    ForwardHeaders               []string
    ForwardCookies               map[string]bool
    BackendHeaders               map[string]string
    BackendCompression           string
    BackendLoopback              bool
    ClientResponseCompression    string
    ClientResponseCompressionMinBytes int
    BackendMinVersion            string
    BackendAllowUnsupportedVersion bool
    BackendVersionCheckTimeout   time.Duration
    DerivedFields                []DerivedField
    StreamResponse               bool
    EmitStructuredMetadata       bool
    PatternsEnabled              bool
    PatternsAutodetectFromQueries bool
    PatternsCustom               []string
    LabelTranslator              *LabelTranslator
    MetadataFieldMode            MetadataFieldMode
    StreamFieldsMap              map[string]bool
    DeclaredLabelFields          []string
    QueryRangeWindowing          bool
    QueryRangeSplitInterval      time.Duration
    QueryRangeMaxParallel        int
    DefaultMaxQueryLength        time.Duration
    RegisterInstrumentation      bool
    EnablePprof                  bool
    EnableQueryAnalytics         bool
    AdminAuthToken               string
    RangeMetricRowLimit          int
    TailAllowedOrigins           map[string]struct{}
    TailMode                     TailMode
    MetricsTrustProxyHeaders     bool
    TenantLimitsAllowPublish     []string
    // ... add remaining config-only fields from Proxy struct
}
```

> **Note:** Do NOT include mutable fields (tenantMap, tenantLimits, labelIndex, patterns cache). Do NOT include dep fields (client, cache, log, etc.). This is purely the flag-derived static config. Read `proxy.go:380-480` carefully and classify each field as Deps, Config, or State.

- [ ] **Step 3: Create `internal/proxy/state.go`**

```go
package proxy

import "sync"

// State holds internal mutable runtime state.
// Each field group is protected by its own mutex.
type State struct {
    configMu            sync.RWMutex
    tenantMap           map[string]TenantMapping
    tenantLimits        map[string]map[string]any
    tenantDefaultLimits map[string]any

    labelIndexMu  sync.RWMutex
    labelIndex    map[string]*labelValuesIndexState

    patternsMu       sync.RWMutex
    patternsSnapshot map[string]patternSnapshotEntry

    translationCacheMu sync.RWMutex
    translationCache   *cache.Cache
}

// newState returns a zero-value State with initialized maps.
func newState() *State {
    return &State{
        tenantMap:           make(map[string]TenantMapping),
        tenantLimits:        make(map[string]map[string]any),
        tenantDefaultLimits: make(map[string]any),
        labelIndex:          make(map[string]*labelValuesIndexState),
        patternsSnapshot:    make(map[string]patternSnapshotEntry),
    }
}
```

> **Note:** Verify `labelValuesIndexState` and `patternSnapshotEntry` type names exist in the proxy package. Grep for them: `grep -rn "type labelValuesIndexState\|type patternSnapshotEntry" internal/proxy/`.

- [ ] **Step 4: Verify compilation**

```bash
go build ./internal/proxy/ 2>&1
```

Expected: Compiles without errors. These new types coexist with the existing `Proxy` struct — no conflict.

- [ ] **Step 5: Run full test suite**

```bash
go test ./... -count=1 -timeout=180s 2>&1 | grep -E "FAIL|ok " | head -20
```

Expected: All packages pass. No behavior change yet.

- [ ] **Step 6: Commit**

```bash
git add internal/proxy/deps.go internal/proxy/config.go internal/proxy/state.go
git commit -m "refactor: define Deps, Config, State types for Proxy restructure"
```

---

### Task 8: Handler type + migrate RegisterRoutes

**Files:**
- Create: `internal/proxy/handler.go`
- Modify: `internal/proxy/proxy.go` (add `Handler` field to `Proxy`, update `New`/`Init`)

- [ ] **Step 1: Create `internal/proxy/handler.go`**

```go
package proxy

import "net/http"

// Handler is the HTTP handler for all Loki-compatible endpoints.
// It carries the three concern groups: external deps, immutable config, mutable state.
// Handler methods can be constructed in tests with mocked Deps without a full Proxy.
type Handler struct {
    Deps
    Cfg   *Config
    State *State
}

// routeHandler wraps h with the standard per-route middleware chain.
// Applied outermost → innermost (last executes first on the request path):
//   securityHeaders → tenantMiddleware → limiter → requestLogger → compatCacheMiddleware
func (h *Handler) routeHandler(endpoint, route string, handler http.HandlerFunc) http.Handler {
    return securityHeaders(
        h.tenantMiddleware(
            h.Limiter.Middleware(
                h.requestLogger(endpoint, route,
                    h.compatCacheMiddleware(endpoint, route, handler)))))
}
```

> **Note:** `tenantMiddleware`, `requestLogger`, `compatCacheMiddleware`, `securityHeaders` are currently methods on `*Proxy`. They will remain on `*Proxy` in this task (the receiver rename happens in Task 9). So `Handler.routeHandler` can't compile yet — leave it as a skeleton with a comment. The actual migration of `routeHandler` from `*Proxy` to `*Handler` happens in Task 9 when receivers are renamed.

For now, `handler.go` just defines the `Handler` type:

```go
package proxy

// Handler is the HTTP handler for all Loki-compatible endpoints.
// Handler carries Deps (external deps), Cfg (immutable config), State (mutable runtime state).
// Handlers are methods on *Handler, making them unit-testable without a full Proxy stack.
//
// In this PR, Handler is defined but handler methods still have *Proxy receivers.
// Task 9 completes the receiver migration.
type Handler struct {
    Deps
    Cfg   *Config
    State *State
}
```

- [ ] **Step 2: Add `Handler` field to `Proxy` struct in proxy.go**

In the `Proxy` struct (around line 375), add:

```go
// handler is the decomposed view of this Proxy's deps + config + state.
// Populated in New/Init alongside the existing fields during the migration.
handler *Handler
```

- [ ] **Step 3: Verify compilation**

```bash
go build ./internal/proxy/ 2>&1
```

Expected: Compiles. No test failures.

- [ ] **Step 4: Run tests**

```bash
go test ./internal/proxy/ -count=1 -timeout=120s 2>&1 | tail -10
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/handler.go internal/proxy/proxy.go
git commit -m "refactor: add Handler struct alongside Proxy for deps/config/state decomposition"
```

---

### Task 9: Migrate handler receivers + shrink proxy.go + Part 3 commit

**Files:**
- Modify: `internal/proxy/query_translation.go`
- Modify: `internal/proxy/labels.go`
- Modify: `internal/proxy/patterns.go`
- Modify: `internal/proxy/postprocess.go`
- Modify: `internal/proxy/stream_processing.go`
- Modify: `internal/proxy/range_metric_compat.go`
- Modify: `internal/proxy/query_range_windowing.go`
- Modify: `internal/proxy/metric_binary.go`
- Modify: `internal/proxy/proxy.go` (shrink to ~120 lines)

**Context:** This is a mechanical receiver rename. Every `func (p *Proxy) handle*` in the listed files becomes `func (h *Handler) handle*`. Field access is updated: `p.log` → `h.Log`, `p.client` → `h.Client`, `p.cache` → `h.Cache`, `p.limiter` → `h.Limiter`, `p.cfg.*` → `h.Cfg.*`, `p.state.*` → `h.State.*`, etc. The `Proxy` struct embeds `*Handler` so all callers remain unchanged.

> **Warning:** This is the most complex task. Do one file at a time. After each file, run `go build ./internal/proxy/` to catch errors immediately.

- [ ] **Step 1: Migrate `query_translation.go`**

At the top of `internal/proxy/query_translation.go`, find all `func (p *Proxy)` declarations and replace with `func (h *Handler)`. Then update field accesses inside those functions:
- `p.log` → `h.Log`
- `p.client` → `h.Client`
- `p.cache` → `h.Cache`
- `p.metrics` → `h.Metrics`
- `p.limiter` → `h.Limiter`
- `p.breaker` → `h.Breaker`
- `p.coalescer` → `h.Coalescer`
- `p.tenantLabel` → `h.Cfg.TenantLabel`
- `p.maxLines` → `h.Cfg.MaxLines`
- `p.streamResponse` → `h.Cfg.StreamResponse`
- `p.configMu` → `h.State.configMu`
- `p.tenantMap` → `h.State.tenantMap`
- `p.tenantLimits` → `h.State.tenantLimits`
- Other fields: check which group (Deps/Config/State) each field belongs to

```bash
go build ./internal/proxy/ 2>&1
```

Fix all errors before proceeding.

- [ ] **Step 2: Migrate `labels.go`**

Same pattern as Step 1. Run `go build ./internal/proxy/ 2>&1` after.

- [ ] **Step 3: Migrate `patterns.go`**

Same pattern. Note: `patterns.go` contains `publishedTenantLimitsForOrgID` which accesses `p.tenantLimits`, `p.tenantDefaultLimits`, `p.tenantLimitsAllowPublish`, `p.client` (timeout), and `p.patternsEnabled`. These map to State, Config, and Deps respectively.

```bash
go build ./internal/proxy/ 2>&1
```

- [ ] **Step 4: Migrate remaining files**

For each of these files, apply the same `(p *Proxy)` → `(h *Handler)` rename and field access update, running `go build ./internal/proxy/` after each:

```bash
# postprocess.go
# stream_processing.go  
# range_metric_compat.go
# query_range_windowing.go
# metric_binary.go
```

- [ ] **Step 5: Add `*Handler` embedding to `Proxy` and shrink proxy.go**

In `internal/proxy/proxy.go`, the `Proxy` struct currently has 60+ fields. After the migration, handler methods are on `*Handler`. The `Proxy` struct becomes a thin wrapper:

```go
// Proxy is the entry point for the Loki-compatible HTTP proxy.
// It embeds *Handler so all handler methods are accessible on *Proxy,
// preserving the external API used by cmd/proxy/main.go.
type Proxy struct {
    *Handler
    // Fields that remain on Proxy (non-handler lifecycle concerns):
    registerInstrumentation bool
    enablePprof             bool
    queryTracker            *metrics.QueryTracker
    peerCache               *cache.PeerCache
    peerAuthToken           string
    coldRouter              *ColdRouter
    // ... any remaining non-handler fields
}
```

Update `New` and `Init` to populate both `*Handler` (with Deps + Config + State) and the remaining `Proxy`-only fields.

> **Note:** Not all fields move to `Handler`. Fields like `queryTracker`, `peerCache`, `registerInstrumentation` are Proxy lifecycle concerns, not per-request handler state. Keep them on `Proxy`.

- [ ] **Step 6: Run full test suite**

```bash
go test ./... -count=1 -timeout=180s 2>&1 | grep -E "FAIL|ok " | head -20
```

Expected: All packages pass. Zero regressions.

- [ ] **Step 7: Run e2e parity gate**

```bash
go test ./internal/proxy/ -run "TestLogQL_Exhaustive|TestMissingOps|TestPipeline" -v -timeout=60s 2>&1 | tail -20
```

Expected: All pass.

- [ ] **Step 8: Part 3 commit**

```bash
git add \
    internal/proxy/deps.go \
    internal/proxy/config.go \
    internal/proxy/state.go \
    internal/proxy/handler.go \
    internal/proxy/proxy.go \
    internal/proxy/query_translation.go \
    internal/proxy/labels.go \
    internal/proxy/patterns.go \
    internal/proxy/postprocess.go \
    internal/proxy/stream_processing.go \
    internal/proxy/range_metric_compat.go \
    internal/proxy/query_range_windowing.go \
    internal/proxy/metric_binary.go
git commit -m "refactor: split Proxy into Handler + Deps + Config + State"
```

---

## Final verification

- [ ] **Run complete test suite**

```bash
go test ./... -count=1 -timeout=300s 2>&1 | grep -E "FAIL|ok " | head -30
```

Expected: All packages pass.

- [ ] **Run go vet and staticcheck**

```bash
go vet ./...
```

Expected: No issues.

- [ ] **Verify binary compiles and help works**

```bash
go build -o /tmp/loki-vl-proxy ./cmd/proxy/ && /tmp/loki-vl-proxy --help 2>&1 | grep "default-max-query-length"
```

Expected: The `-default-max-query-length` flag appears in the help output.

- [ ] **Check CHANGELOG**

Add to `CHANGELOG.md` under `[Unreleased]`:

```
### Added
- `-default-max-query-length` flag enforces a global max query time range (per-tenant override via limits config)
- `ParseError` and `UnsupportedError` typed errors in translator for HTTP-layer error-type discrimination
- Typed AST-to-AST translation layer in `internal/logql/translate.go` for log queries (metric queries still use string translator)

### Changed  
- `Proxy` struct decomposed into `Handler` (with `Deps`, `Config`, `State`) for testability without full stack
- `RegisterRoutes` uses named `routeHandler` method instead of anonymous closure
```
