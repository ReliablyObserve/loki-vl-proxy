# Proxy Architecture Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve loki-vl-proxy in three sequential PRs: (1) query-range limit enforcement and error-type distinction, (2) wire the existing typed AST into the translator so string-based dispatch is replaced by AST walking, (3) restructure the Proxy monolith into Deps + Config + State for testability.

**Invariant:** Loki and VictoriaLogs compatibility must remain 100% throughout. All 554 translator unit tests, the e2e parity suite, and the exhaustive LogQL gap tests must pass after every PR.

**Codebase snapshot:**
- `internal/logql/` — 4,322-line typed AST package (scanner, parser, AST nodes, validate). Already complete. `translate.go` is 24 lines that just roundtrips `expr.String()` back into the string-based translator — this is the key gap.
- `internal/translator/translator.go` — 3,209-line monolith mixing string extraction, rewriting, and LogsQL emission.
- `internal/proxy/proxy.go` — 1,909-line `Proxy` struct with 60+ fields: external deps, immutable config, and mutable runtime state all in one type.

---

## PR 1 — Query-range enforcement + error types + middleware documentation

### Overview

Three independent, safe changes that can ship immediately. No Loki/VL compatibility risk.

### 1.1 — Query-length enforcement

**Context:** `max_query_length` (default `"30d1h"`) and `max_query_range` (default `"0s"`) are already published via `/config/tenant/v1/limits` and `/loki/api/v1/drilldown-limits` but are not enforced. Long VL queries currently time out silently.

**Change:** In `handleQueryRange` (and `handleQuery` for instant queries), after extracting `start`/`end` nanoseconds, read the effective per-tenant limit and reject with HTTP 400 if exceeded.

```go
// internal/proxy/limits_enforcement.go (new file)

// effectiveMaxQueryLength returns the enforced max query length for orgID.
// Returns 0 when enforcement is disabled (default, matches Loki global default).
func (p *Proxy) effectiveMaxQueryLength(orgID string) time.Duration {
    limits := p.publishedTenantLimitsForOrgID(orgID)
    raw, _ := limits["max_query_length"].(string)
    d, err := parseLokiDuration(raw)
    if err != nil || d == 0 {
        return p.defaultMaxQueryLength // from flag
    }
    return d
}
```

New flag: `-default-max-query-length` (type `time.Duration`, default `0` = unlimited). A per-tenant override in the limits config takes precedence.

Error response format matches Loki exactly:
```json
{"status":"error","errorType":"bad_data","error":"query length 721h1m0s exceeds limit 168h0m0s"}
```

**Files changed:**
- `internal/proxy/limits_enforcement.go` — new: `effectiveMaxQueryLength`, `parseLokiDuration` helper
- `internal/proxy/query_translation.go` — add enforcement call in `handleQueryRange` and `handleQuery`
- `cmd/proxy/main.go` — register `-default-max-query-length` flag
- `internal/proxy/proxy.go` — add `defaultMaxQueryLength time.Duration` field to Config/Proxy

**Tests:**
- Unit: `TestEffectiveMaxQueryLength_NoLimit`, `TestEffectiveMaxQueryLength_FlagDefault`, `TestEffectiveMaxQueryLength_TenantOverride`
- Handler: `TestHandleQueryRange_ExceedsMaxQueryLength_Returns400`, `TestHandleQueryRange_WithinLimit_Passes`

### 1.2 — ParseError vs UnsupportedError distinction

**Context:** `translator.TranslateLogQL` returns all errors via `fmt.Errorf`. Clients (and Grafana) cannot distinguish invalid LogQL syntax from valid-but-untranslatable queries.

**Change:** New file `internal/translator/errors.go`:

```go
// ParseError: input is not valid LogQL syntax.
// Proxy HTTP layer returns errorType "parse error".
type ParseError struct {
    Msg string
    Pos int // byte offset in input, -1 if unknown
}
func (e *ParseError) Error() string { return e.Msg }

// UnsupportedError: valid LogQL that cannot be translated to LogsQL.
// Proxy HTTP layer returns errorType "bad_data".
type UnsupportedError struct {
    Msg  string
    Func string // the LogQL function/construct that is unsupported
}
func (e *UnsupportedError) Error() string { return e.Msg }
```

Translator returns `&ParseError{}` for: unmatched braces, invalid regex in drop/keep, malformed stream selectors.
Translator returns `&UnsupportedError{}` for: `count_values`, missing `| unwrap` on range agg, metric agg without range.

Proxy HTTP layer in `query_translation.go` uses `errors.As` to set `errorType`:

```go
var parseErr *translator.ParseError
var unsupErr *translator.UnsupportedError
switch {
case errors.As(err, &parseErr):
    p.writeJSONError(w, http.StatusBadRequest, "parse error", err.Error())
case errors.As(err, &unsupErr):
    p.writeJSONError(w, http.StatusBadRequest, "bad_data", err.Error())
default:
    p.writeJSONError(w, http.StatusBadRequest, "bad_data", err.Error())
}
```

**Files changed:**
- `internal/translator/errors.go` — new: `ParseError`, `UnsupportedError`
- `internal/translator/translator.go` — update ~12 error return sites
- `internal/proxy/query_translation.go` — update error dispatch in `handleQueryRange`, `handleQuery`, `handleLabels`, etc.

**Tests:**
- `TestTranslateLogQL_ParseError_UnmatchedBrace`
- `TestTranslateLogQL_UnsupportedError_CountValues`
- `TestTranslateLogQL_UnsupportedError_MissingUnwrap`
- HTTP handler test: verify `errorType` field value per error class

### 1.3 — Explicit middleware stack documentation

**Change:** Extract the anonymous `rl` closure in `RegisterRoutes` into a named method:

```go
// routeHandler wraps h with the standard per-route middleware chain.
// Applied outermost → innermost (last executes first on the request path):
//   securityHeaders → tenantMiddleware → limiter → requestLogger → compatCache
func (p *Proxy) routeHandler(endpoint, route string, h http.HandlerFunc) http.Handler {
    return securityHeaders(
        p.tenantMiddleware(
            p.limiter.Middleware(
                p.requestLogger(endpoint, route,
                    p.compatCacheMiddleware(endpoint, route, h)))))
}
```

Add matching doc comment to `wrapHandler` in `cmd/proxy/main.go` listing the global chain order. No behaviour change.

**Files changed:**
- `internal/proxy/proxy.go` — extract `routeHandler` method, update `RegisterRoutes` to call it
- `cmd/proxy/main.go` — doc comment on `wrapHandler`

---

## PR 2 — Wire typed AST into translator (replace string-based dispatch)

### Overview

`internal/logql/translate.go` currently does:
```go
func Translate(expr Expr, opts TranslateOptions) (string, error) {
    canonical := expr.String()
    return translator.TranslateLogQLWithStreamFields(canonical, opts.LabelFn, opts.StreamFields)
}
```

This roundtrip through `expr.String()` throws away all the typed information the parser produced. PR 2 replaces this with a real AST walker: `translate.go` pattern-matches on concrete `logql.Expr` types and emits LogsQL directly, without re-parsing the canonical string.

### Package layout after PR 2

```
internal/logql/
  scanner.go         (unchanged — 462 lines)
  parser.go          (unchanged — 972 lines)
  ast.go             (unchanged — 476 lines)
  validate.go        (unchanged — 81 lines)
  semantic.go        (unchanged — 101 lines)
  translate.go       (REWRITTEN — from 24 → ~600 lines, real AST walker)

internal/translator/
  translator.go      (unchanged public API; string-based paths retained for
                      OpaqueMetricExpr and legacy callers not yet migrated)
  errors.go          (from PR 1)
  [all existing test files unchanged]
```

### AST walker design

`translate.go` becomes a typed switch over `logql.Expr`:

```go
func Translate(expr Expr, opts TranslateOptions) (string, error) {
    return translateExpr(expr, opts)
}

func translateExpr(expr Expr, opts TranslateOptions) (string, error) {
    switch e := expr.(type) {
    case *LogQuery:
        return translateLogQuery(e, opts)
    case *RangeAggregation:
        return translateRangeAgg(e, opts)
    case *VectorAggregation:
        return translateVectorAgg(e, opts)
    case *BinOpExpr:
        return translateBinOp(e, opts)
    case *LiteralExpr:
        return e.String(), nil
    case *OpaqueMetricExpr:
        // label_replace, label_join — fall through to string-based translator
        return translator.TranslateLogQLWithStreamFields(e.Raw, opts.LabelFn, opts.StreamFields)
    default:
        return "", &translator.UnsupportedError{Msg: fmt.Sprintf("unsupported expression type %T", expr)}
    }
}
```

`translateLogQuery` walks `*LogQuery` — it emits the VL stream selector from `*StreamSelector`, then each `Stage` in `Pipeline` via `translateStage`:

```go
func translateStage(s Stage, opts TranslateOptions, aliases map[string]string) (string, error) {
    switch st := s.(type) {
    case *LineFilterStage:   return translateLineFilter(st, opts)
    case *ParserStage:       return translateParser(st, opts, aliases)
    case *LabelFilterStage:  return translateLabelFilter(st, opts, aliases)
    case *DropStage:         return translateDrop(st, opts)
    case *KeepStage:         return translateKeep(st, opts)
    case *UnwrapStage:       return translateUnwrap(st, opts)
    case *LineFormatStage:   return translateLineFormat(st, opts)
    case *LabelFormatStage:  return translateLabelFormat(st, opts)
    case *DecolorizeStage:   return "", nil // VL no-op
    default:
        return "", &translator.UnsupportedError{Msg: fmt.Sprintf("unsupported stage %T", s)}
    }
}
```

### Alias tracking

`ParserStage` with `json field_alias="original_field"` aliases need to be tracked so subsequent `LabelFilterStage` nodes on alias names are rewritten to the original VL field name. `aliases map[string]string` is built as pipeline stages are processed and passed to `translateLabelFilter`.

### OpaqueMetricExpr passthrough

`label_replace` and `label_join` are parsed by the logql parser as `*OpaqueMetricExpr` (the parser does not model them as typed nodes, by design — they require post-processing markers in the translator). These fall through to the existing string-based translator. This is correct and maintains all existing marker-based post-processing.

### Compatibility guarantee

- All 554 translator unit tests run unchanged (they call `translator.TranslateLogQL`, not `logql.Translate`)
- `logql.Translate` is tested separately via `internal/logql/translate_test.go`
- e2e parity suite runs before merge
- The public API of `internal/translator` is untouched — proxy callers are not changed in this PR

### Key test additions

```go
// internal/logql/translate_test.go additions
TestTranslate_StreamSelectorOnly
TestTranslate_LineFilter_Contains
TestTranslate_LineFilter_Pattern
TestTranslate_Parser_JSON_WithAlias
TestTranslate_Parser_JSON_Alias_ThenFilter      // the json_field_alias_then_filter case
TestTranslate_Parser_Unpack_WithFilter
TestTranslate_LabelFilter_Numeric
TestTranslate_DropStage
TestTranslate_KeepStage
TestTranslate_UnwrapStage_Bare
TestTranslate_UnwrapStage_Bytes
TestTranslate_LineFormatStage
TestTranslate_LabelFormatStage
TestTranslate_RangeAgg_Rate
TestTranslate_RangeAgg_QuantileOverTime
TestTranslate_VectorAgg_SumBy
TestTranslate_VectorAgg_TopK
TestTranslate_BinOp_Simple
TestTranslate_OpaqueMetricExpr_LabelReplace_Fallback
```

---

## PR 3 — Proxy Deps / Config / State restructure

### Overview

The `Proxy` struct has 60+ fields covering three distinct concerns: external injectable dependencies, immutable startup configuration, and mutable runtime state. This PR splits them into named types, making handlers unit-testable without a full stack.

### New types

```go
// internal/proxy/deps.go

// Deps holds external dependencies — injectable and mockable in unit tests.
type Deps struct {
    Backend     *url.URL
    Client      *http.Client      // VL query client
    TailClient  *http.Client      // WebSocket tail client
    Cache       *cache.Cache      // L1 memory cache
    CompatCache *cache.Cache      // L2 disk-backed compat cache
    Log         *slog.Logger
    Metrics     *metrics.Metrics
    Limiter     *mw.RateLimiter
    Breaker     *mw.CircuitBreaker
    Coalescer   *mw.Coalescer
}
```

```go
// internal/proxy/config.go

// Config holds immutable startup configuration — set once from flags, never written after Init.
type Config struct {
    TenantLabel              string
    AuthEnabled              bool
    RequireTenantHeader      bool
    QueryRangeWindowing      bool
    SplitInterval            time.Duration
    MaxParallel              int
    WindowTimeout            time.Duration
    DefaultMaxQueryLength    time.Duration   // from PR 1
    // ... all remaining ~30 flag-derived fields
}
```

```go
// internal/proxy/state.go

// State holds internal mutable runtime state.
// Each field group is protected by its own mutex — no shared lock.
type State struct {
    configMu            sync.RWMutex
    tenantMap           map[string]TenantMapping
    tenantLimits        map[string]map[string]any
    tenantDefaultLimits map[string]any

    labelIndexMu        sync.RWMutex
    labelIndex          map[string]*labelValuesIndexState

    patternsMu          sync.RWMutex
    patternsSnapshot    map[string]patternSnapshotEntry

    translationCacheMu  sync.RWMutex
    translationCache    *cache.Cache
}
```

### Handler — new method receiver

```go
// internal/proxy/handler.go

// Handler is the HTTP handler for all Loki-compatible endpoints.
// Handlers are methods on *Handler so they can be constructed in tests
// with mocked Deps, without spinning up cache, patterns engine, etc.
type Handler struct {
    Deps
    Cfg   *Config
    State *State
}

// All handler methods migrate from *Proxy to *Handler:
// func (p *Proxy) handleQueryRange  →  func (h *Handler) handleQueryRange
// func (p *Proxy) handleLabels      →  func (h *Handler) handleLabels
// ... (all methods in query_translation.go, labels.go, patterns.go, etc.)
```

### Proxy — thin backward-compatible wrapper

```go
// internal/proxy/proxy.go (shrinks to ~120 lines)

type Proxy struct{ *Handler }

func New(cfg ProxyConfig) (*Proxy, error) {
    deps   := buildDeps(cfg)
    config := buildConfig(cfg)
    state  := buildState(cfg)
    return &Proxy{Handler: &Handler{Deps: deps, Cfg: config, State: state}}, nil
}

// Init, RegisterRoutes, GetMetrics, ValidateBackendVersionCompatibility,
// DownstreamConnectionPressure — all promoted via *Handler embedding.
// cmd/proxy/main.go unchanged.
```

### Testability win

```go
// Example: unit test for query-length enforcement (from PR 1)
func TestHandleQueryRange_MaxQueryLength(t *testing.T) {
    h := &Handler{
        Deps: Deps{
            Backend: mustURL("http://fake-vl"),
            Client:  &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
                return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
            })},
            Log:     slog.Default(),
            Metrics: metrics.NewNoop(),
        },
        Cfg:   &Config{DefaultMaxQueryLength: time.Hour},
        State: newState(),
    }
    req := httptest.NewRequest("GET", `/loki/api/v1/query_range?query={app="x"}&start=0&end=`+
        strconv.FormatInt(int64(2*time.Hour), 10), nil)
    w := httptest.NewRecorder()
    h.handleQueryRange(w, req)
    if w.Code != http.StatusBadRequest {
        t.Errorf("want 400, got %d", w.Code)
    }
}
```

### Migration strategy — receiver rename only

Handler methods are currently `func (p *Proxy) handle*`. The migration is mechanical:
1. Define `Handler`, `Deps`, `Config`, `State`
2. Move fields from `Proxy` to their new homes
3. Replace `(p *Proxy)` with `(h *Handler)` on all handler methods in: `query_translation.go`, `labels.go`, `patterns.go`, `postprocess.go`, `stream_processing.go`, `range_metric_compat.go`, `query_range_windowing.go`, `metric_binary.go`
4. Update field access: `p.cache` → `h.Cache`, `p.log` → `h.Log`, `p.cfg.*` → `h.Cfg.*`, `p.state.*` → `h.State.*`
5. Keep `Proxy` as embedding wrapper — all public callers unchanged

### Files changed

| File | Change |
|------|--------|
| `internal/proxy/deps.go` | New — `Deps` struct + `buildDeps` |
| `internal/proxy/config.go` | New — `Config` struct + `buildConfig` |
| `internal/proxy/state.go` | New — `State` struct + `buildState`, `newState` |
| `internal/proxy/handler.go` | New — `Handler` struct + `RegisterRoutes`, `routeHandler` |
| `internal/proxy/proxy.go` | Shrinks to ~120 lines — thin `Proxy` wrapper + `New` |
| `internal/proxy/query_translation.go` | Receiver `*Proxy` → `*Handler`, field access updated |
| `internal/proxy/labels.go` | Same |
| `internal/proxy/patterns.go` | Same |
| `internal/proxy/postprocess.go` | Same |
| `internal/proxy/stream_processing.go` | Same |
| `internal/proxy/range_metric_compat.go` | Same |
| `internal/proxy/query_range_windowing.go` | Same |
| `internal/proxy/metric_binary.go` | Same |
| `cmd/proxy/main.go` | No change |

### Tests

All existing unit tests and e2e parity tests must pass unchanged (they go through `proxy.New`, which keeps the same signature). New handler unit tests can be added using `&Handler{Deps: ..., Cfg: ..., State: ...}` directly.

---

## Sequencing and compatibility gates

Each PR merges to main independently:

```
PR 1 → merge → PR 2 (rebased on post-PR1 main) → merge → PR 3 (rebased) → merge
```

Every PR must pass before merge:
- `go test ./...` — full unit suite (554 translator tests + all proxy tests)
- `go vet ./...` and `staticcheck ./...`
- e2e compat suite: `TestLogQL_Exhaustive_QueryParity`, `TestLogQL_Exhaustive_KnownGaps`, `TestMissingOps_*`
- `scripts/ci/check_quality_gate.py` — Loki compat must remain 100%

No PR introduces behaviour changes to the translation logic. PR 2 only affects `internal/logql/translate.go`; proxy callers in `internal/proxy/` still call `translator.TranslateLogQL` throughout all three PRs. Migrating proxy call-sites to `logql.Translate` (so they benefit from AST-level early validation) is explicitly out of scope — that is a follow-on PR 4 after all three PRs are merged and stable.
