# LogsQL Typed AST & Builder Design

## Goal

Replace the 3000-line string-concatenation translator (`internal/translator`) with a typed LogsQL AST (`internal/logsql`) that:

1. **Translation target (A)** — The translator builds `logsql.Expr` nodes instead of concatenating strings. `expr.String()` produces the final LogsQL query at the boundary.
2. **Parity testing (C)** — Structural assertions (LogQL → expected LogsQL AST node types), round-trip string comparison against the current translator, and live e2e semantic verification.

Proxy-evaluated constructs (binary metric ops, subqueries, `label_replace`, `label_join`, `without`-grouping) stay as marker nodes in this iteration. Native LogsQL constructs become proper typed nodes.

---

## Approach: New `internal/logsql` package (Option 1)

Mirrors the structure of `internal/logql`. Self-contained, independently testable, consistent with existing codebase pattern.

---

## Section 1: Package structure

| File | Responsibility |
|------|----------------|
| `ast.go` | All typed AST nodes — `Expr` interface, filter nodes, pipe stage nodes, stats functions, marker nodes for proxy-evaluated constructs |
| `scanner.go` | Lexer — tokenises LogsQL syntax (`:=`, `:~`, `NOT`, `AND`, `OR`, `\|`, pipe keywords, all operator prefixes) |
| `parser.go` | Recursive-descent parser — `Parse(string) (Expr, error)`, `ParseFilter(string) (FilterExpr, error)` |
| `capabilities.go` | `Capabilities` struct mapping VL version → available constructs; `CapabilitiesFor(semver string) Capabilities`; maps proxy's existing `backendVersionSemver` + feature flags (`supportsStreamMetadata`, `supportsDensePatternWindowing`, `supportsMetadataSubstring`) to per-construct availability |
| `builder.go` | Builder API — `NewQuery()`, `WithFilter()`, `Pipe()`, `Stats()`, etc.; accepts `Capabilities`; chooses native constructs when VL version supports them, falls back to marker nodes when it doesn't |
| `logsql_test.go` | Round-trip tests (parse → `String()` == original), structural assertion helpers, capability profile tests, fuzz seeds |

No `semantic.go` or `validate.go` initially — LogsQL has almost no parse-time semantic constraints. Validation is VL's job.

### Hybrid migration path

The existing `internal/logql/translate.go` continues to produce strings during migration. Once `builder.go` exists, the translator's `buildStatsQuery`, `buildFilterExpr`, etc. are rewritten one-by-one to return `logsql.Expr` nodes and call `expr.String()` at the boundary — callers see no change.

---

## Section 2: AST node hierarchy

### Top-level

```go
type Query struct {
    Filter FilterExpr // everything before the first pipe (may be nil for wildcard)
    Pipes  []Pipe     // ordered pipe stages
}

func (q *Query) String() string
```

### Filter expressions (`FilterExpr` interface)

The real LogsQL grammar has **34 filter node types** and **12 field filter operators**. Initial scope covers what the translator currently emits plus forward-compatible extensions:

**Message filters:**
- `Word{Value string}` — bare word match
- `Phrase{Value string}` — `"text"` quoted phrase
- `Prefix{Value string}` — `text*` prefix
- `Substring{Value string}` — `*text*` substring
- `Exact{Value string}` — `="text"`
- `Regexp{Pattern string}` — `~"pattern"`
- `Sequence{Parts []string}` — `seq("a","b")`
- `CaseInsensitive{Value string}` — `i("text")`

**Field filters:**
- `FieldFilter{Field string, Op FieldOp, Value string, Negate bool}`

```go
type FieldOp int
const (
    FieldOpExact    FieldOp = iota // :="val"
    FieldOpRegexp                  // :~"pat"
    FieldOpPrefix                  // :prefix*
    FieldOpSubstring               // :*text*
    FieldOpEmpty                   // :""
    FieldOpAny                     // :*
    FieldOpGT                      // :>val
    FieldOpGTE                     // :>=val
    FieldOpLT                      // :<val
    FieldOpLTE                     // :<=val
    FieldOpRange                   // :range(min,max)
    FieldOpIn                      // :in(a,b,c)
)
```

**Stream/time:**
- `StreamFilter{Matchers []FieldFilter}` — `{key="val",...}`
- `TimeFilter{Range string}` — `_time:5m`, `_time:[abs,abs]`

**Logical:**
- `AndExpr{Left, Right FilterExpr}` — conjunction (implicit or explicit `AND`)
- `OrExpr{Left, Right FilterExpr}` — `f1 OR f2`
- `NotExpr{Expr FilterExpr}` — `NOT f`
- `Wildcard{}` — `*`

### Pipe stages (`Pipe` interface)

**Parsers:**
- `PipeUnpackJSON{}` → `| unpack_json`
- `PipeUnpackLogfmt{}` → `| unpack_logfmt`
- `PipeExtract{Pattern, From, If string}` → `| extract "pattern" from field`
- `PipeExtractRegexp{Pattern, From string}` → `| extract_regexp "pat" from field`

**Filters/fields:**
- `PipeFilter{Expr FilterExpr}` → `| filter <expr>`
- `PipeFields{Labels []string}` → `| fields f1,f2`
- `PipeDelete{Labels []string}` → `| delete f1,f2`

**Transform:**
- `PipeFormat{Template, ResultField string}` → `| format "<tmpl>" as field`
- `PipeRename{Pairs [][2]string}` → `| rename old as new`
- `PipeReplace{Field, Old, New string}` → `| replace (field, "old", "new")`
- `PipeReplaceRegexp{Field, Regex, Replacement string}` → `| replace_regexp (...)`
- `PipePackJSON{Fields []string, ResultField string}` → `| pack_json fields (f1,...) as result`
- `PipePackLogfmt{Fields []string, ResultField string}` → `| pack_logfmt ...`

**Aggregation:**
- `PipeStats{By []GroupKey, Funcs []StatsFuncAlias}` → `| stats by (f1,f2) count() as c`
- `PipeMath{Expr, Alias string}` → `| math alias:=expr`
- `PipeSort{By []SortField, Limit int}` → `| sort by (f desc) limit N`
- `PipeLimit{N int}` → `| limit N`

**Stats functions** (26 total inside `PipeStats`):

```go
type StatsFunc interface{ statsFunc() }

Count(), Sum(field), Min(field), Max(field), Avg(field), Median(field),
Quantile(phi float64, field), Stddev(field), Stdvar(field),
Rate(), RateSum(field),
CountUniq(field), CountUniqHash(field), UniqValues(field, limit),
FieldMax(field), FieldMin(field), JSONValues(field),
Any(field), CountEmpty(field),
SumLen(field), Values(field, limit), Histogram(field),
Last(field), First(field), RowAny(fields...), RowMax(by, fields...)
```

```go
type GroupKey struct {
    Field string // field name to group by
}

type StatsFuncAlias struct {
    Func  StatsFunc
    Alias string
}

type SortField struct {
    Field string
    Desc  bool
}
```

**Marker nodes** (proxy-evaluated, serialize back to current marker strings):

```go
type DeferredBinaryOp struct {
    Op    string
    Left  string // already-serialised LogsQL for left operand
    Right string
    VM    *VectorMatchInfo
}

type DeferredSubquery struct {
    OuterFunc string
    Inner     string
    Range     string
    Step      string
}

type DeferredLabelReplace struct {
    Inner  string
    Args   [4]string // dst, src, replacement, regex
}

type DeferredLabelJoin struct {
    Inner  string
    Dst    string
    Sep    string
    Srcs   []string
}

type DeferredGroup struct {
    Inner     string
    GroupType string // "without" or "by"
    Labels    []string
}
```

---

## Section 3: Builder API

Lives in `builder.go`. Two construction modes: direct (caller assembles nodes) and capability-aware (builder picks best construct for detected VL version).

### Direct construction

```go
q := logsql.NewQuery(
    logsql.And(
        logsql.FieldExact("app", "nginx"),
        logsql.Word("error"),
    ),
    logsql.PipeUnpackJSON{},
    logsql.PipeFilter{Expr: logsql.FieldGTE("status", "500")},
    logsql.PipeStats{
        By:    []logsql.GroupKey{{Field: "host"}},
        Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count(), Alias: "count"}},
    },
)
// → `app:="nginx" error | unpack_json | filter status:>=500 | stats by (host) count() as count`
q.String()
```

### Capability-aware construction

```go
b := logsql.NewBuilder(caps) // caps = logsql.CapabilitiesFor(p.backendVersionSemver)

// | top N by(field) on v1.50+, else | sort by (field desc) | limit N
b.BestTopN(10, "host") // returns []Pipe

// ipv4_range filter on v1.45+, else regexp approximation
b.BestIPv4Range("client_ip", "192.168.0.0/24") // returns FilterExpr
```

### Translator integration

Translator's `build*` functions rewritten one-by-one to return `logsql.Pipe` or `logsql.FilterExpr`, assembled into a `logsql.Query`, then `.String()` called once at the boundary. Each rewrite is a self-contained PR touching one translator function.

---

## Section 4: Capabilities

```go
type Capabilities struct {
    // Mapped from proxy.backendFeatures
    StreamMetadata        bool // v1.30+
    MetadataSubstring     bool // v1.49+
    DensePatternWindowing bool // v1.50+

    // LogsQL construct availability
    PipeUniq       bool // v1.19+
    PipeTop        bool // v1.20+
    PipeHits       bool // v1.13+
    PipeRunning    bool // v1.32+
    PipeBlock      bool // v1.39+
    StatsRateSum   bool // v1.44+
    StatsHistogram bool // v1.31+
    FieldIPv4Range bool // v1.45+
}

func CapabilitiesFor(semver string) Capabilities
```

The proxy already has `backendVersionSemver` (string, under `backendVersionMu`) and three booleans. `CapabilitiesFor` parses the semver once at startup and populates the struct. The proxy stores one `Capabilities` value and passes it to `logsql.NewBuilder(caps)` at query construction time.

---

## Parity testing strategy

**Level 1 — Structural assertions (pure Go unit tests):**
```go
// rate({app="api"}[5m]) should produce a stats query with count-based rate
got, _ := logsql.Parse(translator.TranslateLogQL(`rate({app="api"}[5m])`))
q := got.(*logsql.Query)
stats := q.Pipes[len(q.Pipes)-1].(logsql.PipeStats)
assert(stats.Funcs[0].Func == logsql.Count())
```

**Level 2 — Round-trip string comparison:**
```go
// New AST translator must produce identical output to the old regex translator
logqlQuery := `sum by (app) (rate({app="api"} | json [5m]))`
oldOutput := translator.TranslateLogQL(logqlQuery)
newExpr, _ := NewTranslator(caps).Translate(logqlQuery)
assert(newExpr.String() == oldOutput)
```

**Level 3 — Semantic parity (e2e, needs compose stack):**
Run the same LogQL against both Loki and the proxy, compare response values. Existing e2e-compat tests already do this — the LogsQL AST parity tests extend coverage into structural validation.

---

## Implementation order

1. **`ast.go`** — node types, `String()` methods, marker nodes
2. **`parser.go` + `scanner.go`** — parse what the translator currently emits (translator-output subset + forward extensions)
3. **`builder.go`** — direct construction API
4. **Translator migration** — rewrite `buildFilterExpr` → returns `logsql.FilterExpr`; then `buildStatsQuery`; then remaining functions; each as a separate PR

The LogQL parser (`internal/logql`) already exists and is the template for the lexer and parser structure.
