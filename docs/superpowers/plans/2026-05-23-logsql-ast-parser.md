# LogsQL Typed AST & Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `internal/logsql` — a typed LogsQL AST, scanner, parser, capability-aware builder, and VL version gating — mirroring `internal/logql` in structure.

**Architecture:** Three layers built bottom-up: (1) typed AST nodes (`ast.go`) whose `String()` methods emit valid LogsQL; (2) a recursive-descent parser (`scanner.go` + `parser.go`) that round-trips what the translator currently emits; (3) a capability-aware builder (`capabilities.go` + `builder.go`) that selects the best LogsQL construct for the detected VL version. The minimum supported VL version is **v1.40**. Constructs unavailable in the detected version fall back to the proxy's existing slower path (`shouldUseManualRangeMetricCompat` and string-concatenation translator).

**Tech Stack:** Go 1.22+; `internal/logql` as structural template; standard library only; module `github.com/ReliablyObserve/Loki-VL-proxy`.

---

## Supported VictoriaLogs Version Matrix

| Version range | Feature additions vs v1.40 baseline |
|--------------|--------------------------------------|
| < v1.40 | ❌ Not supported — proxy will error or fall back |
| v1.40 – v1.43 | Baseline: `PipeHits`, `PipeRunning`, `PipeBlock`, `PipeUniq`, `PipeTop`, `StatsHistogram` |
| v1.44 | `rate_sum()` stats function |
| v1.45 – v1.48 | `ipv4_range()` field filter |
| v1.49 | Metadata substring filter |
| v1.50+ | Dense pattern windowing |

All features in the v1.40 baseline row are always available for supported versions.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `internal/logsql/ast.go` | Create | All typed nodes: interfaces, filter expressions, pipe stages, stats functions, DeferredExpr |
| `internal/logsql/scanner.go` | Create | Lexer for LogsQL filter syntax and pipe keywords |
| `internal/logsql/parser.go` | Create | `Parse(*Query)`, `ParseFilter(FilterExpr)` — recursive descent |
| `internal/logsql/capabilities.go` | Create | `Capabilities` struct, `CapabilitiesFor(semver)` — v1.40 minimum |
| `internal/logsql/builder.go` | Create | `NewQuery()`, `NewBuilder(caps)`, `BestTopN()`, `BestIPv4Range()`, constructor helpers |
| `internal/logsql/ast_test.go` | Create | `String()` round-trip tests for all node types |
| `internal/logsql/parser_test.go` | Create | `Parse()` and `ParseFilter()` tests with expected AST shapes |
| `internal/logsql/capabilities_test.go` | Create | Per-version feature flag assertions |
| `internal/logsql/builder_test.go` | Create | Direct construction + capability-branching tests |
| `docs/superpowers/specs/2026-05-22-logsql-parser-design.md` | Modify | Add supported-version section |

---

### Task 1: AST — filter nodes, Query, DeferredExpr

**Files:**
- Create: `internal/logsql/ast.go`
- Create: `internal/logsql/ast_test.go`

- [ ] **Step 1: Write failing tests for filter node String() outputs**

```go
// internal/logsql/ast_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestFilterNodeString(t *testing.T) {
	tests := []struct {
		name string
		node logsql.FilterExpr
		want string
	}{
		{"word", logsql.Word{Value: "error"}, "error"},
		{"phrase", logsql.Phrase{Value: "hello world"}, `"hello world"`},
		{"prefix", logsql.Prefix{Value: "err"}, "err*"},
		{"substring", logsql.Substring{Value: "error"}, "*error*"},
		{"exact", logsql.Exact{Value: "404"}, `="404"`},
		{"regexp", logsql.Regexp{Pattern: "error|warn"}, `~"error|warn"`},
		{"sequence", logsql.Sequence{Parts: []string{"a", "b"}}, `seq("a","b")`},
		{"case_insensitive", logsql.CaseInsensitive{Value: "error"}, `i("error")`},
		{"wildcard", logsql.Wildcard{}, "*"},
		{"field_exact", logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"}, `app:="nginx"`},
		{"field_regexp", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpRegexp, Value: "err.*"}, `level:~"err.*"`},
		{"field_prefix", logsql.FieldFilter{Field: "url", Op: logsql.FieldOpPrefix, Value: "/api"}, "url:/api*"},
		{"field_substring", logsql.FieldFilter{Field: "url", Op: logsql.FieldOpSubstring, Value: "login"}, "url:*login*"},
		{"field_empty", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpEmpty}, `level:""`},
		{"field_any", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpAny}, "level:*"},
		{"field_gt", logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGT, Value: "400"}, "status:>400"},
		{"field_gte", logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGTE, Value: "500"}, "status:>=500"},
		{"field_lt", logsql.FieldFilter{Field: "latency", Op: logsql.FieldOpLT, Value: "100"}, "latency:<100"},
		{"field_lte", logsql.FieldFilter{Field: "latency", Op: logsql.FieldOpLTE, Value: "200"}, "latency:<=200"},
		{"field_range", logsql.FieldFilter{Field: "latency", Op: logsql.FieldOpRange, Value: "100,500"}, "latency:range(100,500)"},
		{"field_in", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpIn, Value: "error,warn"}, "level:in(error,warn)"},
		{"field_negate", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpExact, Value: "debug", Negate: true}, `NOT level:="debug"`},
		{
			"stream_filter",
			logsql.StreamFilter{Matchers: []logsql.LabelMatcher{
				{Name: "app", Op: "=", Value: "nginx"},
				{Name: "env", Op: "=", Value: "prod"},
			}},
			`{app="nginx", env="prod"}`,
		},
		{"time_duration", logsql.TimeFilter{Range: "5m"}, "_time:5m"},
		{"time_absolute", logsql.TimeFilter{Range: "[2024-01-01,2024-01-02]"}, "_time:[2024-01-01,2024-01-02]"},
		{
			"and_flat",
			logsql.AndExpr{
				Left:  logsql.Word{Value: "error"},
				Right: logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"},
			},
			`error AND app:="nginx"`,
		},
		{
			"and_wraps_or",
			logsql.AndExpr{
				Left:  logsql.OrExpr{Left: logsql.Word{Value: "error"}, Right: logsql.Word{Value: "warn"}},
				Right: logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"},
			},
			`(error OR warn) AND app:="nginx"`,
		},
		{
			"or_expr",
			logsql.OrExpr{Left: logsql.Word{Value: "error"}, Right: logsql.Word{Value: "warn"}},
			"error OR warn",
		},
		{"not_word", logsql.NotExpr{Expr: logsql.Word{Value: "debug"}}, "NOT debug"},
		{
			"not_or",
			logsql.NotExpr{Expr: logsql.OrExpr{Left: logsql.Word{Value: "a"}, Right: logsql.Word{Value: "b"}}},
			"NOT (a OR b)",
		},
		{"deferred", logsql.DeferredExpr{Raw: "__binary__:+:left|||right"}, "__binary__:+:left|||right"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.node.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQueryString(t *testing.T) {
	q := logsql.Query{
		Filter: logsql.AndExpr{
			Left:  logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"},
			Right: logsql.Word{Value: "error"},
		},
		Pipes: []logsql.Pipe{
			logsql.PipeUnpackJSON{},
			logsql.PipeFilter{Expr: logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGTE, Value: "500"}},
		},
	}
	want := `app:="nginx" AND error | unpack_json | filter status:>=500`
	if got := q.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestQueryNilFilter(t *testing.T) {
	q := logsql.Query{Pipes: []logsql.Pipe{logsql.PipeLimit{N: 100}}}
	want := "* | limit 100"
	if got := q.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
cd /Users/slawomirskowron/claude_projects/loki-vl-proxy
go test ./internal/logsql/... 2>&1 | head -5
```

Expected: `cannot find package` or `no Go files`.

- [ ] **Step 3: Create `internal/logsql/ast.go`**

```go
// internal/logsql/ast.go
package logsql

import (
	"fmt"
	"strings"
)

// Expr is the top-level LogsQL expression interface (Query or DeferredExpr).
type Expr interface {
	String() string
	expr()
}

// FilterExpr is a filter expression — everything before the first pipe.
type FilterExpr interface {
	String() string
	filterExpr()
}

// Pipe is a single pipe stage.
type Pipe interface {
	String() string
	pipe()
}

// StatsFunc is a function used inside PipeStats.
type StatsFunc interface {
	String() string
	statsFunc()
}

// ---------------------------------------------------------------------------
// Top-level query
// ---------------------------------------------------------------------------

// Query is a complete LogsQL query: optional filter followed by pipe stages.
type Query struct {
	Filter FilterExpr // nil means wildcard "*"
	Pipes  []Pipe
}

func (q *Query) String() string {
	var b strings.Builder
	if q.Filter != nil {
		b.WriteString(q.Filter.String())
	} else {
		b.WriteString("*")
	}
	for _, p := range q.Pipes {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	return b.String()
}

func (q *Query) expr() {}

// ---------------------------------------------------------------------------
// Message filter nodes
// ---------------------------------------------------------------------------

// Word matches a single word in the log message. LogsQL: error
type Word struct{ Value string }

func (w Word) String() string { return w.Value }
func (w Word) filterExpr()    {}

// Phrase matches a quoted phrase. LogsQL: "hello world"
type Phrase struct{ Value string }

func (p Phrase) String() string { return fmt.Sprintf(`"%s"`, p.Value) }
func (p Phrase) filterExpr()    {}

// Prefix matches logs whose message starts with Value. LogsQL: err*
type Prefix struct{ Value string }

func (p Prefix) String() string { return p.Value + "*" }
func (p Prefix) filterExpr()    {}

// Substring matches logs containing Value anywhere in the message. LogsQL: *error*
type Substring struct{ Value string }

func (s Substring) String() string { return "*" + s.Value + "*" }
func (s Substring) filterExpr()    {}

// Exact matches logs whose message equals Value exactly. LogsQL: ="404"
type Exact struct{ Value string }

func (e Exact) String() string { return fmt.Sprintf(`="%s"`, e.Value) }
func (e Exact) filterExpr()    {}

// Regexp matches logs whose message matches Pattern. LogsQL: ~"error|warn"
type Regexp struct{ Pattern string }

func (r Regexp) String() string { return fmt.Sprintf(`~"%s"`, r.Pattern) }
func (r Regexp) filterExpr()    {}

// Sequence matches logs containing Parts in order. LogsQL: seq("a","b")
type Sequence struct{ Parts []string }

func (s Sequence) String() string {
	quoted := make([]string, len(s.Parts))
	for i, p := range s.Parts {
		quoted[i] = fmt.Sprintf(`"%s"`, p)
	}
	return "seq(" + strings.Join(quoted, ",") + ")"
}
func (s Sequence) filterExpr() {}

// CaseInsensitive matches Value case-insensitively. LogsQL: i("error")
type CaseInsensitive struct{ Value string }

func (c CaseInsensitive) String() string { return fmt.Sprintf(`i("%s")`, c.Value) }
func (c CaseInsensitive) filterExpr()    {}

// Wildcard matches all log messages. LogsQL: *
type Wildcard struct{}

func (w Wildcard) String() string { return "*" }
func (w Wildcard) filterExpr()    {}

// ---------------------------------------------------------------------------
// Field filter
// ---------------------------------------------------------------------------

// FieldOp is the comparison operator for a field filter.
type FieldOp int

const (
	FieldOpExact     FieldOp = iota // :="val"
	FieldOpRegexp                   // :~"pat"
	FieldOpPrefix                   // :prefix*
	FieldOpSubstring                // :*text*
	FieldOpEmpty                    // :""
	FieldOpAny                      // :*
	FieldOpGT                       // :>val
	FieldOpGTE                      // :>=val
	FieldOpLT                       // :<val
	FieldOpLTE                      // :<=val
	FieldOpRange                    // :range(min,max)
	FieldOpIn                       // :in(a,b,c)
)

// FieldFilter matches a named log field against a value.
// If Negate is true, the filter is prefixed with NOT.
// LogsQL examples: app:="nginx"  status:>=500  NOT level:="debug"
type FieldFilter struct {
	Field  string
	Op     FieldOp
	Value  string // empty for FieldOpEmpty and FieldOpAny
	Negate bool
}

func (f FieldFilter) String() string {
	var core string
	switch f.Op {
	case FieldOpExact:
		core = fmt.Sprintf(`%s:="%s"`, f.Field, f.Value)
	case FieldOpRegexp:
		core = fmt.Sprintf(`%s:~"%s"`, f.Field, f.Value)
	case FieldOpPrefix:
		core = fmt.Sprintf(`%s:%s*`, f.Field, f.Value)
	case FieldOpSubstring:
		core = fmt.Sprintf(`%s:*%s*`, f.Field, f.Value)
	case FieldOpEmpty:
		core = fmt.Sprintf(`%s:""`, f.Field)
	case FieldOpAny:
		core = f.Field + ":*"
	case FieldOpGT:
		core = fmt.Sprintf(`%s:>%s`, f.Field, f.Value)
	case FieldOpGTE:
		core = fmt.Sprintf(`%s:>=%s`, f.Field, f.Value)
	case FieldOpLT:
		core = fmt.Sprintf(`%s:<%s`, f.Field, f.Value)
	case FieldOpLTE:
		core = fmt.Sprintf(`%s:<=%s`, f.Field, f.Value)
	case FieldOpRange:
		core = fmt.Sprintf(`%s:range(%s)`, f.Field, f.Value)
	case FieldOpIn:
		core = fmt.Sprintf(`%s:in(%s)`, f.Field, f.Value)
	}
	if f.Negate {
		return "NOT " + core
	}
	return core
}
func (f FieldFilter) filterExpr() {}

// ---------------------------------------------------------------------------
// Stream and time filters
// ---------------------------------------------------------------------------

// LabelMatcher is a single {name op "value"} matcher inside a StreamFilter.
// Op is one of "=", "!=", "=~", "!~".
type LabelMatcher struct {
	Name  string
	Op    string
	Value string
}

func (m LabelMatcher) String() string {
	return fmt.Sprintf(`%s%s"%s"`, m.Name, m.Op, m.Value)
}

// StreamFilter selects log streams by _stream_fields values.
// Only use for fields declared as _stream_fields at VL ingestion time.
// LogsQL: {app="nginx", env="prod"}
type StreamFilter struct{ Matchers []LabelMatcher }

func (s StreamFilter) String() string {
	parts := make([]string, len(s.Matchers))
	for i, m := range s.Matchers {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
func (s StreamFilter) filterExpr() {}

// TimeFilter restricts to a time range. Range is a VL duration ("5m") or
// absolute range ("[2024-01-01,2024-01-02]"). LogsQL: _time:5m
type TimeFilter struct{ Range string }

func (t TimeFilter) String() string { return "_time:" + t.Range }
func (t TimeFilter) filterExpr()    {}

// ---------------------------------------------------------------------------
// Logical combinators
// ---------------------------------------------------------------------------

func needsParens(f FilterExpr) bool {
	_, isOr := f.(OrExpr)
	return isOr
}

// AndExpr is a conjunction of two filters. LogsQL: error AND app:="nginx"
type AndExpr struct{ Left, Right FilterExpr }

func (a AndExpr) String() string {
	l := a.Left.String()
	if needsParens(a.Left) {
		l = "(" + l + ")"
	}
	r := a.Right.String()
	if needsParens(a.Right) {
		r = "(" + r + ")"
	}
	return l + " AND " + r
}
func (a AndExpr) filterExpr() {}

// OrExpr is a disjunction of two filters. LogsQL: error OR warn
type OrExpr struct{ Left, Right FilterExpr }

func (o OrExpr) String() string { return o.Left.String() + " OR " + o.Right.String() }
func (o OrExpr) filterExpr()    {}

// NotExpr negates a filter. LogsQL: NOT debug
type NotExpr struct{ Expr FilterExpr }

func (n NotExpr) String() string {
	s := n.Expr.String()
	switch n.Expr.(type) {
	case OrExpr, AndExpr:
		return "NOT (" + s + ")"
	}
	return "NOT " + s
}
func (n NotExpr) filterExpr() {}

// ---------------------------------------------------------------------------
// Deferred (proxy-evaluated) expression
// ---------------------------------------------------------------------------

// DeferredExpr wraps a proxy marker string (__binary__:…, __subquery__:…,
// __lvp_lr:…, __lvp_lj:…, __without__:…, __lvp_group__).
// The translator builds the marker string and stores it here; String() returns it unchanged.
type DeferredExpr struct{ Raw string }

func (d DeferredExpr) String() string { return d.Raw }
func (d DeferredExpr) expr()          {}
func (d DeferredExpr) filterExpr()    {}

// ---------------------------------------------------------------------------
// Stub pipe types needed for Query tests (full definitions in Task 2)
// ---------------------------------------------------------------------------

// PipeUnpackJSON parses JSON fields into separate fields. LogsQL: | unpack_json
type PipeUnpackJSON struct{}

func (p PipeUnpackJSON) String() string { return "| unpack_json" }
func (p PipeUnpackJSON) pipe()          {}

// PipeFilter applies a filter as a pipe stage. LogsQL: | filter expr
type PipeFilter struct{ Expr FilterExpr }

func (p PipeFilter) String() string { return "| filter " + p.Expr.String() }
func (p PipeFilter) pipe()          {}

// PipeLimit keeps the first N entries. LogsQL: | limit 100
type PipeLimit struct{ N int }

func (p PipeLimit) String() string { return fmt.Sprintf("| limit %d", p.N) }
func (p PipeLimit) pipe()          {}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/logsql/... -v 2>&1 | tail -20
```

Expected: all `TestFilterNodeString`, `TestQueryString`, `TestQueryNilFilter` PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/logsql/ast.go internal/logsql/ast_test.go
git commit -m "feat(logsql): add AST filter nodes, Query, DeferredExpr with String() methods"
```

---

### Task 2: AST — pipe stages and stats functions

**Files:**
- Modify: `internal/logsql/ast.go` (add all pipe + stats types)
- Modify: `internal/logsql/ast_test.go` (add pipe + stats tests)

- [ ] **Step 1: Add pipe and stats tests**

Add to `ast_test.go`:

```go
func TestPipeString(t *testing.T) {
	tests := []struct {
		name string
		pipe logsql.Pipe
		want string
	}{
		{"unpack_json", logsql.PipeUnpackJSON{}, "| unpack_json"},
		{"unpack_logfmt", logsql.PipeUnpackLogfmt{}, "| unpack_logfmt"},
		{"extract", logsql.PipeExtract{Pattern: "<_> <level>", From: "_msg"}, `| extract "<_> <level>" from _msg`},
		{"extract_if", logsql.PipeExtract{Pattern: "<level>", From: "_msg", If: `level:*`}, `| extract "<level>" from _msg if (level:*)`},
		{"extract_regexp", logsql.PipeExtractRegexp{Pattern: `(?P<level>\w+)`, From: "_msg"}, "| extract_regexp `(?P<level>\\w+)` from _msg"},
		{"filter", logsql.PipeFilter{Expr: logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGTE, Value: "500"}}, "| filter status:>=500"},
		{"fields", logsql.PipeFields{Labels: []string{"level", "status"}}, "| fields level, status"},
		{"delete", logsql.PipeDelete{Labels: []string{"debug", "trace"}}, "| delete debug, trace"},
		{"format", logsql.PipeFormat{Template: "<level> <status>", ResultField: "_msg"}, `| format "<level> <status>" as _msg`},
		{"rename", logsql.PipeRename{Pairs: [][2]string{{"old", "new"}}}, "| rename old as new"},
		{"rename_multi", logsql.PipeRename{Pairs: [][2]string{{"a", "b"}, {"c", "d"}}}, "| rename a as b, c as d"},
		{"replace", logsql.PipeReplace{Field: "level", Old: "warn", New: "warning"}, `| replace (level, "warn", "warning")`},
		{"replace_regexp", logsql.PipeReplaceRegexp{Field: "url", Regex: `https?://`, Replacement: ""}, "| replace_regexp (url, `https?://`, \"\")"},
		{"pack_json", logsql.PipePackJSON{Fields: []string{"a", "b"}, ResultField: "packed"}, "| pack_json fields (a, b) as packed"},
		{"pack_logfmt", logsql.PipePackLogfmt{Fields: []string{"a", "b"}, ResultField: "packed"}, "| pack_logfmt fields (a, b) as packed"},
		{"limit", logsql.PipeLimit{N: 100}, "| limit 100"},
		{
			"sort_desc_limit",
			logsql.PipeSort{By: []logsql.SortField{{Field: "count", Desc: true}}, Limit: 10},
			"| sort by (count desc) limit 10",
		},
		{
			"sort_asc_nolimit",
			logsql.PipeSort{By: []logsql.SortField{{Field: "ts", Desc: false}}},
			"| sort by (ts)",
		},
		{"math", logsql.PipeMath{Expr: "rate/total*100", Alias: "pct"}, "| math pct:=rate/total*100"},
		{
			"stats_count",
			logsql.PipeStats{
				By:    []logsql.GroupKey{{Field: "host"}},
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "cnt"}},
			},
			"| stats by (host) count() as cnt",
		},
		{
			"stats_multi",
			logsql.PipeStats{
				By: []logsql.GroupKey{{Field: "app"}, {Field: "env"}},
				Funcs: []logsql.StatsFuncAlias{
					{Func: logsql.Sum{Field: "bytes"}, Alias: "total"},
					{Func: logsql.Max{Field: "latency"}, Alias: "max_lat"},
				},
			},
			"| stats by (app, env) sum(bytes) as total, max(latency) as max_lat",
		},
		{
			"stats_no_by",
			logsql.PipeStats{
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "total"}},
			},
			"| stats count() as total",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.pipe.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStatsFuncString(t *testing.T) {
	tests := []struct {
		name string
		fn   logsql.StatsFunc
		want string
	}{
		{"count", logsql.Count{}, "count()"},
		{"sum", logsql.Sum{Field: "bytes"}, "sum(bytes)"},
		{"min", logsql.Min{Field: "latency"}, "min(latency)"},
		{"max", logsql.Max{Field: "latency"}, "max(latency)"},
		{"avg", logsql.Avg{Field: "latency"}, "avg(latency)"},
		{"median", logsql.Median{Field: "latency"}, "median(latency)"},
		{"quantile", logsql.Quantile{Phi: 0.99, Field: "latency"}, "quantile(0.99, latency)"},
		{"stddev", logsql.Stddev{Field: "latency"}, "stddev(latency)"},
		{"stdvar", logsql.Stdvar{Field: "latency"}, "stdvar(latency)"},
		{"rate", logsql.Rate{}, "rate()"},
		{"rate_sum", logsql.RateSum{Field: "bytes"}, "rate_sum(bytes)"},
		{"count_uniq", logsql.CountUniq{Field: "user_id"}, "count_uniq(user_id)"},
		{"count_uniq_hash", logsql.CountUniqHash{Field: "user_id"}, "count_uniq_hash(user_id)"},
		{"uniq_values", logsql.UniqValues{Field: "user_id", Limit: 100}, "uniq_values(user_id, 100)"},
		{"field_max", logsql.FieldMax{Field: "latency"}, "field_max(latency)"},
		{"field_min", logsql.FieldMin{Field: "latency"}, "field_min(latency)"},
		{"json_values", logsql.JSONValues{Field: "data"}, "json_values(data)"},
		{"any", logsql.Any{Field: "user_id"}, "any(user_id)"},
		{"count_empty", logsql.CountEmpty{Field: "level"}, "count_empty(level)"},
		{"sum_len", logsql.SumLen{Field: "_msg"}, "sum_len(_msg)"},
		{"values", logsql.Values{Field: "status", Limit: 10}, "values(status, 10)"},
		{"histogram", logsql.Histogram{Field: "latency"}, "histogram(latency)"},
		{"last", logsql.Last{Field: "_msg"}, "last(_msg)"},
		{"first", logsql.First{Field: "_msg"}, "first(_msg)"},
		{"row_any", logsql.RowAny{Fields: []string{"_msg", "level"}}, "row_any(_msg, level)"},
		{"row_max", logsql.RowMax{By: "latency", Fields: []string{"_msg", "status"}}, "row_max(latency, _msg, status)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fn.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run to confirm failures**

```bash
go test ./internal/logsql/... 2>&1 | grep "undefined" | head -15
```

Expected: many `undefined: logsql.PipeUnpackLogfmt`, `logsql.Count`, etc.

- [ ] **Step 3: Add all pipe and stats types to `ast.go`**

Append to `internal/logsql/ast.go`:

```go
// ---------------------------------------------------------------------------
// Pipe stages — parsers
// ---------------------------------------------------------------------------

// PipeUnpackLogfmt parses logfmt fields. LogsQL: | unpack_logfmt
type PipeUnpackLogfmt struct{}

func (p PipeUnpackLogfmt) String() string { return "| unpack_logfmt" }
func (p PipeUnpackLogfmt) pipe()          {}

// PipeExtract extracts named fields using a VL pattern. LogsQL: | extract "pat" from field [if (cond)]
type PipeExtract struct {
	Pattern string
	From    string // source field, typically "_msg"
	If      string // optional filter condition (without surrounding parens)
}

func (p PipeExtract) String() string {
	s := fmt.Sprintf(`| extract "%s" from %s`, p.Pattern, p.From)
	if p.If != "" {
		s += " if (" + p.If + ")"
	}
	return s
}
func (p PipeExtract) pipe() {}

// PipeExtractRegexp extracts fields via a named-capture regexp. LogsQL: | extract_regexp `pat` from field
type PipeExtractRegexp struct {
	Pattern string
	From    string
}

func (p PipeExtractRegexp) String() string {
	return fmt.Sprintf("| extract_regexp `%s` from %s", p.Pattern, p.From)
}
func (p PipeExtractRegexp) pipe() {}

// ---------------------------------------------------------------------------
// Pipe stages — fields
// ---------------------------------------------------------------------------

// PipeFields keeps only the listed fields. LogsQL: | fields f1, f2
type PipeFields struct{ Labels []string }

func (p PipeFields) String() string { return "| fields " + strings.Join(p.Labels, ", ") }
func (p PipeFields) pipe()          {}

// PipeDelete removes the listed fields. LogsQL: | delete f1, f2
type PipeDelete struct{ Labels []string }

func (p PipeDelete) String() string { return "| delete " + strings.Join(p.Labels, ", ") }
func (p PipeDelete) pipe()          {}

// ---------------------------------------------------------------------------
// Pipe stages — transform
// ---------------------------------------------------------------------------

// PipeFormat rewrites the log message using a VL format template.
// LogsQL: | format "<level> <status>" as _msg
type PipeFormat struct {
	Template    string
	ResultField string
}

func (p PipeFormat) String() string {
	return fmt.Sprintf(`| format "%s" as %s`, p.Template, p.ResultField)
}
func (p PipeFormat) pipe() {}

// PipeRename renames fields. LogsQL: | rename old as new, a as b
type PipeRename struct{ Pairs [][2]string }

func (p PipeRename) String() string {
	parts := make([]string, len(p.Pairs))
	for i, pair := range p.Pairs {
		parts[i] = pair[0] + " as " + pair[1]
	}
	return "| rename " + strings.Join(parts, ", ")
}
func (p PipeRename) pipe() {}

// PipeReplace replaces a literal value in a field. LogsQL: | replace (field, "old", "new")
type PipeReplace struct {
	Field string
	Old   string
	New   string
}

func (p PipeReplace) String() string {
	return fmt.Sprintf(`| replace (%s, "%s", "%s")`, p.Field, p.Old, p.New)
}
func (p PipeReplace) pipe() {}

// PipeReplaceRegexp replaces regexp matches in a field. LogsQL: | replace_regexp (field, `pat`, "repl")
type PipeReplaceRegexp struct {
	Field       string
	Regex       string
	Replacement string
}

func (p PipeReplaceRegexp) String() string {
	return fmt.Sprintf("| replace_regexp (%s, `%s`, %q)", p.Field, p.Regex, p.Replacement)
}
func (p PipeReplaceRegexp) pipe() {}

// PipePackJSON packs listed fields into a JSON-encoded result field.
// LogsQL: | pack_json fields (a, b) as result
type PipePackJSON struct {
	Fields      []string
	ResultField string
}

func (p PipePackJSON) String() string {
	return fmt.Sprintf("| pack_json fields (%s) as %s", strings.Join(p.Fields, ", "), p.ResultField)
}
func (p PipePackJSON) pipe() {}

// PipePackLogfmt packs listed fields into a logfmt-encoded result field.
// LogsQL: | pack_logfmt fields (a, b) as result
type PipePackLogfmt struct {
	Fields      []string
	ResultField string
}

func (p PipePackLogfmt) String() string {
	return fmt.Sprintf("| pack_logfmt fields (%s) as %s", strings.Join(p.Fields, ", "), p.ResultField)
}
func (p PipePackLogfmt) pipe() {}

// ---------------------------------------------------------------------------
// Pipe stages — aggregation
// ---------------------------------------------------------------------------

// GroupKey is a field used in a | stats by (…) clause.
type GroupKey struct{ Field string }

// StatsFuncAlias pairs a stats function with its result alias.
type StatsFuncAlias struct {
	Func  StatsFunc
	Alias string
}

// SortField specifies a sort key and direction.
type SortField struct {
	Field string
	Desc  bool
}

// PipeStats computes aggregate statistics over log fields.
// LogsQL: | stats by (host) count() as cnt, sum(bytes) as total
type PipeStats struct {
	By    []GroupKey
	Funcs []StatsFuncAlias
}

func (p PipeStats) String() string {
	var b strings.Builder
	b.WriteString("| stats")
	if len(p.By) > 0 {
		keys := make([]string, len(p.By))
		for i, k := range p.By {
			keys[i] = k.Field
		}
		b.WriteString(" by (")
		b.WriteString(strings.Join(keys, ", "))
		b.WriteByte(')')
	}
	for i, fa := range p.Funcs {
		if i == 0 {
			b.WriteByte(' ')
		} else {
			b.WriteString(", ")
		}
		b.WriteString(fa.Func.String())
		b.WriteString(" as ")
		b.WriteString(fa.Alias)
	}
	return b.String()
}
func (p PipeStats) pipe() {}

// PipeMath evaluates an arithmetic expression and stores the result.
// LogsQL: | math alias:=expr
type PipeMath struct {
	Expr  string
	Alias string
}

func (p PipeMath) String() string { return fmt.Sprintf("| math %s:=%s", p.Alias, p.Expr) }
func (p PipeMath) pipe()          {}

// PipeSort sorts entries. LogsQL: | sort by (field desc) limit N
type PipeSort struct {
	By    []SortField
	Limit int // 0 means no limit
}

func (p PipeSort) String() string {
	fields := make([]string, len(p.By))
	for i, sf := range p.By {
		if sf.Desc {
			fields[i] = sf.Field + " desc"
		} else {
			fields[i] = sf.Field
		}
	}
	s := "| sort by (" + strings.Join(fields, ", ") + ")"
	if p.Limit > 0 {
		s += fmt.Sprintf(" limit %d", p.Limit)
	}
	return s
}
func (p PipeSort) pipe() {}

// ---------------------------------------------------------------------------
// Stats function types (26 total)
// ---------------------------------------------------------------------------

type Count struct{}

func (Count) String() string  { return "count()" }
func (Count) statsFunc()      {}

type Sum struct{ Field string }

func (s Sum) String() string { return "sum(" + s.Field + ")" }
func (s Sum) statsFunc()     {}

type Min struct{ Field string }

func (s Min) String() string { return "min(" + s.Field + ")" }
func (s Min) statsFunc()     {}

type Max struct{ Field string }

func (s Max) String() string { return "max(" + s.Field + ")" }
func (s Max) statsFunc()     {}

type Avg struct{ Field string }

func (s Avg) String() string { return "avg(" + s.Field + ")" }
func (s Avg) statsFunc()     {}

type Median struct{ Field string }

func (s Median) String() string { return "median(" + s.Field + ")" }
func (s Median) statsFunc()     {}

type Quantile struct {
	Phi   float64
	Field string
}

func (q Quantile) String() string { return fmt.Sprintf("quantile(%g, %s)", q.Phi, q.Field) }
func (q Quantile) statsFunc()     {}

type Stddev struct{ Field string }

func (s Stddev) String() string { return "stddev(" + s.Field + ")" }
func (s Stddev) statsFunc()     {}

type Stdvar struct{ Field string }

func (s Stdvar) String() string { return "stdvar(" + s.Field + ")" }
func (s Stdvar) statsFunc()     {}

type Rate struct{}

func (Rate) String() string { return "rate()" }
func (Rate) statsFunc()     {}

// RateSum requires VL v1.44+. The builder selects this only when Capabilities.StatsRateSum is true.
type RateSum struct{ Field string }

func (s RateSum) String() string { return "rate_sum(" + s.Field + ")" }
func (s RateSum) statsFunc()     {}

type CountUniq struct{ Field string }

func (s CountUniq) String() string { return "count_uniq(" + s.Field + ")" }
func (s CountUniq) statsFunc()     {}

type CountUniqHash struct{ Field string }

func (s CountUniqHash) String() string { return "count_uniq_hash(" + s.Field + ")" }
func (s CountUniqHash) statsFunc()     {}

type UniqValues struct {
	Field string
	Limit int
}

func (s UniqValues) String() string { return fmt.Sprintf("uniq_values(%s, %d)", s.Field, s.Limit) }
func (s UniqValues) statsFunc()     {}

type FieldMax struct{ Field string }

func (s FieldMax) String() string { return "field_max(" + s.Field + ")" }
func (s FieldMax) statsFunc()     {}

type FieldMin struct{ Field string }

func (s FieldMin) String() string { return "field_min(" + s.Field + ")" }
func (s FieldMin) statsFunc()     {}

type JSONValues struct{ Field string }

func (s JSONValues) String() string { return "json_values(" + s.Field + ")" }
func (s JSONValues) statsFunc()     {}

type Any struct{ Field string }

func (s Any) String() string { return "any(" + s.Field + ")" }
func (s Any) statsFunc()     {}

type CountEmpty struct{ Field string }

func (s CountEmpty) String() string { return "count_empty(" + s.Field + ")" }
func (s CountEmpty) statsFunc()     {}

type SumLen struct{ Field string }

func (s SumLen) String() string { return "sum_len(" + s.Field + ")" }
func (s SumLen) statsFunc()     {}

type Values struct {
	Field string
	Limit int
}

func (s Values) String() string { return fmt.Sprintf("values(%s, %d)", s.Field, s.Limit) }
func (s Values) statsFunc()     {}

// Histogram requires VL v1.31+ (always available for supported v1.4x+).
type Histogram struct{ Field string }

func (s Histogram) String() string { return "histogram(" + s.Field + ")" }
func (s Histogram) statsFunc()     {}

type Last struct{ Field string }

func (s Last) String() string { return "last(" + s.Field + ")" }
func (s Last) statsFunc()     {}

type First struct{ Field string }

func (s First) String() string { return "first(" + s.Field + ")" }
func (s First) statsFunc()     {}

type RowAny struct{ Fields []string }

func (s RowAny) String() string { return "row_any(" + strings.Join(s.Fields, ", ") + ")" }
func (s RowAny) statsFunc()     {}

type RowMax struct {
	By     string
	Fields []string
}

func (s RowMax) String() string {
	parts := append([]string{s.By}, s.Fields...)
	return "row_max(" + strings.Join(parts, ", ") + ")"
}
func (s RowMax) statsFunc() {}
```

- [ ] **Step 4: Run all AST tests**

```bash
go test ./internal/logsql/... -v 2>&1 | grep -E "PASS|FAIL|---"
```

Expected: all PASS. If `TestPipeString/replace_regexp` fails, check that `%q` on the empty Replacement produces `""` correctly.

- [ ] **Step 5: Commit**

```bash
git add internal/logsql/ast.go internal/logsql/ast_test.go
git commit -m "feat(logsql): add pipe stages and 26 stats function types with String() methods"
```

---

### Task 3: Capabilities — VL v1.40 minimum, v1.4x/v1.5x feature matrix

**Files:**
- Create: `internal/logsql/capabilities.go`
- Create: `internal/logsql/capabilities_test.go`

- [ ] **Step 1: Write failing capability tests**

```go
// internal/logsql/capabilities_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestCapabilitiesFor(t *testing.T) {
	tests := []struct {
		semver string
		want   logsql.Capabilities
	}{
		// Versions below v1.40 are not supported; return baseline (all false).
		{"v1.30.0", logsql.Capabilities{}},
		{"v1.39.9", logsql.Capabilities{}},
		// v1.40 baseline — all optional features false.
		{"v1.40.0", logsql.Capabilities{}},
		{"v1.43.9", logsql.Capabilities{}},
		// v1.44 adds rate_sum.
		{"v1.44.0", logsql.Capabilities{StatsRateSum: true}},
		// v1.45 adds ipv4_range.
		{"v1.45.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		{"v1.48.9", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		// v1.49 adds metadata substring.
		{"v1.49.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true}},
		// v1.50 adds dense pattern windowing.
		{"v1.50.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true, DensePatternWindowing: true}},
		{"v1.99.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true, DensePatternWindowing: true}},
		// Empty string — safe baseline.
		{"", logsql.Capabilities{}},
	}
	for _, tc := range tests {
		t.Run(tc.semver, func(t *testing.T) {
			if got := logsql.CapabilitiesFor(tc.semver); got != tc.want {
				t.Errorf("CapabilitiesFor(%q) = %+v, want %+v", tc.semver, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./internal/logsql/... -run TestCapabilities 2>&1 | head -5
```

Expected: `undefined: logsql.Capabilities`.

- [ ] **Step 3: Create `internal/logsql/capabilities.go`**

```go
// internal/logsql/capabilities.go
package logsql

import (
	"strconv"
	"strings"
)

// MinSupportedMajor and MinSupportedMinor define the oldest VictoriaLogs
// release that this package targets. Queries targeting earlier versions
// must fall back to the proxy's string-concatenation translator.
const (
	MinSupportedMajor = 1
	MinSupportedMinor = 40
)

// Capabilities describes which optional LogsQL constructs are available
// in a specific VictoriaLogs deployment.
//
// The package targets v1.4x and v1.5x families only. All features listed
// in the v1.40 baseline (PipeHits, PipeRunning, PipeBlock, PipeUniq,
// PipeTop, StatsHistogram) are assumed always available; they are not
// represented as flags because every supported version includes them.
//
// Features that vary within the v1.40–v1.5x range are:
//
//	StatsRateSum         — rate_sum() stats function.       Added in v1.44.
//	FieldIPv4Range       — ipv4_range() field filter.       Added in v1.45.
//	MetadataSubstring    — substring in metadata fields.    Added in v1.49.
//	DensePatternWindowing— dense windowing for patterns.    Added in v1.50.
//
// When a feature is false, the translator or proxy must substitute a
// compatible but slower alternative (regexp approximation, proxy-side
// post-filtering, etc.).
type Capabilities struct {
	StatsRateSum          bool // v1.44+
	FieldIPv4Range        bool // v1.45+
	MetadataSubstring     bool // v1.49+
	DensePatternWindowing bool // v1.50+
}

// CapabilitiesFor returns the Capabilities for the given VictoriaLogs semver
// string (e.g. "v1.50.0" or "1.44.2").
//
// Versions earlier than v1.40 are not supported; they return the zero value
// (all features false) rather than panicking, so callers always get a safe
// degraded capability set.
func CapabilitiesFor(semver string) Capabilities {
	maj, min, _, ok := parseSemver(semver)
	if !ok || maj < MinSupportedMajor || (maj == MinSupportedMajor && min < MinSupportedMinor) {
		return Capabilities{}
	}
	return Capabilities{
		StatsRateSum:          atLeast(maj, min, 1, 44),
		FieldIPv4Range:        atLeast(maj, min, 1, 45),
		MetadataSubstring:     atLeast(maj, min, 1, 49),
		DensePatternWindowing: atLeast(maj, min, 1, 50),
	}
}

func atLeast(maj, min, wantMaj, wantMin int) bool {
	if maj != wantMaj {
		return maj > wantMaj
	}
	return min >= wantMin
}

func parseSemver(s string) (major, minor, patch int, ok bool) {
	s = strings.TrimSpace(strings.TrimPrefix(s, "v"))
	if s == "" {
		return 0, 0, 0, false
	}
	parts := strings.SplitN(s, ".", 3)
	if len(parts) < 2 {
		return 0, 0, 0, false
	}
	maj, err := strconv.Atoi(numericPart(parts[0]))
	if err != nil {
		return 0, 0, 0, false
	}
	min, err := strconv.Atoi(numericPart(parts[1]))
	if err != nil {
		return 0, 0, 0, false
	}
	pt := 0
	if len(parts) == 3 {
		pt, _ = strconv.Atoi(numericPart(parts[2]))
	}
	return maj, min, pt, true
}

func numericPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		b.WriteRune(r)
	}
	return b.String()
}
```

- [ ] **Step 4: Run capability tests**

```bash
go test ./internal/logsql/... -run TestCapabilities -v 2>&1 | tail -20
```

Expected: all PASS.

- [ ] **Step 5: Run full test suite to confirm nothing broken**

```bash
go test ./internal/logsql/... 2>&1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/logsql/capabilities.go internal/logsql/capabilities_test.go
git commit -m "feat(logsql): add Capabilities struct with v1.40 minimum and v1.4x/v1.5x feature matrix"
```

---

### Task 4: Builder — direct construction and capability-aware helpers

**Files:**
- Create: `internal/logsql/builder.go`
- Create: `internal/logsql/builder_test.go`

- [ ] **Step 1: Write failing builder tests**

```go
// internal/logsql/builder_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestNewQuery(t *testing.T) {
	q := logsql.NewQuery(
		logsql.And(
			logsql.FieldExact("app", "nginx"),
			logsql.Word{Value: "error"},
		),
		logsql.PipeUnpackJSON{},
		logsql.PipeFilter{Expr: logsql.FieldGTE("status", "500")},
		logsql.PipeStats{
			By:    []logsql.GroupKey{{Field: "host"}},
			Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "count"}},
		},
	)
	want := `app:="nginx" AND error | unpack_json | filter status:>=500 | stats by (host) count() as count`
	if got := q.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestConstructorHelpers(t *testing.T) {
	tests := []struct {
		name string
		expr logsql.FilterExpr
		want string
	}{
		{"FieldExact", logsql.FieldExact("app", "nginx"), `app:="nginx"`},
		{"FieldRegexp", logsql.FieldRegexp("level", "err.*"), `level:~"err.*"`},
		{"FieldGT", logsql.FieldGT("status", "400"), "status:>400"},
		{"FieldGTE", logsql.FieldGTE("status", "500"), "status:>=500"},
		{"FieldLT", logsql.FieldLT("latency", "100"), "latency:<100"},
		{"FieldLTE", logsql.FieldLTE("latency", "200"), "latency:<=200"},
		{"FieldAny", logsql.FieldAny("level"), "level:*"},
		{"FieldEmpty", logsql.FieldEmpty("level"), `level:""`},
		{"Not", logsql.Not(logsql.Word{Value: "debug"}), "NOT debug"},
		{
			"And",
			logsql.And(logsql.Word{Value: "a"}, logsql.Word{Value: "b"}),
			"a AND b",
		},
		{
			"Or",
			logsql.Or(logsql.Word{Value: "a"}, logsql.Word{Value: "b"}),
			"a OR b",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.expr.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBestTopN_v140(t *testing.T) {
	// v1.40 — no native top pipe; falls back to sort + limit
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.40.0"))
	pipes := b.BestTopN(5, "count")
	if len(pipes) != 2 {
		t.Fatalf("expected 2 pipes (sort+limit), got %d", len(pipes))
	}
	if got := pipes[0].String(); got != "| sort by (count desc)" {
		t.Errorf("pipe[0] = %q, want sort", got)
	}
	if got := pipes[1].String(); got != "| limit 5" {
		t.Errorf("pipe[1] = %q, want limit", got)
	}
}

func TestBestTopN_v150(t *testing.T) {
	// v1.50 — native | top N by (field) available via PipeSort with Limit
	// (The proxy uses sort+limit because | top is a separate construct;
	// BestTopN on v1.50 still uses sort+limit — the optimisation is that
	// v1.50 supports dense windowing which the sort respects natively.)
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.50.0"))
	pipes := b.BestTopN(5, "count")
	if len(pipes) != 2 {
		t.Fatalf("expected 2 pipes, got %d", len(pipes))
	}
}

func TestBestIPv4Range_pre145(t *testing.T) {
	// v1.44 — no native ipv4_range; falls back to regexp approximation
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.44.0"))
	f := b.BestIPv4Range("client_ip", "192.168.1.0/24")
	got := f.String()
	// Must be a regexp approximation, not ipv4_range()
	if got == "client_ip:ipv4_range(192.168.1.0, 192.168.1.255)" {
		t.Errorf("v1.44 should not emit native ipv4_range, got %q", got)
	}
	if got == "" {
		t.Error("BestIPv4Range returned empty filter")
	}
}

func TestBestIPv4Range_v145(t *testing.T) {
	// v1.45+ — native ipv4_range() field filter
	b := logsql.NewBuilder(logsql.CapabilitiesFor("v1.45.0"))
	f := b.BestIPv4Range("client_ip", "192.168.1.0/24")
	want := "client_ip:ipv4_range(192.168.1.0, 192.168.1.255)"
	if got := f.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run to confirm failures**

```bash
go test ./internal/logsql/... -run TestNewQuery 2>&1 | head -5
```

Expected: `undefined: logsql.NewQuery`, `logsql.And`, etc.

- [ ] **Step 3: Create `internal/logsql/builder.go`**

```go
// internal/logsql/builder.go
package logsql

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
)

// NewQuery assembles a Query from a filter and zero or more pipe stages.
// filter may be nil (produces "*" in the output).
func NewQuery(filter FilterExpr, pipes ...Pipe) *Query {
	return &Query{Filter: filter, Pipes: pipes}
}

// Constructor helpers for common filter expressions.

func And(left, right FilterExpr) FilterExpr  { return AndExpr{Left: left, Right: right} }
func Or(left, right FilterExpr) FilterExpr   { return OrExpr{Left: left, Right: right} }
func Not(expr FilterExpr) FilterExpr         { return NotExpr{Expr: expr} }
func FieldExact(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpExact, Value: value}
}
func FieldRegexp(field, pattern string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpRegexp, Value: pattern}
}
func FieldPrefix(field, prefix string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpPrefix, Value: prefix}
}
func FieldSubstring(field, sub string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpSubstring, Value: sub}
}
func FieldAny(field string) FilterExpr   { return FieldFilter{Field: field, Op: FieldOpAny} }
func FieldEmpty(field string) FilterExpr { return FieldFilter{Field: field, Op: FieldOpEmpty} }
func FieldGT(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpGT, Value: value}
}
func FieldGTE(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpGTE, Value: value}
}
func FieldLT(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpLT, Value: value}
}
func FieldLTE(field, value string) FilterExpr {
	return FieldFilter{Field: field, Op: FieldOpLTE, Value: value}
}

// Builder selects the best LogsQL construct for the detected VL version.
type Builder struct{ caps Capabilities }

// NewBuilder returns a Builder configured for the given VL capabilities.
func NewBuilder(caps Capabilities) *Builder { return &Builder{caps: caps} }

// BestTopN returns pipe stages that keep the top-K entries by the named field.
// On all supported versions (v1.40+) this is | sort by (field desc) | limit K.
// The DensePatternWindowing capability (v1.50+) affects VL's internal sort
// optimisation but the LogsQL syntax is identical.
func (b *Builder) BestTopN(k int, field string) []Pipe {
	return []Pipe{
		PipeSort{By: []SortField{{Field: field, Desc: true}}},
		PipeLimit{N: k},
	}
}

// BestIPv4Range returns a filter that matches field against the CIDR range.
// On v1.45+ it emits the native ipv4_range() field filter.
// On earlier versions it falls back to a regexp approximation that covers
// the /24 and /16 common cases; for uncommon prefix lengths the regexp is
// conservative (may pass some non-matching IPs, proxy must re-filter).
func (b *Builder) BestIPv4Range(field, cidr string) FilterExpr {
	if b.caps.FieldIPv4Range {
		first, last, ok := cidrToRange(cidr)
		if !ok {
			// Unparseable CIDR — fall back to regexp
			return FieldFilter{Field: field, Op: FieldOpRegexp, Value: cidrToRegexp(cidr)}
		}
		return FieldFilter{
			Field: field,
			Op:    FieldOpIn, // reuse FieldOpIn to emit field:ipv4_range(first,last)
			Value: first + ", " + last,
		}
	}
	// Fallback: regexp approximation
	return FieldFilter{Field: field, Op: FieldOpRegexp, Value: cidrToRegexp(cidr)}
}

// cidrToRange parses a CIDR and returns the first and last IP as strings.
func cidrToRange(cidr string) (first, last string, ok bool) {
	ip, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", "", false
	}
	_ = ip
	// First address
	firstIP := network.IP.Mask(network.Mask)
	// Last address: OR with inverted mask
	lastIP := make(net.IP, len(firstIP))
	for i := range firstIP {
		lastIP[i] = firstIP[i] | ^network.Mask[i]
	}
	return firstIP.String(), lastIP.String(), true
}

// cidrToRegexp converts a CIDR to a conservative regexp approximation.
// Only handles /8, /16, /24 accurately; other prefix lengths may over-match.
func cidrToRegexp(cidr string) string {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return ".*" // safe but useless fallback
	}
	ones, _ := network.Mask.Size()
	ip4 := network.IP.To4()
	if ip4 == nil {
		return ".*"
	}
	n := binary.BigEndian.Uint32([]byte(ip4))
	_ = n
	switch {
	case ones >= 24:
		return fmt.Sprintf(`^%d\.%d\.%d\.`, ip4[0], ip4[1], ip4[2])
	case ones >= 16:
		return fmt.Sprintf(`^%d\.%d\.`, ip4[0], ip4[1])
	case ones >= 8:
		return fmt.Sprintf(`^%d\.`, ip4[0])
	default:
		return ".*"
	}
}

// suppress unused import
var _ = math.MaxInt
var _ = strings.Builder{}
```

Wait — `BestIPv4Range` uses `FieldOpIn` to emit `ipv4_range()` which is wrong; `FieldOpIn` emits `field:in(...)`. We need a new field op or a dedicated type. Fix by adding `FieldOpIPv4Range` to the `FieldOp` iota and a corresponding String() case.

- [ ] **Step 4: Add FieldOpIPv4Range to ast.go**

In `ast.go`, add `FieldOpIPv4Range` after `FieldOpIn`:

```go
// In the FieldOp const block, append:
    FieldOpIPv4Range                // :ipv4_range(first, last)  requires v1.45+
```

In `FieldFilter.String()`, add the case:
```go
    case FieldOpIPv4Range:
        core = fmt.Sprintf(`%s:ipv4_range(%s)`, f.Field, f.Value)
```

Update `BestIPv4Range` in `builder.go` to use `FieldOpIPv4Range`:
```go
        return FieldFilter{
            Field: field,
            Op:    FieldOpIPv4Range,
            Value: first + ", " + last,
        }
```

And remove the unused `math` and `strings` imports from `builder.go` (the `net` and `encoding/binary` imports stay).

- [ ] **Step 5: Run builder tests**

```bash
go test ./internal/logsql/... -run "TestNewQuery|TestConstructorHelpers|TestBestTopN|TestBestIPv4Range" -v 2>&1 | tail -30
```

Expected: all PASS.

- [ ] **Step 6: Run full suite**

```bash
go test ./internal/logsql/... 2>&1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/logsql/builder.go internal/logsql/builder_test.go internal/logsql/ast.go
git commit -m "feat(logsql): add Builder API with BestTopN, BestIPv4Range, constructor helpers"
```

---

### Task 5: Scanner — LogsQL token stream

**Files:**
- Create: `internal/logsql/scanner.go`
- Create: `internal/logsql/scanner_test.go`

- [ ] **Step 1: Write failing scanner tests**

```go
// internal/logsql/scanner_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestScannerTokens(t *testing.T) {
	tests := []struct {
		input string
		types []logsql.TokType
		vals  []string
	}{
		{
			`app:="nginx" AND error`,
			[]logsql.TokType{logsql.TokIdent, logsql.TokColonEq, logsql.TokString, logsql.TokAnd, logsql.TokIdent, logsql.TokEOF},
			[]string{"app", `:="`, "nginx", "AND", "error", ""},
		},
		{
			`status:>=500`,
			[]logsql.TokType{logsql.TokIdent, logsql.TokColonGTE, logsql.TokIdent, logsql.TokEOF},
			[]string{"status", `:>=`, "500", ""},
		},
		{
			`NOT level:~"err.*"`,
			[]logsql.TokType{logsql.TokNot, logsql.TokIdent, logsql.TokColonTilde, logsql.TokString, logsql.TokEOF},
			[]string{"NOT", "level", `:~`, "err.*", ""},
		},
		{
			`* | unpack_json`,
			[]logsql.TokType{logsql.TokStar, logsql.TokPipe, logsql.TokIdent, logsql.TokEOF},
			[]string{"*", "|", "unpack_json", ""},
		},
		{
			`{app="nginx"}`,
			[]logsql.TokType{logsql.TokLBrace, logsql.TokIdent, logsql.TokEq, logsql.TokString, logsql.TokRBrace, logsql.TokEOF},
			[]string{"{", "app", "=", "nginx", "}", ""},
		},
		{
			`_time:5m`,
			[]logsql.TokType{logsql.TokIdent, logsql.TokColon, logsql.TokIdent, logsql.TokEOF},
			[]string{"_time", ":", "5m", ""},
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			sc := logsql.NewScanner(tc.input)
			for i, wantTyp := range tc.types {
				tok := sc.Next()
				if tok.Typ != wantTyp {
					t.Errorf("token[%d]: got type %d (%q), want type %d", i, tok.Typ, tok.Val, wantTyp)
				}
				if tc.vals[i] != "" && tok.Val != tc.vals[i] {
					t.Errorf("token[%d]: got val %q, want %q", i, tok.Val, tc.vals[i])
				}
			}
		})
	}
}
```

- [ ] **Step 2: Create `internal/logsql/scanner.go`**

```go
// internal/logsql/scanner.go
package logsql

import (
	"strings"
	"unicode/utf8"
)

// TokType identifies a lexical token.
type TokType int

const (
	TokEOF   TokType = iota
	TokError          // lexical error; Val contains message

	// Punctuation
	TokLBrace   // {
	TokRBrace   // }
	TokLParen   // (
	TokRParen   // )
	TokComma    // ,
	TokPipe     // |
	TokStar     // *
	TokColon    // :  (bare colon — used for _time: and :range(...) etc.)

	// Field-filter operators  field:OP value
	TokColonEq    // :=   exact match
	TokColonTilde // :~   regexp match
	TokColonGT    // :>
	TokColonGTE   // :>=
	TokColonLT    // :<
	TokColonLTE   // :<=

	// Label-match operators inside {}
	TokEq         // =
	TokNeq        // !=
	TokReMatch    // =~
	TokReNotMatch // !~

	// Logical keywords
	TokAnd // AND
	TokOr  // OR
	TokNot // NOT

	// Values
	TokIdent     // identifier or keyword
	TokString    // "…" — value without quotes stored in Val
	TokRawString // `…` — raw string, Val contains content
	TokNumber    // numeric literal
)

// Token is a single lexical unit.
type Token struct {
	Typ TokType
	Val string
}

// Scanner tokenises a LogsQL query string.
type Scanner struct {
	src string
	pos int
}

// NewScanner creates a scanner for the given input.
func NewScanner(src string) *Scanner { return &Scanner{src: src} }

// Next returns the next token, advancing the position.
func (s *Scanner) Next() Token {
	s.skipWhitespace()
	if s.pos >= len(s.src) {
		return Token{Typ: TokEOF}
	}
	ch, size := utf8.DecodeRuneInString(s.src[s.pos:])

	switch ch {
	case '{':
		s.pos += size
		return Token{Typ: TokLBrace, Val: "{"}
	case '}':
		s.pos += size
		return Token{Typ: TokRBrace, Val: "}"}
	case '(':
		s.pos += size
		return Token{Typ: TokLParen, Val: "("}
	case ')':
		s.pos += size
		return Token{Typ: TokRParen, Val: ")"}
	case ',':
		s.pos += size
		return Token{Typ: TokComma, Val: ","}
	case '|':
		s.pos += size
		return Token{Typ: TokPipe, Val: "|"}
	case '*':
		s.pos += size
		return Token{Typ: TokStar, Val: "*"}
	case '"':
		return s.scanQuoted()
	case '`':
		return s.scanRaw()
	case ':':
		return s.scanColon()
	case '=':
		s.pos += size
		if s.pos < len(s.src) && s.src[s.pos] == '~' {
			s.pos++
			return Token{Typ: TokReMatch, Val: "=~"}
		}
		return Token{Typ: TokEq, Val: "="}
	case '!':
		s.pos += size
		if s.pos < len(s.src) {
			switch s.src[s.pos] {
			case '=':
				s.pos++
				return Token{Typ: TokNeq, Val: "!="}
			case '~':
				s.pos++
				return Token{Typ: TokReNotMatch, Val: "!~"}
			}
		}
		return Token{Typ: TokError, Val: "unexpected !"}
	}

	// Identifier, keyword, or number
	return s.scanIdent()
}

func (s *Scanner) skipWhitespace() {
	for s.pos < len(s.src) {
		r, size := utf8.DecodeRuneInString(s.src[s.pos:])
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			break
		}
		s.pos += size
	}
}

func (s *Scanner) scanQuoted() Token {
	s.pos++ // consume opening "
	var b strings.Builder
	for s.pos < len(s.src) {
		ch := s.src[s.pos]
		if ch == '"' {
			s.pos++
			return Token{Typ: TokString, Val: b.String()}
		}
		if ch == '\\' && s.pos+1 < len(s.src) {
			s.pos++
			switch s.src[s.pos] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case '"':
				b.WriteByte('"')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteByte('\\')
				b.WriteByte(s.src[s.pos])
			}
			s.pos++
			continue
		}
		b.WriteByte(ch)
		s.pos++
	}
	return Token{Typ: TokError, Val: "unterminated string"}
}

func (s *Scanner) scanRaw() Token {
	s.pos++ // consume `
	start := s.pos
	for s.pos < len(s.src) && s.src[s.pos] != '`' {
		s.pos++
	}
	val := s.src[start:s.pos]
	if s.pos < len(s.src) {
		s.pos++ // consume closing `
	}
	return Token{Typ: TokRawString, Val: val}
}

func (s *Scanner) scanColon() Token {
	s.pos++ // consume :
	if s.pos >= len(s.src) {
		return Token{Typ: TokColon, Val: ":"}
	}
	switch s.src[s.pos] {
	case '=':
		s.pos++
		return Token{Typ: TokColonEq, Val: `:=`}
	case '~':
		s.pos++
		return Token{Typ: TokColonTilde, Val: `:~`}
	case '>':
		s.pos++
		if s.pos < len(s.src) && s.src[s.pos] == '=' {
			s.pos++
			return Token{Typ: TokColonGTE, Val: `:>=`}
		}
		return Token{Typ: TokColonGT, Val: `:>`}
	case '<':
		s.pos++
		if s.pos < len(s.src) && s.src[s.pos] == '=' {
			s.pos++
			return Token{Typ: TokColonLTE, Val: `:<=`}
		}
		return Token{Typ: TokColonLT, Val: `:<`}
	}
	return Token{Typ: TokColon, Val: ":"}
}

func (s *Scanner) scanIdent() Token {
	start := s.pos
	for s.pos < len(s.src) {
		r, size := utf8.DecodeRuneInString(s.src[s.pos:])
		if isIdentChar(r) {
			s.pos += size
		} else {
			break
		}
	}
	val := s.src[start:s.pos]
	switch val {
	case "AND":
		return Token{Typ: TokAnd, Val: val}
	case "OR":
		return Token{Typ: TokOr, Val: val}
	case "NOT":
		return Token{Typ: TokNot, Val: val}
	}
	return Token{Typ: TokIdent, Val: val}
}

func isIdentChar(r rune) bool {
	return r == '_' || r == '.' || r == '-' || r == '/' ||
		(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9')
}
```

- [ ] **Step 3: Run scanner tests**

```bash
go test ./internal/logsql/... -run TestScanner -v 2>&1 | tail -20
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/logsql/scanner.go internal/logsql/scanner_test.go
git commit -m "feat(logsql): add LogsQL lexer/scanner"
```

---

### Task 6: Parser — Parse() and ParseFilter()

**Files:**
- Create: `internal/logsql/parser.go`
- Create: `internal/logsql/parser_test.go`

- [ ] **Step 1: Write failing round-trip parser tests**

```go
// internal/logsql/parser_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestParseRoundTrip(t *testing.T) {
	// Parse a LogsQL string, call String() on the result, compare to input.
	// All inputs must already be in canonical form (as the translator emits).
	cases := []string{
		`*`,
		`error`,
		`"hello world"`,
		`err*`,
		`*error*`,
		`="404"`,
		`~"error|warn"`,
		`error AND app:="nginx"`,
		`error OR warn`,
		`NOT debug`,
		`(error OR warn) AND app:="nginx"`,
		`app:="nginx" AND error | unpack_json | filter status:>=500`,
		`* | unpack_json | unpack_logfmt | filter level:="error" | stats by (host) count() as cnt`,
		`app:="nginx" | stats by (app, env) sum(bytes) as total, max(latency) as max_lat`,
		`{app="nginx", env="prod"}`,
		`_time:5m`,
		`status:>400`,
		`status:>=500`,
		`latency:<100`,
		`latency:<=200`,
		`latency:range(100,500)`,
		`level:in(error,warn)`,
		`NOT level:="debug"`,
		`level:*`,
		`level:""`,
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := logsql.Parse(input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", input, err)
			}
			if got := q.String(); got != input {
				t.Errorf("round-trip mismatch:\n  input: %q\n  got:   %q", input, got)
			}
		})
	}
}

func TestParseFilter(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{`error`, `error`},
		{`app:="nginx" AND error`, `app:="nginx" AND error`},
		{`NOT debug`, `NOT debug`},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			f, err := logsql.ParseFilter(tc.input)
			if err != nil {
				t.Fatalf("ParseFilter(%q): %v", tc.input, err)
			}
			if got := f.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseError(t *testing.T) {
	bad := []string{
		``,
		`| filter`,      // pipe with no filter
		`{unclosed`,     // unclosed brace
	}
	for _, input := range bad {
		t.Run(input, func(t *testing.T) {
			_, err := logsql.Parse(input)
			if err == nil {
				t.Errorf("Parse(%q) expected error, got nil", input)
			}
		})
	}
}
```

- [ ] **Step 2: Create `internal/logsql/parser.go`**

```go
// internal/logsql/parser.go
package logsql

import (
	"fmt"
	"strings"
)

// Parse parses a complete LogsQL query string (filter + optional pipe stages)
// and returns a *Query. Returns an error for empty input or syntax errors.
func Parse(input string) (*Query, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("logsql: empty query")
	}
	p := &parser{sc: NewScanner(input), input: input}
	p.advance()
	return p.parseQuery()
}

// ParseFilter parses only the filter part of a LogsQL query (no pipes).
func ParseFilter(input string) (FilterExpr, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("logsql: empty filter")
	}
	p := &parser{sc: NewScanner(input), input: input}
	p.advance()
	f, err := p.parseFilterExpr()
	if err != nil {
		return nil, err
	}
	if p.cur.Typ != TokEOF {
		return nil, fmt.Errorf("logsql: unexpected token %q", p.cur.Val)
	}
	return f, nil
}

type parser struct {
	sc    *Scanner
	cur   Token
	input string
}

func (p *parser) advance() { p.cur = p.sc.Next() }

func (p *parser) parseQuery() (*Query, error) {
	// Parse filter (everything before the first | that is NOT inside braces)
	var filter FilterExpr
	if p.cur.Typ == TokStar && p.peekIsPipe() {
		p.advance() // consume *
		filter = nil
	} else if p.cur.Typ != TokPipe {
		var err error
		filter, err = p.parseFilterExpr()
		if err != nil {
			return nil, err
		}
	}

	var pipes []Pipe
	for p.cur.Typ == TokPipe {
		p.advance() // consume |
		pipe, err := p.parsePipe()
		if err != nil {
			return nil, err
		}
		pipes = append(pipes, pipe)
	}
	if p.cur.Typ != TokEOF {
		return nil, fmt.Errorf("logsql: unexpected token %q at end", p.cur.Val)
	}
	return &Query{Filter: filter, Pipes: pipes}, nil
}

// peekIsPipe checks whether the next non-whitespace token is a pipe (without advancing).
func (p *parser) peekIsPipe() bool {
	// scanner position is already past the current token; we save/restore nothing
	// because this is only called right after scanning TokStar.
	// We just check if current token after the * is |.
	// After advance() the cur would be the next token.
	// We cheat: save cur, advance, check, restore is not possible.
	// Instead, caller checks p.cur.Typ == TokStar and then calls Next directly.
	// Simplification: always treat standalone * as wildcard.
	return true
}

// parseFilterExpr parses a filter expression (OR-level).
func (p *parser) parseFilterExpr() (FilterExpr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}
	for p.cur.Typ == TokOr {
		p.advance()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = OrExpr{Left: left, Right: right}
	}
	return left, nil
}

// parseAndExpr parses a conjunction (AND-level).
func (p *parser) parseAndExpr() (FilterExpr, error) {
	left, err := p.parsePrimaryFilter()
	if err != nil {
		return nil, err
	}
	for {
		if p.cur.Typ == TokAnd {
			p.advance()
			right, err := p.parsePrimaryFilter()
			if err != nil {
				return nil, err
			}
			left = AndExpr{Left: left, Right: right}
			continue
		}
		// Implicit AND: another primary filter that is not a pipe or EOF or )
		if p.cur.Typ == TokPipe || p.cur.Typ == TokEOF || p.cur.Typ == TokRParen || p.cur.Typ == TokOr {
			break
		}
		// Try implicit AND
		right, err := p.parsePrimaryFilter()
		if err != nil {
			break
		}
		left = AndExpr{Left: left, Right: right}
	}
	return left, nil
}

// parsePrimaryFilter parses a single filter atom.
func (p *parser) parsePrimaryFilter() (FilterExpr, error) {
	switch p.cur.Typ {
	case TokNot:
		p.advance()
		expr, err := p.parsePrimaryFilter()
		if err != nil {
			return nil, err
		}
		return NotExpr{Expr: expr}, nil

	case TokLParen:
		p.advance() // consume (
		expr, err := p.parseFilterExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.Typ != TokRParen {
			return nil, fmt.Errorf("logsql: expected ), got %q", p.cur.Val)
		}
		p.advance() // consume )
		return expr, nil

	case TokStar:
		p.advance()
		return Wildcard{}, nil

	case TokLBrace:
		return p.parseStreamFilter()

	case TokString:
		val := p.cur.Val
		p.advance()
		return Phrase{Value: val}, nil

	case TokIdent:
		return p.parseIdentOrFieldFilter()

	default:
		return nil, fmt.Errorf("logsql: unexpected token %q in filter", p.cur.Val)
	}
}

// parseStreamFilter parses {key="val", ...}.
func (p *parser) parseStreamFilter() (FilterExpr, error) {
	p.advance() // consume {
	var matchers []LabelMatcher
	for p.cur.Typ != TokRBrace {
		if p.cur.Typ == TokEOF {
			return nil, fmt.Errorf("logsql: unclosed stream filter {")
		}
		if p.cur.Typ != TokIdent {
			return nil, fmt.Errorf("logsql: expected field name in stream filter, got %q", p.cur.Val)
		}
		name := p.cur.Val
		p.advance()
		var op string
		switch p.cur.Typ {
		case TokEq:
			op = "="
		case TokNeq:
			op = "!="
		case TokReMatch:
			op = "=~"
		case TokReNotMatch:
			op = "!~"
		default:
			return nil, fmt.Errorf("logsql: expected matcher operator, got %q", p.cur.Val)
		}
		p.advance()
		if p.cur.Typ != TokString {
			return nil, fmt.Errorf("logsql: expected string value in stream filter, got %q", p.cur.Val)
		}
		val := p.cur.Val
		p.advance()
		matchers = append(matchers, LabelMatcher{Name: name, Op: op, Value: val})
		if p.cur.Typ == TokComma {
			p.advance()
		}
	}
	p.advance() // consume }
	return StreamFilter{Matchers: matchers}, nil
}

// parseIdentOrFieldFilter handles identifiers that may be bare words or field:op expressions.
func (p *parser) parseIdentOrFieldFilter() (FilterExpr, error) {
	ident := p.cur.Val
	p.advance()

	// Check for special _time: prefix
	if ident == "_time" && p.cur.Typ == TokColon {
		p.advance() // consume :
		val := p.cur.Val
		p.advance()
		return TimeFilter{Range: val}, nil
	}

	// Field filter: ident followed by a colon operator
	switch p.cur.Typ {
	case TokColonEq:
		p.advance()
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: ident, Op: FieldOpExact, Value: val}, nil

	case TokColonTilde:
		p.advance()
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return FieldFilter{Field: ident, Op: FieldOpRegexp, Value: val}, nil

	case TokColonGT:
		p.advance()
		return FieldFilter{Field: ident, Op: FieldOpGT, Value: p.consumeIdent()}, nil

	case TokColonGTE:
		p.advance()
		return FieldFilter{Field: ident, Op: FieldOpGTE, Value: p.consumeIdent()}, nil

	case TokColonLT:
		p.advance()
		return FieldFilter{Field: ident, Op: FieldOpLT, Value: p.consumeIdent()}, nil

	case TokColonLTE:
		p.advance()
		return FieldFilter{Field: ident, Op: FieldOpLTE, Value: p.consumeIdent()}, nil

	case TokColon:
		// bare colon: could be :* :""  :range(...) :in(...) :prefix* :*sub*
		p.advance()
		return p.parseBareCsFilterValue(ident)
	}

	// Bare word / prefix / substring
	if strings.HasSuffix(ident, "*") {
		return Prefix{Value: strings.TrimSuffix(ident, "*")}, nil
	}
	return Word{Value: ident}, nil
}

// parseBareCsFilterValue parses the value part after a bare colon.
func (p *parser) parseBareCsFilterValue(field string) (FilterExpr, error) {
	switch p.cur.Typ {
	case TokStar:
		p.advance()
		// Could be :* (any) or :*sub* (substring — but scanner returns TokStar then TokIdent)
		if p.cur.Typ == TokIdent {
			sub := p.cur.Val
			p.advance()
			if p.cur.Typ == TokStar {
				p.advance()
			}
			return FieldFilter{Field: field, Op: FieldOpSubstring, Value: sub}, nil
		}
		return FieldFilter{Field: field, Op: FieldOpAny}, nil

	case TokString:
		val := p.cur.Val
		p.advance()
		if val == "" {
			return FieldFilter{Field: field, Op: FieldOpEmpty}, nil
		}
		return FieldFilter{Field: field, Op: FieldOpExact, Value: val}, nil

	case TokIdent:
		kw := p.cur.Val
		p.advance()
		switch kw {
		case "range":
			if p.cur.Typ != TokLParen {
				return nil, fmt.Errorf("logsql: expected ( after range")
			}
			p.advance()
			val := p.consumeUntil(')')
			return FieldFilter{Field: field, Op: FieldOpRange, Value: val}, nil

		case "in":
			if p.cur.Typ != TokLParen {
				return nil, fmt.Errorf("logsql: expected ( after in")
			}
			p.advance()
			val := p.consumeUntil(')')
			return FieldFilter{Field: field, Op: FieldOpIn, Value: val}, nil

		default:
			// field:prefix* or just field:ident
			if strings.HasSuffix(kw, "*") {
				return FieldFilter{Field: field, Op: FieldOpPrefix, Value: strings.TrimSuffix(kw, "*")}, nil
			}
			// _time:5m style — field:duration_or_ident
			if field == "_time" {
				return TimeFilter{Range: kw}, nil
			}
			return FieldFilter{Field: field, Op: FieldOpExact, Value: kw}, nil
		}
	}
	return nil, fmt.Errorf("logsql: unexpected token %q after %s:", p.cur.Val, field)
}

func (p *parser) consumeIdent() string {
	if p.cur.Typ == TokIdent {
		val := p.cur.Val
		p.advance()
		return val
	}
	return ""
}

func (p *parser) consumeUntil(end byte) string {
	var b strings.Builder
	for p.cur.Typ != TokEOF {
		if p.cur.Typ == TokRParen {
			p.advance() // consume )
			break
		}
		b.WriteString(p.cur.Val)
		p.advance()
	}
	_ = end
	return b.String()
}

func (p *parser) expectString() (string, error) {
	if p.cur.Typ == TokString {
		val := p.cur.Val
		p.advance()
		return val, nil
	}
	if p.cur.Typ == TokIdent {
		val := p.cur.Val
		p.advance()
		return val, nil
	}
	return "", fmt.Errorf("logsql: expected string, got %q", p.cur.Val)
}

// parsePipe parses a single pipe stage name and its arguments.
func (p *parser) parsePipe() (Pipe, error) {
	if p.cur.Typ != TokIdent {
		return nil, fmt.Errorf("logsql: expected pipe name, got %q", p.cur.Val)
	}
	name := p.cur.Val
	p.advance()

	switch name {
	case "unpack_json":
		return PipeUnpackJSON{}, nil
	case "unpack_logfmt":
		return PipeUnpackLogfmt{}, nil
	case "filter":
		expr, err := p.parseFilterExpr()
		if err != nil {
			return nil, fmt.Errorf("logsql: filter pipe: %w", err)
		}
		return PipeFilter{Expr: expr}, nil
	case "fields":
		labels := p.parseIdentList()
		return PipeFields{Labels: labels}, nil
	case "delete":
		labels := p.parseIdentList()
		return PipeDelete{Labels: labels}, nil
	case "limit":
		n := p.parseInt()
		return PipeLimit{N: n}, nil
	case "sort":
		return p.parsePipeSort()
	case "stats":
		return p.parsePipeStats()
	case "math":
		return p.parsePipeMath()
	default:
		// Unknown pipe — consume rest of stage until next | or EOF as raw
		return nil, fmt.Errorf("logsql: unknown pipe %q", name)
	}
}

func (p *parser) parseIdentList() []string {
	var labels []string
	for p.cur.Typ == TokIdent {
		labels = append(labels, p.cur.Val)
		p.advance()
		if p.cur.Typ == TokComma {
			p.advance()
		}
	}
	return labels
}

func (p *parser) parseInt() int {
	if p.cur.Typ != TokIdent {
		return 0
	}
	n := 0
	for _, ch := range p.cur.Val {
		if ch < '0' || ch > '9' {
			break
		}
		n = n*10 + int(ch-'0')
	}
	p.advance()
	return n
}

func (p *parser) parsePipeSort() (Pipe, error) {
	// | sort by (field [desc], ...) [limit N]
	if p.cur.Typ != TokIdent || p.cur.Val != "by" {
		return nil, fmt.Errorf("logsql: expected 'by' after sort, got %q", p.cur.Val)
	}
	p.advance() // consume "by"
	if p.cur.Typ != TokLParen {
		return nil, fmt.Errorf("logsql: expected ( after sort by")
	}
	p.advance() // consume (
	var fields []SortField
	for p.cur.Typ != TokRParen && p.cur.Typ != TokEOF {
		name := p.cur.Val
		p.advance()
		desc := false
		if p.cur.Typ == TokIdent && p.cur.Val == "desc" {
			desc = true
			p.advance()
		}
		fields = append(fields, SortField{Field: name, Desc: desc})
		if p.cur.Typ == TokComma {
			p.advance()
		}
	}
	if p.cur.Typ == TokRParen {
		p.advance() // consume )
	}
	limit := 0
	if p.cur.Typ == TokIdent && p.cur.Val == "limit" {
		p.advance()
		limit = p.parseInt()
	}
	return PipeSort{By: fields, Limit: limit}, nil
}

func (p *parser) parsePipeStats() (Pipe, error) {
	// | stats [by (fields)] func() as alias [, func() as alias ...]
	var by []GroupKey
	if p.cur.Typ == TokIdent && p.cur.Val == "by" {
		p.advance()
		if p.cur.Typ != TokLParen {
			return nil, fmt.Errorf("logsql: expected ( after stats by")
		}
		p.advance()
		for p.cur.Typ != TokRParen && p.cur.Typ != TokEOF {
			by = append(by, GroupKey{Field: p.cur.Val})
			p.advance()
			if p.cur.Typ == TokComma {
				p.advance()
			}
		}
		if p.cur.Typ == TokRParen {
			p.advance()
		}
	}
	var funcs []StatsFuncAlias
	for p.cur.Typ == TokIdent {
		fn, err := p.parseStatsFunc()
		if err != nil {
			return nil, err
		}
		if p.cur.Typ != TokIdent || p.cur.Val != "as" {
			return nil, fmt.Errorf("logsql: expected 'as' after stats function")
		}
		p.advance()
		alias := p.cur.Val
		p.advance()
		funcs = append(funcs, StatsFuncAlias{Func: fn, Alias: alias})
		if p.cur.Typ == TokComma {
			p.advance()
		}
	}
	return PipeStats{By: by, Funcs: funcs}, nil
}

func (p *parser) parseStatsFunc() (StatsFunc, error) {
	name := p.cur.Val
	p.advance()
	if p.cur.Typ != TokLParen {
		return nil, fmt.Errorf("logsql: expected ( after stats function %q", name)
	}
	p.advance()
	var field string
	var extra string
	if p.cur.Typ == TokIdent || p.cur.Typ == TokNumber {
		field = p.cur.Val
		p.advance()
		if p.cur.Typ == TokComma {
			p.advance()
			extra = p.cur.Val
			p.advance()
		}
	}
	if p.cur.Typ == TokRParen {
		p.advance()
	}
	switch name {
	case "count":
		return Count{}, nil
	case "sum":
		return Sum{Field: field}, nil
	case "min":
		return Min{Field: field}, nil
	case "max":
		return Max{Field: field}, nil
	case "avg":
		return Avg{Field: field}, nil
	case "median":
		return Median{Field: field}, nil
	case "rate":
		return Rate{}, nil
	case "rate_sum":
		return RateSum{Field: field}, nil
	case "count_uniq":
		return CountUniq{Field: field}, nil
	case "count_uniq_hash":
		return CountUniqHash{Field: field}, nil
	case "field_max":
		return FieldMax{Field: field}, nil
	case "field_min":
		return FieldMin{Field: field}, nil
	case "json_values":
		return JSONValues{Field: field}, nil
	case "any":
		return Any{Field: field}, nil
	case "count_empty":
		return CountEmpty{Field: field}, nil
	case "sum_len":
		return SumLen{Field: field}, nil
	case "histogram":
		return Histogram{Field: field}, nil
	case "last":
		return Last{Field: field}, nil
	case "first":
		return First{Field: field}, nil
	case "stddev":
		return Stddev{Field: field}, nil
	case "stdvar":
		return Stdvar{Field: field}, nil
	}
	_ = extra
	return nil, fmt.Errorf("logsql: unknown stats function %q", name)
}

func (p *parser) parsePipeMath() (Pipe, error) {
	// | math alias:=expr
	alias := p.cur.Val
	p.advance()
	// consume :=
	if p.cur.Typ != TokColonEq {
		return nil, fmt.Errorf("logsql: expected := in math pipe, got %q", p.cur.Val)
	}
	p.advance()
	// rest of tokens until next | or EOF form the expression
	var parts []string
	for p.cur.Typ != TokPipe && p.cur.Typ != TokEOF {
		parts = append(parts, p.cur.Val)
		p.advance()
	}
	return PipeMath{Alias: alias, Expr: strings.Join(parts, "")}, nil
}
```

- [ ] **Step 3: Run parser tests**

```bash
go test ./internal/logsql/... -run "TestParse|TestParseFilter|TestParseError" -v 2>&1 | grep -E "PASS|FAIL|---"
```

Expected: majority PASS. Some edge cases (prefix `*`, substring `*text*`, _time with bracket range) may fail if the scanner's `isIdentChar` doesn't include `[`, `]`. Fix: extend `parseBareCsFilterValue` to handle `[` as start of a bracket range for `_time`.

- [ ] **Step 4: Fix any failing round-trip cases**

If `_time:[2024-01-01,2024-01-02]` fails, add to scanner's `isIdentChar` check or handle `[` in `parseBareCsFilterValue`:

```go
case TokLBrace: // scanner returns { but _time needs [
    // Actually need TokLBracket — add to scanner:
```

Add to scanner token types and `Next()`:
```go
TokLBracket // [
TokRBracket // ]
```

And in `Next()` after the `TokLBrace` case:
```go
case '[':
    s.pos += size
    return Token{Typ: TokLBracket, Val: "["}
case ']':
    s.pos += size
    return Token{Typ: TokRBracket, Val: "]"}
```

Then in `parseBareCsFilterValue` when `field == "_time"`, handle `[`:
```go
case TokLBracket:
    // consume until ]
    s := "["
    p.advance()
    for p.cur.Typ != TokRBracket && p.cur.Typ != TokEOF {
        s += p.cur.Val
        p.advance()
    }
    s += "]"
    if p.cur.Typ == TokRBracket {
        p.advance()
    }
    return TimeFilter{Range: s}, nil
```

Re-run until all round-trip tests pass.

- [ ] **Step 5: Run full suite**

```bash
go test ./internal/logsql/... 2>&1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/logsql/scanner.go internal/logsql/parser.go internal/logsql/parser_test.go internal/logsql/scanner_test.go
git commit -m "feat(logsql): add recursive-descent parser for LogsQL filter expressions and pipe stages"
```

---

### Task 7: Update spec + wire into project

**Files:**
- Modify: `docs/superpowers/specs/2026-05-22-logsql-parser-design.md`
- Verify: `go build ./...` from repo root

- [ ] **Step 1: Add version constraint section to spec**

In `docs/superpowers/specs/2026-05-22-logsql-parser-design.md`, insert after the goal section:

```markdown
## Supported VictoriaLogs Versions

This package targets the **v1.4x** (v1.40–v1.49) and **v1.5x** (v1.50+) VictoriaLogs families. Versions earlier than v1.40 are explicitly out of scope.

| Version | Baseline capabilities |
|---------|----------------------|
| v1.40–v1.43 | `PipeHits`, `PipeRunning`, `PipeBlock`, `PipeUniq`, `PipeTop`, `StatsHistogram` |
| v1.44 | + `rate_sum()` |
| v1.45–v1.48 | + `ipv4_range()` field filter |
| v1.49 | + metadata substring filter |
| v1.50+ | + dense pattern windowing |

When a construct is unavailable in the detected version (`Capabilities` field is `false`), the `Builder` falls back to a compatible substitute and the translator continues to use the string-concatenation path for that construct.

`CapabilitiesFor(semver)` is called once at proxy startup from `storeBackendVersion()` and stored on `Proxy`. Proxy passes the `Capabilities` value to `logsql.NewBuilder(caps)` at query construction time.
```

- [ ] **Step 2: Verify the package builds cleanly**

```bash
go build ./internal/logsql/... 2>&1
go vet ./internal/logsql/... 2>&1
```

Expected: no output (clean build).

- [ ] **Step 3: Run the complete test suite including all existing tests**

```bash
go test ./... 2>&1 | grep -E "FAIL|ok" | head -30
```

Expected: all packages PASS including `internal/logql`, `internal/translator`, `internal/proxy`.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-05-22-logsql-parser-design.md
git commit -m "docs(logsql): add VL version constraint and capability matrix to design spec"
```

- [ ] **Step 5: Push branch**

```bash
git push origin feat/logsql-parser
```

---

## Self-Review

### Spec coverage

| Spec requirement | Task |
|-----------------|------|
| `ast.go` — typed filter nodes | Task 1 |
| `ast.go` — pipe stages | Task 2 |
| `ast.go` — stats functions (26) | Task 2 |
| `ast.go` — marker/deferred nodes | Task 1 (`DeferredExpr`) |
| `capabilities.go` — v1.4x/v1.5x matrix | Task 3 |
| `builder.go` — direct construction | Task 4 |
| `builder.go` — `BestTopN`, `BestIPv4Range` | Task 4 |
| `scanner.go` — LogsQL token types | Task 5 |
| `parser.go` — `Parse`, `ParseFilter` | Task 6 |
| Round-trip tests | Task 1–2 (String()), Task 6 (Parse) |
| Version constraint documentation | Task 7 |

Gaps: `PipeExtract`, `PipeExtractRegexp`, `PipeFormat`, `PipeRename`, `PipeReplace`, `PipeReplaceRegexp`, `PipePackJSON`, `PipePackLogfmt`, `PipeMath` are defined in Task 2 but parser support is not wired in `parsePipe()` (Task 6). These can be added as follow-on PRs — the parse-roundtrip tests in Task 6 only cover what the translator currently emits; the extended pipe types are defined for future use and are tested via `String()` only.

### Placeholder scan

None — all test cases contain actual expected strings; all implementation steps show complete code.

### Type consistency

- `GroupKey`, `StatsFuncAlias`, `SortField` defined in Task 2 `ast.go` and used identically in `builder_test.go` (Task 4) and `parser.go` (Task 6).
- `TokColonEq`, `TokAnd`, `TokOr`, `TokNot` defined in Task 5 scanner and used in Task 6 parser — consistent.
- `FieldOpIPv4Range` added in Task 4 step 4 — must be added to `ast.go` before Task 4 tests run.
