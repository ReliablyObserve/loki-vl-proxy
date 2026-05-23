package logsql

import (
	"fmt"
	"strings"
)

// Expr is the top-level LogsQL expression interface.
type Expr interface {
	String() string
	expr()
}

// FilterExpr is a filter expression node in a LogsQL query.
type FilterExpr interface {
	String() string
	filterExpr()
}

// Pipe is a single pipe stage. String() returns the full pipe token including
// the leading "| " separator (e.g. "| unpack_json", "| filter status:>=500").
// Query.String() inserts a space before calling p.String(), so the result is
// "filter | unpack_json" not "filter| unpack_json".
type Pipe interface {
	String() string
	pipe()
}

// StatsFunc is a function used inside PipeStats (e.g. count(), sum(field)).
// Concrete implementations are defined alongside PipeStats in the pipe types section.
type StatsFunc interface {
	String() string
	statsFunc()
}

// Query is a complete LogsQL query: an optional filter followed by zero or more pipes.
type Query struct {
	Filter FilterExpr
	Pipes  []Pipe
}

func (q Query) String() string {
	var b strings.Builder
	if q.Filter == nil {
		b.WriteString("*")
	} else {
		b.WriteString(q.Filter.String())
	}
	for _, p := range q.Pipes {
		b.WriteByte(' ')
		b.WriteString(p.String())
	}
	return b.String()
}

func (q Query) expr() {}

// --- Simple filter nodes ---

// Word matches a single word anywhere in the log line.
type Word struct{ Value string }

func (w Word) String() string { return w.Value }
func (w Word) filterExpr()    {}

// Phrase matches an exact phrase (including spaces).
type Phrase struct{ Value string }

func (p Phrase) String() string { return `"` + p.Value + `"` }
func (p Phrase) filterExpr()    {}

// Prefix matches log lines containing a word with the given prefix.
type Prefix struct{ Value string }

func (p Prefix) String() string { return p.Value + "*" }
func (p Prefix) filterExpr()    {}

// Substring matches log lines containing the given substring.
type Substring struct{ Value string }

func (s Substring) String() string { return "*" + s.Value + "*" }
func (s Substring) filterExpr()    {}

// Exact matches an exact value using the `=` operator.
type Exact struct{ Value string }

func (e Exact) String() string { return `="` + e.Value + `"` }
func (e Exact) filterExpr()    {}

// Regexp matches using a regular expression.
type Regexp struct{ Pattern string }

func (r Regexp) String() string { return `~"` + r.Pattern + `"` }
func (r Regexp) filterExpr()    {}

// Sequence matches a sequence of words in order.
type Sequence struct{ Parts []string }

func (s Sequence) String() string {
	quoted := make([]string, len(s.Parts))
	for i, p := range s.Parts {
		quoted[i] = `"` + p + `"`
	}
	return "seq(" + strings.Join(quoted, ",") + ")"
}
func (s Sequence) filterExpr() {}

// CaseInsensitive wraps a value for case-insensitive matching.
type CaseInsensitive struct{ Value string }

func (c CaseInsensitive) String() string { return `i("` + c.Value + `")` }
func (c CaseInsensitive) filterExpr()    {}

// Wildcard matches any log line.
type Wildcard struct{}

func (w Wildcard) String() string { return "*" }
func (w Wildcard) filterExpr()    {}

// --- FieldFilter ---

// FieldOp is the operator used in a field filter.
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

// FieldFilter matches a named field using the given operator and value.
type FieldFilter struct {
	Field  string
	Op     FieldOp
	Value  string
	Negate bool
}

func (f FieldFilter) String() string {
	var core string
	switch f.Op {
	case FieldOpExact:
		core = f.Field + `:="` + f.Value + `"`
	case FieldOpRegexp:
		core = f.Field + `:~"` + f.Value + `"`
	case FieldOpPrefix:
		core = f.Field + ":" + f.Value + "*"
	case FieldOpSubstring:
		core = f.Field + ":*" + f.Value + "*"
	case FieldOpEmpty:
		core = f.Field + `:""`
	case FieldOpAny:
		core = f.Field + ":*"
	case FieldOpGT:
		core = f.Field + ":>" + f.Value
	case FieldOpGTE:
		core = f.Field + ":>=" + f.Value
	case FieldOpLT:
		core = f.Field + ":<" + f.Value
	case FieldOpLTE:
		core = f.Field + ":<=" + f.Value
	case FieldOpRange:
		core = f.Field + ":range(" + f.Value + ")"
	case FieldOpIn:
		core = f.Field + ":in(" + f.Value + ")"
	default:
		panic(fmt.Sprintf("logsql: unknown FieldOp %d", f.Op))
	}
	if f.Negate {
		return "NOT " + core
	}
	return core
}

func (f FieldFilter) filterExpr() {}

// --- Stream and Time filters ---

// LabelMatcher is a single name op "value" matcher inside a StreamFilter.
type LabelMatcher struct {
	Name  string
	Op    string
	Value string
}

func (m LabelMatcher) String() string {
	return m.Name + m.Op + `"` + m.Value + `"`
}

// StreamFilter matches log streams by label selectors.
type StreamFilter struct {
	Matchers []LabelMatcher
}

func (s StreamFilter) String() string {
	parts := make([]string, len(s.Matchers))
	for i, m := range s.Matchers {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (s StreamFilter) filterExpr() {}

// TimeFilter constrains the query to a time range.
type TimeFilter struct {
	Range string // e.g. "5m" or "[2024-01-01,2024-01-02]"
}

func (t TimeFilter) String() string { return "_time:" + t.Range }
func (t TimeFilter) filterExpr()    {}

// --- Logic combinators ---

// AndExpr is a logical AND of two filter expressions.
type AndExpr struct {
	Left, Right FilterExpr
}

func (a AndExpr) String() string {
	left := a.Left.String()
	if _, ok := a.Left.(OrExpr); ok {
		left = "(" + left + ")"
	}
	right := a.Right.String()
	if _, ok := a.Right.(OrExpr); ok {
		right = "(" + right + ")"
	}
	return left + " AND " + right
}

func (a AndExpr) filterExpr() {}

// OrExpr is a logical OR of two filter expressions.
type OrExpr struct {
	Left, Right FilterExpr
}

func (o OrExpr) String() string {
	return o.Left.String() + " OR " + o.Right.String()
}

func (o OrExpr) filterExpr() {}

// NotExpr negates a filter expression.
type NotExpr struct {
	Expr FilterExpr
}

func (n NotExpr) String() string {
	s := n.Expr.String()
	switch n.Expr.(type) {
	case OrExpr, AndExpr:
		return "NOT (" + s + ")"
	}
	return "NOT " + s
}

func (n NotExpr) filterExpr() {}

// --- DeferredExpr ---

// DeferredExpr holds a raw expression string for deferred/opaque nodes.
// It implements both Expr and FilterExpr.
type DeferredExpr struct {
	Raw string
}

func (d DeferredExpr) String() string { return d.Raw }
func (d DeferredExpr) expr()          {}
func (d DeferredExpr) filterExpr()    {}

// --- Stub pipe types (full definitions in Task 2) ---

// PipeUnpackJSON unpacks JSON fields from log lines.
type PipeUnpackJSON struct{}

func (p PipeUnpackJSON) String() string { return "| unpack_json" }
func (p PipeUnpackJSON) pipe()          {}

// PipeFilter applies an additional filter expression in the pipeline.
type PipeFilter struct {
	Expr FilterExpr
}

func (p PipeFilter) String() string { return "| filter " + p.Expr.String() }
func (p PipeFilter) pipe()          {}

// PipeLimit limits the number of log entries returned.
type PipeLimit struct {
	N int
}

func (p PipeLimit) String() string { return fmt.Sprintf("| limit %d", p.N) }
func (p PipeLimit) pipe()          {}

// --- Helper types for PipeStats and PipeSort ---

// GroupKey is a single field used in a "by (f1, f2)" grouping clause.
type GroupKey struct{ Field string }

// StatsFuncAlias pairs a StatsFunc with its output alias.
type StatsFuncAlias struct {
	Func  StatsFunc
	Alias string
}

// SortField is a single field in a sort expression, with optional descending flag.
type SortField struct {
	Field string
	Desc  bool
}

// --- Parser pipe stages ---

// PipeUnpackLogfmt unpacks logfmt key=value pairs from log lines.
type PipeUnpackLogfmt struct{}

func (p PipeUnpackLogfmt) String() string { return "| unpack_logfmt" }
func (p PipeUnpackLogfmt) pipe()          {}

// PipeExtract extracts fields from a log line using a pattern.
type PipeExtract struct {
	Pattern string
	From    string // source field, e.g. "_msg"
	If      string // optional condition WITHOUT surrounding parens
}

func (p PipeExtract) String() string {
	s := fmt.Sprintf(`| extract %q from %s`, p.Pattern, p.From)
	if p.If != "" {
		s += " if (" + p.If + ")"
	}
	return s
}
func (p PipeExtract) pipe() {}

// PipeExtractRegexp extracts fields using a named-capture regular expression.
type PipeExtractRegexp struct {
	Pattern string
	From    string
}

func (p PipeExtractRegexp) String() string {
	return fmt.Sprintf("| extract_regexp `%s` from %s", p.Pattern, p.From)
}
func (p PipeExtractRegexp) pipe() {}

// --- Fields pipe stages ---

// PipeFields keeps only the listed fields.
type PipeFields struct{ Labels []string }

func (p PipeFields) String() string { return "| fields " + strings.Join(p.Labels, ", ") }
func (p PipeFields) pipe()          {}

// PipeDelete removes the listed fields.
type PipeDelete struct{ Labels []string }

func (p PipeDelete) String() string { return "| delete " + strings.Join(p.Labels, ", ") }
func (p PipeDelete) pipe()          {}

// --- Transform pipe stages ---

// PipeFormat formats log fields into a new field using a template.
type PipeFormat struct {
	Template    string
	ResultField string
}

func (p PipeFormat) String() string {
	return fmt.Sprintf(`| format %q as %s`, p.Template, p.ResultField)
}
func (p PipeFormat) pipe() {}

// PipeRename renames fields using pairs of [old, new] names.
type PipeRename struct{ Pairs [][2]string }

func (p PipeRename) String() string {
	parts := make([]string, len(p.Pairs))
	for i, pair := range p.Pairs {
		parts[i] = pair[0] + " as " + pair[1]
	}
	return "| rename " + strings.Join(parts, ", ")
}
func (p PipeRename) pipe() {}

// PipeReplace replaces occurrences of a substring in a field.
type PipeReplace struct {
	Field string
	Old   string
	New   string
}

func (p PipeReplace) String() string {
	return fmt.Sprintf(`| replace (%s, %q, %q)`, p.Field, p.Old, p.New)
}
func (p PipeReplace) pipe() {}

// PipeReplaceRegexp replaces regex matches in a field with a replacement string.
type PipeReplaceRegexp struct {
	Field       string
	Regex       string
	Replacement string
}

func (p PipeReplaceRegexp) String() string {
	return fmt.Sprintf("| replace_regexp (%s, `%s`, %q)", p.Field, p.Regex, p.Replacement)
}
func (p PipeReplaceRegexp) pipe() {}

// PipePackJSON packs the listed fields into a JSON value stored in ResultField.
type PipePackJSON struct {
	Fields      []string
	ResultField string
}

func (p PipePackJSON) String() string {
	return fmt.Sprintf("| pack_json fields (%s) as %s", strings.Join(p.Fields, ", "), p.ResultField)
}
func (p PipePackJSON) pipe() {}

// PipePackLogfmt packs the listed fields into a logfmt value stored in ResultField.
type PipePackLogfmt struct {
	Fields      []string
	ResultField string
}

func (p PipePackLogfmt) String() string {
	return fmt.Sprintf("| pack_logfmt fields (%s) as %s", strings.Join(p.Fields, ", "), p.ResultField)
}
func (p PipePackLogfmt) pipe() {}

// --- Aggregation pipe stages ---

// PipeStats groups log entries and computes statistics.
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
		b.WriteString(")")
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

// PipeMath evaluates a math expression and stores the result in Alias.
type PipeMath struct {
	Expr  string
	Alias string
}

func (p PipeMath) String() string { return fmt.Sprintf("| math %s:=%s", p.Alias, p.Expr) }
func (p PipeMath) pipe()          {}

// PipeSort sorts log entries by one or more fields.
type PipeSort struct {
	By    []SortField
	Limit int // 0 = no limit
}

func (p PipeSort) String() string {
	parts := make([]string, len(p.By))
	for i, sf := range p.By {
		if sf.Desc {
			parts[i] = sf.Field + " desc"
		} else {
			parts[i] = sf.Field
		}
	}
	s := "| sort by (" + strings.Join(parts, ", ") + ")"
	if p.Limit > 0 {
		s += fmt.Sprintf(" limit %d", p.Limit)
	}
	return s
}
func (p PipeSort) pipe() {}

// --- Stats functions (26 total) ---

// Count counts the number of log entries.
type Count struct{}

func (Count) String() string { return "count()" }
func (Count) statsFunc()     {}

// Sum computes the sum of a numeric field.
type Sum struct{ Field string }

func (s Sum) String() string { return "sum(" + s.Field + ")" }
func (s Sum) statsFunc()     {}

// Min computes the minimum value of a field.
type Min struct{ Field string }

func (m Min) String() string { return "min(" + m.Field + ")" }
func (m Min) statsFunc()     {}

// Max computes the maximum value of a field.
type Max struct{ Field string }

func (m Max) String() string { return "max(" + m.Field + ")" }
func (m Max) statsFunc()     {}

// Avg computes the average value of a field.
type Avg struct{ Field string }

func (a Avg) String() string { return "avg(" + a.Field + ")" }
func (a Avg) statsFunc()     {}

// Median computes the median value of a field.
type Median struct{ Field string }

func (m Median) String() string { return "median(" + m.Field + ")" }
func (m Median) statsFunc()     {}

// Quantile computes the given quantile (phi) of a field.
type Quantile struct {
	Phi   float64
	Field string
}

func (q Quantile) String() string { return fmt.Sprintf("quantile(%g, %s)", q.Phi, q.Field) }
func (q Quantile) statsFunc()     {}

// Stddev computes the standard deviation of a field.
type Stddev struct{ Field string }

func (s Stddev) String() string { return "stddev(" + s.Field + ")" }
func (s Stddev) statsFunc()     {}

// Stdvar computes the variance of a field.
type Stdvar struct{ Field string }

func (s Stdvar) String() string { return "stdvar(" + s.Field + ")" }
func (s Stdvar) statsFunc()     {}

// Rate computes the ingestion rate.
type Rate struct{}

func (Rate) String() string { return "rate()" }
func (Rate) statsFunc()     {}

// RateSum computes the per-second sum rate of a numeric field (requires VL v1.44+).
type RateSum struct{ Field string }

func (r RateSum) String() string { return "rate_sum(" + r.Field + ")" }
func (r RateSum) statsFunc()     {}

// CountUniq counts the number of unique values of a field.
type CountUniq struct{ Field string }

func (c CountUniq) String() string { return "count_uniq(" + c.Field + ")" }
func (c CountUniq) statsFunc()     {}

// CountUniqHash counts unique values using a hash (approximate, memory-efficient).
type CountUniqHash struct{ Field string }

func (c CountUniqHash) String() string { return "count_uniq_hash(" + c.Field + ")" }
func (c CountUniqHash) statsFunc()     {}

// UniqValues collects unique values of a field up to Limit entries.
type UniqValues struct {
	Field string
	Limit int
}

func (u UniqValues) String() string { return fmt.Sprintf("uniq_values(%s, %d)", u.Field, u.Limit) }
func (u UniqValues) statsFunc()     {}

// FieldMax returns the log entry with the maximum value of a field.
type FieldMax struct{ Field string }

func (f FieldMax) String() string { return "field_max(" + f.Field + ")" }
func (f FieldMax) statsFunc()     {}

// FieldMin returns the log entry with the minimum value of a field.
type FieldMin struct{ Field string }

func (f FieldMin) String() string { return "field_min(" + f.Field + ")" }
func (f FieldMin) statsFunc()     {}

// JSONValues collects field values as a JSON array.
type JSONValues struct{ Field string }

func (j JSONValues) String() string { return "json_values(" + j.Field + ")" }
func (j JSONValues) statsFunc()     {}

// Any returns an arbitrary value of a field.
type Any struct{ Field string }

func (a Any) String() string { return "any(" + a.Field + ")" }
func (a Any) statsFunc()     {}

// CountEmpty counts log entries where the field is empty.
type CountEmpty struct{ Field string }

func (c CountEmpty) String() string { return "count_empty(" + c.Field + ")" }
func (c CountEmpty) statsFunc()     {}

// SumLen computes the total byte length of a field across all log entries.
type SumLen struct{ Field string }

func (s SumLen) String() string { return "sum_len(" + s.Field + ")" }
func (s SumLen) statsFunc()     {}

// Values collects field values up to Limit entries.
type Values struct {
	Field string
	Limit int
}

func (v Values) String() string { return fmt.Sprintf("values(%s, %d)", v.Field, v.Limit) }
func (v Values) statsFunc()     {}

// Histogram computes a histogram of a field (requires VL v1.31+).
type Histogram struct{ Field string }

func (h Histogram) String() string { return "histogram(" + h.Field + ")" }
func (h Histogram) statsFunc()     {}

// Last returns the last value of a field in the time range.
type Last struct{ Field string }

func (l Last) String() string { return "last(" + l.Field + ")" }
func (l Last) statsFunc()     {}

// First returns the first value of a field in the time range.
type First struct{ Field string }

func (f First) String() string { return "first(" + f.Field + ")" }
func (f First) statsFunc()     {}

// RowAny returns an arbitrary log entry with all requested fields.
type RowAny struct{ Fields []string }

func (r RowAny) String() string { return "row_any(" + strings.Join(r.Fields, ", ") + ")" }
func (r RowAny) statsFunc()     {}

// RowMax returns the log entry with the maximum value of By, including the listed fields.
type RowMax struct {
	By     string
	Fields []string
}

func (r RowMax) String() string {
	all := append([]string{r.By}, r.Fields...)
	return "row_max(" + strings.Join(all, ", ") + ")"
}
func (r RowMax) statsFunc() {}
