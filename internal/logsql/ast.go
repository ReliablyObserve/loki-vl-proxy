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
