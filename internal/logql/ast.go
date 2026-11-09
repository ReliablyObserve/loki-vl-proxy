package logql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Expr is the top-level LogQL expression interface.
type Expr interface {
	String() string
	expr()
}

// Stage is a log pipeline stage.
type Stage interface {
	String() string
	stage()
}

// MatchOp is a label matcher operator.
type MatchOp int

const (
	MatchEq    MatchOp = iota // =
	MatchNeq                  // !=
	MatchRe                   // =~
	MatchNotRe                // !~
)

// LabelMatcher is a single {name op "value"} matcher.
type LabelMatcher struct {
	Name  string
	Op    MatchOp
	Value string
}

func (m LabelMatcher) String() string {
	return m.Name + m.Op.String() + strconv.Quote(m.Value)
}

// String is the operator as LogQL writes it.
func (o MatchOp) String() string {
	switch o {
	case MatchEq:
		return "="
	case MatchNeq:
		return "!="
	case MatchRe:
		return "=~"
	case MatchNotRe:
		return "!~"
	}
	return ""
}

// StreamSelector is the {label="value",...} part of a log query.
type StreamSelector struct {
	Matchers []LabelMatcher
}

func (s *StreamSelector) String() string {
	parts := make([]string, len(s.Matchers))
	for i, m := range s.Matchers {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (s *StreamSelector) expr()  {}
func (s *StreamSelector) stage() {}

// LineFilterOp is the operator used in a line filter stage.
type LineFilterOp int

const (
	LineFilterContains    LineFilterOp = iota // |=
	LineFilterExcludes                        // !=
	LineFilterMatchRe                         // |~
	LineFilterExcludeRe                       // !~
	LineFilterContainsPat                     // |>
	LineFilterExcludePat                      // !>
)

// LineFilterAlt is one `or` alternative of a line filter: `or "b"` or `or ip("1.2.3.4")`.
type LineFilterAlt struct {
	Value string
	IP    bool
}

// LineFilterStage is a `|= "value"` pipeline stage. Or holds the
// alternatives of `|= "a" or "b"`: Loki matches any of them for |=, |~ and |>
// and none of them for !=, !~ and !> (the negated chain is a conjunction).
type LineFilterStage struct {
	Op    LineFilterOp
	Value string
	IP    bool // Distinguishes ip("address") from the literal "ip(address)".
	Or    []LineFilterAlt
}

func (s *LineFilterStage) String() string {
	op := lineFilterOpText(s.Op)
	if len(s.Or) == 0 {
		if s.IP {
			return op + " ip(" + strconv.Quote(s.Value) + ")"
		}
		return op + " " + strconv.Quote(s.Value)
	}
	str := op + " " + lineFilterOperand(s.Value, s.IP)
	for _, alt := range s.Or {
		str += " or " + lineFilterOperand(alt.Value, alt.IP)
	}
	return str
}

// lineFilterOpText is the operator of a line filter as written: |=, !=, |~, !~, |> or !>.
func lineFilterOpText(op LineFilterOp) string {
	switch op {
	case LineFilterContains:
		return "|="
	case LineFilterExcludes:
		return "!="
	case LineFilterMatchRe:
		return "|~"
	case LineFilterExcludeRe:
		return "!~"
	case LineFilterContainsPat:
		return "|>"
	case LineFilterExcludePat:
		return "!>"
	}
	return ""
}

func lineFilterOperand(value string, ip bool) string {
	if ip {
		return "ip(" + strconv.Quote(value) + ")"
	}
	return strconv.Quote(value)
}

// negated reports whether the operator is one of !=, !~ and !>.
func (s *LineFilterStage) negated() bool {
	return s.Op == LineFilterExcludes || s.Op == LineFilterExcludeRe || s.Op == LineFilterExcludePat
}

// flatString renders the stage the way Loki prints a negated or chain, which
// it turns into one filter per alternative (`!~ "a" !~ ip("b")`).
func (s *LineFilterStage) flatString() string {
	if !s.negated() || len(s.Or) == 0 {
		return s.String()
	}
	op := s.String()[:2]
	if len(s.Or) == 0 {
		if s.IP {
			return op + " ip(" + strconv.Quote(s.Value) + ")"
		}
		return op + " " + strconv.Quote(s.Value)
	}
	str := op + " " + lineFilterOperand(s.Value, s.IP)
	for _, alt := range s.Or {
		str += " " + op + " " + lineFilterOperand(alt.Value, alt.IP)
	}
	return str
}

func (s *LineFilterStage) stage() {}

// ParserType identifies the parser variant.
type ParserType int

const (
	ParserJSON    ParserType = iota // json
	ParserLogfmt                    // logfmt
	ParserRegexp                    // regexp `...`
	ParserPattern                   // pattern `...`
	ParserUnpack                    // unpack
)

// ParserStage is a `| json` / `| logfmt` / `| regexp ...` stage.
type ParserStage struct {
	Type  ParserType
	Param string
	// Strict and KeepEmpty are logfmt's --strict and --keep-empty flags.
	Strict, KeepEmpty bool
	// Fields holds the explicit `| json a="expr", b` / `| logfmt ...`
	// extraction list; nil when the stage extracts every field.
	Fields []ExtractionField
}

// ExtractionField is one `name="expression"` (or bare `name`) entry of an
// explicit json/logfmt extraction list. Expression equals Name for the bare form.
type ExtractionField struct {
	Name       string
	Expression string
}

func (s *ParserStage) String() string {
	switch s.Type {
	case ParserJSON:
		return strings.TrimSpace("| json " + s.Param)
	case ParserLogfmt:
		flags := ""
		if s.Strict {
			flags += "--strict "
		}
		if s.KeepEmpty {
			flags += "--keep-empty "
		}
		return strings.TrimSpace("| logfmt " + flags + s.Param)
	case ParserRegexp:
		return "| regexp " + quoteParserArgument(s.Param)
	case ParserPattern:
		return "| pattern " + quoteParserArgument(s.Param)
	case ParserUnpack:
		return "| unpack"
	}
	return "| json"
}

func (s *ParserStage) stage() {}

func quoteParserArgument(value string) string {
	if strconv.CanBackquote(value) {
		return "`" + value + "`"
	}
	return strconv.Quote(value)
}

// LabelFilterStage is a `| level="error"` stage (raw expression).
type LabelFilterStage struct {
	Raw string
	// Expr is the parsed filter, nil for a stage that is not a filter Loki
	// accepts.
	Expr *LabelFilterExpr
}

func (s *LabelFilterStage) String() string {
	return "| " + s.Raw
}

func (s *LabelFilterStage) stage() {}

// DropMatcher is a conditional matcher in `| drop field=value` or `| keep field=value`.
type DropMatcher struct {
	Name  string
	Op    string // "=", "!=", "=~", "!~"
	Value string
}

func (m DropMatcher) String() string {
	return m.Name + m.Op + strconv.Quote(m.Value)
}

// DropStage is a `| drop label1, label2` or `| drop level="debug"` stage.
type DropStage struct {
	Labels   []string      // bare: | drop a, b
	Matchers []DropMatcher // conditional: | drop level="debug"
}

func (s *DropStage) String() string {
	parts := make([]string, 0, len(s.Labels)+len(s.Matchers))
	parts = append(parts, s.Labels...)
	for _, m := range s.Matchers {
		parts = append(parts, m.String())
	}
	return "| drop " + strings.Join(parts, ", ")
}

func (s *DropStage) stage() {}

// KeepStage is a `| keep label1, label2` or `| keep level="debug"` stage.
type KeepStage struct {
	Labels   []string
	Matchers []DropMatcher
}

func (s *KeepStage) String() string {
	parts := make([]string, 0, len(s.Labels)+len(s.Matchers))
	parts = append(parts, s.Labels...)
	for _, m := range s.Matchers {
		parts = append(parts, m.String())
	}
	return "| keep " + strings.Join(parts, ", ")
}

func (s *KeepStage) stage() {}

// DecolorizeStage removes ANSI color codes.
type DecolorizeStage struct{}

func (s *DecolorizeStage) String() string { return "| decolorize" }
func (s *DecolorizeStage) stage()         {}

// UnwrapStage is a `| unwrap label` or `| unwrap bytes(label)` stage.
type UnwrapStage struct {
	Label     string
	Converter string // e.g. "bytes", "duration" — empty means bare unwrap
}

func (s *UnwrapStage) String() string {
	if s.Converter == "" {
		return "| unwrap " + s.Label
	}
	return fmt.Sprintf("| unwrap %s(%s)", s.Converter, s.Label)
}

func (s *UnwrapStage) stage() {}

// LineFormatStage is a `| line_format "template"` stage.
type LineFormatStage struct {
	Template string
	// Tmpl is the parsed template; nil when the parser did not type it.
	Tmpl *LineTemplate
}

// KeyText is the template text a plan key uses: the canonical form when the
// template is typed. A template that does not parse never equals a canonical
// form, since every canonical form parses.
func (s *LineFormatStage) KeyText() string {
	if s.Tmpl != nil {
		return s.Tmpl.Canonical
	}
	return s.Template
}

func (s *LineFormatStage) String() string {
	return "| line_format " + strconv.Quote(s.Template)
}

func (s *LineFormatStage) stage() {}

// LabelFormatStage is a `| label_format ...` stage (raw expression).
type LabelFormatStage struct {
	Raw     string
	Formats []LabelFormat
}

// LabelFormat is one `dst="template"` or `dst=src` (Rename) label_format entry.
type LabelFormat struct {
	Name   string
	Value  string
	Rename bool
}

func (s *LabelFormatStage) String() string {
	return "| label_format " + s.Raw
}

func (s *LabelFormatStage) stage() {}

// LogQuery is a stream selector with an optional pipeline.
type LogQuery struct {
	Selector *StreamSelector
	Pipeline []Stage
}

func (q *LogQuery) String() string {
	var b strings.Builder
	b.WriteString(q.Selector.String())
	for _, s := range q.Pipeline {
		b.WriteByte(' ')
		b.WriteString(s.String())
	}
	return b.String()
}

func (q *LogQuery) expr() {}

// Grouping is a `by (label,...)` or `without (label,...)` clause.
type Grouping struct {
	Without bool
	Labels  []string
}

func (g *Grouping) String() string {
	kw := "by"
	if g.Without {
		kw = "without"
	}
	return fmt.Sprintf("%s (%s)", kw, strings.Join(g.Labels, ", "))
}

// RangeOp is the aggregation function applied over a range.
type RangeOp string

const (
	RangeRate             RangeOp = "rate"
	RangeCountOverTime    RangeOp = "count_over_time"
	RangeBytesRate        RangeOp = "bytes_rate"
	RangeBytesOverTime    RangeOp = "bytes_over_time"
	RangeAvgOverTime      RangeOp = "avg_over_time"
	RangeSumOverTime      RangeOp = "sum_over_time"
	RangeMinOverTime      RangeOp = "min_over_time"
	RangeMaxOverTime      RangeOp = "max_over_time"
	RangeStdvarOverTime   RangeOp = "stdvar_over_time"
	RangeStddevOverTime   RangeOp = "stddev_over_time"
	RangeQuantileOverTime RangeOp = "quantile_over_time"
	RangeFirstOverTime    RangeOp = "first_over_time"
	RangeLastOverTime     RangeOp = "last_over_time"
	RangeAbsentOverTime   RangeOp = "absent_over_time"
	RangeRateCounter      RangeOp = "rate_counter"
)

// RangeAggregation is e.g. `rate({app="api"}[5m])`. LogQL has no subquery
// grammar, so Inner is always the (possibly parenthesised) log query.
type RangeAggregation struct {
	Op       RangeOp
	Inner    Expr    // *LogQuery
	Range    string  // range, e.g. "5m" or "1h"
	Offset   string  // optional offset modifier, e.g. "1h" from [5m] offset 1h
	Param    float64 // for quantile_over_time
	HasParam bool
	Grouping *Grouping
	// RangeDuration and OffsetDuration are Range and Offset as durations: 0
	// when the offset is absent or the literal is not a positive duration.
	RangeDuration  time.Duration
	OffsetDuration time.Duration
}

func (r *RangeAggregation) String() string {
	rangeStr := "[" + r.Range + "]"
	if r.Offset != "" {
		rangeStr += " offset " + r.Offset
	}
	inner := r.Inner.String() + rangeStr
	if r.HasParam {
		inner = formatNumber(r.Param) + "," + inner
	}
	s := fmt.Sprintf("%s(%s)", string(r.Op), inner)
	if r.Grouping != nil && r.Grouping.String() != "" {
		s += " " + r.Grouping.String()
	}
	return s
}

// HasOffset reports whether the selector carries an offset modifier, even a
// zero one.
func (r *RangeAggregation) HasOffset() bool { return r.Offset != "" }

func (r *RangeAggregation) expr() {}

// VectorOp is the vector aggregation function.
type VectorOp string

const (
	VectorSum      VectorOp = "sum"
	VectorAvg      VectorOp = "avg"
	VectorMin      VectorOp = "min"
	VectorMax      VectorOp = "max"
	VectorCount    VectorOp = "count"
	VectorStddev   VectorOp = "stddev"
	VectorStdvar   VectorOp = "stdvar"
	VectorBottomK  VectorOp = "bottomk"
	VectorTopK     VectorOp = "topk"
	VectorSort     VectorOp = "sort"
	VectorSortDesc VectorOp = "sort_desc"
)

// VectorAggregation is e.g. `sum by (app) (rate(...))`.
type VectorAggregation struct {
	Op       VectorOp
	Grouping *Grouping
	Param    float64 // for topk/bottomk
	HasParam bool
	Inner    Expr
}

func (v *VectorAggregation) String() string {
	var b strings.Builder
	b.WriteString(string(v.Op))
	if v.Grouping != nil && v.Grouping.String() != "" {
		b.WriteByte(' ')
		b.WriteString(v.Grouping.String())
	}
	b.WriteString(" (")
	if v.HasParam {
		b.WriteString(formatNumber(v.Param) + ",")
	}
	b.WriteString(v.Inner.String())
	b.WriteByte(')')
	return b.String()
}

func (v *VectorAggregation) expr() {}

// VectorMatching describes the on/ignoring + group_left/group_right modifiers
// on a binary operation.
type VectorMatching struct {
	Card        string   // "on" or "ignoring"
	MatchLabels []string // labels for on/ignoring
	GroupSide   string   // "group_left" or "group_right" (empty = one-to-one)
	Include     []string // labels to include from the "one" side
}

func (vm *VectorMatching) String() string {
	if vm == nil {
		return ""
	}
	var b strings.Builder
	if vm.Card != "" {
		b.WriteString(vm.Card)
		b.WriteString("(")
		b.WriteString(strings.Join(vm.MatchLabels, ", "))
		b.WriteString(")")
	}
	if vm.GroupSide != "" {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(vm.GroupSide)
		b.WriteString("(")
		b.WriteString(strings.Join(vm.Include, ", "))
		b.WriteString(")")
	}
	return b.String()
}

// BinOpExpr is a binary operation between two metric expressions.
type BinOpExpr struct {
	Left, Right    Expr
	Op             string
	ReturnBool     bool
	VectorMatching *VectorMatching
}

func (b *BinOpExpr) String() string {
	left, right := b.Left.String(), b.Right.String()
	if _, ok := b.Left.(*BinOpExpr); ok {
		left = "(" + left + ")"
	}
	if _, ok := b.Right.(*BinOpExpr); ok {
		right = "(" + right + ")"
	}
	s := left + " " + b.Op
	if b.ReturnBool {
		s += " bool"
	}
	if vm := b.VectorMatching.String(); vm != "" {
		s += " " + vm
	}
	s += " " + right
	return s
}

func (b *BinOpExpr) expr() {}

// LiteralExpr is a scalar literal value.
type LiteralExpr struct {
	Value float64
}

// BadLiteralExpr is a number literal Go cannot read as a float (0x1, 1e999): the lexer takes it as a number, and
// Loki reports the conversion error when it has read the whole query, so validateSemantics raises Err.
type BadLiteralExpr struct {
	Err string
}

func (b *BadLiteralExpr) String() string { return b.Err }
func (b *BadLiteralExpr) expr()          {}

func (l *LiteralExpr) String() string {
	return formatNumber(l.Value)
}

// formatNumber prints a number the parser reads back to the same value: no
// exponent (`1000000`, not `1e+06`), which the scanner cuts into `1e`, `+`, `06`.
func formatNumber(v float64) string {
	if v == 0 && math.Signbit(v) {
		// Loki's lexer reads `-0` as a duration (Go's ParseDuration accepts "0" without a unit); `- 0` is the
		// negated number.
		return "- 0"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func (l *LiteralExpr) expr() {}

// OpaqueMetricExpr holds the raw text of a metric-level function call that the
// parser does not type as its own node (label_replace, vector, approx_topk). It
// passes through to VictoriaLogs without modification.
//
// Func names the call. A label_replace or vector call written as Loki's grammar
// has it also carries its parts (Typed): label_replace its sample expression
// (Inner) and its four strings (Args: destination label, replacement, source
// label, regexp, unquoted), vector its number (Value). Raw is the text either way.
type OpaqueMetricExpr struct {
	Raw   string
	Func  string
	Typed bool
	Inner Expr
	Args  []string
	Value float64
}

func (o *OpaqueMetricExpr) String() string { return o.Raw }
func (o *OpaqueMetricExpr) expr()          {}

// StripIncompleteUnwrapStubs removes UnwrapStage nodes with an empty Label from a
// log query's pipeline. These are emitted by Grafana's metric builder while the
// user selects an unwrap field. Returns lq unchanged if no incomplete stubs exist.
func StripIncompleteUnwrapStubs(lq *LogQuery) *LogQuery {
	hasStub := false
	for _, s := range lq.Pipeline {
		if u, ok := s.(*UnwrapStage); ok && u.Label == "" {
			hasStub = true
			break
		}
	}
	if !hasStub {
		return lq
	}
	var pipeline []Stage
	for _, s := range lq.Pipeline {
		if u, ok := s.(*UnwrapStage); ok && u.Label == "" {
			continue
		}
		pipeline = append(pipeline, s)
	}
	return &LogQuery{Selector: lq.Selector, Pipeline: pipeline}
}
