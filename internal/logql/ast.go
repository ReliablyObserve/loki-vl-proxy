package logql

import (
	"fmt"
	"strings"
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
	var op string
	switch m.Op {
	case MatchEq:
		op = "="
	case MatchNeq:
		op = "!="
	case MatchRe:
		op = "=~"
	case MatchNotRe:
		op = "!~"
	}
	return fmt.Sprintf(`%s%s"%s"`, m.Name, op, m.Value)
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

// LineFilterStage is a `|= "value"` pipeline stage.
type LineFilterStage struct {
	Op    LineFilterOp
	Value string
}

func (s *LineFilterStage) String() string {
	var op string
	switch s.Op {
	case LineFilterContains:
		op = "|="
	case LineFilterExcludes:
		op = "!="
	case LineFilterMatchRe:
		op = "|~"
	case LineFilterExcludeRe:
		op = "!~"
	case LineFilterContainsPat:
		op = "|>"
	case LineFilterExcludePat:
		op = "!>"
	}
	return fmt.Sprintf(`%s "%s"`, op, s.Value)
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
}

func (s *ParserStage) String() string {
	switch s.Type {
	case ParserJSON:
		return "| json"
	case ParserLogfmt:
		return "| logfmt"
	case ParserRegexp:
		return fmt.Sprintf("| regexp `%s`", s.Param)
	case ParserPattern:
		return fmt.Sprintf("| pattern `%s`", s.Param)
	case ParserUnpack:
		return "| unpack"
	}
	return "| json"
}

func (s *ParserStage) stage() {}

// LabelFilterStage is a `| level="error"` stage (raw expression).
type LabelFilterStage struct {
	Raw string
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
	return fmt.Sprintf(`%s%s"%s"`, m.Name, m.Op, m.Value)
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
}

func (s *LineFormatStage) String() string {
	return fmt.Sprintf(`| line_format "%s"`, s.Template)
}

func (s *LineFormatStage) stage() {}

// LabelFormatStage is a `| label_format ...` stage (raw expression).
type LabelFormatStage struct {
	Raw string
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
	if len(g.Labels) == 0 {
		return ""
	}
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

// RangeAggregation is e.g. `rate({app="api"}[5m])` or a subquery
// `max_over_time(rate({app="api"}[5m])[1h:5m])`.
type RangeAggregation struct {
	Op       RangeOp
	Inner    Expr    // *LogQuery for plain range; any Expr for subquery
	Range    string  // outer range, e.g. "5m" or "1h"
	Step     string  // subquery step (e.g. "5m" from [1h:5m]); empty for plain range
	Offset   string  // optional offset modifier, e.g. "1h" from [5m] offset 1h
	Param    float64 // for quantile_over_time
	HasParam bool
	Grouping *Grouping
}

func (r *RangeAggregation) String() string {
	rangeStr := "[" + r.Range + "]"
	if r.Step != "" {
		rangeStr = "[" + r.Range + ":" + r.Step + "]"
	}
	if r.Offset != "" {
		rangeStr += " offset " + r.Offset
	}
	inner := r.Inner.String() + rangeStr
	if r.HasParam {
		inner = fmt.Sprintf("%g,%s", r.Param, inner)
	}
	s := fmt.Sprintf("%s(%s)", string(r.Op), inner)
	if r.Grouping != nil && r.Grouping.String() != "" {
		s += " " + r.Grouping.String()
	}
	return s
}

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
		fmt.Fprintf(&b, "%g,", v.Param)
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
	VectorMatching *VectorMatching
}

func (b *BinOpExpr) String() string {
	s := b.Left.String() + " " + b.Op
	if vm := b.VectorMatching.String(); vm != "" {
		s += " " + vm
	}
	s += " " + b.Right.String()
	return s
}

func (b *BinOpExpr) expr() {}

// LiteralExpr is a scalar literal value.
type LiteralExpr struct {
	Value float64
}

func (l *LiteralExpr) String() string {
	return fmt.Sprintf("%g", l.Value)
}

func (l *LiteralExpr) expr() {}
