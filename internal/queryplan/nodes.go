// Package queryplan is the logical plan of a LogQL query: Loki's meaning of the
// query, as typed nodes, with a canonical key. It is a function of the
// validated logql.Expr and of Inputs only. No VictoriaLogs input (field names,
// stream fields, capabilities) and no time input (window, step, limit) enters
// it, so a plan can be memoised and shared across VictoriaLogs versions.
//
// Build converts the AST structurally and totally: it parses no text and calls
// no String method. Leaf values such as groupings, label filters and extraction
// lists are shared with the Expr, which is immutable once validated.
package queryplan

import (
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Kind is how the query is evaluated.
type Kind uint8

// The kinds of evaluation. Loki's semantics differ between them (keep
// grouping, instant topk).
const (
	KindLogs Kind = iota
	KindRange
	KindInstant
)

// Compat is the Loki version whose semantics the plan follows.
type Compat uint8

// Compat37 is the only version Build accepts until the 4.0 rules exist.
const Compat37 Compat = iota

// Exposure is the client-name profile: which label names clients may write and see.
type Exposure struct {
	LokiProfile bool
	DottedNames bool
}

// Inputs are everything besides the Expr that decides the logical plan.
type Inputs struct {
	Kind   Kind
	Compat Compat
	// StreamLabels are the Loki stream label names, sorted and deduplicated by
	// the caller; Build keeps them as given.
	StreamLabels []string
	Exposure     Exposure
}

// Plan is a logical plan: the root node, the inputs it was built from and the
// canonical key of both.
type Plan struct {
	Root Node
	In   Inputs
	// Key is Root's key followed by the encoded Inputs.
	Key []byte
}

// Node is a logical node of the metric or log side.
type Node interface {
	Props() *Props
}

// Props is what a node exposes to the planner and the cache.
type Props struct {
	// Key is the canonical bytes of the subtree. It is self-delimiting and
	// does not depend on Inputs, so a subtree has the same key alone as inside
	// a larger query.
	Key []byte
	// Lookback is the largest range plus offset on any path below the node: the
	// freshness input.
	Lookback time.Duration
	// Offset is the largest offset below the node.
	Offset time.Duration
}

// The leaf types below are the AST's own; a plan shares them with its Expr.
type (
	// MatchOp is =, !=, =~ or !~.
	MatchOp = logql.MatchOp
	// LineOp is a line filter operator.
	LineOp = logql.LineFilterOp
	// ParserKind is json, logfmt, regexp, pattern or unpack.
	ParserKind = logql.ParserType
	// Extraction is one entry of a json or logfmt extraction list.
	Extraction = logql.ExtractionField
	// LFExpr is a parsed label filter: And, Or or Leaf with its typed value.
	LFExpr = logql.LabelFilterExpr
	// LabelFormatEntry is one `dst="template"` or `dst=src` entry.
	LabelFormatEntry = logql.LabelFormat
	// Grouping is a by or without clause. A nil *Grouping is no clause; `by ()`
	// is a non-nil Grouping without labels.
	Grouping = logql.Grouping
	// Matching is the on, ignoring, group_left and group_right modifiers.
	Matching = logql.VectorMatching
)

// LabelSource says where a matcher's label comes from. The resolver sets it.
type LabelSource uint8

// LabelSourceUnresolved is the only source until the resolver exists.
const LabelSourceUnresolved LabelSource = iota

// Matcher is a stream selector entry or a conditional keep/drop entry.
type Matcher struct {
	Name, Value string
	Op          MatchOp
	Src         LabelSource
}

// ---- pipeline side ----

// LogSelect is a stream selector with its pipeline.
type LogSelect struct {
	Matchers []Matcher
	// Stages are in Loki order: a Loki pipeline is sequential.
	Stages []Stage
	// Sem is filled by the resolver; zero in this package.
	Sem PipelineSem
	p   Props
}

// Props implements Node.
func (n *LogSelect) Props() *Props { return &n.p }

// Stage is a pipeline stage.
type Stage interface{ stage() }

// LineMatch is one alternative of a line filter.
type LineMatch struct {
	Value string
	IP    bool
}

// LineFilter is `|= "a" or "b"`: the alternatives are ORed for |=, |~ and |>
// and ANDed as negations for !=, !~ and !>.
type LineFilter struct {
	Op   LineOp
	Alts []LineMatch
}

// Parse is a parser stage.
type Parse struct {
	Kind   ParserKind
	Fields []Extraction
	// Expr is the source of a regexp or pattern parser.
	Expr string
	// Param is a json or logfmt stage's argument text as written (the field
	// list, and the logfmt flags once logql types them).
	Param string
}

// LabelFilter is a label filter stage.
type LabelFilter struct {
	Expr *LFExpr
	// Raw is the stage text, only when Expr is nil (a filter logql does not type).
	Raw string
}

// Project is a keep or drop stage.
type Project struct {
	Keep  bool
	Names []string
	Conds []Matcher
}

// LineFormat is a line_format stage.
type LineFormat struct {
	// Raw is the template as written; logql types it later.
	Raw string
}

// LabelFormat is a label_format stage.
type LabelFormat struct {
	Entries []LabelFormatEntry
}

// Decolorize is a decolorize stage.
type Decolorize struct{}

// Unwrap is an unwrap stage.
type Unwrap struct {
	Field string
	// Conv is the converter as written: empty, bytes, duration, duration_seconds.
	Conv string
}

func (*LineFilter) stage()  {}
func (*Parse) stage()       {}
func (*LabelFilter) stage() {}
func (*Project) stage()     {}
func (*LineFormat) stage()  {}
func (*LabelFormat) stage() {}
func (*Decolorize) stage()  {}
func (*Unwrap) stage()      {}

// PipelineSem is the resolved semantics of a pipeline. The resolver fills it
// in a later change; Build leaves it zero.
type PipelineSem struct {
	Parsers      int
	ErrorPolicy  ErrorPolicy
	ErrorAt      int
	LineMutated  bool
	Extracted    []string
	Out          OutLabels
	NeedsLevel   LevelNeed
	NeedsService bool
	KeepLine     bool
}

// MetricSem is the resolved semantics of a metric node.
type MetricSem struct {
	ErrSamples ErrSampleRule
	AbsentOut  []string
	NowDep     bool
}

// ErrorPolicy says how a pipeline treats lines a parser fails on.
type ErrorPolicy uint8

// LevelNeed says how a query needs detected_level.
type LevelNeed uint8

// ErrSampleRule says how errored samples surface in an unwrap metric.
type ErrSampleRule uint8

// OutLabels are the labels after the last stage.
type OutLabels struct {
	Names   []string
	Dynamic bool
}

// ---- metric side ----

// RangeAgg is a range aggregation over a log selector.
type RangeAgg struct {
	Op     logql.RangeOp
	In     *LogSelect
	Range  time.Duration
	Offset time.Duration
	// HasOffset is true for `offset 0s` too: a zero Offset is not "no offset".
	HasOffset bool
	// Phi is quantile_over_time's value as Loki reads it (any clamp for
	// VictoriaLogs is a physical rule).
	Phi float64
	// By is the range-level grouping of the unwrap functions.
	By  *Grouping
	Sem MetricSem
	p   Props
}

// VectorAgg is sum, avg, min, max, count, stddev or stdvar.
type VectorAgg struct {
	Op logql.VectorOp
	By *Grouping
	In Node
	p  Props
}

// Rank is topk, bottomk, sort or sort_desc: per-step semantics, never a pushdown target.
type Rank struct {
	Op logql.VectorOp
	// K is the topk or bottomk count, 0 for sort and sort_desc.
	K  int
	By *Grouping
	In Node
	p  Props
}

// BinOp is a binary operation between two metric expressions or scalars.
type BinOp struct {
	Op    string
	Bool  bool
	Match *Matching
	L, R  Node
	p     Props
}

// Scalar is a number literal.
type Scalar struct {
	V float64
	p Props
}

// Opaque is a metric-level call logql keeps as text (label_replace, vector,
// approx_topk) until it types them. It is a ratchet line, never lowered.
type Opaque struct {
	Raw string
	p   Props
}

// Props implements Node.
func (n *RangeAgg) Props() *Props  { return &n.p }
func (n *VectorAgg) Props() *Props { return &n.p }
func (n *Rank) Props() *Props      { return &n.p }
func (n *BinOp) Props() *Props     { return &n.p }
func (n *Scalar) Props() *Props    { return &n.p }
func (n *Opaque) Props() *Props    { return &n.p }
