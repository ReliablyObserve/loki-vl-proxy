// Package physical holds the typed operators a translated query runs as: the VictoriaLogs query and the
// post-processing the proxy applies to its answer. Today the translator carries that post-processing inside the
// LogsQL text as markers (`__without__:`, `__lvp_lr:`, `__binary__:`); translator.DecodePhysical reads the markers
// into these operators and translator.EncodePhysical writes them back. The package imports only the standard
// library and internal/logsql, so the logical plan can build operators without the translator.
package physical

import "slices"

// Op is a node of the physical plan.
type Op interface{ op() }

// VLQuery is the LogsQL the proxy sends to VictoriaLogs, as text: the clean query, without markers. The typed
// query arrives with the handlers that read it.
type VLQuery struct{ Text string }

// VLStats is a VictoriaLogs stats query as the translator writes it. Text is the whole query; when Shaped, Base is
// the query before its first stats pipe and By, ByExplicit, Func and Field are that pipe's grouping and function,
// as text (a bucket key such as `_time:1m offset 1s` stays one entry of By). The typed query arrives with the
// stats lowering.
type VLStats struct {
	Text       string
	Shaped     bool
	Base       string
	By         []string
	ByExplicit bool
	Func       string
	Field      string
	// Gate is the unwrap gate that ends Base (the pipes that select and convert the unwrapped value), and
	// UnwrapField the field it converts; both empty without one. UngatedText is Text without the gate.
	Gate        string
	UnwrapField string
	UngatedText string
}

// Grouping is the by or without clause of a level the proxy evaluates; a nil *Grouping is no clause. `by ()` is kept
// as written (non-nil, no labels); Canon folds it to nil, because the proxy reads both as one group.
type Grouping struct {
	Labels  []string
	Without bool
}

// RankStep ranks the series of In at each step: topk and bottomk keep the K best of each group of By (NaN last),
// sort and sort_desc keep every series (K is 0). The proxy evaluates it over the answer of In, so it has no text of
// its own: the legacy encoding has no marker for it and Encode writes In, the request the proxy sends on.
type RankStep struct {
	Op string // topk, bottomk, sort, sort_desc
	K  int
	By *Grouping
	In Op
}

// SeriesAgg folds the series of In at each step, per group of By: stddev and stdvar (population), and sum, avg, min,
// max and count when the level below is itself evaluated by the proxy (the layers above a ranking). Like RankStep it
// has no text: Encode writes In.
type SeriesAgg struct {
	Op string
	By *Grouping
	In Op
}

// Fold groups the series of In by the labels of its levels, innermost first, and combines the ones that end with
// the same label set.
type Fold struct {
	Levels []FoldLevel
	In     Op
}

// FoldLevel is one `<op> without (labels)` or `<op> by (labels)` level of a Fold.
type FoldLevel struct {
	// Op is the aggregation the proxy folds itself (sum, min, max, avg or count). It is empty when the query's own
	// aggregation does the fold, which the proxy reads from the LogQL.
	Op      string
	Labels  []string
	Without bool
}

// LabelMap rewrites the labels of the series of In: the label_replace calls of the query, innermost first.
type LabelMap struct {
	Replace []LabelReplace
	In      Op
}

// LabelReplace is the four arguments of a label_replace call after the vector.
type LabelReplace struct {
	DstLabel    string
	Replacement string
	SrcLabel    string
	Regex       string
}

// Binary combines the series of two operands.
type Binary struct {
	Op string
	// Bool is false in the legacy encoding: the translator drops the `bool` modifier, because the proxy always
	// answers 1 or 0 for a comparison.
	Bool  bool
	Match *Matching // nil without an on, ignoring, group_left or group_right modifier
	// Tail is the modifier text of the legacy encoding, `@@@on:app@@@group_left:pod`, which Match was read
	// from. EncodePhysical writes it back, so a change to Match alone does not reach the encoded text.
	Tail string
	L, R Op
	// Text is the whole binary expression as the translator wrote it. DecodePhysical sets it and EncodePhysical
	// returns it as it is, so reading a query allocates no copy; a Binary built without it is written from Op, L, R
	// and Tail.
	Text string
}

// Matching is the vector matching of a Binary.
type Matching struct {
	MatchOn    bool // on() was written, with or without labels
	GroupSide  string
	On         []string
	Ignoring   []string
	GroupLeft  []string
	GroupRight []string
}

// ScalarOp is a number as an operand of a Binary.
type ScalarOp struct {
	V float64
	// Text is the number as the translator wrote it (`100`, `1e3`), which EncodePhysical writes back.
	Text string
}

// VectorOp is vector(n) as an operand of a Binary: one series without labels, V at every step. Unlike a ScalarOp it is
// a vector, so it takes part in vector matching (on, ignoring, group_left, group_right) and a comparison with it
// filters. The proxy answers it from the constant and sends nothing to VictoriaLogs for it; the legacy text encoding
// has no form for it, so DecodePhysical never produces it.
type VectorOp struct {
	V float64
	// Text is the operand as the lowering writes it (`vector(2)`), which Encode writes back.
	Text string
}

// Post is the post-processing of a decoded query that runs after VictoriaLogs answers: the fold of a `without`
// aggregation the proxy combines itself, then the label_replace calls.
type Post struct {
	// Without is the fold, and Without.Without reports whether there is one (see Folds). It is a value, not a
	// pointer, so reading a query costs no allocation.
	Without      FoldLevel
	LabelReplace []LabelReplace
}

// Folds reports whether the query carries a `without` fold.
func (p Post) Folds() bool { return p.Without.Without }

// Verbatim keeps the text of a query whose markers EncodePhysical cannot write back byte for byte: a marker in the
// middle of the query, a padded or empty label list. In is the decoded query, and the proxy reads it as it reads
// any other. The text the translator produces never needs it.
type Verbatim struct {
	Text string
	In   Op
}

func (VLQuery) op()   {}
func (VLStats) op()   {}
func (RankStep) op()  {}
func (SeriesAgg) op() {}
func (Fold) op()      {}
func (LabelMap) op()  {}
func (Binary) op()    {}
func (ScalarOp) op()  {}
func (VectorOp) op()  {}
func (Verbatim) op()  {}

// Split separates the post-processing of a decoded query from the operator it applies to. The core is what the
// proxy sends to VictoriaLogs: translator.EncodePhysical(core) is the clean query text. A core that is a RankStep or
// a SeriesAgg is evaluated by the proxy: SplitEval separates its levels from the operand it evaluates them over, and
// Split of that operand gives the request (the core) and the operand's own post-processing.
func Split(op Op) (core Op, post Post) {
	if v, ok := op.(Verbatim); ok {
		op = v.In
	}
	if v, ok := op.(LabelMap); ok {
		post.LabelReplace, op = v.Replace, v.In
	}
	if v, ok := op.(Fold); ok {
		if len(v.Levels) > 0 {
			post.Without = v.Levels[0]
		}
		op = v.In
	}
	return op, post
}

// Level is one level the proxy evaluates over the series of the operand below it: a ranking (topk, bottomk, sort,
// sort_desc; K is the count of the first two) or a series aggregation (stddev, stdvar, sum, avg, min, max, count).
type Level struct {
	Op string
	K  int
	By *Grouping
}

// Ranks reports whether the level is a ranking.
func (l Level) Ranks() bool {
	switch l.Op {
	case "topk", "bottomk", "sort", "sort_desc":
		return true
	}
	return false
}

// SplitEval separates the levels the proxy evaluates from the operand they are evaluated over: the RankStep and
// SeriesAgg chain of core, innermost level first (the order the proxy applies them in). A core that is neither has
// no levels and is its own operand.
func SplitEval(core Op) (levels []Level, operand Op) {
	for {
		switch v := core.(type) {
		case RankStep:
			levels, core = append(levels, Level{v.Op, v.K, v.By}), v.In
		case SeriesAgg:
			levels, core = append(levels, Level{v.Op, 0, v.By}), v.In
		default:
			slices.Reverse(levels)
			return levels, core
		}
	}
}

// Evaluated reports whether op holds a level the proxy evaluates (a RankStep or a SeriesAgg), anywhere in the
// operators that wrap or combine queries.
func Evaluated(op Op) bool {
	switch v := op.(type) {
	case RankStep, SeriesAgg:
		return true
	case Verbatim:
		return Evaluated(v.In)
	case LabelMap:
		return Evaluated(v.In)
	case Fold:
		return Evaluated(v.In)
	case Binary:
		return Evaluated(v.L) || Evaluated(v.R)
	}
	return false
}
