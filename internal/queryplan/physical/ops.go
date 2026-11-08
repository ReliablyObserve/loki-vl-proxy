// Package physical holds the typed operators a translated query runs as: the VictoriaLogs query and the
// post-processing the proxy applies to its answer. Today the translator carries that post-processing inside the
// LogsQL text as markers (`__without__:`, `__lvp_lr:`, `__binary__:`); translator.DecodePhysical reads the markers
// into these operators and translator.EncodePhysical writes them back. The package imports only the standard
// library and internal/logsql, so the logical plan can build operators without the translator.
package physical

// Op is a node of the physical plan.
type Op interface{ op() }

// VLQuery is the LogsQL the proxy sends to VictoriaLogs, as text: the clean query, without markers. The typed
// query arrives with the handlers that read it.
type VLQuery struct{ Text string }

// VLStats is a VictoriaLogs stats query. Declared only: its fields arrive with the stats handlers.
type VLStats struct{}

// RankStep ranks the series of each step (topk, bottomk, sort). Declared only: its fields arrive with the
// handlers that evaluate it.
type RankStep struct{}

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

// Post is the post-processing of a decoded query that runs after VictoriaLogs answers: the fold of a `without`
// aggregation the proxy combines itself, then the label_replace calls.
type Post struct {
	Without      *FoldLevel
	LabelReplace []LabelReplace
}

// Verbatim keeps the text of a query whose markers EncodePhysical cannot write back byte for byte: a marker in the
// middle of the query, a padded or empty label list. In is the decoded query, and the proxy reads it as it reads
// any other. The text the translator produces never needs it.
type Verbatim struct {
	Text string
	In   Op
}

func (VLQuery) op()  {}
func (VLStats) op()  {}
func (RankStep) op() {}
func (Fold) op()     {}
func (LabelMap) op() {}
func (Binary) op()   {}
func (ScalarOp) op() {}
func (Verbatim) op() {}

// Split separates the post-processing of a decoded query from the operator it applies to. The core is what the
// proxy sends to VictoriaLogs: translator.EncodePhysical(core) is the clean query text.
func Split(op Op) (core Op, post Post) {
	if v, ok := op.(Verbatim); ok {
		op = v.In
	}
	if v, ok := op.(LabelMap); ok {
		post.LabelReplace, op = v.Replace, v.In
	}
	if v, ok := op.(Fold); ok {
		if len(v.Levels) > 0 {
			level := &v.Levels[0]
			post.Without = level
		}
		op = v.In
	}
	return op, post
}
