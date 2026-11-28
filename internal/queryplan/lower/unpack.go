package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// The `| unpack` parser. Loki's unpack reads a line a packing agent wrote (a JSON object holding an `_entry` key
// with a string): only then does it promote the object's string keys to labels, and it replaces the line with the
// value of `_entry`. A line without a string `_entry`, one that is not an object, or one that is empty goes on
// unchanged and gets no label (a line that is not JSON object also gets __error__).
//
// VictoriaLogs has no pipe with that rule. `unpack_json` promotes every key of every JSON line (numbers, booleans
// and nested values too, as text), never touches the line, and an empty value is a missing one. The string
// translator writes `| unpack` as `| unpack_json`. That text keeps the lines Loki keeps wherever no later stage reads
// what the unpack produced: the number of lines a count, a rate or an absence check sees per stream is the same
// whatever the unpack promotes, and a drop or a keep after it changes labels, not lines. It is wrong where a later
// stage reads the promoted labels (a label filter, an unwrapped value, a label_format) or the line (a line filter, a
// parser, a line_format, decolorize, the bytes of the line), and for a log query, which returns the line.
//
// What the text does not carry is the split of the count into one series per set of promoted labels, which Loki
// makes and which unpack_json promotes too many keys for: it is the series approximation the json lowering ships,
// handled where the proxy reads the answer (it reads an unpack as a json parser there).
//
// So only a plain range aggregation (count_over_time, rate, absent_over_time) over a query whose only parser is the
// unpack and whose stages after it are drops and keeps is lowered, written as the json parser the translator writes
// for it (the same pipe, so the text is equal to the translator's); each also under a sum with no by or
// without clause, which adds up the series however they split. Every other shape is delegated under the name of
// what it reads (semantics/translator-writes-unpack-as-unpack-json).

// The constructs this file delegates. The metric ones carry the prefix of the aggregation that owns the query.
const (
	// unpackLineConstruct is a log query holding an unpack: it returns the line, which Loki replaces by `_entry`.
	unpackLineConstruct = "LogSelect unpack line"
	// unpackLineReadConstruct is a stage or a range function that reads the line the unpack replaced.
	unpackLineReadConstruct = "unpack line read"
	// unpackLabelReadConstruct is a stage or a range function that reads the labels the unpack promoted.
	unpackLabelReadConstruct = "unpack label read"
	// unpackOrderConstruct is an unpack among other parsers, which read the line it replaced or the labels it set.
	unpackOrderConstruct = "unpack parser order"
	// unpackParserConstruct is the unpack in a query the lowering does not read the stages of (a vector aggregation's
	// operand other than an ungrouped sum of a count, a rate or an absence check, a nest).
	unpackParserConstruct = "LogSelect unpack parser"
)

// unpackStage is the index of the unpack stage of the log query, or -1.
func unpackStage(sel *queryplan.LogSelect) int {
	for i, st := range sel.Stages {
		if p, ok := st.(*queryplan.Parse); ok && p.Type == logql.ParserUnpack {
			return i
		}
	}
	return -1
}

// unpackRange is unpackRangeFor for a range aggregation lowered on its own.
func unpackRange(ra *queryplan.RangeAgg) (*queryplan.RangeAgg, error) {
	return unpackRangeFor(ra, "RangeAgg")
}

// unpackRangeFor is the range aggregation to lower for one over an unpack: ra itself when it has none, a copy whose
// unpack stage is the json stage the translator writes the same pipe for when its result reads neither the line nor
// the labels the unpack makes, and a NotLoweredError naming what it reads otherwise, under owner, the construct of
// the aggregation that lowers it.
func unpackRangeFor(ra *queryplan.RangeAgg, owner string) (*queryplan.RangeAgg, error) {
	at, err := unpackLowered(ra, owner)
	if at < 0 || err != nil {
		return ra, err
	}
	in := *ra.In
	in.Stages = append([]queryplan.Stage(nil), ra.In.Stages...)
	in.Stages[at] = &queryplan.Parse{Type: logql.ParserJSON}
	cp := *ra
	cp.In = &in
	return &cp, nil
}

// unpackLowered is the index of the unpack stage of ra's log query when its result reads neither the line nor the
// labels the unpack makes, -1 when it has none, and a NotLoweredError naming what it reads otherwise, under owner.
func unpackLowered(ra *queryplan.RangeAgg, owner string) (int, error) {
	at := unpackStage(ra.In)
	if at < 0 {
		return -1, nil
	}
	reads := func(what string) error { return &NotLoweredError{Construct: owner + " LogSelect " + what} }
	parsers := 0
	for _, st := range ra.In.Stages {
		if _, ok := st.(*queryplan.Parse); ok {
			parsers++
		}
	}
	if parsers != 1 {
		return -1, reads(unpackOrderConstruct)
	}
	// An unwrapped value is a promoted label, whichever function reads it.
	if unwrapStage(ra.In) != nil {
		return -1, reads(unpackLabelReadConstruct)
	}
	switch ra.Op {
	case logql.RangeCountOverTime, logql.RangeRate, logql.RangeAbsentOverTime:
	case logql.RangeBytesOverTime, logql.RangeBytesRate:
		return -1, reads(unpackLineReadConstruct)
	default:
		return -1, reads(unpackLabelReadConstruct)
	}
	for _, st := range ra.In.Stages[at+1:] {
		switch st.(type) {
		case *queryplan.Drop, *queryplan.Keep:
		case *queryplan.LabelFilter, *queryplan.LabelFormat:
			return -1, reads(unpackLabelReadConstruct)
		default:
			return -1, reads(unpackLineReadConstruct)
		}
	}
	return at, nil
}

// unpackSkipped is ra, a count or a rate over an unpack at stage at that unpackLowered allows, with no stage from the
// unpack on:
// for a sum with no by or without clause above it. Loki parses no line for such a sum (it needs no label: the
// recorded rule semantics/loki-pipeline-error-rules, and TestUnpackSumMatchesLoki over lines that are no JSON object),
// and the stages it is written for after the unpack are drops and, for a count, keeps (a rate keeps the unpack behind
// a keep: keepsAfterRate), which change labels and keep every line,
// so the number of lines in each window is the one the query without them counts. VictoriaLogs then reads no line
// for the unpack (semantics/ungrouped-sum-skips-unpack).
func unpackSkipped(ra *queryplan.RangeAgg, at int) *queryplan.RangeAgg {
	in := *ra.In
	in.Stages = ra.In.Stages[:at:at]
	cp := *ra
	cp.In = &in
	return &cp
}

// SkipsUnpack reports a plan whose root is a sum with no by or without clause of a count_over_time or a rate over an
// unpack the lowering writes with no stage from the unpack on (unpackSkipped): the proxy serves it from the plan,
// where the string translator writes the unpack_json pipe and the drops after it
// (semantics/ungrouped-sum-skips-unpack).
func SkipsUnpack(p *queryplan.Plan) bool {
	return skipsUnpack(p.Root)
}

// skipsUnpack is SkipsUnpack of a node.
func skipsUnpack(n queryplan.Node) bool {
	va, ok := n.(*queryplan.VectorAgg)
	if !ok {
		return false
	}
	ra, ok := va.In.(*queryplan.RangeAgg)
	if !ok || ra.In == nil || ra.Op == logql.RangeAbsentOverTime || !sumReadsNoSeries(va, ra) || unwrapStage(ra.In) != nil {
		return false
	}
	at, err := unpackLowered(ra, "VectorAgg")
	return at >= 0 && err == nil && !keepsAfterRate(ra, at)
}

// keepsAfterRate reports a rate with a keep after the stage at. The handlers group a rate behind a keep by the labels
// it leaves, so the sum keeps the unpack there (the translator's text): written with no unpack, a rate grouped by the
// stream was up to 1.35x slower than the translator's text on 155k streams, and as one series it answers a point at an
// instant over a selector with no line, where Loki answers none. A count behind a keep counts every line either way:
// `stats count()`, the text of the count with no keep.
func keepsAfterRate(ra *queryplan.RangeAgg, at int) bool {
	if ra.Op != logql.RangeRate {
		return false
	}
	for _, st := range ra.In.Stages[at+1:] {
		if _, ok := st.(*queryplan.Keep); ok {
			return true
		}
	}
	return false
}

// WritesSkippedUnpack reports a plan with, at any depth, SkipsUnpack's shape: the lowering writes it with no stage from
// the unpack on wherever it stands. The proxy serves the root (SkipsUnpack) and the operands of binary operations and
// label_replace calls above it (SkipsUnpackWrapped); the shadows hold every such plan as the case's.
func WritesSkippedUnpack(p *queryplan.Plan) bool {
	return writesSkippedUnpack(p.Root)
}

// writesSkippedUnpack is WritesSkippedUnpack of a node.
func writesSkippedUnpack(n queryplan.Node) bool {
	switch n := n.(type) {
	case *queryplan.VectorAgg:
		return skipsUnpack(n) || writesSkippedUnpack(n.In)
	case *queryplan.Rank:
		return writesSkippedUnpack(n.In)
	case *queryplan.LabelReplace:
		return writesSkippedUnpack(n.In)
	case *queryplan.BinOp:
		return writesSkippedUnpack(n.L) || writesSkippedUnpack(n.R)
	}
	return false
}

// SkipsUnpackWrapped reports a plan whose root is a binary operation or a label_replace call over SkipsUnpack's shape:
// every node between the root and each such sum is a binary operation or a label_replace call, and no other operand
// holds the shape deeper (under a ranking or another aggregation, which the proxy evaluates as layers of their own).
// Loki evaluates the operand as the query it is, so it needs no label there either; the lowering writes the operand
// with no stage from the unpack on, and the rest of the text as it writes the root's operands
// (semantics/ungrouped-sum-skips-unpack).
func SkipsUnpackWrapped(p *queryplan.Plan) bool {
	switch p.Root.(type) {
	case *queryplan.BinOp, *queryplan.LabelReplace:
	default:
		return false
	}
	skips, clean := wrappedSkip(p.Root)
	return skips && clean
}

// wrappedSkip reports whether n holds SkipsUnpack's shape under binary operations and label_replace calls only
// (skips), and whether no part of n holds it elsewhere (clean).
func wrappedSkip(n queryplan.Node) (skips, clean bool) {
	switch n := n.(type) {
	case *queryplan.BinOp:
		ls, lc := wrappedSkip(n.L)
		rs, rc := wrappedSkip(n.R)
		return ls || rs, lc && rc
	case *queryplan.LabelReplace:
		return wrappedSkip(n.In)
	case *queryplan.VectorAgg:
		if skipsUnpack(n) {
			return true, true
		}
	}
	return false, !writesSkippedUnpack(n)
}
