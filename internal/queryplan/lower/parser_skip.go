package lower

import (
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// A sum that needs no label skips its json and logfmt parsers (semantics/ungrouped-sum-skips-parsers).
//
// `sum(count_over_time({…} | json | logfmt | drop __error__, __error_details__ [r]))`, the count Logs Drilldown
// asks for on every service page, counts the lines in each window. Loki parses no line for a sum with no by or
// without clause (or an empty by clause) directly over a count or a rate: the sum needs no label, so its parsers
// do not run and no line fails them (the recorded rule semantics/loki-pipeline-error-rules). A json or logfmt parser
// never drops a line, and neither does a drop; a keep changes labels only. So the number of lines in each window is
// the one the query with no stage from the first parser on counts.
//
// The string translator writes `| unpack_json`, `| unpack_logfmt` and the drops there, which make VictoriaLogs
// parse every selected line, and with both parsers the proxy also read 25 lines to choose one of them (the parser
// probe). The plan writes no stage from the first parser on for this shape (unpackSkipped), and the proxy sends
// no probe for it (the plan reads no parser, internal/proxy preferWorkingParser).
//
// The shape the plan skips: only line filters before the first parser; from it on only bare json and logfmt
// parsers (no extraction list, no flag: `| json a="b"` and `| logfmt --strict` keep today's text), in any order
// and number, and drops, plus keeps for a count (a rate behind a keep is grouped by the kept labels, keepsAfterRate).
// Every other stage keeps the translator's text: a label filter, a line filter after a parser (it keeps the line
// set Loki keeps, but was not proven here), a formatting stage, an unwrap, a regexp, pattern or unpack parser (an
// unpack has its own rule, SkipsUnpack), absent_over_time and the bytes functions.

// bareParsersSkipped is the index of the first parser of ra's log query when ra is a count or a rate whose log
// query has the shape a sum that needs no label skips from that parser on, and -1 otherwise.
func bareParsersSkipped(ra *queryplan.RangeAgg) int {
	if ra.In == nil || ra.Op != logql.RangeCountOverTime && ra.Op != logql.RangeRate {
		return -1
	}
	at := -1
	for i, st := range ra.In.Stages {
		switch s := st.(type) {
		case *queryplan.LineFilter:
			if at >= 0 {
				return -1
			}
		case *queryplan.Parse:
			if s.Type != logql.ParserJSON && s.Type != logql.ParserLogfmt || s.Param != "" || s.Strict || s.KeepEmpty || s.Fields != nil {
				return -1
			}
			if at < 0 {
				at = i
			}
		case *queryplan.Drop:
			if at < 0 {
				return -1
			}
		case *queryplan.Keep:
			if at < 0 || ra.Op == logql.RangeRate {
				return -1
			}
		default:
			return -1
		}
	}
	return at
}

// skipsBareParsers reports a node that is a sum that needs no label (sumReadsNoSeries) of a count or a rate whose
// json and logfmt parsers the lowering skips (bareParsersSkipped).
func skipsBareParsers(n queryplan.Node) bool {
	va, ok := n.(*queryplan.VectorAgg)
	if !ok {
		return false
	}
	ra, ok := va.In.(*queryplan.RangeAgg)
	return ok && sumReadsNoSeries(va, ra) && bareParsersSkipped(ra) >= 0
}

// skipsParsers reports a node the lowering writes with no stage from its first parser on: SkipsUnpack's shape, or a
// sum that needs no label over bare json and logfmt parsers (skipsBareParsers).
func skipsParsers(n queryplan.Node) bool {
	return skipsUnpack(n) || skipsBareParsers(n)
}

// SkipsParsers reports a plan whose root is a sum the lowering writes with no stage from its first parser on: an
// unpack (SkipsUnpack), or bare json and logfmt parsers followed by drops (and keeps, for a count). The proxy
// serves it from the plan and sends no parser probe for it (semantics/ungrouped-sum-skips-unpack,
// semantics/ungrouped-sum-skips-parsers).
func SkipsParsers(p *queryplan.Plan) bool {
	return skipsParsers(p.Root)
}

// SkipsParsersWrapped is SkipsUnpackWrapped for SkipsParsers' shape: a plan whose root is a binary operation or a
// label_replace call over such sums only.
func SkipsParsersWrapped(p *queryplan.Plan) bool {
	switch p.Root.(type) {
	case *queryplan.BinOp, *queryplan.LabelReplace:
	default:
		return false
	}
	skips, clean := wrappedSkipOf(p.Root, skipsParsers)
	return skips && clean
}

// WritesSkippedParsers reports a plan with, at any depth, SkipsParsers' shape: the lowering writes it with no stage
// from its first parser on wherever it stands, so its text is not the string translator's there. The shadows hold
// every such plan as its case's.
func WritesSkippedParsers(p *queryplan.Plan) bool {
	return writesSkippedOf(p.Root, skipsParsers)
}
