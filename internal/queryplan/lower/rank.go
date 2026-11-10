package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// rank lowers topk, bottomk, sort and sort_desc to physical.RankStep levels over the query the proxy sends for their
// operand.
//
// The proxy evaluates a ranking itself: it reads the ranking from the LogQL before it translates anything
// (parseInstantMetricPostAggQuery), translates the operand alone, and ranks each step of the answer, k series per
// group of the clause. The string translator has no ranking text (no marker, no pipe, k is never written) and its
// text for the whole query is not what the proxy sends, so the reference of these roots is the proxy's route
// (proxy.PostAggregationPhysical), not the translator's text for the whole query: a RankStep per ranking, the
// innermost first, over the operand's own operators, which encode as the request the proxy sends. A ranking over
// a ranking is a RankStep over a RankStep, the route reading every ranking down to the operand.
//
// The operand is lowered as a query of its own, the way the translator translates the text the proxy slices:
//   - a range aggregation (rangeAgg), with or without an unwrapped value: the proxy sends the operand alone, which
//     groups an unwrap by nothing where the translator's text for the whole ranking groups it by the stream;
//   - a vector aggregation (vectorAgg, a nest included), as it stands alone;
//   - a label_replace around either (labelReplaceOperand);
//
// A shape the route does not evaluate the way a RankStep says is left to the string translator under a named
// construct:
//   - a binary operation operand (Rank operand BinOp: the proxy routes it through the binary evaluation);
//   - any other operand (a log query, vector(), a number): Rank operand <construct>;
//   - a ranking inside a label_replace or an aggregation that sits under a ranking (the route evaluates one chain
//     of rankings over the operand it slices, and translates what is left as text, which drops a ranking there);
//   - sort and sort_desc with a clause, which the route does not take;
//   - everything the operand's own lowering leaves to the translator keeps its name (an operand that does not lower
//     alone does not lower under a ranking);
//   - a spelling the route reads unlike the plan (screenRank).
func rank(n *queryplan.Rank, prof Profile) (physical.Op, error) {
	var buf [6]queryplan.Node
	chain, operand := rankChain(n, buf[:0])
	for _, layer := range chain {
		if err := rankLayerError(layer); err != nil {
			return nil, err
		}
	}
	if len(chain) == 1 && sortCostsMore(chain[0].(*queryplan.Rank), operand) {
		return nil, errRankPattern
	}
	op, err := rankOperand(operand, prof)
	if err != nil {
		return nil, err
	}
	for i := len(chain) - 1; i >= 0; i-- {
		op = rankLayer(chain[i], op)
	}
	return op, nil
}

// sortCostsMore reports whether the lowering of a lone sort or sort_desc over x allocates more than the proxy's route
// for it (measured over the grids, TestRankLoweringAllocatesNoMoreThanTheProxyRoute): the route reads a sort by its
// prefix with one parse, so it costs the least of the rankings, and a count, bytes or absent_over_time with a
// pattern line filter (|> or !>) is the range aggregation whose lowering is closest to the translator's own cost.
// The proxy's route is the comparator, so the shape is left to the translator rather than written with more
// allocations than it makes.
func sortCostsMore(r *queryplan.Rank, x queryplan.Node) bool {
	ra, ok := x.(*queryplan.RangeAgg)
	if !ok || r.Op != logql.VectorSort && r.Op != logql.VectorSortDesc || ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate {
		return false
	}
	for _, st := range ra.In.Stages {
		if lf, ok := st.(*queryplan.LineFilter); ok && (lf.Op == logql.LineFilterContainsPat || lf.Op == logql.LineFilterExcludePat) {
			return true
		}
	}
	return false
}

// rankChain is the chain of rankings and vector aggregations from n down to its deepest ranking, outermost first
// (appended to dst), and the operand under that ranking. The route reads the chain as one evaluation: every layer
// above the deepest ranking is evaluated by the proxy over what the ranking keeps, and the operand is the query it
// translates (parseAggregationOverRanking).
func rankChain(n *queryplan.Rank, dst []queryplan.Node) (chain []queryplan.Node, operand queryplan.Node) {
	deepest := 0
	var x queryplan.Node = n
	for {
		var next queryplan.Node
		switch v := x.(type) {
		case *queryplan.Rank:
			deepest, next = len(dst), v.In
		case *queryplan.VectorAgg:
			next = v.In
		}
		if next == nil {
			break
		}
		dst, x = append(dst, x), next
	}
	chain = dst[:deepest+1]
	return chain, chain[deepest].(*queryplan.Rank).In
}

// rankLayerError is the NotLoweredError of a layer the route does not evaluate as a RankStep or a SeriesAgg: the
// route reads sort and sort_desc by their prefix, which a clause does not fit, and the aggregations sum, avg, min,
// max and count above a ranking (stddev and stdvar keep their own route, which takes no ranking under it). The
// parser takes no k below 1, which the route does not take either.
func rankLayerError(layer queryplan.Node) error {
	switch v := layer.(type) {
	case *queryplan.Rank:
		if (v.Op == logql.VectorSort || v.Op == logql.VectorSortDesc) && v.By != nil {
			return errRankGrouping
		}
	case *queryplan.VectorAgg:
		switch v.Op {
		case logql.VectorSum, logql.VectorAvg, logql.VectorMin, logql.VectorMax, logql.VectorCount:
		default:
			return &NotLoweredError{Construct: "VectorAgg " + string(v.Op)}
		}
	}
	return nil
}

// rankLayer is the level the proxy evaluates for a layer of the chain, over the operators of the layer below.
func rankLayer(layer queryplan.Node, in physical.Op) physical.Op {
	switch v := layer.(type) {
	case *queryplan.Rank:
		return physical.RankStep{Op: string(v.Op), K: v.K, By: clause(v.By), In: in}
	case *queryplan.VectorAgg:
		return physical.SeriesAgg{Op: string(v.Op), By: clause(v.By), In: in}
	}
	return in
}

// clause is the clause of a level as physical operators hold it; nil is no clause.
func clause(g *queryplan.Grouping) *physical.Grouping {
	if g == nil {
		return nil
	}
	return &physical.Grouping{Labels: g.Labels, Without: g.Without}
}

// rankOperand lowers the operand of a ranking: the query the proxy translates for it.
func rankOperand(x queryplan.Node, prof Profile) (physical.Op, error) {
	switch v := x.(type) {
	case *queryplan.RangeAgg:
		op, err := rangeAgg(v, prof)
		return op, operandError(err, errRankOperandRange)
	case *queryplan.VectorAgg:
		return vectorAgg(v, prof)
	case *queryplan.LabelReplace:
		return labelReplaceOver(v, prof, false)
	case *queryplan.BinOp:
		return nil, errRankBinOp
	}
	return nil, &NotLoweredError{Construct: "Rank operand " + Construct(x)}
}

var (
	errRankGrouping     = &NotLoweredError{Construct: "Rank grouping"}
	errRankBinOp        = &NotLoweredError{Construct: "Rank operand BinOp"}
	errRankPattern      = &NotLoweredError{Construct: "Rank operand pattern"}
	errRankOperandRange = &NotLoweredError{Construct: "Rank operand RangeAgg"}
	errRankSpelling     = &NotLoweredError{Construct: "Rank spelling"}
)

// screenRank turns away a ranking the proxy's route does not read the way the plan does, which holds no text. The
// route slices the operand out of the query as written (rankOperandSpan) and translates that text: a lone ranking
// wrapped whole in parentheses, or a sort spelled with a blank before its parenthesis, is not routed and is
// translated whole (Rank spelling), and the operand it slices is translated by the string translator, which reads
// blanks and parentheses around the operand's own calls unlike the plan (ScreenText on the operand, as the proxy
// slices it). A chain of rankings wrapped whole in parentheses is routed, and lowers.
func screenRank(query string, n *queryplan.Rank) error {
	var buf [6]queryplan.Node
	chain, operand := rankChain(n, buf[:0])
	for _, layer := range chain {
		if r, ok := layer.(*queryplan.Rank); ok && r.By != nil && (r.Op == logql.VectorSort || r.Op == logql.VectorSortDesc) {
			return errRankGrouping
		}
	}
	text, ok := rankOperandSpan(query, chain)
	if !ok {
		return errRankSpelling
	}
	// The translator reads a blank between a range function and its parenthesis as text, which a range aggregation
	// screened on its own does not turn away (nor one in the first argument of a label_replace).
	if paddedRangeCall(text, operand) {
		return errRankSpelling
	}
	// A plan of the operand alone, which the screen reads and does not keep.
	alone := queryplan.Plan{Root: operand}
	return ScreenText(text, &alone)
}

// paddedRangeCall reports whether the operand text x is a range aggregation (or label_replace calls around one)
// spelled with a blank, or parentheses, before the parenthesis of the range function's call.
func paddedRangeCall(text string, x queryplan.Node) bool {
	for {
		lr, ok := x.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		arg, found := labelReplaceArgument(text)
		if !found {
			return false
		}
		text, x = arg, lr.In
	}
	_, isRange := x.(*queryplan.RangeAgg)
	return isRange && paddedCall(text)
}

// rankOperandSpan is the text of the operand the proxy translates for the chain of rankings and aggregations
// (rankChain), sliced from the query the way parseInstantMetricPostAggQuery slices it. ok is false when the proxy
// does not route the query as a ranking (the string translator then translates the whole of it). The query is the
// text the handlers route: keywords in lower case, comments and logfmt flags removed (lowercaseQueryKeywords).
//
// A chain of several layers is one route (parseAggregationOverRanking): the layers above the deepest ranking are
// peeled with their arguments and parentheses, and the deepest gives the operand. A lone ranking is read by the
// prefix of sort and sort_desc, and by the arguments of topk and bottomk.
func rankOperandSpan(query string, chain []queryplan.Node) (operand string, ok bool) {
	text := strings.TrimSpace(logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(query))))
	deepest := chain[len(chain)-1].(*queryplan.Rank)
	if len(chain) == 1 {
		operand, ok = loneRankOperand(text, deepest)
		return operand, ok && operand != ""
	}
	text = logql.UnwrapParens(text)
	for _, layer := range chain[:len(chain)-1] {
		var name string
		switch v := layer.(type) {
		case *queryplan.Rank:
			name = string(v.Op)
		case *queryplan.VectorAgg:
			name = string(v.Op)
		}
		args, found := logql.AggregationArgs(text, name)
		if !found {
			return "", false
		}
		if name == "topk" || name == "bottomk" {
			if comma := logql.TopLevelCommaIndex(args); comma > 0 {
				args = args[comma+1:]
			}
		}
		text = logql.UnwrapParens(args)
	}
	operand, ok = rankArguments(text, deepest)
	return operand, ok && operand != ""
}

// loneRankOperand is the operand the route slices from a ranking that is not under another one.
func loneRankOperand(text string, r *queryplan.Rank) (string, bool) {
	if r.Op == logql.VectorSort || r.Op == logql.VectorSortDesc {
		prefix := string(r.Op) + "("
		if !strings.HasPrefix(text, prefix) || !strings.HasSuffix(text, ")") {
			return "", false
		}
		return strings.TrimSpace(text[len(prefix) : len(text)-1]), true
	}
	return rankArguments(text, r)
}

// rankArguments is the operand of the ranking r written at the start of text: the arguments of the name, without
// the k of a topk or bottomk.
func rankArguments(text string, r *queryplan.Rank) (string, bool) {
	args, ok := logql.AggregationArgs(text, string(r.Op))
	if !ok {
		return "", false
	}
	if r.Op == logql.VectorSort || r.Op == logql.VectorSortDesc {
		return strings.TrimSpace(args), true
	}
	comma := logql.TopLevelCommaIndex(args)
	if comma <= 0 {
		return "", false
	}
	return strings.TrimSpace(args[comma+1:]), true
}

// paddedCall reports whether the operand text is parenthesized, or has a blank between its function name and the
// call's parenthesis: the shapes the translator reads as text.
func paddedCall(rest string) bool {
	j := 0
	for j < len(rest) && isBlank(rest[j]) {
		j++
	}
	if j < len(rest) && rest[j] == '(' {
		return true
	}
	k := j
	for k < len(rest) && isNameByte(rest[k]) {
		k++
	}
	l := k
	for l < len(rest) && isBlank(rest[l]) {
		l++
	}
	return k > j && l > k && l < len(rest) && rest[l] == '('
}

// RankOperand is the text of the operand the proxy translates for the ranking at the top of p, sliced from query the
// way the proxy slices it (rankOperandSpan), and false when p is no ranking or the proxy does not route the query as
// one. The tests that hold the slice to the proxy's own call it.
func RankOperand(query string, p *queryplan.Plan) (string, bool) {
	n, ok := p.Root.(*queryplan.Rank)
	if !ok {
		return "", false
	}
	var buf [6]queryplan.Node
	chain, _ := rankChain(n, buf[:0])
	return rankOperandSpan(query, chain)
}
