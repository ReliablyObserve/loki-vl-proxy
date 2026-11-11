package lower

import (
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// evaluated lowers the levels the proxy evaluates itself: topk, bottomk, sort and sort_desc (physical.RankStep), and
// stddev and stdvar, with the sum, avg, min, max and count layers above them (physical.SeriesAgg), over the query the
// proxy sends for their operand.
//
// The proxy reads these from the LogQL before it translates anything (parseInstantMetricPostAggQuery), translates
// the operand alone, and folds each step of the answer: k series per group of the clause for a ranking, the
// population standard deviation or variance of each group's series for stddev and stdvar. The string translator has
// no text for either (no marker, no pipe) and its text for the whole query is not what the proxy sends, so the
// reference of these roots is the proxy's route (proxy.PostAggregationPhysical), not the translator's text for the
// whole query: one level per layer, the innermost first, over the operand's own operators, which encode as the
// request the proxy sends. A level over a level is a level over a level, the route reading every ranking down to
// the operand.
//
// The operand is lowered as a query of its own, the way the translator translates the text the proxy slices:
//   - a range aggregation (rangeAgg), with or without an unwrapped value: the proxy sends the operand alone, which
//     groups an unwrap by nothing where the translator's text for the whole ranking groups it by the stream;
//   - a vector aggregation (vectorAgg, a nest included), as it stands alone;
//   - a label_replace around either (labelReplaceOperand);
//
// A level below the first one the route evaluates (a stddev under a ranking, a ranking under a stddev, a stddev
// under a sum) is lowered to the chain Loki evaluates, which the proxy does not run: it translates that text, and
// the translator drops the level. The shadow records the shape as a known divergence until the plan serves it.
//
// A shape the route does not evaluate the way a level says is left to the string translator under a named
// construct:
//   - a binary operation operand (Rank operand BinOp, SeriesAgg operand BinOp: the proxy routes it through the
//     binary evaluation);
//   - any other operand (a log query, vector(), a number): Rank operand <construct>, SeriesAgg operand <construct>;
//   - a ranking or stddev inside a label_replace that sits under a level (the route translates that text, which drops
//     the level there);
//   - sort and sort_desc with a clause, which the route does not take;
//   - a stddev or stdvar by a label of a query with a parser (VectorAgg stddev parsed label), whose operand the
//     route rewrites as the series-level form of max by;
//   - everything the operand's own lowering leaves to the translator keeps its name (an operand that does not lower
//     alone does not lower under a level);
//   - a spelling the route reads unlike the plan (screenEvaluated).
func evaluated(n queryplan.Node, prof Profile) (physical.Op, error) {
	var buf [6]queryplan.Node
	chain, operand := evalChain(n, buf[:0])
	for _, layer := range chain {
		if err := rankLayerError(layer); err != nil {
			return nil, err
		}
	}
	if r, ok := chain[0].(*queryplan.Rank); ok && len(chain) == 1 && sortCostsMore(r, operand) {
		return nil, errRankPattern
	}
	if parsedLabelFold(chain[0]) && hasParserStage(operand) {
		return nil, errSeriesParsedLabel
	}
	op, err := evaluatedOperand(operand, prof, chain[len(chain)-1])
	if err != nil {
		return nil, err
	}
	for i := len(chain) - 1; i >= 0; i-- {
		op = evaluatedLevel(chain[i], op)
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

// evalChain is the chain of rankings and vector aggregations from n down to its deepest level the proxy evaluates (a
// ranking, stddev or stdvar), outermost first (appended to dst), and the operand under that level. The route reads
// the chain as one evaluation: every layer above the deepest level is evaluated by the proxy over what the level
// below keeps, and the operand is the query it translates (parseAggregationOverRanking, parseStddevStdvarQuery).
// n holds such a level (evaluates).
func evalChain(n queryplan.Node, dst []queryplan.Node) (chain []queryplan.Node, operand queryplan.Node) {
	deepest := 0
	x := n
	for {
		next := levelInput(x)
		if next == nil {
			break
		}
		if isEvaluated(x) {
			deepest = len(dst)
		}
		dst, x = append(dst, x), next
	}
	chain = dst[:deepest+1]
	return chain, levelInput(chain[deepest])
}

// levelInput is what a ranking or a vector aggregation reads, nil for any other node.
func levelInput(x queryplan.Node) queryplan.Node {
	switch v := x.(type) {
	case *queryplan.Rank:
		return v.In
	case *queryplan.VectorAgg:
		return v.In
	}
	return nil
}

// isEvaluated reports whether the proxy evaluates the node itself: a ranking, stddev or stdvar.
func isEvaluated(x queryplan.Node) bool {
	switch v := x.(type) {
	case *queryplan.Rank:
		return true
	case *queryplan.VectorAgg:
		return v.Op == logql.VectorStddev || v.Op == logql.VectorStdvar
	}
	return false
}

// evaluates reports whether a chain of rankings and vector aggregations from n holds a level the proxy evaluates.
func evaluates(n queryplan.Node) bool {
	for n != nil {
		if isEvaluated(n) {
			return true
		}
		n = levelInput(n)
	}
	return false
}

// rankLayerError is the NotLoweredError of a layer the route does not evaluate as a RankStep or a SeriesAgg: the
// route reads sort and sort_desc by their prefix, which a clause does not fit. The parser takes no k below 1, which
// the route does not take either.
func rankLayerError(layer queryplan.Node) error {
	switch v := layer.(type) {
	case *queryplan.Rank:
		if (v.Op == logql.VectorSort || v.Op == logql.VectorSortDesc) && v.By != nil {
			return errRankGrouping
		}
	case *queryplan.VectorAgg:
		switch v.Op {
		case logql.VectorSum, logql.VectorAvg, logql.VectorMin, logql.VectorMax, logql.VectorCount, logql.VectorStddev, logql.VectorStdvar:
		default:
			return &NotLoweredError{Construct: "VectorAgg " + string(v.Op)}
		}
	}
	return nil
}

// evaluatedLevel is the level the proxy evaluates for a layer of the chain, over the operators of the layer below.
func evaluatedLevel(layer queryplan.Node, in physical.Op) physical.Op {
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

// evaluatedOperand lowers the operand of the deepest level (top) of a chain: the query the proxy translates for it.
func evaluatedOperand(x queryplan.Node, prof Profile, top queryplan.Node) (physical.Op, error) {
	prefix, binary, ranged := "Rank", errRankBinOp, errRankOperandRange
	if _, ranks := top.(*queryplan.Rank); !ranks {
		prefix, binary, ranged = "SeriesAgg", errSeriesBinOp, errSeriesOperandRange
	}
	switch v := x.(type) {
	case *queryplan.RangeAgg:
		op, err := rangeAgg(v, prof)
		return op, operandError(err, ranged)
	case *queryplan.VectorAgg:
		return vectorAgg(v, prof)
	case *queryplan.LabelReplace:
		return labelReplaceOver(v, prof, false)
	case *queryplan.BinOp:
		return nil, binary
	}
	return nil, &NotLoweredError{Construct: prefix + " operand " + Construct(x)}
}

var (
	errRankGrouping       = &NotLoweredError{Construct: "Rank grouping"}
	errRankBinOp          = &NotLoweredError{Construct: "Rank operand BinOp"}
	errRankPattern        = &NotLoweredError{Construct: "Rank operand pattern"}
	errRankOperandRange   = &NotLoweredError{Construct: "Rank operand RangeAgg"}
	errRankSpelling       = &NotLoweredError{Construct: "Rank spelling"}
	errSeriesBinOp        = &NotLoweredError{Construct: "SeriesAgg operand BinOp"}
	errSeriesOperandRange = &NotLoweredError{Construct: "SeriesAgg operand RangeAgg"}
	errSeriesSpelling     = &NotLoweredError{Construct: "SeriesAgg spelling"}
	errSeriesParsedLabel  = &NotLoweredError{Construct: "VectorAgg stddev parsed label"}
)

// parsedLabelFold reports whether the layer is a stddev or stdvar by labels (not without): the shape whose operand
// the route rewrites when the query has a parser, the labels may then be parsed ones (translatePostAggOperand).
func parsedLabelFold(layer queryplan.Node) bool {
	va, ok := layer.(*queryplan.VectorAgg)
	return ok && (va.Op == logql.VectorStddev || va.Op == logql.VectorStdvar) && va.By != nil && !va.By.Without && len(va.By.Labels) > 0
}

// hasParserStage reports whether a json, logfmt, regexp, pattern or unpack stage stands anywhere under x.
func hasParserStage(x queryplan.Node) bool {
	switch v := x.(type) {
	case *queryplan.LogSelect:
		return v != nil && v.Sem != nil && v.Sem.Parsers > 0
	case *queryplan.RangeAgg:
		return hasParserStage(v.In)
	case *queryplan.VectorAgg:
		return hasParserStage(v.In)
	case *queryplan.Rank:
		return hasParserStage(v.In)
	case *queryplan.LabelReplace:
		return hasParserStage(v.In)
	case *queryplan.BinOp:
		return hasParserStage(v.L) || hasParserStage(v.R)
	}
	return false
}

// screenEvaluated turns away a chain of levels the proxy's route does not read the way the plan does, which holds no
// text. The route slices the operand out of the query as written (evaluatedOperandSpan) and translates that text: a
// lone ranking wrapped whole in parentheses, or a sort spelled with a blank before its parenthesis, is not routed and
// is translated whole (Rank spelling), a stddev or stdvar by a label of an operand that has a parser is rewritten
// (VectorAgg stddev parsed label), and the operand it slices is translated by the string translator, which reads
// blanks and parentheses around the operand's own calls unlike the plan (ScreenText on the operand, as the proxy
// slices it). A chain wrapped whole in parentheses is routed, and lowers.
func screenEvaluated(query string, n queryplan.Node) error {
	var buf [6]queryplan.Node
	chain, operand := evalChain(n, buf[:0])
	for _, layer := range chain {
		if r, ok := layer.(*queryplan.Rank); ok && r.By != nil && (r.Op == logql.VectorSort || r.Op == logql.VectorSortDesc) {
			return errRankGrouping
		}
	}
	text, below, ok := evaluatedOperandSpan(query, chain)
	if !ok {
		if _, ranks := chain[len(chain)-1].(*queryplan.Rank); ranks {
			return errRankSpelling
		}
		return errSeriesSpelling
	}
	// A level below the one a lone route evaluates, behind parentheses: the route sends the translator's text for the
	// operand, which reads the parenthesized text as a phrase and costs less than any evaluation of the chain.
	if len(chain) > 1 && isEvaluated(chain[0]) && loneRoute(chain) && wrappedWhole(below) {
		if _, ranks := chain[len(chain)-1].(*queryplan.Rank); ranks {
			return errRankSpelling
		}
		return errSeriesSpelling
	}
	// The route reads the text under the first layer for a parser, quoted text included.
	if parsedLabelFold(chain[0]) && logql.ParserStageTextRE.MatchString(below) {
		return errSeriesParsedLabel
	}
	// The translator reads a blank between a range function and its parenthesis as text, which a range aggregation
	// screened on its own does not turn away (nor one in the first argument of a label_replace).
	if paddedRangeCall(text, operand) {
		if _, ranks := chain[len(chain)-1].(*queryplan.Rank); ranks {
			return errRankSpelling
		}
		return errSeriesSpelling
	}
	// A plan of the operand alone, which the screen reads and does not keep.
	alone := queryplan.Plan{Root: operand}
	return ScreenText(text, &alone)
}

// loneRoute reports whether the route evaluates only the first layer of the chain and translates everything below it
// as text: no ranking sits under a layer chain it takes (parseAggregationOverRanking: a ranking below sum, avg, min,
// max, count and rankings, none of them a stddev or stdvar).
func loneRoute(chain []queryplan.Node) bool {
	lastRank := -1
	for i, layer := range chain {
		if _, ok := layer.(*queryplan.Rank); ok {
			lastRank = i
		}
	}
	if lastRank < 1 {
		return true
	}
	for _, layer := range chain[:lastRank] {
		if va, ok := layer.(*queryplan.VectorAgg); ok && (va.Op == logql.VectorStddev || va.Op == logql.VectorStdvar) {
			return true
		}
	}
	return false
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

// evaluatedOperandSpan is the text of the operand the proxy translates for the chain of levels (evalChain), sliced
// from the query the way the proxy's route slices it, and the text under the chain's first layer (the one the route
// reads for a parser). ok is false when the proxy does not route the query so (the string translator then translates
// the whole of it). The query is the text the handlers route: keywords in lower case, comments and logfmt flags
// removed (lowercaseQueryKeywords).
//
// A chain of several layers is one route (parseAggregationOverRanking): the layers above the deepest ranking are
// peeled with their arguments and parentheses, and the deepest gives the operand. A lone ranking is read by the
// prefix of sort and sort_desc, and by the arguments of topk and bottomk; a lone stddev or stdvar by
// logql.SeriesFoldOperand.
func evaluatedOperandSpan(query string, chain []queryplan.Node) (operand, below string, ok bool) {
	text := strings.TrimSpace(logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(query))))
	deepest := chain[len(chain)-1]
	if len(chain) == 1 {
		switch v := deepest.(type) {
		case *queryplan.Rank:
			operand, ok = loneRankOperand(text, v)
		case *queryplan.VectorAgg:
			operand, ok = logql.SeriesFoldOperand(text, string(v.Op))
		}
		return operand, operand, ok && operand != ""
	}
	text = logql.UnwrapParens(text)
	for i, layer := range chain[:len(chain)-1] {
		args, found := levelArguments(text, layer)
		if !found {
			return "", "", false
		}
		if i == 0 {
			below = strings.TrimSpace(args)
		}
		text = logql.UnwrapParens(args)
	}
	operand, ok = levelArguments(text, deepest)
	return strings.TrimSpace(operand), below, ok && strings.TrimSpace(operand) != ""
}

// levelName is the name a ranking or vector aggregation is written with.
func levelName(layer queryplan.Node) string {
	switch v := layer.(type) {
	case *queryplan.Rank:
		return string(v.Op)
	case *queryplan.VectorAgg:
		return string(v.Op)
	}
	return ""
}

// levelArguments is the operand of the level written at the start of text: the arguments of its name, without the k
// of a topk or bottomk.
func levelArguments(text string, layer queryplan.Node) (string, bool) {
	name := levelName(layer)
	args, ok := logql.AggregationArgs(text, name)
	if !ok {
		return "", false
	}
	if name == "topk" || name == "bottomk" {
		comma := logql.TopLevelCommaIndex(args)
		if comma <= 0 {
			return "", false
		}
		args = args[comma+1:]
	}
	return args, true
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
	args, ok := levelArguments(text, r)
	return strings.TrimSpace(args), ok
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

// EvaluatedOperand is the text of the operand the proxy translates for the levels it evaluates at the top of p (a
// ranking, stddev or stdvar, with the layers around them), sliced from query the way the proxy slices it
// (evaluatedOperandSpan), and false when p holds no such level at its top or the proxy does not route the query so.
// The tests that hold the slice to the proxy's own call it.
func EvaluatedOperand(query string, p *queryplan.Plan) (string, bool) {
	if !evaluates(p.Root) {
		return "", false
	}
	var buf [6]queryplan.Node
	chain, _ := evalChain(p.Root, buf[:0])
	operand, _, ok := evaluatedOperandSpan(query, chain)
	return operand, ok
}
