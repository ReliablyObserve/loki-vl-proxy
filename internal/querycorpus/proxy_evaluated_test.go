package querycorpus_test

import (
	"fmt"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/blankspace"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The proxy evaluates a ranking, a stddev or a stdvar itself: it reads it from the LogQL before it translates
// anything, translates the operand alone and folds each step of the answer. The string translator's text for the
// whole query is not what the proxy sends, so the shadow's reference for these roots is the proxy's route
// (proxy.PostAggregationPhysical): a RankStep or SeriesAgg per level over the operand's operators, and the operand's
// request byte for byte. These rows are counted apart (native_proxy_evaluated in query-plan.json).
//
// A binary operation with an operand that has a without clause is held to the proxy's route as well: the binary
// route reads the query again and evaluates each operand as a query of its own, folded by its own clause
// (proxy.BinaryOperandsPhysical), where the translator's text for the whole query drops the clauses and folds the
// whole operation by the first one (semantics/translator-folds-binary-by-operand-without).

// knownLabelReplaceAroundRanking is the registry case of a label_replace around a ranking, which the proxy does not
// route as a ranking: the string translator's text for it drops the ranking.
const knownLabelReplaceAroundRanking = "semantics/label-replace-around-ranking-and-vector"

// knownNestedEvaluation is the registry case of a stddev, a stdvar or a ranking below the one level the route
// evaluates (or under a level it does not take): the proxy sends the translator's text for the operand, which drops
// the level.
const knownNestedEvaluation = "semantics/stddev-stdvar-nested-or-binary"

// evaluatedRoot reads the plan, not the lowering, for a chain of rankings and vector aggregations that holds a level
// the proxy evaluates (a ranking, stddev or stdvar). routed says the proxy routes the whole chain as one evaluation,
// so the lowering is held to its route. Otherwise divergence names the registry case of the shapes the proxy sends as
// the translator's text, which drops a level (a level below the first one the route evaluates, a level under an
// aggregation or a label_replace the route does not take): the plan is right, the shape is not served from it yet.
// Both are zero for a plan with no such level.
//
// The route (parseInstantMetricPostAggQuery) takes, in this order: a chain of layers sum, avg, min, max, count and
// rankings above the deepest ranking (a stddev or stdvar above it is not taken), the ranking at the top, and a
// stddev or stdvar at the top; whatever lies under the level it takes is translated as text.
func evaluatedRoot(p *queryplan.Plan) (routed bool, divergence string) {
	n := p.Root
	underLabelReplace := false
	for {
		lr, ok := n.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		n = lr.In
		underLabelReplace = true
	}
	var chain []queryplan.Node
	for {
		var in queryplan.Node
		switch v := n.(type) {
		case *queryplan.Rank:
			in = v.In
		case *queryplan.VectorAgg:
			in = v.In
		}
		if in == nil {
			break
		}
		chain, n = append(chain, n), in
	}
	deepest, lastRank := -1, -1
	for i, layer := range chain {
		if evaluatedLayer(layer) {
			deepest = i
		}
		if _, ok := layer.(*queryplan.Rank); ok {
			lastRank = i
		}
	}
	switch {
	case deepest < 0:
		return false, ""
	case underLabelReplace && lastRank >= 0 && !anyStddev(chain):
		// A label_replace around a ranking (and the sum, avg, min, max and count above it) is routed as the ranking,
		// and the calls rewrite its answer.
		return true, ""
	case underLabelReplace && lastRank >= 0:
		// A label_replace around a ranking nested with a stddev or stdvar: the translator's text for it drops a level.
		return false, knownLabelReplaceAroundRanking
	case underLabelReplace:
		return false, knownNestedEvaluation
	}
	top := -1
	switch {
	case lastRank >= 1 && !anyStddev(chain[:lastRank]):
		top = lastRank
	case evaluatedLayer(chain[0]):
		top = 0
	}
	if top == deepest {
		return true, ""
	}
	return false, knownNestedEvaluation
}

// routedRoot is evaluatedRoot, and routed for a binary operation the proxy's binary route evaluates operand by
// operand (lower.RoutedBinary: an operand with a without clause), which the shadows hold to that route
// (checkProxyEvaluated, checkBinaryRoute).
func routedRoot(p *queryplan.Plan) (routed bool, divergence string) {
	if lower.RoutedBinary(p.Root) {
		return true, ""
	}
	return evaluatedRoot(p)
}

// evaluatedLayer reports whether the layer is a ranking, stddev or stdvar.
func evaluatedLayer(n queryplan.Node) bool {
	switch v := n.(type) {
	case *queryplan.Rank:
		return true
	case *queryplan.VectorAgg:
		return v.Op == logql.VectorStddev || v.Op == logql.VectorStdvar
	}
	return false
}

// anyStddev reports whether one of the layers is a stddev or stdvar.
func anyStddev(chain []queryplan.Node) bool {
	for _, n := range chain {
		if v, ok := n.(*queryplan.VectorAgg); ok && (v.Op == logql.VectorStddev || v.Op == logql.VectorStdvar) {
			return true
		}
	}
	return false
}

// operandTranslate is the proxy's translation of a query under the profile (translateQueryOpts): the translator's
// text, with the `* ` a query that starts with a pipe needs in front of it.
func (p planProfile) operandTranslate(q string) (string, error) {
	s, ok := p.translate(q)
	if spaced := blankspace.UnwrapSpaced(q); spaced != q {
		// A blank the translator does not read as a space (an unwrap written `|<TAB>unwrap`, which it rejects or reads
		// as no unwrap): the proxy serves the operand from the plan, which writes the spaced spelling's text
		// (semantics/translator-reads-only-space-between-tokens).
		if respelled, rok := p.translate(spaced); rok {
			s, ok = respelled, true
		}
	}
	if !ok {
		return "", fmt.Errorf("the string translator rejects %q", q)
	}
	if t := strings.TrimSpace(s); strings.HasPrefix(t, "|") {
		s = "* " + t
	}
	// The translator writes a label called not or in bare, which VictoriaLogs rejects; the plan quotes it.
	return lower.QuoteKeywordFieldText(s), nil
}

// routedQuery is the query as the handlers route it: the request's rewrite and validation (handlerText).
func routedQuery(q string) string { return handlerText(q) }

// proxyReference is the proxy's route for q under the profile: the operators, the operand's request, and whether
// the proxy routes the query at all.
func proxyReference(q string, prof planProfile) (want physical.Op, operand string, routed bool, err error) {
	return proxy.PostAggregationPhysical(routedQuery(q), prof.operandTranslate)
}

// checkProxyEvaluated holds the lowering of a ranking to the proxy's route: the same operators (physical.Equal) and
// the operand's request byte for byte. It returns "" or what differs. A binary operation is held to the binary route
// (checkBinaryRoute).
func checkProxyEvaluated(q string, prof planProfile, got physical.Op) string {
	if b, ok := got.(physical.Binary); ok {
		return checkBinaryRoute(q, prof, b)
	}
	want, operand, routed, err := proxyReference(q, prof)
	switch {
	case err != nil:
		return fmt.Sprintf("%s: %q: lowered, the proxy's route fails: %v", prof.name, q, err)
	case !routed:
		return fmt.Sprintf("%s: %q: lowered, the proxy does not route it as one evaluation", prof.name, q)
	case !physical.Equal(want, got):
		return fmt.Sprintf("%s: %q:\n lowered %s %#v\n   proxy %s %#v", prof.name, q, translator.EncodePhysical(got), got, operand, want)
	}
	evaluated := got
	if lm, ok := got.(physical.LabelMap); ok {
		// label_replace calls around the evaluation: the operand's request is the one under them.
		evaluated = lm.In
	}
	if _, in := physical.SplitEval(evaluated); physical.Encode(in) != operand {
		return fmt.Sprintf("%s: %q:\n lowered text %s\n   proxy text %s", prof.name, q, physical.Encode(in), operand)
	}
	return ""
}

// checkBinaryRoute holds the lowering of a binary operation to the proxy's binary route
// (proxy.BinaryOperandLeaves): the same operators (physical.Equal), each query operand's request byte for byte at any
// depth of a chain, and the lowering must not write the translator's text for the whole query, which is not the
// route's reading. It returns "" or what differs.
func checkBinaryRoute(q string, prof planProfile, got physical.Binary) string {
	want, leaves, routed, err := proxy.BinaryOperandLeaves(routedQuery(q), prof.operandTranslate)
	switch {
	case err != nil:
		return fmt.Sprintf("%s: %q: lowered, the proxy's binary route fails: %v", prof.name, q, err)
	case !routed:
		return fmt.Sprintf("%s: %q: lowered, the proxy does not route it as a binary operation", prof.name, q)
	case !physical.Equal(want, got):
		return fmt.Sprintf("%s: %q:\n lowered %s %#v\n   proxy %#v", prof.name, q, translator.EncodePhysical(got), got, want)
	}
	gotLeaves := binaryLeaves(got, nil)
	if len(gotLeaves) != len(leaves) {
		return fmt.Sprintf("%s: %q: %d operands lowered, the route has %d", prof.name, q, len(gotLeaves), len(leaves))
	}
	for i, x := range gotLeaves {
		if leaves[i] != "" && physical.Encode(x) != leaves[i] {
			return fmt.Sprintf("%s: %q: operand %d:\n lowered text %s\n   proxy text %s", prof.name, q, i, physical.Encode(x), leaves[i])
		}
	}
	// The text has no spelling for a bool modifier (the route reads it from the query), so a comparison with bool
	// encodes like the translator's text, which drops it. Otherwise the translator's text for the whole query is not
	// the route's reading, unless it decodes to the route's operators (a sum `without ()` against a number: the route
	// sends the operand alone, which the translator happens to write).
	if legacy, ok := prof.translate(q); ok && physical.Encode(got) == legacy && !holdsBool(got) {
		if lw, err := translator.DecodePhysical(legacy); err != nil || !physical.Equal(lw, want) {
			return fmt.Sprintf("%s: %q: lowered to the translator's text for the whole query, which the route does not read", prof.name, q)
		}
	}
	return ""
}

// holdsBool reports a binary operation with a bool modifier at any depth of b.
func holdsBool(b physical.Binary) bool {
	if b.Bool {
		return true
	}
	for _, x := range [2]physical.Op{b.L, b.R} {
		if nested, ok := x.(physical.Binary); ok && holdsBool(nested) {
			return true
		}
	}
	return false
}

// binaryLeaves appends the operands of b that are no binary operation, left first, at any depth.
func binaryLeaves(b physical.Binary, out []physical.Op) []physical.Op {
	for _, x := range [2]physical.Op{b.L, b.R} {
		if nested, ok := x.(physical.Binary); ok {
			out = binaryLeaves(nested, out)
			continue
		}
		out = append(out, x)
	}
	return out
}
