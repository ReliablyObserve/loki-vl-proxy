package querycorpus_test

import (
	"fmt"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The proxy evaluates a ranking, a stddev or a stdvar itself: it reads it from the LogQL before it translates
// anything, translates the operand alone and folds each step of the answer. The string translator's text for the
// whole query is not what the proxy sends, so the shadow's reference for these roots is the proxy's route
// (proxy.PostAggregationPhysical): a RankStep or SeriesAgg per level over the operand's operators, and the operand's
// request byte for byte. These rows are counted apart (native_proxy_evaluated in query-plan.json).

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
	case underLabelReplace && lastRank >= 0:
		// The proxy does not route a label_replace around a ranking as one, and the translator's text for it drops
		// the ranking.
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
	if !ok {
		return "", fmt.Errorf("the string translator rejects %q", q)
	}
	if t := strings.TrimSpace(s); strings.HasPrefix(t, "|") {
		s = "* " + t
	}
	// The translator writes a label called not or in bare, which VictoriaLogs rejects; the plan quotes it.
	return lower.QuoteKeywordFieldText(s), nil
}

// routedQuery is the query as the handlers route it: keywords in lower case, comments and logfmt flags removed
// (lowercaseQueryKeywords).
func routedQuery(q string) string {
	return logql.LowercaseKeywords(logql.StripParserFlags(logql.StripComments(q)))
}

// proxyReference is the proxy's route for q under the profile: the operators, the operand's request, and whether
// the proxy routes the query at all.
func proxyReference(q string, prof planProfile) (want physical.Op, operand string, routed bool, err error) {
	return proxy.PostAggregationPhysical(routedQuery(q), prof.operandTranslate)
}

// checkProxyEvaluated holds the lowering of a ranking to the proxy's route: the same operators (physical.Equal) and
// the operand's request byte for byte. It returns "" or what differs.
func checkProxyEvaluated(q string, prof planProfile, got physical.Op) string {
	want, operand, routed, err := proxyReference(q, prof)
	switch {
	case err != nil:
		return fmt.Sprintf("%s: %q: lowered, the proxy's route fails: %v", prof.name, q, err)
	case !routed:
		return fmt.Sprintf("%s: %q: lowered, the proxy does not route it as one evaluation", prof.name, q)
	case !physical.Equal(want, got):
		return fmt.Sprintf("%s: %q:\n lowered %s %#v\n   proxy %s %#v", prof.name, q, translator.EncodePhysical(got), got, operand, want)
	}
	if _, in := physical.SplitEval(got); physical.Encode(in) != operand {
		return fmt.Sprintf("%s: %q:\n lowered text %s\n   proxy text %s", prof.name, q, physical.Encode(in), operand)
	}
	return ""
}
