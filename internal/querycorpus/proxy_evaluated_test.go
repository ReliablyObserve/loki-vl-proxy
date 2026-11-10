package querycorpus_test

import (
	"fmt"
	"strings"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The proxy evaluates a ranking itself: it reads it from the LogQL before it translates anything, translates the
// operand alone and ranks each step of the answer. The string translator's text for the whole query is not what the
// proxy sends, so the shadow's reference for these roots is the proxy's route (proxy.PostAggregationPhysical): a
// RankStep per ranking over the operand's operators, and the operand's request byte for byte. These rows are counted
// apart (native_proxy_evaluated in query-plan.json).

// knownLabelReplaceAroundRanking is the registry case of a label_replace around a ranking, which the proxy does not
// route as a ranking: the string translator's text for it drops the ranking.
const knownLabelReplaceAroundRanking = "semantics/label-replace-around-ranking-and-vector"

// evaluatedRoot reports whether the plan's root is a ranking the proxy evaluates (rank), or a label_replace around
// one (underLabelReplace), whose ranking the proxy does not route and whose reference text is the translator's
// wrong one.
func evaluatedRoot(p *queryplan.Plan) (rank, underLabelReplace bool) {
	n := p.Root
	for {
		lr, ok := n.(*queryplan.LabelReplace)
		if !ok {
			break
		}
		n = lr.In
		underLabelReplace = true
	}
	_, isRank := n.(*queryplan.Rank)
	if !underLabelReplace {
		return isRank, false
	}
	return false, isRank
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
	return s, nil
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
		return fmt.Sprintf("%s: %q: lowered, the proxy does not route it as a ranking", prof.name, q)
	case !physical.Equal(want, got):
		return fmt.Sprintf("%s: %q:\n lowered %s %#v\n   proxy %s %#v", prof.name, q, translator.EncodePhysical(got), got, operand, want)
	}
	if _, in := physical.SplitEval(got); physical.Encode(in) != operand {
		return fmt.Sprintf("%s: %q:\n lowered text %s\n   proxy text %s", prof.name, q, physical.Encode(in), operand)
	}
	return ""
}
