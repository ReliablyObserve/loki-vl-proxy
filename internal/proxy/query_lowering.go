package proxy

import (
	"context"
	"slices"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The first construct served from the typed query-plan lowering instead of the string translator: a log query whose
// stages are line filters (including pattern filters) and label filters with no parser (lower.Plan answers
// LogSelect natively for exactly these). Every other query, and every query the lowering answers with an error,
// is translated by the string translator as before, so nothing is lost by a lowering that does not cover it.
//
// The lowering writes the text the string translator writes, byte for byte, on every query the lowering covers
// under every translator profile (internal/querycorpus TestPlanShadow asserts physical.Encode(lower.Plan(...)) is
// the translator's text), so the translation cache, the markers and the handlers see no difference.
// -query-plan-lowering=false turns it off.

// QueryPlanFlippedConstructs are the plan constructs (lower.Construct) the proxy serves from the typed lowering:
// a query whose plan is one of them and which lower.Plan lowers is translated by it instead of the string
// translator. The corpus shadow requires each to be lowered natively, equal to the translator, byte for byte, and
// records the list in conformance/registry/query-plan.json; widening it is how a construct is flipped.
var QueryPlanFlippedConstructs = []string{"LogSelect"}

// loweringProfile is the lowering's view of the options the string translator is given: the one place the
// translator's options become a lower.Profile, shared by the flip and the live shadow.
func loweringProfile(label translator.LabelTranslateFunc, streamFields map[string]bool, caps logsql.Capabilities, keepLine bool) lower.Profile {
	return lower.Profile{Caps: caps, Label: label, StreamFields: streamFields, KeepLine: keepLine}
}

// loweringPlan is the typed pipeline up to the lowering for a canonical query (logql.CanonicalizeLogRanges):
// parse, build the logical plan, resolve it. It is the one place the lowering parses a query, for the flip and
// for the live shadow (which has no request and passes noFlowCtx), and the parse the request makes is counted in
// its flow like any other. It leaves the plan memo of the handlers' reads (resolvedPlan) alone: the plan of a
// translation is not one the handlers read, and storing it would cost the allocations of a memo entry.
func loweringPlan(ctx context.Context, canon string) (*queryplan.Plan, error) {
	expr, err := parseLogQLExpr(ctx, canon)
	if err != nil {
		return nil, err
	}
	plan, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil, err
	}
	queryplan.Resolve(plan)
	return plan, nil
}

// lowerLogSelect translates a canonical client query (logql.CanonicalizeLogRanges) with the typed lowering and
// reports whether it did: false for a query the lowering does not cover, which the string translator then
// translates (and rejects with its own error, if it does). The decision is cheap and made before any plan is
// built: a query that does not start with a stream selector is a metric query (the other half of the traffic),
// and one with a parser or formatting stage (logql.MayHaveNonFilterStage) is not covered, so neither is parsed.
// The lowering itself has the last word: it answers an error for anything it does not write.
func (p *Proxy) lowerLogSelect(ctx context.Context, canon string, prof lower.Profile) (text string, ok bool) {
	if canon == "" || canon[0] != '{' || logqlpkg.MayHaveNonFilterStage(canon) {
		return "", false
	}
	defer func() {
		if r := recover(); r != nil {
			// The panic value can hold query text, so it is not logged: the string translator answers instead.
			p.recordLoweringShadow(shadowPanic, "LogSelect")
			text, ok = "", false
		}
	}()
	plan, err := loweringPlan(ctx, canon)
	if err != nil {
		return "", false
	}
	construct := lower.Construct(plan.Root)
	if !slices.Contains(QueryPlanFlippedConstructs, construct) {
		return "", false
	}
	op, err := lower.Plan(plan, prof)
	if err != nil {
		return "", false
	}
	// The corpus capture records the queries a translation saw (a no-op without the corpuscapture build tag); a
	// query the lowering answered is one of them.
	querycorpus.Record(querycorpus.LogQLTranslate, canon)
	p.recordLoweringShadow(shadowFlipped, construct)
	return physical.Encode(op), true
}
