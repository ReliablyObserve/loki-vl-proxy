package proxy

import (
	"context"
	"slices"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The constructs served from the typed query-plan lowering instead of the string translator. A log query whose
// stages are line filters (including pattern filters) and label filters with no parser (lower.Plan answers
// LogSelect natively for exactly these), and a metric query whose plan is one of the flipped metric constructs and
// whose lowered text is the string translator's (writesTranslatorText). Every other query, and every query the
// lowering answers with an error, is translated by the string translator as before, so nothing is lost by a
// lowering that does not cover it.
//
// The lowering writes the text the string translator writes, byte for byte, on every query the lowering covers
// under every translator profile (internal/querycorpus TestPlanShadow asserts physical.Encode(lower.PlanQuery(...))
// is the translator's text), so the translation cache, the markers and the handlers see no difference.
// -query-plan-lowering=false turns it off.

// QueryPlanFlippedConstructs are the plan constructs (lower.Construct) the proxy serves from the typed lowering:
// a query whose plan is one of them and which lower.Plan lowers is translated by it instead of the string
// translator. The corpus shadow requires each to be lowered natively, equal to the translator, byte for byte, and
// records the list in conformance/registry/query-plan.json; widening it is how a construct is flipped. A proxy reads it
// once, when it is built.
var QueryPlanFlippedConstructs = []string{"LogSelect", "VectorAgg", "RangeAgg"}

// loweringProfile is the lowering's view of the options the string translator is given: the one place the
// translator's options become a lower.Profile, shared by the flip and the live shadow.
func loweringProfile(label translator.LabelTranslateFunc, streamFields map[string]bool, caps logsql.Capabilities, keepLine bool) lower.Profile {
	return lower.Profile{Caps: caps, Label: label, StreamFields: streamFields, KeepLine: keepLine}
}

// loweringPlan is the typed pipeline up to the lowering for a canonical query (logql.CanonicalizeLogRanges):
// parse, build the logical plan, resolve it. It is the one place the lowering parses a query, for the flip and
// for the live shadow (which has no request and passes noFlowCtx), and the parse the request makes is counted in
// its flow like any other. A query the handlers already planned or parsed (the plan memo, resolvedPlan and
// sharedLogQLExpr) is not parsed again: its resolved plan is read, or built from its shared parse. The lowering
// only reads a plan, so a shared one serves it. It does not store into the memo: the plan of a translation is not
// one the handlers read, and storing it would cost the allocations of a memo entry on every translation.
func loweringPlan(ctx context.Context, canon string) (*queryplan.Plan, error) {
	var expr logqlpkg.Expr
	if len(canon) <= validationCacheMaxQueryBytes {
		if e, ok := planMemoLoad(canon); ok {
			if e.plan != nil {
				return e.plan, nil
			}
			expr = e.expr
		}
	}
	if expr == nil {
		var err error
		if expr, err = parseLogQLExpr(ctx, canon); err != nil {
			return nil, err
		}
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
	if !slices.Contains(p.flippedConstructs, construct) || !filterOnly(plan.Root) {
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

// keepIdentityConstruct is the flip of a range function over an unwrapped label whose series a keep before the
// unwrap decides (lower.KeepDecidesUnwrapIdentity): the string translator groups its samples by the stream, Loki by
// the labels the keep leaves, and the lowering writes Loki's identity, so the query is served from it
// (semantics/keep-before-unwrap-series-identity). The corpus shadow holds these rows as a known divergence of the
// translator, and the Loki differential (internal/querycorpus TestKeepUnwrapIdentityMatchLoki) holds the answers.
const keepIdentityConstruct = "UnwrapKeepIdentity"

// lowerMetric translates a canonical metric query with the typed lowering and reports whether it did: a query whose
// series a keep before its unwrap decides (keepIdentityConstruct), or one whose plan is a flipped metric construct
// and whose lowered text is the string translator's (writesTranslatorText). The query is planned once for both
// decisions, and only when one can serve it: a query without both words of the keep identity is planned only when the
// allocation-free text screen (lower.ScreenMetricIn) names a flipped construct for it, so a metric query the plan leaves
// to the string translator pays one scan of its text and no parse.
func (p *Proxy) lowerMetric(ctx context.Context, canon string, prof lower.Profile) (text string, ok bool) {
	if canon == "" || canon[0] == '{' {
		return "", false
	}
	flipped := lower.MetricSetOf(p.flippedConstructs)
	metricFlipped := flipped != 0
	if (!strings.Contains(canon, "keep") || !strings.Contains(canon, "unwrap")) && lower.ScreenMetricIn(canon, flipped) == "" {
		return "", false
	}
	construct := keepIdentityConstruct
	defer func() {
		if r := recover(); r != nil {
			// The panic value can hold query text, so it is not logged: the string translator answers instead.
			p.recordLoweringShadow(shadowPanic, construct)
			text, ok = "", false
		}
	}()
	plan, err := loweringPlan(ctx, canon)
	if err != nil {
		return "", false
	}
	// The keep identity is decided first: it is served whatever is flipped. The screen named the construct; the plan's
	// root has the last word.
	if !lower.KeepDecidesUnwrapIdentity(plan) {
		if !metricFlipped {
			return "", false
		}
		if construct = lower.Construct(plan.Root); !slices.Contains(p.flippedConstructs, construct) || !writesTranslatorText(plan, prof) {
			return "", false
		}
	}
	op, err := lower.PlanQuery(canon, plan, prof)
	if err != nil {
		return "", false
	}
	querycorpus.Record(querycorpus.LogQLTranslate, canon)
	p.recordLoweringShadow(shadowFlipped, construct)
	return physical.Encode(op), true
}

// writesTranslatorText reports a metric plan whose lowered text is the one the corpus shadow holds to the string
// translator's, byte for byte (TestPlanShadow): it turns away the plans the shadow holds to anything else.
//   - A ranking, stddev or stdvar at any level: the handlers evaluate it themselves and translate its operand, so the
//     lowering writes the operand's request and the proxy's evaluation (internal/querycorpus native_proxy_evaluated),
//     and the translator's text for the whole query drops the level (semantics/stddev-stdvar-nested-or-binary).
//   - A binary operation the binary route evaluates operand by operand (lower.RoutedBinary: a without clause or a
//     vector(n) operand).
//   - The recorded known divergences the plan writes as Loki reads them, where the translator writes another text:
//     a keep before an unwrap anywhere but where the proxy serves its identity (lower.WritesKeepUnwrapIdentity), a
//     numeric equality after a parser (semantics/label-filter-numeric-equality), a label named like a field
//     VictoriaLogs reserves (semantics/translator-writes-reserved-field-names), and an unwrap of one in another case,
//     which the lowering writes in the outbound guard's form (semantics/reserved-field-name-case-outbound).
//   - A label filter value the translator reads as syntax (misreadFilterValue).
//
// Serving these from the plan changes the text VictoriaLogs sees or its cache key, so they stay with the string
// translator until a flip carries their own proof. A label named like a LogsQL keyword is the one difference left:
// the plan quotes it and the translator writes it bare, which VictoriaLogs rejects
// (semantics/translator-writes-keyword-field-names-bare), as the corpus shadow and the log query flip already hold.
func writesTranslatorText(plan *queryplan.Plan, prof lower.Profile) bool {
	return !holdsEvaluatedLayer(plan.Root) && !lower.RoutedBinary(plan.Root) && !lower.WritesKeepUnwrapIdentity(plan) &&
		!lower.WritesNumericEquality(plan) && !lower.ReadsReservedField(plan, prof) && !lower.UnwrapsReservedFieldOtherCase(plan) &&
		!misreadFilterValue(plan.Root)
}

// misreadFilterValue reports a plan with a label filter whose value the string translator reads as syntax, where Loki
// and the plan read the value: one that holds detected_level or service_name (the translator unpacks the line,
// semantics/translator-reads-quoted-text-as-syntax), one that holds a \x00, \x01 or \x02 byte (its stage markers,
// semantics/translator-reads-marker-bytes-in-quoted-value), and a signed number, which the translator copies with
// the blank a query may write after the sign (semantics/translator-writes-signed-number-with-blank). It reads the
// values the parser decoded, never query text.
func misreadFilterValue(n queryplan.Node) bool {
	switch v := n.(type) {
	case *queryplan.LogSelect:
		for _, st := range v.Stages {
			if lf, ok := st.(*queryplan.LabelFilter); ok && misreadLeaf(lf.Expr) {
				return true
			}
		}
	case *queryplan.RangeAgg:
		return v.In != nil && misreadFilterValue(v.In)
	case *queryplan.VectorAgg:
		return misreadFilterValue(v.In)
	case *queryplan.Rank:
		return misreadFilterValue(v.In)
	case *queryplan.LabelReplace:
		return misreadFilterValue(v.In)
	case *queryplan.BinOp:
		return misreadFilterValue(v.L) || misreadFilterValue(v.R)
	}
	return false
}

// misreadLeaf reports a leaf of e whose value misreadFilterValue names.
func misreadLeaf(e *logqlpkg.LabelFilterExpr) bool {
	if e == nil {
		return false
	}
	if !e.IsLeaf() {
		return misreadLeaf(e.Left) || misreadLeaf(e.Right)
	}
	switch e.Kind {
	case logqlpkg.LabelFilterValueNumber, logqlpkg.LabelFilterValueDuration, logqlpkg.LabelFilterValueBytes:
		return e.Value != "" && (e.Value[0] == '-' || e.Value[0] == '+')
	}
	return strings.Contains(e.Value, "detected_level") || strings.Contains(e.Value, "service_name") ||
		strings.ContainsAny(e.Value, "\x00\x01\x02")
}

// holdsEvaluatedLayer reports a plan with a ranking, a stddev or a stdvar anywhere in it.
func holdsEvaluatedLayer(n queryplan.Node) bool {
	switch v := n.(type) {
	case *queryplan.Rank:
		return true
	case *queryplan.VectorAgg:
		return v.Op == logqlpkg.VectorStddev || v.Op == logqlpkg.VectorStdvar || holdsEvaluatedLayer(v.In)
	case *queryplan.LabelReplace:
		return holdsEvaluatedLayer(v.In)
	case *queryplan.BinOp:
		return holdsEvaluatedLayer(v.L) || holdsEvaluatedLayer(v.R)
	}
	return false
}

// filterOnly reports a log query whose stages are line filters and label filters only: the queries served from the
// lowering. The text screen (logql.MayHaveNonFilterStage) turns the others away before a plan is built; this is the
// typed check behind it, so a stage the screen misses stays with the string translator.
func filterOnly(n queryplan.Node) bool {
	sel, ok := n.(*queryplan.LogSelect)
	if !ok {
		return false
	}
	for _, st := range sel.Stages {
		switch st.(type) {
		case *queryplan.LineFilter, *queryplan.LabelFilter:
		default:
			return false
		}
	}
	return true
}
