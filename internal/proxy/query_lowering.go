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
var QueryPlanFlippedConstructs = []string{"LogSelect", "VectorAgg", "RangeAgg", "BinOp", "LabelReplace"}

// QueryPlanFlippedLogStages are the stage families of a log query (lower.LogStages) the proxy serves from the typed
// lowering, on top of line and label filters: a log query whose stages are all in flipped families, which the text
// screen (lower.ScreenLogStages) and the typed gate (servesLogStages) admit, is translated by the lowering. The plan
// writes the string translator's text for them, byte for byte, on every corpus row under every proxy profile
// (TestQueryPlanLowering_LogCorpusTextParity), or the text Loki reads where the translator's is a recorded known
// divergence (UnspacedParser: a regexp or pattern parser written with no blank before its expression, which the
// translator copies into a pipe VictoriaLogs rejects, semantics/translator-copies-unspaced-parser); conformance/
// registry/query-plan.json records the list.
var QueryPlanFlippedLogStages = []string{"LevelFilter", "UnspacedParser", "JSON", "Logfmt"}

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
// built: a query that does not start with a stream selector is a metric query (the other half of the traffic); one
// with only line and label filters (logql.MayHaveNonFilterStage) is served as it is; and one with a parser, a
// formatting stage or a filter on a derived label is planned only when the allocation-free text screen
// (lower.ScreenLogStages) names flipped stage families for it, and served only when the typed gate (servesLogStages)
// agrees. The lowering itself has the last word: it answers an error for anything it does not write.
func (p *Proxy) lowerLogSelect(ctx context.Context, canon string, prof lower.Profile) (text string, ok bool) {
	if canon == "" || canon[0] != '{' {
		return "", false
	}
	stages, allowed, screened := screenLogQuery(canon, p.flippedLogStages)
	if !screened {
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
	if !slices.Contains(p.flippedConstructs, construct) {
		return "", false
	}
	if stages == 0 && !filterOnly(plan.Root) || stages != 0 && !servesLogStages(plan, prof, allowed) {
		return "", false
	}
	text, err = lower.PlanLogText(plan, prof)
	if err != nil {
		return "", false
	}
	// The corpus capture records the queries a translation saw (a no-op without the corpuscapture build tag); a
	// query the lowering answered is one of them.
	querycorpus.Record(querycorpus.LogQLTranslate, canon)
	p.recordLoweringShadow(shadowFlipped, construct)
	return text, true
}

// screenLogQuery is the text screen of lowerLogSelect for a canonical log query, under the flipped stage families:
// ok for a query the lowering plans (one with only line and label filters, or whose stage families the screen names
// and flipped holds), with the families the screen named and the ones the typed gate then serves.
func screenLogQuery(canon string, flipped lower.LogStages) (stages, allowed lower.LogStages, ok bool) {
	allowed = flipped
	if !logqlpkg.MayHaveNonFilterStage(canon) {
		return 0, allowed, true
	}
	stages = lower.ScreenLogStagesIn(canon, allowed)
	if stages&allowed&lower.LogUnspacedParser != 0 {
		// The translator's text for the query is rejected by VictoriaLogs: its regexp and pattern parsers are served
		// with the unspaced one.
		allowed |= lower.LogRegexpPattern
	}
	return stages, allowed, stages != 0 && stages&^allowed == 0
}

// screenMetricQuery is the text screen of lowerMetric for a canonical metric query, under the flipped metric
// constructs: true for a query the lowering plans.
func screenMetricQuery(canon string, flipped lower.MetricSet) bool {
	return strings.Contains(canon, "keep") && strings.Contains(canon, "unwrap") || lower.ScreenMetricIn(canon, flipped) != ""
}

// lowersParse reports a query the translation plans, read with the text screens of the stage families and
// constructs the proxy serves by default (QueryPlanFlippedLogStages, QueryPlanFlippedConstructs): the request's
// validation then keeps its parse in the plan memo, where the lowering reads it (loweringPlan), so the query is
// parsed once. A query it names that the translation does not plan, or reads in another spelling
// (logql.CanonicalizeLogRanges), costs a memo entry that maps the text to its own tree; one it misses costs a parse,
// as before.
func lowersParse(query string) bool {
	if query == "" {
		return false
	}
	if query[0] == '{' {
		_, _, ok := screenLogQuery(query, lower.LogStagesOf(QueryPlanFlippedLogStages))
		return ok
	}
	return screenMetricQuery(query, lower.MetricSetOf(QueryPlanFlippedConstructs))
}

// turnedAwayByStages reports a parsed query the text screens turn away for a stage of its log query, read from the
// tree in a few steps: a vector(n) call at the top or as a binary operand, a decolorize or unpack stage, a detected_level filter after a stage (after a json or logfmt
// parser in a metric query), and an
// equality on a number, duration or byte size after a json or logfmt parser (in a log query also an unwrap and a
// service_name filter). The validation asks it before lowersParse, so a query it names costs no text scan; a query it
// misses is read by the screens as before. It never names a query the translation plans.
func turnedAwayByStages(expr logqlpkg.Expr) bool {
	logQuery := true
	for {
		switch x := expr.(type) {
		case *logqlpkg.RangeAggregation:
			expr, logQuery = x.Inner, false
			continue
		case *logqlpkg.VectorAggregation:
			expr, logQuery = x.Inner, false
			continue
		case *logqlpkg.LogQuery:
			return pipelineTurnedAway(x.Pipeline, logQuery)
		case *logqlpkg.OpaqueMetricExpr:
			return isVectorCall(x)
		case *logqlpkg.BinOpExpr:
			// vector(n) on either side.
			return isVectorCall(x.Left) || isVectorCall(x.Right)
		}
		return false
	}
}

// isVectorCall reports a vector(n) call, which the metric screen turns away at any level.
func isVectorCall(e logqlpkg.Expr) bool {
	o, ok := e.(*logqlpkg.OpaqueMetricExpr)
	return ok && o.Func == "vector"
}

// pipelineTurnedAway is turnedAwayByStages of one pipeline.
func pipelineTurnedAway(stages []logqlpkg.Stage, logQuery bool) bool {
	staged, keyParsed := false, false
	for _, st := range stages {
		switch x := st.(type) {
		case *logqlpkg.DecolorizeStage:
			return true
		case *logqlpkg.UnwrapStage:
			if logQuery {
				return true
			}
		case *logqlpkg.ParserStage:
			if x.Type == logqlpkg.ParserUnpack {
				return true
			}
			staged = true
			keyParsed = keyParsed || x.Type == logqlpkg.ParserJSON || x.Type == logqlpkg.ParserLogfmt
		case *logqlpkg.LabelFilterStage:
			if x.Expr != nil && labelFilterTurnedAway(x.Expr, staged, keyParsed, logQuery) {
				return true
			}
		case *logqlpkg.LineFilterStage:
		default:
			staged = true
		}
	}
	return false
}

// labelFilterTurnedAway reports a leaf of a label filter the screens turn away: detected_level after a stage (in a
// metric query, after a json or logfmt parser), an
// equality on a number, duration or byte size after a json or logfmt parser, and (in a log query) service_name.
func labelFilterTurnedAway(e *logqlpkg.LabelFilterExpr, staged, keyParsed, logQuery bool) bool {
	if !e.IsLeaf() {
		return labelFilterTurnedAway(e.Left, staged, keyParsed, logQuery) || labelFilterTurnedAway(e.Right, staged, keyParsed, logQuery)
	}
	switch {
	case e.Name == "detected_level" && (logQuery && staged || keyParsed), e.Name == "service_name" && logQuery:
		return true
	case keyParsed && (e.Cmp == "=" || e.Cmp == "==" || e.Cmp == "!="):
		switch e.Kind {
		case logqlpkg.LabelFilterValueNumber, logqlpkg.LabelFilterValueDuration, logqlpkg.LabelFilterValueBytes:
			return true
		}
	}
	return false
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
	if !screenMetricQuery(canon, flipped) {
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

// servesLogStages is the typed gate of a log query with stage families (lower.PlanLogStages): every family in allowed
// (the flipped ones, and the regexp and pattern parsers of a query with an unspaced one), at least one, and none of
// the recorded known divergences, where the plan writes the query as Loki reads it and the
// string translator writes another text (writesTranslatorText names them): a numeric equality after a parser, a label
// named like a field VictoriaLogs reserves, a label filter value the translator reads as syntax. Those stay with the
// string translator until a flip carries their own proof.
func servesLogStages(plan *queryplan.Plan, prof lower.Profile, allowed lower.LogStages) bool {
	stages, ok := lower.PlanLogStages(plan)
	return ok && stages != 0 && stages&^allowed == 0 && !lower.WritesNumericEquality(plan) &&
		!lower.ReadsReservedField(plan, prof) && !misreadFilterValue(plan.Root)
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
