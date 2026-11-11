package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The post-aggregation handlers evaluate a ranking, a stddev or a stdvar (and the layers above a ranking)
// themselves: they translate the operand alone and fold its answer, so the string translator never sees the whole
// query and the lowering shadow of the translation (shadowLowering) only sees the operand. shadowProxyEvaluated is
// the live shadow of the whole query on that route: on the same sampled share (-translate-shadow-sample), in the
// background, it lowers the query and holds it to what the handler runs, the reference the corpus shadow uses
// (postAggregationOp, proxy.PostAggregationPhysical): the levels the proxy applies to the answer, and the operand's
// request byte for byte. It reads nothing the handler does not already hold, translates nothing and changes nothing.

// evaluatedShadowInput is what the handler routed and sent for one proxy-evaluated query.
type evaluatedShadowInput struct {
	ctx     context.Context // the request's values (tenant, request marker), never its cancellation
	query   string          // the query as the handler routed it
	postAgg instantMetricPostAgg
	operand string // the operand's text as translatePostAggOperand returned it
}

// shadowProxyEvaluated is called by both post-aggregation handlers once the operand is translated. When the sample
// skips the query it costs a nil check and a draw, and allocates nothing.
func (p *Proxy) shadowProxyEvaluated(ctx context.Context, query string, postAgg instantMetricPostAgg, operand string) {
	if s := p.loweringShadow; s.sampled() {
		p.startProxyEvaluatedShadow(s, evaluatedShadowInput{ctx: ctx, query: query, postAgg: postAgg, operand: operand})
	}
}

// startProxyEvaluatedShadow hands a sampled comparison to a goroutine. It is not inlined so that the input escapes
// only on this path, never on a request the sample skips.
//
//go:noinline
func (p *Proxy) startProxyEvaluatedShadow(s *loweringShadow, in evaluatedShadowInput) {
	if !p.takeShadowSlot(s) {
		return
	}
	in.ctx = context.WithoutCancel(in.ctx)
	go func() {
		defer s.done()
		p.runProxyEvaluatedShadow(s, in)
	}()
}

// evaluatedShadowProfile is the lowering profile of the operand's translation: the options translateQueryOpts gave
// the translator for the text postAggOperandText translates (the operand, or for a stddev or stdvar by a parsed
// label its `max by` form), with the tenant's stream label names as last known (no refresh is started).
func (p *Proxy) evaluatedShadowProfile(in evaluatedShadowInput) lower.Profile {
	text, _ := postAggTranslatedText(in.postAgg)
	streamLabels := p.plainStreamLabelNamesFrom(in.ctx, strings.TrimSpace(text), p.knownTenantStreamLabelNames)
	p.configMu.RLock()
	labelFn := translator.WithStreamLabels(p.labelTranslator.ToVL, streamLabels)
	streamFields := p.streamFieldsMap
	p.configMu.RUnlock()
	caps := p.logsqlCapabilities()
	caps.NoDetectedLevelBodyScan = !p.detectedLevelBodyScan
	return loweringProfile(labelFn, streamFields, caps, false)
}

// evaluatedShadowConstruct is the construct label of a proxy-evaluated root: the node kind, and for a stddev or
// stdvar its operation (VectorAgg stddev, VectorAgg stdvar).
func evaluatedShadowConstruct(root queryplan.Node) string {
	if v, ok := root.(*queryplan.VectorAgg); ok && (v.Op == logqlpkg.VectorStddev || v.Op == logqlpkg.VectorStdvar) {
		return "VectorAgg " + string(v.Op)
	}
	return lower.Construct(root)
}

// runProxyEvaluatedShadow compares the lowering of the whole query with the handler's route and counts the outcome.
func (p *Proxy) runProxyEvaluatedShadow(s *loweringShadow, in evaluatedShadowInput) {
	run := p.newShadowRun(s)
	defer run.recoverPanic(in.query)

	plan, err := loweringPlan(noFlowCtx, logqlpkg.CanonicalizeLogRanges(in.query))
	if err != nil {
		// The handler routed and translated the query; the typed pipeline rejects it.
		run.result(shadowNewError)
		return
	}
	run.construct = evaluatedShadowConstruct(plan.Root)
	if run.out() {
		run.result(shadowBudget)
		return
	}
	var lowered physical.Op
	if err = lower.ScreenText(in.query, plan); err == nil {
		lowered, err = s.lower(plan, p.evaluatedShadowProfile(in))
	}
	if err != nil {
		var nl *lower.NotLoweredError
		switch {
		case errors.As(err, &nl):
			run.construct = nl.Construct
			run.result(shadowNotLowered)
		case errors.Is(err, lower.ErrNotLowered):
			run.result(shadowNotLowered)
		default:
			run.result(shadowNewError)
		}
		return
	}
	// The translator writes a label called not or in bare, which VictoriaLogs rejects; the lowering quotes it. The
	// handler's text is held to the lowering with that name quoted, as in the corpus shadow.
	operand := lower.QuoteKeywordFieldText(in.operand)
	want, err := postAggregationOp(in.postAgg, operand)
	if err != nil {
		run.result(shadowOldError)
		return
	}
	if run.out() {
		run.result(shadowBudget)
		return
	}
	wantLevels, _ := physical.SplitEval(want)
	gotLevels, gotOperand := physical.SplitEval(lowered)
	switch {
	case len(gotLevels) > len(wantLevels):
		// The plan holds a stddev, stdvar or ranking inside the operand the handler translates as text, which drops
		// it (the corpus shadow's known divergence): the route is the one that differs.
		run.result(shadowKnownDivergence)
	case physical.Equal(want, lowered) && physical.Encode(gotOperand) == operand:
		run.result(shadowNativeProxyEvaluated)
	default:
		run.result(shadowMismatch)
		if s.logAllowed(&s.lastLog) {
			// The levels are the operation, k and the kind and size of the clause of each level: no name from the query.
			p.log.Warn("lowering shadow: the lowering differs from the proxy's route",
				"construct", shadowConstruct(run.construct),
				"logql", redactQuery(in.query, p.debugLogRawQueries),
				"route_levels", fmt.Sprint(levelsText(wantLevels)),
				"lowering_levels", fmt.Sprint(levelsText(gotLevels)),
				"route_operand", redactQuery(operand, p.debugLogRawQueries),
				"lowering_operand", redactQuery(physical.Encode(gotOperand), p.debugLogRawQueries))
		}
	}
}

// levelsText writes the levels of an evaluation for a log line: operation, k, and the kind and number of labels of
// the clause.
func levelsText(levels []physical.Level) []string {
	out := make([]string, len(levels))
	for i, l := range levels {
		out[i] = l.Op
		if l.K != 0 {
			out[i] += fmt.Sprintf("(%d)", l.K)
		}
		if l.By != nil {
			kind := "by"
			if l.By.Without {
				kind = "without"
			}
			out[i] += fmt.Sprintf(" %s(%d)", kind, len(l.By.Labels))
		}
	}
	return out
}
