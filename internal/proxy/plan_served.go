package proxy

import (
	"context"
	"strings"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
)

// lowerTranslation is the typed lowering's part of a translation (translateQueryOpts): the text the proxy serves from
// the query plan for a canonical query (logql.CanonicalizeLogRanges), and false when the string translator translates
// it. A log query first, then a parser's series identity (lowerMetric writes the translator's series for what that
// does not serve; baseIdentity is a window its evaluators do not read), then a metric construct.
func (p *Proxy) lowerTranslation(ctx context.Context, canon string, prof lower.Profile, baseIdentity bool) (string, bool) {
	if lowered, ok := p.lowerLogSelect(ctx, canon, prof); ok {
		return lowered, true
	}
	if !baseIdentity {
		if lowered, ok := p.lowerDynamicIdentity(ctx, canon, prof); ok {
			return lowered, true
		}
	}
	return p.lowerMetric(ctx, canon, prof)
}

// PlanServedText is the text the proxy serves from the query plan for a client query under prof, with the decisions
// of a translation (lowerTranslation, for a request whose window the parser's series identity reads), and false when
// the string translator translates the query (or -query-plan-lowering is off). It changes nothing the proxy does: it is
// the reference the corpus shadow holds a recorded divergence the proxy serves from the plan to, so a row counted as
// served is one the proxy serves.
func (p *Proxy) PlanServedText(query string, prof lower.Profile) (string, bool) {
	if !p.queryPlanLowering {
		return "", false
	}
	return p.lowerTranslation(noFlowCtx, logqlpkg.CanonicalizeLogRanges(strings.TrimSpace(query)), prof, false)
}
