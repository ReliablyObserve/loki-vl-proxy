package proxy

import (
	"fmt"
	"net/http"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/lokierr"
)

// The operator limits on the size of a parsed query (-max-query-ast-nodes, -max-query-nesting-depth). Loki has no
// such limit below its input size limit, so the defaults are the query length limit in effect: no query that passed
// that limit has more nodes or levels than bytes (logql.Complexity, FuzzComplexityBoundedByLength), and the limits
// refuse a query only after an operator lowers them. A query shorter than both limits is not walked at all.

// complexityKey keys a query's complexity facts in validationCache, apart from its validation.
type complexityKey string

// complexityFacts are the nodes and depth of a query (logql.Complexity): a function of the query text alone, so
// one entry serves every proxy of the process whatever its limits. The verdict against a limit is never stored.
type complexityFacts struct{ nodes, depth int }

// queryComplexityFacts reads the facts of a query that validates, from the cache or from the shared parse; ok is
// false for a query that does not parse (the validation answers it).
func queryComplexityFacts(query string) (complexityFacts, bool) {
	cacheable := len(query) <= validationCacheMaxQueryBytes
	if cacheable {
		if v, ok := validationCache.Load(complexityKey(query)); ok {
			if f, ok := v.(complexityFacts); ok {
				return f, true
			}
		}
	}
	expr, ok := sharedLogQLExpr(query)
	if !ok {
		return complexityFacts{}, false
	}
	nodes, depth := logqlpkg.Complexity(expr)
	f := complexityFacts{nodes, depth}
	if cacheable {
		storeValidationResult(complexityKey(query), f)
	}
	return f, true
}

// queryComplexityError is the typed 400 of a query over this proxy's node or depth limit, nil when it is within
// both or does not parse. The limits are this proxy's: the facts are shared, the verdict is not.
func (p *Proxy) queryComplexityError(query string) *lokierr.Error {
	lim := p.limits()
	if lim.QueryASTNodes >= len(query) && lim.QueryNestingDepth >= len(query) {
		return nil
	}
	f, ok := queryComplexityFacts(query)
	if !ok {
		return nil
	}
	switch {
	case f.nodes > lim.QueryASTNodes:
		p.metrics.RecordQueryComplexityRejection("ast_nodes")
		return lokierr.QueryTooComplex(false, lim.QueryASTNodes, flagMaxQueryASTNodes, fmt.Errorf("the query has %d nodes, over the limit of %d", f.nodes, lim.QueryASTNodes))
	case f.depth > lim.QueryNestingDepth:
		p.metrics.RecordQueryComplexityRejection("nesting_depth")
		return lokierr.QueryTooComplex(true, lim.QueryNestingDepth, flagMaxQueryNestingDepth, fmt.Errorf("the query is %d levels deep, over the limit of %d", f.depth, lim.QueryNestingDepth))
	}
	return nil
}

// validQueryComplexityError is queryComplexityError for a query that is still to be validated: a query the
// validation rejects is answered by that rejection, never by a limit.
func (p *Proxy) validQueryComplexityError(query string) *lokierr.Error {
	lim := p.limits()
	if lim.QueryASTNodes >= len(query) && lim.QueryNestingDepth >= len(query) {
		return nil
	}
	if queryLengthLimit(query, lim.QueryLengthBytes) != nil || cachedQueryValidation(query).msg != "" {
		return nil
	}
	return p.queryComplexityError(query)
}

// queryParamComplexityError holds the query of a route that reads a selector or a query (lokiQueryParamRule) to the
// limits: the match[] selectors of a series request, the query parameter of every other. The error is nil when the
// limits are not below the length limit, or the query does not validate.
func (p *Proxy) queryParamComplexityError(rule lokiQueryParamRule, r *http.Request) error {
	lim := p.limits()
	if lim.QueryASTNodes >= lim.QueryLengthBytes && lim.QueryNestingDepth >= lim.QueryLengthBytes {
		return nil
	}
	if rule == lokiSeriesMatchers {
		for _, group := range append(append([]string(nil), r.Form["match"]...), r.Form["match[]"]...) {
			if err := p.validQueryComplexityError(group); err != nil {
				return err
			}
		}
		return nil
	}
	if err := p.validQueryComplexityError(r.FormValue("query")); err != nil {
		return err
	}
	return nil
}
