package metrics

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// QueryComplexityLimits are the values of the limit label of loki_vl_proxy_query_complexity_rejections_total: the
// operator limits on the size of a query (-max-query-ast-nodes, -max-query-nesting-depth) and on the proxy's own
// planning time (-max-query-plan-time). The set is fixed, so the label is bounded.
var QueryComplexityLimits = [...]string{"ast_nodes", "nesting_depth", "plan_time"}

const (
	queryComplexityRejectionsName = "loki_vl_proxy_query_complexity_rejections_total"
	queryComplexityRejectionsHelp = "Requests refused for the size of their query or the time the proxy's own planning of it took, by the limit that refused them: ast_nodes (-max-query-ast-nodes), nesting_depth (-max-query-nesting-depth), plan_time (-max-query-plan-time)."
)

// queryComplexityRejections counts the refusals, by QueryComplexityLimits.
type queryComplexityRejections [len(QueryComplexityLimits)]atomic.Int64

// RecordQueryComplexityRejection counts one request refused by limit (one of QueryComplexityLimits; any other value
// is not counted, so the label cannot grow).
func (m *Metrics) RecordQueryComplexityRejection(limit string) {
	if m == nil {
		return
	}
	for i, name := range QueryComplexityLimits {
		if name == limit {
			m.queryComplexity[i].Add(1)
			return
		}
	}
}

// QueryComplexityRejectionCount is the refusals counted for limit (one of QueryComplexityLimits).
func (m *Metrics) QueryComplexityRejectionCount(limit string) int64 {
	if m == nil {
		return 0
	}
	for i, name := range QueryComplexityLimits {
		if name == limit {
			return m.queryComplexity[i].Load()
		}
	}
	return 0
}

func (m *Metrics) writeQueryComplexityMetrics(sb *strings.Builder) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s counter\n", queryComplexityRejectionsName, queryComplexityRejectionsHelp, queryComplexityRejectionsName)
	for i, name := range QueryComplexityLimits {
		fmt.Fprintf(sb, "%s{limit=%q} %d\n", queryComplexityRejectionsName, name, m.queryComplexity[i].Load())
	}
}

func (p *OTLPPusher) queryComplexityMetrics(now int64) []map[string]interface{} {
	points := make([]map[string]interface{}, 0, len(QueryComplexityLimits))
	for i, name := range QueryComplexityLimits {
		points = append(points, p.counterDP(queryComplexityRejectionsName, p.metrics.queryComplexity[i].Load(), now, attr("limit", name)))
	}
	return []map[string]interface{}{p.sumMetric(queryComplexityRejectionsName, queryComplexityRejectionsHelp, "", points...)}
}
