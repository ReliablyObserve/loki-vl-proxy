package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// BenchmarkParserSkipLowering is the lowering of the Logs Drilldown count over two parsers, which the plan skips, and
// of a grouped breakdown over the same parsers, which it writes as before (semantics/ungrouped-sum-skips-parsers).
func BenchmarkParserSkipLowering(b *testing.B) {
	prof := Profile{Caps: logsql.CapabilitiesFor("v1.54.0")}
	const pipe = `{cluster="c1", namespace="ns1", container="ingress"} | json | logfmt | drop __error__, __error_details__`
	for _, tc := range []struct{ name, q string }{
		{"count", `sum(count_over_time(` + pipe + ` [3h]))`},
		{"rate", `sum(rate(` + pipe + ` [3h]))`},
		{"grouped", `sum by (status) (count_over_time(` + pipe + ` [3h]))`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			p := rangeAggPlan(tc.q)
			if p == nil {
				b.Fatal("no plan")
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := PlanQuery(tc.q, p, prof); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
