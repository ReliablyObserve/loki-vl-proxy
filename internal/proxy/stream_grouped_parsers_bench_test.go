package proxy

import (
	"net/http/httptest"
	"testing"
)

// BenchmarkStreamGroupedParsersTurnAway is what the rule costs a request it does not take, at the start of the
// ordered JSON route (serveStreamGroupedParsers): the shared plan is a memo hit, the shape check reads it.
func BenchmarkStreamGroupedParsersTurnAway(b *testing.B) {
	p := &Proxy{queryPlanLowering: true}
	for _, q := range []string{
		`sum by (level) (count_over_time({app="x"} | json | drop __error__, __error_details__ [5m]))`,
		`sum by (route) (count_over_time({app="x"} | json | logfmt | route!="" | drop __error__, __error_details__ [5m]))`,
	} {
		r := httptest.NewRequest("GET", "/loki/api/v1/query_range", nil)
		w := httptest.NewRecorder()
		p.serveStreamGroupedParsers(w, r, q, true) // fills the plan memo
		b.Run(q[:40], func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if p.serveStreamGroupedParsers(w, r, q, true) {
					b.Fatal("served")
				}
			}
		})
	}
}
