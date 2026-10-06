package proxy

import "testing"

// A log range in one of Loki's alternative forms and its plain form translate
// to the same VictoriaLogs query, and a metric of the alternative form is a
// stats query (it used to translate to a bare filter that VictoriaLogs rejected).
// conformance: semantics/parenthesized-log-range, loki_api_v1_query_range, loki_api_v1_query
func TestTranslateQueryAcceptsParenthesizedLogRange(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	for _, tc := range []struct{ alt, plain string }{
		{`rate(({app="x"} |= "err")[5m])`, `rate({app="x"} |= "err" [5m])`},
		{`count_over_time(({app="x"} | json)[1m] offset 1h)`, `count_over_time({app="x"} | json [1m] offset 1h)`},
		{`sum by (a) (count_over_time((({app="x"} |= "e")[5m])))`, `sum by (a) (count_over_time({app="x"} |= "e" [5m]))`},
		{`sum_over_time(({app="x"} | json | unwrap v)[5m])`, `sum_over_time({app="x"} | json | unwrap v [5m])`},
		{`rate({app="x"}[5m] | json)`, `rate({app="x"} | json [5m])`},
		{`({app="x"} |= "a" or "b")`, `{app="x"} |= "a" or "b"`},
	} {
		alt, err := p.translateQuery(tc.alt)
		if err != nil {
			t.Fatalf("%s: %v", tc.alt, err)
		}
		plain, err := p.translateQuery(tc.plain)
		if err != nil {
			t.Fatalf("%s: %v", tc.plain, err)
		}
		if alt != plain {
			t.Errorf("%s\n got %s\nwant %s (the plain form)", tc.alt, alt, plain)
		}
	}
}
