package logql

import "testing"

// BenchmarkLabelFilterLiterals is the cost of reading label filters whose values are numbers, durations and bytes
// sizes, for queries both accepted and rejected (ParseLabelFilterExpr alone, then the whole validation).
func BenchmarkLabelFilterLiterals(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"accepted", `{app="a"} | json | latency>=250ms and size<1.5KB, status>=500`},
		{"accepted_hex_duration", `{app="a"} | json | a<0x1f and b>1h30m and c>=5KiB or d==1e3`},
		{"accepted_ident_heavy", `{app="a"} | json | status_code>=500 and method="GET", path=~"/api/.*" or level!="debug"`},
		{"rejected_type", `{app="a"} | json | a>"x" and b<5m`},
		{"rejected_syntax", `{app="a"} | json | a<5m and b>`},
		{"metric_compare", `sum(rate({app="a"}[5m])) > 100`},
		{"metric_offset", `sum by (app) (count_over_time({app="a"} | json | status>=500 [1h] offset 5m)) / 2`},
		{"line_filters", `{app="a"} |= "error" != "debug" |~ "x.*y" | logfmt | level="error"`},
		{"keyword_names", `{app="a"} | json | rate_x>5 and byte_count<5kB or by_x="y"`},
		{"rejected_metric_literal", `sum(rate({app="a"}[5m])) > 0x`},
		{"rejected_binary_log", `{app="a"} | json | a>5 + 3`},
	} {
		b.Run("validate_"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = ValidateLogQL(tc.query)
			}
		})
	}
	for _, tc := range []struct{ name, text string }{
		{"accepted", `latency>=250ms and size<1.5KB, status>=500`},
		{"accepted_hex_duration", `a<0x1f and b>1h30m and c>=5KiB or d==1e3`},
		{"rejected_type", `a>"x" and b<5m`},
	} {
		b.Run("expr_"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = ParseLabelFilterExpr(tc.text)
			}
		})
	}
}
