package logql

import "testing"

// BenchmarkStripComments: the query rewrite every request goes through, for a query without a comment, with comments
// it removes, and with a comment before a vector aggregation's call, which it keeps.
func BenchmarkStripComments(b *testing.B) {
	for _, bc := range []struct{ name, q string }{
		{"none", `sum by (app) (rate({app="a"} | json | level="error" [5m]))`},
		{"removed", "sum by (app) (rate({app=\"a\"} # c\n| json | level=\"error\" # d\n[5m])) # e"},
		{"kept", "sum # c\nby (app) (rate({app=\"a\"} | json | level=\"error\" [5m]))"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = StripComments(bc.q)
			}
		})
	}
}
