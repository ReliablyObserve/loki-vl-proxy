package logql

import "testing"

// BenchmarkValidateErrorPhases is the validation of rejected queries whose answer depends on the order of Loki's
// phases: an action ahead of a later syntax error, a vector operation's parameter, a build error behind a tree
// check, and a valid query with stages the parser keeps for the order (a selector regex, a pattern parser).
func BenchmarkValidateErrorPhases(b *testing.B) {
	for _, c := range []struct{ name, q string }{
		{"action_before_syntax", `{a="b"} | pattern "<_>" | unpack a`},
		{"selector_regex_before_syntax", `{a=~"(b"} | a=="b"`},
		{"sum_parameter", `sum(1, rate({a="b"}[5m]))`},
		{"build_behind_tree", `count_over_time({a="b"} | line_format "{{.a" [5m]) by (a)`},
		{"valid_regex_selector", `sum by (level) (count_over_time({app=~"api-.+"} | pattern "<_> <level> <_>" [5m]))`},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = ValidateLogQL(c.q)
			}
		})
	}
}
