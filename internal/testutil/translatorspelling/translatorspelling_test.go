package translatorspelling

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// Case names the spelling of each class, and none for the spellings the translator reads as the plan does.
func TestCase(t *testing.T) {
	for q, want := range map[string]string{
		`label_replace(rate({a=~"x\\(y"}[5m]) + rate({b="z"}[5m]), "d", "$1", "app", "(.*)")`: QuotedText,
		"rate({app=\"a\"} |= `,)` [5m]) * 2":                                                  QuotedText,
		`topk(2, count_over_time({env="production"} != "(" [1m]) * 2)`:                        QuotedText,
		`sum_over_time({app="a"} | unwrap bytes (n) [5m])`:                                    UnwrapConversionBlank,
		`sum_over_time({app="a"} | unwrap bytes( n ) [5m])`:                                   UnwrapConversionBlank,
		`quantile_over_time(0.50, {app="a"} | unwrap n [5m])`:                                 QuantileLiteral,
		`quantile_over_time(.5, {app="a"} | unwrap n [5m])`:                                   QuantileLiteral,
		`rate({app="a"}[5m]) -2`:                                                              BinaryText,
		`sum(rate({app="a"}[5m]))/2`:                                                          BinaryText,
		`sum by (app) (rate({app="a"}[5m])) * 1e3`:                                            BinaryText,
		`sum(rate({app="a"}[5m])) / (2)`:                                                      BinaryText,
		`rate({app="a"}[5m]) / on(app,env) rate({app="b"}[5m])`:                               BinaryText,
		`rate({app="a"}[5m]) / on(app) group_left rate({app="b"}[5m])`:                        BinaryText,
		"rate({app=\"a\"}[5m]) \t/ rate({app=\"b\"}[5m])":                                     BinaryText,
		// Spellings the translator reads as the plan does.
		`rate({app="a"}[5m]) / 2`: "",
		`sum by (job) (sum_over_time({app="uw"} | json | x="1" | unwrap amount [5m])) / on(job) group_left(a, b) sum by (job) (count_over_time({app="uw"} | logfmt [5m]))`: "",
		`rate({app="a"} |= "(" [5m])`:                                  "",
		`quantile_over_time(0.5, {app="a"} | unwrap n [5m])`:           "",
		`sum_over_time({app="a"} | unwrap bytes(n) [5m])`:              "",
		`label_replace(rate({app="a"}[5m]), "d", "$1", "app", "(.*)")`: "",
	} {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		if got := Case(q, p); got != want {
			t.Errorf("%q: %q, want %q", q, got, want)
		}
	}
}
