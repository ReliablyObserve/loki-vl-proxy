package translator

import (
	"regexp"
	"strings"
	"testing"
)

// An ungrouped sum over a range function with unwrap folds every series of the
// range aggregation into one (Loki v3.7.7 pkg/logql/evaluator.go
// newVectorAggEvaluator / VectorAggEvaluator.Next: a grouping of no labels), so
// the translation must end in a stats pipe with no `by`: the outer sum is a
// stage of the VictoriaLogs query, not something the proxy re-derives from the
// per-stream stage before it. The inner stage keeps the series identity
// (`_stream`, and `_msg` for the functions whose value is not additive) for the
// functions that take a value per series.
//
// conformance: semantics/unwrap-rate-ungrouped-sum-per-stream, metric-series-identity
func TestUngroupedSumOverUnwrapEndsInAnOuterSum(t *testing.T) {
	selectors := map[string]string{
		"no parser":      `{app="a"}`,
		"logfmt":         `{app="a"} | logfmt`,
		"json":           `{app="a"} | json`,
		"logfmt, no err": `{app="a"} | logfmt | __error__=""`,
		"json, no err":   `{app="a"} | json | __error__=""`,
	}
	funcs := []struct {
		name, finalStats string
	}{
		{"rate", `stats sum(__lvp_rate)`},
		{"sum_over_time", `stats sum(__lvp_inner)`},
		{"avg_over_time", `stats sum(__lvp_inner)`},
		{"max_over_time", `stats sum(__lvp_inner)`},
		{"min_over_time", `stats sum(__lvp_inner)`},
	}
	for name, selector := range selectors {
		for _, fn := range funcs {
			for _, window := range []string{"5m", "1h"} {
				query := `sum(` + fn.name + `(` + selector + ` | unwrap amount [` + window + `]))`
				t.Run(name+"/"+fn.name+"/"+window, func(t *testing.T) {
					got, err := TranslateLogQLWithLabels(query, nil)
					if err != nil {
						t.Fatal(err)
					}
					pipes := strings.Split(got, " | stats ")
					last := "stats " + pipes[len(pipes)-1]
					if last != fn.finalStats {
						t.Fatalf("final pipe %q, want the ungrouped %q\nlogsql: %s", last, fn.finalStats, got)
					}
					if strings.Contains(last, " by ") || strings.Contains(last, "by (") {
						t.Fatalf("the outer sum groups: %s", got)
					}
					if len(pipes) < 3 {
						t.Fatalf("want an inner stats stage before the outer sum: %s", got)
					}
					inner := pipes[1]
					if !strings.HasPrefix(inner, "by (_stream") {
						t.Fatalf("the inner stage must keep the series identity: %s", got)
					}
					if fn.name == "rate" {
						// rate over an unwrapped label is the sum of the values per second
						// (pkg/logql/range_vector.go rateLogs with computeValues).
						want := regexp.MustCompile(`math __lvp_inner/` + regexp.QuoteMeta(durationSecondsText(window)) + ` as __lvp_rate`)
						if !want.MatchString(got) {
							t.Fatalf("rate must divide the summed values by the window: %s", got)
						}
					}
				})
			}
		}
	}
}

func durationSecondsText(window string) string {
	if window == "1h" {
		return "3600"
	}
	return "300"
}
