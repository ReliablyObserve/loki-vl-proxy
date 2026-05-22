package logql

import (
	"testing"
)

// FuzzParse ensures that Parse + String + Translate never panic on arbitrary input.
// Run with: go test -fuzz=FuzzParse ./internal/logql/
func FuzzParse(f *testing.F) {
	seeds := []string{
		// basic valid
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} | json | level="error"`,
		`rate({app="api"}[5m])`,
		`sum by (app) (rate({app="api"}[5m]))`,
		`{}`,
		// truncated / malformed
		``,
		`{`,
		`{app=`,
		`| json`,
		`not logql at all`,
		// pipeline features
		`{app="nginx"} | drop a, b | keep c | decolorize`,
		`{app="nginx"} | logfmt | level="error"`,
		`{app="nginx"} | unwrap bytes(size)`,
		`{app="nginx"} | line_format "{{.msg}}"`,
		`{app="nginx"} | label_format new=old`,
		// range ops
		`count_over_time({app="api"}[1h])`,
		`bytes_rate({app="api"}[5m])`,
		`avg_over_time({app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.99, {app="api"} | unwrap latency [5m])`,
		`rate_counter({app="api"} | unwrap latency [5m])`,
		`absent_over_time({app="api"}[5m])`,
		// with offset
		`rate({app="api"}[5m] offset 1h)`,
		// subqueries
		`max_over_time(rate({app="api"}[5m])[1h:5m])`,
		// vector aggs with param
		`topk(5, rate({app="api"}[5m]))`,
		`bottomk(3, count_over_time({app="api"}[1h]))`,
		`sum(rate({app="api"}[5m])) by (app)`,
		// binary ops
		`rate({app="api"}[5m]) / rate({app="api"}[5m])`,
		`100 * sum by (app) (rate({app="api"}[5m])) / sum by (app) (rate({app="api"}[5m]))`,
		`rate({app="api"}[5m]) and rate({app="api"}[5m])`,
		// vector matching
		`rate({app="api"}[5m]) * on (app) group_left (team) rate({app="api"}[5m])`,
		// label filters
		`{app="nginx"} | json | status>=400`,
		`{app="nginx"} | json | status!=200`,
		// wildcard
		`*`,
		// unknown stages
		`{app="nginx"} | custom_stage`,
		// partial inputs
		`rate(`,
		`sum by (`,
		`{app="nginx"} |`,
		`topk(`,
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		expr, err := Parse(input)
		if err != nil {
			return
		}
		// If Parse succeeded, String() and Translate() must not panic.
		_ = expr.String()
		_, _ = Translate(expr, TranslateOptions{})
	})
}
