package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

var benchQueries = map[string]string{
	"stream_simple":   `{app="nginx"}`,
	"stream_pipeline": `{app="nginx"} |= "error" | json | level="error"`,
	"range_rate":      `rate({app="api"}[5m])`,
	"range_unwrap":    `avg_over_time({app="api"} | unwrap latency [5m])`,
	"vector_by":       `sum by (app, env) (rate({app="api"}[5m]))`,
	"vector_topk":     `topk(10, sum by (app) (rate({app="api"}[5m])))`,
	"binary_ratio": `100 * sum by (app) (rate({app="api", status=~"5.."}[5m])) ` +
		`/ sum by (app) (rate({app="api"}[5m]))`,
	"subquery":       `max_over_time(rate({app="api"}[5m])[1h:5m])`,
	"vector_match":   `rate({app="api"}[5m]) * on (app) group_left (team) rate({app="api"}[5m])`,
	"long_pipeline":  `{app="nginx"} |= "error" | json | level="error" | drop trace_id, span_id | line_format "{{.msg}}"`,
	"many_labels":    `{app="nginx", env="prod", region="us-east", host=~"host-[0-9]+", dc!="eu"}`,
	"quantile":       `quantile_over_time(0.99, {app="api"} | unwrap latency [5m])`,
	"offset":         `rate({app="api"}[5m] offset 1h)`,
	"trailing_by":    `sum(count_over_time({service_name="argocd", service_name != ""}[5s])) by (service_name)`,
}

func BenchmarkParse(b *testing.B) {
	for name, q := range benchQueries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = logql.Parse(q)
			}
		})
	}
}

func BenchmarkValidateLogQL(b *testing.B) {
	for name, q := range benchQueries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = logql.ValidateLogQL(q)
			}
		})
	}
}

func BenchmarkParseAndValidate(b *testing.B) {
	for name, q := range benchQueries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = logql.ParseAndValidate(q)
			}
		})
	}
}

func BenchmarkTranslate(b *testing.B) {
	exprs := make(map[string]logql.Expr, len(benchQueries))
	for name, q := range benchQueries {
		e, err := logql.Parse(q)
		if err != nil {
			b.Logf("skipping %s: %v", name, err)
			continue
		}
		exprs[name] = e
	}
	for name, expr := range exprs {
		e := expr
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = logql.Translate(e, logql.TranslateOptions{})
			}
		})
	}
}
