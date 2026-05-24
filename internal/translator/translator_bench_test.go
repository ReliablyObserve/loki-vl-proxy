package translator

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func BenchmarkTranslateLogQL_Simple(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`{app="nginx"}`)
	}
}

func BenchmarkTranslateLogQL_MultiLabel(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`{app="api-gateway",namespace="prod",level="error",cluster="us-east-1"}`)
	}
}

func BenchmarkTranslateLogQL_LineFilter(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`{app="nginx"} |= "error" != "timeout" |~ "status=[45]\\d{2}"`)
	}
}

func BenchmarkTranslateLogQL_JSONPipeline(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`{app="api"} | json | status >= 500 | method = "POST"`)
	}
}

func BenchmarkTranslateLogQL_MetricQuery(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`sum(rate({app="nginx"}[5m])) by (host)`)
	}
}

func BenchmarkNormalizeQuery(b *testing.B) {
	for b.Loop() {
		NormalizeQuery(`{host="h1",app="nginx",namespace="prod"} |= "error"`)
	}
}

func BenchmarkTranslateMetricQuery_WithBool(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`sum(rate({app="nginx"}[5m]) > bool 0) by (host)`)
	}
}

func BenchmarkExtractOuterAggregation(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`sum by (host, namespace) (count_over_time({app="api"}[5m]))`)
	}
}

func BenchmarkTranslateMetricQuery_VectorMatch(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`sum(rate({app="a"}[5m])) by (host) / sum(rate({app="b"}[5m])) on (host)`)
	}
}

// --- Pain-point benchmarks: paths migrated to typed AST in this PR ---

// Rate translation: exercises PipeMath (the hottest math pipe path).
func BenchmarkTranslate_Rate(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`rate({app="nginx",env="production"}[5m])`)
	}
}

func BenchmarkTranslate_SumRate(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`sum(rate({app="nginx",env="production"}[5m])) by (host)`)
	}
}

// Binary metric: two rate() paths joined — exercises PipeMath twice + outer stats.
func BenchmarkTranslate_BinaryRateDiv(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`sum(rate({app="nginx",level="error"}[5m])) / sum(rate({app="nginx"}[5m]))`)
	}
}

func BenchmarkTranslate_BinaryRateMul100(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`rate({app="nginx"}[5m]) * 100`)
	}
}

// stdvar exercises PipeMath for variance and rate in the same query.
func BenchmarkTranslate_StdVar(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`stdvar(rate({app="nginx"}[5m])) by (host)`)
	}
}

// bytes_rate exercises PipeMath with sum_len inner function.
func BenchmarkTranslate_BytesRate(b *testing.B) {
	for b.Loop() {
		TranslateLogQL(`sum(bytes_rate({app="nginx",env="production"}[5m])) by (host)`)
	}
}

// ip() filter exercises Capabilities + BestIPv4Range (capability-aware path).
func BenchmarkTranslate_IPFilter(b *testing.B) {
	caps := logsql.CapabilitiesFor("1.45.0")
	for b.Loop() {
		TranslateLogQLWithCapabilities(`{app="nginx"} | ip("10.0.0.0/8")`, nil, nil, caps)
	}
}

func BenchmarkTranslate_IPFilterFallback(b *testing.B) {
	caps := logsql.CapabilitiesFor("1.40.0") // no native ipv4_range
	for b.Loop() {
		TranslateLogQLWithCapabilities(`{app="nginx"} | ip("10.0.0.0/8")`, nil, nil, caps)
	}
}

// PipeMath.String() microbench — pure AST serialization cost.
func BenchmarkPipeMathString(b *testing.B) {
	pipe := logsql.PipeMath{Alias: "__lvp_rate", Expr: "__lvp_inner/300"}
	for b.Loop() {
		_ = pipe.String()
	}
}

// PipeStats.String() microbench — stats pipe serialization.
func BenchmarkPipeStatsString(b *testing.B) {
	pipe := logsql.PipeStats{
		By:    []logsql.GroupKey{{Field: "app"}, {Field: "namespace"}},
		Funcs: []logsql.StatsFuncAlias{{Func: logsql.DeferredExpr{Raw: "count()"}, Alias: "__lvp_inner"}},
	}
	for b.Loop() {
		_ = pipe.String()
	}
}
