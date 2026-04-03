package translator

import "testing"

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
