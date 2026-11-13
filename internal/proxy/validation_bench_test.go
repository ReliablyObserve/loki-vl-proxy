package proxy

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// validationBenchShapes are queries longer than validationCacheMaxQueryBytes, so every call is a cache miss: the
// work a query the cache has not seen costs.
func validationBenchShapes() map[string]string {
	pad := strings.Repeat("x", validationCacheMaxQueryBytes)
	return map[string]string{
		"log":      `{app="` + pad + `"} |= "error" | json | level="error"`,
		"rate":     `sum by (pod) (rate({app="` + pad + `"}[5m]))`,
		"binary":   `sum(rate({app="` + pad + `"}[5m])) / sum(count_over_time({app="a"}[5m])) and on (pod) rate({app="b"}[1m])`,
		"quantile": `quantile_over_time(0.99, {app="` + pad + `"} | json | unwrap d [5m]) by (pod)`,
		"sorted":   `sort_desc(sum by (pod) (label_replace(rate({app="` + pad + `"}[5m]), "x", "$1", "pod", "(.*)")))`,
	}
}

func BenchmarkQueryValidationMiss(b *testing.B) {
	for name, q := range validationBenchShapes() {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = cachedQueryValidation(q)
			}
		})
	}
}

func BenchmarkQueryValidationHit(b *testing.B) {
	q := `sum by (pod) (rate({app="api"} | json | level="error" [5m]))`
	_ = cachedQueryValidation(q)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cachedQueryValidation(q)
	}
}

func BenchmarkMetadataQueryValidationMiss(b *testing.B) {
	pad := strings.Repeat("x", validationCacheMaxQueryBytes)
	for _, tc := range []struct{ name, endpoint, query string }{
		{"labels-selector", "labels", `{app="` + pad + `"}`},
		{"labels-metric", "labels", `sum(rate({app="` + pad + `"}[5m]))`},
		{"labels-pipeline", "labels", `{app="` + pad + `"} |= "x" | json`},
		{"labels-rejected", "labels", `{app=~"(` + pad + `"}`},
		{"labels-stage-error", "labels", `{app="` + pad + `"} |~ "("`},
		{"detected-fields-log", "detected_fields", `{app="` + pad + `"} | json`},
		{"detected-fields-metric", "detected_fields", `sum(rate({app="` + pad + `"}[5m]))`},
	} {
		r := httptest.NewRequest("GET", "/?"+url.Values{"query": {tc.query}}.Encode(), nil)
		_ = r.ParseForm()
		rule := lokiQueryParamRules[tc.endpoint]
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = lokiQueryParamError(rule, r, 0, nil)
			}
		})
	}
}
