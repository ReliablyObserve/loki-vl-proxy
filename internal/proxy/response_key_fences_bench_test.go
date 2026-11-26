package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// BenchmarkResponseKeyFences measures the fence chain every query, query_range and log window request builds,
// on the shapes clients send: a plain stream selector, a line filtered log query, a metric over a parser, the
// shapes that fire fences and a long dashboard query.
func BenchmarkResponseKeyFences(b *testing.B) {
	for _, tc := range []struct{ name, query string }{
		{"selector", `{app="a",env="prod"}`},
		{"line_filter", `{app="a",env="prod"} |= "error" != "timeout"`},
		{"json_logs", `{app="a"} | json | status >= 500 | line_format "{{.msg}}"`},
		{"rate_by", `sum by (level) (rate({app="a"} |= "error" [5m]))`},
		{"json_metric", `sum by (a) (count_over_time({app="a"} | json | drop __error__ [$__auto]))`},
		{"json_required_metric", `sum by (k) (count_over_time({app="a"} | json | drop __error__, __error_details__ | k!="" [1m]))`},
		{"mixed_required_metric", `sum by (k) (count_over_time({app="a"} | json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!="" [1m]))`},
		{"json_metric_parent", `max(sum by (k) (count_over_time({app="a"} | json | drop __error__, __error_details__ | k!="" [1m])))`},
		{"keep_unwrap", `sum by (x) (sum_over_time({app="a"} | json | keep x | unwrap n [5m]))`},
		{"keep_unwrap_replace", `label_replace(sum by (x) (sum_over_time({app="a"} | json | keep x | unwrap n [5m])), "p", "x-$1", "x", "(.*)")`},
		{"ip", `{app="a"} | json | addr = ip("10.0.0.0/8") | status = "500"`},
		{"escaped_regexp", `{app="a"} |~ "\\d+ ms" | json | status = "500"`},
		{"marker_bytes", `{app="a"} | o="\x02\x00\x02" | json`},
		{"numeric_equality", `{app="a"} | json | status = 500`},
		{"numeric_equality_metric", `sum(count_over_time({app="a"} | logfmt | status != 200 [5m]))`},
		{"reserved_metric", `sum(count_over_time({app="a"} | _time="10" [5m]))`},
		{"logfmt_presence", `sum(count_over_time({app="a"} | logfmt | msg != "" [5m]))`},
		{"topk_json", `topk(3, sum by (x) (count_over_time({app="a"} | json x="x" [5m])))`},
		{"unwrap_dynamic", `sum by (app) (avg_over_time({app="x"} | json | unwrap n [5m]))`},
		{"level_group", `sum by (detected_level) (count_over_time({app="a"} [$__auto]))`},
		{"level_group_json", `sum by (detected_level) (count_over_time({app="a"} | json | drop __error__, __error_details__ | detected_level != "" [$__auto]))`},
		{"level_after_json", `{app="a"} | json | detected_level="error"`},
		{"upper_case", `SUM BY (level) (RATE({app="API"} |= "Error" [5m]))`},
		{"topk_upper", `TOPK(3, sum by (x) (count_over_time({app="a"} | JSON [5m])))`},
		{"topk_binary", `topk(3, sum by (x) (rate({app="a"}[5m])) / sum by (x) (rate({app="b"}[5m])))`},
		{"sort_plain", `sort_desc(sum by (x) (rate({app="a"}[5m])))`},
		{"replace_ref", `label_replace(sum by (x) (rate({app="a"}[5m])), "p", "x-$1", "p", "(.*)")`},
		{"replace_ranking", `label_replace(topk(2, sum by (x) (rate({app="a"}[5m]))), "p", "x-$1", "x", "(.*)")`},
		{"topk_replace_variable", `topk(3, label_replace(rate({app="a"}[$__interval]), "p", "x-$1", "p", "(.*)") * 2)`},
		{"unwrap_replace_variable", `label_replace(sum by (x) (avg_over_time({app="a"} | json | unwrap n [$__range])), "p", "x-$1", "p", "(.*)")`},
		{"dashboard", `sum by (namespace, pod) (rate({cluster="prod", namespace=~"team-.+", app!="noisy"} |= "level=error" != "healthcheck" | logfmt | status >= 500 | line_format "{{.method}} {{.path}}" [$__rate_interval])) / sum by (namespace, pod) (rate({cluster="prod", namespace=~"team-.+", app!="noisy"} [$__rate_interval]))`},
	} {
		v := url.Values{"query": {tc.query}, "start": {"1700000000"}, "end": {"1700003600"}, "step": {"15"}}
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+v.Encode(), nil)
		r.ParseForm()
		for _, site := range []struct {
			name string
			site responseKeySite
		}{{"compat_range", responseKeyCompatQueryRange}, {"inner", responseKeyInnerQueryRange}} {
			b.Run(tc.name+"/"+site.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = responseKeyFences(r, tc.query, site.site)
				}
			})
		}
	}
}
