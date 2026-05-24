package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestFilterNodeString(t *testing.T) {
	tests := []struct {
		name string
		node logsql.FilterExpr
		want string
	}{
		{"word", logsql.Word{Value: "error"}, "error"},
		{"phrase", logsql.Phrase{Value: "hello world"}, `"hello world"`},
		{"prefix", logsql.Prefix{Value: "err"}, "err*"},
		{"substring", logsql.Substring{Value: "error"}, "*error*"},
		{"exact", logsql.Exact{Value: "404"}, `="404"`},
		{"regexp", logsql.Regexp{Pattern: "error|warn"}, `~"error|warn"`},
		{"sequence", logsql.Sequence{Parts: []string{"a", "b"}}, `seq("a","b")`},
		{"case_insensitive", logsql.CaseInsensitive{Value: "error"}, `i("error")`},
		{"wildcard", logsql.Wildcard{}, "*"},
		{"field_exact", logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"}, `app:="nginx"`},
		{"field_regexp", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpRegexp, Value: "err.*"}, `level:~"err.*"`},
		{"field_prefix", logsql.FieldFilter{Field: "url", Op: logsql.FieldOpPrefix, Value: "/api"}, "url:/api*"},
		{"field_substring", logsql.FieldFilter{Field: "url", Op: logsql.FieldOpSubstring, Value: "login"}, "url:*login*"},
		{"field_empty", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpEmpty}, `level:""`},
		{"field_any", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpAny}, "level:*"},
		{"field_gt", logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGT, Value: "400"}, "status:>400"},
		{"field_gte", logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGTE, Value: "500"}, "status:>=500"},
		{"field_lt", logsql.FieldFilter{Field: "latency", Op: logsql.FieldOpLT, Value: "100"}, "latency:<100"},
		{"field_lte", logsql.FieldFilter{Field: "latency", Op: logsql.FieldOpLTE, Value: "200"}, "latency:<=200"},
		{"field_range", logsql.FieldFilter{Field: "latency", Op: logsql.FieldOpRange, Value: "100,500"}, "latency:range(100,500)"},
		{"field_in", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpIn, Value: "error,warn"}, "level:in(error,warn)"},
		{"field_negate", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpExact, Value: "debug", Negate: true}, `NOT level:="debug"`},
		{
			"stream_filter",
			logsql.StreamFilter{Matchers: []logsql.LabelMatcher{
				{Name: "app", Op: "=", Value: "nginx"},
				{Name: "env", Op: "=", Value: "prod"},
			}},
			`{app="nginx", env="prod"}`,
		},
		{"time_duration", logsql.TimeFilter{Range: "5m"}, "_time:5m"},
		{"time_absolute", logsql.TimeFilter{Range: "[2024-01-01,2024-01-02]"}, "_time:[2024-01-01,2024-01-02]"},
		{
			"and_flat",
			logsql.AndExpr{
				Left:  logsql.Word{Value: "error"},
				Right: logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"},
			},
			`error AND app:="nginx"`,
		},
		{
			"and_wraps_or",
			logsql.AndExpr{
				Left:  logsql.OrExpr{Left: logsql.Word{Value: "error"}, Right: logsql.Word{Value: "warn"}},
				Right: logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"},
			},
			`(error OR warn) AND app:="nginx"`,
		},
		{
			"or_expr",
			logsql.OrExpr{Left: logsql.Word{Value: "error"}, Right: logsql.Word{Value: "warn"}},
			"error OR warn",
		},
		{"not_word", logsql.NotExpr{Expr: logsql.Word{Value: "debug"}}, "NOT debug"},
		{
			"not_or",
			logsql.NotExpr{Expr: logsql.OrExpr{Left: logsql.Word{Value: "a"}, Right: logsql.Word{Value: "b"}}},
			"NOT (a OR b)",
		},
		{"deferred", logsql.DeferredExpr{Raw: "__binary__:+:left|||right"}, "__binary__:+:left|||right"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.node.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQueryString(t *testing.T) {
	q := logsql.Query{
		Filter: logsql.AndExpr{
			Left:  logsql.FieldFilter{Field: "app", Op: logsql.FieldOpExact, Value: "nginx"},
			Right: logsql.Word{Value: "error"},
		},
		Pipes: []logsql.Pipe{
			logsql.PipeUnpackJSON{},
			logsql.PipeFilter{Expr: logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGTE, Value: "500"}},
		},
	}
	want := `app:="nginx" AND error | unpack_json | filter status:>=500`
	if got := q.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestQueryNilFilter(t *testing.T) {
	q := logsql.Query{Pipes: []logsql.Pipe{logsql.PipeLimit{N: 100}}}
	want := "* | limit 100"
	if got := q.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPipeString(t *testing.T) {
	tests := []struct {
		name string
		pipe logsql.Pipe
		want string
	}{
		{"unpack_json", logsql.PipeUnpackJSON{}, "| unpack_json"},
		{"unpack_json_from", logsql.PipeUnpackJSON{From: "_msg"}, "| unpack_json from _msg"},
		{"unpack_logfmt", logsql.PipeUnpackLogfmt{}, "| unpack_logfmt"},
		{"unpack_logfmt_from", logsql.PipeUnpackLogfmt{From: "_msg"}, "| unpack_logfmt from _msg"},
		{"extract", logsql.PipeExtract{Pattern: "<_> <level>", From: "_msg"}, `| extract "<_> <level>" from _msg`},
		{"extract_if", logsql.PipeExtract{Pattern: "<level>", From: "_msg", If: `level:*`}, `| extract "<level>" from _msg if (level:*)`},
		{"extract_regexp", logsql.PipeExtractRegexp{Pattern: `(?P<level>\w+)`, From: "_msg"}, "| extract_regexp `(?P<level>\\w+)` from _msg"},
		{"filter", logsql.PipeFilter{Expr: logsql.FieldFilter{Field: "status", Op: logsql.FieldOpGTE, Value: "500"}}, "| filter status:>=500"},
		{"fields", logsql.PipeFields{Labels: []string{"level", "status"}}, "| fields level, status"},
		{"delete", logsql.PipeDelete{Labels: []string{"debug", "trace"}}, "| delete debug, trace"},
		{"format", logsql.PipeFormat{Template: "<level> <status>", ResultField: "_msg"}, `| format "<level> <status>" as _msg`},
		{"rename", logsql.PipeRename{Pairs: [][2]string{{"old", "new"}}}, "| rename old as new"},
		{"rename_multi", logsql.PipeRename{Pairs: [][2]string{{"a", "b"}, {"c", "d"}}}, "| rename a as b, c as d"},
		{"replace", logsql.PipeReplace{Field: "level", Old: "warn", New: "warning"}, `| replace (level, "warn", "warning")`},
		{"replace_regexp", logsql.PipeReplaceRegexp{Field: "url", Regex: `https?://`, Replacement: ""}, "| replace_regexp (url, `https?://`, \"\")"},
		{"pack_json", logsql.PipePackJSON{Fields: []string{"a", "b"}, ResultField: "packed"}, "| pack_json fields (a, b) as packed"},
		{"pack_logfmt", logsql.PipePackLogfmt{Fields: []string{"a", "b"}, ResultField: "packed"}, "| pack_logfmt fields (a, b) as packed"},
		{"limit", logsql.PipeLimit{N: 100}, "| limit 100"},
		{
			"sort_desc_limit",
			logsql.PipeSort{By: []logsql.SortField{{Field: "count", Desc: true}}, Limit: 10},
			"| sort by (count desc) limit 10",
		},
		{
			"sort_asc_nolimit",
			logsql.PipeSort{By: []logsql.SortField{{Field: "ts", Desc: false}}},
			"| sort by (ts)",
		},
		{"math", logsql.PipeMath{Expr: "rate/total*100", Alias: "pct"}, "| math pct:=rate/total*100"},
		{
			"stats_count",
			logsql.PipeStats{
				By:    []logsql.GroupKey{{Field: "host"}},
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "cnt"}},
			},
			"| stats by (host) count() as cnt",
		},
		{
			"stats_multi",
			logsql.PipeStats{
				By: []logsql.GroupKey{{Field: "app"}, {Field: "env"}},
				Funcs: []logsql.StatsFuncAlias{
					{Func: logsql.Sum{Field: "bytes"}, Alias: "total"},
					{Func: logsql.Max{Field: "latency"}, Alias: "max_lat"},
				},
			},
			"| stats by (app, env) sum(bytes) as total, max(latency) as max_lat",
		},
		{
			"stats_no_by",
			logsql.PipeStats{
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "total"}},
			},
			"| stats count() as total",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.pipe.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStatsFuncString(t *testing.T) {
	tests := []struct {
		name string
		fn   logsql.StatsFunc
		want string
	}{
		{"count", logsql.Count{}, "count()"},
		{"sum", logsql.Sum{Field: "bytes"}, "sum(bytes)"},
		{"min", logsql.Min{Field: "latency"}, "min(latency)"},
		{"max", logsql.Max{Field: "latency"}, "max(latency)"},
		{"avg", logsql.Avg{Field: "latency"}, "avg(latency)"},
		{"median", logsql.Median{Field: "latency"}, "median(latency)"},
		{"quantile", logsql.Quantile{Phi: 0.99, Field: "latency"}, "quantile(0.99, latency)"},
		{"stddev", logsql.Stddev{Field: "latency"}, "stddev(latency)"},
		{"stdvar", logsql.Stdvar{Field: "latency"}, "stdvar(latency)"},
		{"rate", logsql.Rate{}, "rate()"},
		{"rate_sum", logsql.RateSum{Field: "bytes"}, "rate_sum(bytes)"},
		{"count_uniq", logsql.CountUniq{Field: "user_id"}, "count_uniq(user_id)"},
		{"count_uniq_hash", logsql.CountUniqHash{Field: "user_id"}, "count_uniq_hash(user_id)"},
		{"uniq_values", logsql.UniqValues{Field: "user_id", Limit: 100}, "uniq_values(user_id, 100)"},
		{"field_max", logsql.FieldMax{Field: "latency"}, "field_max(latency)"},
		{"field_min", logsql.FieldMin{Field: "latency"}, "field_min(latency)"},
		{"json_values", logsql.JSONValues{Field: "data"}, "json_values(data)"},
		{"any", logsql.Any{Field: "user_id"}, "any(user_id)"},
		{"count_empty", logsql.CountEmpty{Field: "level"}, "count_empty(level)"},
		{"sum_len", logsql.SumLen{Field: "_msg"}, "sum_len(_msg)"},
		{"values", logsql.Values{Field: "status", Limit: 10}, "values(status, 10)"},
		{"histogram", logsql.Histogram{Field: "latency"}, "histogram(latency)"},
		{"last", logsql.Last{Field: "_msg"}, "last(_msg)"},
		{"first", logsql.First{Field: "_msg"}, "first(_msg)"},
		{"row_any", logsql.RowAny{Fields: []string{"_msg", "level"}}, "row_any(_msg, level)"},
		{"row_max", logsql.RowMax{By: "latency", Fields: []string{"_msg", "status"}}, "row_max(latency, _msg, status)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fn.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
