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
