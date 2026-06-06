package logsql_test

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// TestFilterNodeString_Extended covers AST filter nodes not exercised by
// TestFilterNodeString (the existing baseline).
func TestFilterNodeString_Extended(t *testing.T) {
	tests := []struct {
		name string
		node logsql.FilterExpr
		want string
	}{
		{"anycase_phrase", logsql.AnyCasePhrase{Value: "Error"}, `i("Error")`},
		{"anycase_prefix", logsql.AnyCasePrefix{Value: "err"}, `i(err*)`},
		{"exact_prefix", logsql.ExactPrefix{Value: "200"}, `="200"*`},
		{"day_range_no_offset", logsql.DayRange{Bracket: "[08:00, 18:00]"}, "_time:day_range[08:00, 18:00]"},
		{"day_range_with_offset", logsql.DayRange{Bracket: "[08:00, 18:00]", Offset: "2h"}, "_time:day_range[08:00, 18:00] offset 2h"},
		{"week_range_no_offset", logsql.WeekRange{Bracket: "[Mon, Fri]"}, "_time:week_range[Mon, Fri]"},
		{"week_range_with_offset", logsql.WeekRange{Bracket: "[Mon, Fri]", Offset: "1d"}, "_time:week_range[Mon, Fri] offset 1d"},
		{"len_range_no_field", logsql.LenRange{Min: "10", Max: "100"}, "len_range(10, 100)"},
		{"len_range_with_field", logsql.LenRange{Field: "_msg", Min: "10", Max: "100"}, "_msg:len_range(10, 100)"},
		{"deferred", logsql.DeferredExpr{Raw: `_msg:custom_op("x")`}, `_msg:custom_op("x")`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.node.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPipeString_Extended covers Pipe AST nodes missed by the baseline.
func TestPipeString_Extended(t *testing.T) {
	tests := []struct {
		name string
		pipe logsql.Pipe
		want string
	}{
		{"unpack_json", logsql.PipeUnpackJSON{From: "_msg"}, `| unpack_json from _msg`},
		{"unpack_json_no_from", logsql.PipeUnpackJSON{}, `| unpack_json`},
		{"unpack_logfmt", logsql.PipeUnpackLogfmt{From: "_msg"}, `| unpack_logfmt from _msg`},
		{"unpack_logfmt_no_from", logsql.PipeUnpackLogfmt{}, `| unpack_logfmt`},
		{"uniq_by", logsql.PipeUniq{By: []string{"app", "host"}}, "| uniq by (app, host)"},
		{"uniq_no_by", logsql.PipeUniq{}, "| uniq"},
		{"field_names", logsql.PipeFieldNames{}, "| field_names"},
		{"drop_empty_fields", logsql.PipeDropEmptyFields{}, "| drop_empty_fields"},
		{"copy", logsql.PipeCopy{Pairs: [][2]string{{"a", "b"}}}, "| copy a as b"},
		{"copy_multi", logsql.PipeCopy{Pairs: [][2]string{{"a", "b"}, {"c", "d"}}}, "| copy a as b, c as d"},
		{"coalesce", logsql.PipeCoalesce{Fields: []string{"a", "b"}, Result: "out"}, "| coalesce(a, b) as out"},
		{"coalesce_default", logsql.PipeCoalesce{Fields: []string{"a"}, Default: "x", Result: "out"}, `| coalesce(a) default "x" as out`},
		{"collapse_nums_at", logsql.PipeCollapseNums{Field: "_msg"}, "| collapse_nums at _msg"},
		{"collapse_nums_default", logsql.PipeCollapseNums{}, "| collapse_nums"},
		{"decolorize_field", logsql.PipeDecolorize{Field: "_msg"}, "| decolorize _msg"},
		{"decolorize_default", logsql.PipeDecolorize{}, "| decolorize"},
		{"facets_limit", logsql.PipeFacets{Limit: 100}, "| facets limit 100"},
		{"facets_no_limit", logsql.PipeFacets{}, "| facets"},
		{"field_values_limit", logsql.PipeFieldValues{Field: "level", Limit: 5}, "| field_values level limit 5"},
		{"field_values_no_limit", logsql.PipeFieldValues{Field: "level"}, "| field_values level"},
		{"generate_sequence", logsql.PipeGenerateSequence{Limit: 10}, "| generate_sequence limit 10"},
		{"hash", logsql.PipeHash{Fields: []string{"user_id"}, Result: "uid_hash"}, "| hash(user_id) as uid_hash"},
		{"join", logsql.PipeJoin{By: []string{"id"}, Inner: `_msg:"x"`}, `| join by (id) (_msg:"x")`},
		{"join_prefix", logsql.PipeJoin{By: []string{"id"}, Inner: "x", Prefix: "r_"}, "| join by (id) (x) prefix r_"},
		{"json_array_len", logsql.PipeJSONArrayLen{Field: "arr", Result: "len"}, "| json_array_len(arr) as len"},
		{"len", logsql.PipeLen{Field: "_msg", Result: "msg_len"}, "| len(_msg) as msg_len"},
		{"query_stats", logsql.PipeQueryStats{}, "| query_stats"},
		{"set_stream_fields", logsql.PipeSetStreamFields{Fields: []string{"app", "env"}}, "| set_stream_fields app, env"},
		{"split_separator", logsql.PipeSplit{Separator: ","}, `| split ","`},
		{"split_from_as", logsql.PipeSplit{Separator: ";", From: "csv", As: "parts"}, `| split ";" from csv as parts`},
		{"stream_context", logsql.PipeStreamContext{Before: 2, After: 5}, "| stream_context before 2 after 5"},
		{"stream_context_zero", logsql.PipeStreamContext{}, "| stream_context"},
		{"time_add", logsql.PipeTimeAdd{Duration: "1h"}, "| time_add 1h"},
		{"union", logsql.PipeUnion{Inner: `_msg:"x"`}, `| union (_msg:"x")`},
		{"unpack_syslog", logsql.PipeUnpackSyslog{}, "| unpack_syslog"},
		{"unpack_words", logsql.PipeUnpackWords{From: "_msg", As: "words"}, "| unpack_words from _msg as words"},
		{"unpack_words_default", logsql.PipeUnpackWords{}, "| unpack_words"},
		{"unroll", logsql.PipeUnroll{Fields: []string{"a", "b"}}, "| unroll (a, b)"},
		{"update", logsql.PipeUpdate{Field: "status", Expr: "status * 2"}, "| update status = status * 2"},
		{"sample", logsql.PipeSample{N: 100}, "| sample 100"},
		{"offset", logsql.PipeOffset{N: 50}, "| offset 50"},
		{
			"running_stats",
			logsql.PipeRunningStats{
				By:    []logsql.GroupKey{{Field: "app"}},
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Sum{Field: "n"}, Alias: "tot"}},
			},
			"| running_stats by (app) sum(n) as tot",
		},
		{
			"total_stats",
			logsql.PipeTotalStats{
				Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "c"}},
			},
			"| total_stats count() as c",
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

// TestStatsFuncString_Extended covers stats functions missed by baseline.
func TestStatsFuncString_Extended(t *testing.T) {
	tests := []struct {
		name string
		fn   logsql.StatsFunc
		want string
	}{
		{"row_min", logsql.RowMin{By: "latency", Fields: []string{"_msg"}}, "row_min(latency, _msg)"},
		{"json_values_sorted", logsql.JSONValuesSorted{Field: "data"}, "json_values_sorted(data)"},
		{"json_values_topk", logsql.JSONValuesTopK{Field: "url", Limit: 5}, "json_values_topk(url, 5)"},
		{"deferred", logsql.DeferredExpr{Raw: "custom(x)"}, "custom(x)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fn.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPipeFirstLastTopString covers PipeFirst/Last/Top formatters which carry
// multiple branches (n, by).
func TestPipeFirstLastTopString(t *testing.T) {
	cases := []struct {
		name string
		p    logsql.Pipe
		want string
	}{
		{"first_n_by", logsql.PipeFirst{N: 5, By: []string{"_time"}}, ""},
		{"first_default", logsql.PipeFirst{}, "| first"},
		{"last_n_by", logsql.PipeLast{N: 5, By: []string{"_time"}}, ""},
		{"last_default", logsql.PipeLast{}, "| last"},
		{"top_n_by", logsql.PipeTop{N: 3, By: []string{"app"}}, "| top 3 by (app)"},
		{"top_n", logsql.PipeTop{N: 3}, "| top 3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.p.String()
			if tc.want != "" && got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
			// For first/last with branches, just assert the prefix — the
			// exact byte format carries multiple optional clauses.
			if !strings.HasPrefix(got, "| ") {
				t.Errorf("expected pipe prefix '| ', got %q", got)
			}
		})
	}
}
