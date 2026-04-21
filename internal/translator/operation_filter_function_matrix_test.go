package translator

import (
	"fmt"
	"strings"
	"testing"
)

func TestMatrix_BinaryMetricOperators_AllSupported(t *testing.T) {
	ops := []string{"+", "-", "*", "/", "%", "^", "==", "!=", ">", "<", ">=", "<="}

	for _, op := range ops {
		t.Run("right_scalar_"+op, func(t *testing.T) {
			logql := fmt.Sprintf(`rate({app="x"}[5m]) %s 2`, op)
			got, err := TranslateLogQL(logql)
			if err != nil {
				t.Fatalf("translate right-scalar binary op: %v", err)
			}
			if !strings.HasPrefix(got, BinaryMetricPrefix+op+":") {
				t.Fatalf("expected binary prefix for %q, got %q", op, got)
			}

			parsedOp, left, right, _, ok := ParseBinaryMetricExprFull(got)
			if !ok {
				t.Fatalf("expected parseable binary metric expression, got %q", got)
			}
			if parsedOp != op {
				t.Fatalf("unexpected parsed op: got=%q want=%q", parsedOp, op)
			}
			if !strings.Contains(left, "__lvp_rate") {
				t.Fatalf("expected translated metric query on left, got %q", left)
			}
			if right != "2" {
				t.Fatalf("expected scalar RHS=2, got %q", right)
			}
		})

		t.Run("left_scalar_"+op, func(t *testing.T) {
			logql := fmt.Sprintf(`2 %s rate({app="x"}[5m])`, op)
			got, err := TranslateLogQL(logql)
			if err != nil {
				t.Fatalf("translate left-scalar binary op: %v", err)
			}
			if !strings.HasPrefix(got, BinaryMetricPrefix+op+":") {
				t.Fatalf("expected binary prefix for %q, got %q", op, got)
			}

			parsedOp, left, right, _, ok := ParseBinaryMetricExprFull(got)
			if !ok {
				t.Fatalf("expected parseable binary metric expression, got %q", got)
			}
			if parsedOp != op {
				t.Fatalf("unexpected parsed op: got=%q want=%q", parsedOp, op)
			}
			if left != "2" {
				t.Fatalf("expected scalar LHS=2, got %q", left)
			}
			if !strings.Contains(right, "__lvp_rate") {
				t.Fatalf("expected translated metric query on right, got %q", right)
			}
		})
	}
}

func TestMatrix_FilterOperators_AllSupported(t *testing.T) {
	cases := []struct {
		name           string
		logql          string
		wantSubstring  string
		secondaryCheck string
	}{
		{name: "line_contains", logql: `{app="x"} |= "error"`, wantSubstring: `~"error"`},
		{name: "line_not_contains", logql: `{app="x"} != "debug"`, wantSubstring: `NOT ~"debug"`},
		{name: "line_regex", logql: `{app="x"} |~ "err.*"`, wantSubstring: `~"err.*"`},
		{name: "line_not_regex", logql: `{app="x"} !~ "dbg.*"`, wantSubstring: `NOT ~"dbg.*"`},
		{name: "label_eq", logql: `{app="x"} | status == 200`, wantSubstring: `status:=200`},
		{name: "label_assign_eq", logql: `{app="x"} | status = 200`, wantSubstring: `status:=200`},
		{name: "label_ne", logql: `{app="x"} | status != 200`, wantSubstring: `-status:=200`},
		{name: "label_re", logql: `{app="x"} | status =~ "2.."`, wantSubstring: `status:~"2.."`},
		{name: "label_not_re", logql: `{app="x"} | status !~ "5.."`, wantSubstring: `-status:~"5.."`},
		{name: "label_gt", logql: `{app="x"} | duration_ms > 100`, wantSubstring: `duration_ms:>100`},
		{name: "label_lt", logql: `{app="x"} | duration_ms < 100`, wantSubstring: `duration_ms:<100`},
		{name: "label_gte", logql: `{app="x"} | duration_ms >= 100`, wantSubstring: `duration_ms:>=100`},
		{name: "label_lte", logql: `{app="x"} | duration_ms <= 100`, wantSubstring: `duration_ms:<=100`},
		{name: "and_split", logql: `{app="x"} | status >= 500 and method="GET"`, wantSubstring: " and ", secondaryCheck: "method:=GET"},
		{name: "or_split", logql: `{app="x"} | status >= 500 or status = 429`, wantSubstring: " or ", secondaryCheck: "status:=429"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("translate filter query: %v", err)
			}
			if !strings.Contains(got, tc.wantSubstring) {
				t.Fatalf("expected translation to contain %q, got %q", tc.wantSubstring, got)
			}
			if tc.secondaryCheck != "" && !strings.Contains(got, tc.secondaryCheck) {
				t.Fatalf("expected translation to contain %q, got %q", tc.secondaryCheck, got)
			}
		})
	}
}

func TestMatrix_LokiPipeFunctions_Supported(t *testing.T) {
	cases := []struct {
		name          string
		logql         string
		wantFragments []string
	}{
		{name: "json", logql: `{app="x"} | json`, wantFragments: []string{"| unpack_json"}},
		{name: "logfmt", logql: `{app="x"} | logfmt`, wantFragments: []string{"| unpack_logfmt"}},
		{name: "line_format", logql: `{app="x"} | line_format "{{.app}}"`, wantFragments: []string{`| format "<app>"`}},
		{name: "label_format_multi", logql: `{app="x"} | label_format app_name="{{.app}}", level_name="{{.level}}"`, wantFragments: []string{`| format "<app>" as app_name`, `| format "<level>" as level_name`}},
		{name: "decolorize", logql: `{app="x"} | decolorize`, wantFragments: []string{"| decolorize"}},
		{name: "drop", logql: `{app="x"} | drop temp`, wantFragments: []string{"| delete temp"}},
		{name: "keep", logql: `{app="x"} | keep app, level`, wantFragments: []string{"| fields _time, _msg, _stream, app, level"}},
		{name: "pattern", logql: `{app="x"} | pattern "<method> <path>"`, wantFragments: []string{"| extract "}},
		{name: "regexp", logql: `{app="x"} | regexp "(?P<method>GET|POST)"`, wantFragments: []string{"| extract_regexp "}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("translate function pipe: %v", err)
			}
			for _, want := range tc.wantFragments {
				if !strings.Contains(got, want) {
					t.Fatalf("expected %q in translation, got %q", want, got)
				}
			}
		})
	}
}

func TestMatrix_MetricFunctions_AllSupported(t *testing.T) {
	cases := []struct {
		name          string
		logql         string
		wantFragments []string
	}{
		{name: "rate", logql: `rate({app="x"}[5m])`, wantFragments: []string{"| stats by (_stream) count() as __lvp_inner", "| math __lvp_inner/300 as __lvp_rate", "| stats by (_stream) sum(__lvp_rate)"}},
		{name: "count_over_time", logql: `count_over_time({app="x"}[5m])`, wantFragments: []string{"| stats count()"}},
		{name: "bytes_over_time", logql: `bytes_over_time({app="x"}[5m])`, wantFragments: []string{"| stats sum_len(_msg)"}},
		{name: "bytes_rate", logql: `bytes_rate({app="x"}[5m])`, wantFragments: []string{"| stats by (_stream) sum_len(_msg) as __lvp_inner", "| math __lvp_inner/300 as __lvp_rate", "| stats by (_stream) sum(__lvp_rate)"}},
		{name: "sum_over_time", logql: `sum_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats sum(duration)"}},
		{name: "avg_over_time", logql: `avg_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats avg(duration)"}},
		{name: "max_over_time", logql: `max_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats max(duration)"}},
		{name: "min_over_time", logql: `min_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats min(duration)"}},
		{name: "first_over_time", logql: `first_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats first(duration)"}},
		{name: "last_over_time", logql: `last_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats last(duration)"}},
		{name: "stddev_over_time", logql: `stddev_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats stddev(duration)"}},
		{name: "stdvar_over_time", logql: `stdvar_over_time({app="x"} | unwrap duration [5m])`, wantFragments: []string{BinaryMetricPrefix + "^:", "| stats stddev(duration)", "|||2"}},
		{name: "absent_over_time", logql: `absent_over_time({app="x"}[5m])`, wantFragments: []string{"| stats count()"}},
		{name: "quantile_over_time", logql: `quantile_over_time(0.95, {app="x"} | unwrap duration [5m])`, wantFragments: []string{"| stats quantile(0.95, duration)"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("translate metric function: %v", err)
			}
			for _, want := range tc.wantFragments {
				if !strings.Contains(got, want) {
					t.Fatalf("expected %q in translation, got %q", want, got)
				}
			}
		})
	}
}
