package logql

import "testing"

// Loki v3.7.9's own answers (recorded with the test oracle under test/oracle/loki, `facts`; no Loki code) for every
// range function with and without unwrap and a by clause, literals beside a set operator, duration units, the @
// modifier and label_replace regexes: "" where Loki accepts the query, its error text where it rejects it.
// conformance: semantics/proxy-accepts-invalid-logql
func TestRangeAggregationRulesLikeLoki(t *testing.T) {
	for _, c := range []struct{ query, lokiErr string }{
		{"count_over_time({a=\"x\"}[5m])", ""},
		{"count_over_time({a=\"x\"} | unwrap v [5m])", "parse error : invalid aggregation count_over_time with unwrap"},
		{"count_over_time({a=\"x\"} | unwrap v [5m]) by (b)", "parse error : grouping not allowed for count_over_time aggregation"},
		{"count_over_time({a=\"x\"}[5m]) by (b)", "parse error : grouping not allowed for count_over_time aggregation"},
		{"rate({a=\"x\"}[5m])", ""},
		{"rate({a=\"x\"} | unwrap v [5m])", ""},
		{"rate({a=\"x\"} | unwrap v [5m]) by (b)", "parse error : grouping not allowed for rate aggregation"},
		{"rate({a=\"x\"}[5m]) by (b)", "parse error : grouping not allowed for rate aggregation"},
		{"bytes_over_time({a=\"x\"}[5m])", ""},
		{"bytes_over_time({a=\"x\"} | unwrap v [5m])", "parse error : invalid aggregation bytes_over_time with unwrap"},
		{"bytes_over_time({a=\"x\"} | unwrap v [5m]) by (b)", "parse error : grouping not allowed for bytes_over_time aggregation"},
		{"bytes_over_time({a=\"x\"}[5m]) by (b)", "parse error : grouping not allowed for bytes_over_time aggregation"},
		{"bytes_rate({a=\"x\"}[5m])", ""},
		{"bytes_rate({a=\"x\"} | unwrap v [5m])", "parse error : invalid aggregation bytes_rate with unwrap"},
		{"bytes_rate({a=\"x\"} | unwrap v [5m]) by (b)", "parse error : grouping not allowed for bytes_rate aggregation"},
		{"bytes_rate({a=\"x\"}[5m]) by (b)", "parse error : grouping not allowed for bytes_rate aggregation"},
		{"absent_over_time({a=\"x\"}[5m])", ""},
		{"absent_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"absent_over_time({a=\"x\"} | unwrap v [5m]) by (b)", "parse error : grouping not allowed for absent_over_time aggregation"},
		{"absent_over_time({a=\"x\"}[5m]) by (b)", "parse error : grouping not allowed for absent_over_time aggregation"},
		{"rate_counter({a=\"x\"}[5m])", "parse error : invalid aggregation rate_counter without unwrap"},
		{"rate_counter({a=\"x\"} | unwrap v [5m])", ""},
		{"rate_counter({a=\"x\"} | unwrap v [5m]) by (b)", "parse error : grouping not allowed for rate_counter aggregation"},
		{"rate_counter({a=\"x\"}[5m]) by (b)", "parse error : grouping not allowed for rate_counter aggregation"},
		{"sum_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation sum_over_time without unwrap"},
		{"sum_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"sum_over_time({a=\"x\"} | unwrap v [5m]) by (b)", "parse error : grouping not allowed for sum_over_time aggregation"},
		{"sum_over_time({a=\"x\"}[5m]) by (b)", "parse error : grouping not allowed for sum_over_time aggregation"},
		{"avg_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation avg_over_time without unwrap"},
		{"avg_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"avg_over_time({a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"avg_over_time({a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation avg_over_time without unwrap"},
		{"min_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation min_over_time without unwrap"},
		{"min_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"min_over_time({a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"min_over_time({a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation min_over_time without unwrap"},
		{"max_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation max_over_time without unwrap"},
		{"max_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"max_over_time({a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"max_over_time({a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation max_over_time without unwrap"},
		{"stddev_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation stddev_over_time without unwrap"},
		{"stddev_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"stddev_over_time({a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"stddev_over_time({a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation stddev_over_time without unwrap"},
		{"stdvar_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation stdvar_over_time without unwrap"},
		{"stdvar_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"stdvar_over_time({a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"stdvar_over_time({a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation stdvar_over_time without unwrap"},
		{"first_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation first_over_time without unwrap"},
		{"first_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"first_over_time({a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"first_over_time({a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation first_over_time without unwrap"},
		{"last_over_time({a=\"x\"}[5m])", "parse error : invalid aggregation last_over_time without unwrap"},
		{"last_over_time({a=\"x\"} | unwrap v [5m])", ""},
		{"last_over_time({a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"last_over_time({a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation last_over_time without unwrap"},
		{"quantile_over_time(0.5, {a=\"x\"}[5m])", "parse error : invalid aggregation quantile_over_time without unwrap"},
		{"quantile_over_time(0.5, {a=\"x\"} | unwrap v [5m])", ""},
		{"quantile_over_time(0.5, {a=\"x\"} | unwrap v [5m]) by (b)", ""},
		{"quantile_over_time(0.5, {a=\"x\"}[5m]) by (b)", "parse error : invalid aggregation quantile_over_time without unwrap"},
		{"1 unless vector(2)", "parse error : unexpected literal for left leg of logical/set binary operation (unless): 1.000000"},
		{"vector(2) unless 1", "parse error : unexpected literal for right leg of logical/set binary operation (unless): 1.000000"},
		{"1 and 2", "parse error : unexpected literal for left leg of logical/set binary operation (and): 1.000000"},
		{"vector(1) or vector(2)", ""},
		{"sum(rate({a=\"x\"}[5m])) and 1", "parse error : unexpected literal for right leg of logical/set binary operation (and): 1.000000"},
		{"rate({a=\"x\"}[5minutes])", "parse error at line 0, col 13: unknown unit \"minutes\" in duration \"5minutes\""},
		{"rate({a=\"x\"}[5mins])", "parse error at line 0, col 13: unknown unit \"mins\" in duration \"5mins\""},
		{"rate({a=\"x\"}[5M])", "parse error at line 0, col 13: unknown unit \"M\" in duration \"5M\""},
		{"rate({a=\"x\"}[1H])", "parse error at line 0, col 13: unknown unit \"H\" in duration \"1H\""},
		{"rate({a=\"x\"}[5min])", "parse error at line 0, col 13: unknown unit \"min\" in duration \"5min\""},
		{"rate({a=\"x\"}[5m] @ start())", "parse error at line 1, col 18: syntax error: unexpected IDENTIFIER, expecting )"},
		{"label_replace(rate({a=\"x\"}[5m]), \"d\", \"$1\", \"a\", \"(x\")", "parse error : invalid regex in label_replace: error parsing regexp: missing closing ): `^(?:(x)$`"},
		{"label_replace(rate({a=\"x\"}[5m]), \"d\", \"$1\", \"a\", \"\")", ""},
		{"label_replace(rate({a=\"x\"}[5m]), \"d-x\", \"$1\", \"a\", \"(.*)\")", ""},
		{"{a=\"x\"} | bytes < 0B", "parse error at line 1, col 19: decimal literal has no digits"},
		{"{a=\"x\"} | b < 0B", "parse error at line 1, col 15: decimal literal has no digits"},
	} {
		t.Run(c.query, func(t *testing.T) {
			if _, msg := ValidateLogQLExpr(c.query); msg != c.lokiErr {
				t.Errorf("%q: %q, Loki %q", c.query, msg, c.lokiErr)
			}
		})
	}
}
