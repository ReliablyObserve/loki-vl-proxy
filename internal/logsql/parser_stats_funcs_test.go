package logsql_test

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// Every stats function VictoriaLogs takes parses to its typed node and writes back as it was read.
// conformance: architecture-syntax-package-layering
func TestParseStatsFunctionsRoundTrip(t *testing.T) {
	for _, fn := range []string{
		"count()", "count_empty(x)", "sum(x)", "sum_len(x)", "min(x)", "max(x)", "avg(x)", "median(x)", "stddev(x)", "histogram(x)",
		"first(x)", "last(x)", "any(x)", "rate()", "rate_sum(x)",
		"count_uniq(x)", "count_uniq(a, b)", "count_uniq_hash(x)", "field_max(x, a, b)", "field_min(x, a)",
		"quantile(0.99, x)", "quantile(0, x)", "quantile(1, x)",
		"json_values(x)", "json_values_sorted(x)", "json_values_topk(x, 3)",
		"uniq_values(x, 4)", "values(x, 2)",
		"row_any(a, b)", "row_min(_time, a, b)", "row_max(_time, a)", "row_min(_time)",
	} {
		query := "* | stats " + fn + " as r"
		q, err := logsql.Parse(query)
		if err != nil {
			t.Errorf("%s: %v", fn, err)
			continue
		}
		if got := q.String(); got != query {
			t.Errorf("%s: written back as %s", fn, got)
		}
	}
	// A function name is read in any case, and count(*) is count().
	for in, want := range map[string]string{"COUNT(*)": "count()", "Sum(x)": "sum(x)", "ROW_ANY(a)": "row_any(a)", "Quantile(0.5, x)": "quantile(0.5, x)"} {
		q, err := logsql.Parse("* | stats " + in)
		if err != nil || q.String() != "* | stats "+want {
			t.Errorf("%s: %v, %v", in, q, err)
		}
	}
	// A stats function that takes a field names it quoted when it holds a character LogsQL needs quoted.
	if q, err := logsql.Parse(`* | stats json_values_topk("a b", 3), values("c.d", 2), uniq_values("e f", 1)`); err != nil || !strings.Contains(q.String(), `json_values_topk("a b", 3), values("c.d", 2), uniq_values("e f", 1)`) {
		t.Errorf("quoted names: %v, %v", q, err)
	}
}

// A malformed stats function is refused with its name in the error.
// conformance: architecture-syntax-package-layering
func TestParseStatsFunctionErrors(t *testing.T) {
	for _, c := range []struct{ fn, want string }{
		{"row_any a", "expected ( after row_any"},
		{"row_max _time", "expected ( after row_max"},
		{"row_min _time", "expected ( after row_min"},
		{"quantile(2, x)", `stats func "quantile": phi must be a number from 0 to 1, got "2"`},
		{"quantile(-0.5, x)", `stats func "quantile": phi must be a number from 0 to 1`},
		{"quantile(x, y)", `stats func "quantile": phi must be a number from 0 to 1, got "x"`},
		{"quantile 0.5", `stats func "quantile"`},
		{"quantile(0.5, x", `stats func "quantile"`},
		{"json_values_topk x", `stats func "json_values_topk"`},
		{"json_values_topk(", `stats func "json_values_topk"`},
		{"uniq_values x", `stats func "uniq_values"`},
		{"uniq_values(x, y)", `stats func "uniq_values": expected a limit, got "y"`},
		{"values(x, y)", `stats func "values": expected a limit, got "y"`},
		{"values x", `stats func "values"`},
		{"values(x, 2", `stats func "values"`},
		{"sum(*)", `stats func "sum": * is not modelled`},
		{"sum(a b)", `stats func "sum": expected , or ), got "b"`},
		{"sum(a, b)", `stats func "sum": needs exactly one field (2 given)`},
		{"sum()", `stats func "sum": needs exactly one field (0 given)`},
		{"sum", `stats func "sum"`},
		{"sum(a", `stats func "sum": expected , or ), got ""`},
		{"field_max(x)", `stats func "field_max": needs the field to compare and the fields to return`},
		{"field_min(x)", `stats func "field_min": needs the field to compare and the fields to return`},
		{"nosuchfn(x)", `unknown stats function "nosuchfn"`},
	} {
		q, err := logsql.Parse("* | stats " + c.fn)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("stats %s: got %v (%v), want an error with %q", c.fn, q, err, c.want)
		}
	}
}
