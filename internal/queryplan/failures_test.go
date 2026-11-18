package queryplan

import "testing"

// The failure classes a request can end in are read from the plan: no data
// read (a number, vector(n)) fails in none; any read of log data can meet a
// failed backend or a limit; only a metric whose pipeline errors reach it (a
// parser error not dropped, an unwrap conversion) can end in a pipeline error.
//
// conformance: semantics/error-cache-plan-policy
func TestPlanFailures(t *testing.T) {
	read := FailBackend | FailLimit
	cases := []struct {
		q    string
		want Failures
	}{
		{`{app="api"}`, read},
		{`{app="api"} | json`, read}, // a log query keeps lines a parser fails on
		{`count_over_time({app="api"}[5m])`, read},
		{`sum(count_over_time({app="api"} | json [5m]))`, read | FailPipeline},
		{`sum(count_over_time({app="api"} | json | drop __error__ [5m]))`, read},
		{`sum(count_over_time({app="api"} | logfmt [5m]))`, read | FailPipeline},
		{`sum(rate({app="api"} | unwrap latency [5m]))`, read | FailPipeline},
		{`sum by (x) (count_over_time({app="api"}[1m])) / sum(count_over_time({app="b"} | json [1m]))`, read | FailPipeline},
		{`topk(3, sum by (x) (count_over_time({app="api"}[1m])))`, read},
		{`label_replace(rate({app="api"} | json [1m]), "a", "$1", "b", "(.*)")`, read | FailPipeline},
		{`vector(1)`, 0},
		{`1 + 2`, 0},
	}
	for _, c := range cases {
		got := resolved(t, c.q).Failures()
		if got != c.want {
			t.Errorf("%s: Failures() = %03b, want %03b", c.q, got, c.want)
		}
	}
	var nilPlan *Plan
	if nilPlan.Failures() != 0 {
		t.Fatal("a nil plan fails in no class")
	}
	if !(FailBackend | FailLimit).Has(FailBackend) || FailBackend.Has(FailLimit) {
		t.Fatal("Has")
	}
	if failuresOf(&LogSelect{}, true, maxDepth+1) != FailBackend {
		t.Fatal("a tree deeper than any plan claims only a backend failure")
	}
	if failuresOf(&Opaque{Raw: "approx_topk"}, true, 0) != read || failuresOf(&RangeAgg{}, false, 0) != 0 {
		t.Fatal("opaque and empty range")
	}
}
