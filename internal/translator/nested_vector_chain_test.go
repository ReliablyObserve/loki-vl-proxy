package translator

import (
	"strings"
	"testing"
)

// conformance: semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
// Every level of a nested vector aggregation keeps exactly its own grouping
// (pkg/logql/evaluator.go VectorAggEvaluator.Next), so each level is a stats
// pipe of its own over the rows of the previous one, one row per Loki series.
func TestNestedVectorAggregationEmitsOneStatsPipePerLevel(t *testing.T) {
	for _, tc := range []struct{ name, logql, want string }{
		{
			"outer by over a series-level inner",
			`min by (zone) (avg by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as __lvp_inner | stats by (zone) min(__lvp_inner)`,
		},
		{
			"outer by over a sum inner",
			`min by (zone) (sum by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone) count() as __lvp_inner | stats by (zone) min(__lvp_inner)`,
		},
		{
			"sum over sum keeps the outer by",
			`sum by (zone) (sum by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone) count() as __lvp_inner | stats by (zone) sum(__lvp_inner)`,
		},
		{
			"no outer grouping is one series",
			`min(avg by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as __lvp_inner | stats min(__lvp_inner)`,
		},
		{
			"by () is one series",
			`min by () (avg by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as __lvp_inner | stats min(__lvp_inner)`,
		},
		{
			"trailing by clause on the outer",
			`min(avg by (pod, zone) (count_over_time({app="x"}[5m]))) by (zone)`,
			`app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as __lvp_inner | stats by (zone) min(__lvp_inner)`,
		},
		{
			// Loki keeps only the labels a series has (evaluator.go:467-477): a
			// by label the inner level dropped is absent, so one series.
			"outer by label the inner group lacks",
			`min by (zone) (avg by (pod) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, _stream, level) count() as __lvp_inner | stats by (pod) avg(__lvp_inner) as __lvp_inner | stats min(__lvp_inner)`,
		},
		{
			"outer by keeps the labels both levels name",
			`max by (pod, zone, svc) (avg by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as __lvp_inner | stats by (pod, zone) max(__lvp_inner)`,
		},
		{
			"three levels",
			`max by (zone) (min by (pod, zone) (max by (pod, zone, svc) (rate({app="x"}[5m]))))`,
			`app:="x" | stats by (pod, zone, svc, _stream, level) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (pod, zone, svc) max(__lvp_rate) as __lvp_inner | stats by (pod, zone) min(__lvp_inner) as __lvp_inner | stats by (zone) max(__lvp_inner)`,
		},
		{
			"three levels, count in the middle",
			`sum by (zone) (count by (pod, zone) (max by (pod, zone, svc) (count_over_time({app="x"}[5m]))))`,
			`app:="x" | stats by (pod, zone, svc, _stream, level) count() as __lvp_inner | stats by (pod, zone, svc) max(__lvp_inner) as __lvp_inner | stats by (pod, zone) count() as __lvp_inner | stats by (zone) sum(__lvp_inner)`,
		},
		{
			"unwrap range aggregation",
			`min by (zone) (sum by (pod, zone) (rate({app="x"} | unwrap d [5m])))`,
			``, // asserted below by shape: the outer by is present
		},
		{
			// An outer without over a by level drops the excluded labels of a
			// known label set: a static by.
			"outer without over a by level",
			`min without (pod) (avg by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as __lvp_inner | stats by (zone) min(__lvp_inner)`,
		},
		{
			"middle without over a by level",
			`sum by (zone) (max without (svc) (avg by (pod, zone, svc) (count_over_time({app="x"}[5m]))))`,
			`app:="x" | stats by (pod, zone, svc, _stream, level) count() as __lvp_inner | stats by (pod, zone, svc) avg(__lvp_inner) as __lvp_inner | stats by (pod, zone) max(__lvp_inner) as __lvp_inner | stats by (zone) sum(__lvp_inner)`,
		},
		{
			"without naming a label the inner level lacks drops nothing",
			`min without (host) (avg by (pod, zone) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | stats by (pod, zone) avg(__lvp_inner) as __lvp_inner | stats by (pod, zone) min(__lvp_inner)`,
		},
		{
			"operand of a binary expression",
			`min by (zone) (avg by (pod, zone) (rate({app="x"}[5m]))) > 3`,
			BinaryMetricPrefix + `>:app:="x" | stats by (pod, zone, _stream, level) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (pod, zone) avg(__lvp_rate) as __lvp_inner | stats by (zone) min(__lvp_inner)|||3`,
		},
		{
			// The label set of the innermost without is the series' own labels,
			// unknown here: the proxy folds the levels over the series rows.
			"innermost without",
			`min by (zone) (avg without (pod) (count_over_time({app="x"}[5m])))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner | stats by (_stream, level) avg(__lvp_inner)` + WithoutMarkerSuffix + `min:pod`,
		},
	} {
		got, err := TranslateLogQL(tc.logql)
		if err != nil {
			t.Errorf("%s: %s: %v", tc.name, tc.logql, err)
			continue
		}
		if tc.want == "" {
			if !strings.Contains(got, "| stats by (pod, zone) sum(__lvp_rate) as __lvp_inner | stats by (zone) min(__lvp_inner)") {
				t.Errorf("%s: %s\n got %s", tc.name, tc.logql, got)
			}
			continue
		}
		if got != tc.want {
			t.Errorf("%s: %s\n got %s\nwant %s", tc.name, tc.logql, got, tc.want)
		}
	}
}

// A nested level the chain does not model (group, topk, stddev, a binary
// operand) keeps the translation it had.
func TestNestedVectorAggregationLeavesOtherShapesAlone(t *testing.T) {
	for _, logql := range []string{
		`min by (zone) (group by (pod, zone) (count_over_time({app="x"}[5m])))`,
		`min by (zone) (stddev by (pod, zone) (count_over_time({app="x"}[5m])))`,
		`sum(rate({app="x"}[5m])) / sum(rate({app="y"}[5m]))`,
	} {
		if _, _, _, ok := tryTranslateVectorChain(logql, nil); ok {
			t.Errorf("%s: handled by the chain", logql)
		}
	}
}

func TestSplitVectorLevel(t *testing.T) {
	for _, tc := range []struct {
		expr    string
		want    vectorLevel
		inner   string
		wantOK  bool
		comment string
	}{
		{`sum by (a, b) (x)`, vectorLevel{op: "sum", labels: []string{"a", "b"}}, `x`, true, "grouping before"},
		{`avg  without ( a ,b ) ( x )`, vectorLevel{op: "avg", labels: []string{"a", "b"}, without: true}, ` x `, true, "spaces"},
		{`min(x) by (a)`, vectorLevel{op: "min", labels: []string{"a"}}, `x`, true, "grouping after"},
		{`max (x) without (a)`, vectorLevel{op: "max", labels: []string{"a"}, without: true}, `x`, true, "without after"},
		{`count(x)`, vectorLevel{op: "count"}, `x`, true, "no grouping"},
		{`count by () (x)`, vectorLevel{op: "count"}, `x`, true, "empty by"},
		{`sum by (a) (f(g(x)))`, vectorLevel{op: "sum", labels: []string{"a"}}, `f(g(x))`, true, "nested parens"},
		{`sum(x) / sum(y)`, vectorLevel{}, ``, false, "binary expression"},
		{`sum by (a) (x) > 1`, vectorLevel{}, ``, false, "comparison"},
		{`sum by (a) (x) by (b)`, vectorLevel{}, ``, false, "two groupings"},
		{`topk(3, x)`, vectorLevel{}, ``, false, "parameter operator"},
		{`stddev by (a) (x)`, vectorLevel{}, ``, false, "operator the chain does not model"},
		{`sum_over_time(x)`, vectorLevel{}, ``, false, "range aggregation"},
		{`count_over_time(x)`, vectorLevel{}, ``, false, "range aggregation with a count prefix"},
		{`sum by (a (x)`, vectorLevel{}, ``, false, "unbalanced"},
	} {
		got, inner, ok := splitVectorLevel(tc.expr)
		if ok != tc.wantOK || (ok && (got.op != tc.want.op || got.without != tc.want.without || strings.Join(got.labels, ",") != strings.Join(tc.want.labels, ",") || inner != tc.inner)) {
			t.Errorf("%s (%s): got %+v %q %v, want %+v %q %v", tc.expr, tc.comment, got, inner, ok, tc.want, tc.inner, tc.wantOK)
		}
	}
}

func TestVectorLabelSetAlgebra(t *testing.T) {
	if got := intersectLabels([]string{"zone", "baz", "pod", "zone"}, []string{"pod", "zone"}); strings.Join(got, ",") != "zone,pod" {
		t.Errorf("intersectLabels = %v", got)
	}
	if got := intersectLabels([]string{"baz"}, nil); got != nil {
		t.Errorf("intersectLabels with nothing kept = %v", got)
	}
	if got := subtractLabels([]string{"pod", "zone", "svc"}, []string{"pod", "baz"}); strings.Join(got, ",") != "zone,svc" {
		t.Errorf("subtractLabels = %v", got)
	}
}

// conformance: semantics/nested-aggregation-outer-by-native-routes, metric-series-identity
// The word scan that lets a query with one aggregation skip the parse of the
// nest never says no to a nest (every spelling of the grouping) and costs a
// query that is not one nothing: no allocation.
func TestHasNestedVectorKeywords(t *testing.T) {
	for _, nest := range []string{
		`min by (zone) (avg by (pod, zone) (count_over_time({app="x"}[5m])))`,
		`min(avg by (pod, zone) (count_over_time({app="x"}[5m]))) by (zone)`,
		`min(avg(count_over_time({app="x"}[5m])) by (pod, zone)) by (zone)`,
		`min by(zone)(avg by(pod,zone)(count_over_time({app="x"}[5m])))`,
		"max by (zone)\n\t(sum without (pod)\n(rate({app=\"x\"}[5m])))",
		`topk(2, max(sum without (pod) (rate({app="x"}[5m]))))`,
		`max by (zone) (min by (pod, zone) (max by (pod, zone, svc) (count_over_time({app="x"}[5m]))))`,
		`sort(count by (zone) (avg without (svc) (count_over_time({app="x"}[5m]))))`,
		`label_replace(sum by (a) (max by (a, b) (rate({app="x"}[5m]))), "d", "$1", "a", "(.*)")`,
	} {
		if !HasNestedVectorKeywords(nest) {
			t.Errorf("a nest was not recognised: %s", nest)
		}
	}
	for _, single := range []string{
		`sum by (app) (count_over_time({env="production"}[1m]))`,
		`topk(5, sum by (app) (rate({env="production"}[5m])))`,
		`sum(rate({env="production"} | json [5m]))`,
		`max by (service_name) (max_over_time({service_name="cache-redis"} | logfmt | unwrap size_bytes [1m]))`,
		`sum by (count, min) (min_over_time({app="x"} | unwrap sum [5m]))`,
		`{app="x"} | json | level="warn"`,
	} {
		if HasNestedVectorKeywords(single) {
			t.Errorf("one aggregation was taken for a nest: %s", single)
		}
		single := single
		if allocs := testing.AllocsPerRun(50, func() { _, _, _, _ = tryTranslateVectorChain(single, func(s string) string { return s }) }); allocs != 0 {
			t.Errorf("%d allocations for a query that is not a nest: %s", int(allocs), single)
		}
	}
}
