package translator

import (
	"strings"
	"testing"
)

func pushDownOf(t *testing.T, logql string) (string, bool) {
	t.Helper()
	identity := func(s string) string { return s }
	translated, err := TranslateLogQLWithStreamFields(logql, identity, nil)
	if err != nil {
		t.Fatal(err)
	}
	series, _, _ := ParseWithoutMarker(translated)
	// The proxy groups a bare metric by the stream before it asks (preserveMetricStreamIdentity).
	if !strings.Contains(series, "| stats by (") {
		series = strings.Replace(series, "| stats ", "| stats by (_stream, level) ", 1)
	}
	return PushDownWithout(logql, series, identity, identity)
}

const pushCut = "| replace_regexp(`,pod=\"[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\"`, \"\") at _stream " +
	"| replace_regexp(`^\\{pod=\"[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\",?`, \"{\") at _stream "

// conformance: semantics/without-over-inner-aggregation-collapses, semantics/nested-aggregation-outer-by-native-routes
func TestPushDownWithoutEmitsTheFoldAsStatsPipes(t *testing.T) {
	for _, tc := range []struct{ name, logql, want string }{
		{
			"alone",
			`avg without (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level) avg(__lvp_inner)`,
		},
		{
			"rate",
			`sum without (pod) (rate({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner | math __lvp_inner/60 as __lvp_rate ` + pushCut + `| stats by (_stream, level) sum(__lvp_rate)`,
		},
		{
			"a sum counts the lines of the series once, then sums the counts",
			`sum without (pod) (count_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level) sum(__lvp_inner)`,
		},
		{
			"a sum of bytes",
			`sum without (pod) (bytes_over_time({app="x"}[1m]))`,
			`app:="x" | stats by (_stream, level) sum_len(_msg) as __lvp_inner ` + pushCut + `| stats by (_stream, level) sum(__lvp_inner)`,
		},
		{
			"a sum under a level above",
			`max by (zone) (sum without (pod) (count_over_time({app="x"}[1m])))`,
			`app:="x" | stats by (_stream, level, zone) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level, zone) sum(__lvp_inner) as __lvp_inner | stats by (zone) max(__lvp_inner)`,
		},
		{
			"trailing spelling",
			`max(count_over_time({app="x"}[1m])) without (pod)`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level) max(__lvp_inner)`,
		},
		{
			"the labels the level above groups by are carried through the first pipes",
			`min by (zone) (avg without (pod) (count_over_time({app="x"}[1m])))`,
			`app:="x" | stats by (_stream, level, zone) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level, zone) avg(__lvp_inner) as __lvp_inner | stats by (zone) min(__lvp_inner)`,
		},
		{
			"an excluded label is no longer kept by the level above",
			`min by (zone, pod) (avg without (pod) (count_over_time({app="x"}[1m])))`,
			`app:="x" | stats by (_stream, level, zone) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level, zone) avg(__lvp_inner) as __lvp_inner | stats by (zone) min(__lvp_inner)`,
		},
		{
			"a label the service name is derived from, or an underscore one, is a column like any other",
			`min by (app, pod_name) (avg without (pod) (count_over_time({app="x"}[1m])))`,
			`app:="x" | stats by (_stream, level, app, pod_name) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level, app, pod_name) avg(__lvp_inner) as __lvp_inner | stats by (app, pod_name) min(__lvp_inner)`,
		},
		{
			"by () above keeps nothing",
			`sum by () (max without (pod) (count_over_time({app="x"}[1m])))`,
			`app:="x" | stats by (_stream, level) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level) max(__lvp_inner) as __lvp_inner | stats sum(__lvp_inner)`,
		},
		{
			"three levels",
			`max by (c) (min by (c, n) (avg without (pod) (count_over_time({app="x"}[1m]))))`,
			`app:="x" | stats by (_stream, level, c, n) count() as __lvp_inner ` + pushCut + `| stats by (_stream, level, c, n) avg(__lvp_inner) as __lvp_inner | stats by (c, n) min(__lvp_inner) as __lvp_inner | stats by (c) max(__lvp_inner)`,
		},
	} {
		got, ok := pushDownOf(t, tc.logql)
		if !ok {
			t.Errorf("%s: not pushed down: %s", tc.name, tc.logql)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: %s\n got %s\nwant %s", tc.name, tc.logql, got, tc.want)
		}
	}
}

// conformance: semantics/without-over-inner-aggregation-collapses
func TestPushDownWithoutLeavesTheFoldToTheProxyWhenNotExact(t *testing.T) {
	for _, logql := range []string{
		// an excluded label that is not a plain name, or that a label is derived from
		`avg without (pod_name) (count_over_time({app="x"}[1m]))`,
		`avg without (k8s.pod) (count_over_time({app="x"}[1m]))`,
		`avg without (app) (count_over_time({app="x"}[1m]))`,
		`avg without (container) (count_over_time({app="x"}[1m]))`,
		`avg without (service_name) (count_over_time({app="x"}[1m]))`,
		`avg without (level) (count_over_time({app="x"}[1m]))`,
		`avg without (detected_level) (count_over_time({app="x"}[1m]))`,
		`avg without (pod, level) (count_over_time({app="x"}[1m]))`,
		// a parser: the series carry the labels it extracts
		`avg without (pod) (count_over_time({app="x"} | json [1m]))`,
		`avg without (pod) (count_over_time({app="x"} | logfmt [1m]))`,
		`sum without (pod) (sum_over_time({app="x"} | logfmt | unwrap amount [1m]))`,
		// a level above that is not a plain `by`, or groups by a derived label
		`min without (zone) (avg without (pod) (count_over_time({app="x"}[1m])))`,
		`min by (service_name) (avg without (pod) (count_over_time({app="x"}[1m])))`,
		`min by (level) (avg without (pod) (count_over_time({app="x"}[1m])))`,
		// a ranking or label transform around the fold, another operator, no without
		`topk(2, avg without (pod) (count_over_time({app="x"}[1m])))`,
		`sort(avg without (pod) (count_over_time({app="x"}[1m])))`,
		`stddev without (pod) (count_over_time({app="x"}[1m]))`,
		`avg by (pod) (count_over_time({app="x"}[1m]))`,
		`min by (zone) (avg by (pod, zone) (count_over_time({app="x"}[1m])))`,
	} {
		if got, ok := pushDownOf(t, logql); ok {
			t.Errorf("pushed down: %s\n%s", logql, got)
		}
	}
	// a label translation that renames the label (a custom mapping)
	translated, _ := TranslateLogQLWithStreamFields(`avg without (pod) (count_over_time({app="x"}[1m]))`, func(s string) string { return s }, nil)
	series, _, _ := ParseWithoutMarker(translated)
	rename := func(s string) string {
		if s == "pod" {
			return "k8s.pod.name"
		}
		return s
	}
	identity := func(s string) string { return s }
	if _, ok := PushDownWithout(`avg without (pod) (count_over_time({app="x"}[1m]))`, series, rename, identity); ok {
		t.Error("pushed down a label the query direction renames")
	}
	if _, ok := PushDownWithout(`avg without (pod) (count_over_time({app="x"}[1m]))`, series, identity, rename); ok {
		t.Error("pushed down a label the response direction renames")
	}
	// a translation that is not the two pipes of a range aggregation
	if _, ok := PushDownWithout(`avg without (pod) (count_over_time({app="x"}[1m]))`, `app:="x" | stats by (pod) count()`, nil, nil); ok {
		t.Error("pushed down a translation that is not the series rows")
	}
	if got, ok := pushDownOf(t, `avg without (pod) (count_over_time({app="x"}[1m]))`); !ok || !strings.Contains(got, "replace_regexp") {
		t.Errorf("control: %v %s", ok, got)
	}
}

// The cut of a sliding `sum without` is the tumbling route's, for a `sum`
// alone; any other operator, a nest, a wrapper and a name the cut does not
// take have none.
//
// conformance: semantics/without-sliding-sum-folded-buckets
func TestWithoutStreamCut(t *testing.T) {
	identity := func(s string) string { return s }
	for _, tc := range []struct {
		logql string
		want  string
		ok    bool
	}{
		{`sum without (pod) (count_over_time({app="x"}[1m]))`, pushCut, true},
		{`sum(count_over_time({app="x"}[1m])) without (pod)`, pushCut, true},
		{`sum without (pod, zone) (bytes_over_time({app="x"}[1m]))`, pushCut + strings.ReplaceAll(pushCut, "pod", "zone"), true},
		{`avg without (pod) (count_over_time({app="x"}[1m]))`, "", false},
		{`max by (app) (sum without (pod) (count_over_time({app="x"}[1m])))`, "", false},
		{`topk(2, sum without (pod) (count_over_time({app="x"}[1m])))`, "", false},
		{`sum without (pod_name) (count_over_time({app="x"}[1m]))`, "", false},
		{`sum without (app) (count_over_time({app="x"}[1m]))`, "", false},
		{`sum without (level) (count_over_time({app="x"}[1m]))`, "", false},
		{`sum by (pod) (count_over_time({app="x"}[1m]))`, "", false},
	} {
		got, ok := WithoutStreamCut(tc.logql, identity, identity)
		if ok != tc.ok || got != tc.want {
			t.Errorf("%s: %q %v, want %q %v", tc.logql, got, ok, tc.want, tc.ok)
		}
	}
}
