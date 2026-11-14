package lower

import (
	"reflect"
	"strings"
	"testing"
)

// keepStats is the stats pipes of the lowered query, after the unwrap gate.
func keepStats(t *testing.T, q string) string {
	t.Helper()
	text := unwrapText(t, q, Profile{})
	i := strings.LastIndex(text, "> ")
	if i < 0 {
		t.Fatalf("%s: no gate in %s", q, text)
	}
	return text[i+2:]
}

// A keep before the unwrap decides the series (queryplan.SeriesLabels): the stats of a function alone group by the
// labels it leaves, a vector aggregation whose clause does not hold them reads a value per such series, and one
// whose clause holds them reads the group's rows, as before. A keep that leaves no label is one series.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestKeepUnwrapIdentityLowering(t *testing.T) {
	const k = `{app="a"} | json | keep app, n | unwrap n [5m]`
	for q, want := range map[string]string{
		`first_over_time(` + k + `)`:                        `| stats by (app) row_min(_time, __lvp_v) as __lvp_inner | unpack_json from __lvp_inner fields (__lvp_v) | math __lvp_v as __lvp_inner | stats by (app) max(__lvp_inner)`,
		`quantile_over_time(0.9, ` + k + `)`:                `| stats by (app) quantile(0.9, __lvp_v)`,
		`stddev_over_time(` + k + `)`:                       `| stats by (app) stddev(__lvp_v)`,
		`stdvar_over_time(` + k + `)`:                       `| stats by (app) stddev(__lvp_v)|||2`,
		`sum(first_over_time(` + k + `))`:                   `| stats by (app) row_min(_time, __lvp_v) as __lvp_inner | unpack_json from __lvp_inner fields (__lvp_v) | math __lvp_v as __lvp_inner | stats sum(__lvp_inner)`,
		`sum(quantile_over_time(0.9, ` + k + `))`:           `| stats by (app) quantile(0.9, __lvp_v) as __lvp_inner | stats sum(__lvp_inner)`,
		`sum(stdvar_over_time(` + k + `))`:                  `| stats by (app) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats sum(__lvp_inner_var)`,
		`sum(sum_over_time(` + k + `))`:                     `| stats by (app) sum(__lvp_v) as __lvp_inner | stats sum(__lvp_inner)`,
		`max by (app) (avg_over_time(` + k + `))`:           `| stats by (app) avg(__lvp_v) as __lvp_inner | stats by (app) max(__lvp_inner)`,
		`sum by (app) (avg_over_time(` + k + `))`:           `| stats by (app) avg(__lvp_v)`,
		`sum by (app) (quantile_over_time(0.9, ` + k + `))`: `| stats by (app) quantile(0.9, __lvp_v)`,
		`sum by (app) (stdvar_over_time(` + k + `))`:        `| stats by (app) stddev(__lvp_v)|||2`,
		// The clause does not hold every kept label: a value per kept series, then the clause's aggregation.
		`sum by (app) (stddev_over_time({app="a"} | json | keep app, pod, n | unwrap n [5m]))`:        `| stats by (app, pod) stddev(__lvp_v) as __lvp_inner | stats by (app) sum(__lvp_inner)`,
		`sum by (app) (quantile_over_time(0.9, {app="a"} | json | keep app, pod, n | unwrap n [5m]))`: `| stats by (app, pod) quantile(0.9, __lvp_v) as __lvp_inner | stats by (app) sum(__lvp_inner)`,
		`max by (app) (stdvar_over_time({app="a"} | json | keep app, pod, n | unwrap n [5m]))`:        `| stats by (app, pod) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats by (app) max(__lvp_inner_var)`,
		`sum by (app) (sum_over_time({app="a"} | json | keep app, pod, n | unwrap n [5m]))`:           `| stats by (app) sum(__lvp_v)`,
		// A keep that leaves no label: one series.
		`first_over_time({app="a"} | json | keep n | unwrap n [5m])`:       `| stats by () row_min(_time, __lvp_v) as __lvp_inner | unpack_json from __lvp_inner fields (__lvp_v) | math __lvp_v as __lvp_inner | stats by () max(__lvp_inner)`,
		`stddev_over_time({app="a"} | json | keep n | unwrap n [5m])`:      `| stats by () stddev(__lvp_v)`,
		`sum(stddev_over_time({app="a"} | json | keep n | unwrap n [5m]))`: `| stats stddev(__lvp_v) as __lvp_inner | stats sum(__lvp_inner)`,
		// A rate of the unwrapped label: the values summed per kept series.
		`rate(` + k + `)`: `| stats by (app) sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | filter __lvp_rate:* | stats by (app) sum(__lvp_rate)`,
		`rate({app="a"} | json | keep n | unwrap n [5m])`: `| stats sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | filter __lvp_rate:* | stats sum(__lvp_rate)`,
		`sum(rate(` + k + `))`:                            `| stats by (app) sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats sum(__lvp_rate)`,
		// The range's own by clause still decides.
		`stddev_over_time(` + k + `) by (pod)`: `| stats by (pod) stddev(__lvp_v)`,
		// A keep the unwrap does not follow alone decides nothing: the stream and each line, as before.
		`stddev_over_time({app="a"} | keep app | json | unwrap n [5m])`: `| stats by (_stream, _msg) stddev(__lvp_v)`,
	} {
		if got := keepStats(t, q); got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
}

// The plan's identity of a range function over an unwrapped label behind a keep: the labels the keep leaves, as
// Loki names them, read alone or under one vector aggregation; nothing for any other shape.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestKeepUnwrapIdentity(t *testing.T) {
	for _, tc := range []struct {
		q      string
		want   []string
		ok     bool
		writes bool
	}{
		{`first_over_time({app="a"} | json | keep app, n | unwrap n [5m])`, []string{"app"}, true, false},
		{`sum by (app) (quantile_over_time(0.9, {app="a"} | json | keep app, pod, n | unwrap n [5m]))`, []string{"app", "pod"}, true, false},
		{`stddev_over_time({app="a"} | json | keep n | unwrap n [5m])`, []string{}, true, false},
		{`stddev_over_time({app="a"} | json | unwrap n [5m])`, nil, false, false},
		{`rate({app="a"} | json | keep app [5m])`, nil, false, false},
		{`stddev_over_time({app="a"} | json | keep app, n="x" | unwrap n [5m])`, nil, false, false},
		{`stddev_over_time({app="a"} | json | keep app, n | drop app | unwrap n [5m])`, nil, false, false},
		{`stddev_over_time({app="a"} | json | keep app | json | unwrap n [5m])`, nil, false, false},
		{`topk(1, stddev_over_time({app="a"} | json | keep app, n | unwrap n [5m]))`, nil, false, true},
		{`stddev_over_time({app="a"} | json | keep app, n | unwrap n [5m]) * 2`, nil, false, true},
		{`label_replace(stddev_over_time({app="a"} | json | keep app, n | unwrap n [5m]), "b", "$1", "app", "(.*)")`, nil, false, true},
		{`count(absent_over_time({app="a"} | json | keep app, n | unwrap n [5m]))`, nil, false, false},
		{`{app="a"} | json | keep app`, nil, false, false},
	} {
		p := rangeAggPlan(tc.q)
		if p == nil {
			t.Fatalf("%s: does not parse", tc.q)
		}
		got, ok := KeepUnwrapIdentity(p)
		if ok != tc.ok || ok && !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %v %v, want %v %v", tc.q, got, ok, tc.want, tc.ok)
		}
		if KeepDecidesUnwrapIdentity(p) != tc.ok {
			t.Errorf("%s: KeepDecidesUnwrapIdentity is not %v", tc.q, tc.ok)
		}
		// The lowering writes the identity at any depth: under a ranking and in a binary operation too.
		if want := tc.ok || tc.writes; WritesKeepUnwrapIdentity(p) != want {
			t.Errorf("%s: WritesKeepUnwrapIdentity is not %v", tc.q, want)
		}
	}
}

// A keep decides nothing where the string translator reads the keep another way (a name that is not a bare
// identifier) or the label function folds two kept labels into one field: the series stay the stream's.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestKeepUnwrapIdentityLeftAlone(t *testing.T) {
	if _, ok := trailingKeepLabels(selectOf(t, `{a="b"} | json | keep x.y, n | unwrap n`), true); ok {
		t.Error("a keep of a dotted name decides the series")
	}
	sel := selectOf(t, `{a="b"} | json | keep x, y, n | unwrap n`)
	if by, ok := keptIdentity(sel, Profile{Label: func(string) string { return "z" }}); ok {
		t.Errorf("two labels named alike decide the series: %v", by)
	}
	if by, ok := keptIdentity(sel, Profile{}); !ok || strings.Join(by, ",") != "x,y" {
		t.Errorf("identity %v %v", by, ok)
	}
}
