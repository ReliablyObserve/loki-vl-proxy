package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// What the lowering writes for a vector aggregation over another one: the innermost level as a query of its own,
// then one stats pipe per level above it, each keeping the grouping labels the level below kept.
//
// conformance: architecture-syntax-package-layering
func TestPlanNestedVectorAggregationText(t *testing.T) {
	v53 := Profile{Caps: logsql.CapabilitiesFor("v1.53.0")}
	dots := Profile{Caps: v53.Caps, Label: func(s string) string { return strings.ReplaceAll(s, ".", "_") }}
	for _, c := range []struct {
		q    string
		prof Profile
		want string
	}{
		// a sum over a sum: the labels of the inner level are the ones the outer keeps.
		{`sum(sum by (app) (count_over_time({app="a"}[5m])))`, v53, `app:="a" | stats by (app) count() as __lvp_inner | stats sum(__lvp_inner)`},
		{`sum by (app) (sum by (app, env) (count_over_time({app="a"}[5m])))`, v53, `app:="a" | stats by (app, env) count() as __lvp_inner | stats by (app) sum(__lvp_inner)`},
		// a label the inner level lacks is cut from the outer clause, and no clause keeps none.
		{`sum by (app) (sum by (env) (count_over_time({app="a"}[5m])))`, v53, `app:="a" | stats by (env) count() as __lvp_inner | stats sum(__lvp_inner)`},
		{`max by () (count by (app) (count_over_time({app="a"}[5m])))`, v53, `app:="a" <DL> | stats by (app, _stream, level, detected_level) count() as __lvp_inner | stats by (app) count() as __lvp_inner | stats max(__lvp_inner)`},
		// the labels kept are the outer clause's own, in its order, once each.
		{`max by (b, a, a) (min by (a, b, c) (bytes_over_time({app="a"}[5m])))`, v53, `app:="a" <DL> | stats by (a, b, c, _stream, level, detected_level) sum_len(_msg) as __lvp_inner | stats by (a, b, c) min(__lvp_inner) as __lvp_inner | stats by (b, a) max(__lvp_inner)`},
		// a without level subtracts its labels from the ones below.
		{`avg without (c) (max by (a, b, c) (count_over_time({app="a"}[5m])))`, v53, `app:="a" <DL> | stats by (a, b, c, _stream, level, detected_level) count() as __lvp_inner | stats by (a, b, c) max(__lvp_inner) as __lvp_inner | stats by (a, b) avg(__lvp_inner)`},
		// three levels, and the clause after the operand.
		{`min(max(sum(count_over_time({app="a"}[5m])) by (a, b)) by (a)) by ()`, v53, `app:="a" | stats by (a, b) count() as __lvp_inner | stats by (a) max(__lvp_inner) as __lvp_inner | stats min(__lvp_inner)`},
		// a rate divides in the innermost level; service_name and detected_level derive per row.
		{`sum by (app) (sum by (app, env) (rate({app="a"}[5m])))`, v53, `app:="a" | stats by (app, env) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (app, env) sum(__lvp_rate) as __lvp_inner | stats by (app) sum(__lvp_inner)`},
		{`max by (service_name) (count by (service_name, detected_level) (count_over_time({app="a"}[5m])))`, v53, `app:="a" <DL> <SN> | stats by (service_name, detected_level, _stream, level) count() as __lvp_inner | stats by (service_name, detected_level) count() as __lvp_inner | stats by (service_name) max(__lvp_inner)`},
		// the label function names the labels of every level.
		{`sum by (k8s.pod.name) (max by (k8s.pod.name) (count_over_time({app="a"}[5m])))`, dots, `app:="a" <DL> | stats by (k8s_pod_name, _stream, level, detected_level) count() as __lvp_inner | stats by (k8s_pod_name) max(__lvp_inner) as __lvp_inner | stats by (k8s_pod_name) sum(__lvp_inner)`},
	} {
		if got := vectorAggText(t, c.q, c.prof); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.q, got, c.want)
		}
	}
}

// A nest whose innermost level is a without is the series rows of that level and the fold of the proxy: it carries
// the labels of the innermost clause and the operator of the level at the top.
//
// conformance: architecture-syntax-package-layering
func TestPlanNestedVectorAggregationFold(t *testing.T) {
	op, err := Plan(rangeAggPlan(`min by (zone) (avg by (pod, zone) (max without (abc, pod) (rate({app="a"}[5m]))))`), Profile{})
	if err != nil {
		t.Fatal(err)
	}
	fold, ok := op.(physical.Fold)
	if !ok || len(fold.Levels) != 1 {
		t.Fatalf("got %#v", op)
	}
	if l := fold.Levels[0]; l.Op != "min" || !l.Without || strings.Join(l.Labels, ",") != "abc,pod" {
		t.Errorf("level %#v", l)
	}
	// The query is the series of the innermost level alone, as for the level on its own.
	single, err := Plan(rangeAggPlan(`max without (abc, pod) (rate({app="a"}[5m]))`), Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if physical.Encode(fold.In) != physical.Encode(single.(physical.Fold).In) {
		t.Errorf("the nest's query is not the innermost level's:\n %s\n %s", physical.Encode(fold.In), physical.Encode(single.(physical.Fold).In))
	}
	// A without above the innermost level is no fold.
	if op, err = Plan(rangeAggPlan(`min by (zone) (max without (pod) (avg by (pod, zone) (rate({app="a"}[5m]))))`), Profile{}); err != nil {
		t.Fatal(err)
	} else if _, ok := op.(physical.VLQuery); !ok {
		t.Errorf("a without above the innermost level: got %#v", op)
	}
}

// Every nest the lowering leaves to the string translator is named.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesNestedVectorAggregations(t *testing.T) {
	for q, construct := range map[string]string{
		`sum(stddev by (app) (rate({app="a"}[5m])))`:                                         "VectorAgg operand",
		`stddev(sum by (app) (rate({app="a"}[5m])))`:                                         "VectorAgg operand",
		`sum(sum by (app) (stdvar(rate({app="a"}[5m]))))`:                                    "VectorAgg operand",
		`sum(topk(2, sum by (app) (rate({app="a"}[5m]))))`:                                   "VectorAgg operand",
		`sum(sum by (app) (rate({app="a"}[5m]) > 1))`:                                        "VectorAgg operand",
		`sum(sum by (app) (label_replace(rate({app="a"}[5m]), "b", "$1", "app", "(.*)")))`:   "VectorAgg operand",
		`sum(sum by (app) (sum_over_time({app="a"} | unwrap n [5m])))`:                       "VectorAgg nested unwrap",
		`sum(sum by (app) (rate({app="a"} | unwrap n [5m])))`:                                "VectorAgg nested unwrap",
		`sum without () (sum by (app) (rate({app="a"}[5m])))`:                                "VectorAgg without empty",
		`sum(sum without () (rate({app="a"}[5m])))`:                                          "VectorAgg without empty",
		`sum(sum by (app) (rate({app="a"}[0s])))`:                                            "VectorAgg range",
		`sum(sum by (app) (count_over_time({app="a"} |= "| stats " [5m])))`:                  "VectorAgg quoted clause",
		`sum(sum without (app) (count_over_time({app="a"} |= "without (x)" [5m])))`:          "VectorAgg quoted clause",
		`sum(sum by (app) (avg_over_time({app="a"} | unwrap n [5m]) by (x)))`:                "VectorAgg nested unwrap",
		`sum by (app) (sum by (app) (absent_over_time({app="a"} | unwrap n [5m]) by (x)))`:   "VectorAgg grouping",
		`sum(sum by (app) (count_over_time({app="a"} | line_format "{{.a | upper}}" [5m])))`: "VectorAgg LogSelect line format function",
		`sum(sum by (app) (count_over_time({app="a"} | detected_level="error" [5m])))`:       "VectorAgg LogSelect label filter detected_level",
		`sum(sum by ( bool ) (count_over_time({app="a"}[5m])))`:                              "VectorAgg quoted clause",
		`sum(sum by (app) (rate({app="a"}[5m]))) by (bool)`:                                  "",
		`sum(sum by (app) (sum(rate({app="a"}[5m])) by (app)) )`:                             "",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := Plan(p, Profile{})
		var nl *NotLoweredError
		switch {
		case construct == "" && err != nil:
			t.Errorf("%s: got %v, want lowered", q, err)
		case construct != "" && (!errors.As(err, &nl) || nl.Construct != construct):
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
	// absent_over_time reads no unwrapped value: the nest is the one over the query without the stage.
	with := vectorAggText(t, `sum(max by (app) (absent_over_time({app="a"} | unwrap n [5m])))`, Profile{})
	without := vectorAggText(t, `sum(max by (app) (absent_over_time({app="a"}[5m])))`, Profile{})
	if with != without {
		t.Errorf("absent_over_time with an unwrap:\n got %s\nwant %s", with, without)
	}
}

// FuzzNestedVectorAggLowering holds the lowering of a vector aggregation over other ones (two to four levels) to the
// string translator on any selector values, line filters, groupings, functions and ranges: both write the same
// query, or the lowering leaves the query to the translator. The nest is kept short: the translator scans the text
// once per level.
//
// conformance: architecture-syntax-package-layering
func FuzzNestedVectorAggLowering(f *testing.F) {
	f.Add("a", "x", "level", uint8(0), uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "é\n", "status", uint8(7), uint8(3), uint8(5), uint8(9), uint8(77))
	f.Add("k8s.pod", "without (x)", "a-b", uint8(200), uint8(9), uint8(11), uint8(70), uint8(255))
	f.Fuzz(func(t *testing.T, sel, line, name string, fn, shape, rng, agg, levels uint8) {
		if len(sel)+len(line)+len(name) > 120 {
			return
		}
		strip := strings.NewReplacer("bool", "", "without", "")
		for _, s := range []string{sel, line, name} {
			if translatorReadsAsSyntax(strip.Replace(s)) {
				return
			}
		}
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			if b.Len() == 0 {
				return "x"
			}
			return b.String()
		}
		matchers := []string{`="`, `!="`, `=~"`, `!~"`}
		selector := `{` + ident(sel) + matchers[shape%4] + escapeValue(sel) + `"}`
		stage := []string{``, ` |= ` + strconv.Quote(line), ` != ` + strconv.Quote(line), ` |~ ` + strconv.Quote(line)}[shape>>4%4]
		function := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}[fn%5]
		span := []string{`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[5m] offset 1h`, `[0s]`}[rng%6]
		a, b := ident(name), ident(sel)
		clauses := []string{``, `by (` + a + `)`, `by (` + a + `, ` + b + `)`, `by (` + b + `, ` + a + `)`, `without (` + a + `)`, `by ()`, `by (service_name, ` + a + `)`,
			`by (detected_level, ` + a + `)`, `without (` + b + `)`, `by (` + a + `, ` + a + `)`}
		ops := []string{"sum", "count", "min", "max", "avg"}
		q := function + `(` + selector + stage + span + `)`
		n := 2 + int(levels)%3
		for i := 0; i < n; i++ {
			op := ops[(int(agg)+i*3)%len(ops)]
			g := clauses[(int(agg>>2)+int(levels>>2)*(i+1)+i*7)%len(clauses)]
			switch {
			case g == "":
				q = op + `(` + q + `)`
			case (int(levels>>5)+i)%2 == 0:
				q = op + ` ` + g + ` (` + q + `)`
			default:
				q = op + `(` + q + `) ` + g
			}
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
