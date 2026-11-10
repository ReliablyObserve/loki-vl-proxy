package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// A ranking without a grouping over a range aggregation lowers to the operand's own operator: the string translator
// writes no ranking (its text for topk(3, rate(..)) is the text of rate(..)), and the proxy asks VictoriaLogs for the
// operand alone and ranks each step of the answer itself. The lowered operator is the operand's, and it is the
// translator's text byte for byte under every profile (rank_grid_test.go in internal/querycorpus holds it on many
// more shapes).
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersRankingsToTheOperand(t *testing.T) {
	for _, operand := range []string{
		`rate({app="a"}[5m])`, `count_over_time({app="a"} |= "x" [5m])`, `bytes_over_time({app=~"a.*"}[1h] offset 1h)`,
		`bytes_rate({app="a", env!="p"}[1m])`, `absent_over_time({app="a"}[5m])`, `rate({app="a"} | json | status >= 500 [5m])`,
		`sum_over_time({app="a"} | json | unwrap amount [5m])`, `last_over_time({app="a"} | logfmt | unwrap amount [5m])`,
		`avg_over_time({app="a"} | json | unwrap amount [5m]) by (pod)`,
	} {
		want, err := PlanQuery(operand, rangeAggPlan(operand), Profile{})
		if err != nil {
			t.Fatalf("%s: %v", operand, err)
		}
		for _, q := range []string{
			`topk(3, ` + operand + `)`, `bottomk(2, ` + operand + `)`, `sort(` + operand + `)`, `sort_desc(` + operand + `)`,
			`TOPK(03, ` + operand + `)`, `topk (3, ` + operand + `)`, `topk by () (3, ` + operand + `)`, `topk(3, ` + operand + `) by ()`,
		} {
			got, err := PlanQuery(q, rangeAggPlan(q), Profile{})
			if err != nil {
				t.Errorf("%s: %v", q, err)
				continue
			}
			if !physical.Equal(want, got) || physical.Encode(want) != physical.Encode(got) {
				t.Errorf("%s: lowered %s, want the operand's %s", q, physical.Encode(got), physical.Encode(want))
			}
			for _, prof := range rangeProfiles() {
				if lowered, legacy, same, construct, ok := binOpLowering(t, q, prof); ok && (construct != "" || !same) {
					t.Errorf("%s: %s: delegated %q or differs\n lowered %s\n  string %s", prof.name, q, construct, lowered, legacy)
				}
			}
		}
	}
}

// What the lowering leaves to the string translator, and the name each shape is delegated under. A grouping is a
// clause the translator turns into a VictoriaLogs grouping the proxy never sends (it groups the ranked answer
// itself); an operand that is not a range aggregation is written as a phrase search; an unwrapped value with no
// parser is grouped by the stream under a ranking, and by nothing without one.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesRankShapes(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		`topk by (app) (3, ` + rate + `)`:                                     "Rank grouping",
		`topk(3, ` + rate + `) by (app)`:                                      "Rank grouping",
		`bottomk without (app) (2, ` + rate + `)`:                             "Rank grouping",
		`sort by (app) (` + rate + `)`:                                        "Rank grouping",
		`topk without () (3, ` + rate + `)`:                                   "Rank grouping",
		`topk(3, sum by (app) (` + rate + `))`:                                "Rank operand VectorAgg",
		`sort_desc(sum(` + rate + `))`:                                        "Rank operand VectorAgg",
		`topk(3, topk(2, ` + rate + `))`:                                      "Rank operand Rank",
		`topk(3, ` + rate + ` > 1)`:                                           "Rank operand BinOp",
		`topk(3, label_replace(` + rate + `, "a", "b", "c", "d"))`:            "Rank operand LabelReplace",
		`topk(3, sum_over_time({app="a"} | unwrap amount [5m]))`:              "Rank operand unwrap",
		`sort(avg_over_time({app="a"} | drop x | unwrap amount [5m]))`:        "Rank operand unwrap",
		`bottomk(2, quantile_over_time(0.5, {app="a"} | unwrap amount [5m]))`: "Rank operand unwrap",
		`topk(3, count_over_time({app="a"} | drop x [5m]))`:                   "Rank operand RangeAgg",
		`topk(3, rate({app="a"} | detected_level="error" [5m]))`:              "Rank operand RangeAgg",
		`sort( ` + rate + `)`:                                                 "Rank spelling",
		`topk(3, rate ({app="a"}[5m]))`:                                       "Rank spelling",
		`topk(3, (` + rate + `))`:                                             "Rank spelling",
		`sort((` + rate + `))`:                                                "Rank spelling",
		`topk(3, sum_over_time({app="a"} | json |unwrap x [5m]))`:             "Rank spelling",
		`(topk(3, ` + rate + `))`:                                             "Rank parenthesized",
		`label_replace(topk(3, ` + rate + `), "a", "b", "c", "d")`:            "LabelReplace operand Rank",
		`topk(3, ` + rate + `) + 1`:                                           "BinOp operand Rank",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := PlanQuery(q, p, Profile{})
		var nl *NotLoweredError
		switch {
		case !errors.As(err, &nl) || nl.Construct != construct:
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		case len(construct) > 48:
			t.Errorf("%s: the construct name %q is longer than the shadow's label allows", q, construct)
		}
	}
}

// FuzzRankLowering holds the lowering of rankings to the string translator on any selector value, line filter,
// ranking name and spelling, k, function, range, clause and blank placement: both write the same query, or the
// lowering leaves the query to the translator. The text a metric query's regexps read as syntax inside a quoted
// value (" bool ", "without") is kept in: the lowering delegates it.
//
// conformance: architecture-syntax-package-layering
func FuzzRankLowering(f *testing.F) {
	f.Add("a", "x", "level", uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "é\n", "status", uint8(7), uint8(3), uint8(5), uint8(9))
	f.Add("k8s.pod", "topk(1, x)", "a-b", uint8(200), uint8(9), uint8(11), uint8(70))
	f.Fuzz(func(t *testing.T, sel, line, name string, fn, shape, rng, rank uint8) {
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
		selector := `{` + ident(sel) + []string{`="`, `!="`, `=~"`, `!~"`}[shape%4] + escapeValue(sel) + `"}`
		stage := []string{
			``, ` |= ` + strconv.Quote(line), ` != ` + strconv.Quote(line), ` | json`, ` | logfmt | ` + ident(name) + `=` + strconv.Quote(line),
			` | json | unwrap ` + ident(name), ` | unwrap ` + ident(name),
		}[shape>>4%7]
		function := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate", "sum_over_time", "last_over_time", "avg_over_time"}[fn%8]
		span := []string{`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[5m] offset 1h`, `[0s]`}[rng%6]
		pad := []string{"", " ", "\t", "\n"}
		operand := function + pad[rng>>4%4] + `(` + selector + stage + span + `)`
		kk := []string{"3", "03", "1", "100", "99999999999"}[rank>>4%5]
		clause := []string{``, ` by ()`, ` by (` + ident(name) + `)`, ` without (` + ident(name) + `)`}[rank>>2%4]
		var q string
		switch rank % 4 {
		case 0:
			q = []string{"topk", "bottomk", "TOPK", "Bottomk"}[rank>>6%4] + clause + `(` + kk + `,` + pad[rng>>6%4] + operand + `)`
		case 1:
			q = []string{"topk", "bottomk"}[rank>>6%2] + `(` + kk + `, ` + operand + `)` + clause
		case 2:
			q = []string{"sort", "sort_desc", "SORT", "Sort_Desc"}[rank>>6%4] + pad[rng>>6%4] + `(` + pad[rng>>4%4] + operand + pad[rng>>2%4] + `)`
		default:
			q = `(` + []string{"topk", "sort"}[rank>>6%2] + `(` + kk + `, ` + operand + `))`
			if rank>>6%2 == 1 {
				q = `(sort(` + operand + `))`
			}
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, construct, ok := binOpLowering(t, q, prof); ok && construct == "" && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
