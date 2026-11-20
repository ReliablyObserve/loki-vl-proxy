package proxy

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// absentSpecByText is parseAbsentOverTimeCompatSpec as it read the label_replace calls before they were read from the
// parsed tree: split off the text one call at a time (translator.SplitLabelReplace), then the rest parsed again.
func absentSpecByText(logql string) (absentOverTimeCompatSpec, bool) {
	query := strings.TrimSpace(logql)
	var replaces []physical.LabelReplace
	for {
		inner, spec, ok := translator.SplitLabelReplace(query)
		if !ok {
			break
		}
		if _, err := regexp.Compile("^(?:" + spec.Regex + ")$"); err != nil {
			return absentOverTimeCompatSpec{}, false
		}
		replaces = append(replaces, spec)
		query = strings.TrimSpace(inner)
	}
	var aggs []*logqlpkg.VectorAggregation
	var absent *logqlpkg.RangeAggregation
	if expr, ok := sharedLogQLExpr(query); ok {
		node := expr
		for {
			agg, ok := node.(*logqlpkg.VectorAggregation)
			if !ok {
				break
			}
			aggs = append(aggs, agg)
			node = agg.Inner
		}
		ra, ok := node.(*logqlpkg.RangeAggregation)
		if !ok || ra.Op != logqlpkg.RangeAbsentOverTime {
			return absentOverTimeCompatSpec{}, false
		}
		if ra.Grouping != nil {
			return absentOverTimeCompatSpec{err: errAbsentGrouping}, true
		}
		absent = ra
		query = ra.String()
	} else if !absentOverTimeCompatRE.MatchString(query) {
		return absentOverTimeCompatSpec{}, false
	}
	matches := absentOverTimeCompatRE.FindStringSubmatch(query)
	if len(matches) != 3 || (absent == nil && len(aggs) > 0) {
		return absentOverTimeCompatSpec{}, false
	}
	window, ok := parsePositiveStepDuration(matches[2])
	if !ok {
		return absentOverTimeCompatSpec{}, false
	}
	baseQuery := strings.TrimSpace(matches[1])
	if baseQuery == "" {
		return absentOverTimeCompatSpec{}, false
	}
	return absentOverTimeCompatSpec{baseQuery: baseQuery, rangeWindow: window, rangeWindowStr: matches[2], aggs: aggs, labelReplaces: replaces}, true
}

// absentSpecString is a spec as text: its vector aggregations printed.
func absentSpecString(s absentOverTimeCompatSpec, ok bool) string {
	aggs := make([]string, len(s.aggs))
	for i, a := range s.aggs {
		aggs[i] = a.String()
	}
	return fmt.Sprintf("%v %q %v %q %q %q %+v", ok, s.baseQuery, s.rangeWindow, s.rangeWindowStr, s.err, aggs, s.labelReplaces)
}

// The label_replace calls around absent_over_time are read from the parsed tree as they were read from the text: the
// same spec on every corpus query and on nests, spellings and malformed calls the corpus does not hold.
//
// conformance: semantics/absent-over-time-semantics
func TestAbsentSpecLabelReplaceFromTheTree(t *testing.T) {
	qs, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	const absent = `absent_over_time({app="a", env="prod"}[5m])`
	wrap := func(n int, inner, regex string) string {
		return strings.Repeat("label_replace(", n) + inner + strings.Repeat(`, "d", "$1", "app", "`+regex+`")`, n)
	}
	extra := []string{
		wrap(1, absent, "(.*)"),
		wrap(17, absent, "(.*)"),
		wrap(300, absent, "(.*)"),
		wrap(2, `sum by (app) (`+absent+`)`, "(.+)-.*"),
		wrap(2, absent, "("),
		wrap(1, `absent_over_time({app="a"}[5m]) by (app)`, "(.*)"),
		wrap(1, `absent_over_time({app="a"}[5m]) by (app)`, "("),
		wrap(3, `rate({app="a"}[5m])`, "(.*)"),
		`label_replace(` + absent + `, "d", "$1", "app", ` + "`(.*)`" + `)`,
		`label_replace(` + absent + `, "d", "a\"b", "app", "(.*)")`,
		`  label_replace(  ` + absent + ` ,"d","$1","app","(.*)")  `,
		`label_replace(label_replace(` + absent + `, "d", "$1", "app", "(.*)"), "e", "$1", "d", "(.*)")`,
		`label_replace(absent_over_time({app="a"}[$__interval]), "d", "$1", "app", "(.*)")`,
		`label_replace(absent_over_time({app="a"}[5m]), "d", "$1", "app")`,
		`label_replace(sum(absent_over_time({app="a"} | json | unwrap n [5m])), "d", "$1", "app", "(.*)")`,
		absent,
		`max by (app) (` + absent + `)`,
	}
	checked, absents := 0, 0
	for _, q := range append(qs, extra...) {
		q = routeHandlerText(q)
		got, gok := parseAbsentOverTimeCompatSpec(q)
		want, wok := absentSpecByText(q)
		if g, w := absentSpecString(got, gok), absentSpecString(want, wok); g != w {
			t.Fatalf("%.200q:\n from the tree %s\n from the text %s", q, g, w)
		}
		checked++
		if gok {
			absents++
		}
	}
	if absents < len(extra)/2 {
		t.Fatalf("%d of %d queries read as absent_over_time", absents, checked)
	}
}
