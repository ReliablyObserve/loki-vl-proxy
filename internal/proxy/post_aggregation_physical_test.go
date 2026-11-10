package proxy

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// PostAggregationPhysical is the proxy's own route written as physical operators: one level per layer the proxy
// evaluates, innermost first, over the translation of the operand alone.
//
// conformance: architecture-syntax-package-layering
func TestPostAggregationPhysicalReadsTheRoute(t *testing.T) {
	var asked []string
	translate := func(q string) (string, error) {
		asked = append(asked, q)
		return translator.TranslateLogQL(q)
	}
	app := &physical.Grouping{Labels: []string{"app"}}
	operandOf := func(q string) physical.Op {
		text, err := translator.TranslateLogQL(q)
		if err != nil {
			t.Fatal(err)
		}
		op, err := translator.DecodePhysical(text)
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	rate := `rate({app="a"}[5m])`
	sum := `sum by (app) (` + rate + `)`
	for _, c := range []struct {
		query   string
		operand string
		want    func(in physical.Op) physical.Op
	}{
		{`topk(3, ` + rate + `)`, rate, func(in physical.Op) physical.Op { return physical.RankStep{Op: "topk", K: 3, In: in} }},
		{`bottomk by (app) (2, ` + sum + `)`, sum, func(in physical.Op) physical.Op {
			return physical.RankStep{Op: "bottomk", K: 2, By: app, In: in}
		}},
		{`topk(2, ` + sum + `) without (pod)`, sum, func(in physical.Op) physical.Op {
			return physical.RankStep{Op: "topk", K: 2, By: &physical.Grouping{Labels: []string{"pod"}, Without: true}, In: in}
		}},
		{`sort_desc(` + rate + `)`, rate, func(in physical.Op) physical.Op { return physical.RankStep{Op: "sort_desc", In: in} }},
		{`sort(` + sum + `)`, sum, func(in physical.Op) physical.Op { return physical.RankStep{Op: "sort", In: in} }},
		{`stddev by (app) (` + rate + `)`, rate, func(in physical.Op) physical.Op { return physical.SeriesAgg{Op: "stddev", By: app, In: in} }},
		{`stdvar(` + rate + `)`, rate, func(in physical.Op) physical.Op { return physical.SeriesAgg{Op: "stdvar", In: in} }},
		{`bottomk(2, sort_desc(` + rate + `))`, rate, func(in physical.Op) physical.Op {
			return physical.RankStep{Op: "bottomk", K: 2, In: physical.RankStep{Op: "sort_desc", In: in}}
		}},
		{`max by (app) (sum(topk(3, ` + rate + `)))`, rate, func(in physical.Op) physical.Op {
			return physical.SeriesAgg{Op: "max", By: app, In: physical.SeriesAgg{Op: "sum", In: physical.RankStep{Op: "topk", K: 3, In: in}}}
		}},
	} {
		asked = nil
		op, text, routed, err := PostAggregationPhysical(c.query, translate)
		if err != nil || !routed {
			t.Fatalf("%s: routed %v, %v", c.query, routed, err)
		}
		if !reflect.DeepEqual(asked, []string{c.operand}) {
			t.Errorf("%s: translated %q, want the operand %q alone", c.query, asked, c.operand)
		}
		want := c.want(operandOf(c.operand))
		if !physical.Equal(op, want) {
			t.Errorf("%s: got %#v, want %#v", c.query, op, want)
		}
		if _, operand := physical.SplitEval(op); physical.Encode(operand) != text {
			t.Errorf("%s: operand text %q, want %q", c.query, physical.Encode(operand), text)
		}
		if inner, ok := PostAggregationRoute(c.query); !ok || inner != c.operand {
			t.Errorf("%s: route operand %q, %v", c.query, inner, ok)
		}
	}
	// A query the route does not take answers not routed; a failed translation is the translation's error.
	for _, q := range []string{rate, `sum(` + rate + `)`, `(topk(3, ` + rate + `))`, `topk(3, ` + rate + `) + 1`, `{app="a"}`} {
		if _, _, routed, err := PostAggregationPhysical(q, translate); routed || err != nil {
			t.Errorf("%s: routed %v, %v", q, routed, err)
		}
	}
	boom := errors.New("boom")
	if _, _, routed, err := PostAggregationPhysical(`topk(3, `+rate+`)`, func(string) (string, error) { return "", boom }); !routed || !errors.Is(err, boom) {
		t.Errorf("a failed translation: routed %v, %v", routed, err)
	}
}

// translatePostAggOperand is postAggOperandText with the proxy's translation: the parsed-label rewrite of a stddev
// reads the same text either way.
//
// conformance: architecture-syntax-package-layering
func TestPostAggOperandTextIsTheRewriteOfTheParsedLabelForm(t *testing.T) {
	q := `stddev by (route) (count_over_time({app="a"} | logfmt [5m]))`
	postAgg, ok := parseInstantMetricPostAggQuery(q)
	if !ok {
		t.Fatal("not routed")
	}
	var asked []string
	text, err := postAggOperandText(postAgg, func(s string) (string, error) {
		asked = append(asked, s)
		return translator.TranslateLogQL(s)
	})
	if err != nil || len(asked) != 1 || asked[0] != `max by (route) (count_over_time({app="a"} | logfmt [5m]))` {
		t.Fatalf("asked %q, %v", asked, err)
	}
	plain, _ := translator.TranslateLogQL(`count_over_time({app="a"} | logfmt [5m])`)
	if text == "" || text == plain {
		t.Fatalf("the rewrite is the plain translation %q", text)
	}
}
