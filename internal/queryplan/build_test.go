package queryplan

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func mustBuild(t testing.TB, q string, in Inputs) *Plan {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	p, err := Build(expr, in)
	if err != nil {
		t.Fatalf("build %q: %v", q, err)
	}
	return p
}

func stageNames(sel *LogSelect) []string {
	var out []string
	for _, s := range sel.Stages {
		out = append(out, strings.TrimSuffix(strings.TrimPrefix(fmt.Sprintf("%T", s), "*logql."), "Stage"))
	}
	return out
}

func TestBuildLogSelectStages(t *testing.T) {
	cases := []struct {
		q      string
		stages []string
		check  func(t *testing.T, s []Stage)
	}{
		{q: `{app="x", env!="dev", pod=~"a.*", z!~"b"}`},
		{q: `{app="x"} |= "a" or ip("1.2.3.4") != "b"`, stages: []string{"LineFilter", "LineFilter"}, check: func(t *testing.T, s []Stage) {
			lf := s[0].(*LineFilter)
			want := []logql.LineFilterAlt{{Value: "1.2.3.4", IP: true}}
			if lf.Op != logql.LineFilterContains || lf.Value != "a" || !reflect.DeepEqual(lf.Or, want) {
				t.Errorf("line filter = %+v", lf)
			}
		}},
		{q: `{app="x"} | json a="b", c`, stages: []string{"Parser"}, check: func(t *testing.T, s []Stage) {
			p := s[0].(*Parse)
			if p.Type != logql.ParserJSON || len(p.Fields) != 2 || p.Param == "" {
				t.Errorf("json parse = %+v", p)
			}
		}},
		{q: `{app="x"} | regexp "(?P<a>.+)" | pattern "<a> <_>" | logfmt | unpack`, stages: []string{"Parser", "Parser", "Parser", "Parser"}, check: func(t *testing.T, s []Stage) {
			if p := s[0].(*Parse); p.Type != logql.ParserRegexp || p.Param != "(?P<a>.+)" {
				t.Errorf("regexp parse = %+v", p)
			}
			if p := s[1].(*Parse); p.Type != logql.ParserPattern || p.Param != "<a> <_>" {
				t.Errorf("pattern parse = %+v", p)
			}
		}},
		{q: `{app="x"} | status>=500 and (a="1" or b=~"x")`, stages: []string{"LabelFilter"}, check: func(t *testing.T, s []Stage) {
			if lf := s[0].(*LabelFilter); lf.Expr == nil || lf.Expr.Op != logql.LabelFilterAnd {
				t.Errorf("label filter = %+v", lf)
			}
		}},
		{q: `{app="x"} | drop a, b | keep c | drop level="debug" | keep k!~"x"`, stages: []string{"Drop", "Keep", "Drop", "Keep"}, check: func(t *testing.T, s []Stage) {
			if d := s[0].(*Drop); !reflect.DeepEqual(d.Labels, []string{"a", "b"}) {
				t.Errorf("drop = %+v", d)
			}
			if c := s[2].(*Drop); len(c.Matchers) != 1 || c.Matchers[0] != (logql.DropMatcher{Name: "level", Op: "=", Value: "debug"}) {
				t.Errorf("drop cond = %+v", c)
			}
			if c := s[3].(*Keep); c.Matchers[0].Op != "!~" {
				t.Errorf("keep cond = %+v", c)
			}
		}},
		{q: `{app="x"} | line_format "{{.msg}}" | label_format a=b, c="{{.d}}" | decolorize`, stages: []string{"LineFormat", "LabelFormat", "Decolorize"}, check: func(t *testing.T, s []Stage) {
			if lf := s[0].(*LineFormat); lf.Template != "{{.msg}}" {
				t.Errorf("line_format = %+v", lf)
			}
			if lf := s[1].(*LabelFormat); len(lf.Formats) != 2 || !lf.Formats[0].Rename || lf.Formats[1].Rename {
				t.Errorf("label_format = %+v", lf)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.q, func(t *testing.T) {
			sel, ok := mustBuild(t, c.q, Inputs{}).Root.(*LogSelect)
			if !ok {
				t.Fatalf("root is not a LogSelect")
			}
			if c.stages != nil {
				if got := stageNames(sel); !reflect.DeepEqual(got, c.stages) {
					t.Fatalf("stages = %v, want %v", got, c.stages)
				}
			}
			if c.check != nil {
				c.check(t, sel.Stages)
			}
		})
	}

	sel := mustBuild(t, `{app="x", env!="dev", pod=~"a.*", z!~"b"}`, Inputs{}).Root.(*LogSelect)
	want := []Matcher{
		{Name: "app", Value: "x", Op: logql.MatchEq}, {Name: "env", Value: "dev", Op: logql.MatchNeq},
		{Name: "pod", Value: "a.*", Op: logql.MatchRe}, {Name: "z", Value: "b", Op: logql.MatchNotRe},
	}
	if !reflect.DeepEqual(sel.Matchers, want) {
		t.Errorf("matchers = %+v", sel.Matchers)
	}
}

func TestBuildUntypedLabelFilterKeepsRaw(t *testing.T) {
	expr, err := logql.Parse(`{app="x"} | status>=500`)
	if err != nil {
		t.Fatal(err)
	}
	expr.(*logql.LogQuery).Pipeline[0].(*logql.LabelFilterStage).Expr = nil
	p, err := Build(expr, Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	lf := p.Root.(*LogSelect).Stages[0].(*LabelFilter)
	if lf.Expr != nil || lf.Raw != "status>=500" {
		t.Errorf("label filter = %+v", lf)
	}
}

func TestBuildMetricNodes(t *testing.T) {
	p := mustBuild(t, `sum by (app) (rate({app="x"} | json | unwrap n [5m] offset 1h))`, Inputs{Kind: KindRange})
	va, ok := p.Root.(*VectorAgg)
	if !ok || va.Op != logql.VectorSum || va.By == nil || va.By.Without {
		t.Fatalf("root = %#v", p.Root)
	}
	ra, ok := va.In.(*RangeAgg)
	if !ok || ra.Op != logql.RangeRate || ra.Range != 5*time.Minute || ra.Offset != time.Hour || !ra.HasOffset {
		t.Fatalf("range agg = %#v", va.In)
	}
	if got := stageNames(ra.In); !reflect.DeepEqual(got, []string{"Parser", "Unwrap"}) {
		t.Errorf("stages = %v", got)
	}
	if p.Root.Props().Lookback != 65*time.Minute || p.Root.Props().Offset != time.Hour {
		t.Errorf("props = %+v", p.Root.Props())
	}

	q := mustBuild(t, `quantile_over_time(0.9, {a="b"} | unwrap n [1m]) by (x)`, Inputs{}).Root.(*RangeAgg)
	if q.Phi != 0.9 || q.By == nil || q.HasOffset {
		t.Errorf("quantile = %+v", q)
	}

	for _, c := range []struct {
		q  string
		op logql.VectorOp
		k  int
	}{
		{`topk(3, rate({a="b"}[1m]))`, logql.VectorTopK, 3},
		{`bottomk(2, rate({a="b"}[1m]))`, logql.VectorBottomK, 2},
		{`sort(rate({a="b"}[1m]))`, logql.VectorSort, 0},
		{`sort_desc(rate({a="b"}[1m]))`, logql.VectorSortDesc, 0},
	} {
		r, ok := mustBuild(t, c.q, Inputs{}).Root.(*Rank)
		if !ok || r.Op != c.op || r.K != c.k {
			t.Errorf("%s: root = %#v", c.q, r)
		}
	}

	b := mustBuild(t, `sum(rate({a="b"}[1m])) / on (x) group_left (y) 2`, Inputs{}).Root.(*BinOp)
	if b.Op != "/" || b.Match == nil || b.Match.Card != "on" || b.Match.GroupSide != "group_left" {
		t.Errorf("binop = %+v", b)
	}
	if s, ok := b.R.(*Scalar); !ok || s.V != 2 {
		t.Errorf("right = %#v", b.R)
	}
	if _, ok := mustBuild(t, `label_replace(rate({a="b"}[1m]), "x", "y", "z", "w")`, Inputs{}).Root.(*Opaque); !ok {
		t.Errorf("label_replace is not Opaque")
	}
	if _, ok := mustBuild(t, `vector(1)`, Inputs{}).Root.(*Opaque); !ok {
		t.Errorf("vector is not Opaque")
	}
	if _, ok := mustBuild(t, `{a="b"}`, Inputs{}).Root.(*LogSelect); !ok {
		t.Errorf("selector is not a LogSelect")
	}
}

func TestBuildErrors(t *testing.T) {
	expr, _ := logql.Parse(`{a="b"}`)
	for _, in := range []Inputs{{Compat: Compat(1)}, {Kind: Kind(9)}} {
		var u *UnsupportedError
		if _, err := Build(expr, in); !errors.As(err, &u) {
			t.Errorf("Build(%+v) error = %v, want UnsupportedError", in, err)
		}
	}
	var u *UnsupportedError
	if _, err := Build(nil, Inputs{}); !errors.As(err, &u) {
		t.Errorf("Build(nil) error = %v", err)
	}
}

// A subtree has the same key alone and inside a larger query; Inputs are
// appended to the plan key only.
func TestKeySubtreeAndInputs(t *testing.T) {
	inner := `sum without (pod) (rate({app="a"} | json [5m]))`
	alone := mustBuild(t, inner, Inputs{})
	bin := mustBuild(t, inner+` / on (app) `+inner, Inputs{}).Root.(*BinOp)
	for _, side := range []Node{bin.L, bin.R} {
		if !bytes.Equal(side.Props().Key, alone.Root.Props().Key) {
			t.Errorf("side key differs from the standalone key")
		}
	}
	if !bytes.HasPrefix(alone.Key, alone.Root.Props().Key) || len(alone.Key) == len(alone.Root.Props().Key) {
		t.Errorf("plan key does not extend the root key with the inputs")
	}
	other := mustBuild(t, inner, Inputs{Kind: KindInstant, StreamLabels: []string{"app"}, Exposure: Exposure{DottedNames: true}})
	if !bytes.Equal(other.Root.Props().Key, alone.Root.Props().Key) {
		t.Errorf("inputs changed the node key")
	}
	if bytes.Equal(other.Key, alone.Key) {
		t.Errorf("inputs did not change the plan key")
	}
}

func TestBuildDeterministic(t *testing.T) {
	q := `topk(2, sum by (a) (rate({a="b"} | logfmt x="y" | z>1 [1m] offset 5m))) > bool 3`
	if a, b := mustBuild(t, q, Inputs{}), mustBuild(t, q, Inputs{}); !bytes.Equal(a.Key, b.Key) {
		t.Errorf("same query, different keys")
	}
}

// An Expr the parser cannot produce is refused with an UnsupportedError and no plan, wherever it sits, and the
// builds after it are not affected.
func TestBuildRefusesWhatItHasNoNodeFor(t *testing.T) {
	sel := &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "a", Value: "b"}}}
	good := &logql.RangeAggregation{Op: logql.RangeRate, Inner: &logql.LogQuery{Selector: sel}, Range: "1m"}
	bad := &logql.RangeAggregation{Op: logql.RangeRate, Inner: &logql.LiteralExpr{Value: 1}}
	cases := map[string]logql.Expr{
		"nothing":                nil,
		"a selector as a stage":  &logql.LogQuery{Selector: sel, Pipeline: []logql.Stage{&logql.StreamSelector{}}},
		"a literal as a range":   bad,
		"under a vector":         &logql.VectorAggregation{Op: logql.VectorSum, Inner: bad},
		"under a rank":           &logql.VectorAggregation{Op: logql.VectorTopK, Param: 1, HasParam: true, Inner: bad},
		"left of a binary":       &logql.BinOpExpr{Op: "+", Left: bad, Right: good},
		"right of a binary":      &logql.BinOpExpr{Op: "+", Left: good, Right: bad},
		"in a range's selector":  &logql.RangeAggregation{Op: logql.RangeRate, Inner: &logql.LogQuery{Selector: sel, Pipeline: []logql.Stage{nil}}},
		"a nil vector's operand": &logql.VectorAggregation{Op: logql.VectorSum},
	}
	for name, e := range cases {
		var u *UnsupportedError
		p, err := Build(e, Inputs{})
		if p != nil || !errors.As(err, &u) || u.Error() == "" {
			t.Errorf("%s: plan %v, error %v", name, p, err)
		}
	}
	// A selector that is missing builds an empty one, and a range with no text reads as a zero range.
	p, err := Build(&logql.LogQuery{}, Inputs{})
	if err != nil || len(p.Root.(*LogSelect).Matchers) != 0 {
		t.Errorf("a query without a selector: %v", err)
	}
	if p, err := Build(good, Inputs{}); err != nil || p.Root.(*RangeAgg).Range != 0 {
		t.Errorf("a range without durations: %v", err)
	}
}

// Every node reports its key, lookback and offset; the nodes of a query with offsets take the largest.
func TestBuildProps(t *testing.T) {
	p := mustBuild(t, `sum(rate({a="b"}[5m] offset 1h)) / sum(rate({a="b"}[1h] offset 10m)) + 1 + vector(1)`, Inputs{})
	got := p.Root.Props()
	if got.Lookback != 70*time.Minute || got.Offset != time.Hour {
		t.Errorf("props = %+v", got)
	}
	var walk func(n Node)
	walk = func(n Node) {
		if len(n.Props().Key) == 0 {
			t.Errorf("%T has no key", n)
		}
		switch x := n.(type) {
		case *BinOp:
			walk(x.L)
			walk(x.R)
		case *VectorAgg:
			walk(x.In)
		case *RangeAgg:
			walk(x.In)
		}
	}
	walk(p.Root)
	if s := mustBuild(t, `1`, Inputs{}).Root.(*Scalar); s.V != 1 {
		t.Errorf("scalar = %v", s.V)
	}
}

// A bare stream selector is a log selector, at the root and as the input of a range aggregation.
func TestBuildBareStreamSelector(t *testing.T) {
	sel := &logql.StreamSelector{Matchers: []logql.LabelMatcher{{Name: "a", Value: "b"}}}
	if p, err := Build(sel, Inputs{}); err != nil || len(p.Root.(*LogSelect).Matchers) != 1 {
		t.Errorf("root selector: %v", err)
	}
	r := &logql.RangeAggregation{Op: logql.RangeCountOverTime, Inner: sel, Range: "1m", RangeDuration: time.Minute}
	p, err := Build(r, Inputs{})
	if err != nil || p.Root.(*RangeAgg).In == nil || p.Root.(*RangeAgg).Range != time.Minute {
		t.Errorf("selector as a range input: %v", err)
	}
	checkKey(t, p, r, "bare selector")
}
