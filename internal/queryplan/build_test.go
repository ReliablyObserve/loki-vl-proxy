package queryplan

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
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
		out = append(out, fmt.Sprintf("%T", s))
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
			want := []LineMatch{{Value: "a"}, {Value: "1.2.3.4", IP: true}}
			if lf.Op != logql.LineFilterContains || !reflect.DeepEqual(lf.Alts, want) {
				t.Errorf("line filter = %+v", lf)
			}
		}},
		{q: `{app="x"} | json a="b", c`, stages: []string{"Parse"}, check: func(t *testing.T, s []Stage) {
			p := s[0].(*Parse)
			if p.Kind != logql.ParserJSON || len(p.Fields) != 2 || p.Param == "" || p.Expr != "" {
				t.Errorf("json parse = %+v", p)
			}
		}},
		{q: `{app="x"} | regexp "(?P<a>.+)" | pattern "<a> <_>" | logfmt | unpack`, stages: []string{"Parse", "Parse", "Parse", "Parse"}, check: func(t *testing.T, s []Stage) {
			if p := s[0].(*Parse); p.Kind != logql.ParserRegexp || p.Expr != "(?P<a>.+)" || p.Param != "" {
				t.Errorf("regexp parse = %+v", p)
			}
			if p := s[1].(*Parse); p.Kind != logql.ParserPattern || p.Expr != "<a> <_>" {
				t.Errorf("pattern parse = %+v", p)
			}
		}},
		{q: `{app="x"} | status>=500 and (a="1" or b=~"x")`, stages: []string{"LabelFilter"}, check: func(t *testing.T, s []Stage) {
			if lf := s[0].(*LabelFilter); lf.Expr == nil || lf.Expr.Op != logql.LabelFilterAnd || lf.Raw != "" {
				t.Errorf("label filter = %+v", lf)
			}
		}},
		{q: `{app="x"} | drop a, b | keep c | drop level="debug" | keep k!~"x"`, stages: []string{"Project", "Project", "Project", "Project"}, check: func(t *testing.T, s []Stage) {
			if d := s[0].(*Project); d.Keep || !reflect.DeepEqual(d.Names, []string{"a", "b"}) {
				t.Errorf("drop = %+v", d)
			}
			if k := s[1].(*Project); !k.Keep || len(k.Conds) != 0 {
				t.Errorf("keep = %+v", k)
			}
			if c := s[2].(*Project); len(c.Conds) != 1 || c.Conds[0] != (Matcher{Name: "level", Value: "debug", Op: logql.MatchEq}) {
				t.Errorf("drop cond = %+v", c)
			}
			if c := s[3].(*Project); c.Conds[0].Op != logql.MatchNotRe {
				t.Errorf("keep cond = %+v", c)
			}
		}},
		{q: `{app="x"} | line_format "{{.msg}}" | label_format a=b, c="{{.d}}" | decolorize`, stages: []string{"LineFormat", "LabelFormat", "Decolorize"}, check: func(t *testing.T, s []Stage) {
			if lf := s[0].(*LineFormat); lf.Raw != "{{.msg}}" {
				t.Errorf("line_format = %+v", lf)
			}
			if lf := s[1].(*LabelFormat); len(lf.Entries) != 2 || !lf.Entries[0].Rename || lf.Entries[1].Rename {
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
				got := stageNames(sel)
				for i := range got {
					got[i] = got[i][len("*queryplan."):]
				}
				if !reflect.DeepEqual(got, c.stages) {
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
	if got := stageNames(ra.In); !reflect.DeepEqual(got, []string{"*queryplan.Parse", "*queryplan.Unwrap"}) {
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

func FuzzBuild(f *testing.F) {
	for _, s := range []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "a" or "b" != ip("1.2.3.4") | json a="b" | logfmt | status>=500 or (a="1" and b=~"x")`,
		`{app="nginx"} | drop a, b="c" | keep d | line_format "{{.m}}" | label_format x=y | decolorize | regexp "(?P<a>.)" | pattern "<a>"`,
		`sum by (app) (rate({app="api"} | json | unwrap duration(d) [5m] offset 1h))`,
		`sum without () (count_over_time({app="api"}[1m]))`,
		`quantile_over_time(0.99, {a="b"} | unwrap n [5m]) by (x)`,
		`topk(5, bytes_rate({a="b"}[1m])) / on (a) group_left (b) sort(rate({a="b"}[1m]))`,
		`label_replace(rate({a="b"}[1m]), "x", "y", "z", "w") + vector(1) + 3`,
		`absent_over_time({a="b"}[5m]) and rate_counter({a="b"} | unwrap n [1m])`,
		`not logql`, `{`, ``, `*`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		expr, err := logql.Parse(q)
		if err != nil {
			return
		}
		in := Inputs{Kind: KindRange, StreamLabels: []string{"app"}}
		a, err := Build(expr, in)
		if err != nil {
			t.Fatalf("Build(%q): %v", q, err)
		}
		b, err := Build(expr, in)
		if err != nil || !bytes.Equal(a.Key, b.Key) || !bytes.HasPrefix(a.Key, a.Root.Props().Key) {
			t.Fatalf("Build(%q) is not deterministic: %v", q, err)
		}
	})
}
