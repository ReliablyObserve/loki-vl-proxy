package queryplan

import (
	"reflect"
	"testing"
	"time"
)

func resolved(t testing.TB, q string) *Plan {
	t.Helper()
	e, err := parseCanon(q)
	if err != nil {
		t.Fatalf("%q: %v", q, err)
	}
	p, err := Build(e, Inputs{})
	if err != nil {
		t.Fatalf("%q: %v", q, err)
	}
	Resolve(p)
	return p
}

// firstSelect is the first LogSelect the resolver meets: the left-most one.
func firstSelect(n Node) *LogSelect {
	for {
		switch x := n.(type) {
		case *LogSelect:
			return x
		case *RangeAgg:
			return x.In
		case *VectorAgg:
			n = x.In
		case *Rank:
			n = x.In
		case *LabelReplace:
			n = x.In
		case *BinOp:
			n = x.L
		default:
			return nil
		}
	}
}

func TestResolvePipelineErrors(t *testing.T) {
	cases := []struct {
		q       string
		parsers int
		policy  ErrorPolicy
		at      int
		rules   []RuleID
	}{
		{q: `{a="x"}`, policy: ErrorNoParser, at: -1},
		{q: `{a="x"} |= "e" | drop __error__`, policy: ErrorNoParser, at: -1},
		{q: `{a="x"} | json`, parsers: 1, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: `{a="x"} | json | drop __error__`, parsers: 1, policy: ErrorDroppedLabel, at: 1, rules: []RuleID{RuleDropErrorKeepsLine}},
		{q: `{a="x"} | logfmt | drop __error__, __error_details__`, parsers: 1, policy: ErrorDroppedLabel, at: 1, rules: []RuleID{RuleDropErrorKeepsLine}},
		{q: `{a="x"} | logfmt | drop __error_details__, __error__`, parsers: 1, policy: ErrorDroppedLabel, at: 1, rules: []RuleID{RuleDropErrorKeepsLine}},
		{q: `{a="x"} | json | drop __error_details__`, parsers: 1, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: `{a="x"} | json | drop __error__="JSONParserErr"`, parsers: 1, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: `{a="x"} | json | drop __error__, b`, parsers: 1, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: `{a="x"} | json | drop __error__ | line_format "x"`, parsers: 1, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: `{a="x"} | drop __error__ | json`, parsers: 1, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: `{a="x"} | json | logfmt | drop __error__`, parsers: 2, policy: ErrorDroppedLabel, at: 2, rules: []RuleID{RuleDropErrorKeepsLine}},
		{q: `{a="x"} | json | drop __error__ | logfmt`, parsers: 2, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: "{a=\"x\"} | regexp `(?P<b>.)` | pattern `<c>` | unpack", parsers: 3, policy: ErrorPropagate, at: -1, rules: []RuleID{RuleParserErrorLabel}},
		{q: `sum(count_over_time({a="x"} | json | drop __error__ [5m]))`, parsers: 1, policy: ErrorDroppedLabel, at: 1, rules: []RuleID{RuleDropErrorKeepsLine}},
	}
	for _, c := range cases {
		p := resolved(t, c.q)
		s := firstSelect(p.Root)
		if s.Sem == nil {
			t.Fatalf("%q: no PipelineSem", c.q)
		}
		got := *s.Sem
		if got.Parsers != c.parsers || got.ErrorPolicy != c.policy || got.ErrorAt != c.at || !reflect.DeepEqual(p.Sem.Rules, c.rules) {
			t.Errorf("%q: parsers %d policy %d at %d rules %v, want %d %d %d %v", c.q, got.Parsers, got.ErrorPolicy, got.ErrorAt, p.Sem.Rules, c.parsers, c.policy, c.at, c.rules)
		}
	}
}

func TestResolveEveryPipelineOfABinaryExpression(t *testing.T) {
	p := resolved(t, `sum(rate({a="x"} | json [5m])) / sum(rate({a="y"} | json | drop __error__ [5m]))`)
	b := p.Root.(*BinOp)
	l, r := firstSelect(b.L), firstSelect(b.R)
	if l.Sem.ErrorPolicy != ErrorPropagate || r.Sem.ErrorPolicy != ErrorDroppedLabel {
		t.Fatalf("left %+v right %+v", *l.Sem, *r.Sem)
	}
	if want := []RuleID{RuleParserErrorLabel, RuleDropErrorKeepsLine}; !reflect.DeepEqual(p.Sem.Rules, want) {
		t.Fatalf("rules %v, want %v", p.Sem.Rules, want)
	}
}

func TestResolveUngrouped(t *testing.T) {
	cases := map[string]bool{
		`sum(rate({a="x"}[5m]))`:                                                true,
		`sum by () (rate({a="x"}[5m]))`:                                         false,
		`sum by (a) (rate({a="x"}[5m]))`:                                        false,
		`sum(rate({a="x"}[5m])) by (a)`:                                         false,
		`sum without (a) (rate({a="x"}[5m]))`:                                   false,
		`topk(3, rate({a="x"}[5m]))`:                                            true,
		`topk by (a) (3, rate({a="x"}[5m]))`:                                    false,
		`sort(rate({a="x"}[5m]))`:                                               true,
		`label_replace(sum(rate({a="x"}[5m])), "b", "$1", "a", "(.*)")`:         true,
		`label_replace(sum by (a) (rate({a="x"}[5m])), "b", "$1", "a", "(.*)")`: false,
		`rate({a="x"}[5m])`:                                                     false,
		`count_over_time({a="x"}[5m])`:                                          false,
		`sum(rate({a="x"}[5m])) / sum(rate({a="y"}[5m]))`:                       false,
		`{a="x"}`:                             false,
		`sum(sum by (a) (rate({a="x"}[5m])))`: true,
	}
	for q, want := range cases {
		if got := resolved(t, q).Sem.Ungrouped; got != want {
			t.Errorf("%q: ungrouped %v, want %v", q, got, want)
		}
	}
}

func TestResolveOffsets(t *testing.T) {
	cases := []struct {
		q    string
		kind OffsetKind
		off  time.Duration
	}{
		{`rate({a="x"}[5m])`, OffsetNone, 0},
		{`{a="x"}`, OffsetNone, 0},
		{`rate({a="x"}[5m] offset 1h)`, OffsetUniform, time.Hour},
		{`rate({a="x"}[5m] offset 0s)`, OffsetUniform, 0},
		{`sum(rate({a="x"}[5m] offset 1h)) / sum(rate({a="y"}[5m] offset 60m))`, OffsetUniform, time.Hour},
		{`sum(rate({a="x"}[5m] offset 1h)) / sum(rate({a="y"}[5m]))`, OffsetMixed, 0},
		{`sum(rate({a="x"}[5m])) / sum(rate({a="y"}[5m] offset 1h))`, OffsetMixed, 0},
		{`sum(rate({a="x"}[5m] offset 1h)) / sum(rate({a="y"}[5m] offset 2h))`, OffsetDistinct, 0},
		{`rate({a="x"}[5m] offset 1h) / rate({a="y"}[5m] offset 2h) / rate({a="z"}[5m])`, OffsetMixed, 0},
		{`rate({a="x"}[5m] offset 0s) / rate({a="y"}[5m])`, OffsetMixed, 0},
	}
	for _, c := range cases {
		s := resolved(t, c.q).Sem
		if s.OffsetKind != c.kind || s.Offset != c.off {
			t.Errorf("%q: offset %d %v, want %d %v", c.q, s.OffsetKind, s.Offset, c.kind, c.off)
		}
	}
}

func TestResolveIsIdempotentAndKeepsTheKey(t *testing.T) {
	q := `sum by (a) (count_over_time({a="x"} | json | drop __error__ [5m] offset 1h))`
	e, err := parseCanon(q)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Build(e, Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	key := string(p.Key)
	Resolve(p)
	first := p.Sem
	sel := firstSelect(p.Root).Sem
	Resolve(p)
	if !reflect.DeepEqual(first, p.Sem) || firstSelect(p.Root).Sem != sel {
		t.Fatal("a second Resolve changed the plan")
	}
	if string(p.Key) != key {
		t.Fatal("Resolve changed the key")
	}
}

// Resolve on every corpus row: total, and the same answer for the same tree.
func TestResolveCorpus(t *testing.T) {
	texts, exprs := corpus(t)
	for i, e := range exprs {
		a, err := Build(e, Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := Build(e, Inputs{})
		Resolve(a)
		Resolve(b)
		if !reflect.DeepEqual(a.Sem, b.Sem) {
			t.Fatalf("%q: two resolves differ", texts[i])
		}
	}
}

// Resolve allocates one PipelineSem per pipeline and the rule list: two allocations for a query of the usual shape,
// one more per further pipeline.
func TestResolveAllocLocks(t *testing.T) {
	for q, want := range map[string]float64{
		`sum by (a) (rate({a="x"} | json [5m]))`:                          2,
		`rate({a="x"}[5m])`:                                               1,
		`sum(rate({a="x"} | json [5m])) / sum(rate({a="y"} | json [5m]))`: 3,
	} {
		p := resolved(t, q)
		got := testing.AllocsPerRun(100, func() {
			p.Sem = PlanSem{}
			Resolve(p)
		})
		if got > want {
			t.Errorf("%q: %v allocations, want at most %v", q, got, want)
		}
	}
}
