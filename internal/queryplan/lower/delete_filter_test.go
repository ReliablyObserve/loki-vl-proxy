package lower

import (
	"errors"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// A delete removes the rows the same log query reads: on every corpus log query and profile, PlanLogFilter is the
// filter of the text PlanLogText writes when that text has no pipe, and NotLoweredError when it has one or when the
// lowering refuses the query.
//
// conformance: semantics/delete-api-via-victorialogs
func TestPlanLogFilterIsTheReadQuerysFilter(t *testing.T) {
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		t.Fatal(err)
	}
	profiles := []Profile{{}, {StreamFields: map[string]bool{"app": true}}, {Label: translate.LabelTranslateFunc(strings.ToUpper)}}
	filters, piped := 0, 0
	for _, q := range qs {
		if !strings.HasPrefix(strings.TrimSpace(q), "{") {
			continue
		}
		expr, err := logql.Parse(q)
		if err != nil {
			continue
		}
		p, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		if _, ok := p.Root.(*queryplan.LogSelect); !ok {
			continue
		}
		queryplan.Resolve(p)
		for _, prof := range profiles {
			text, terr := PlanLogText(p, prof)
			f, ferr := PlanLogFilter(p, prof)
			var nl *NotLoweredError
			switch {
			case terr != nil:
				if ferr == nil {
					t.Errorf("%q: PlanLogText refuses (%v), PlanLogFilter writes %v", q, terr, f)
				}
			case ferr != nil:
				if !errors.As(ferr, &nl) || nl.Construct != deletePipesConstruct || !strings.Contains(text, "|") {
					t.Errorf("%q: PlanLogFilter %v for the text %q", q, ferr, text)
				}
				piped++
			default:
				got := "*"
				if f != nil {
					got = f.String()
				}
				if got != text {
					t.Errorf("%q: filter %q, read query %q", q, got, text)
				}
				filters++
			}
		}
	}
	if filters == 0 || piped == 0 {
		t.Fatalf("filters %d, piped %d: the corpus should hold both", filters, piped)
	}
	var nl *NotLoweredError
	if _, err := PlanLogFilter(planOf(t, `sum(rate({a="b"}[5m]))`), Profile{}); !errors.As(err, &nl) || nl.Construct != "VectorAgg" {
		t.Errorf("a metric plan: %v", err)
	}
	for q, want := range map[string]bool{`{a="b"}`: true, `{a="b"} |= "x" != "y"`: true, `{a="b"} | json`: false, `{a="b"} | logfmt | c="d"`: false} {
		_, err := PlanLogFilter(planOf(t, q), Profile{})
		if (err == nil) != want {
			t.Errorf("%s: %v, want filter %v", q, err, want)
		}
	}
}
