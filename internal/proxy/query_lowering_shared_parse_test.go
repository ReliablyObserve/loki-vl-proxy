package proxy

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// sharedParseServed are queries the translation serves from the lowering, and sharedParseTurnedAway ones it leaves
// to the string translator, with the stage families and constructs the proxy serves by default.
var (
	sharedParseServed = []string{
		`{app="shared-a"} |= "x" | status >= 500`,
		`{app="shared-b"} |= "x" | detected_level="error"`,
		`{app="shared-c"} |pattern"<_> <m> <_>" | m="GET"`,
		`sum by (app) (rate({app="shared-d"} |= "x" [5m]))`,
		`{app="shared-json"} |= "x" | json | status="500"`,
		"{app=\"shared-regexp\"} | regexp `(?P<m>[A-Z]+) ` | m=\"GET\"",
		`{app="shared-line-format"} |= "x" | line_format "{{.a}} {{.b}}"`,
		`{app="shared-label-format"} | label_format x="{{.a}}"`,
		`{app="shared-logfmt"} | logfmt | status >= 500`,
	}
	sharedParseTurnedAway = []string{
		`{app="shared-e"} |= "x" | service_name="api"`,
		`topk(3, sum by (app) (rate({app="shared-f"}[5m])))`,
	}
)

// The request's validation parses a query once, and a query the translation serves from the lowering is not parsed
// again: the validation keeps its parse in the plan memo (lowersParse), the lowering builds its plan from it, and
// the request's flow counts no parse. A query the lowering leaves to the string translator, and one with a blank in
// front (the translation reads it trimmed), is not kept: no translation reads it.
//
// conformance: architecture-syntax-package-layering, observability-query-flow
func TestQueryPlanLowering_ReadsTheValidationsParse(t *testing.T) {
	p := newLoweringProfileProxy(t, "underscores", func(c *Config) { c.LabelStyle = LabelStyleUnderscores }, "")
	for _, q := range sharedParseServed {
		if v := cachedQueryValidation(q); v.msg != "" {
			t.Fatalf("%q: %s", q, v.msg)
		}
		e, ok := planMemoLoad(q)
		if !ok || e.expr == nil {
			t.Fatalf("%q: the validation's parse is not kept", q)
		}
		ctx, _ := flowCtx()
		if _, err := p.translateQueryOpts(ctx, q, false); err != nil {
			t.Fatal(err)
		}
		if f := flowOf(ctx); f.parseCount != 0 {
			t.Errorf("%q: parsed again by the translation: %d parses", q, f.parseCount)
		}
		plan, err := loweringPlan(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if sel := firstLogSelect(plan.Root); sel == nil || len(sel.Stages) == 0 || !sameStage(e.expr, sel.Stages[0]) {
			t.Errorf("%q: the plan is not built from the validation's parse", q)
		}
	}
	for _, q := range append(append([]string(nil), sharedParseTurnedAway...), ` {app="shared-g"} |= "x"`) {
		cachedQueryValidation(q)
		if _, ok := planMemoLoad(q); ok {
			t.Errorf("%q: kept for a translation that does not read it", q)
		}
	}
}

// sameStage reports whether st is the first pipeline stage of the log query in expr, by identity.
func sameStage(expr logqlpkg.Expr, st any) bool {
	for {
		switch x := expr.(type) {
		case *logqlpkg.LogQuery:
			return len(x.Pipeline) > 0 && any(x.Pipeline[0]) == st
		case *logqlpkg.RangeAggregation:
			expr = x.Inner
		case *logqlpkg.VectorAggregation:
			expr = x.Inner
		default:
			return false
		}
	}
}

// The validation's parse of every query of the plan corpus it accepts is the tree the parser gives the query: the
// lowering reads the one for the other.
//
// conformance: architecture-syntax-package-layering
func TestValidationParseIsTheParse(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, q := range corpus {
		expr, msg, _ := validateQueryTree(q)
		if msg != "" || expr == nil {
			continue
		}
		n++
		parsed, err := logqlpkg.Parse(q)
		if err != nil || !reflect.DeepEqual(parsed, expr) {
			t.Errorf("%q: the validation's tree is not the parse (%v)", q, err)
		}
	}
	if n < 1000 {
		t.Fatalf("%d queries validated", n)
	}
}

// firstLogSelect is the log selector of a plan of one log query, under any range and vector aggregation.
func firstLogSelect(n queryplan.Node) *queryplan.LogSelect {
	for {
		switch x := n.(type) {
		case *queryplan.LogSelect:
			return x
		case *queryplan.RangeAgg:
			n = x.In
		case *queryplan.VectorAgg:
			n = x.In
		default:
			return nil
		}
	}
}

// turnedAwayByStages, the validation's typed pre-check before the text screens, never names a query the translation
// serves from the plan: on every query of the plan corpus it names, under every profile, the log and metric lowerings
// leave the query to the string translator, and lowersParse would not have kept it either. It names the turned-away
// shapes of the miss benchmark.
//
// conformance: architecture-syntax-package-layering
func TestTurnedAwayByStagesNamesNoServedQuery(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	corpus, err := plancorpus.Queries(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	named := 0
	for _, prof := range loweringParityProfiles {
		p := newLoweringProfileProxy(t, prof.name, prof.config, prof.version)
		for _, q := range corpus {
			expr, msg, _ := validateQueryTree(q)
			if msg != "" || expr == nil || !turnedAwayByStages(expr) {
				continue
			}
			named++
			canon := logqlpkg.CanonicalizeLogRanges(strings.TrimSpace(q))
			lp := logLoweringProfile(ctx, p, canon, false)
			if _, ok := p.lowerLogSelect(ctx, canon, lp); ok {
				t.Errorf("%s %q: named, but served from the plan", prof.name, q)
			}
			if _, ok := p.lowerMetric(ctx, canon, lp); ok {
				t.Errorf("%s %q: named, but served from the plan", prof.name, q)
			}
			if lowersParse(q) {
				t.Errorf("%s %q: named, but the screens pass it", prof.name, q)
			}
		}
	}
	if named == 0 {
		t.Fatal("no corpus query named")
	}
	for _, q := range []string{
		`{app="a"} |= "x" | decolorize`, `{app="a"} | unpack | x="y"`, `{app="a"} | logfmt | detected_level="error"`,
		`{app="a"} | json | status == 500`, `sum(count_over_time({app="a"} | json | status == 500 [5m]))`,
		`{app="a"} | service_name="x"`, `vector(1)`, `vector(1) + sum(rate({app="a"}[5m]))`,
	} {
		expr, err := logqlpkg.Parse(q)
		if err != nil || !turnedAwayByStages(expr) {
			t.Errorf("%q: not named (%v)", q, err)
		}
	}
	for _, q := range sharedParseServed {
		if expr, err := logqlpkg.Parse(q); err != nil || turnedAwayByStages(expr) {
			t.Errorf("%q: a served query is named (%v)", q, err)
		}
	}
	// An unwrap names a log query (no log query has one), not a range aggregation over one; an empty query is kept by
	// nobody.
	for q, want := range map[string]bool{
		`{app="a"} | json | unwrap n`: true, `sum_over_time({app="a"} | json | unwrap n [5m])`: false,
		`{app="a"} | json | drop x | detected_level="error"`: true, `sum(count_over_time({app="a"} | drop x | detected_level="e" [5m]))`: false,
	} {
		expr, err := logqlpkg.Parse(q)
		if err != nil {
			t.Fatal(err)
		}
		if got := turnedAwayByStages(expr); got != want {
			t.Errorf("turnedAwayByStages(%q) = %v, want %v", q, got, want)
		}
	}
	if lowersParse("") {
		t.Error("an empty query is kept")
	}
}

// A translation that starts with a pipe is sent with the "* " VictoriaLogs needs in front of it, and cached as sent.
//
// conformance: architecture-syntax-package-layering
func TestCacheTranslationPrefixesAPipe(t *testing.T) {
	p := newTestProxy(t, "http://unused")
	if got := p.cacheTranslation("k-pipe", "  | unpack_json"); got != "* | unpack_json" {
		t.Fatalf("cacheTranslation = %q", got)
	}
	if got := p.cacheTranslation("k-plain", `app:="a"`); got != `app:="a"` {
		t.Fatalf("cacheTranslation = %q", got)
	}
	if cached, ok := p.translationCache.Get("k-pipe"); !ok || string(cached) != "* | unpack_json" {
		t.Fatalf("cached %q %v", cached, ok)
	}
}
