package lower

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
)

// logScreenPlan is the resolved plan of a query, or nil when it does not parse or build.
func logScreenPlan(t testing.TB, q string) *queryplan.Plan {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		return nil
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil
	}
	queryplan.Resolve(p)
	return p
}

// The log stage screen names the stage families of a log query the plan may serve, and turns away, before any parse,
// every class of query the plan leaves to the string translator. Each row the screen admits also parses, and its
// families are the plan's (PlanLogStages).
//
// conformance: architecture-syntax-package-layering
func TestScreenLogStages(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  LogStages
	}{
		// Level filters, alone and with line and label filters around them.
		{`{app="api"} | detected_level="error"`, LogLevelFilter},
		{`{service_name="api"} |= "error" | detected_level="error"`, LogLevelFilter},
		{`{app="api"} | detected_level=~"error|warn" |= "x" | status >= 500`, LogLevelFilter},
		{`{app="api"} | detected_level!="debug" or detected_level="info"`, LogLevelFilter},
		{`{app="api"} | (detected_level="error" and path="/x")`, LogLevelFilter},
		{"{app=\"api\"} | detected_level=`error`", LogLevelFilter},
		{`{detected_level="error"} | detected_level="error"`, LogLevelFilter},
		{`{app="api"} | k8s.pod.name="p" | detected_level="error"`, LogLevelFilter},
		{`{app="api"} | detected_level="error" | k8s.pod.name`, 0},
		// What the screen leaves to the string translator.
		{`{app="api"}`, 0},
		{`{app="api"} |= "x" | status >= 500`, 0},
		{`{app="api"} | service_name="api"`, 0},
		{`{app="api"} | detected_level="error" | service_name="api"`, 0},
		{`{app="api"} | json | detected_level="error"`, 0},
		// The parsers and formatting stages are families of their own.
		{`{app="api"} | logfmt`, LogLogfmt},
		{`{app="api"} | json | status="500"`, LogJSON},
		{`{app="api"} |= "x" | json m="a.b", n="c" | m="x"`, LogJSON},
		{`{app="api"} | json a, b`, LogJSON},
		{`{app="api"} | logfmt --strict --keep-empty | status >= 500`, LogLogfmt},
		{`{app="api"} | logfmt | path = "/api" | json`, LogLogfmt | LogJSON},
		{`{app="api"} | json | status == 500`, 0},
		{`{app="api"} | json | status = 500`, 0},
		{`{app="api"} | json | status != 5m`, 0},
		{`{app="api"} | json | status > 500`, LogJSON},
		{`{app="api"} | json | _time="x"`, LogJSON},
		{`{app="api"} | _time="x" | json`, 0},
		{`{app="api"} | json | -x="y"`, 0},
		{`{app="api"} | json | x`, 0},
		{`{app="api"} | drop a, b | keep c, d`, LogDropKeep},
		{`{app="api"} | drop a="x", b`, LogDropKeep},
		{`{app="api"} | keep a="x", b="y"`, LogDropKeep},
		{`{app="api"} | keep a="x", b`, 0},
		{`{app="api"} | keep b, a=~"x"`, 0},
		{`{service_name="api"} | drop detected_level`, LogDropKeep},
		{`{service_name="api"} | keep detected_level`, LogDropKeep},
		{`{service_name="api"} | drop detected_level="info"`, 0},
		{`{app="api"} | drop service_name`, 0},
		{`{app="api"} | json | label_format x="{{.a}} {{ .b }}", y=z`, LogJSON | LogLabelFormat},
		{`{app="api"} | label_format x="{{.a | upper}}"`, 0},
		{`{app="api"} | line_format "{{.a}} and {{.b}}"`, LogLineFormat},
		{`{app="api"} | line_format "plain"`, LogLineFormat},
		{"{app=\"api\"} | line_format `{{.a}}`", LogLineFormat},
		{`{app="api"} | line_format "{{.a.b}}"`, 0},
		{`{app="api"} | line_format"{{.a}}"`, 0},
		{`{app="api"} | line_format "{{"`, 0},
		{`{app="api"} | line_format "{{.a"`, 0},
		{`{app="api"} | line_format "a }} b"`, 0},
		{`{app="api"} | line_format "<a>"`, 0},
		{`{app="api"} | line_format "a\nb"`, 0},
		{`{app="api"} | line_format "{{- .a -}}"`, 0},
		{`{app="api"} | line_format "{{.}}"`, 0},
		{`{app="api"} | json | line_format "{{.a}}" | detected_level="x"`, 0},
		// Regexp and pattern parsers are a family; one written with no blank before its quoted expression is named too.
		{`{app="api"} | regexp "(?P<a>x)"`, LogRegexpPattern},
		{`{app="api"} | pattern "<_> <m>"`, LogRegexpPattern},
		{`{app="api"} |pattern"<_> <m>"`, LogRegexpPattern | LogUnspacedParser},
		{"{app=\"api\"} |regexp`(?P<a>x)` | a=\"x\"", LogRegexpPattern | LogUnspacedParser},
		{`{app="api"} |= "x" |pattern"<_> <m>" | pattern "<n> <_>" | m!="y"`, LogRegexpPattern | LogUnspacedParser},
		{`{app="api"} |pattern"<_> <m>" | json`, LogRegexpPattern | LogUnspacedParser | LogJSON},
		{`{app="api"} |pattern"<_> <m>" | detected_level="error"`, 0},
		{`{app="api"} | pattern="x"`, 0},
		{`{app="api"} | unpack`, 0},
		{`{app="api"} | drop a`, LogDropKeep},
		{`{app="api"} | keep a`, LogDropKeep},
		{`{app="api"} | line_format "{{.a}}"`, LogLineFormat},
		{`{app="api"} | label_format a=b`, LogLabelFormat},
		{`{app="api"} | decolorize`, 0},
		{`{app="api"} | json | decolorize`, 0},
		{`{app="api"} | unwrap d`, 0},
		{`{app="api"} | detected_level="error" # a comment`, 0},
		{`{app="api"} | detected_level="error" | s="a detected_level b"`, 0},
		// The translator's marker bytes in a pipeline value: the plan keeps them (semantics/translator-reads-marker-bytes-in-quoted-value);
		// in a stream selector they stay turned away, as does an escape the parser does not read.
		{`{app="api"} | detected_level="error" | s="\x01"`, LogLevelFilter},
		{"{app=\"api\"} | json | s=`\x02b\x02`", LogJSON},
		{`{app="api"} | json | s="\u0002" | t="\002"`, LogJSON},
		{`{app="api", s="\x02"} | json`, 0},
		{`{app="api"} | json | s="\q"`, 0},
		{`{app="api"} | detected_level="error" | n > -5`, 0},
		{`{app="api"} | detected_level="error" | _time="x"`, 0},
		{`{app="api"} | detected_level="error" | by="x"`, 0},
		{`{app="api"} | detected_level="error" | sum="x"`, LogLevelFilter},
		{`{app="api"} | detected_level="error"}`, 0},
		{`{app="api"} | detected_level`, 0},
		{`{app="api"} | detected_level | x="y"`, 0},
		{`{app="api"} | detected_level  != "a"`, LogLevelFilter},
		{`{app="api"} | A | detected_level="error"`, 0},
		{`{app="api"} | detected_level !> "x"`, 0},
		{`{app="api"} | detected_level=0%0`, 0},
		{`{app="api"} | detected_level="a" + 1`, 0},
		{`{app="api"} | detected_level !~ "x"`, LogLevelFilter},
		{`{app="api"} | detected_level="error" | a = ip("1.2.3.4")`, LogLevelFilter},
		{`{app="api"} | detected_level="error`, 0},
		{`count_over_time({app="api"} | detected_level="error" [5m])`, 0},
		{``, 0},
		{`   `, 0},
	} {
		got := ScreenLogStages(tc.query)
		if got != tc.want {
			t.Errorf("ScreenLogStages(%q) = %b, want %b", tc.query, got, tc.want)
			continue
		}
		if got == 0 {
			continue
		}
		p := logScreenPlan(t, tc.query)
		if p == nil {
			t.Errorf("%q: admitted, but does not plan", tc.query)
			continue
		}
		// The plan holds the parsers (LogRegexpPattern), not their spelling (LogUnspacedParser).
		if planned, ok := PlanLogStages(p); !ok || planned != got&^LogUnspacedParser {
			t.Errorf("%q: the screen names %b, the plan holds %b (%v)", tc.query, got, planned, ok)
		}
	}
}

// PlanLogStages reads the families from the plan's stages: line and label filters on other labels have none, a stage
// no family holds and a filter on service_name refuse the query.
//
// conformance: architecture-syntax-package-layering
func TestPlanLogStages(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  LogStages
		ok    bool
	}{
		{`{app="api"}`, 0, true},
		{`{app="api"} |= "x" | status >= 500`, 0, true},
		{`{app="api"} | detected_level="error"`, LogLevelFilter, true},
		{`{app="api"} | a="1" or detected_level="error"`, LogLevelFilter, true},
		{`{app="api"} | service_name="api"`, 0, false},
		{`{app="api"} | a="1" or service_name="api"`, 0, false},
		{`{app="api"} | json`, LogJSON, true},
		{`{app="api"} | logfmt | a="x"`, LogLogfmt, true},
		{`{app="api"} | pattern "<_> <m>" | m="x"`, LogRegexpPattern, true},
		{"{app=\"api\"} | regexp `(?P<a>x)`", LogRegexpPattern, true},
		{`{app="api"} | unpack`, 0, false},
		{`{app="api"} | decolorize`, 0, false},
		{`{app="api"} | detected_level`, 0, false},
		{`{app="api"} | drop a | keep b`, LogDropKeep, true},
		{`{app="api"} | line_format "x"`, LogLineFormat, true},
		{`{app="api"} | label_format a=b`, LogLabelFormat, true},
		{`{app="api"} | json | service_name="x"`, 0, false},
		{`count_over_time({app="api"}[5m])`, 0, false},
	} {
		p := logScreenPlan(t, tc.query)
		if p == nil {
			t.Fatalf("%q does not plan", tc.query)
		}
		if got, ok := PlanLogStages(p); got != tc.want || ok != tc.ok {
			t.Errorf("PlanLogStages(%q) = %b, %v; want %b, %v", tc.query, got, ok, tc.want, tc.ok)
		}
	}
	if namesDerivedLabel(nil, "detected_level") {
		t.Error("a nil filter names a label")
	}
	if got := LogStagesOf([]string{"LevelFilter", "VectorAgg", "", "UnspacedParser"}); got != LogLevelFilter|LogUnspacedParser {
		t.Errorf("LogStagesOf = %b", got)
	}
	if got := LogStagesOf([]string{"JSON", "Logfmt", "DropKeep", "LabelFormat", "LineFormat", "RegexpPattern"}); got != LogJSON|LogLogfmt|LogDropKeep|LogLabelFormat|LogLineFormat|LogRegexpPattern {
		t.Errorf("LogStagesOf = %b", got)
	}
}

// The screen makes no allocation, whatever it reads.
//
// conformance: architecture-syntax-package-layering
func TestScreenLogStagesAllocatesNothing(t *testing.T) {
	for _, q := range []string{
		`{service_name="api"} |= "error" | detected_level="error"`,
		`{app="api"} | detected_level="error" | s="\x01"`,
		`{app="api"} | json | status="500"`,
		`{app="api"} | logfmt --strict | keep a, b | label_format x="{{.a}}" | line_format "{{.x}}"`,
	} {
		if n := testing.AllocsPerRun(100, func() { _ = ScreenLogStages(q) }); n != 0 {
			t.Errorf("ScreenLogStages(%q): %v allocations", q, n)
		}
	}
}

// FuzzScreenLogStages holds the screen, on any text, to a set of known families, with no allocation; and where it
// admits a query that parses and builds, to a plan that is a log query whose families it named.
func FuzzScreenLogStages(f *testing.F) {
	for _, q := range []string{
		`{app="api"} | detected_level="error"`, `{service_name="api"} |= "error" | detected_level=~"error|warn"`,
		`{app="api"} | json | status="500"`, `{app="api"} | logfmt | detected_level="x"`, `{app="api"} | service_name="x"`,
		`{app="api"} | detected_level="error" | s="\x01"`, `{app="api"} |pattern"<_> <m>"`, "{a=`b`} | detected_level=`x`",
		`{app="api"} | (detected_level="error" and path="/x")`, `{app="api"} | drop a, b | keep c`,
		`{app="api"} | logfmt --strict | a="b"`, `{app="api"} | keep a="x", b`, `{app="api"} | line_format "{{.a}} {{ .b }}"`,
		`{app="api"} | json | label_format x="{{.a}}", y=z`, `{a="b"} | drop detected_level`, `{a="b"} | json m="a.b" | m>1`,
	} {
		f.Add(q)
	}
	const known = LogLevelFilter | LogRegexpPattern | LogUnspacedParser | LogJSON | LogLogfmt | LogDropKeep | LogLabelFormat | LogLineFormat
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 1024 {
			return
		}
		got := ScreenLogStages(q)
		if got&^known != 0 {
			t.Fatalf("ScreenLogStages(%q) = %b", q, got)
		}
		if n := testing.AllocsPerRun(1, func() { _ = ScreenLogStages(q) }); n != 0 {
			t.Fatalf("ScreenLogStages(%q): %v allocations", q, n)
		}
		// A query Loki rejects is rejected by the request's validation before any translation.
		if got == 0 || logql.ParseAndValidate(q) != nil {
			return
		}
		p := logScreenPlan(t, q)
		if p == nil {
			return
		}
		// A plan that holds a family the screen did not name, or a stage no family holds, is a query the screen admits in
		// error: the typed gate still turns it away, but the screen must not. A label filter the parser keeps untyped
		// (text it reads as no comparison, which the string translator copies) costs a plan the gate turns away: the
		// corpus test counts those.
		if untypedLabelFilter(p) {
			return
		}
		if planned, ok := PlanLogStages(p); !ok || planned&^got != 0 {
			t.Fatalf("%q: the screen names %b, the plan holds %b (%v)", q, got, planned, ok)
		}
	})
}

// untypedLabelFilter reports a log query with a label filter the parser keeps as text.
func untypedLabelFilter(p *queryplan.Plan) bool {
	sel, ok := p.Root.(*queryplan.LogSelect)
	if !ok {
		return false
	}
	for _, st := range sel.Stages {
		if lf, ok := st.(*queryplan.LabelFilter); ok && lf.Expr == nil {
			return true
		}
	}
	return false
}

// ScreenLogStagesIn answers 0 at a family outside the set it is given: a regexp or pattern parser with the blank only
// after an unspaced one when the set holds LogUnspacedParser.
//
// conformance: architecture-syntax-package-layering
func TestScreenLogStagesIn(t *testing.T) {
	const (
		level    = `{app="a"} | detected_level="error"`
		spaced   = `{app="a"} | pattern "<_> <m>"`
		unspaced = `{app="a"} |pattern"<_> <m>"`
		both     = `{app="a"} |pattern"<_> <m>" | pattern "<n> <_>"`
		reversed = `{app="a"} | pattern "<n> <_>" |pattern"<_> <m>"`
	)
	for _, tc := range []struct {
		query   string
		allowed LogStages
		want    LogStages
	}{
		{level, LogLevelFilter, LogLevelFilter}, {level, LogUnspacedParser, 0},
		{spaced, LogLevelFilter | LogUnspacedParser, 0}, {spaced, LogRegexpPattern, LogRegexpPattern},
		{unspaced, LogUnspacedParser, LogRegexpPattern | LogUnspacedParser}, {unspaced, LogLevelFilter, 0},
		{both, LogUnspacedParser, LogRegexpPattern | LogUnspacedParser}, {reversed, LogUnspacedParser, 0},
		{reversed, LogRegexpPattern | LogUnspacedParser, LogRegexpPattern | LogUnspacedParser}, {level, 0, 0},
		{`{a="b"} | json | x="y"`, LogLevelFilter | LogUnspacedParser, 0}, {`{a="b"} | json | x="y"`, LogJSON, LogJSON},
		{`{a="b"} | json | logfmt`, LogJSON, 0}, {`{a="b"} | keep detected_level`, LogLevelFilter, 0},
		{`{a="b"} | keep detected_level`, LogDropKeep, LogDropKeep}, {`{a="b"} | line_format "{{.a}}"`, LogLabelFormat, 0},
		{`{a="b"} | logfmt --strict`, LogJSON, 0}, {`{a="b"} | logfmt --strict`, LogLogfmt, LogLogfmt},
	} {
		if got := ScreenLogStagesIn(tc.query, tc.allowed); got != tc.want {
			t.Errorf("ScreenLogStagesIn(%q, %b) = %b, want %b", tc.query, tc.allowed, got, tc.want)
		}
	}
}
