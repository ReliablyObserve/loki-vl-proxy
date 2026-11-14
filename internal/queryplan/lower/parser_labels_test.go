package lower

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus/plancorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The labels a metric query reads out of its parsers (parser_labels.go): the string translator takes them from the
// text of the query, the plan from typed data. Each shape below is lowered to the translator's own text under
// every profile (the whole space is TestParserLabelsGridShadow's).
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersParserLabels(t *testing.T) {
	for _, q := range []string{
		// a grouping or unwrapped label under its sanitized name
		`sum by (duration_ms) (count_over_time({app="a"} | json [5m]))`,
		`sum by (duration_ms) (rate({app="a"} | json [5m]))`,
		`count by (duration_ms) (rate({app="a"} | logfmt [5m]))`,
		`avg by (a_b, c_d) (bytes_over_time({app="a"} | json [5m]))`,
		`sum(sum_over_time({app="a"} | json | unwrap duration_ms [5m]))`,
		`sum by (user_id) (sum_over_time({app="a"} | json | unwrap bytes(size_bytes) [5m]))`,
		`sum_over_time({app="a"} | json | unwrap duration_ms [5m])`,
		`quantile_over_time(0.95, {app="a"} | logfmt | unwrap latency_ms [5m]) by (a_b)`,
		`last_over_time({app="a"} | logfmt | unwrap latency_ms [5m]) by (level)`,
		`sum(rate({app="a"} | json | unwrap duration_ms [5m]))`,
		`sum by (a_b) (rate({app="a"} | json | unwrap duration_ms [5m]))`,
		// a name the query defines itself keeps its value
		`sum by (a_b) (count_over_time({app="a"} | json a_b="x.y" [5m]))`,
		`sum by (a_b) (count_over_time({app="a"} | pattern "<a_b>" [5m]))`,
		`sum by (a_b) (count_over_time({app="a"} | json | label_format a_b=c [5m]))`,
		// the collision rename of a parsed label
		`sum by (level_extracted) (count_over_time({app="a"} | json [5m]))`,
		`sum_over_time({app="a"} | logfmt | unwrap x_extracted [5m])`,
		// the series of a without fold over a parser
		`sum without (pod) (count_over_time({app="a"} | logfmt [5m]))`,
		`count without (pod) (count_over_time({app="a"} | logfmt a, b [5m]))`,
		`avg without (pod) (rate({app="a"} | json a="x.y", b [5m]))`,
		`max without (pod) (rate({app="a"} | regexp "(?P<a>x)(?P<b>y)" [5m]))`,
		`count without (pod) (count_over_time({app="a"} | pattern "<a> <_> <b>" [5m]))`,
		`count without (pod) (count_over_time({app="a"} | json level [5m]))`,
		`count without (pod) (count_over_time({app="a"} | logfmt a_b [5m]))`,
		`count without (pod) (max_over_time({app="a"} | logfmt a, n | unwrap n [5m]))`,
		`sum without (pod) (rate({app="a"} | logfmt a, n | unwrap n [5m]))`,
		`avg without (pod) (sum_over_time({app="a"} | logfmt | unwrap n [5m]))`,
		`count by (zone) (max without (pod) (count_over_time({app="a"} | logfmt a [5m])))`,
		// the series behind a trailing keep
		`rate({app="a"} | keep a [5m])`,
		`rate({app="a"} | json | keep a, b_c [5m])`,
		`count(rate({app="a"} | keep level, a [5m]))`,
		`min by (x) (count_over_time({app="a"} | logfmt | keep a [5m]))`,
		`sum(rate({app="a"} | keep service_name [5m]))`,
		`rate({app="a"} | keep a="x" [5m])`,
		`rate({app="a"} | json | unwrap n | keep a [5m])`,
		`count(absent_over_time({app="a"} | keep a | unwrap a [5m]))`,
		`sum by (a_b) (rate({app="a"} | json | keep a_b [5m]))`,
	} {
		for _, prof := range rangeProfiles() {
			lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof)
			switch {
			case !ok:
				t.Errorf("%s: %s: the string translator rejects it", prof.name, q)
			case delegated:
				t.Errorf("%s: %s: not lowered", prof.name, q)
			case !same:
				t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	}
}

// What the lowering still leaves to the string translator in this family, named: a keep that leaves no label (an
// explicit `by ()`), a collision rename that reuses scratch fields a label filter computed, an existence check
// the translator moves out of text the lowering does not hold, and the unwrap nest.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesParserLabels(t *testing.T) {
	for q, construct := range map[string]string{
		`rate({app="a"} | keep __error__ [5m])`:                                                   "RangeAgg keep identity",
		`count(rate({app="a"} | keep __error__, __error_details__ [5m]))`:                         "VectorAgg keep identity",
		`min(count_over_time({app="a"} | logfmt | keep __error__ [5m]))`:                          "VectorAgg keep identity",
		`sum by (level_extracted) (count_over_time({app="a"} | json | level_extracted="x" [5m]))`: "VectorAgg parser extracted label",
		`sum_over_time({app="a"} | logfmt | a_extracted="x" | unwrap a_extracted [5m])`:           "RangeAgg parser extracted label",
		`sum by (a_b) (count_over_time({app="a"} | json | a_b != "" [5m]))`:                       "VectorAgg parser label cost",
		`sum by (a_b) (rate({app="a"} | json | a_b != "" [5m]))`:                                  "VectorAgg parser label filter",
		`sum by (a_b) (count_over_time({app="a"} | json | status >= 500 [5m]))`:                   "VectorAgg parser label cost",
		`sum by (service_version) (count_over_time({app="a"} | json | drop __error__ [5m]))`:      "VectorAgg parser label cost",
		`sum_over_time({app="a"} | json | status >= 500 | unwrap duration_ms [5m])`:               "RangeAgg parser label cost",
		`max(sum without (pod) (max_over_time({app="a"} | logfmt amount | unwrap amount [5m])))`:  "VectorAgg nested unwrap",
		`sum by (a_extracted) (count_over_time({app="a"} | json |= "| unpack_json" [5m]))`:        "VectorAgg parser extracted label",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := Plan(p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
}

// A resolved label that the printed log query checks for existence (`| filter label:!""`, which only a label filter
// after the parser prints, and that the lowering delegates by cost first) is moved by the translator behind the key
// pipes: the lowering delegates the text it would have to edit.
func TestKeyPipesDelegatesAnExistenceCheck(t *testing.T) {
	m, err := metricLogQuery(selectOf(t, `{a="b"} | json`), Profile{}, "VectorAgg")
	if err != nil {
		t.Fatal(err)
	}
	m.pipes += existsHead + "a_b" + existsTail
	_, err = m.keyPipes([]string{"a_b"}, nil, "", "VectorAgg", false)
	var nl *NotLoweredError
	if !errors.As(err, &nl) || nl.Construct != "VectorAgg parser label filter" {
		t.Fatalf("got %v, want not lowered: VectorAgg parser label filter", err)
	}
}

// parsedSeriesLabels follows the translator's reading of the stages: the names an extraction list, a regexp's named
// groups and a pattern's captures give, the stages that end the list (an unwrap) or leave it unknown.
func TestParsedSeriesLabels(t *testing.T) {
	for _, tc := range []struct {
		q      string
		unwrap string
		want   string // "-" for unknown
	}{
		{`{app="a"} | json a, b`, "", "a,b"},
		{`{app="a"} | json a="x.y", b`, "", "a,b"},
		{`{app="a"} | json a="x[0]"`, "", "-"},
		{`{app="a"} | logfmt a="b"`, "", "-"},
		{`{app="a"} | logfmt a, b`, "", "a,b"},
		{`{app="a"} | logfmt`, "", "-"},
		{`{app="a"} | json`, "", "-"},
		{`{app="a"} | logfmt --strict a`, "", "-"},
		{`{app="a"} | json a | json b`, "", "a,b"},
		{`{app="a"} | json a, a`, "", "a"},
		{`{app="a"} | json a | unwrap a`, "a", ""},
		{`{app="a"} | json a, n | unwrap n | json zz`, "n", "a"},
		{`{app="a"} | regexp "(?P<a>x)(?P<b>y)"`, "", "a,b"},
		{`{app="a"} | regexp "(x)"`, "", ""},
		{`{app="a"} | regexp "(?P<a>x"`, "", "-"},
		{`{app="a"} | pattern "<a> <_> <b>"`, "", "a,b"},
		{`{app="a"} | json level`, "", "-"},
		{`{app="a"} | json detected_level`, "", "-"},
		{`{app="a"} | json service_name`, "", "-"},
		{`{app="a"} | json app`, "", "-"},
		{`{app="a"} | json env`, "", "env"},
		{`{app="a"} | json a | label_format b=a`, "", "-"},
		{`{app="a"} | json a | drop a`, "", "-"},
		{`{app="a"} | json a | keep a`, "", "-"},
		{`{app="a"} | unpack`, "", "-"},
		{`{app="a"} | json a | line_format "x" | decolorize | a="1"`, "", "a"},
		{`{app="a"} | json a != "x"`, "", "-"},
		{`{app="a"} | json a |= "x" != "y"`, "", "a"},
		{`{app="a"} != "y" | json a`, "", "a"},
		{`{app="a"} | json a !> "<_>"`, "", "-"},
		{`{app="a"} | json a |> "<_>"`, "", "a"},
		{`{app="a"} | json a | pattern "<b>" !~ "x"`, "", "-"},
	} {
		got := "-"
		if labels, ok := parsedSeriesLabels(selectOf(t, tc.q), tc.unwrap); ok {
			got = ""
			for i, l := range labels {
				if i > 0 {
					got += ","
				}
				got += l
			}
		}
		if got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.q, got, tc.want)
		}
	}
}

// The key pipes of a stats pipe: which labels are resolved, which the query defines, what is delegated.
func TestKeyPipes(t *testing.T) {
	logOf := func(q string) metricLog {
		m, err := metricLogQuery(selectOf(t, q), Profile{}, "VectorAgg")
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return m
	}
	for _, tc := range []struct {
		q    string
		by   []string
		arg  string
		want []string
	}{
		{`{a="b"} | json`, []string{"a_b"}, "", []string{`| format if ("a.b":*) "<a.b>" as a_b keep_original_fields`, `| format if ("a-b":*) "<a-b>" as a_b keep_original_fields`}},
		{`{a="b"} | json`, []string{"a_b", "a_b"}, "", []string{`| format if ("a.b":*) "<a.b>" as a_b keep_original_fields`, `| format if ("a-b":*) "<a-b>" as a_b keep_original_fields`}},
		{`{a="b"} | json`, []string{"ab", "_ab", "service_name", "detected_level", "k8s.pod.name"}, "", nil},
		{`{a="b"}`, []string{"a_b"}, "", nil},
		{`{a="b"} | regexp "(?P<a_b>x)"`, []string{"a_b"}, "", nil},
		{`{a="b"} | json a_b="x.y"`, []string{"a_b"}, "", nil},
		{`{a="b"} | json | label_format a_b=c`, []string{"a_b"}, "", nil},
		{`{a="b"} | pattern "<a_b>"`, []string{"a_b"}, "", nil},
		{`{a="b"} | json`, []string{"x"}, "n_m", []string{`| format if ("n.m":*) "<n.m>" as n_m keep_original_fields`, `| format if ("n-m":*) "<n-m>" as n_m keep_original_fields`, `| filter n_m:*`}},
		{`{a="b"} | json n="x.y" | line_format "x"`, []string{"x"}, "n", []string{`| filter n:*`}},
	} {
		keys, err := logOf(tc.q).keyPipes(tc.by, nil, tc.arg, "VectorAgg", false)
		if err != nil {
			t.Fatalf("%s: %v", tc.q, err)
		}
		want := ""
		for _, p := range tc.want {
			want += " " + p
		}
		if keys.text != want {
			t.Errorf("%s by %v arg %q:\n got %q\nwant %q", tc.q, tc.by, tc.arg, keys.text, want)
		}
	}
}

// parserLabelAllocGrid is the metric queries over a parser that the lowering writes from the labels the parsers
// extract: groupings under a sanitized name, a without fold, a trailing keep and an unwrapped label.
func parserLabelAllocGrid(t testing.TB) []string {
	t.Helper()
	qs, err := plancorpus.Queries("../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{` | json`, ` | logfmt`, ` | json a, b`, ` | logfmt a_b`, ` | json | status >= 500`, ` | regexp "(?P<a>x)"`, ` | keep a`, ` | json | keep a, b_c`,
		` | json a, b | status >= 500`, ` | logfmt a | drop x`, ` | json | keep a | b="1"`, ` | logfmt a, b | a="x" | b != ""`, ` | json | drop __error__`,
		` | json | a_b != ""`, ` | logfmt | a_b != "" | c_d != ""`, ` | json | a_b != "" | c="d"`, ` | json | c_d != "" | a_b != ""`} {
		for _, g := range []string{``, `by (app)`, `by (a_b)`, `by (a_b, c_d)`, `by (service_name, a_b)`, `without (pod)`} {
			for _, op := range []string{"sum", "count", "avg"} {
				for _, f := range []string{"count_over_time", "bytes_over_time", "rate", "bytes_rate"} {
					if g == "" {
						qs = append(qs, fmt.Sprintf(`%s(%s({app="a"}%s [5m]))`, op, f, p))
					} else {
						qs = append(qs, fmt.Sprintf(`%s %s (%s({app="a"}%s [5m]))`, op, g, f, p))
					}
				}
			}
		}
	}
	for _, p := range []string{` | json`, ` | logfmt`, ` | json a_b`, ` | logfmt a, n`, ` | json | status >= 500`} {
		for _, u := range []string{`n`, `duration_ms`, `bytes(size_bytes)`} {
			for _, f := range []string{"sum_over_time", "max_over_time", "last_over_time", "quantile_over_time", "rate"} {
				body := fmt.Sprintf(`{app="a"}%s | unwrap %s [5m]`, p, u)
				if f == "quantile_over_time" {
					body = "0.9, " + body
				}
				qs = append(qs, fmt.Sprintf(`%s(%s)`, f, body), fmt.Sprintf(`sum by (a_b) (%s(%s))`, f, body), fmt.Sprintf(`count without (pod) (%s(%s))`, f, body))
			}
		}
	}
	return qs
}

// readsParserLabels reports whether the plan is a metric query that this step newly lowers: the innermost level a
// without clause over a parser (the fold's series are the labels the parsers name), a rate, or count, min, max or
// avg, behind a trailing keep, or a grouping or unwrapped label that may be a sanitized key of a json or logfmt
// parser. The log query holds the stages whose own lowering costs no more than the translator's (a label filter,
// drop or format after a parser costs more, before this step too).
func readsParserLabels(p *queryplan.Plan) bool {
	var inner *queryplan.VectorAgg
	var groupings []*queryplan.Grouping
	n := p.Root
	for {
		va, ok := n.(*queryplan.VectorAgg)
		if !ok {
			break
		}
		inner = va
		groupings = append(groupings, va.By)
		n = va.In
	}
	ra, ok := n.(*queryplan.RangeAgg)
	if !ok {
		return false
	}
	groupings = append(groupings, ra.By)
	unpack, parser := false, false
	for _, st := range ra.In.Stages {
		if ps, ok := st.(*queryplan.Parse); ok {
			parser = true
			unpack = unpack || ps.Type == logql.ParserJSON || ps.Type == logql.ParserLogfmt
		}
	}
	if inner != nil && inner.By != nil && inner.By.Without && parser {
		return true
	}
	rate := ra.Op == logql.RangeRate || ra.Op == logql.RangeBytesRate
	if _, kept := trailingKeepLabels(ra.In, false); kept && (rate || inner != nil && inner.Op != logql.VectorSum) {
		return true
	}
	for _, g := range groupings {
		if g == nil || g.Without {
			continue
		}
		for _, l := range g.Labels {
			if unpack && translate.ParsedLabelCandidate(l) {
				return true
			}
		}
	}
	if u := unwrapStage(ra.In); u != nil && unpack && translate.ParsedLabelCandidate(u.Label) {
		return true
	}
	return false
}

// The queries of parserLabelAllocGrid that the plan lowers, and are in the shapes of this step.
func parserLabelQueries(t testing.TB) []string {
	t.Helper()
	var out []string
	for _, q := range parserLabelAllocGrid(t) {
		p := rangeAggPlan(q)
		if p == nil || !readsParserLabels(p) {
			continue
		}
		if _, err := PlanQuery(q, p, Profile{}); err != nil {
			continue
		}
		out = append(out, q)
	}
	return out
}

// A metric query over a parser that the lowering writes allocates no more than the string translator does for the
// same text, with and without the tenant's stream labels (Parse, Build, Resolve and Plan against the translation):
// the shapes where it would are left to the translator, named.
//
// minAllocsPerRun is the lowest of three testing.AllocsPerRun(5, f) samples. AllocsPerRun counts
// every allocation of the process, so another test's goroutine still running during a full package
// run can only add to a sample; the lowest sample is f's own count.
func minAllocsPerRun(f func()) float64 {
	low := testing.AllocsPerRun(5, f)
	for i := 0; i < 2; i++ {
		low = min(low, testing.AllocsPerRun(5, f))
	}
	return low
}

// conformance: architecture-syntax-package-layering
func TestParserLabelsLoweringAllocatesNoMoreThanTheTranslator(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	checked := 0
	for _, label := range []func(string) string{nil, benchStreamLabels} {
		for _, q := range parserLabelAllocGrid(t) {
			p := rangeAggPlan(q)
			if p == nil || !readsParserLabels(p) {
				continue
			}
			if _, err := PlanQuery(q, p, Profile{Label: label}); err != nil {
				continue
			}
			tr := minAllocsPerRun(func() { _, _ = translator.TranslateLogQLWithLabels(q, label) })
			lo := minAllocsPerRun(func() {
				expr, _ := logql.Parse(logql.CanonicalizeLogRanges(q))
				pl, _ := queryplan.Build(expr, queryplan.Inputs{})
				queryplan.Resolve(pl)
				_, _ = PlanQuery(q, pl, Profile{Label: label})
			})
			checked++
			if lo > tr {
				t.Errorf("label=%v: %s: lowered %.0f allocs, translator %.0f", label != nil, q, lo, tr)
			}
		}
	}
	t.Logf("%d lowered shapes checked (both label modes)", checked)
	if checked < 1000 {
		t.Fatalf("only %d lowered queries checked", checked)
	}
}

// FuzzParserLabelsLowering holds the lowering of the parser-label constructs to the string translator on any
// selector values, parser extraction lists and captures, label names (with underscores, dots, derived names,
// `_extracted` ones), keeps, unwrapped labels, groupings and functions: both write the same query, or the lowering
// leaves the query to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzParserLabelsLowering(f *testing.F) {
	f.Add("a", "x_y", "n", uint8(0), uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "level_extracted", "dur_ms", uint8(7), uint8(3), uint8(5), uint8(9), uint8(77))
	f.Add("k8s.pod", "a-b", "é", uint8(200), uint8(9), uint8(11), uint8(70), uint8(255))
	f.Fuzz(func(t *testing.T, sel, label, name string, fn, parser, shape, agg, tail uint8) {
		if len(sel)+len(label)+len(name) > 120 {
			return
		}
		strip := strings.NewReplacer("bool", "", "without", "")
		for _, s := range []string{sel, label, name} {
			// A control byte in a selector value under a label filter on an `_extracted` label is written unlike the
			// translator by the range aggregation lowering before this step (count_over_time({x="\x02"} | json |
			// A_extracted="x" [5m])); it is not this step's.
			if translatorReadsAsSyntax(strip.Replace(s)) || strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
				return
			}
		}
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			// A capture or label needs a letter: `<_>` is the pattern's unnamed capture, which the translator
			// writes as an extract that VictoriaLogs rejects.
			if !strings.ContainsAny(b.String(), "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
				return "x"
			}
			return b.String()
		}
		a, l, n := ident(sel), ident(label), ident(name)
		selector := `{` + a + `="` + escapeValue(sel) + `"}`
		parsers := []string{` | json`, ` | logfmt`, ` | json ` + l, ` | logfmt ` + l + `, ` + n, ` | json ` + l + `="x.y"`, ` | regexp "(?P<` + l + `>x)"`, ` | pattern "<` + l + `>"`,
			` | json | logfmt`, ` | json ` + n + ` | json ` + l}
		stages := []string{``, ` | ` + l + ` != ""`, ` | ` + l + `="x"`, ` | keep ` + l, ` | keep ` + n + `, ` + l, ` | keep ` + l + `="x"`, ` | drop ` + n, ` | __error__=""`,
			` | label_format ` + l + `=` + n, ` | line_format "{{.` + l + `}}"`}
		body := selector + parsers[parser%uint8(len(parsers))] + stages[tail%uint8(len(stages))]
		clauses := []string{``, `by (` + l + `)`, `by (` + n + `, ` + l + `)`, `by (` + a + `)`, `without (` + n + `)`, `without (` + l + `)`, `by ()`, `by (service_name, ` + l + `)`}
		clause := clauses[shape%uint8(len(clauses))]
		ops := []string{"sum", "count", "min", "max", "avg"}
		op := ops[agg%uint8(len(ops))]
		var q string
		switch fn % 3 {
		case 0:
			function := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}[fn/3%5]
			q = function + `(` + body + `[5m])`
		default:
			u := []string{`unwrap ` + l, `unwrap bytes(` + l + `)`, `unwrap duration(` + n + `)`, `unwrap ` + n}[fn>>2%4]
			function := []string{"sum_over_time", "avg_over_time", "last_over_time", "quantile_over_time", "rate", "max_over_time"}[fn>>4%6]
			inner := body + ` | ` + u + ` [5m]`
			if function == "quantile_over_time" {
				inner = `0.9, ` + inner
			}
			q = function + `(` + inner + `)`
		}
		switch {
		case clause == "":
			q = op + `(` + q + `)`
		case agg>>3%2 == 0:
			q = op + ` ` + clause + ` (` + q + `)`
		default:
			q = op + `(` + q + `) ` + clause
		}
		if agg>>5 == 7 {
			q = `sum(` + q + `)`
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
