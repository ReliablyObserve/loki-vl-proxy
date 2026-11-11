package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// vectorAggText is the text of the lowering of q under prof, with the detected_level chain and the service_name
// pipes shown as <DL> and <SN>, and the fold the proxy applies shown as a suffix.
func vectorAggText(t *testing.T, q string, prof Profile) string {
	t.Helper()
	p := rangeAggPlan(q)
	if p == nil {
		t.Fatalf("%s: does not parse", q)
	}
	op, err := Plan(p, prof)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	text := physical.Encode(op)
	text = strings.ReplaceAll(text, translate.RateDetectedLevelChain(), "<DL>")
	return strings.ReplaceAll(text, translate.ServiceNameGroupingPipes(prof.Caps), "<SN>")
}

// What the lowering writes for the aggregations over the range functions it lowers: the grouping of the stats, the
// series identity of count, min, max and avg, the rate's division, the derived names (service_name, detected_level)
// and the fold of a without clause.
//
// conformance: architecture-syntax-package-layering
func TestPlanVectorAggregationText(t *testing.T) {
	v53 := Profile{Caps: logsql.CapabilitiesFor("v1.53.0")}
	v49 := Profile{Caps: logsql.CapabilitiesFor("v1.49.0")}
	dots := Profile{Caps: v53.Caps, Label: func(s string) string { return strings.ReplaceAll(s, ".", "_") }}
	for _, c := range []struct {
		q    string
		prof Profile
		want string
	}{
		{`sum(count_over_time({app="a"}[5m]))`, v53, `app:="a" | stats count()`},
		{`sum by (app) (count_over_time({app="a"}[5m]))`, v53, `app:="a" | stats by (app) count()`},
		{`sum(bytes_over_time({app="a"}[5m])) by (app, env)`, v53, `app:="a" | stats by (app, env) sum_len(_msg)`},
		{`sum by () (absent_over_time({app="a"}[5m]))`, v53, `app:="a" | stats count()`},
		{`sum by (k8s.pod.name) (count_over_time({app="a"}[5m]))`, dots, `app:="a" | stats by (k8s_pod_name) count()`},
		{`sum by (app, app) (count_over_time({app="a"}[5m]))`, v53, `app:="a" | stats by (app) count()`},
		{`sum by (service_name) (count_over_time({app="a"}[5m]))`, v53, `app:="a" <SN> | stats by (service_name) count()`},
		{`sum by (detected_level) (count_over_time({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (detected_level) count()`},
		{`sum by (detected_level, service_name) (count_over_time({app="a"}[5m]))`, v49, `app:="a" <DL> <SN> | stats by (detected_level, service_name) count()`},
		{`count(count_over_time({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (_stream, level, detected_level) count() as __lvp_inner | stats count()`},
		{`max by (app) (bytes_over_time({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (app, _stream, level, detected_level) sum_len(_msg) as __lvp_inner | stats by (app) max(__lvp_inner)`},
		{`avg by (level) (count_over_time({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (level, _stream, detected_level) count() as __lvp_inner | stats by (level) avg(__lvp_inner)`},
		{`min by (detected_level) (count_over_time({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (detected_level, _stream, level) count() as __lvp_inner | stats by (detected_level) min(__lvp_inner)`},
		{`sum(rate({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (_stream, level, detected_level) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats sum(__lvp_rate)`},
		{`sum by (app) (rate({app="a"}[5m]))`, v53, `app:="a" | stats by (app) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (app) sum(__lvp_rate)`},
		{`sum by (service_name) (bytes_rate({app="a"}[1m]))`, v49, `app:="a" <SN> | stats by (service_name) sum_len(_msg) as __lvp_inner | math __lvp_inner/60 as __lvp_rate | stats by (service_name) sum(__lvp_rate)`},
		{`count by (app) (rate({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (app, _stream, level, detected_level) count() as __lvp_inner | stats by (app) count()`},
		{`avg(rate({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (_stream, level, detected_level) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats avg(__lvp_rate)`},
		{`min by (service_name) (bytes_rate({app="a"}[5m]))`, v53, `app:="a" <DL> <SN> | stats by (service_name, _stream, level, detected_level) sum_len(_msg) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (service_name) min(__lvp_rate)`},
		// without: no grouping for a sum of a count, the series for the rest, and the fold the proxy applies.
		{`sum without (app) (count_over_time({app="a"}[5m]))`, v53, `app:="a" | stats count()__without__:sum:app`},
		{`sum without (a, b) (rate({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (_stream, level, detected_level) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (_stream, level, detected_level) sum(__lvp_rate)__without__:sum:a,b`},
		{`count without (app) (count_over_time({app="a"}[5m]))`, v53, `app:="a" <DL> | stats by (_stream, level, detected_level) count() as __lvp_inner | stats by (_stream, level, detected_level) count()__without__:count:app`},
		{`avg without (k8s.pod.name) (rate({app="a"}[5m]))`, dots, `app:="a" <DL> | stats by (_stream, level, detected_level) count() as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (_stream, level, detected_level) avg(__lvp_rate)__without__:avg:k8s.pod.name`},
	} {
		if got := vectorAggText(t, c.q, c.prof); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.q, got, c.want)
		}
	}
}

// A without clause is a Fold around the query, with the labels as the client wrote them (not renamed) and the
// operator the proxy combines the series with.
//
// conformance: architecture-syntax-package-layering
func TestPlanVectorAggregationFold(t *testing.T) {
	prof := Profile{Label: func(s string) string { return "x_" + s }}
	op, err := Plan(rangeAggPlan(`max without (pod, k8s.node) (count_over_time({app="a"}[5m]))`), prof)
	if err != nil {
		t.Fatal(err)
	}
	fold, ok := op.(physical.Fold)
	if !ok || len(fold.Levels) != 1 {
		t.Fatalf("got %#v", op)
	}
	if l := fold.Levels[0]; l.Op != "max" || !l.Without || strings.Join(l.Labels, ",") != "pod,k8s.node" {
		t.Errorf("level %#v", l)
	}
	if _, ok := fold.In.(physical.VLQuery); !ok {
		t.Errorf("fold wraps %#v", fold.In)
	}
	// by is no fold.
	if op, err = Plan(rangeAggPlan(`max by (pod) (count_over_time({app="a"}[5m]))`), prof); err != nil {
		t.Fatal(err)
	} else if _, ok := op.(physical.VLQuery); !ok {
		t.Errorf("by clause: got %#v", op)
	}
}

// Every case the lowering leaves to the string translator is named, and the names count the shapes by construct.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesVectorAggregations(t *testing.T) {
	for q, construct := range map[string]string{
		`sum(rate({app="a"}[5m]) > 1)`:                                             "VectorAgg operand",
		`sum without (a) (rate({app="a"}[5m])) / 2`:                                "BinOp operand without",
		`sum by (app) (avg_over_time({app="a"} | unwrap n [5m]) by (x))`:           "VectorAgg grouping",
		`sum by (a) (rate({app="a"} | drop a="| json" | keep x [5m]))`:             "VectorAgg quoted clause",
		`count(count_over_time({app="a"} | drop a="| json" [5m]))`:                 "VectorAgg quoted clause",
		`sum without (x) (count_over_time({app="a"} | drop a="without (x)" [5m]))`: "VectorAgg quoted clause",
		`sum(rate({app="a"} | detected_level="" [5m]))`:                            "VectorAgg " + levelPresenceConstruct,
		`sum(rate({app="a"} | line_format "{{.a | upper}}" [5m]))`:                 "VectorAgg LogSelect line format function",
		`sum(rate({app="a"}[0s]))`:                                                 "VectorAgg range",
		`sum without () (rate({app="a"}[5m]))`:                                     "VectorAgg without empty",
		`sum(count_over_time({app="a"} |= "without (x)"[5m]))`:                     "VectorAgg quoted clause",
		`sum by (app) (rate({app="a"} |= "a bool b"[5m]))`:                         "VectorAgg quoted clause",
		`sum by (app) (count_over_time({app="a"} |= "| stats "[5m]))`:              "VectorAgg quoted clause",
		`sum without (x) (rate({x!="  "}[5m]))`:                                    "VectorAgg quoted clause",
		`sum without (x) (rate({app="a"} |= "without(0)\xa2"[5m]))`:                "VectorAgg quoted clause",
		`sum(rate({app="a"} |~ "a|||b"[5m]))`:                                      "VectorAgg quoted clause",
		`rate({app="a"} |~ "|||"[5m])`:                                             "RangeAgg quoted clause",
		`rate({app="a"} |= "| jsoné"[5m])`:                                         "RangeAgg quoted clause",
		`bytes_rate({app="a"} != "| json\xa3\xae0wl"[5m])`:                         "RangeAgg quoted clause",
		`sum(count_over_time({app="a"} |= "| logfmtü"[5m]))`:                       "VectorAgg quoted clause",
		`count_over_time({app="a"} |= "without (x)"[5m])`:                          "RangeAgg quoted clause",
		`rate({app="a"} |= "a bool b"[5m])`:                                        "RangeAgg quoted clause",
		`count_over_time({app="a"} | m="a bool b" [5m] offset 1h)`:                 "RangeAgg quoted clause",
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
	// A kept line, a grouping label the label function removes.
	var nl *NotLoweredError
	if _, err := Plan(rangeAggPlan(`sum(rate({app="a"}[5m]))`), Profile{KeepLine: true}); !errors.As(err, &nl) || nl.Construct != "VectorAgg keepLine" {
		t.Errorf("keepLine: got %v", err)
	}
	drop := func(s string) string {
		if s == "gone" {
			return ""
		}
		return s
	}
	if _, err := Plan(rangeAggPlan(`sum by (gone) (rate({app="a"}[5m]))`), Profile{Label: drop}); !errors.As(err, &nl) || nl.Construct != "VectorAgg grouping label" {
		t.Errorf("grouping label: got %v", err)
	}
}

// A query with blanks between the operand's parenthesis and the range function (`sum( rate(...) )`), or between the
// function and its parenthesis, is a phrase to the string translator: its regexps do not skip them. The proxy's
// request rewrite leaves the blanks, so the proxy answers a phrase search for it today where Loki answers the
// metric. The plan has no text and lowers the aggregation as Loki reads it (Plan); the text screen turns the query
// away (PlanQuery, ScreenText), so the lowering equals the translator wherever it answers. The difference is
// recorded for the day metric queries flip.
//
// conformance: architecture-syntax-package-layering
func TestPlanVectorAggregationBlankPadding(t *testing.T) {
	canonical := vectorAggText(t, `sum(rate({app="a"}[5m]))`, Profile{})
	for _, q := range []string{
		`sum( rate({app="a"}[5m]))`, `sum(rate ({app="a"}[5m]))`, `sum( rate( {app="a"} [5m] ) )`, "sum(\n\trate\t(\t{app=\"a\"}[5m]))",
		`sum by (app) ( rate({app="a"}[5m]))`, `sum(count_over_time ({app="a"}[5m])) by (app)`, `count without (pod) (  bytes_over_time ({app="a"}[5m]))`,
	} {
		legacy, err := translator.TranslateLogQLWithCapabilities(q, nil, nil, logsql.Capabilities{})
		if err != nil || !strings.HasPrefix(legacy, `"`) {
			t.Errorf("%q: the translator writes %q (%v): the difference this test records is gone", q, legacy, err)
		}
		p := rangeAggPlan(q)
		if _, err := Plan(p, Profile{}); err != nil {
			t.Errorf("%q: Plan: %v", q, err)
		}
		var nl *NotLoweredError
		if _, err := PlanQuery(q, p, Profile{}); !errors.As(err, &nl) || nl.Construct != "VectorAgg operand spelling" {
			t.Errorf("%q: PlanQuery = %v, want not lowered: VectorAgg operand spelling", q, err)
		}
		if ScreenText(q, p) == nil {
			t.Errorf("%q: ScreenText lets it through", q)
		}
	}
	if got := vectorAggText(t, `sum( rate({app="a"}[5m]))`, Profile{}); got != canonical {
		t.Errorf("blanks change the lowering:\n got %s\nwant %s", got, canonical)
	}
	// Blanks elsewhere are read alike by both: they stay lowered.
	for _, q := range []string{
		`sum by ( app ) (rate({ app = "a" } [ 5m ] ))`, "  sum(rate({app=\"a\"}[5m]))\n", `sum  by  (app)  (rate({app="a"}[5m]) )`,
		`sum(rate({app="a"} |= "( rate (" [5m]))`, `sum(rate({app="a"} |= ` + "`" + `( rate (` + "`" + `[5m]))`, `sum(rate( {app="a"}[5m]))`,
		`sum(rate({app="a"}[5m]) ) by (app, env)`,
	} {
		p := rangeAggPlan(q)
		if err := ScreenText(q, p); err != nil {
			t.Errorf("%q: turned away: %v", q, err)
		}
	}
	// A metric wrapped whole in parentheses is a phrase to the translator (both constructs).
	for q, construct := range map[string]string{
		`(sum(rate({app="a"}[5m])))`: "VectorAgg parenthesized", "( sum by (app) (rate({app=\"a\"}[5m])) )": "VectorAgg parenthesized",
		`sum((rate({app="a"}[5m])))`: "VectorAgg operand spelling", `((rate({app="a"}[5m])))`: "RangeAgg parenthesized", "(count_over_time({app=\"a\"}[5m])\n)": "RangeAgg parenthesized",
	} {
		var nl *NotLoweredError
		if _, err := PlanQuery(q, rangeAggPlan(q), Profile{}); !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%q: PlanQuery = %v, want not lowered: %s", q, err, construct)
		}
	}
	// A binary of two parenthesized operands is not wrapped whole; the binary's screen reads its operands (each a
	// phrase to the translator).
	for _, q := range []string{`(sum(rate({app="a"}[5m]))) / (sum(rate({app="b"}[5m])))`} {
		var nl *NotLoweredError
		if err := ScreenText(q, rangeAggPlan(q)); wrappedWhole(q) || !errors.As(err, &nl) || nl.Construct != "BinOp operand spelling" {
			t.Errorf("%q: ScreenText = %v, want not lowered: BinOp operand spelling", q, err)
		}
	}
	// A range aggregation alone reads blanks inside its parentheses alike (a blank before its parenthesis is a
	// phrase to the translator: TestPlanRangeCallBlankBeforeParenthesis).
	for _, q := range []string{`rate( {app="a"} [5m] )`, `count_over_time( {app="a"}[5m])`} {
		if err := ScreenText(q, rangeAggPlan(q)); err != nil {
			t.Errorf("%q: turned away: %v", q, err)
		}
	}
}

// FuzzVectorAggLowering holds the lowering of vector aggregations to the string translator on any selector values,
// line filters, label filters, groupings, functions, ranges and offsets: both write the same query, or the lowering
// leaves the query to the translator. The text a metric query's regexps read as syntax inside a quoted value
// (" bool ", "without") is kept in: the lowering delegates it.
//
// conformance: architecture-syntax-package-layering
func FuzzVectorAggLowering(f *testing.F) {
	f.Add("a", "x", "level", "error", uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "é\n", "status", ".*", uint8(7), uint8(3), uint8(5), uint8(9))
	f.Add("k8s.pod", "without (x)", "a-b", "10.0.0.0/8", uint8(200), uint8(9), uint8(11), uint8(70))
	f.Fuzz(func(t *testing.T, sel, line, name, val string, fn, shape, rng, agg uint8) {
		strip := strings.NewReplacer("bool", "", "without", "")
		for _, s := range []string{sel, line, name, val} {
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
		matchers := []string{`="`, `!="`, `=~"`, `!~"`}
		selector := `{` + ident(sel) + matchers[shape%4] + escapeValue(sel) + `"}`
		stage := []string{
			``, ` |= ` + strconv.Quote(line), ` != ` + strconv.Quote(line), ` |~ ` + strconv.Quote(line),
			` | ` + ident(name) + `=` + strconv.Quote(val), ` | ` + ident(name) + `>=` + strconv.Itoa(len(val)),
		}[shape>>4%6]
		function := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}[fn%5]
		span := []string{`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[5m] offset 1h`, `[0s]`}[rng%6]
		op := []string{"sum", "count", "min", "max", "avg", "stddev", "stdvar"}[agg%7]
		labels := ident(name)
		grouping := []string{``, ` by (` + labels + `)`, ` by (` + labels + `, service_name)`, ` without (` + labels + `)`, ` by (detected_level)`, ` by ()`, ` without (` + labels + `, ` + ident(sel) + `)`}[agg>>3%7]
		// blanks the translator reads unlike the plan sit between the operand's parenthesis and the function, and
		// between the function and its parenthesis; the screen of PlanQuery turns those away.
		pad := []string{"", " ", "\t", "\n"}
		inner := function + pad[rng>>4%4] + `(` + selector + stage + span + `)`
		var q string
		if agg&0x80 != 0 {
			q = op + `(` + pad[rng>>6%4] + inner + `)` + grouping
		} else {
			q = op + grouping + ` (` + pad[rng>>6%4] + inner + `)`
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
