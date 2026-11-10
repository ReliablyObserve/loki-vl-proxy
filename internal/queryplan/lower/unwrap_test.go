package lower

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// unwrapText is the text of the lowered query with the unwrap gate of field and conv replaced by a short mark, so a
// table can read it.
func unwrapText(t *testing.T, q string, prof Profile) string {
	t.Helper()
	p := rangeAggPlan(q)
	if p == nil {
		t.Fatalf("%s: does not parse", q)
	}
	op, err := PlanQuery(q, p, prof)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	text := physical.Encode(op)
	for _, conv := range []string{"", "duration", "bytes"} {
		for _, field := range []string{"n", "max", "a_b"} {
			text = strings.ReplaceAll(text, translate.UnwrapGateFor(field, conv), " <gate "+field+" "+conv+">")
		}
	}
	return strings.ReplaceAll(text, translate.RateDetectedLevelChain(), "<chain>")
}

// The range functions over an unwrapped label write the log query of the metric row, the unwrap gate and the stats
// pipes the string translator writes: one stats over the converted value (grouped by the labels of the range's
// by clause), the row functions of first and last unpacked per series, the squared deviation of stdvar left to the
// proxy as a binary expression, the sum of a rate per series divided by the range's seconds.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersUnwrapRangeAggregations(t *testing.T) {
	for q, want := range map[string]string{
		`sum_over_time({app="a"} | unwrap n [5m])`:                          `app:="a" <gate n > | stats sum(__lvp_v)`,
		`avg_over_time({app="a"} |= "x" | unwrap bytes(n) [5m])`:            `app:="a" ~"x" <gate n bytes> | stats avg(__lvp_v)`,
		`min_over_time({app="a"} | unwrap duration(n) [5m])`:                `app:="a" <gate n duration> | stats min(__lvp_v)`,
		`max_over_time({app="a"} | unwrap duration_seconds(n) [5m])`:        `app:="a" <gate n duration> | stats max(__lvp_v)`,
		`stddev_over_time({app="a"} | unwrap max [5m])`:                     `app:="a" <gate max > | stats stddev(__lvp_v)`,
		`avg_over_time({app="a"} | unwrap n [5m]) by (pod)`:                 `app:="a" <gate n > | stats by (pod) avg(__lvp_v)`,
		`avg_over_time({app="a"} | unwrap n [5m]) by ()`:                    `app:="a" <gate n > | stats by () avg(__lvp_v)`,
		`quantile_over_time(0.5, {app="a"} | unwrap n [5m])`:                `app:="a" <gate n > | stats quantile(0.5, __lvp_v)`,
		`rate_counter({app="a"} | unwrap n [5m])`:                           `app:="a" <gate n > | stats __rate_counter__(__lvp_v)`,
		`sum_over_time({app="a"} | unwrap n | __error__="" [5m])`:           `app:="a" __error__:="" <gate n > | stats sum(__lvp_v)`,
		`absent_over_time({app="a"} | unwrap n [5m])`:                       `app:="a" | stats count()`,
		`stdvar_over_time({app="a"} | unwrap n [5m])`:                       `__binary__:^:app:="a" <gate n > | stats stddev(__lvp_v)|||2`,
		`stdvar_over_time({app="a"} | unwrap n [5m]) by (pod)`:              `__binary__:^:app:="a" <gate n > | stats by (pod) stddev(__lvp_v)|||2`,
		`last_over_time({app="a"} | unwrap n [5m]) by (pod)`:                `app:="a" <gate n > | stats by (pod) row_max(_time, __lvp_v) as __lvp_inner | unpack_json from __lvp_inner fields (__lvp_v) | math __lvp_v as __lvp_inner | stats by (pod) max(__lvp_inner)`,
		`first_over_time({app="a"} | unwrap n [5m])`:                        `app:="a" <chain> <gate n > | stats by (_stream, level, detected_level) row_min(_time, __lvp_v) as __lvp_inner | unpack_json from __lvp_inner fields (__lvp_v) | math __lvp_v as __lvp_inner | stats by (_stream, level, detected_level) max(__lvp_inner)`,
		`rate({app="a"} | unwrap n [5m])`:                                   `app:="a" <chain> <gate n > | stats by (_stream, level, detected_level) sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (_stream, level, detected_level) sum(__lvp_rate)`,
		`sum(sum_over_time({app="a"} | unwrap n [5m]))`:                     `app:="a" <gate n > | stats by (_stream) sum(__lvp_v) as __lvp_inner | stats sum(__lvp_inner)`,
		`sum by (pod) (sum_over_time({app="a"} | unwrap n [5m]))`:           `app:="a" <gate n > | stats by (pod) sum(__lvp_v)`,
		`count by (pod) (max_over_time({app="a"} | unwrap n [5m]))`:         `app:="a" <chain> <gate n > | stats by (pod, _stream, level, detected_level) max(__lvp_v) as __lvp_inner | stats by (pod) count()`,
		`avg(quantile_over_time(0.9, {app="a"} | unwrap n [5m]))`:           `app:="a" <gate n > | stats by (_stream) quantile(0.9, __lvp_v) as __lvp_inner | stats avg(__lvp_inner)`,
		`avg by (pod) (quantile_over_time(0.9, {app="a"} | unwrap n [5m]))`: `app:="a" <gate n > | stats by (pod) quantile(0.9, __lvp_v)`,
		`sum(stdvar_over_time({app="a"} | unwrap n [5m]))`:                  `app:="a" <gate n > | stats by (_stream) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats sum(__lvp_inner_var)`,
		`sum by (pod) (rate({app="a"} | unwrap n [5m]))`:                    `app:="a" <gate n > | stats by (pod) sum(__lvp_v) as __lvp_inner | math __lvp_inner/300 as __lvp_rate | stats by (pod) sum(__lvp_rate)`,
		`count(absent_over_time({app="a"} | unwrap n [5m]))`:                `app:="a" <chain> | stats by (_stream, level, detected_level) count() as __lvp_inner | stats count()`,
		`sum without (pod) (sum_over_time({app="a"} | unwrap n [5m]))`:      `app:="a" <gate n > | stats by (_stream) sum(__lvp_v) as __lvp_inner | stats by (_stream) sum(__lvp_inner)__without__:sum:pod`,
	} {
		if got := unwrapText(t, q, Profile{}); got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
}

// The unwrap gate and the stats are written after the first pipe of the log query's derivations: a grouping by
// service_name has the pipes that derive it, a stream label the restore.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersUnwrapDerivedGroupings(t *testing.T) {
	q := `avg_over_time({app="a"} | unwrap n [5m]) by (service_name)`
	got := unwrapText(t, q, Profile{})
	if !strings.HasPrefix(got, `app:="a" | format "<service.name>" as service_name `) || !strings.HasSuffix(got, ` <gate n > | stats by (service_name) avg(__lvp_v)`) {
		t.Errorf("%s: %s", q, got)
	}
	q = `avg_over_time({app="a"} | unwrap n [5m]) by (detected_level)`
	got = unwrapText(t, q, Profile{})
	if !strings.HasPrefix(got, `app:="a" <chain> <gate n > | stats by (detected_level) avg(__lvp_v)`) {
		t.Errorf("%s: %s", q, got)
	}
}

// Everything the lowering leaves to the string translator is named. A spelling or text the translator reads unlike
// the plan is turned away by the text screen (PlanQuery) or by the typed check.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesUnwrapAggregations(t *testing.T) {
	for q, construct := range map[string]string{
		`sum_over_time({app="a"} |unwrap n [5m])`:                               "RangeAgg unwrap spelling",
		`sum_over_time({app="a"} | unwrap bytes (n) [5m])`:                      "RangeAgg unwrap spelling",
		`sum_over_time({app="a"} | unwrap bytes( n ) [5m])`:                     "RangeAgg unwrap spelling",
		`sum_over_time({app="a"} |= "| unwrap x" | unwrap n [5m])`:              "RangeAgg unwrap spelling",
		`sum_over_time({app="a"} |= "| json" | unwrap n [5m])`:                  "RangeAgg quoted clause",
		`sum_over_time({app="a"} | unwrap n | s="|json" [5m])`:                  "RangeAgg quoted clause",
		`sum_over_time({app="a"} |= "| stats by (x)" | unwrap n [5m])`:          "RangeAgg quoted clause",
		`sum_over_time({app="a"} |= "| unpack_json" | unwrap n [5m])`:           "RangeAgg quoted clause",
		`sum_over_time({app="a"} |= "a bool b" | unwrap n [5m])`:                "RangeAgg quoted clause",
		`sum_over_time({app="a"} |= "x without (y)" | unwrap n [5m])`:           "RangeAgg quoted clause",
		`rate({app="a"} | unwrap n [0s])`:                                       "RangeAgg range",
		`avg_over_time({app="a"} | unwrap n [5m]) without (pod)`:                "RangeAgg grouping",
		`avg_over_time({app="a"} | unwrap n [5m]) by (bool)`:                    "RangeAgg grouping label",
		`sum_over_time({app="a"} | json | unwrap n [5m])`:                       "RangeAgg parser",
		`sum_over_time({app="a"} | logfmt | unwrap n [5m])`:                     "RangeAgg parser",
		`sum_over_time({app="a"} | unwrap n | drop x [5m])`:                     "RangeAgg drop or keep",
		`sum_over_time({app="a"} | unwrap n | unwrap m [5m])`:                   "RangeAgg unwrap stages",
		`sum_over_time({app="a"} | drop x | unwrap n [5m])`:                     "RangeAgg drop or keep",
		`sum_over_time({app="a"} | line_format "x" | unwrap n [5m])`:            "RangeAgg LogSelect pipeline",
		`sum_over_time({app="a"} | detected_level="e" | unwrap n [5m])`:         "RangeAgg LogSelect label filter detected_level",
		`sum_over_time({app="a"} |~ "\x41b" | unwrap n [5m])`:                   "RangeAgg LogSelect escaped spelling",
		`stddev(sum_over_time({app="a"} | unwrap n [5m]))`:                      "VectorAgg stddev",
		`stdvar by (app) (sum_over_time({app="a"} | unwrap n [5m]))`:            "VectorAgg stdvar",
		`sum by (app) (avg_over_time({app="a"} | unwrap n [5m]) by (x))`:        "VectorAgg grouping",
		`sum(rate({app="a"} | unwrap n [0s]))`:                                  "VectorAgg range",
		`sum without () (sum_over_time({app="a"} | unwrap n [5m]))`:             "VectorAgg without empty",
		`sum by ( bool ) (sum_over_time({app="a"} | unwrap n [5m]))`:            "VectorAgg quoted clause",
		`sum by (app) (sum_over_time({app="a"} |= "| stats " | unwrap n [5m]))`: "VectorAgg quoted clause",
		`sum without (x) (sum_over_time({x!="  "} | unwrap n [5m]))`:            "VectorAgg quoted clause",
		`avg(sum_over_time({app="a"} |= "| json" | unwrap n [5m]))`:             "VectorAgg quoted clause",
		`sum(sum_over_time({app="a"} |unwrap n [5m]))`:                          "VectorAgg unwrap spelling",
		`sum(sum_over_time({app="a"} | json | unwrap n [5m]))`:                  "VectorAgg parser",
		`sum(bytes_rate({app="a"} | unwrap n [5m]))`:                            "VectorAgg unwrap bytes_rate",
		// the quoted text a translator regexp reads out of a rate or a count, min, max and avg (a parser in the text
		// takes the level from their series identity, an unwrap makes a rate a sum).
		`rate({app="a"} |= "| unwrap x" [5m])`:               "RangeAgg quoted clause",
		`rate({app="a"} |= "| json" [5m])`:                   "RangeAgg quoted clause",
		`count(count_over_time({app="a"} |= "| json" [5m]))`: "VectorAgg quoted clause",
		`sum(rate({app="a"} |= "|logfmt" [5m]))`:             "VectorAgg quoted clause",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := PlanQuery(q, p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
	// A plan built without the parameter's spelling (not from the parser) leaves the quantile to the translator.
	for q, construct := range map[string]string{
		`quantile_over_time(0.5, {app="a"} | unwrap n [5m])`:      "RangeAgg unwrap quantile spelling",
		`sum(quantile_over_time(0.5, {app="a"} | unwrap n [5m]))`: "VectorAgg unwrap quantile spelling",
	} {
		p := rangeAggPlan(q)
		ra, ok := p.Root.(*queryplan.RangeAgg)
		if !ok {
			ra = p.Root.(*queryplan.VectorAgg).In.(*queryplan.RangeAgg)
		}
		ra.PhiText = ""
		if _, err := Plan(p, Profile{}); !errors.Is(err, ErrNotLowered) || !strings.Contains(err.Error(), construct) {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
	// A kept line and a stream label name the string translator writes as it is.
	for _, q := range []string{`sum_over_time({app="a"} | unwrap n [5m])`, `sum(rate({app="a"} | unwrap n [5m]))`} {
		_, err := Plan(rangeAggPlan(q), Profile{KeepLine: true})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || !strings.HasSuffix(nl.Construct, " keepLine") {
			t.Errorf("%s under keepLine: got %v", q, err)
		}
	}
}

// A quantile is written with the parameter as the client spelled it, which the typed function prints only in the
// shortest form of its value: another spelling (`0.50`, `5e-1`) is left to the string translator.
//
// conformance: architecture-syntax-package-layering
func TestPlanKeepsQuantileSpelling(t *testing.T) {
	for _, phi := range []string{"0", "1", "0.5", "0.95", "0.999", "0.01"} {
		q := `quantile_over_time(` + phi + `, {app="a"} | unwrap n [5m])`
		got := unwrapText(t, q, Profile{})
		if want := `app:="a" <gate n > | stats quantile(` + phi + `, __lvp_v)`; got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
	for _, phi := range []string{"0.50", "5e-1", "1e0", "0.0", "00.5", "1.", "0.9500"} {
		q := `quantile_over_time(` + phi + `, {app="a"} | unwrap n [5m])`
		if _, err := PlanQuery(q, rangeAggPlan(q), Profile{}); !errors.Is(err, ErrNotLowered) || !strings.Contains(err.Error(), "RangeAgg unwrap quantile spelling") {
			t.Errorf("%s: got %v, want not lowered: RangeAgg unwrap quantile spelling", q, err)
		}
	}
}

// The unwrap stage the lowering reads is the one the string translator reads from the text.
//
// conformance: architecture-syntax-package-layering
func TestUnwrapFromText(t *testing.T) {
	for q, want := range map[string][3]string{
		`sum_over_time({a="b"} | unwrap n [5m])`:                   {"n", "", "true"},
		`sum_over_time({a="b"} | unwrap bytes(n) [5m])`:            {"n", "bytes", "true"},
		`sum_over_time({a="b"} | unwrap duration_seconds(n) [5m])`: {"n", "duration", "true"},
		`sum_over_time({a="b"} | unwrap n | __error__="" [5m])`:    {"n", "", "true"},
		`sum_over_time({a="b"} |unwrap n [5m])`:                    {"", "", "false"},
		`sum_over_time({a="b"} |= "| unwrap m" | unwrap n [5m])`:   {`m"`, "", "true"},
	} {
		field, conv, ok := unwrapFromText(q)
		if field != want[0] || conv != want[1] || strconv.FormatBool(ok) != want[2] {
			t.Errorf("%s: got %q %q %v, want %v", q, field, conv, ok, want)
		}
	}
	if !textHasUnwrapOrParser("a | json", false) || !textHasUnwrapOrParser("a |\tlogfmt", false) || textHasUnwrapOrParser("a | jsonx", false) ||
		textHasUnwrapOrParser("a | json_x", false) || textHasUnwrapOrParser("a | unwrap n", false) || !textHasUnwrapOrParser("a | unwrap n", true) {
		t.Error("textHasUnwrapOrParser reads the words wrong")
	}
}

// unwrapLowering lowers q under prof and compares the text with the string translator's, byte for byte. delegated is
// true when the lowering answers ErrNotLowered; ok is false when q is not translated.
func unwrapLowering(t testing.TB, q string, prof rangeProfile) (lowered, legacy string, delegated, ok bool) {
	t.Helper()
	p := rangeAggPlan(q)
	if p == nil {
		return "", "", false, false
	}
	legacy, terr := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.label, prof.stream, prof.caps)
	op, lerr := PlanQuery(q, p, prof.lowerProfile())
	if errors.Is(lerr, ErrNotLowered) {
		return "", legacy, true, terr == nil
	}
	if (terr == nil) != (lerr == nil) {
		t.Fatalf("%s: translator error %v, lowering error %v", q, terr, lerr)
	}
	if terr != nil {
		return "", "", false, false
	}
	return physical.Encode(op), legacy, false, true
}

// FuzzUnwrapLowering holds the lowering of range and vector aggregations over an unwrapped label to the string
// translator on any selector values, line filters, label filters, unwrapped labels, conversions, functions, by
// clauses, ranges and offsets: both write the same text, byte for byte, or the lowering leaves the query to the
// translator. Values the translator reads as syntax in a metric query are left out, as in FuzzRangeAggLowering.
//
// conformance: architecture-syntax-package-layering
func FuzzUnwrapLowering(f *testing.F) {
	f.Add("a", "x", "level", "error", uint8(0), uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "é\n", "status", ".*", uint8(7), uint8(3), uint8(5), uint8(9), uint8(4))
	f.Add("k8s.pod", "[5m]", "a-b", "10.0.0.0/8", uint8(200), uint8(9), uint8(11), uint8(31), uint8(77))
	f.Fuzz(func(t *testing.T, sel, line, name, val string, fn, shape, rng, group, wrap uint8) {
		for _, s := range []string{sel, line, name, val} {
			if translatorReadsAsSyntax(s) || strings.Contains(s, "stats") || strings.Contains(s, "service_name") || strings.Contains(s, "detected_level") {
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
			if b.Len() == 0 {
				return "x"
			}
			return b.String()
		}
		matchers := []string{`="`, `!="`, `=~"`, `!~"`}
		selector := `{` + ident(sel) + matchers[shape%4] + escapeValue(sel) + `"}`
		stage := []string{
			``, ` |= ` + strconv.Quote(line), ` != ` + strconv.Quote(line), ` |~ ` + strconv.Quote(line), ` |> ` + strconv.Quote(line),
			` | ` + ident(name) + `=` + strconv.Quote(val), ` | ` + ident(name) + `>=` + strconv.Itoa(len(val)),
		}[shape>>2%7]
		label := ident(name)
		unwrap := []string{`| unwrap ` + label, `| unwrap bytes(` + label + `)`, `| unwrap duration(` + label + `)`, `| unwrap duration_seconds(` + label + `)`,
			`| unwrap ` + label + ` | __error__=""`}[shape>>5%5]
		functions := []string{"sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "first_over_time", "last_over_time", "stddev_over_time",
			"stdvar_over_time", "quantile_over_time", "rate", "rate_counter", "absent_over_time"}
		function := functions[int(fn)%len(functions)]
		span := []string{`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[5m] offset 1h`, `[1m] offset 0s`}[rng%6]
		body := selector + stage + ` ` + unwrap + ` ` + span
		if function == "quantile_over_time" {
			body = []string{"0.5", "0.95", "0", "1", "0.50"}[group%5] + ", " + body
		}
		q := function + `(` + body + `)`
		// the clause of the range (only some functions take one) or of the vector aggregation around it.
		clauses := []string{``, `by (` + ident(val) + `)`, `by ()`, `by (` + ident(val) + `, ` + ident(sel) + `)`, `without (` + ident(val) + `)`}
		clause := clauses[group>>3%5]
		switch ops := []string{"", "sum", "count", "min", "max", "avg"}; {
		case wrap%6 == 0 || function == "absent_over_time" && clause == "by ()":
			if clause != "" {
				q += ` ` + clause
			}
		default:
			op := ops[int(wrap)%len(ops)]
			if clause == "" {
				q = op + `(` + q + `)`
			} else if wrap&0x80 != 0 {
				q = op + `(` + q + `) ` + clause
			} else {
				q = op + ` ` + clause + ` (` + q + `)`
			}
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, delegated, ok := unwrapLowering(t, q, prof); ok && !delegated && lowered != legacy {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
