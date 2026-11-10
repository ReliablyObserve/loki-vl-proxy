package lower

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// binOpLowering holds the lowering of q to the string translator under prof: same is whether both write the same
// operators and the same text, byte for byte; delegated whether the lowering leaves q to the translator (then the
// construct it names); ok is false when q does not parse or the translator rejects it.
func binOpLowering(t testing.TB, q string, prof rangeProfile) (lowered, legacy string, same bool, construct string, ok bool) {
	t.Helper()
	p := rangeAggPlan(q)
	if p == nil {
		return "", "", false, "", false
	}
	legacy, terr := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.label, prof.stream, prof.caps)
	op, lerr := PlanQuery(q, p, prof.lowerProfile())
	var nl *NotLoweredError
	if errors.As(lerr, &nl) {
		return "", legacy, false, nl.Construct, terr == nil
	}
	if terr != nil {
		if lerr == nil {
			t.Fatalf("%s: %s: lowered, the translator rejects it: %v", prof.name, q, terr)
		}
		return "", "", false, "", false
	}
	if lerr != nil {
		t.Fatalf("%s: %s: lowering failed: %v", prof.name, q, lerr)
	}
	want, _ := translator.DecodePhysical(legacy)
	lowered = physical.Encode(op)
	return lowered, legacy, lowered == legacy && physical.Equal(want, op), "", true
}

// A binary operation between the range and vector aggregations the lowering writes, and numbers, lowers to the
// operators the string translator's text decodes to: the operator, each operand's own query, a number as written,
// and the vector matching modifiers in the order the query has them, with Loki's reading of on(), ignoring() and
// group_left/group_right; label_replace calls lower to the label rewrite around the expression, innermost first.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersBinaryOperations(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	sum := `sum by (app) (count_over_time({app="b"}[5m]))`
	lower := func(q string) physical.Op {
		t.Helper()
		op, err := PlanQuery(q, rangeAggPlan(q), Profile{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return op
	}
	alone := func(q string) physical.Op { return lower(q) }
	for _, c := range []struct {
		q    string
		want physical.Op
	}{
		{rate + ` / 2`, physical.Binary{Op: "/", L: alone(rate), R: physical.ScalarOp{V: 2, Text: "2"}}},
		{`0.5 * ` + sum, physical.Binary{Op: "*", L: physical.ScalarOp{V: 0.5, Text: "0.5"}, R: alone(sum)}},
		{rate + ` - -1`, physical.Binary{Op: "-", L: alone(rate), R: physical.ScalarOp{V: -1, Text: "-1"}}},
		{rate + ` >= ` + sum, physical.Binary{Op: ">=", L: alone(rate), R: alone(sum)}},
		{rate + ` / on(app) ` + sum, physical.Binary{Op: "/", L: alone(rate), R: alone(sum), Tail: "@@@on:app",
			Match: &physical.Matching{MatchOn: true, On: []string{"app"}}}},
		{rate + ` / on() ` + sum, physical.Binary{Op: "/", L: alone(rate), R: alone(sum), Tail: "@@@on:",
			Match: &physical.Matching{MatchOn: true}}},
		{rate + ` + ignoring() ` + sum, physical.Binary{Op: "+", L: alone(rate), R: alone(sum), Tail: "@@@ignoring:",
			Match: &physical.Matching{}}},
		{rate + ` * on(app, env) group_left(pod) ` + sum, physical.Binary{Op: "*", L: alone(rate), R: alone(sum),
			Tail: "@@@on:app, env@@@group_left:pod", Match: &physical.Matching{MatchOn: true, On: []string{"app", "env"}, GroupSide: "group_left", GroupLeft: []string{"pod"}}}},
		{rate + ` / ignoring(pod) group_right() ` + sum, physical.Binary{Op: "/", L: alone(rate), R: alone(sum),
			Tail: "@@@ignoring:pod@@@group_right:", Match: &physical.Matching{Ignoring: []string{"pod"}, GroupSide: "group_right"}}},
		{`label_replace(` + sum + `, "d", "$1", "app", "(.*)")`, physical.LabelMap{
			Replace: []physical.LabelReplace{{DstLabel: "d", Replacement: "$1", SrcLabel: "app", Regex: "(.*)"}}, In: alone(sum)}},
		{`label_replace(label_replace(` + rate + ` * 2, "a", "x", "app", ".*"), "b", "y", "a", ".+")`, physical.LabelMap{
			Replace: []physical.LabelReplace{{DstLabel: "a", Replacement: "x", SrcLabel: "app", Regex: ".*"}, {DstLabel: "b", Replacement: "y", SrcLabel: "a", Regex: ".+"}},
			In:      physical.Binary{Op: "*", L: alone(rate), R: physical.ScalarOp{V: 2, Text: "2"}}}},
		{`label_replace(sum without (pod) (` + rate + `), "d", "$1", "app", "(.*)")`, physical.LabelMap{
			Replace: []physical.LabelReplace{{DstLabel: "d", Replacement: "$1", SrcLabel: "app", Regex: "(.*)"}}, In: alone(`sum without (pod) (` + rate + `)`)}},
	} {
		if got := lower(c.q); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %#v\nwant %#v", c.q, got, c.want)
		}
	}
}

// Every case the lowering leaves to the string translator is named.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesBinaryOperations(t *testing.T) {
	rate := `rate({app="a"}[5m])`
	for q, construct := range map[string]string{
		rate + ` and ` + rate:                  "BinOp set operator",
		rate + ` or ` + rate:                   "BinOp set operator",
		rate + ` unless ` + rate:               "BinOp set operator",
		rate + ` > bool 1`:                     "BinOp bool",
		`1 + 2`:                                "BinOp scalar operands",
		`sum without (pod) (` + rate + `) / 2`: "BinOp operand without",
		rate + ` / (` + rate + ` + 1)`:         "BinOp operand BinOp",
		`vector(1) + ` + rate:                  "BinOp operand VectorLit",
		`topk(2, ` + rate + `) / 2`:            "BinOp operand Rank",
		`{app="a"} > 1`:                        "BinOp operand LogSelect",
		`label_replace(` + rate + `, "a", "b", "c", "d") / 2`:                     "BinOp operand LabelReplace",
		`stddev(` + rate + `) / 2`:                                                "BinOp operand VectorAgg",
		`rate({app="a"} | keep x [5m]) / 2`:                                       "BinOp operand RangeAgg",
		`rate({app="a"} |~ "(?i)x" [5m]) / 2`:                                     "BinOp quoted text",
		`rate({app="a"} |= "a  b" [5m]) / 2`:                                      "BinOp quoted text",
		`rate({app="a"} |= "@@@" [5m]) / 2`:                                       "BinOp quoted text",
		`sum by ( bool ) (` + rate + `) / 2`:                                      "BinOp spelling",
		rate + `  / 2`:                                                            "BinOp spelling",
		rate + ` / on(app,env) ` + rate:                                           "BinOp vector matching spelling",
		rate + ` / on(app) group_left ` + rate:                                    "BinOp vector matching spelling",
		rate + `/2`:                                                               "BinOp operator spelling",
		rate + "\n/ 2":                                                            "BinOp operator spelling",
		rate + ` / 1e3`:                                                           "BinOp number spelling",
		rate + ` / 2.50`:                                                          "BinOp number spelling",
		`(` + rate + `) / 2`:                                                      "BinOp operand spelling",
		`sum( ` + rate + `) / 2`:                                                  "BinOp operand spelling",
		`label_replace(topk(2, ` + rate + ` > 1), "a", "b", "c", "d")`:            "LabelReplace operand Rank",
		`label_replace(vector(1), "a", "b", "c", "d")`:                            "LabelReplace operand VectorLit",
		`label_replace(stddev(` + rate + `), "a", "b", "c", "d")`:                 "LabelReplace operand VectorAgg",
		`label_replace(` + rate + ` and ` + rate + `, "a", "b", "c", "d")`:        "LabelReplace operand BinOp",
		`label_replace(sum by (service_name) (` + rate + `), "a", "b", "c", "d")`: "LabelReplace service_name grouping",
		`label_replace(` + rate + `, "a", "\x00", "c", "d")`:                      "LabelReplace argument with NUL",
		strings.Repeat(`label_replace(`, 17) + rate + strings.Repeat(`, "a", "b", "c", "d")`, 17): "LabelReplace nesting",
		`label_replace (` + rate + `, "a", "b", "c", "d")`:                                        "LabelReplace spelling",
		`label_replace((` + rate + `), "a", "b", "c", "d")`:                                       "LabelReplace operand spelling",
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
		if len(construct) > 48 {
			t.Errorf("%s: the construct name %q is longer than the shadow's label allows", q, construct)
		}
	}
}

// The binary lowering writes what the string translator writes for the corpus shapes, under every profile, byte for
// byte (the corpus and binop_grid_test.go hold it on many more).
//
// conformance: architecture-syntax-package-layering
func TestPlanBinaryOperationsMatchTranslator(t *testing.T) {
	for _, q := range []string{
		`sum by (level) (count_over_time({app="api-gateway",env="production"}[5m])) * 100`,
		`sum(rate({app="a"}[5m])) / sum(rate({app="b"}[5m]))`,
		`rate({app="a",pod="p1"}[5m]) / ignoring(pod) rate({app="a",pod="p2"}[5m])`,
		`sum by (app) (rate({app="a"}[5m])) > on(app) group_left(env) sum by (app, env) (rate({app="b"}[5m]))`,
		`2 * bytes_rate({app="a"} |= "x" [1m] offset 1h)`,
		`sum by (detected_level) (rate({app="a"}[5m])) / 60`,
		`sum by (service_name) (count_over_time({app="a"}[5m])) / 2`,
		`label_replace(sum by (app) (rate({app="api-gateway"}[5m])), "app_short", "$1", "app", "(.*)-.*")`,
		`label_replace(label_replace(sum by (app) (count_over_time({namespace="prod"}[1m])) * 2, "x", "$1", "app", "(.*)"), "y", "$1", "app", "(.*)")`,
		`label_replace(max without (pod) (rate({app="a"}[5m])), "d", "$1", "app", "(.*)")`,
	} {
		for _, prof := range rangeProfiles() {
			lowered, legacy, same, construct, ok := binOpLowering(t, q, prof)
			if !ok || construct != "" || !same {
				t.Errorf("%s: %s: delegated %q (ok %v)\n lowered %s\n  string %s", prof.name, q, construct, ok, lowered, legacy)
			}
		}
	}
}

// FuzzBinOpLowering holds the lowering of binary operations and label_replace calls to the string translator on
// any operator, operands of every kind, numbers, vector matching modifiers, bool, label_replace arguments and the
// values and line filters of the operands: both write the same operators and text, or the lowering leaves the query
// to the translator.
//
// conformance: architecture-syntax-package-layering
func FuzzBinOpLowering(f *testing.F) {
	f.Add("a", "x", "app", "$1", uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "a  b", "pod", "(.*)", uint8(7), uint8(3), uint8(5), uint8(9))
	f.Add("k8s.pod", "(x)", "a-b", "*bad", uint8(200), uint8(91), uint8(11), uint8(70))
	f.Add("é", "@@@on:x", "env", "", uint8(13), uint8(250), uint8(130), uint8(255))
	f.Add("a", "x", "app", "$1", uint8(0), uint8(0), uint8(0), uint8(0x20))
	f.Add("a", "x", "app", "$1", uint8(1), uint8(8), uint8(0x80), uint8(0x40))
	f.Add("a", "x", "app", "$1", uint8(2), uint8(6), uint8(0), uint8(0xe0))
	f.Fuzz(func(t *testing.T, sel, line, label, arg string, opSel, left, right, mod uint8) {
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
		lbl := ident(label)
		selector := `{app="` + escapeValue(sel) + `"}`
		filter := []string{``, ` |= ` + strconv.Quote(line), ` != ` + strconv.Quote(line), ` | ` + lbl + `="` + escapeValue(line) + `"`}[mod%4]
		// A blank, tab or newline between the function and its parenthesis is a phrase to the translator, which the
		// text screens of the operands leave to it.
		callPad := []string{"", " ", "\t", "\n"}[mod>>5&3]
		inner := []string{"rate", "count_over_time", "bytes_rate", "bytes_over_time", "absent_over_time"}[left%5] + callPad + `(` + selector + filter + `[5m])`
		operand := func(k uint8) string {
			switch k % 12 {
			case 0, 1:
				return inner
			case 2:
				return `sum(` + inner + `)`
			case 3:
				return `sum by (` + lbl + `) (` + inner + `)`
			case 4:
				return `count by (` + lbl + `, app) (` + inner + `)`
			case 5:
				return `max(` + inner + `) by (` + lbl + `)`
			case 6:
				return strconv.Itoa(int(k))
			case 7:
				return []string{"0.5", "-1", "1e3", "100", "2.50"}[k/12%5]
			case 8:
				return `sum without (` + lbl + `) (` + inner + `)`
			case 9:
				return `vector(` + strconv.Itoa(int(k)) + `)`
			case 10:
				return `(` + inner + ` * 2)`
			}
			return `stddev(` + inner + `)`
		}
		op := []string{"/", "*", "+", "-", "%", "^", "==", "!=", ">=", "<=", ">", "<", "and", "or", "unless", "> bool"}[opSel%16]
		matching := []string{``, ` on(` + lbl + `)`, ` ignoring(` + lbl + `)`, ` on()`, ` on(` + lbl + `, app) group_left(` + ident(sel) + `)`,
			` ignoring(` + lbl + `) group_right`, ` on (` + lbl + `)`, ` on(` + lbl + `,app)`}[mod>>2%8]
		q := operand(left>>3) + ` ` + op + matching + ` ` + operand(right)
		if right&0x80 != 0 {
			q = `label_replace(` + q + `, "` + escapeValue(lbl) + `", "` + escapeValue(arg) + `", "` + lbl + `", "` + escapeValue(arg) + `")`
		}
		if mod&0x80 != 0 {
			q = `label_replace(` + operand(right) + `, "d", "` + escapeValue(arg) + `", "` + lbl + `", "(.*)")`
		}
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, construct, ok := binOpLowering(t, q, prof); ok && construct == "" && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}
