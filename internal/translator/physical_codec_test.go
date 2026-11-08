package translator

import (
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// checkPhysicalCodec holds the codec to its two promises for any text: the encoded operators are the text, and
// the clean query and the post-processing are what the proxy's two-step strip returned before the codec.
func checkPhysicalCodec(t testing.TB, s string) physical.Op {
	t.Helper()
	op, err := DecodePhysical(s)
	if err != nil {
		t.Fatalf("DecodePhysical(%q): %v", s, err)
	}
	if got := EncodePhysical(op); got != s {
		t.Fatalf("EncodePhysical(DecodePhysical(%q)) = %q", s, got)
	}
	if v, ok := op.(physical.Verbatim); ok {
		// The cheap check DecodePhysical makes (encodesTo) must agree with building the text.
		if EncodePhysical(v.In) == s {
			t.Fatalf("%q is kept as text but its operands write it", s)
		}
	} else {
		if got := EncodePhysical(rebuilt(op)); got != s {
			t.Fatalf("EncodePhysical of the operands of %q = %q", s, got)
		}
	}
	wantClean, wantOp, wantLabels := ParseWithoutMarker(s)
	wantClean, wantSpecs := ParseAllLabelReplaceMarkers(wantClean)
	core, post := physical.Split(op)
	if got := EncodePhysical(core); got != wantClean {
		t.Fatalf("clean query of %q = %q, want %q", s, got, wantClean)
	}
	var gotOp string
	var gotLabels []string
	if post.Without != nil {
		gotOp, gotLabels = post.Without.Op, post.Without.Labels
	}
	if gotOp != wantOp || !reflect.DeepEqual(gotLabels, wantLabels) {
		t.Fatalf("without of %q = %q %#v, want %q %#v", s, gotOp, gotLabels, wantOp, wantLabels)
	}
	if !reflect.DeepEqual(post.LabelReplace, wantSpecs) {
		t.Fatalf("label_replace of %q = %#v, want %#v", s, post.LabelReplace, wantSpecs)
	}
	return op
}

// rebuilt is op with the text of every Binary dropped, so EncodePhysical writes it from the operands, the way it
// writes a Binary built without text.
func rebuilt(op physical.Op) physical.Op {
	switch v := op.(type) {
	case physical.Binary:
		v.Text, v.L, v.R = "", rebuilt(v.L), rebuilt(v.R)
		return v
	case physical.Fold:
		v.In = rebuilt(v.In)
		return v
	case physical.LabelMap:
		v.In = rebuilt(v.In)
		return v
	case physical.Verbatim:
		v.In = rebuilt(v.In)
		return v
	}
	return op
}

// conformance: semantics/label-replace-new-label, semantics/without-over-inner-aggregation-collapses
func TestPhysicalCodecEdgeTable(t *testing.T) {
	const q = `app:=x | stats by (pod, node) count() as c`
	lr := func(dst string) string {
		return labelReplaceMarker(LabelReplaceSpec{DstLabel: dst, Replacement: "$1", SrcLabel: "app", Regex: "(.*)"})
	}
	cases := []struct {
		name, in string
		verbatim bool // the text is not one the translator writes: the codec keeps it as it came
		check    func(t *testing.T, core physical.Op, post physical.Post)
	}{
		{name: "clean query", in: q, check: func(t *testing.T, core physical.Op, post physical.Post) {
			if core != (physical.VLQuery{Text: q}) || post.Without != nil || post.LabelReplace != nil {
				t.Errorf("core %#v, post %#v", core, post)
			}
		}},
		{name: "without", in: q + "__without__:pod,node", check: func(t *testing.T, core physical.Op, post physical.Post) {
			if want := (physical.FoldLevel{Labels: []string{"pod", "node"}, Without: true}); post.Without == nil || !reflect.DeepEqual(*post.Without, want) {
				t.Errorf("without %#v, want %#v", post.Without, want)
			}
		}},
		{name: "without with an op", in: q + "__without__:max:pod", check: func(t *testing.T, core physical.Op, post physical.Post) {
			if post.Without == nil || post.Without.Op != "max" || !reflect.DeepEqual(post.Without.Labels, []string{"pod"}) {
				t.Errorf("without %#v", post.Without)
			}
		}},
		{name: "without then label_replace", in: q + "__without__:pod" + lr("a")},
		{name: "label_replace then without", in: q + lr("a") + "__without__:pod", verbatim: true},
		{name: "label_replace once", in: q + lr("a")},
		{name: "label_replace three", in: q + lr("a") + lr("b") + lr("c"), check: func(t *testing.T, core physical.Op, post physical.Post) {
			if len(post.LabelReplace) != 3 || post.LabelReplace[0].DstLabel != "a" || post.LabelReplace[2].DstLabel != "c" {
				t.Errorf("label_replace %#v, want a, b, c in order", post.LabelReplace)
			}
		}},
		{name: "label_replace three under without", in: q + "__without__:sum:pod" + lr("a") + lr("b") + lr("c")},
		{name: "padded labels", in: q + "__without__:pod, node", verbatim: true},
		{name: "empty label list", in: q + "__without__:", verbatim: true},
		{name: "an op and no labels", in: q + "__without__:sum:", verbatim: true},
		{name: "invalid label_replace payload", in: q + "__lvp_lr:%%%__"},
		{name: "binary", in: "__binary__:/:A|||B", check: func(t *testing.T, core physical.Op, post physical.Post) {
			b, ok := core.(physical.Binary)
			if !ok || b.Op != "/" || b.L != (physical.VLQuery{Text: "A"}) || b.R != (physical.VLQuery{Text: "B"}) || b.Match != nil || b.Tail != "" {
				t.Errorf("core %#v", core)
			}
		}},
		{name: "binary with matching", in: "__binary__:*:A|||B@@@on:app, pod@@@group_left:team", check: func(t *testing.T, core physical.Op, post physical.Post) {
			b, ok := core.(physical.Binary)
			if !ok || b.Match == nil || !b.Match.MatchOn || !reflect.DeepEqual(b.Match.On, []string{"app", "pod"}) || b.Match.GroupSide != "group_left" || !reflect.DeepEqual(b.Match.GroupLeft, []string{"team"}) {
				t.Errorf("core %#v", core)
			}
		}},
		{name: "binary with ignoring and group_right", in: "__binary__:+:A|||B@@@ignoring:pod@@@group_right:", check: func(t *testing.T, core physical.Op, post physical.Post) {
			b, ok := core.(physical.Binary)
			if !ok || b.Match == nil || !reflect.DeepEqual(b.Match.Ignoring, []string{"pod"}) || b.Match.GroupSide != "group_right" {
				t.Errorf("core %#v", core)
			}
		}},
		{name: "binary over without", in: "__binary__:/:A|||B@@@on:app__without__:pod", check: func(t *testing.T, core physical.Op, post physical.Post) {
			if _, ok := core.(physical.Binary); !ok || post.Without == nil || !reflect.DeepEqual(post.Without.Labels, []string{"pod"}) {
				t.Errorf("core %#v, post %#v", core, post)
			}
		}},
		{name: "binary under label_replace", in: "__binary__:/:A|||B" + lr("a"), check: func(t *testing.T, core physical.Op, post physical.Post) {
			if _, ok := core.(physical.Binary); !ok || len(post.LabelReplace) != 1 {
				t.Errorf("core %#v, post %#v", core, post)
			}
		}},
		{name: "binary with markers on its operands", in: "__binary__:-:A__without__:pod" + lr("a") + "|||B", verbatim: true, check: func(t *testing.T, core physical.Op, post physical.Post) {
			// The strip reads the whole text, as it always did: the markers inside the first operand leave the
			// query, and what is left is a binary.
			if core != (physical.Binary{Op: "-", L: physical.VLQuery{Text: "A"}, R: physical.VLQuery{Text: "B"}, Text: "__binary__:-:A|||B"}) || post.Without == nil || len(post.LabelReplace) != 1 {
				t.Errorf("core %#v, post %#v", core, post)
			}
		}},
		{name: "nested binary", in: "__binary__:/:__binary__:+:A|||B|||C"},
		{name: "scalar right", in: "__binary__:*:A|||1e3", check: func(t *testing.T, core physical.Op, post physical.Post) {
			b, ok := core.(physical.Binary)
			if !ok || b.R != (physical.ScalarOp{V: 1000, Text: "1e3"}) {
				t.Errorf("core %#v", core)
			}
		}},
		{name: "scalar left", in: "__binary__:-:100|||B", check: func(t *testing.T, core physical.Op, post physical.Post) {
			b, ok := core.(physical.Binary)
			if !ok || b.L != (physical.ScalarOp{V: 100, Text: "100"}) {
				t.Errorf("core %#v", core)
			}
		}},
		{name: "scalar query", in: "0.5", check: func(t *testing.T, core physical.Op, post physical.Post) {
			if core != (physical.ScalarOp{V: 0.5, Text: "0.5"}) {
				t.Errorf("core %#v", core)
			}
		}},
		{name: "binary marker without operands", in: "__binary__:/:AB", check: func(t *testing.T, core physical.Op, post physical.Post) {
			if core != (physical.VLQuery{Text: "__binary__:/:AB"}) {
				t.Errorf("core %#v", core)
			}
		}},
		{name: "phrase fallback", in: `error "connection reset" timeout`},
		{name: "star prefix", in: `* | stats count() as c`},
		{name: "empty", in: ""},
		{name: "a marker in a phrase", in: `foo__without__:bar | stats count()`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := checkPhysicalCodec(t, tc.in)
			_, isVerbatim := op.(physical.Verbatim)
			if isVerbatim != tc.verbatim {
				t.Errorf("kept as text = %v, want %v", isVerbatim, tc.verbatim)
			}
			if tc.check != nil {
				core, post := physical.Split(op)
				tc.check(t, core, post)
			}
		})
	}
}

// The text of a translated query is what the codec is for: every query the translator writes decodes to operators
// that encode back to it, whatever the options.
// conformance: semantics/label-replace-new-label, semantics/without-over-inner-aggregation-collapses
func TestPhysicalCodecRoundTripsTranslatorOutput(t *testing.T) {
	queries := []string{
		`sum without (pod) (rate({app="x"}[5m]))`,
		`max without (pod, node) (rate({app="x"}[5m]))`,
		`sum without (instance) (sum without (pod) (rate({app="x"}[5m])))`,
		`label_replace(label_replace(rate({app="x"}[5m]), "a", "$1", "app", "(.*)"), "b", "x", "a", ".*")`,
		`label_replace(sum without (pod) (rate({app="x"}[5m])), "a", "$1", "app", "(.*)")`,
		`rate({app="x"}[5m]) / on (app) group_left (team) rate({app="y"}[5m])`,
		`sum without (pod) (rate({app="x"}[5m])) / sum without (pod) (rate({app="y"}[5m]))`,
		`rate({app="x"}[5m]) * 100`,
		`2 * rate({app="x"}[5m])`,
		`topk(3, sum without (pod) (rate({app="x"}[5m])))`,
		`{app="x"} |= "error"`,
		`{app="x"} | json | level="error"`,
	}
	for _, caps := range []logsql.Capabilities{logsql.CapabilitiesFor("v1.49.0"), logsql.CapabilitiesFor("v1.53.0")} {
		for _, q := range queries {
			s, err := TranslateLogQLWithCapabilities(q, nil, nil, caps)
			if err != nil {
				continue
			}
			op := checkPhysicalCodec(t, s)
			if _, v := op.(physical.Verbatim); v {
				t.Errorf("%q: %q is kept as text", q, s)
			}
		}
	}
}

// FuzzPhysicalCodec holds the codec to its promises for arbitrary text, seeded with the translation of the capture
// sample and with marker-bearing text.
func FuzzPhysicalCodec(f *testing.F) {
	for _, s := range []string{
		"", "100", "1e3", "q__without__:pod", "q__without__:sum:pod,node", "q__without__:pod, node", "q__without__:",
		"q" + labelReplaceMarker(LabelReplaceSpec{DstLabel: "a", Replacement: "$1", SrcLabel: "app", Regex: "(.*)"}),
		"q__without__:pod" + labelReplaceMarker(LabelReplaceSpec{DstLabel: "a", Replacement: "b", SrcLabel: "c", Regex: "d"}),
		"__binary__:/:A|||B", "__binary__:*:A|||B@@@on:app, pod@@@group_left:team", "__binary__:*:A|||100", "__binary__:/:AB",
		"__binary__:/:__binary__:+:A|||B|||C", "q__lvp_lr:%%%__", `error "x" y`,
	} {
		f.Add(s)
	}
	for _, s := range captureSampleTranslations() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		checkPhysicalCodec(t, s)
	})
}

// scalarValue turns a query away before strconv.ParseFloat copies it into an error; the answer must stay the one
// TrimSpace and ParseFloat give.
func TestScalarValueMatchesParseFloat(t *testing.T) {
	ref := func(s string) (float64, bool) {
		s = strings.TrimSpace(s)
		if s == "" {
			return 0, false
		}
		v, err := strconv.ParseFloat(s, 64)
		return v, err == nil
	}
	check := func(s string) {
		t.Helper()
		gv, gok := scalarValue(s)
		wv, wok := ref(s)
		if gok != wok || (gok && gv != wv && !(math.IsNaN(gv) && math.IsNaN(wv))) {
			t.Fatalf("scalarValue(%q) = %v, %v, want %v, %v", s, gv, gok, wv, wok)
		}
	}
	for _, s := range []string{"", " ", "1", "-42", "+1.5", "1e5", "1.5e-3", ".5", "5.", "inf", "-Inf", "+INF", "infinity", "NaN", "nan",
		"0x1p-2", "0X1.8P3", "0x_1p0", "1_000", "0x1_0p0", " 5 ", "\t5\n", " 5", "5 ", "1e", "e1", "abc", "nginx", "app:=x", "{app=\"x\"}",
		"nan(1)", "infinitx", "1 2", "0x", "--1", "1e400", "-1e400"} {
		check(s)
	}
	for b := 0; b < 256; b++ {
		check(string([]byte{byte(b)}))
		check("1" + string([]byte{byte(b)}))
		check(string([]byte{byte(b)}) + "1")
	}
	alphabet := "0123456789+-._ eExXpPaAfFiInNtTyYgz\t"
	for _, a := range alphabet {
		for _, b := range alphabet {
			for _, c := range alphabet {
				check(string([]rune{a, b, c}))
			}
		}
	}
}
