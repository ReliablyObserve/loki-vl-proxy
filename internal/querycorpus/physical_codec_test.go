package querycorpus_test

import (
	"reflect"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// checkPhysicalCodec holds the codec to its two promises for any text: the encoded operators are the text, and
// the clean query and the post-processing are what the proxy's two-step strip returned before the codec.
func checkPhysicalCodec(t testing.TB, s string) physical.Op {
	t.Helper()
	op, err := translator.DecodePhysical(s)
	if err != nil {
		t.Fatalf("translator.DecodePhysical(%q): %v", s, err)
	}
	if got := translator.EncodePhysical(op); got != s {
		t.Fatalf("translator.EncodePhysical(translator.DecodePhysical(%q)) = %q", s, got)
	}
	if v, ok := op.(physical.Verbatim); ok {
		// The cheap check DecodePhysical makes (encodesTo) must agree with building the text.
		if translator.EncodePhysical(v.In) == s {
			t.Fatalf("%q is kept as text but its operands write it", s)
		}
	} else {
		if got := translator.EncodePhysical(rebuilt(op)); got != s {
			t.Fatalf("EncodePhysical of the operands of %q = %q", s, got)
		}
	}
	assertSplitPost(t, s)
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
	t.Cleanup(querycorpus.PauseRecording())
	const q = `app:=x | stats by (pod, node) count() as c`
	lr := func(dst string) string {
		return lrMarker(dst, "$1", "app", "(.*)")
	}
	cases := []struct {
		name, in string
		verbatim bool // the text is not one the translator writes: the codec keeps it as it came
		check    func(t *testing.T, core physical.Op, post physical.Post)
	}{
		{name: "clean query", in: q, check: func(t *testing.T, core physical.Op, post physical.Post) {
			if core != (physical.VLQuery{Text: q}) || post.Folds() || post.LabelReplace != nil {
				t.Errorf("core %#v, post %#v", core, post)
			}
		}},
		{name: "without", in: q + "__without__:pod,node", check: func(t *testing.T, core physical.Op, post physical.Post) {
			if want := (physical.FoldLevel{Labels: []string{"pod", "node"}, Without: true}); !post.Folds() || !reflect.DeepEqual(post.Without, want) {
				t.Errorf("without %#v, want %#v", post.Without, want)
			}
		}},
		{name: "without with an op", in: q + "__without__:max:pod", check: func(t *testing.T, core physical.Op, post physical.Post) {
			if !post.Folds() || post.Without.Op != "max" || !reflect.DeepEqual(post.Without.Labels, []string{"pod"}) {
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
			if _, ok := core.(physical.Binary); !ok || !post.Folds() || !reflect.DeepEqual(post.Without.Labels, []string{"pod"}) {
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
			if core != (physical.Binary{Op: "-", L: physical.VLQuery{Text: "A"}, R: physical.VLQuery{Text: "B"}, Text: "__binary__:-:A|||B"}) || !post.Folds() || len(post.LabelReplace) != 1 {
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
	t.Cleanup(querycorpus.PauseRecording())
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
			s, err := translator.TranslateLogQLWithCapabilities(q, nil, nil, caps)
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
