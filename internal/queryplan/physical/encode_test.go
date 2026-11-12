package physical

import "testing"

// Encode writes the legacy text: the query, then the without marker, then the label_replace markers.
func TestEncodeWritesTheLegacyText(t *testing.T) {
	q := VLQuery{Text: `app:="x"`}
	lr := LabelReplace{DstLabel: "a", Replacement: "$1", SrcLabel: "b", Regex: "(.*)"}
	cases := []struct {
		name string
		op   Op
		want string
	}{
		{"query", q, `app:="x"`},
		{"scalar", ScalarOp{V: 100, Text: "100"}, "100"},
		{"vector", VectorOp{V: 2, Text: "vector(2)"}, "vector(2)"},
		{"binary with a vector", Binary{Op: "+", L: q, R: VectorOp{V: 2, Text: "vector(2)"}}, `__binary__:+:app:="x"|||vector(2)`},
		{"verbatim", Verbatim{Text: "kept", In: q}, "kept"},
		{"fold", Fold{Levels: []FoldLevel{{Op: "sum", Labels: []string{"pod", "node"}, Without: true}}, In: q}, `app:="x"__without__:sum:pod,node`},
		{"fold without labels", Fold{Levels: []FoldLevel{{Without: true}}, In: q}, `app:="x"`},
		{"binary", Binary{Op: "/", L: q, R: ScalarOp{Text: "2"}}, `__binary__:/:app:="x"|||2`},
		{"binary text", Binary{Op: "/", Text: "whole"}, "whole"},
		{"label_replace", LabelMap{Replace: []LabelReplace{lr}, In: q}, string(AppendLabelReplaceMarker([]byte(`app:="x"`), lr))},
		{"ranking", RankStep{Op: "topk", K: 3, By: &Grouping{Labels: []string{"app"}}, In: q}, `app:="x"`},
		{"series aggregation", SeriesAgg{Op: "stddev", In: LabelMap{Replace: []LabelReplace{lr}, In: q}}, string(AppendLabelReplaceMarker([]byte(`app:="x"`), lr))},
		{"ranking over ranking", RankStep{Op: "sort", In: RankStep{Op: "topk", K: 2, In: q}}, `app:="x"`},
		{"unknown", RankStep{}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Encode(c.op); got != c.want {
				t.Fatalf("Encode = %q, want %q", got, c.want)
			}
		})
	}
	if got := string(AppendLabelReplaceMarker(nil, lr)); got[:len(LabelReplacePrefix)] != LabelReplacePrefix || got[len(got)-2:] != "__" {
		t.Fatalf("label_replace marker = %q", got)
	}
}

// A vector(n) operand has a form of its own: it never equals the number n (a vector takes part in vector matching, a
// number does not), only its value is compared, not the text it was written as, and the matching around it is.
func TestCanonOfAVectorOperand(t *testing.T) {
	q := VLQuery{Text: `app:="x"`}
	bin := func(r Op, m *Matching) Op { return Binary{Op: "+", L: q, R: r, Match: m} }
	equal := [][2]Op{
		{VectorOp{V: 2}, VectorOp{V: 2, Text: "vector(2)"}},
		{bin(VectorOp{V: 0.5}, nil), bin(VectorOp{V: 0.5, Text: "vector(0.5)"}, nil)},
	}
	for i, c := range equal {
		if !Equal(c[0], c[1]) {
			t.Errorf("equal pair %d differs", i)
		}
	}
	on := &Matching{MatchOn: true}
	differ := [][2]Op{
		{VectorOp{V: 2}, ScalarOp{V: 2}},
		{VectorOp{V: 2}, VectorOp{V: 3}},
		{VectorOp{V: 0}, VectorOp{V: -1}},
		{bin(VectorOp{V: 2}, nil), bin(ScalarOp{V: 2}, nil)},
		{bin(VectorOp{V: 2}, nil), bin(VectorOp{V: 2}, on)},
		{VectorOp{V: 2}, q},
	}
	for i, c := range differ {
		if Equal(c[0], c[1]) {
			t.Errorf("differing pair %d is equal", i)
		}
	}
	if Evaluated(bin(VectorOp{V: 1}, nil)) {
		t.Error("a vector(n) operand is not a level the proxy evaluates")
	}
}

// A `without` level with no labels is `without ()`: it removes no label, so the fold combines no series. It has no
// legacy marker (the marker needs a label), so Encode writes the operand's request alone, and Split still reports the
// level, with no labels. An empty label list and no list are the same clause to Equal.
func TestFoldOfNoLabelsKeepsEverySeries(t *testing.T) {
	q := VLQuery{Text: `app:="x" | stats by (_stream) count()`}
	for _, labels := range [][]string{nil, {}} {
		f := Fold{Levels: []FoldLevel{{Op: "sum", Labels: labels, Without: true}}, In: q}
		if got := Encode(f); got != q.Text {
			t.Errorf("Encode = %q, want the operand's request %q", got, q.Text)
		}
		core, post := Split(f)
		if core != Op(q) || !post.Folds() || post.Without.Op != "sum" || len(post.Without.Labels) != 0 {
			t.Errorf("Split = %#v, %#v", core, post)
		}
	}
	empty := Fold{Levels: []FoldLevel{{Op: "sum", Labels: []string{}, Without: true}}, In: q}
	none := Fold{Levels: []FoldLevel{{Op: "sum", Without: true}}, In: q}
	if !Equal(empty, none) {
		t.Error("an empty label list differs from no list")
	}
	for name, other := range map[string]Op{
		"the operand alone": q,
		"another operation": Fold{Levels: []FoldLevel{{Op: "max", Without: true}}, In: q},
		"a label":           Fold{Levels: []FoldLevel{{Op: "sum", Labels: []string{"pod"}, Without: true}}, In: q},
		"a by level":        Fold{Levels: []FoldLevel{{Op: "sum"}}, In: q},
	} {
		if Equal(none, other) {
			t.Errorf("a fold of no label equals %s", name)
		}
	}
}
