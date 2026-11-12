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
