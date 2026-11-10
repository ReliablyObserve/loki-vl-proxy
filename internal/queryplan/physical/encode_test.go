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
