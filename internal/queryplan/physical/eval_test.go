package physical

import (
	"reflect"
	"testing"
)

// SplitEval peels the levels the proxy evaluates, innermost first, and leaves the operand; Split of the operand
// then gives the request and the operand's own post-processing.
//
// conformance: architecture-syntax-package-layering
func TestSplitEvalPeelsTheLevelsInnermostFirst(t *testing.T) {
	q := VLQuery{Text: `app:="x" | stats count()`}
	by := &Grouping{Labels: []string{"app"}}
	lr := []LabelReplace{{DstLabel: "a", Replacement: "$1", SrcLabel: "b", Regex: "(.*)"}}
	operand := LabelMap{Replace: lr, In: Fold{Levels: []FoldLevel{{Op: "sum", Labels: []string{"pod"}, Without: true}}, In: q}}
	core := RankStep{Op: "bottomk", K: 2, In: SeriesAgg{Op: "sum", By: by, In: RankStep{Op: "topk", K: 3, By: by, In: operand}}}

	levels, got := SplitEval(core)
	want := []Level{{"topk", 3, by}, {"sum", 0, by}, {"bottomk", 2, nil}}
	if !reflect.DeepEqual(levels, want) {
		t.Fatalf("levels = %#v, want %#v", levels, want)
	}
	if !reflect.DeepEqual(got, operand) {
		t.Fatalf("operand = %#v, want %#v", got, operand)
	}
	req, post := Split(got)
	if req != q || !post.Folds() || post.Without.Op != "sum" || !reflect.DeepEqual(post.LabelReplace, lr) {
		t.Fatalf("Split(operand) = %#v, %#v", req, post)
	}
	if Encode(core) != Encode(operand) {
		t.Fatalf("Encode(core) = %q, want the operand's %q", Encode(core), Encode(operand))
	}
	for i, l := range levels {
		if l.Ranks() != (i != 1) {
			t.Errorf("level %d (%s): Ranks = %v", i, l.Op, l.Ranks())
		}
	}
	// A core that is no level is its own operand.
	if levels, got := SplitEval(q); len(levels) != 0 || got != Op(q) {
		t.Fatalf("SplitEval(query) = %#v, %#v", levels, got)
	}
	// label_replace around a ranking splits into the outer post-processing and the ranking.
	wrapped := LabelMap{Replace: lr, In: RankStep{Op: "topk", K: 1, In: q}}
	rankCore, post := Split(wrapped)
	if _, ok := rankCore.(RankStep); !ok || !reflect.DeepEqual(post.LabelReplace, lr) {
		t.Fatalf("Split(LabelMap{RankStep}) = %#v, %#v", rankCore, post)
	}
}

// Canon is the structure the proxy evaluates: the operator, its K and clause, and the operand. `by ()` is no clause,
// `without ()` is not (the proxy groups it by every label), and the label order is part of the clause.
//
// conformance: architecture-syntax-package-layering
func TestCanonOfEvaluatedLevels(t *testing.T) {
	q := VLQuery{Text: `app:="x" | stats count()`}
	rank := func(op string, k int, by *Grouping) Op { return RankStep{Op: op, K: k, By: by, In: q} }
	app := func(without bool) *Grouping { return &Grouping{Labels: []string{"app"}, Without: without} }
	equal := [][2]Op{
		{rank("topk", 3, nil), rank("topk", 3, &Grouping{})},
		{rank("topk", 3, nil), rank("topk", 3, &Grouping{Labels: []string{}})},
		{rank("topk", 3, &Grouping{Without: true}), rank("topk", 3, &Grouping{Without: true, Labels: []string{}})},
		{rank("topk", 3, app(false)), rank("topk", 3, &Grouping{Labels: []string{"app"}})},
		{SeriesAgg{Op: "stddev", In: q}, SeriesAgg{Op: "stddev", By: &Grouping{}, In: q}},
	}
	for i, c := range equal {
		if !Equal(c[0], c[1]) {
			t.Errorf("equal pair %d differs", i)
		}
	}
	differ := [][2]Op{
		{rank("topk", 3, nil), rank("topk", 4, nil)},
		{rank("topk", 3, nil), rank("bottomk", 3, nil)},
		{rank("topk", 3, nil), rank("topk", 3, &Grouping{Without: true})},
		{rank("topk", 3, app(false)), rank("topk", 3, app(true))},
		{rank("topk", 3, &Grouping{Labels: []string{"a", "b"}}), rank("topk", 3, &Grouping{Labels: []string{"b", "a"}})},
		{rank("topk", 3, nil), q},
		{rank("sort", 0, nil), SeriesAgg{Op: "sort", In: q}},
		{rank("topk", 3, nil), RankStep{Op: "topk", K: 3, In: VLQuery{Text: `app:="y" | stats count()`}}},
		{rank("topk", 3, nil), RankStep{Op: "topk", K: 3, In: RankStep{Op: "topk", K: 3, In: q}}},
	}
	for i, c := range differ {
		if Equal(c[0], c[1]) {
			t.Errorf("different pair %d is equal", i)
		}
	}
	if !Evaluated(LabelMap{In: rank("sort", 0, nil)}) || Evaluated(LabelMap{In: q}) || !Evaluated(Binary{L: q, R: SeriesAgg{In: q}}) {
		t.Error("Evaluated misreads the operators")
	}
}
