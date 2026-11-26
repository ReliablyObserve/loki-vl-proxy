package logql

import (
	"reflect"
	"testing"
)

// A drop or keep list keeps the order it is written in: each matcher holds how many bare names precede it, and the
// stage prints the list back as written, which parses to the same stage.
//
// conformance: semantics/keep-names-and-matchers
func TestDropKeepListWrittenOrder(t *testing.T) {
	for query, bare := range map[string][]int{
		`{a="b"} | keep x, y="1"`:                      {1},
		`{a="b"} | keep y="1", x`:                      {0},
		`{a="b"} | keep a="1", b, c!="2", d, e=~"3"`:   {0, 1, 2},
		`{a="b"} | drop a, b!~"1", c`:                  {1},
		`{a="b"} | keep a="1", a="2", a`:               {0, 0},
		`{a="b"} | keep level=~"error|warn", app, env`: {0},
	} {
		expr, err := Parse(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		lq := expr.(*LogQuery)
		var got []int
		var matchers []DropMatcher
		switch s := lq.Pipeline[0].(type) {
		case *KeepStage:
			matchers = s.Matchers
		case *DropStage:
			matchers = s.Matchers
		}
		for _, m := range matchers {
			got = append(got, m.Bare)
		}
		if !reflect.DeepEqual(got, bare) {
			t.Errorf("%s: bare names before each matcher %v, want %v", query, got, bare)
		}
		again, err := Parse(expr.String())
		if err != nil || !reflect.DeepEqual(again.(*LogQuery).Pipeline, lq.Pipeline) {
			t.Errorf("%s: printed %q does not parse back to the stage (%v)", query, expr.String(), err)
		}
		if lq.Pipeline[0].String() != query[len(`{a="b"} `):] {
			t.Errorf("%s: printed %q", query, lq.Pipeline[0].String())
		}
	}
	// A list built without positions prints the bare names first, as before.
	k := &KeepStage{Labels: []string{"a", "b"}, Matchers: []DropMatcher{{Name: "c", Op: "=", Value: "1", Bare: 9}}}
	if got := k.String(); got != `| keep a, b, c="1"` {
		t.Errorf("hand-built stage prints %q", got)
	}
}
