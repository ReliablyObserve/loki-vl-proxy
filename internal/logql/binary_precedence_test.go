package logql

import (
	"reflect"
	"testing"
)

// Precedence and associativity match Loki v3.7.7 pkg/logql/syntax/syntax.y.
// String adds explicit parentheses, so reparsing must preserve the same tree.
func TestBinaryPrecedenceAndAssociativity(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`vector(1) + vector(2) * vector(3)`, `vector(1) + (vector(2) * vector(3))`},
		{`vector(1) * vector(2) + vector(3)`, `(vector(1) * vector(2)) + vector(3)`},
		{`vector(8) - vector(3) - vector(2)`, `(vector(8) - vector(3)) - vector(2)`},
		{`vector(20) / vector(5) / vector(2)`, `(vector(20) / vector(5)) / vector(2)`},
		{`vector(2) ^ vector(3) ^ vector(2)`, `vector(2) ^ (vector(3) ^ vector(2))`},
		{`vector(2) * vector(3) ^ vector(2)`, `vector(2) * (vector(3) ^ vector(2))`},
		{`(vector(1) + vector(2)) * vector(3)`, `(vector(1) + vector(2)) * vector(3)`},
		{`(vector(2) ^ vector(3)) ^ vector(2)`, `(vector(2) ^ vector(3)) ^ vector(2)`},
		{`vector(1) > bool on() vector(2) + vector(3)`, `vector(1) > bool on() (vector(2) + vector(3))`},
		{`vector(1) or vector(2) and vector(3)`, `vector(1) or (vector(2) and vector(3))`},
		{`vector(1) and vector(2) unless vector(3)`, `(vector(1) and vector(2)) unless vector(3)`},
		{`vector(1) or vector(2) > bool vector(3)`, `vector(1) or (vector(2) > bool vector(3))`},
		{`vector(1) + on(app) group_left(zone) vector(2) * vector(3)`, `vector(1) + on(app) group_left(zone) (vector(2) * vector(3))`},
	} {
		t.Run(tc.query, func(t *testing.T) {
			expr, err := Parse(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if expr.String() != tc.want {
				t.Fatalf("got %q, want %q", expr.String(), tc.want)
			}
			reparsed, err := Parse(expr.String())
			if err != nil || !reflect.DeepEqual(expr, reparsed) {
				t.Fatalf("reparse changed tree: %s err=%v", expr.String(), err)
			}
		})
	}
}
