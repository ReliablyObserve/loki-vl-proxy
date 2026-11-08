package logql

import (
	"reflect"
	"strings"
	"testing"
)

// A label_replace or vector call written as Loki's grammar has it carries its parts; any other call, or one Loki
// rejects, keeps only its text. Raw and String are the text either way.
// conformance: semantics/label-replace-new-label
func TestOpaqueCallsAreTyped(t *testing.T) {
	for _, tc := range []struct {
		query string
		typed bool
		args  []string
		value float64
		inner string // the inner expression's text, "" for none
	}{
		{`label_replace(rate({a="b"}[5m]), "dst", "$1", "src", "(.*)")`, true, []string{"dst", "$1", "src", "(.*)"}, 0, `rate({a="b"}[5m])`},
		{"label_replace( rate({a=\"b\"}[5m]) , `dst` , \"\" , \"src\" , `x\\d` )", true, []string{"dst", "", "src", `x\d`}, 0, `rate({a="b"}[5m])`},
		{`label_replace(label_replace(rate({a="b"}[5m]), "x", "y", "z", "w"), "a", "b", "c", "d")`, true, []string{"a", "b", "c", "d"}, 0, ""},
		{`label_replace(sum by (app) (rate({a="b"}[5m])), "d", "$1")`, false, nil, 0, ""},
		{`label_replace(rate({a="b"}[5m]), "d", "$1", "app", "x", "y")`, false, nil, 0, ""},
		{`label_replace(rate({a="b"}[5m]), d, "$1", "app", "x")`, false, nil, 0, ""},
		{`label_replace(rate({a="b"}[5m]), "d", "$1", "app", 5)`, false, nil, 0, ""},
		{`vector(1)`, true, nil, 1, ""},
		{`vector( 2.5 )`, true, nil, 2.5, ""},
		{`vector("x")`, false, nil, 0, ""},
		{`approx_topk(3, sum by (a) (rate({a="b"}[5m])))`, false, nil, 0, ""},
	} {
		e, err := Parse(tc.query)
		if err != nil {
			t.Fatalf("%q: %v", tc.query, err)
		}
		o, ok := e.(*OpaqueMetricExpr)
		if !ok {
			t.Fatalf("%q: %T", tc.query, e)
		}
		if o.Raw != tc.query && o.Raw != strings.TrimSpace(tc.query) || o.String() != o.Raw {
			t.Errorf("%q: Raw %q, String %q", tc.query, o.Raw, o.String())
		}
		if o.Typed != tc.typed || !reflect.DeepEqual(o.Args, tc.args) || o.Value != tc.value {
			t.Errorf("%q: typed %v args %q value %v", tc.query, o.Typed, o.Args, o.Value)
		}
		if tc.inner != "" && (o.Inner == nil || o.Inner.String() != tc.inner) {
			t.Errorf("%q: inner %v", tc.query, o.Inner)
		}
		if !tc.typed && (o.Inner != nil || o.Args != nil) {
			t.Errorf("%q: an untyped call has parts: %#v", tc.query, o)
		}
	}
	// A nested label_replace is typed at every level.
	e, _ := Parse(`label_replace(label_replace(rate({a="b"}[5m]), "x", "y", "z", "w"), "a", "b", "c", "d")`)
	if in, ok := e.(*OpaqueMetricExpr).Inner.(*OpaqueMetricExpr); !ok || !in.Typed || in.Args[0] != "x" {
		t.Errorf("inner label_replace = %#v", e.(*OpaqueMetricExpr).Inner)
	}
}
