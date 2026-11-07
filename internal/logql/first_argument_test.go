package logql

import "testing"

func TestFirstArgumentLenSkipsStringsAndBrackets(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`a, b)`, `a`},
		{`rate({a="x"}[5m]), "d")`, `rate({a="x"}[5m])`},
		{`rate({a=") , ("}[5m]), "d")`, `rate({a=") , ("}[5m])`},
		{`rate({a="x"} |= ")" [5m]) + rate({b="z"}[5m]), "d")`, `rate({a="x"} |= ")" [5m]) + rate({b="z"}[5m])`},
		{"rate({a=\"x\"} |= `,)(` [5m]), \"d\")", "rate({a=\"x\"} |= `,)(` [5m])"},
		{`rate({a="\",)"}[5m]), "d")`, `rate({a="\",)"}[5m])`},
		{`sum by (a, b) (x))`, `sum by (a, b) (x)`},
		{`unclosed(`, `unclosed(`},
	} {
		if got := tc.in[:FirstArgumentLen(tc.in)]; got != tc.want {
			t.Errorf("FirstArgumentLen(%q) takes %q, want %q", tc.in, got, tc.want)
		}
	}
}
