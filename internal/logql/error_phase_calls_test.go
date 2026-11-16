package logql

import "testing"

// Queries whose phase order runs through label_replace's sample expression, approx_topk's build, vector(), and a
// vector operation's number parameter, with Loki's answers from the Loki oracle (test/oracle/loki check, Loki 3.7.9,
// recorded 2026-11-16): an action in label_replace or before it answers ahead of a later syntax error, a build
// error inside label_replace answers after the tree checks, and a number that is the whole expression is no
// parameter.
//
// conformance: semantics/loki-error-phase-order
func TestErrorPhaseOrderAcrossCalls(t *testing.T) {
	for _, c := range []struct{ q, loki string }{
		{`count_over_time({a=~"("}[5m]) + label_replace(rate({a="b"}[5m]), "d", "$1", "a", "(.*)") )`, "parse error : error parsing regexp: missing closing ): `(`"},
		{`rate({a="b"}[5m]) + label_replace(sum(rate({a="b"} | pattern "<_>" [5m]) + rate({a="b"}[5m])), "d", "$1", "a", "(.*)") )`, `parse error : invalid pattern parser: at least one capture is required`},
		{`label_replace(sum by (a) (rate({a=~"(b"}[5m])), "d", "$1", "a", "(.*)") + 1 )`, "parse error : error parsing regexp: missing closing ): `(b`"},
		{`sum(rate({a="b"}[5m])) or vector(1) + count_over_time({a="b"} | json x="a[" [5m])`, `parse error : stage '| json x="a["' : cannot parse expression [a[]: syntax error: unexpected $end, expecting STRING or INDEX`},
		{`topk(0x1, rate({a="b"}[5m]))`, `parse error : invalid parameter topk(0x1,`},
		{`topk(1e1, rate({a="b"}[5m])) )`, `parse error at line 1, col 30: syntax error: unexpected )`},
		{`sum(1 + 2)`, ``},
		{`sum(1)`, ``},
		{`sum(-1)`, ``},
		{`label_replace(count_over_time({a="b"} | line_format "{{.a" |= "x" [5m]), "d", "$1", "a", "(.*)")`, "parse error : stage '| line_format \"{{.a\"' : invalid line template: template: line:1: unclosed action"},
		{`sum(label_replace(count_over_time({a="b"} | json x="a[" [5m]), "d", "$1", "a", "(.*)")) by (d) / 2`, "parse error : stage '| json x=\"a[\"' : cannot parse expression [a[]: syntax error: unexpected $end, expecting STRING or INDEX"},
	} {
		if got := ValidateLogQL(c.q); got != c.loki {
			t.Errorf("%s:\n  got  %q\n  loki %q", c.q, got, c.loki)
		}
	}
}
