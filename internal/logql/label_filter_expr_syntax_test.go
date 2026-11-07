package logql

import (
	"strings"
	"testing"
)

// shapeOf renders the operator tree with its filters, `,` for and (the way Loki
// prints it) and "or" for or, one pair of parentheses per binary node.
func shapeOf(e *LabelFilterExpr) string {
	switch e.Op {
	case LabelFilterAnd:
		return "(" + shapeOf(e.Left) + " , " + shapeOf(e.Right) + ")"
	case LabelFilterOr:
		return "(" + shapeOf(e.Left) + " or " + shapeOf(e.Right) + ")"
	}
	return e.Leaf
}

// conformance: semantics/label-filter-expression-syntax
func TestParseLabelFilterExpr_Precedence(t *testing.T) {
	// Every expectation was read from Loki v3.7.9 (syntax.ParseExpr(...).String()):
	// AND binds tighter than OR; COMMA and juxtaposition carry no precedence, so
	// their right operand runs to the end of the group.
	cases := []struct{ in, want string }{
		{`a="1"`, `a="1"`},
		{`((a="1"))`, `a="1"`},
		{`foo>=5,bar<25ms`, `(foo>=5 , bar<25ms)`},
		{`(status>=500 or duration_ms>1000), method="GET"`, `((status>=500 or duration_ms>1000) , method="GET")`},
		{`(status_code < 500 and status_code > 200) or latency >= 250ms`, `((status_code < 500 , status_code > 200) or latency >= 250ms)`},
		{`a="1" or b="2" and c="3"`, `(a="1" or (b="2" , c="3"))`},
		{`a="1" and b="2" or c="3"`, `((a="1" , b="2") or c="3")`},
		{`a="1", b="2" or c="3"`, `(a="1" , (b="2" or c="3"))`},
		{`a="1" or b="2", c="3"`, `(a="1" or (b="2" , c="3"))`},
		{`a="1" b="2" or c="3"`, `(a="1" , (b="2" or c="3"))`},
		{`a="1" or b="2" c="3"`, `(a="1" or (b="2" , c="3"))`},
		{`a="1" b="2" or c="3" d="4"`, `(a="1" , (b="2" or (c="3" , d="4")))`},
		{`a="1" or b="2" or c="3" and d="4"`, `((a="1" or b="2") or (c="3" , d="4"))`},
		{`a="1" and b="2" and c="3" or d="4"`, `(((a="1" , b="2") , c="3") or d="4")`},
		{`a="1", b="2", c="3" or d="4", e="5"`, `(a="1" , (b="2" , (c="3" or (d="4" , e="5"))))`},
		{`a>=5 and b<=10, c=~"x.*" or d!~"y"`, `(a>=5 , (b<=10 , (c=~"x.*" or d!~"y")))`},
		{`(a="1" b="2") c="3"`, `((a="1" , b="2") , c="3")`},
		{`a="1" (b="2" or c="3") d="4"`, `(a="1" , ((b="2" or c="3") , d="4"))`},
		{`A="1" AND B="2" Or C="3"`, `((A="1" , B="2") or C="3")`},
		{`a="x,y", b="(z)", c="p and q"`, `(a="x,y" , (b="(z)" , c="p and q"))`},
		{"a=`x\\d`, b!=`y`", "(a=`x\\d` , b!=`y`)"},
		{`a=ip("1.2.3.4"), b!=ip("10.0.0.0/8")`, `(a=ip("1.2.3.4") , b!=ip("10.0.0.0/8"))`},
		{`a=ip ("1.2.3.4") b="2"`, `(a=ip ("1.2.3.4") , b="2")`},
		{`a>-5 b<.5 c==1e3 d>10KB e<=1h5m`, `(a>-5 , (b<.5 , (c==1e3 , (d>10KB , e<=1h5m))))`},
		{`a!="" or b=~""`, `(a!="" or b=~"")`},
	}
	for _, tc := range cases {
		got, err := ParseLabelFilterExpr(tc.in)
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if s := shapeOf(got); s != tc.want {
			t.Errorf("%s\n  got  %s\n  want %s", tc.in, s, tc.want)
		}
		// The canonical rendering reads back as the same tree.
		again, err := ParseLabelFilterExpr(got.String())
		if err != nil || again.String() != got.String() {
			t.Errorf("%s: String() %q reads back as %v / %v", tc.in, got.String(), again, err)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestParseLabelFilterExpr_SyntaxErrors(t *testing.T) {
	// Offsets, token names and expected-token lists are Loki v3.7.9's, minus the
	// query prefix.
	cases := []struct {
		in        string
		offset    int
		token     string
		expecting string
	}{
		{`(`, 1, "$end", "IDENTIFIER or ("},
		{`()`, 1, ")", "IDENTIFIER or ("},
		{`( )`, 2, ")", "IDENTIFIER or ("},
		{`(,)`, 1, ",", "IDENTIFIER or ("},
		{`(a="1"`, 6, "$end", ""},
		{`((a="1")`, 8, "$end", ""},
		{`(a="1"))`, 7, ")", ""},
		{`a="1")`, 5, ")", ""},
		{`a="1" or`, 8, "$end", "IDENTIFIER or ("},
		{`a="1" and`, 9, "$end", "IDENTIFIER or ("},
		{`a="1",`, 6, "$end", "IDENTIFIER or ("},
		{`(a="1" or )`, 10, ")", "IDENTIFIER or ("},
		{`(a="1" ,)`, 8, ")", "IDENTIFIER or ("},
		{`a="1" , , b="2"`, 8, ",", "IDENTIFIER or ("},
		{`a="1" or or b="2"`, 9, "or", "IDENTIFIER or ("},
		{`a="1" and and b="2"`, 10, "and", "IDENTIFIER or ("},
		{`and a="1"`, 0, "and", ""},
		{`or a="1"`, 0, "or", ""},
		{`, a="1"`, 0, ",", ""},
		{`)`, 0, ")", ""},
		{`(and a="1")`, 1, "and", "IDENTIFIER or ("},
		{`a`, 1, "$end", ""},
		{`a =`, 3, "$end", ""},
		{`a > `, 4, "$end", ""},
		{`a > )`, 4, ")", ""},
		{`a "x"`, 2, "STRING", ""},
		{`a ( `, 2, "(", ""},
		{`a , b="1"`, 2, ",", ""},
		{`a="1" b`, 7, "$end", ""},
		{`a="1" b "x"`, 8, "STRING", ""},
		{`a = "x" = `, 8, "=", ""},
		{`a = "x" "y"`, 8, "STRING", ""},
		{`a = "x" 5`, 8, "NUMBER", ""},
		{`a = "x" 5s`, 8, "DURATION", ""},
		{`a = "x" 5KB`, 8, "BYTES", ""},
		{`(a = "x" 5)`, 9, "NUMBER", ""},
		{`a !~ )`, 5, ")", "STRING"},
		{`5`, 0, "NUMBER", ""},
		{`"x"`, 0, "STRING", ""},
		{`= "x"`, 0, "=", ""},
		{`a="1" =~ "x"`, 6, "=~", ""},
		{``, 0, "$end", ""},
	}
	for _, tc := range cases {
		_, err := ParseLabelFilterExpr(tc.in)
		if err == nil {
			t.Errorf("%q: parsed, want error at %d: unexpected %s", tc.in, tc.offset, tc.token)
			continue
		}
		if err.Offset != tc.offset || err.Token != tc.token || err.Expecting != tc.expecting {
			t.Errorf("%q: got offset %d unexpected %s expecting %q; want offset %d unexpected %s expecting %q",
				tc.in, err.Offset, err.Token, err.Expecting, tc.offset, tc.token, tc.expecting)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestParseLabelFilterExpr_ValueKinds(t *testing.T) {
	// The value of a filter has the kind Loki's grammar wants for its operator,
	// alone or in a comma-joined or grouped expression. Offsets are in the text
	// after the "z=\"1\", " that makes it one.
	const prefix = `z="1", `
	cases := []struct {
		in        string
		offset    int
		token     string
		expecting string
	}{
		{`a =~ 5`, 5, "NUMBER", "STRING"},
		{`a !~ )`, 5, ")", "STRING"},
		{`a > "5"`, 4, "STRING", ""},
		{`a > b`, 4, "IDENTIFIER", ""},
		{`a == "x"`, 5, "STRING", ""},
		{`a ! 5`, 2, "IDENTIFIER", ""},
		{`a > ip("1.2.3.4")`, 4, "ip", ""},
		{`a = ip`, 4, "IDENTIFIER", ""},
		{`a = ip("1.2.3.4"`, 16, "$end", ")"},
		{`a = ip(`, 7, "$end", "STRING"},
		{`a = ip(5)`, 7, "NUMBER", "STRING"},
		{`a = true`, 4, "IDENTIFIER", ""},
		{`a = b c = "d"`, 4, "IDENTIFIER", ""},
	}
	for _, tc := range cases {
		texts := []string{prefix + tc.in}
		if !strings.Contains(tc.in, "ip(") { // an unclosed ip( takes the group's ")"
			texts = append(texts, "("+tc.in+") or "+prefix+`b="2"`)
		}
		for _, text := range texts {
			_, err := ParseLabelFilterExpr(text)
			if err == nil {
				t.Errorf("%q: parsed, want unexpected %s", text, tc.token)
				continue
			}
			want := tc.offset + len(prefix)
			if text[0] == '(' {
				want = tc.offset + 1
			}
			if err.Offset != want || err.Token != tc.token || err.Expecting != tc.expecting {
				t.Errorf("%q: got offset %d unexpected %s expecting %q; want offset %d unexpected %s expecting %q",
					text, err.Offset, err.Token, err.Expecting, want, tc.token, tc.expecting)
			}
		}
	}
	// A single filter is checked the same way, at its own offsets.
	for _, tc := range cases {
		_, err := ParseLabelFilterExpr(tc.in)
		if err == nil || err.Offset != tc.offset || err.Token != tc.token || err.Expecting != tc.expecting {
			t.Errorf("single filter %q: got %+v, want offset %d unexpected %s expecting %q", tc.in, err, tc.offset, tc.token, tc.expecting)
		}
	}
	// What Loki accepts stays accepted.
	for _, text := range []string{`a =~ ""`, `a > 5`, `a >= 1.5`, `a < 250ms`, `a <= 10KB`, `a == 5`, `a = 5`, `a != 5s`, `a = "5"`, `a = -5`, `a = ip("10.0.0.0/8")`, `((a = "x"))`} {
		if _, err := ParseLabelFilterExpr(text); err != nil {
			t.Errorf("%q: %v", text, err)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestParseLabelFilterExpr_UnterminatedLiteral(t *testing.T) {
	// Loki's lexer reports a string without its closing quote before any parse
	// error, at the opening quote. (A fuzz finding: `A=""A="` rendered as
	// `(A="" and A=")`, whose quote swallowed the parenthesis.)
	for _, tc := range []struct {
		in     string
		offset int
	}{
		{`A=""A="`, 6},
		{`a="1`, 2},
		{"a=`x", 2},
		{`a="x\"`, 2},
	} {
		_, err := ParseLabelFilterExpr(tc.in)
		if err == nil || err.Message != "literal not terminated" || err.Offset != tc.offset {
			t.Errorf("%q: got %+v, want literal not terminated at %d", tc.in, err, tc.offset)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestParseLabelFilterExpr_BoundedNesting(t *testing.T) {
	// A query of nothing but parentheses, or of thousands of comma-joined
	// filters, must not grow the stack with its size.
	deep := strings.Repeat("(", 5000) + `a="1"` + strings.Repeat(")", 5000)
	if _, err := ParseLabelFilterExpr(deep); err == nil || err.Token != "(" {
		t.Errorf("5000 nested groups: got %+v, want a parenthesis rejected", err)
	}
	chain := strings.Repeat(`a="1", `, 5000) + `a="1"`
	if _, err := ParseLabelFilterExpr(chain); err == nil || err.Token != "IDENTIFIER" {
		t.Errorf("5000 comma-joined filters: got %+v, want one rejected", err)
	}
	for _, ok := range []string{
		strings.Repeat("(", 200) + `a="1" or b="2"` + strings.Repeat(")", 200),
		strings.Repeat(`a="1", `, 500) + `a="1"`,
		strings.Repeat(`a="1" and `, 5000) + `a="1"`, // a loop, not recursion
	} {
		if _, err := ParseLabelFilterExpr(ok); err != nil {
			t.Errorf("%.30s...: %v", ok, err)
		}
	}
}
