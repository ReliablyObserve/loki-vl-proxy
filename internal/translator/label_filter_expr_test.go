package translator

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// shapeOf renders the operator tree with its filters, `,` for and (the way Loki
// prints it) and "or" for or, one pair of parentheses per binary node.
func shapeOf(e *logql.LabelFilterExpr) string {
	switch e.Op {
	case logql.LabelFilterAnd:
		return "(" + shapeOf(e.Left) + " , " + shapeOf(e.Right) + ")"
	case logql.LabelFilterOr:
		return "(" + shapeOf(e.Left) + " or " + shapeOf(e.Right) + ")"
	}
	return e.Leaf
}

// conformance: semantics/label-filter-expression-syntax
func TestExtractPipelineStage_LineFilterAfterCompleteStage(t *testing.T) {
	for _, tc := range []struct{ in, stage, rest string }{
		{`(a="1" or b="2") != "x"`, `(a="1" or b="2")`, `!= "x"`},
		{`a="1", b=5 !~ "x"`, `a="1", b=5`, `!~ "x"`},
		{`json != "x"`, `json`, `!= "x"`},
		// A label filter operator keeps its stage.
		{`a != "x"`, `a != "x"`, ``},
		{`a="1", b!="2"`, `a="1", b!="2"`, ``},
		{`(a="1" or b!~"2")`, `(a="1" or b!~"2")`, ``},
	} {
		if stage, rest := extractPipelineStage(tc.in); stage != tc.stage || rest != tc.rest {
			t.Errorf("%s: got %q + %q, want %q + %q", tc.in, stage, rest, tc.stage, tc.rest)
		}
	}
	// Many operators after a prefix that can never complete are cheap.
	long := `x y ` + strings.Repeat(`a!="1" `, 20000)
	if stage, rest := extractPipelineStage(long); rest != "" || stage == "" {
		t.Errorf("a stage with junk before its operators stays whole, got %d + %d bytes", len(stage), len(rest))
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestMayBeCompound_NeverMissesACompoundExpression(t *testing.T) {
	// A false answer must be exact: every expression with more than one filter
	// is flagged, and the plain single filters are not.
	r := rand.New(rand.NewSource(3))
	for i := 0; i < 20000; i++ {
		text := renderTokens(r, randomTokens(r, true))
		if expr, err := logql.ParseLabelFilterExpr(text); err != nil {
			t.Fatalf("%q: %v", text, err)
		} else if !expr.IsLeaf() && !mayBeCompound(text) {
			t.Fatalf("%q holds several filters but is not flagged", text)
		}
	}
	for _, leaf := range append([]string{`a=ip("1.2.3.4")`, `a != IP("10.0.0.0/8")`}, refLeaves...) {
		if mayBeCompound(leaf) {
			t.Errorf("%q is one filter", leaf)
		}
	}
	for _, text := range []string{`a="1"b="2"`, `a="1"and b="2"`, `a="1" OR b="2"`, `a=1,b=2`, `(a="1")`, `a="1" b=2`, `a=~"x" b!="y"`, `a=ip("1.2.3.4"), b=1`, `(a=ip("1.2.3.4"))`, `a=ip("1.2.3.4") b=1`, `xip("1.2.3.4")`} {
		if !mayBeCompound(text) {
			t.Errorf("%q holds several filters but is not flagged", text)
		}
	}
	for _, text := range []string{`a="x and y"`, `a="x, (y)"`, "a=`or`", `andy="1"`, `order>=5`, `a!~"or"`} {
		if mayBeCompound(text) {
			t.Errorf("%q is one filter", text)
		}
	}
}

// translateStage translates one `| <stage>` after a json parser.
func translateStage(t *testing.T, stage string) string {
	t.Helper()
	out, err := TranslateLogQL(`{app="a"} | json | ` + stage)
	if err != nil {
		t.Fatalf("%s: %v", stage, err)
	}
	return strings.TrimPrefix(out, `app:="a" | unpack_json `)
}

// conformance: semantics/label-filter-expression-syntax
func TestTranslateLabelFilterExpression_LogsQL(t *testing.T) {
	cases := []struct{ stage, want string }{
		// The registry request: a group, then a comma.
		{`(status>=500 or duration_ms>1000), method="GET"`, `| filter ((status:>=500 or (duration_ms:>1000 OR "duration.ms":>1000 OR "duration-ms":>1000)) and method:="GET")`},
		{`foo>=5,bar<25ms`, `| filter (foo:>=5 and bar:<25ms)`},
		// and/or/comma precedence is explicit in the output.
		{`a="1" or b="2" and c="3"`, `| filter (a:="1" or (b:="2" and c:="3"))`},
		{`a="1" and b="2" or c="3"`, `| filter ((a:="1" and b:="2") or c:="3")`},
		{`a="1", b="2" or c="3"`, `| filter (a:="1" and (b:="2" or c:="3"))`},
		{`a="1" or b="2", c="3"`, `| filter (a:="1" or (b:="2" and c:="3"))`},
		{`a="1" b="2" or c="3" d="4"`, `| filter (a:="1" and (b:="2" or (c:="3" and d:="4")))`},
		{`a="1" or b="2" or c="3"`, `| filter (a:="1" or b:="2" or c:="3")`},
		{`((a="1" or b="2") and (c="3" or d="4")) or e="5"`, `| filter (((a:="1" or b:="2") and (c:="3" or d:="4")) or e:="5")`},
		// Matchers inside a group.
		{`(a!="1", b=~"x.*", c!~"y")`, `| filter (-a:="1" and b:~"x" and -c:="y")`},
		{`a!="1", b!="2"`, `| filter (-a:="1" and -b:="2")`},
		{`a="x,y", b="(z)", c="p and q"`, `| filter (a:="x,y" and b:="(z)" and c:="p and q")`},
		// Numbers, durations and bytes mix with strings.
		{`(status>=500 and latency>250ms) or size>10KB, ok="1"`, `| filter ((status:>=500 and latency:>250ms) or (size:>10KB and ok:="1"))`},
		// A parenthesized single filter is that filter.
		{`((a="1"))`, `| filter a:="1"`},
		// A later stage after the group.
		{`(a="1" or b="2") | c="3"`, `| filter (a:="1" or b:="2") | filter c:="3"`},
		// Two groups are two filters; neither replaces the other.
		{`(a="1" or b="2") | (a="3" or c="4")`, `| filter (a:="1" or b:="2") | filter (a:="3" or c:="4")`},
		{`a="1" | a="2", b="3"`, `| filter a:="1" | filter (a:="2" and b:="3")`},
		// A line filter may follow a group.
		{`(a="1" or b="2") != "x"`, `| filter (a:="1" or b:="2") NOT ~"x"`},
		{`(a="1" or b="2") |= "x"`, `| filter (a:="1" or b:="2") ~"x"`},
		{`a="1", b!="2" !~ "x"`, `| filter (a:="1" and -b:="2") NOT ~"x"`},
	}
	for _, tc := range cases {
		if got := translateStage(t, tc.stage); got != tc.want {
			t.Errorf("%s\n  got  %s\n  want %s", tc.stage, got, tc.want)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestTranslateLabelFilterExpression_PipelineContexts(t *testing.T) {
	cases := []struct{ query, want string }{
		// No parser before the group: the filter ANDs with the selector.
		{`{app="a"} | (b="1" or c="2"), d="3"`, `app:="a" ((b:="1" or c:="2") and d:="3")`},
		// After a pipe stage the filter needs its own pipe.
		{`{app="a"} | line_format "x" | (b="1" or c="2"), d="3"`, `app:="a" | format "x" | filter ((b:="1" or c:="2") and d:="3")`},
		// The alias of a json extraction is replaced per filter.
		{`{app="a"} | json http="status" | (http>=500 or x>1), method="GET"`, `app:="a" | unpack_json | copy "status" as http | filter ((status:>=500 or x:>1) and method:="GET")`},
		// detected_level reads the level field.
		{`{app="a"} | json | detected_level="error" or (level="warn", x>1)`, `app:="a" | unpack_json | filter (level:="error" or (level:="warn" and x:>1))`},
		// Metric queries aggregate in VictoriaLogs.
		{`sum by (a) (count_over_time({app="a"} | json | (status>=500 or x>1), method="GET" [5m]))`, `app:="a" | unpack_json | filter ((status:>=500 or x:>1) and method:="GET") | stats by (a) count()`},
	}
	for _, tc := range cases {
		out, err := TranslateLogQL(tc.query)
		if err != nil {
			t.Errorf("%s: %v", tc.query, err)
			continue
		}
		if out != tc.want {
			t.Errorf("%s\n  got  %s\n  want %s", tc.query, out, tc.want)
		}
	}

	// without/by keep the grouped filter in front of | stats: the aggregation
	// stays in VictoriaLogs, no row leaves it.
	for _, query := range []string{
		`sum without (a) (rate({app="a"} | json | (status>=500 or x>1), method="GET" [5m]))`,
		`sum by (a) (rate({app="a"} | json | (status>=500 or x>1), method="GET" | unwrap x [5m]))`,
		`sum by (a) (bytes_over_time({app="a"} | logfmt | (status>=500 or x>1) and y="1" [1m]))`,
	} {
		out, err := TranslateLogQL(query)
		if err != nil {
			t.Errorf("%s: %v", query, err)
			continue
		}
		filter := strings.Index(out, "| filter ((")
		stats := strings.Index(out, "| stats")
		if filter < 0 || stats < filter {
			t.Errorf("%s: the grouped filter must precede | stats: %s", query, out)
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestTranslateLabelFilterExpression_ServiceNameLeafStaysOneOperand(t *testing.T) {
	out := translateStage(t, `service_name="x", y>1`)
	if !strings.HasPrefix(out, "| filter ((service_name:=") || !strings.HasSuffix(out, `) and y:>1)`) {
		t.Errorf("the service_name alternation must be one parenthesized operand: %s", out)
	}
}

// ---------------------------------------------------------------------------
// Differential tests.
//
// refTok is one token of a label filter expression: a filter (L), a connector
// (& and, | or, ',' comma), or a parenthesis.
type refTok struct {
	kind byte
	text string
}

// refParse is a second, independent implementation of Loki's grammar: a
// shift-reduce parser that resolves the grammar's conflicts the way goyacc does
// for syntax.y. A reduction `lf OP lf` happens when the lookahead has a lower or
// equal precedence than the rule (AND 2, OR 1) and shifts otherwise; a rule
// without a precedence (COMMA, the juxtaposed `lf lf`) and a lookahead without
// one (a filter, "(", COMMA) always shift. It returns false for a sequence the
// grammar rejects.
func refParse(toks []refTok) (*logql.LabelFilterExpr, bool) {
	type item struct {
		tree *logql.LabelFilterExpr
		tok  byte // 0 for a tree
	}
	var stack []item
	prec := func(k byte) int {
		switch k {
		case '&':
			return 2
		case '|':
			return 1
		}
		return 0 // ',', 'J' (juxtaposition), filters and "(" carry none
	}
	reduce := func(la byte) bool {
		n := len(stack)
		if n >= 3 && stack[n-3].tok == '(' && stack[n-2].tree != nil && stack[n-1].tok == ')' {
			stack = append(stack[:n-3], stack[n-2])
			return true
		}
		if n < 3 || stack[n-1].tree == nil || stack[n-3].tree == nil || stack[n-2].tok == 0 || stack[n-2].tok == '(' {
			return false
		}
		op := stack[n-2].tok
		shift := false
		if la != 0 && la != ')' {
			rp, tp := prec(op), prec(la)
			shift = rp == 0 || tp == 0 || tp > rp
		}
		if shift {
			return false
		}
		kind := logql.LabelFilterAnd
		if op == '|' {
			kind = logql.LabelFilterOr
		}
		stack = append(stack[:n-3], item{tree: &logql.LabelFilterExpr{Op: kind, Left: stack[n-3].tree, Right: stack[n-1].tree}})
		return true
	}
	afterOperand := false
	depth := 0
	for i := 0; i <= len(toks); i++ {
		var la byte
		if i < len(toks) {
			la = toks[i].kind
		}
		for reduce(la) {
		}
		switch la {
		case 0:
			if !afterOperand || depth != 0 || len(stack) != 1 {
				return nil, false
			}
			return stack[0].tree, true
		case 'L', '(':
			if afterOperand {
				stack = append(stack, item{tok: 'J'})
			}
			if la == 'L' {
				stack = append(stack, item{tree: &logql.LabelFilterExpr{Leaf: toks[i].text}})
				afterOperand = true
			} else {
				stack = append(stack, item{tok: '('})
				depth++
				afterOperand = false
			}
		case '&', '|', ',':
			if !afterOperand {
				return nil, false
			}
			stack = append(stack, item{tok: la})
			afterOperand = false
		case ')':
			if !afterOperand || depth == 0 {
				return nil, false
			}
			stack = append(stack, item{tok: ')'})
			depth--
			afterOperand = true
		}
	}
	return nil, false
}

var refLeaves = []string{
	`a="1"`, `b!="2"`, `c=~"x.*"`, `d!~"y"`, `e>5`, `f>=1.5`, `g<10`, `h<=3`, `i==4`, `j>250ms`,
	`k<=1h5m`, `l>10KB`, `m!=5`, `n=""`, `s="a, b"`, `t="(x)"`, `u="a and b"`, `v=-5`,
}

// randomTokens draws a token sequence; valid grows a well-formed one, otherwise
// the sequence is random soup that the grammar mostly rejects.
func randomTokens(r *rand.Rand, valid bool) []refTok {
	var out []refTok
	var operand func(depth int)
	operand = func(depth int) {
		if depth > 0 && r.Intn(3) == 0 {
			out = append(out, refTok{kind: '('})
			expr(r, &out, depth-1, operand)
			out = append(out, refTok{kind: ')'})
			return
		}
		out = append(out, refTok{kind: 'L', text: refLeaves[r.Intn(len(refLeaves))]})
	}
	if valid {
		expr(r, &out, 3, operand)
		return out
	}
	kinds := []byte{'L', 'L', 'L', '&', '|', ',', '(', ')'}
	for n := 1 + r.Intn(9); n > 0; n-- {
		k := kinds[r.Intn(len(kinds))]
		tok := refTok{kind: k}
		if k == 'L' {
			tok.text = refLeaves[r.Intn(len(refLeaves))]
		}
		out = append(out, tok)
	}
	return out
}

func expr(r *rand.Rand, out *[]refTok, depth int, operand func(int)) {
	operand(depth)
	for n := r.Intn(4); n > 0; n-- {
		switch r.Intn(5) {
		case 0:
			*out = append(*out, refTok{kind: '&'})
		case 1:
			*out = append(*out, refTok{kind: '|'})
		case 2:
			*out = append(*out, refTok{kind: ','})
		}
		operand(depth)
	}
}

func renderTokens(r *rand.Rand, toks []refTok) string {
	var b strings.Builder
	for i, tok := range toks {
		if i > 0 {
			b.WriteByte(' ')
		}
		switch tok.kind {
		case 'L':
			b.WriteString(tok.text)
		case '&':
			b.WriteString([]string{"and", "AND", "And"}[r.Intn(3)])
		case '|':
			b.WriteString([]string{"or", "OR", "Or"}[r.Intn(3)])
		default:
			b.WriteByte(tok.kind)
		}
	}
	return b.String()
}

// conformance: semantics/label-filter-expression-syntax
func TestLabelFilterExpression_ReferenceParserAgrees(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	var accepted, rejected int
	for i := 0; i < 20000; i++ {
		toks := randomTokens(r, i%2 == 0)
		text := renderTokens(r, toks)
		want, wantOK := refParse(toks)
		got, err := logql.ParseLabelFilterExpr(text)
		if wantOK != (err == nil) {
			t.Fatalf("%q: reference ok=%v, parser error=%v", text, wantOK, err)
		}
		if !wantOK {
			rejected++
			continue
		}
		accepted++
		if shapeOf(got) != shapeOf(want) {
			t.Fatalf("%q\n  parser    %s\n  reference %s", text, shapeOf(got), shapeOf(want))
		}
	}
	if accepted < 5000 || rejected < 3000 {
		t.Fatalf("the draw must cover both outcomes: %d accepted, %d rejected", accepted, rejected)
	}
}

// logsqlBool evaluates the boolean structure of an emitted LogsQL filter with
// LogsQL's own precedence (NOT, then and, then or; a space ANDs). A filter is
// an atom that atoms maps to its truth value; a "-" before something that is not
// a known atom negates it.
func logsqlBool(t *testing.T, src string, atoms map[string]bool) bool {
	t.Helper()
	type tok struct{ kind, text string }
	var toks []tok
	for i := 0; i < len(src); {
		switch src[i] {
		case ' ':
			i++
		case '(', ')':
			toks = append(toks, tok{kind: string(src[i])})
			i++
		default:
			j := i
			for j < len(src) && src[j] != ' ' && src[j] != '(' && src[j] != ')' {
				if src[j] == '"' {
					for j++; src[j] != '"'; j++ {
						if src[j] == '\\' {
							j++
						}
					}
				}
				j++
			}
			word := src[i:j]
			switch strings.ToLower(word) {
			case "and", "or", "not":
				toks = append(toks, tok{kind: strings.ToLower(word)})
			default:
				toks = append(toks, tok{kind: "atom", text: word})
			}
			i = j
		}
	}
	pos := 0
	var parseOr func() bool
	var parseUnary func() bool
	parseUnary = func() bool {
		if pos >= len(toks) {
			t.Fatalf("%s: unexpected end", src)
		}
		tk := toks[pos]
		pos++
		switch tk.kind {
		case "not":
			return !parseUnary()
		case "(":
			v := parseOr()
			if pos >= len(toks) || toks[pos].kind != ")" {
				t.Fatalf("%s: missing )", src)
			}
			pos++
			return v
		case "atom":
			if v, ok := atoms[tk.text]; ok {
				return v
			}
			if inner, ok := strings.CutPrefix(tk.text, "-"); ok {
				if v, ok := atoms[inner]; ok {
					return !v
				}
			}
			t.Fatalf("%s: unknown atom %q", src, tk.text)
		}
		t.Fatalf("%s: unexpected %q", src, tk.kind)
		return false
	}
	parseAnd := func() bool {
		v := parseUnary()
		for pos < len(toks) && (toks[pos].kind == "and" || toks[pos].kind == "atom" || toks[pos].kind == "(" || toks[pos].kind == "not") {
			if toks[pos].kind == "and" {
				pos++
			}
			w := parseUnary() // evaluate even when v is already false
			v = v && w
		}
		return v
	}
	parseOr = func() bool {
		v := parseAnd()
		for pos < len(toks) && toks[pos].kind == "or" {
			pos++
			w := parseAnd()
			v = v || w
		}
		return v
	}
	v := parseOr()
	if pos != len(toks) {
		t.Fatalf("%s: trailing %q", src, toks[pos].kind)
	}
	return v
}

// evalTree evaluates a Loki operator tree under a truth assignment of its
// filters.
func evalTree(e *logql.LabelFilterExpr, truth map[string]bool) bool {
	switch e.Op {
	case logql.LabelFilterAnd:
		return evalTree(e.Left, truth) && evalTree(e.Right, truth)
	case logql.LabelFilterOr:
		return evalTree(e.Left, truth) || evalTree(e.Right, truth)
	}
	return truth[e.Leaf]
}

// conformance: semantics/label-filter-expression-syntax
func TestLabelFilterExpression_EmittedLogsQLEvaluatesLikeLokiTree(t *testing.T) {
	// Each filter translates to one LogsQL atom; assigning the filters random
	// truth values, the emitted expression must answer like the tree Loki's
	// grammar reads from the LogQL text (the reference parser).
	atomOf := map[string]string{}
	for _, leaf := range refLeaves {
		atom, ok := translateSingleLabelFilter(leaf, nil, logsql.Capabilities{})
		if !ok {
			t.Fatalf("leaf %q does not translate", leaf)
		}
		atomOf[leaf] = atom
	}
	r := rand.New(rand.NewSource(11))
	for i := 0; i < 5000; i++ {
		toks := randomTokens(r, true)
		text := renderTokens(r, toks)
		ref, ok := refParse(toks)
		if !ok {
			t.Fatalf("%q: reference rejects a well-formed sequence", text)
		}
		expr, err := logql.ParseLabelFilterExpr(text)
		if err != nil {
			t.Fatalf("%q: %v", text, err)
		}
		emitted, ok := translateLabelFilterExpr(expr, nil, logsql.Capabilities{})
		if !ok {
			t.Fatalf("%q does not translate", text)
		}
		for round := 0; round < 6; round++ {
			truth := map[string]bool{}
			atoms := map[string]bool{}
			for _, leaf := range refLeaves {
				truth[leaf] = r.Intn(2) == 0
				atoms[atomOf[leaf]] = truth[leaf]
			}
			if want, got := evalTree(ref, truth), logsqlBool(t, emitted, atoms); want != got {
				t.Fatalf("%q\n  emitted %s\n  truth %v\n  Loki tree says %v, emitted LogsQL says %v", text, emitted, truth, want, got)
			}
		}
	}
}

// conformance: semantics/label-filter-expression-syntax
func TestLabelFilterExpression_JSONAliasRewrittenPerFilter(t *testing.T) {
	got := rewriteJSONAliasedFilter(`(http>=500 or x>1), http<600`, map[string]string{"http": "status"})
	want := `((status>=500 or x>1) and status<600)`
	if got != want {
		t.Errorf("got %s want %s", got, want)
	}
	if got := rewriteJSONAliasedFilter(`http>=500`, map[string]string{"http": "status"}); got != "status>=500" {
		t.Errorf("a single filter keeps the old rewrite, got %s", got)
	}
}

func FuzzLabelFilterExpression(f *testing.F) {
	for _, seed := range []string{
		`(status>=500 or duration_ms>1000), method="GET"`,
		`foo>=5,bar<25ms`,
		`a="1", b="2" or c="3"`,
		`a="1" or b="2" and c="3" d="4"`,
		`((a="1")) AND (b!~"x")`,
		`a=ip("1.2.3.4") or b!=ip("10.0.0.0/8")`,
		`a="x,y", b="(z)"`,
		`(`, `)`, `()`, `a="1" or`, `a =`, `, a`, `A=""A="`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		expr, err := logql.ParseLabelFilterExpr(text)
		if err != nil {
			if err.Offset < 0 || err.Offset > len(text) || err.Token == "" && err.Message == "" {
				t.Fatalf("%q: error outside the text: %+v", text, err)
			}
			return
		}
		// The canonical rendering reads back as the same tree and is stable.
		again, err := logql.ParseLabelFilterExpr(expr.String())
		if err != nil {
			t.Fatalf("%q renders as %q, which does not parse: %v", text, expr.String(), err)
		}
		if again.String() != expr.String() {
			t.Fatalf("%q renders as %q, which reads back as %q", text, expr.String(), again.String())
		}
		// Translation never panics, whatever the filters hold.
		_, _ = translateLabelFilterExpr(expr, nil, logsql.Capabilities{})
		_, _ = TranslateLogQL(fmt.Sprintf(`{app="a"} | json | %s`, text))
	})
}
