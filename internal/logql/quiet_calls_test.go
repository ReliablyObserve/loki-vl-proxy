package logql

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// callReadQueries are queries with calls kept as text in every place the parser reads them: nested calls, calls
// beside binary operators and inside aggregations, braces, ranges, label filters with parentheses, strings holding
// parentheses, comments, blanks, and calls Loki rejects.
var callReadQueries = []string{
	`label_replace(rate({app="a"}[1m]), "dst", "$1", "src", "(.*)")`,
	`label_replace(label_replace(rate({app="a"}[1m]), "d", "$1", "s", "(.*)"), "d", "$1", "s", "(.*)")`,
	`label_replace(label_replace(label_replace(sum by (app) (rate({app=~"a.*"}[1m])), "a", "", "b", ""), "a", "", "b", ""), "a", "", "b", "")`,
	`sum(label_replace(sum(label_replace(rate({app="a"} |~ "(x)" [1m]), "d", "$1", "s", "(.*)")), "d", "$1", "s", "(.*)"))`,
	`label_replace(label_replace(rate({app="a"}[1m]) + vector(1), "d", "$1", "s", "(.*)") + vector(1), "d", "$1", "s", "(.*)")`,
	`label_replace(vector(1) * label_replace(vector(2), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"} | json | x=~"(a|b)" or (y="1" and z!="2") [1m]), "a", "(", "b", ")"), "a", ")", "b", "(")`,
	`label_replace(label_replace(sum_over_time({app="a"} | logfmt | unwrap duration(d) [5m]), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(count_over_time({app="a"} | line_format "{{.a}} (b)" [1m]), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace( label_replace( rate({app="a"}[1m]) , "a", "b", "c", "d") , "a", "b", "c", "d")`,
	"label_replace(\n\tlabel_replace(rate({app=\"a\"}[1m]), \"a\", \"b\", \"c\", \"d\"),\n\t\"a\", \"b\", \"c\", \"d\")",
	"label_replace(label_replace(rate({app=\"a\"}[1m]), \"a\", \"b\", \"c\", \"d\") # a comment (\n, \"a\", \"b\", \"c\", \"d\")",
	"label_replace(label_replace(rate({app=\"a\"}[1m]) # (((\n, `a`, `b`, `c`, `d`), \"a\", \"b\", \"c\", \"d\")",
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c"), "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "("), "a", "b", "c", "[")`,
	`label_replace(label_replace(rate({app=""}[1m]), "a", "b", "c", "d"), "a", "b", "c", "(")`,
	`label_replace(label_replace(rate({app=~"("}[1m]), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"}[1m]) + vector(x), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", 1), "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "\q", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(sum(rate({app="a"}[1m])) # open, "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(approx_topk(1, label_replace(rate({app="a"}[1m]), "a", "b", "c", "d")), "a", "b", "c", "d")`,
	`approx_topk(2, sum by (a) (label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d"), "a", "b", "c", "d")))`,
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d") and on (a) label_replace(rate({app="b"}[1m]), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`LABEL_REPLACE(Label_Replace(rate({app="a"}[1m]), "a", "b", "c", "d"), "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d")`,
	`label_replace(label_replace(rate({app="a"}[1m]), "a", "b", "c", "d"))) + vector(1)`,
}

// parseNoting parses q as Parse does and returns the log queries the parser noted (noteChecked, and the one it read
// last), in their order.
func parseNoting(q string) (Expr, []*LogQuery, error) {
	p := &parser{sc: newScanner(q)}
	p.advance()
	expr, err := p.parseExpr()
	if err == nil && p.cur.Typ != TokEOF {
		err = p.syntaxErrorAt("")
	}
	noted := append([]*LogQuery(nil), p.checkedLogQueries[:min(p.checkedLogQueryCount, len(p.checkedLogQueries))]...)
	noted = append(noted, p.moreCheckedLogQueries...)
	if p.curLogQuery != nil && mayFailWhileParsed(p.curLogQuery) {
		noted = append(noted, p.curLogQuery)
	}
	return expr, noted, err
}

// logQueriesInTextOrder are the log queries of expr Loki builds while it parses (mayFailWhileParsed), in the order
// of the text: what the parser notes, read from the tree.
func logQueriesInTextOrder(expr Expr) []*LogQuery {
	var out []*LogQuery
	var walk func(Expr)
	walk = func(e Expr) {
		switch x := e.(type) {
		case *LogQuery:
			if mayFailWhileParsed(x) {
				out = append(out, x)
			}
		case *RangeAggregation:
			walk(x.Inner)
		case *VectorAggregation:
			walk(x.Inner)
		case *BinOpExpr:
			walk(x.Left)
			walk(x.Right)
		case *OpaqueMetricExpr:
			walk(x.Inner)
		}
	}
	walk(expr)
	return out
}

// callReadsDiffer reports how reading q with the jumps over nested calls (quiet_calls.go), with the typed calls not
// read again and with label_replace's first argument respelled in place differs from reading every call again, or
// "".
func callReadsDiffer(q string) string {
	type reads struct {
		parsed      Expr
		parseErr    string
		validation  string
		spelling    string
		canonical   string
		matchers    string
		logSelector string
	}
	read := func() reads {
		r := reads{validation: ValidateLogQL(q), spelling: CanonicalSpelling(q), canonical: CanonicalizeLogRanges(q),
			matchers: ValidateMatchersQuery(q), logSelector: ValidateLogSelectorQuery(q)}
		var err error
		if r.parsed, err = Parse(q); err != nil {
			r.parseErr = err.Error()
		}
		return r
	}
	with := read()
	parenJumpsOff.Store(true)
	without := read()
	parenJumpsOff.Store(false)
	if !reflect.DeepEqual(with, without) {
		return "the jumps over nested calls change what is read"
	}
	spellPerArgument.Store(true)
	perArgument := CanonicalSpelling(q)
	spellPerArgument.Store(false)
	if perArgument != with.spelling {
		return "label_replace's first argument read in place is respelled otherwise than read apart"
	}
	expr, noted, err := parseNoting(q)
	if err != nil {
		return ""
	}
	if want := logQueriesInTextOrder(expr); !reflect.DeepEqual(noted, want) && (len(noted) > 0 || len(want) > 0) {
		return "the parser noted other log queries than its tree holds, or in another order"
	}
	return typedCallRereadDiffers(expr, q)
}

// typedCallRereadDiffers reports a typed call whose text, read again as the validation read it before it skipped
// typed calls (opaqueFirstArgument, untypedCall), gives another sample expression or an error.
func typedCallRereadDiffers(expr Expr, q string) string {
	switch x := expr.(type) {
	case *RangeAggregation:
		return typedCallRereadDiffers(x.Inner, q)
	case *VectorAggregation:
		return typedCallRereadDiffers(x.Inner, q)
	case *BinOpExpr:
		if msg := typedCallRereadDiffers(x.Left, q); msg != "" {
			return msg
		}
		return typedCallRereadDiffers(x.Right, q)
	case *OpaqueMetricExpr:
		if !x.Typed {
			return ""
		}
		inner, msg := untypedCall(q, x)
		if msg != "" {
			return "a typed call read again has an error: " + msg
		}
		if x.Func == "label_replace" && !reflect.DeepEqual(inner, x.Inner) {
			return "a typed call read again has another sample expression"
		}
		return typedCallRereadDiffers(x.Inner, q)
	}
	return ""
}

// The jumps over nested calls, the shared notes of the calls' parsers, the validation's skip of typed calls and the
// spelling of label_replace's first argument in place read every query as reading each call again does.
//
// conformance: semantics/nested-calls-read-once
func TestCallReadsMatchReadingAgain(t *testing.T) {
	queries := append([]string(nil), callReadQueries...)
	for _, shape := range deepCallShapes {
		for _, d := range []int{1, 2, 3, 17} {
			queries = append(queries, deepCall(shape, d))
		}
	}
	for _, q := range queries {
		if msg := callReadsDiffer(q); msg != "" {
			t.Errorf("%q: %s", q, msg)
		}
	}
}

// conformance: semantics/nested-calls-read-once
func FuzzCallReadsMatchReadingAgain(f *testing.F) {
	for _, q := range callReadQueries {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if len(q) > 4096 {
			return
		}
		if msg := callReadsDiffer(q); msg != "" {
			t.Errorf("%q: %s", q, msg)
		}
	})
}

// A deep spaced nesting is respelled at every level, and only there (label_replace's first argument is read where
// it stands, spellLabelReplace).
//
// conformance: semantics/nested-calls-read-once
func TestCanonicalSpellingDeepNesting(t *testing.T) {
	spaced := deepCallShape{"spaced", `sum by (app) ( rate({app="api"}[1m]) )`, `label_replace( %s , "dst", "$1", "src", "(.*)")`}
	want := deepCallShape{"spelled", `sum by (app) (rate({app="api"}[1m]))`, `label_replace(%s , "dst", "$1", "src", "(.*)")`}
	for _, d := range []int{1, 2, 64, deepCallMaxDepth(spaced)} {
		got := CanonicalSpelling(deepCall(spaced, d))
		if w := deepCall(want, d); got != w {
			t.Errorf("depth %d: respelled %.120q..., want %.120q...", d, got, w)
		}
	}
	// A call that is not label_replace's grammar keeps the spelling of the whole query.
	for _, q := range []string{
		`label_replace( label_replace( rate({app="a"}[1m]), "a", "b", "c"), "a", "b", "c", "d")`,
		`label_replace( label_replace( rate({app="a"}[1m]), "a", "b", "c", 1), "a", "b", "c", "d")`,
		`label_replace( approx_topk(1, rate({app="a"}[1m])), "a", "b", "c", "d")`,
	} {
		if got := CanonicalSpelling(q); got != q {
			t.Errorf("CanonicalSpelling(%q) = %q, want it kept", q, got)
		}
	}
	if strings.Contains(CanonicalSpelling(`label_replace( rate({app="a"}[1m]) , "a", "b", "c", "d" )`), `"d" )`) {
		t.Error("the blank before label_replace's closing parenthesis is kept")
	}
}

// callErrorsLoki are Loki's answers, recorded with the Loki parse oracle (test/oracle/loki check), to calls kept as
// text nested to every depth below its input size limit: accepted nestings, and calls Loki rejects (three strings,
// five, a number, an invalid escape or regex, a call a comment leaves open, an error inside or around a nesting, on
// one line and on several). A row names a query or a shape and a depth.
const callErrorsLoki = "testdata/call_errors_loki.jsonl"

// recordedCallShapes are the shapes of callErrorsLoki's rows: deepCallShapes, and the innermost errors written with
// no blank.
func recordedCallShapes() map[string]deepCallShape {
	shapes := map[string]deepCallShape{
		"three-arguments-innermost-compact": {"three-arguments-innermost-compact", `label_replace(rate({app="api"}[1m]),"a","","a")`, `label_replace(%s,"a","","a","")`},
		"bad-regex-innermost-compact":       {"bad-regex-innermost-compact", `label_replace(rate({app="api"}[1m]),"a","","a","(")`, `label_replace(%s,"a","","a","")`},
	}
	for _, s := range deepCallShapes {
		shapes[s.name] = s
	}
	return shapes
}

// The validation answers every recorded row as Loki does: accepted, or rejected with Loki's text, line and column.
//
// conformance: semantics/nested-calls-read-once
func TestCallErrorsMatchRecordedLoki(t *testing.T) {
	data, err := os.ReadFile(filepath.FromSlash(callErrorsLoki))
	if err != nil {
		t.Fatal(err)
	}
	shapes := recordedCallShapes()
	rows := 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var row struct {
			ID, Q, Shape, Error string
			Depth               int
			Accept              bool
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		q := row.Q
		if row.Shape != "" {
			shape, ok := shapes[row.Shape]
			if !ok {
				t.Fatalf("%s: no shape %q", row.ID, row.Shape)
			}
			q = deepCall(shape, row.Depth)
		}
		rows++
		if got := ValidateLogQL(q); got != row.Error || (got == "") != row.Accept {
			t.Errorf("%s: ValidateLogQL = %q, Loki answers %q", row.ID, got, row.Error)
		}
	}
	if rows < 90 {
		t.Fatalf("%d recorded rows, want the 91 recorded", rows)
	}
}
