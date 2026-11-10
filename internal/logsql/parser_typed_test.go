package logsql_test

import (
	"reflect"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func parsePipe(t *testing.T, in string) logsql.Pipe {
	t.Helper()
	q, err := logsql.Parse(in)
	if err != nil {
		t.Fatalf("Parse(%q): %v", in, err)
	}
	if len(q.Pipes) != 1 {
		t.Fatalf("Parse(%q): %d pipes, want 1", in, len(q.Pipes))
	}
	return q.Pipes[0]
}

// The pipes parse into typed nodes, not text: the fields below are what the proxy reads from them.
// conformance: architecture-syntax-package-layering
func TestParsePipesAreTyped(t *testing.T) {
	cond := logsql.FieldAny("level")
	for _, c := range []struct {
		in   string
		want logsql.Pipe
	}{
		{`* | format if (level:*) "<level>" as lvl keep_original_fields`,
			logsql.PipeFormat{Template: "<level>", ResultField: "lvl", If: cond, KeepOriginalFields: true}},
		{`* | format "a\"b" skip_empty_results`, logsql.PipeFormat{Template: `a"b`, SkipEmptyResults: true}},
		{`* | extract if (level:*) "<a>" from b`, logsql.PipeExtract{Pattern: "<a>", From: "b", If: cond}},
		{"* | extract_regexp `(?P<a>\\d+)` skip_empty_results", logsql.PipeExtractRegexp{Pattern: `(?P<a>\d+)`, SkipEmptyResults: true}},
		{`* | replace ("a", "b") at c limit 2`, logsql.PipeReplace{Field: "c", Old: "a", New: "b", Limit: 2}},
		{"* | replace_regexp if (level:*) (`^\\d`, \"0\") at c", logsql.PipeReplaceRegexp{Field: "c", Regex: `^\d`, Replacement: "0", If: cond}},
		{`* | rename a as b, "c.d" as e`, logsql.PipeRename{Pairs: [][2]string{{"a", "b"}, {`"c.d"`, "e"}}}},
		{`* | mv a b`, logsql.PipeRename{Pairs: [][2]string{{"a", "b"}}}},
		{`* | copy a b, c as d`, logsql.PipeCopy{Pairs: [][2]string{{"a", "b"}, {"c", "d"}}}},
		{`* | pack_json fields (a, b) as p`, logsql.PipePackJSON{Fields: []string{"a", "b"}, ResultField: "p"}},
		{`* | pack_logfmt p`, logsql.PipePackLogfmt{ResultField: "p"}},
		{`* | unpack_json if (level:*) from _msg fields (a, "b.c", d*) preserve_keys (e) result_prefix "p_" keep_original_fields`,
			logsql.PipeUnpackJSON{From: "_msg", If: cond, Fields: []string{"a", `"b.c"`, "d*"}, PreserveKeys: []string{"e"}, ResultPrefix: "p_", KeepOriginalFields: true}},
		{`* | unpack_json a`, logsql.PipeUnpackJSON{From: "a"}},
		{`* | unpack_logfmt fields (a) skip_empty_results`, logsql.PipeUnpackLogfmt{Fields: []string{"a"}, SkipEmptyResults: true}},
		{`* | coalesce(a, "b.c") default "d" as e`, logsql.PipeCoalesce{Fields: []string{"a", `"b.c"`}, Default: "d", Result: "e"}},
		{`* | stats by (_time:5m offset 1h, a) count(b) if (level:*) n, sum(c) s`, logsql.PipeStats{
			By: []logsql.GroupKey{{Field: "_time", Bucket: "5m", Offset: "1h"}, {Field: "a"}},
			Funcs: []logsql.StatsFuncAlias{
				{Func: logsql.Count{Fields: []string{"b"}}, Alias: "n", If: cond},
				{Func: logsql.Sum{Field: "c"}, Alias: "s"},
			}}},
		{`* | stats count()`, logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}}}}},
		{`* | count() n`, logsql.PipeStats{Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, Alias: "n"}}}},
		{`* | math a * 2 as b | limit 3`, nil}, // math keeps the pipe after it
	} {
		if c.want == nil {
			q, err := logsql.Parse(c.in)
			if err != nil || len(q.Pipes) != 2 {
				t.Errorf("Parse(%q) = %v, %v: want two pipes", c.in, q, err)
			}
			continue
		}
		if got := parsePipe(t, c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("Parse(%q) pipe = %#v, want %#v", c.in, got, c.want)
		}
	}
}

func parseFilter(t *testing.T, in string) logsql.FilterExpr {
	t.Helper()
	f, err := logsql.ParseFilter(in)
	if err != nil {
		t.Fatalf("ParseFilter(%q): %v", in, err)
	}
	return f
}

// Filters parse into the nodes the translator builds: negation is Exclude or NotExpr, filters written one after
// another are AllOf, a quoted field name stays as written.
// conformance: architecture-syntax-package-layering
func TestParseFiltersAreTyped(t *testing.T) {
	ff := func(field string, op logsql.FieldOp, value string) logsql.FieldFilter {
		return logsql.FieldFilter{Field: field, Op: op, Value: value}
	}
	for _, c := range []struct {
		in   string
		want logsql.FilterExpr
	}{
		{`"a.b":="c"`, ff(`"a.b"`, logsql.FieldOpExact, "c")},
		{"`a.b`:*", ff("`a.b`", logsql.FieldOpAny, "")},
		{`-a:*`, logsql.Exclude{Expr: ff("a", logsql.FieldOpAny, "")}},
		{`-(a:* OR b:*)`, logsql.Exclude{Expr: logsql.OrExpr{Left: ff("a", logsql.FieldOpAny, ""), Right: ff("b", logsql.FieldOpAny, "")}}},
		{`!a`, logsql.NotExpr{Expr: logsql.Word{Value: "a"}}},
		{`a:!""`, ff("a", logsql.FieldOpNotExact, "")},
		{`a:!"b"`, ff("a", logsql.FieldOpNotExact, "b")},
		{`a:!~"b"`, logsql.NotExpr{Expr: ff("a", logsql.FieldOpRegexp, "b")}},
		{`a:b`, ff("a", logsql.FieldOpPhrase, "b")},
		{`a:"b c"`, ff("a", logsql.FieldOpPhrase, "b c")},
		{`a:"b"*`, ff("a", logsql.FieldOpPrefix, "b")},
		{`a:=b`, ff("a", logsql.FieldOpExact, "b")},
		{`a:>=1.5`, ff("a", logsql.FieldOpGTE, "1.5")},
		{`a:<=1h5m`, ff("a", logsql.FieldOpLTE, "1h5m")},
		{`a:in(b, "c")`, ff("a", logsql.FieldOpIn, `b, "c"`)},
		{`a b`, logsql.AllOf{logsql.Word{Value: "a"}, logsql.Word{Value: "b"}}},
		{`a b OR c`, logsql.OrExpr{Left: logsql.AllOf{logsql.Word{Value: "a"}, logsql.Word{Value: "b"}}, Right: logsql.Word{Value: "c"}}},
		{`a and b`, logsql.AndExpr{Left: logsql.Word{Value: "a"}, Right: logsql.Word{Value: "b"}}},
		{`_msg:(i(a) OR i(b))`, logsql.FieldScope{Field: "_msg", Expr: logsql.OrExpr{
			Left: logsql.CaseInsensitive{Value: "a"}, Right: logsql.CaseInsensitive{Value: "b"}}}},
		{`a:i(b*)`, logsql.AnyCasePrefix{Field: "a", Value: "b"}},
		{`seq("a", "b")`, logsql.Sequence{Parts: []string{"a", "b"}}},
		{`a:contains_all("b")`, logsql.ContainsAll{Field: "a", Parts: []string{"b"}}},
		{`exact("a")`, logsql.FilterFunc{Name: "exact", Args: `"a"`}},
		{`_time:5m offset 1h`, logsql.TimeFilter{Range: "5m offset 1h"}},
		{`_time:[2026-11-07T00:10:00Z, 2026-11-07T01:00:00Z)`, logsql.TimeFilter{Range: "[2026-11-07T00:10:00Z, 2026-11-07T01:00:00Z)"}},
		{`_stream:{a="b"}`, logsql.StreamFilter{Matchers: []logsql.LabelMatcher{{Name: "a", Op: "=", Value: "b"}}}},
		{`ü:"b"`, ff("ü", logsql.FieldOpPhrase, "b")},
	} {
		if got := parseFilter(t, c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("ParseFilter(%q) = %#v, want %#v", c.in, got, c.want)
		}
	}
}

// Whitespace decides how filters group: a * touches its word, and filters written together need a space.
// conformance: architecture-syntax-package-layering
func TestParseFilterAdjacency(t *testing.T) {
	if got := parseFilter(t, `a:* -(b:c)`); !reflect.DeepEqual(got, logsql.AllOf{
		logsql.FieldFilter{Field: "a", Op: logsql.FieldOpAny},
		logsql.Exclude{Expr: logsql.FieldFilter{Field: "b", Op: logsql.FieldOpPhrase, Value: "c"}},
	}) {
		t.Errorf("a:* -(b:c) = %#v", got)
	}
	if got := parseFilter(t, `a:*b*`); !reflect.DeepEqual(got, logsql.FieldFilter{Field: "a", Op: logsql.FieldOpSubstring, Value: "b"}) {
		t.Errorf("a:*b* = %#v", got)
	}
	for _, in := range []string{`a"b"`, `"a"b`, `(a)b`, `(a)(b)`, `{a="b"}c`, `a(b)`, `a!b`} {
		if f, err := logsql.ParseFilter(in); err == nil {
			t.Errorf("ParseFilter(%q) = %q, want an error: filters need whitespace between them", in, f.String())
		}
	}
}

// Printing a node and reading it back gives the node, for the nodes the translator builds.
// conformance: architecture-syntax-package-layering
func TestBuiltPipesReadBack(t *testing.T) {
	cond := logsql.Or(logsql.FieldAny("a"), logsql.Exclude{Expr: logsql.FieldExact("b", "c d")})
	for _, p := range []logsql.Pipe{
		logsql.PipeFormat{Template: "<a>\n\"q\" \\", ResultField: "r", If: cond, SkipEmptyResults: true},
		logsql.PipeExtract{Pattern: "<a> <b>", From: "_msg", If: cond},
		logsql.PipeExtractRegexp{Pattern: "(?P<a>`x`)", KeepOriginalFields: true},
		logsql.PipeReplace{Field: "f", Old: "a\"b", New: "c\\d", If: cond, Limit: 3},
		logsql.PipeReplaceRegexp{Field: "f", Regex: `[\p{L}]+`, Replacement: "$1", If: cond},
		logsql.PipeUnpackJSON{From: "m", If: cond, Fields: []string{"a", `"b c"`}, ResultPrefix: `p"`, SkipEmptyResults: true},
		logsql.PipeUnpackLogfmt{Fields: []string{"a"}, KeepOriginalFields: true},
		logsql.PipeStats{By: []logsql.GroupKey{{Field: "_time", Bucket: "1m", Offset: "5s"}}, Funcs: []logsql.StatsFuncAlias{{Func: logsql.Count{}, If: cond, Alias: "n"}}},
		logsql.PipePackJSON{ResultField: "p"},
		logsql.PipeCoalesce{Fields: []string{"a", `"b.c"`}, Default: "d e", Result: "r"},
	} {
		s := logsql.NewQuery(logsql.Wildcard{}, p).String()
		got := parsePipe(t, s)
		if !reflect.DeepEqual(got, p) {
			t.Errorf("%s reads back as %#v, want %#v", s, got, p)
		}
	}
}

// Printing puts parentheses where the filters bind more loosely than what they sit in.
// conformance: architecture-syntax-package-layering
func TestFilterPrinterParenthesises(t *testing.T) {
	a, b, c := logsql.Word{Value: "a"}, logsql.Word{Value: "b"}, logsql.Word{Value: "c"}
	or := logsql.OrExpr{Left: b, Right: c}
	for _, tc := range []struct {
		f    logsql.FilterExpr
		want string
	}{
		{logsql.AllOf{a, or}, "a (b OR c)"},
		{logsql.AndExpr{Left: a, Right: logsql.AnyOf{b, c}}, "a AND (b OR c)"},
		{logsql.NotExpr{Expr: logsql.AllOf{a, b}}, "NOT (a b)"},
		{logsql.Exclude{Expr: or}, "-(b OR c)"},
		{logsql.Exclude{Expr: logsql.NotExpr{Expr: a}}, "-(NOT a)"},
		{logsql.OrExpr{Left: logsql.AllOf{a, b}, Right: c}, "a b OR c"},
		{logsql.Word{Value: "offset"}, `"offset"`},
		{logsql.Prefix{Value: "a b"}, `"a b"*`},
		{logsql.FieldFilter{Field: "f", Op: logsql.FieldOpPhrase, Value: "+"}, `f:"+"`},
		{logsql.FieldFilter{Field: "f", Op: logsql.FieldOpExact, Value: "a\nb"}, `f:="a\nb"`},
	} {
		if got := tc.f.String(); got != tc.want {
			t.Errorf("%#v prints %q, want %q", tc.f, got, tc.want)
		}
		if f, err := logsql.ParseFilter(tc.want); err != nil {
			t.Errorf("ParseFilter(%q): %v", tc.want, err)
		} else if f.String() != tc.want {
			t.Errorf("%q prints again as %q", tc.want, f.String())
		}
	}
}

// conformance: architecture-syntax-package-layering
func TestScannerSpaceStringsAndKeywords(t *testing.T) {
	sc := logsql.NewScanner(`a  "b" 'c\'d' ! and Or NoT # comment` + "\n" + `é`)
	var kinds []logsql.TokType
	var space []bool
	var vals []string
	for tok := sc.Next(); tok.Typ != logsql.TokEOF; tok = sc.Next() {
		kinds = append(kinds, tok.Typ)
		space = append(space, sc.SpaceBefore())
		vals = append(vals, tok.Val)
	}
	wantKinds := []logsql.TokType{logsql.TokIdent, logsql.TokString, logsql.TokString, logsql.TokBang, logsql.TokAnd, logsql.TokOr, logsql.TokNot, logsql.TokIdent}
	if !reflect.DeepEqual(kinds, wantKinds) {
		t.Errorf("token kinds = %v, want %v", kinds, wantKinds)
	}
	if vals[2] != "c'd" || vals[7] != "é" {
		t.Errorf("token values = %q", vals)
	}
	if !space[1] || !space[7] {
		t.Errorf("space before = %v", space)
	}
	for _, bad := range []string{`"abc`, `'abc`, "`abc", `"a\qb"`, "\"a\nb\""} {
		if tok := logsql.NewScanner(bad).Next(); tok.Typ != logsql.TokError {
			t.Errorf("Next(%q) = %v, want an error token", bad, tok)
		}
	}
	sc = logsql.NewScanner(`"a.b":x`)
	sc.Next()
	if sc.Raw() != `"a.b"` {
		t.Errorf("Raw() = %q, want the quoted source", sc.Raw())
	}
}

// AnyOfLower is AnyOf with the lowercase operator: it prints and nests alike, and parses to the same filter.
func TestAnyOfLowerIsAnyOfInLowercase(t *testing.T) {
	a, b := logsql.Word{Value: "a"}, logsql.Word{Value: "b"}
	lower, upper := logsql.AnyOfLower{logsql.Group{Expr: a}, logsql.Group{Expr: b}}, logsql.AnyOf{logsql.Group{Expr: a}, logsql.Group{Expr: b}}
	if got, want := lower.String(), "(a) or (b)"; got != want {
		t.Fatalf("AnyOfLower = %q, want %q", got, want)
	}
	if got, want := (logsql.AndExpr{Left: a, Right: lower}).String(), "a AND ((a) or (b))"; got != want {
		t.Fatalf("nested = %q, want %q", got, want)
	}
	pl, err := logsql.Parse(lower.String())
	if err != nil {
		t.Fatal(err)
	}
	pu, err := logsql.Parse(upper.String())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pl, pu) {
		t.Fatalf("`or` and `OR` parse differently: %#v vs %#v", pl, pu)
	}
}
