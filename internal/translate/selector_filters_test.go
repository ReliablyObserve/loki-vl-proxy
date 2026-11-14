package translate

import (
	"errors"
	"regexp"
	"regexp/syntax"
	"slices"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// evalRow evaluates a field filter over one row the way VictoriaLogs does: a missing field is the empty value, an
// exact filter compares the whole value and a regexp filter matches anywhere in it.
func evalRow(t *testing.T, f logsql.FilterExpr, row map[string]string) bool {
	t.Helper()
	switch n := f.(type) {
	case logsql.Wildcard:
		return true
	case logsql.FieldFilter:
		v := row[strconvUnquote(n.Field)]
		switch n.Op {
		case logsql.FieldOpExact:
			return v == n.Value
		case logsql.FieldOpNotExact:
			return v != n.Value
		case logsql.FieldOpRegexp:
			return patternRegexp(n.Value).MatchString(v)
		case logsql.FieldOpAny:
			return v != ""
		case logsql.FieldOpIn:
			return false // the empty in() list matches no row
		}
	case logsql.Exclude:
		return !evalRow(t, n.Expr, row)
	case logsql.NotExpr:
		return !evalRow(t, n.Expr, row)
	case logsql.Group:
		return evalRow(t, n.Expr, row)
	case logsql.AllOf:
		for _, e := range n {
			if !evalRow(t, e, row) {
				return false
			}
		}
		return true
	case logsql.AnyOf:
		for _, e := range n {
			if evalRow(t, e, row) {
				return true
			}
		}
		return false
	}
	t.Fatalf("unexpected filter node %T (%s)", f, f)
	return false
}

// strconvUnquote is a field name without the double quotes LogsQL needs around a dotted one.
func strconvUnquote(field string) string { return unquoted(field) }

// A regexp of a stream selector matches the whole value with '.' matching a line break; a label filter's matches the
// whole value with '.' not matching one. The literal and the literal alternation forms accept the same values.
// conformance: semantics/label-filter-expression-syntax
func TestWholeValueRegexpFilterVerdicts(t *testing.T) {
	values := []string{"", "a", "b", "c", "ab", "abc", "ac", "bc", "a\nb", "x\n", "\nx", "a.b", "axb", "abcd", " a", "a|b", "(", "(a|b)c"}
	for _, pattern := range []string{
		KeepAllPattern, NonEmptyPattern, ".*", ".+", "abc", "a|b", "a|b|c", "a.b", ".*abc.*", "[ab]", "ab[cd]", "(a|b)c", "a.", "x.*", "\nx", "(?s).*", "(?s).+", "a\\.b", "(?i)ABC", "ab|", "",
	} {
		for _, dotNL := range []bool{true, false} {
			for _, negate := range []bool{false, true} {
				var anchored string
				if dotNL {
					anchored = "^(?s:" + pattern + ")$"
				} else {
					anchored = "^(?-s:" + pattern + ")$"
				}
				// A label filter's pattern arrives simplified (LokiLabelFilterPattern), so the reference is the
				// whole-value match of the pattern it was given.
				re, err := regexp.Compile(anchored)
				if err != nil {
					continue
				}
				if !dotNL && (pattern == ".*" || pattern == ".+") {
					continue // a label filter's pattern arrives as KeepAllPattern or NonEmptyPattern
				}
				f := WholeValueRegexpFilterExpr("l", pattern, negate, dotNL)
				for _, v := range values {
					want := re.MatchString(v) != negate
					if got := evalRow(t, f, map[string]string{"l": v}); got != want {
						t.Errorf("l%s~%q dotNL=%v on %q = %v, want %v (filter %s)", map[bool]string{false: "=", true: "!"}[negate], pattern, dotNL, v, got, want, f)
					}
				}
			}
		}
	}
	if got := SelectorRegexpFilter("l", "a|b", false).String(); got != `l:~"^(?s:a|b)$"` {
		t.Errorf("a selector keeps an alternation as a regexp: %s", got)
	}
}

// The values a regexp matching a finite set of literals matches.
// conformance: semantics/label-filter-expression-syntax
func TestLiteralSet(t *testing.T) {
	for _, c := range []struct {
		pattern string
		limit   int
		want    []string
	}{
		{"a|b", 16, []string{"a", "b"}},
		{"api|apx", 16, []string{"api", "apx"}}, // Go factors the common prefix into ap[ix]
		{"(a|b)(c|d)", 16, []string{"ac", "ad", "bc", "bd"}},
		{"(ab)", 16, []string{"ab"}},
		{"[a-c]", 16, []string{"a", "b", "c"}},
		{"x[0-2]y", 16, []string{"x0y", "x1y", "x2y"}},
		{"abc", 16, []string{"abc"}},
		{"a|b|c|d", 3, nil},    // over the limit
		{"[a-z]", 16, nil},     // 26 values
		{"(a|b)(c|d)", 3, nil}, // the product is over the limit
		{"a|", 16, nil},        // the empty value is no literal
		{"a.", 16, nil},
		{"a*", 16, nil},
		{"(?i)a", 16, nil},
		{"(?i)[a-b]", 16, nil},
		{"", 16, nil},
	} {
		reg, err := syntax.Parse(c.pattern, syntax.Perl)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := LiteralSet(reg.Simplify(), c.limit)
		if ok != (c.want != nil) {
			t.Errorf("LiteralSet(%q, %d) ok = %v (%v)", c.pattern, c.limit, ok, got)
			continue
		}
		if ok && !slices.Equal(got, c.want) {
			t.Errorf("LiteralSet(%q, %d) = %v, want %v", c.pattern, c.limit, got, c.want)
		}
	}
	// An alternation over MaxExactAlternatives literals is not turned into exact filters.
	var long string
	for i := 0; i <= MaxExactAlternatives; i++ {
		long += string(rune('a'+i)) + "|"
	}
	long += "z"
	if f := WholeValueRegexpFilterExpr("l", long, false, false); evalRow(t, f, map[string]string{"l": "az"}) || !evalRow(t, f, map[string]string{"l": "b"}) {
		t.Errorf("long alternation: %s", f)
	}
}

// A selector's matchers are filters on the stream labels and fields; detected_level is no stream label, so a missing
// label reads as the empty value: a matcher that accepts the empty value keeps every stream, any other keeps none.
// conformance: semantics/label-filter-expression-syntax
func TestStreamSelectorFilterDetectedLevel(t *testing.T) {
	m := func(op logql.MatchOp, v string) []logql.LabelMatcher {
		return []logql.LabelMatcher{{Name: "detected_level", Op: op, Value: v}}
	}
	row := map[string]string{"app": "x"}
	for _, c := range []struct {
		op    logql.MatchOp
		value string
		keeps bool
	}{
		{logql.MatchEq, "info", false},
		{logql.MatchEq, "", true},
		{logql.MatchNeq, "info", true},
		{logql.MatchNeq, "", false},
		{logql.MatchRe, ".*", true},
		{logql.MatchRe, ".+", false},
		{logql.MatchRe, "info|", true},
		{logql.MatchNotRe, ".+", true},
		{logql.MatchNotRe, ".*", false},
	} {
		f, err := StreamSelectorFilter(m(c.op, c.value), nil, nil)
		if err != nil {
			t.Fatalf("%s %q: %v", c.op, c.value, err)
		}
		if got := evalRow(t, f, row); got != c.keeps {
			t.Errorf("{detected_level%s%q} keeps a stream = %v, want %v (%s)", c.op, c.value, got, c.keeps, f)
		}
	}
	// A detected_level stream field is an index label, matched like any other.
	f, err := StreamSelectorFilter(m(logql.MatchEq, "info"), nil, map[string]bool{"detected_level": true})
	if err != nil || f.String() != `{detected_level="info"}` {
		t.Errorf("detected_level as a stream field: %v, %v", f, err)
	}
	// A regexp that does not compile is refused with a parse error.
	_, err = StreamSelectorFilter(m(logql.MatchRe, "("), nil, nil)
	var pe *ParseError
	if !errors.As(err, &pe) || pe.Pos != -1 {
		t.Errorf("invalid regexp: %v", err)
	}
	_, err = SelectorFilter([]SelectorMatcher{{Name: "app", Op: "~", Value: "x"}}, nil, nil)
	if !errors.As(err, &pe) {
		t.Errorf("unknown operator: %v", err)
	}
	if f, err := SelectorFilter(nil, nil, nil); err != nil || f.String() != "*" {
		t.Errorf("an empty selector keeps every row: %v, %v", f, err)
	}
}

// Matchers on declared stream fields form one stream filter, written first; every other matcher is a field filter.
// conformance: semantics/label-filter-expression-syntax
func TestStreamSelectorFilterStreamFields(t *testing.T) {
	streamFields := map[string]bool{"app": true, "env": true, "k8s.pod.name": true}
	labelFn := func(s string) string {
		if s == "pod" {
			return "k8s.pod.name"
		}
		return s
	}
	m := func(n string, op logql.MatchOp, v string) logql.LabelMatcher {
		return logql.LabelMatcher{Name: n, Op: op, Value: v}
	}
	for _, c := range []struct {
		name string
		ms   []logql.LabelMatcher
		want string
	}{
		{"one stream field", []logql.LabelMatcher{m("app", logql.MatchEq, "x")}, `{app="x"}`},
		{"stream fields join in one filter", []logql.LabelMatcher{m("app", logql.MatchEq, "x"), m("env", logql.MatchEq, "p")}, `{app="x", env="p"}`},
		{"the stream filter comes first", []logql.LabelMatcher{m("zone", logql.MatchEq, "a"), m("app", logql.MatchEq, "x")}, `{app="x"} zone:="a"`},
		{"a translated label is a stream field when its field is", []logql.LabelMatcher{m("pod", logql.MatchEq, "p")}, `{pod="p"}`},
		{"not equal is a field filter", []logql.LabelMatcher{m("app", logql.MatchNeq, "x")}, `-app:="x"`},
		{"a regexp is anchored to the whole value", []logql.LabelMatcher{m("app", logql.MatchRe, "x|y")}, `app:~"^(?s:x|y)$"`},
		{"a negated regexp", []logql.LabelMatcher{m("app", logql.MatchNotRe, "x.*")}, `-app:~"^(?s:x.*)$"`},
		{"not empty", []logql.LabelMatcher{m("zone", logql.MatchNeq, "")}, `zone:!""`},
		{"empty", []logql.LabelMatcher{m("zone", logql.MatchEq, "")}, `zone:=""`},
		{"a dotted label is quoted", []logql.LabelMatcher{m("k8s.pod.name", logql.MatchNeq, "p")}, `-"k8s.pod.name":="p"`},
	} {
		f, err := StreamSelectorFilter(c.ms, labelFn, streamFields)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got := f.String(); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
		mustParseLogsQL(t, c.want)
	}
	// With a name function the plan quotes the field names, and a label VictoriaLogs keeps for itself is a stream tag.
	name := func(s string) string { return "N_" + s }
	f, err := StreamSelectorFilterNamed([]logql.LabelMatcher{m("zone", logql.MatchEq, "a"), m("_time", logql.MatchEq, "t"), m("_stream", logql.MatchRe, "a.*")}, nil, nil, name)
	if err != nil || f.String() != `N_zone:="a" {_time="t"} {_stream=~"a.*"}` {
		t.Errorf("named: %v, %v", f, err)
	}
	if got, want := ReservedFieldStreamFilter("_time", "!=", "a").String(), `{_time!="a"}`; got != want {
		t.Errorf("ReservedFieldStreamFilter = %s, want %s", got, want)
	}
	for _, c := range []struct {
		m    logql.LabelMatcher
		want bool
	}{
		{m("app", logql.MatchEq, "x"), true},
		{m("pod", logql.MatchEq, "p"), true},
		{m("app", logql.MatchNeq, "x"), false},
		{m("app", logql.MatchRe, "x"), false},
		{m("service_name", logql.MatchEq, "x"), false},
		{m("detected_level", logql.MatchEq, "x"), false},
		{m("zone", logql.MatchEq, "x"), false},
	} {
		if got := StreamFilterMatcher(c.m, labelFn, streamFields); got != c.want {
			t.Errorf("StreamFilterMatcher(%s%s%q) = %v, want %v", c.m.Name, c.m.Op, c.m.Value, got, c.want)
		}
	}
	if StreamFilterMatcher(m("app", logql.MatchEq, "x"), nil, nil) {
		t.Error("no declared stream field: no stream filter")
	}
	if StreamFilterMatcher(m("detected_level", logql.MatchEq, "x"), nil, map[string]bool{"detected_level": true}) != true {
		t.Error("a detected_level stream field is matched in the stream filter")
	}
}

// Loki derives service_name from the first non-empty of its source labels, else unknown_service; a selector on it
// matches the rows whose derived value the matcher accepts.
// conformance: semantics/label-filter-expression-syntax
func TestServiceNameMatcherFilter(t *testing.T) {
	derive := func(row map[string]string) string {
		for _, f := range SyntheticServiceNameFields {
			if v := row[f]; v != "" {
				return v
			}
		}
		return UnknownServiceNameValue
	}
	rows := []map[string]string{
		{"service_name": "api"},
		{"app": "web"},
		{"app": "web", "service": "svc"},
		{"service_name": "", "app": "web"},
		{},
		{"k8s.container.name": "c"},
		{"name": "n", "job": "j"},
		{"job": "unknown_service"},
		{"app": "api", "job": "j"},
	}
	for _, c := range []struct {
		value string
		neg   bool
		re    bool
	}{
		{"api", false, false}, {"api", true, false}, {"web", false, false}, {"svc", false, false}, {"unknown_service", false, false},
		{"unknown_service", true, false}, {"c", false, false}, {"n", false, false}, {"", false, false}, {"", true, false},
		{"w.*", false, true}, {"w.*", true, true}, {".*", false, true}, {".*", true, true}, {"", false, true}, {"", true, true},
		{"api|web", false, true}, {"unknown.*", false, true}, {".+", false, true},
	} {
		f := ServiceNameMatcherFilterExpr(c.value, c.neg, c.re)
		for _, row := range rows {
			derived := derive(row)
			var accepts bool
			if c.re {
				accepts = regexp.MustCompile("^(?s:" + c.value + ")$").MatchString(derived)
			} else {
				accepts = derived == c.value
			}
			if want := accepts != c.neg; evalRow(t, f, row) != want {
				t.Errorf("service_name value=%q neg=%v re=%v on %v (derived %q) = %v, want %v", c.value, c.neg, c.re, row, derived, !want, want)
			}
		}
	}
	// Every row has a non-empty service_name: = "" keeps no stream and != "" keeps every one.
	if got := ServiceNameMatcherFilterExpr("", false, false); got.String() != "-*" {
		t.Errorf(`service_name="" = %s`, got)
	}
	if got := ServiceNameMatcherFilterExpr("", true, false); got.String() != "*" {
		t.Errorf(`service_name!="" = %s`, got)
	}
	// A selector on service_name is the derived filter, also through the field filter of a matcher.
	if f := MatcherFieldFilter("service_name", false, false, "api", nil); f.String() != ServiceNameMatcherFilterExpr("api", false, false).String() {
		t.Errorf("MatcherFieldFilter(service_name) = %s", f)
	}
	if MatcherFieldFilter("", false, false, "x", nil) != nil {
		t.Error("an empty label is no field")
	}
}

// conformance: architecture-syntax-package-layering
func TestFieldIdentifierNormalization(t *testing.T) {
	for in, want := range map[string][2]string{
		" a . b ":    {"a.b", "a.b"},
		"a\t.\nb":    {"a.b", "a.b"},
		"a..b":       {"a..b", "a.b"},
		".a.":        {".a.", "a"},
		"a b":        {"a b", "a b"},
		"`a\"b`":     {"ab", "ab"},
		" ":          {"", ""},
		"":           {"", ""},
		"...":        {"...", ""},
		"a. . .b":    {"a...b", "a.b"},
		"plain_name": {"plain_name", "plain_name"},
	} {
		if got := NormalizeFieldIdentifier(in); got != want[0] {
			t.Errorf("NormalizeFieldIdentifier(%q) = %q, want %q", in, got, want[0])
		}
		if got := SanitizeFieldIdentifier(in); got != want[1] {
			t.Errorf("SanitizeFieldIdentifier(%q) = %q, want %q", in, got, want[1])
		}
	}
}
