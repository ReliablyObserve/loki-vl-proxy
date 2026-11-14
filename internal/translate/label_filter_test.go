package translate

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// conformance: semantics/label-filter-expression-syntax
func TestLabelFilterLeaf(t *testing.T) {
	caps := logsql.CapabilitiesFor("v1.53.0")
	for _, c := range []struct {
		field, cmp, value string
		want              string
		ok                bool
	}{
		{"app", "=", "x", `app:="x"`, true},
		{"app", "==", "x", `app:="x"`, true},
		{"app", "!=", "x", `-app:="x"`, true},
		// A regexp is the one Loki matches: api.* is a substring check, a literal alternation an OR of equalities.
		{"app", "=~", "a.*", `app:~"a"`, true},
		{"app", "!~", "a|b", `-(app:="a" OR app:="b")`, true},
		{"app", "=~", "", `app:~".*"`, true},
		// A comparison value is not quoted.
		{"app", ">", "5", `app:>5`, true},
		{"app", ">=", "5", `app:>=5`, true},
		{"app", "<", "5", `app:<5`, true},
		{"app", "<=", "5s", `app:<=5s`, true},
		// level="" selects the rows without a level (absent or empty); != "" the rows with one.
		{"level", "=", "", `-level:*`, true},
		{"level", "!=", "", `level:!""`, true},
		{"app", "=", "", `app:=""`, true},
		{"app", "!=", "", `app:!""`, true},
		{"app", "~", "x", "", false},
		{"app", "", "x", "", false},
	} {
		f, ok := LabelFilterLeaf(c.field, c.cmp, c.value, false, caps)
		if ok != c.ok {
			t.Errorf("LabelFilterLeaf(%s %s %q) ok = %v, want %v", c.field, c.cmp, c.value, ok, c.ok)
			continue
		}
		if ok {
			if got := f.String(); got != c.want {
				t.Errorf("LabelFilterLeaf(%s %s %q) = %s, want %s", c.field, c.cmp, c.value, got, c.want)
			}
			mustParseLogsQL(t, "* "+f.String())
		}
	}
}

// An ip() label filter keeps a line whose label holds an address of the pattern; != keeps a line whose label is
// present and holds none. An address of the other family never matches.
// conformance: semantics/ip-label-filter-embedded-address
func TestLabelFilterLeafIP(t *testing.T) {
	caps := logsql.CapabilitiesFor("v1.53.0")
	eq, ok := LabelFilterLeaf("addr", "=", "10.0.0.0/8", true, caps)
	if !ok || !strings.Contains(eq.String(), "addr:ipv4_range(10.0.0.0, 10.255.255.255)") || strings.Contains(eq.String(), "addr:*") {
		t.Errorf("= ip(10.0.0.0/8): %v, %v", eq, ok)
	}
	ne, ok := LabelFilterLeaf("addr", "!=", "10.0.0.0/8", true, caps)
	if !ok || !strings.HasPrefix(ne.String(), "(addr:* -(") {
		t.Errorf("!= ip(10.0.0.0/8) must require the label and exclude the pattern: %v, %v", ne, ok)
	}
	v6, ok := LabelFilterLeaf("addr", "=", "::1", true, caps)
	if !ok || !strings.Contains(v6.String(), `addr:ipv6_range("::1", "::1")`) {
		t.Errorf("= ip(::1): %v, %v", v6, ok)
	}
	for _, f := range []logsql.FilterExpr{eq, ne, v6} {
		mustParseLogsQL(t, "* "+f.String())
	}
}

// conformance: architecture-syntax-package-layering
func TestLabelFilterOperatorTable(t *testing.T) {
	for cmp, want := range map[string]LabelFilterOp{
		"==": {logsql.FieldOpExact, false, false, false},
		"=":  {logsql.FieldOpExact, false, false, false},
		"!=": {logsql.FieldOpExact, true, false, false},
		"=~": {logsql.FieldOpRegexp, false, true, false},
		"!~": {logsql.FieldOpRegexp, true, true, false},
		">":  {logsql.FieldOpGT, false, false, true},
		">=": {logsql.FieldOpGTE, false, false, true},
		"<":  {logsql.FieldOpLT, false, false, true},
		"<=": {logsql.FieldOpLTE, false, false, true},
	} {
		got, ok := LookupLabelFilterOp(cmp)
		if !ok || got != want || !KnownLabelFilterCmp(cmp) {
			t.Errorf("%s: %+v, %v, known %v; want %+v", cmp, got, ok, KnownLabelFilterCmp(cmp), want)
		}
	}
	for _, cmp := range []string{"~", "", "=>", "!"} {
		if _, ok := LookupLabelFilterOp(cmp); ok || KnownLabelFilterCmp(cmp) {
			t.Errorf("%q is no label filter operator", cmp)
		}
	}
}

// conformance: architecture-syntax-package-layering
func TestFilterTextHelpers(t *testing.T) {
	for s, want := range map[string]bool{
		"a b": true, "a": false, `"a b"`: false, "(a b)": false, "`a b`": false, `"a\" b"`: false, "a(b c)d": false,
		`x:="a" y:="b"`: true, "": false,
	} {
		if got := HasTopLevelSpace(s); got != want {
			t.Errorf("HasTopLevelSpace(%q) = %v, want %v", s, got, want)
		}
		wantParen := s
		if want {
			wantParen = "(" + s + ")"
		}
		if got := ParenthesizeCompound(s); got != wantParen {
			t.Errorf("ParenthesizeCompound(%q) = %q, want %q", s, got, wantParen)
		}
	}
	for s, want := range map[string]bool{
		"*": true, "-*": true, "x:=1": true, "x:~1": true, "x:!1": true, "x:>1": true, "x:<1": true, "foo": false, `~"a"`: false,
	} {
		if got := IsFieldFilter(s); got != want {
			t.Errorf("IsFieldFilter(%q) = %v, want %v", s, got, want)
		}
	}
}

// A label filter's regexp is not anchored as written: Loki simplifies it first, and anchors what it cannot simplify as
// ^(?:re)$. The verdicts below are those of Loki's RegexSimplifier (v3.7.7).
// conformance: semantics/label-filter-expression-syntax
func TestLokiLabelFilterPatternVerdicts(t *testing.T) {
	for _, c := range []struct {
		pattern string
		match   []string
		reject  []string
	}{
		{"api.*", []string{"api", "my-api-x", "xapi"}, []string{"ap", ""}},
		{".*api", []string{"api", "my-api-x"}, []string{"ap"}},
		{"ab.*.*", []string{"ab", "xaby"}, []string{"a"}},
		{"api", []string{"api"}, []string{"apix", "xapi", ""}},
		{"", []string{"", "x", "a\nb"}, nil},
		{".*", []string{"", "x", "a\nb"}, nil},
		{".+", []string{"x", "a\nb"}, []string{""}},
		{"GET|", []string{"GET"}, []string{"GETX", ""}},
		{"a|.*", []string{"a"}, []string{"ab", ""}},
		{"prod-eu|prod", []string{"prod", "prod-eu", "xprodx"}, []string{"pro"}},
		{"api|api-gateway", []string{"api", "api-gateway", "my-api"}, []string{"ap"}},
		{"(?i)abc", []string{"abc", "ABC", "aBc"}, []string{"abcd", ""}},
		{"(?i)api.*", []string{"API", "xapi", "xApIx"}, []string{"ap"}},
		{".*a.*|b", []string{"cat", "b"}, []string{"bb", "xyz"}},
		// Not simplified: anchored as ^(?:re)$, and '.' does not match a newline.
		{"a.c", []string{"abc"}, []string{"abcd", "a\nc", "xabc"}},
		{"[a-z]+", []string{"abc"}, []string{"abc1", ""}},
		{"ab(c|d)", []string{"abc", "abd"}, []string{"ab", "abcd"}},
		{"a.*(b|c)", []string{"ab", "axxc"}, []string{"a", "abx"}},
		{"^foo$", []string{"foo"}, []string{"xfoo"}},
	} {
		re, err := regexp.Compile(AnchorLabelFilterPattern(LokiLabelFilterPattern(c.pattern)))
		if err != nil {
			t.Errorf("%q: %v", c.pattern, err)
			continue
		}
		for _, v := range c.match {
			if !re.MatchString(v) {
				t.Errorf("=~%q must match %q (pattern %q)", c.pattern, v, LokiLabelFilterPattern(c.pattern))
			}
		}
		for _, v := range c.reject {
			if re.MatchString(v) {
				t.Errorf("=~%q must not match %q (pattern %q)", c.pattern, v, LokiLabelFilterPattern(c.pattern))
			}
		}
	}
	// A regexp that does not parse is left as it is for the backend to reject; Loki rejects it at parse time.
	if got := LokiLabelFilterPattern("("); got != "(" {
		t.Errorf("invalid regexp: %q", got)
	}
}

// A parser sanitizes each key into a label name, so a filter on the label reads every spelling of the key.
// conformance: parsed-label-series-identity
func TestParsedKeyVariants(t *testing.T) {
	for label, want := range map[string][]string{
		"ab":              {"ab"},
		"a_b":             {"a_b", "a.b", "a-b"},
		"a_b_c":           {"a_b_c", "a.b_c", "a_b.c", "a.b.c", "a-b-c"},
		"a__b":            {"a__b", "a._b", "a_.b", "a--b"}, // a.b with adjacent dots is no key
		"_a":              {"_a", "-a"},
		"a_":              {"a_", "a-"},
		"_":               {"_", "-"},
		"__":              {"__", "--"},
		"a_b_c_d_e_f_g_h": nil, // more underscores than the bound: uniform spellings only, checked below
	} {
		got := ParsedKeyVariants(label)
		if want == nil {
			want = []string{label, "a.b.c.d.e.f.g.h", "a-b-c-d-e-f-g-h"}
		}
		eqStrings(t, "ParsedKeyVariants("+label+")", got, want)
	}
	// Six underscores are still spelled out: 2^6 dot choices, the label itself and the hyphenated one.
	if got := ParsedKeyVariants("a_b_c_d_e_f_g"); len(got) != 1<<6+1 || got[0] != "a_b_c_d_e_f_g" || got[len(got)-1] != "a-b-c-d-e-f-g" {
		t.Errorf("six underscores: %d variants, first %q, last %q", len(got), got[0], got[len(got)-1])
	}
}

// A label filter is read on the keys of a sanitized label only when the label is one a parser can have produced.
// conformance: parsed-label-series-identity
func TestParsedKeysOf(t *testing.T) {
	for _, c := range []struct {
		label, translated string
		want              []string
	}{
		{"a_b", "a_b", []string{"a_b", "a.b", "a-b"}},
		{"a_b", "x", nil},       // the translation maps the label
		{"ab", "ab", nil},       // no underscore
		{"_a_b", "_a_b", nil},   // starts with an underscore
		{"a.b_c", "a.b_c", nil}, // dotted
		{"a-b_c", "a-b_c", nil}, // hyphenated
		{"a_b" + ParsedKeySep, "a_b" + ParsedKeySep, nil},
		{"a_b" + ExtractedMarker, "a_b" + ExtractedMarker, nil},
		{"", "", nil},
	} {
		got := ParsedKeysOf(c.label, c.translated)
		if len(got) == 0 && len(c.want) == 0 {
			continue
		}
		eqStrings(t, "ParsedKeysOf("+c.label+", "+c.translated+")", got, c.want)
	}
	for label, want := range map[string]bool{
		"a_b": true, "a_1": true, "a_": true, "ab": false, "_a_b": false, "a-b": false, "1_a": false, "a b": false,
		"service_name": false, "detected_level": false, "": false,
	} {
		if got := ParsedLabelCandidate(label); got != want {
			t.Errorf("ParsedLabelCandidate(%q) = %v, want %v", label, got, want)
		}
	}
	var b strings.Builder
	ParsedKeyPipesText(&b, "a_b")
	if want := ` | format if ("a.b":*) "<a.b>" as a_b keep_original_fields | format if ("a-b":*) "<a-b>" as a_b keep_original_fields`; b.String() != want {
		t.Errorf("ParsedKeyPipesText = %q, want %q", b.String(), want)
	}
	b.Reset()
	ParsedKeyPipesText(&b, "ab")
	if b.Len() != 0 {
		t.Errorf("a label with one spelling writes no pipe, got %q", b.String())
	}
}

// An absent label is the empty value: whether a filter accepts it decides how the per-key filters are joined.
// conformance: semantics/numeric-label-filter-not-equal-missing-label
func TestParsedKeyMatchesEmptyAndFirst(t *testing.T) {
	op := func(cmp string) LabelFilterOp {
		o, ok := LookupLabelFilterOp(cmp)
		if !ok {
			t.Fatalf("operator %s", cmp)
		}
		return o
	}
	for _, c := range []struct {
		cmp, value string
		ip         bool
		empty      bool
		first      bool
	}{
		{"=", "", false, true, false},
		{"=", "x", false, false, false},
		{"!=", "", false, false, true},
		{"!=", "x", false, true, false},
		{"=~", "", false, true, false},
		{"=~", "x", false, false, false},
		{"=~", ".*", false, true, false},
		{"=~", "a|", false, false, false},
		{"=~", "(", false, false, false}, // a regexp that does not compile accepts nothing, and rejects everything under !~
		{"!~", "(", false, true, false},
		{"!~", "", false, false, false},
		{"!~", ".*", false, false, false},
		{"!~", "x", false, true, false},
		{">", "5", false, false, false},
		{"<=", "5", false, false, false},
		{"=", "", true, false, false}, // an ip() filter drops a line without the label under both operators
		{"!=", "", true, false, false},
	} {
		if got := ParsedKeyMatchesEmpty(c.value, op(c.cmp), c.ip); got != c.empty {
			t.Errorf("ParsedKeyMatchesEmpty(%s %q ip=%v) = %v, want %v", c.cmp, c.value, c.ip, got, c.empty)
		}
		if got := ParsedKeysKeepsFirst(c.value, op(c.cmp), c.ip); got != c.first {
			t.Errorf("ParsedKeysKeepsFirst(%s %q ip=%v) = %v, want %v", c.cmp, c.value, c.ip, got, c.first)
		}
	}
}

// One filter per candidate key: a filter that rejects the empty value holds when any key satisfies it, one that
// accepts it when every key does.
// conformance: parsed-label-series-identity
func TestParsedKeyFilter(t *testing.T) {
	eq := func(field string) logsql.FilterExpr {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpExact, Value: "1"}
	}
	a, b := eq("a"), eq("b")
	for _, c := range []struct {
		name         string
		filters      []logsql.FilterExpr
		matchesEmpty bool
		want         string
		ok           bool
	}{
		{"one key", []logsql.FilterExpr{a}, false, `a:="1"`, true},
		{"one key, accepts empty", []logsql.FilterExpr{a}, true, `a:="1"`, true},
		{"any key", []logsql.FilterExpr{a, b}, false, `(a:="1" OR b:="1")`, true},
		{"every key", []logsql.FilterExpr{a, b}, true, `(a:="1" b:="1")`, true},
		{"every key of a filter with a top-level space cannot be joined", []logsql.FilterExpr{a, logsql.AnyOf{a, b}}, true, "", false},
		{"any key of such filters is fine", []logsql.FilterExpr{a, logsql.AnyOf{a, b}}, false, `(a:="1" OR a:="1" OR b:="1")`, true},
	} {
		f, ok := ParsedKeyFilter(c.filters, c.matchesEmpty)
		if ok != c.ok {
			t.Errorf("%s: ok = %v, want %v", c.name, ok, c.ok)
			continue
		}
		if ok && f.String() != c.want {
			t.Errorf("%s: %s, want %s", c.name, f, c.want)
		}
	}
}
