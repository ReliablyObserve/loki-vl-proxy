package translate

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
	"unsafe"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// conformance: architecture-syntax-package-layering
func TestExtractStageBuilders(t *testing.T) {
	if got := ExtractRegexp(`(?P<a>\d+)`).String(); got != `| extract_regexp "(?P<a>\\d+)"` {
		t.Errorf("ExtractRegexp = %s", got)
	}
	if got := ExtractRegexp("a`b").String(); got != `| extract_regexp "a`+"`"+`b"` {
		t.Errorf("ExtractRegexp with a backquote = %s", got)
	}
	if p, ok := ExtractPattern(`<a> "q" <b>`); !ok || p.String() != `| extract "<a> \"q\" <b>"` {
		t.Errorf("ExtractPattern = %v, %v", p, ok)
	}
	for _, noop := range []string{"(.*)", ".*", "^.*$", "(?s).*", "(?s:.*)", " (.*) "} {
		if !NoopPattern(noop) {
			t.Errorf("%q is a wildcard-only pattern", noop)
		}
		if p, ok := ExtractPattern(noop); ok || p != nil {
			t.Errorf("ExtractPattern(%q) = %v, %v; want no pipe", noop, p, ok)
		}
	}
	for _, real := range []string{"", "<a>", "(.+)", "x.*"} {
		if NoopPattern(real) {
			t.Errorf("%q is not a wildcard-only pattern", real)
		}
	}
	if got := Decolorize().String(); got != "| decolorize" {
		t.Errorf("Decolorize = %s", got)
	}
}

// conformance: architecture-syntax-package-layering
func TestExtractStageNames(t *testing.T) {
	if got := RegexpCaptureNames(`(?P<a>\d+) (\w+) (?P<b>x)`); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("RegexpCaptureNames = %v", got)
	}
	if got := RegexpCaptureNames(`(?P<a>`); got != nil {
		t.Errorf("a regexp Go does not compile defines no names, got %v", got)
	}
	if got := PatternCaptureNames(`<a> <_> <b1_c> <_x> <1a> <a.b> <A>`); !reflect.DeepEqual(got, []string{"a", "b1_c", "A"}) {
		t.Errorf("PatternCaptureNames = %v", got)
	}
}

// RegexpCaptureNames parses the regexp with regexp/syntax where the translator compiles it: the names and the
// verdict are the same for every regexp, valid or not.
//
// conformance: architecture-syntax-package-layering
func TestRegexpCaptureNamesMatchCompile(t *testing.T) {
	for _, pattern := range []string{
		``, `a`, `(a)`, `(?P<a>x)`, `(?P<a>x)(?P<b>y)`, `(?P<a>x)|(?P<b>y)`, `(?P<a>(?P<b>x))`, `(?:(?P<a>x))`, `(?P<a>x)(?P<a>y)`,
		`(?P<a_b>\d+)(\w+)`, `(?<a>x)`, `(?P<a>`, `(?P<a>x`, `(?P<1a>x)`, `(?P<a-b>x)`, `(?P<>x)`, `[`, `a{1001}`, `a{2,1}`, `(a{1000}){1000}`,
		`(?i)(?P<é>x)`, `(?s:.)(?P<z>.)`, `\p{Foo}`, `(?P<a>x)\1`, `(?=x)`, `x**`, `(((((((((((((((((((((((((((((((((((((((((((((((((((a)))))))))))))))))))))))))))))))))))))))))))))))))))`,
		"(?P<a>x)\n(?P<b>y)", `(?U)(?P<l>.*)`, `^(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<ts>[^\]]+)\]`, strings.Repeat(`(?P<a>x)`, 3), `(?P<` + strings.Repeat("n", 300) + `>x)`,
	} {
		var want []string
		if re, err := regexp.Compile(pattern); err == nil {
			for _, n := range re.SubexpNames() {
				if n != "" {
					want = append(want, n)
				}
			}
		}
		if got := RegexpCaptureNames(pattern); !reflect.DeepEqual(got, want) {
			t.Errorf("RegexpCaptureNames(%q) = %v, regexp.Compile gives %v", pattern, got, want)
		}
		// The shared reading answers the same names the first time, the second (kept) and from the cache, a kept list
		// clipped.
		for i := range 3 {
			if got := SharedRegexpCaptureNames(pattern); !reflect.DeepEqual(got, want) || i > 0 && cap(got) != len(got) {
				t.Errorf("SharedRegexpCaptureNames(%q) = %v (cap %d), regexp.Compile gives %v", pattern, got, cap(got), want)
			}
		}
	}
}

// SharedRegexpCaptureNames keeps a copy of the regexp, never the query text it is part of; a long regexp and one
// met once the cache is full are read as RegexpCaptureNames reads them, and not kept.
//
// conformance: architecture-syntax-package-layering
func TestSharedRegexpCaptureNamesBounds(t *testing.T) {
	query := "{a=\"b\"} | regexp `(?P<shared_probe>x)` | shared_probe=\"x\""
	pattern := query[strings.Index(query, "(?P"):strings.LastIndex(query, "`")]
	names := SharedRegexpCaptureNames(pattern)
	if _, ok := regexpCaptures.Load(pattern); ok {
		t.Fatal("a regexp met once is kept")
	}
	if again := SharedRegexpCaptureNames(pattern); !reflect.DeepEqual(names, []string{"shared_probe"}) || !reflect.DeepEqual(again, names) {
		t.Fatalf("names %v %v", names, again)
	}
	if _, ok := regexpCaptures.Load(pattern); !ok {
		t.Fatal("a regexp met twice is not kept")
	}
	regexpCaptures.Range(func(k, v any) bool {
		if k.(string) == pattern && unsafe.StringData(k.(string)) == unsafe.StringData(pattern) {
			t.Error("the cache keeps the query's own text")
		}
		for _, n := range v.([]string) {
			if p := unsafe.StringData(n); uintptr(unsafe.Pointer(p)) >= uintptr(unsafe.Pointer(unsafe.StringData(query))) &&
				uintptr(unsafe.Pointer(p)) < uintptr(unsafe.Pointer(unsafe.StringData(query)))+uintptr(len(query)) {
				t.Error("a cached name is part of the query's text")
			}
		}
		return true
	})
	long := `(?P<l>x)` + strings.Repeat("y", regexpCaptureMaxBytes)
	SharedRegexpCaptureNames(long)
	if got := SharedRegexpCaptureNames(long); !reflect.DeepEqual(got, []string{"l"}) {
		t.Errorf("a long regexp: %v", got)
	}
	if _, ok := regexpCaptures.Load(long); ok {
		t.Error("a long regexp is kept")
	}
	count := regexpCaptureCount.Load()
	regexpCaptureCount.Store(regexpCapturesMax)
	defer regexpCaptureCount.Store(count)
	SharedRegexpCaptureNames(`(?P<full>x)`)
	if got := SharedRegexpCaptureNames(`(?P<full>x)`); !reflect.DeepEqual(got, []string{"full"}) {
		t.Errorf("a full cache: %v", got)
	}
	if _, ok := regexpCaptures.Load(`(?P<full>x)`); ok {
		t.Error("a full cache grows")
	}
}

// patternCaptureRE is the reading of a pattern's placeholder names PatternCaptureNames scans for: <name>, a name
// that starts with a letter, leftmost first and apart.
var patternCaptureRE = regexp.MustCompile(`<([A-Za-z][A-Za-z0-9_]*)>`)

// PatternCaptureNames finds the names the regexp finds, in the same order, on any text.
//
// conformance: architecture-syntax-package-layering
func FuzzPatternCaptureNames(f *testing.F) {
	for _, s := range []string{
		``, `<a>`, `<_> <m> <_>`, `<a> <_> <b1_c> <_x> <1a> <a.b> <A>`, `<<a>>`, `<a<b>`, `<a`, `a>`, `<>`, `<ab_9>x<c>`,
		`<a><b><c>`, `<é>`, `< a>`, "<a\x00>", `<<<<<a>`, `<a>>><b>`, `<_a> <Z_>`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, text string) {
		var want []string
		for _, m := range patternCaptureRE.FindAllStringSubmatch(text, -1) {
			want = append(want, m[1])
		}
		if got := PatternCaptureNames(text); !reflect.DeepEqual(got, want) {
			t.Fatalf("PatternCaptureNames(%q) = %q, the regexp finds %q", text, got, want)
		}
	})
}

// IsFieldFilterExpr reads a filter's text as IsFieldFilter reads its String, without the string.
//
// conformance: architecture-syntax-package-layering
func TestIsFieldFilterExpr(t *testing.T) {
	for _, f := range []logsql.FilterExpr{
		logsql.FieldFilter{Field: "status", Op: logsql.FieldOpExact, Value: "500"},
		logsql.Exclude{Expr: logsql.FieldFilter{Field: "a", Op: logsql.FieldOpRegexp, Value: "x"}},
		logsql.Wildcard{}, logsql.Phrase{Value: "a:=b"}, logsql.Phrase{Value: "plain"},
		logsql.AllOf{logsql.Phrase{Value: "x"}, logsql.FieldFilter{Field: "b", Op: logsql.FieldOpGT, Value: "1"}},
		logsql.FieldFilter{Field: "c", Op: logsql.FieldOpLT, Value: strings.Repeat("v", 5000)},
	} {
		if got, want := IsFieldFilterExpr(f), IsFieldFilter(f.String()); got != want {
			t.Errorf("%s: IsFieldFilterExpr %v, IsFieldFilter %v", f, got, want)
		}
	}
}
