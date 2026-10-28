package translator

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// labelRegexpValues are the label values the answers below are taken over. A
// missing label reads as "" in both Loki and VictoriaLogs (which stores no
// empty field), so "" also stands for the absent label.
var labelRegexpValues = []string{"", "api", "API", "api-gateway", "my-api", "web", "webhook", "a\nb", "axb", "prod", "prod-eu", "nonprod", "foo-bar", "api.v1", "apixv1", "GET", "GETX", "bar", "buzz", "barbuzz", "nch", "anch", "a", "b", "ab", "4xx", "500", "503"}

// labelRegexpCase is what Loki v3.7.7 selects for `v =~ pattern`, as a stream
// selector matcher ({v=~"..."}, Prometheus' ^(?s:re)$) and as a label filter
// after a parser ({app="x"} | json | v=~"...", RegexSimplifier then ^(?:re)$).
// `!~` selects the complement. The answers were produced by Loki's own code
// (syntax.ParseMatchers and the label filter's stream pipeline) over these
// values; filter is nil where it equals selector.
type labelRegexpCase struct {
	pattern          string
	selector, filter []string
}

var (
	labelRegexpAll      = labelRegexpValues
	labelRegexpNonEmpty = labelRegexpValues[1:]
)

var labelRegexpCases = []labelRegexpCase{
	{"api", []string{"api"}, nil},
	{"(?i)API", []string{"api", "API"}, nil},
	{"api.*", []string{"api", "api-gateway", "api.v1", "apixv1"}, []string{"api", "api-gateway", "my-api", "api.v1", "apixv1"}},
	{".*api", []string{"api", "my-api"}, []string{"api", "api-gateway", "my-api", "api.v1", "apixv1"}},
	{".*api.*", []string{"api", "api-gateway", "my-api", "api.v1", "apixv1"}, nil},
	// A Grafana multi-value variable: the selector picks the two values; the
	// label filter's common prefix makes Loki compare by substring.
	{"api|web", []string{"api", "web"}, nil},
	{"api|api-gateway", []string{"api", "api-gateway"}, []string{"api", "api-gateway", "my-api", "api.v1", "apixv1"}},
	{"api-gateway|api", []string{"api", "api-gateway"}, []string{"api", "api-gateway", "my-api", "api.v1", "apixv1"}},
	{"bar|buzz", []string{"bar", "buzz"}, []string{"foo-bar", "bar", "buzz", "barbuzz"}},
	{"prod", []string{"prod"}, nil},
	{"prod|nonprod", []string{"prod", "nonprod"}, nil},
	{"prod-eu|prod", []string{"prod", "prod-eu"}, []string{"prod", "prod-eu", "nonprod"}},
	{"nch", []string{"nch"}, nil},
	{"ap", nil, nil},
	{"pi", nil, nil},
	// Empty-value semantics.
	{"", []string{""}, labelRegexpAll},
	{".*", labelRegexpAll, nil},
	{".+", labelRegexpNonEmpty, nil},
	{"x?", []string{""}, nil},
	{"GET|", []string{"", "GET"}, []string{"GET"}},
	{"api|", []string{"", "api"}, []string{"api"}},
	{"|api", []string{"", "api"}, []string{"api"}},
	{".*|api", labelRegexpAll, []string{"api"}},
	// '.' and newlines, flags, escaping, anchors written by the user.
	{"a.b", []string{"a\nb", "axb"}, []string{"axb"}},
	{"(?s)a.b", []string{"a\nb", "axb"}, nil},
	{"foo.*bar", []string{"foo-bar"}, nil},
	{`api\.v1`, []string{"api.v1"}, nil},
	{"(?i)api|web", []string{"api", "API", "web"}, nil},
	{"(?i)get", []string{"GET"}, nil},
	{"(api)", []string{"api"}, nil},
	{"^api$", []string{"api"}, nil},
	{"api$", []string{"api"}, nil},
	{"[a-z]+", []string{"api", "web", "webhook", "axb", "prod", "nonprod", "bar", "buzz", "barbuzz", "nch", "anch", "a", "b", "ab"}, nil},
	{"a|b", []string{"a", "b"}, nil},
	{".*-gateway", []string{"api-gateway"}, nil},
	{"api-.*", []string{"api-gateway"}, nil},
	{"api.+", []string{"api-gateway", "api.v1", "apixv1"}, nil},
	{"5..", []string{"500", "503"}, nil},
	{"4xx|5..", []string{"4xx", "500", "503"}, nil},
	{"50[03]", []string{"500", "503"}, nil},
}

// vlFilterMatches evaluates one emitted field filter (`[-]field:="v"`,
// `[-]field:~"re"` or `[-](field:="a" OR field:="b")`) on a row whose field holds value ("" when missing), the way
// VictoriaLogs does: an exact filter compares the whole value; a regexp filter
// is an unanchored match in which '.' matches a newline (regexutil parses with
// syntax.DotNL), except that "" and ".*" match every row and ".+" every
// non-empty value (lib/logstorage parser.go newFilterRegexpOpt and
// filter_regexp.go matchRow, v1.49.0 and v1.53.0). The same filters were run
// against VictoriaLogs v1.49.0 and v1.53.0 storage with these values and
// selected the same rows.
func vlFilterMatches(t *testing.T, filter, field, value string) bool {
	t.Helper()
	negate := strings.HasPrefix(filter, "-")
	body := strings.TrimPrefix(filter, "-")
	if inner, ok := strings.CutPrefix(body, "("); ok {
		// (f:="a" OR f:="b"): any branch.
		inner = strings.TrimSuffix(inner, ")")
		match := false
		for _, branch := range splitOutsideQuotes(inner, " OR ") {
			match = match || vlFilterMatches(t, branch, field, value)
		}
		return match != negate
	}
	name, rest, ok := strings.Cut(body, ":")
	if !ok || name != field || len(rest) < 1 {
		t.Fatalf("not a single filter on %s: %s", field, filter)
	}
	operand, err := strconv.Unquote(rest[1:])
	if err != nil {
		t.Fatalf("filter operand %s: %v", rest[1:], err)
	}
	var match bool
	switch {
	case rest[0] == '=':
		match = value == operand
	case rest[0] == '~' && (operand == "" || operand == ".*"):
		match = true
	case rest[0] == '~' && operand == ".+":
		match = value != ""
	case rest[0] == '~':
		match = regexp.MustCompile("(?s)" + operand).MatchString(value)
	default:
		t.Fatalf("unexpected filter %s", filter)
	}
	return match != negate
}

// splitOutsideQuotes splits s at every sep outside a double-quoted string.
func splitOutsideQuotes(s, sep string) []string {
	var out []string
	start, quoted := 0, false
	for i := 0; i < len(s); i++ {
		switch {
		case quoted && s[i] == '\\':
			i++
		case s[i] == '"':
			quoted = !quoted
		case !quoted && strings.HasPrefix(s[i:], sep):
			out = append(out, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	return append(out, s[start:])
}

func complementOf(selected []string) []string {
	var out []string
	for _, v := range labelRegexpValues {
		if !slices.Contains(selected, v) {
			out = append(out, v)
		}
	}
	return out
}

// conformance: profiles/label-filter-regexp-anchoring, profiles/stream-selector-regexp-anchoring
func TestLabelRegexpMatchesWhatLokiMatches(t *testing.T) {
	for _, version := range []string{"v1.49.0", "v1.53.0"} {
		caps := logsql.CapabilitiesFor(version)
		for _, tc := range labelRegexpCases {
			filterWant := tc.filter
			if filterWant == nil {
				filterWant = tc.selector
			}
			for _, kind := range []struct {
				name, query, prefix string
				want                []string
			}{
				{"selector =~", `{v=~` + strconv.Quote(tc.pattern) + `}`, "", tc.selector},
				{"selector !~", `{v!~` + strconv.Quote(tc.pattern) + `}`, "", complementOf(tc.selector)},
				{"label filter =~", `{app="x"} | json | v=~` + strconv.Quote(tc.pattern), `app:="x" | unpack_json`, filterWant},
				{"label filter !~", `{app="x"} | json | v!~` + strconv.Quote(tc.pattern), `app:="x" | unpack_json`, complementOf(filterWant)},
			} {
				t.Run(version+"/"+kind.name+"/"+tc.pattern, func(t *testing.T) {
					got, err := TranslateLogQLWithCapabilities(kind.query, nil, nil, caps)
					if err != nil {
						t.Fatalf("translate %s: %v", kind.query, err)
					}
					filter := got
					if kind.prefix != "" {
						rest, ok := strings.CutPrefix(got, kind.prefix)
						if !ok {
							t.Fatalf("translation %s does not start with %s", got, kind.prefix)
						}
						filter = strings.TrimPrefix(strings.TrimSpace(rest), "| filter ")
					}
					var selected []string
					for _, v := range labelRegexpValues {
						if vlFilterMatches(t, filter, "v", v) {
							selected = append(selected, v)
						}
					}
					if !slices.Equal(selected, kind.want) {
						t.Errorf("%s -> %s\nselects %q\nLoki     %q", kind.query, got, selected, kind.want)
					}
				})
			}
		}
	}
}

// The VictoriaLogs forms the anchored semantics are emitted as: exact and
// substring filters where the whole-value match is one, the two patterns
// VictoriaLogs short-circuits, exact filters for a label filter's literal
// alternation, the anchored regexp otherwise. Line filters are
// substring regexps in Loki and stay as written.
//
// conformance: profiles/label-filter-regexp-anchoring, profiles/stream-selector-regexp-anchoring
func TestLabelRegexpEmittedForms(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{`{app=~"api"}`, `app:="api"`},
		{`{app!~"api"}`, `-app:="api"`},
		{`{app=~".*api.*"}`, `app:~"api"`},
		{`{app=~"api|web"}`, `app:~"^(?s:api|web)$"`},
		{`{app=~".*"}`, `app:~".*"`},
		{`{app=~".+"}`, `app:~".+"`},
		{`{app=~""}`, `app:~"^(?s:)$"`},
		{`{app=~"(?i)api"}`, `app:~"^(?s:(?i)api)$"`},
		{`{app="x"} | json | v=~"api.*"`, `app:="x" | unpack_json | filter v:~"api"`},
		{`{app="x"} | json | v=~"5.."`, `app:="x" | unpack_json | filter v:~"^(?-s:5..)$"`},
		{`{app="x"} | json | v=~"api|api-gateway"`, `app:="x" | unpack_json | filter v:~"^(?-s:(?s:.*)api(?s:.*)|(?s:.*)api-gateway(?s:.*))$"`},
		{`{app="x"} | json | v=~""`, `app:="x" | unpack_json | filter v:~".*"`},
		{`{app="x"} | json | v=~"GET|"`, `app:="x" | unpack_json | filter v:="GET"`},
		{`{app="x"} | json | v=~"api|web"`, `app:="x" | unpack_json | filter (v:="api" OR v:="web")`},
		{`{app="x"} | json | v!~"api|apx"`, `app:="x" | unpack_json | filter -(v:="api" OR v:="apx")`},
		{`{app=~"api"} |~ "api"`, `app:="api" ~"api"`},
		{`{app="x"} !~ "api"`, `app:="x" NOT ~"api"`},
	} {
		got, err := TranslateLogQL(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		if got != tc.want {
			t.Errorf("%s\n got: %s\nwant: %s", tc.query, got, tc.want)
		}
	}
}

// The proxy-side label-filter conditions (NewLabelFilterCondition, which the
// ordered JSON metric path and the Loki profile evaluate rows with) and the
// LogsQL that path sends (LabelFilterRegexpFieldFilter) agree with Loki's label
// filter on every value of every case.
//
// conformance: profiles/label-filter-regexp-anchoring
func TestLabelFilterConditionAgreesWithLoki(t *testing.T) {
	for _, tc := range labelRegexpCases {
		want := tc.filter
		if want == nil {
			want = tc.selector
		}
		for _, op := range []string{"=~", "!~"} {
			if op == "!~" {
				want = complementOf(want)
			}
			condition, err := NewLabelFilterCondition("v", op, tc.pattern)
			if err != nil {
				t.Fatalf("%s %q: %v", op, tc.pattern, err)
			}
			filter := LabelFilterRegexpFieldFilter("v", tc.pattern, op == "!~")
			var evaluated, sent []string
			for _, v := range labelRegexpValues {
				if condition.Matches(v) {
					evaluated = append(evaluated, v)
				}
				if vlFilterMatches(t, filter, "v", v) {
					sent = append(sent, v)
				}
			}
			if !slices.Equal(evaluated, want) {
				t.Errorf("%s %q: the condition selects %q, Loki %q", op, tc.pattern, evaluated, want)
			}
			if !slices.Equal(sent, want) {
				t.Errorf("%s %q: %s selects %q, Loki %q", op, tc.pattern, filter, sent, want)
			}
		}
	}
}

// FuzzSelectorRegexpFilter checks the exact, substring and match-all forms
// against the reference they replace: the selector filter selects a value if
// and only if Prometheus' ^(?s:pattern)$ matches it.
//
// conformance: profiles/stream-selector-regexp-anchoring
func FuzzSelectorRegexpFilter(f *testing.F) {
	for _, tc := range labelRegexpCases {
		for _, v := range labelRegexpValues {
			f.Add(tc.pattern, v)
		}
	}
	f.Fuzz(func(t *testing.T, pattern, value string) {
		reference, err := regexp.Compile(logsql.AnchorLabelMatcherRegex(pattern))
		if err != nil {
			t.Skip("not a regexp Loki accepts")
		}
		for _, negate := range []bool{false, true} {
			filter := selectorRegexpFieldFilter("v", pattern, negate)
			if got, want := vlFilterMatches(t, filter, "v", value), reference.MatchString(value) != negate; got != want {
				t.Fatalf("%s on %q: %v, ^(?s:%s)$ negated=%v: %v", filter, value, got, pattern, negate, want)
			}
		}
	})
}
