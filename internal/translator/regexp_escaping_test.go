package translator

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// VictoriaLogs unquotes a double-quoted LogsQL string with Go's rules, so a
// regexp's backslashes have to survive as escapes. Verified on v1.52.0:
// `url:~"^/api/v\d+/"` is rejected with "compound token cannot start with",
// `url:~"^/api/v\\d+/"` is accepted. Loki accepts both the quoted spelling
// with a doubled backslash and the backtick spelling with a single one, and
// treats them as the same pattern, so both must translate identically.
//
// conformance: loki_api_v1_query_range
func TestRegexpMatchersReachVictoriaLogsUnescaped(t *testing.T) {
	for _, tc := range []struct{ name, logql, wantPattern string }{
		{"stream matcher, quoted", `{app=~"api-\\d+"}`, `api-\d+`},
		{"stream matcher, backtick", "{app=~`api-\\d+`}", `api-\d+`},
		{"stream matcher, escaped dot", `{app=~"a\\.b"}`, `a\.b`},
		{"label filter, quoted", `{app="x"} | level=~"wa\\w+"`, `wa\w+`},
		{"label filter, backtick", "{app=\"x\"} | level=~`wa\\w+`", `wa\w+`},
		{"negated label filter", `{app="x"} | level!~"inf\\w+"`, `inf\w+`},
		{"line filter", `{app="x"} |~ "\\d+"`, `\d+`},
		// Loki unquotes `"a\\\"b"` to the pattern a\"b: an escaped backslash and
		// an escaped quote, which RE2 reads as a literal quote.
		{"quote inside the pattern", `{app=~"a\\\"b"}`, `a\"b`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			literal := regexp.MustCompile(`~("(?:[^"\\]|\\.)*")`).FindStringSubmatch(got)
			if literal == nil {
				t.Fatalf("no quoted regexp literal in %q", got)
			}
			// What VictoriaLogs does with the literal it receives.
			pattern, err := strconv.Unquote(literal[1])
			if err != nil {
				t.Fatalf("VictoriaLogs would reject %s: %v", literal[1], err)
			}
			if pattern != tc.wantPattern {
				t.Fatalf("backend receives pattern %q, want %q (emitted %s)", pattern, tc.wantPattern, got)
			}
			if _, err := regexp.Compile(pattern); err != nil {
				t.Fatalf("pattern does not compile: %v", err)
			}
		})
	}
}

// FuzzRegexpMatcherEscaping asserts the invariant for arbitrary patterns: what
// the backend unquotes is exactly the pattern Loki compiles.
func FuzzRegexpMatcherEscaping(f *testing.F) {
	for _, seed := range []string{`a\d+`, `^x$`, `a"b`, `\\`, `[a-z]{2,3}`, "tab\there", `(?i)abc`, `\x41`} {
		f.Add(seed)
	}
	literal := regexp.MustCompile(`~("(?:[^"\\]|\\.)*")`)
	f.Fuzz(func(t *testing.T, pattern string) {
		if strings.ContainsAny(pattern, "`{}|") || !isPrintableASCII(pattern) {
			t.Skip("not expressible as a backtick LogQL literal")
		}
		if _, err := regexp.Compile(pattern); err != nil {
			t.Skip("not a regexp Loki would accept")
		}
		got, err := TranslateLogQL("{app=~`" + pattern + "`}")
		if err != nil {
			t.Skip("not a selector the translator accepts")
		}
		match := literal.FindStringSubmatch(got)
		if match == nil {
			t.Skip("pattern did not reach a regexp filter")
		}
		decoded, err := strconv.Unquote(match[1])
		if err != nil {
			t.Fatalf("VictoriaLogs would reject %s (pattern %q): %v", match[1], pattern, err)
		}
		if decoded != pattern {
			t.Fatalf("backend receives %q, Loki compiles %q", decoded, pattern)
		}
	})
}

func isPrintableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}
