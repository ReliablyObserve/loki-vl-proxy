package logsql_test

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// VictoriaLogs unquotes a double-quoted LogsQL string literal with Go semantics
// before it compiles the regexp, so the literal the proxy emits has to unquote
// back to the regexp it meant. Verified against VictoriaLogs v1.52.0: `~"\d+"`
// is answered with
//
//	HTTP 400 cannot read regexp for field "_msg": compound token cannot start
//	with "\""; put it into quotes if needed
//
// while `~"\\d+"` returns the matching rows.
func TestRegexpLiteralUnquotesBackToThePattern(t *testing.T) {
	patterns := []string{
		`\d+`,
		`\w+\s*`,
		`[ \t]`,
		`^192\.168\.1\.`,
		`say "hi"`,
		`a\\b`,
		`(?i)error|warn`,
		``,
	}
	for _, pattern := range patterns {
		t.Run(pattern, func(t *testing.T) {
			if _, err := regexp.Compile(pattern); err != nil {
				t.Fatalf("test input is not a valid regexp: %v", err)
			}

			literal := logsql.QuotePattern(pattern)
			got, err := strconv.Unquote(literal)
			if err != nil {
				t.Fatalf("QuotePattern(%q) = %s, which VictoriaLogs cannot unquote: %v", pattern, literal, err)
			}
			if got != pattern {
				t.Errorf("QuotePattern(%q) = %s, unquotes to %q", pattern, literal, got)
			}
		})
	}
}

// Every emitter that puts a regexp into a query has to use the same quoting:
// the field filter, the bare line-filter regexp and the replace_regexp pipe.
func TestRegexpEmittersQuoteConsistently(t *testing.T) {
	const pattern = `^/api/v\d+/`

	cases := map[string]string{
		"field_filter": logsql.FieldFilter{Field: "url", Op: logsql.FieldOpRegexp, Value: pattern}.String(),
		"line_regexp":  logsql.Regexp{Pattern: pattern}.String(),
		"replace_regexp": logsql.PipeReplaceRegexp{
			Field: "url", Regex: pattern, Replacement: "",
		}.String(),
	}

	quoted := logsql.QuotePattern(pattern)
	for name, emitted := range cases {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(emitted, quoted) {
				t.Errorf("%s emitted %s, which does not carry the quoted regexp %s", name, emitted, quoted)
			}
		})
	}
}
