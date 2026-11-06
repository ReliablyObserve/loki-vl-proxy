package proxy

import (
	"strings"
	"testing"
)

// A line is mined by the format Loki's pattern ingester gives it: a JSON object from its first top-level message
// field holding a string, written between variables, and nothing when it has none; a logfmt line from its keys
// and values, timestamps and trace ids as variables, written back as logfmt; any other line as it reads.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternLineTokensByFormat(t *testing.T) {
	miner := newPatternMiner()
	for _, tc := range []struct {
		line    string
		format  patternFormat
		ok      bool
		pattern string
	}{
		{`{"message":"user login failed for account","level":"info"}`, patternFormatJSON, true, "<_>user login failed for account<_>"},
		{`{"msg":123,"message":"queue drained for tenant now"}`, patternFormatJSON, true, "<_>queue drained for tenant now<_>"},
		{`{"message":"second field wins never here","log":"first field wins over message"}`, patternFormatJSON, true, "<_>first field wins over message<_>"},
		{`{"content":"page rendered for user quickly"}`, patternFormatJSON, true, "<_>page rendered for user quickly<_>"},
		{`{"nested":{"message":"only top level fields count here"}}`, patternFormatJSON, false, ""},
		{`{"service":{"name":"batch-etl"},"job":"export-orders","total":51503,"level":"warn"}`, patternFormatJSON, false, ""},
		{`{"message":"ok"}`, patternFormatJSON, false, ""},
		{`{not json at all here}`, patternFormatJSON, false, ""},
		{`{}`, patternFormatJSON, false, ""},
		{`level=info msg="cache hit" key=k1 ttl=5s`, patternFormatLogfmt, true, `level=info msg="cache hit" key=k1 ttl=5s`},
		{`ts=2026-11-06T10:00:00Z level=info msg=done id=7`, patternFormatLogfmt, true, `ts=<_> level=info msg=done id=7`},
		{`time=1 TraceID=abc level=info msg=done`, patternFormatLogfmt, true, `time=<_> TraceID=<_> level=info msg=done`},
		{`level=info msg=hello world=`, patternFormatPlain, true, `level=info msg=hello world=`},
		{`level=info  msg=hello other=v`, patternFormatPlain, true, `level=info  msg=hello other=v`},
		{`GET /api/v1/users 200 15ms`, patternFormatPlain, true, `GET /api/v1/users 200 15ms`},
		{`a b`, patternFormatPlain, false, ""},
	} {
		tokens, spaces, format, ok := miner.patternLineTokens(tc.line)
		if format != tc.format || ok != tc.ok {
			t.Errorf("%q: format %d ok %v, want %d %v (tokens %q)", tc.line, format, ok, tc.format, tc.ok, tokens)
			continue
		}
		if !ok {
			continue
		}
		if got := miner.joinPatternTokens(tokens, spaces, format); got != tc.pattern {
			t.Errorf("%q: pattern %q, want %q", tc.line, got, tc.pattern)
		}
	}
}

// Loki's logfmt rule: the whole line is key=value pairs, one space apart, a value bare or quoted.
func TestIsPatternLogfmtLine(t *testing.T) {
	for line, want := range map[string]bool{
		`a=1`:                          true,
		`a=1 b="two words" c=x`:        true,
		`level=info msg="cache hit"`:   true,
		`a=1  b=2`:                     false,
		`a=1 b=`:                       false,
		`a= b=1`:                       false,
		`=1`:                           false,
		`a=1 `:                         false,
		`a-b=1`:                        false,
		`a=""`:                         false,
		`a="unterminated`:              false,
		`plain text line`:              false,
		`a=1 trailing words here`:      false,
		`k=v:w/x.y k2=https://h/p?q=1`: true,
	} {
		if got := isPatternLogfmtLine(line); got != want {
			t.Errorf("isPatternLogfmtLine(%q) = %v, want %v", line, got, want)
		}
	}
}

// A logfmt value is written bare unless logfmt has to quote it.
func TestPatternLogfmtValue(t *testing.T) {
	for value, want := range map[string]string{
		"get":       "get",
		"<_>":       "<_>",
		"two word":  `"two word"`,
		"a=b":       `"a=b"`,
		`say "hi"`:  `"say \"hi\""`,
		"tab\there": `"tab\there"`,
		"":          "",
	} {
		if got := patternLogfmtValue(value); got != want {
			t.Errorf("patternLogfmtValue(%q) = %q, want %q", value, got, want)
		}
	}
}

// A pattern is never a raw JSON line: JSON lines are mined from their message field or not at all, by either path.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternsNeverAnswerRawJSONLines(t *testing.T) {
	var rows []string
	for i := 0; i < 40; i++ {
		rows = append(rows,
			`{"_time":"2026-11-06T10:00:01Z","_msg":"{\"service\":{\"name\":\"batch-etl\"},\"job\":\"export-orders\",\"batch_id\":\"batch_ff0ed0\",\"total\":51503,\"level\":\"warn\"}"}`,
			`{"_time":"2026-11-06T10:00:02Z","_msg":"{\"message\":\"payment settled for order quickly\",\"order\":`+strings.Repeat("7", i%5+1)+`}"}`,
		)
	}
	patterns := extractLogPatterns([]byte(strings.Join(rows, "\n")), "1m", 100, defaultLogRowLevels())
	if len(patterns) != 1 {
		t.Fatalf("patterns %v, want the message field's one", patterns)
	}
	for _, p := range patterns {
		if got := p["pattern"].(string); strings.HasPrefix(got, "{") || got != "<_>payment settled for order quickly<_>" {
			t.Fatalf("pattern %q", got)
		}
	}
	miner := newPatternMiner()
	if c := miner.observeGroup("info", `{"total":1,"id":"a"}`, `{"total":9,"id":"a"}`); c != nil {
		t.Fatalf("a JSON group without a message field was mined: %q", c.tokens)
	}
}

func FuzzPatternLineTokens(f *testing.F) {
	for _, seed := range []string{
		`{"message":"user login failed for account","level":"info"}`,
		`{"service":{"name":"x"},"total":1}`,
		`level=info msg="cache hit" key=k1 ttl=5s`,
		`ts=1 a=b c=d e=f`,
		`GET /api/v1/users 200 15ms`,
		`{"msg":"{\"inner\":1} a b c"}`,
		`{`, `}`, `a="b`, `=`,
	} {
		f.Add(seed)
	}
	miner := newPatternMiner()
	f.Fuzz(func(t *testing.T, line string) {
		tokens, spaces, format, ok := miner.patternLineTokens(line)
		if !ok {
			if format == patternFormatPlain && tokens != nil {
				t.Fatalf("%q: rejected with tokens %q", line, tokens)
			}
			return
		}
		if len(tokens) < patternMinTokens || len(tokens) > patternMaxTokens {
			t.Fatalf("%q: %d tokens", line, len(tokens))
		}
		pattern := miner.joinPatternTokens(tokens, spaces, format)
		if format == patternFormatJSON && !strings.HasPrefix(pattern, patternVarPlaceholder) {
			t.Fatalf("%q: JSON pattern %q does not start with a variable", line, pattern)
		}
		if format == patternFormatLogfmt && len(tokens)%2 != 0 {
			t.Fatalf("%q: logfmt tokens %q are not key, value pairs", line, tokens)
		}
	})
}
