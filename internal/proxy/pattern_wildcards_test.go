package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// lokiPatternLineMatches is Loki's pattern line filter (|>) as its documentation and answers define it, the filter
// Logs Drilldown adds when a pattern is included: each literal is found at its first occurrence after the text
// before it (a leading literal anywhere in the line), every capture holds at least one character, and a pattern
// that ends on a literal ends the line there.
func lokiPatternLineMatches(pattern, line string) bool {
	pos, capturing := 0, false
	for pattern != "" {
		if strings.HasPrefix(pattern, "<") {
			if end := strings.IndexByte(pattern, '>'); end > 0 {
				capturing = true
				pattern = pattern[end+1:]
				continue
			}
		}
		next := strings.IndexByte(pattern[1:], '<') + 1
		if next == 0 {
			next = len(pattern)
		}
		literal := pattern[:next]
		pattern = pattern[next:]
		i := strings.Index(line[pos:], literal)
		if i < 0 || capturing && i == 0 {
			return false
		}
		pos += i + len(literal)
		capturing = false
	}
	if capturing {
		return pos < len(line)
	}
	return pos == len(line)
}

// The reference matcher reads patterns as Loki's pattern line filter does.
//
// conformance: loki_api_v1_patterns
func TestLokiPatternLineMatches(t *testing.T) {
	for _, tc := range []struct {
		pattern, line string
		want          bool
	}{
		{"<_>", "x", true},
		{"<_>", "", false},
		{"GET <_> HTTP/1.1", "GET /a HTTP/1.1", true},
		{"GET <_> HTTP/1.1", "GET /a HTTP/1.1 extra", false},
		{"GET <_> HTTP/1.1", "GET  HTTP/1.1", false},
		{"a <_> b <_>", "x a 1 b 2", true},
		{"a <_> b <_>", "a 1 b ", false},
		{"<_>msg<_>", `{"m":"msg"}`, true},
		{"<_> - <_> [<_>]", "1.2.3.4 - admin [now]", true},
	} {
		if got := lokiPatternLineMatches(tc.pattern, tc.line); got != tc.want {
			t.Errorf("%q |> %q = %v, want %v", tc.line, tc.pattern, got, tc.want)
		}
	}
}

func patternRowsOf(level string, lines ...string) []byte {
	var b strings.Builder
	for i, line := range lines {
		row, _ := json.Marshal(map[string]string{"_time": fmt.Sprintf("2026-11-06T10:%02d:%02dZ", i/60%60, i%60), "_msg": line, "level": level})
		b.Write(row)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// Patterns are wildcarded as Loki's are: a value that differs between lines of one kind is <_>, so a kind of line
// is one pattern, not one per path, user agent or number; a value that never varies stays. Every line a pattern
// was mined from still matches it under the pattern line filter Logs Drilldown adds when the pattern is included.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternsWildcardEveryVaryingValue(t *testing.T) {
	var lines []string
	paths := []string{"/api/v1/cart", "/api/v1/users", "/healthz", "/api/v1/orders/17", "/static/app.js", "/login"}
	agents := []string{"kube-probe/1.28", "sqlmap/1.7.8#stable (https://sqlmap.org)", "Mozilla/5.0 (X11; Linux x86_64)", "curl/8.4.0", "Go-http-client/1.1", "python-requests/2.31"}
	users := []string{"-", "admin"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	for i := 0; i < 240; i++ {
		lines = append(lines,
			fmt.Sprintf(`10.0.%d.%d - %s [06/Nov/2026:10:%02d:%02d +0000] "%s %s HTTP/1.1" %d %d "-" "%s" %d`,
				i%7, i%250, users[i%2], i%60, i%59, methods[i%4], paths[i%6], []int{200, 201, 404, 500}[i%4], 100+i*13, agents[i%6], i%9),
			fmt.Sprintf("worker %d processed %d items in %dms", i%5, i*3, i%70),
			fmt.Sprintf("component=collector status=200 took %dms", 5+i%40),
		)
	}
	patterns := extractLogPatterns(patternRowsOf("info", lines...), "1m", 1000, defaultLogRowLevels())
	var texts []string
	for _, p := range patterns {
		texts = append(texts, p["pattern"].(string))
	}
	for _, text := range texts {
		for _, raw := range append(append([]string{"/api/v1/cart", "sqlmap", "kube-probe", "06/Nov"}, paths[1:]...), agents[2:]...) {
			if strings.Contains(text, raw) {
				t.Errorf("pattern %q keeps the varying value %q", text, raw)
			}
		}
	}
	if len(texts) > 12 {
		t.Errorf("%d patterns for three kinds of lines: %q", len(texts), texts)
	}
	for _, want := range []string{"component=collector status=200 took <_>"} {
		found := false
		for _, text := range texts {
			found = found || text == want
		}
		if !found {
			t.Errorf("pattern %q missing: a value that never varies stays (%q)", want, texts)
		}
	}
	for _, line := range lines {
		matched := false
		for _, text := range texts {
			if lokiPatternLineMatches(text, line) {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("line %q matches none of its patterns %q", line, texts)
		}
	}
}

// Keys stay: logfmt lines with different keys in a place are different kinds of line, never key <_>=<_>. A value
// that differs only with the level (level=info in the info pattern, level=warn in the warn one) stays in each.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternsKeepKeysAndLevels(t *testing.T) {
	var body []byte
	for _, level := range []string{"info", "warn", "error"} {
		var lines []string
		for i := 0; i < 30; i++ {
			lines = append(lines,
				fmt.Sprintf(`level=%s msg="cache hit" key=k%d ttl=%ds size_bytes=%d op=get`, level, i, i%9+1, 100+i*7),
				fmt.Sprintf(`level=%s msg="cache write" key=k%d op=set ttl=%ds size_bytes=%d`, level, i, i%9+1, 100+i*7),
			)
		}
		body = append(body, patternRowsOf(level, lines...)...)
	}
	patterns := extractLogPatterns(body, "1m", 1000, defaultLogRowLevels())
	got := map[string]bool{}
	for _, p := range patterns {
		text, level := p["pattern"].(string), p["level"].(string)
		got[level+" "+text] = true
		if strings.Contains(text, "<_>=") {
			t.Errorf("%s pattern %q wildcards a key", level, text)
		}
		if !strings.HasPrefix(text, "level="+level+" ") {
			t.Errorf("%s pattern %q does not keep its level", level, text)
		}
	}
	for _, want := range []string{
		`warn level=warn msg="cache hit" key=<_> ttl=<_> size_bytes=<_> op=get`,
		`warn level=warn msg="cache write" key=<_> op=set ttl=<_> size_bytes=<_>`,
	} {
		if !got[want] {
			t.Errorf("pattern %q missing from %v", want, got)
		}
	}
}

// Wildcarding never loses a line: every line that reads as its cluster's pattern as mined still matches a pattern
// once varying values are variables, whatever the lines. (A line the mined pattern matches only because its leading
// literal may start anywhere, such as a logfmt line with a key twice, can be lost when a variable takes that
// place; the miner keeps no lines to check.)
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func FuzzPatternWildcardsKeepTheirLines(f *testing.F) {
	for _, seed := range [][3]string{
		{`1.2.3.4 - admin [x +0000] "GET /a HTTP/1.1" 200 1 "-" "curl/8" 3`, `5.6.7.8 - - [y +0000] "GET /b/c HTTP/1.1" 404 22 "-" "kube-probe/1.28" 0`, `9.9.9.9 - - [z +0000] "GET /d HTTP/1.1" 500 3 "-" "sqlmap/1.7.8#stable (https://sqlmap.org)" 1`},
		{"level=info msg=\"cache hit\" key=k1 ttl=5s", "level=info msg=\"cache miss\" key=k2 ttl=9s", "level=info msg=done key=k3 ttl=1s"},
		{`{"message":"user login failed for alice"}`, `{"message":"user login failed for bob"}`, `{"msg":"user login ok for carol now"}`},
		{"worker 1 processed 3 items", "worker 2 processed 30 items", "worker 3 processed 300 items"},
		{"a b c d", "a b c e", "a b x d"},
		{`k="a b" c d e`, `k="a c" c d f`, `k="x" c d g`},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, a, b, c string) {
		lines := []string{a, b, c}
		for _, line := range lines {
			// <> would read as a capture in the pattern.
			if strings.ContainsAny(line, "\n\r<>") {
				return
			}
		}
		miner := newPatternMiner()
		for _, line := range lines {
			miner.Observe("info", line, 0)
		}
		var mined []string
		for _, c := range miner.AllClusters() {
			mined = append(mined, miner.joinPatternTokens(c.tokens, c.spacesAfter, c.format))
		}
		entries := patternEntriesByLevelAndPattern(miner)
		for _, line := range lines {
			before := false
			for _, pattern := range mined {
				before = before || pattern == line
			}
			if !before {
				continue
			}
			after := false
			for _, e := range entries {
				after = after || lokiPatternLineMatches(e.pattern, line)
			}
			if !after {
				var patterns []string
				for _, e := range entries {
					patterns = append(patterns, e.pattern)
				}
				t.Fatalf("line %q matched %q as mined, none of %q once wildcarded", line, mined, patterns)
			}
		}
	})
}
