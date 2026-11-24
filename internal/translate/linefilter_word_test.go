package translate

import (
	"regexp"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// vlPhrase evaluates a VictoriaLogs phrase filter over one line the way lib/logstorage getPhrasePos does: the phrase
// is a substring, and a phrase that starts (ends) with a token character is preceded (followed) by a non-token
// character or the edge of the line. Token characters are Unicode letters and digits and _.
func vlPhrase(line, phrase string) bool {
	isToken := func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }
	first, _ := utf8.DecodeRuneInString(phrase)
	last, _ := utf8.DecodeLastRuneInString(phrase)
	for off := 0; off <= len(line)-len(phrase); {
		i := strings.Index(line[off:], phrase)
		if i < 0 {
			return false
		}
		pos := off + i
		ok := true
		if isToken(first) && pos > 0 {
			if r, _ := utf8.DecodeLastRuneInString(line[:pos]); r == utf8.RuneError || isToken(r) {
				ok = false
			}
		}
		if ok && isToken(last) && pos+len(phrase) < len(line) {
			if r, _ := utf8.DecodeRuneInString(line[pos+len(phrase):]); r == utf8.RuneError || isToken(r) {
				ok = false
			}
		}
		if ok {
			return true
		}
		off = pos + 1
	}
	return false
}

func phraseKept(t *testing.T, f logsql.FilterExpr, line string) bool {
	t.Helper()
	switch n := f.(type) {
	case logsql.Phrase:
		return vlPhrase(line, n.Value)
	case logsql.NotExpr:
		return !phraseKept(t, n.Expr, line)
	case logsql.Group:
		return phraseKept(t, n.Expr, line)
	case logsql.AnyOf:
		for _, e := range n {
			if phraseKept(t, e, line) {
				return true
			}
		}
		return false
	}
	t.Fatalf("unexpected node %T (%s)", f, f)
	return false
}

// A regexp line filter that is a literal between word boundaries is a phrase filter, which VictoriaLogs answers from
// its bloom filters; the phrase keeps the lines the regexp keeps.
func TestWordBoundaryFilter(t *testing.T) {
	lines := []string{"failed", "x failed y", "failed:", "(failed)", `"status":"failed"`, "unfailed", "failed_at", "failedx", "fail ed",
		"foo bar", "xfoo bar", "foo barx", "a foo bar.", "error", "errors", "FAILED", "é failed", "a.b", "a_b", "1failed", "failed1"}
	for _, c := range []struct {
		pattern string
		want    string // "" when the regexp stays a regexp
	}{
		{`\bfailed\b`, `"failed"`},
		{`(\bfailed\b)`, `"failed"`},
		{`(?:\bfailed\b)`, `"failed"`},
		{`\bfoo bar\b`, `"foo bar"`},
		{`\ba\.b\b`, `"a.b"`},
		{`\ba_b\b`, `"a_b"`},
		{`\b(?:error|failed)\b`, `("error" OR "failed")`},
		{`\b(error|failed)\b`, `("error" OR "failed")`},
		{`\bfailed\b|\berror\b`, ""}, // two bounded literals in an alternation stay a regexp
		{`failed`, ""},
		{`\bfailed`, ""},
		{`failed\b`, ""},
		{`(?i)\bfailed\b`, ""},
		{`\b:failed\b`, ""},
		{`\bfailed:\b`, ""},
		{`\bé\b`, ""},
		{`\bfail\w+\b`, ""},
		{`\bfail(ed|ure)\b`, ""},
		{`\b\b`, ""},
		{`\b(?:error|fail.)\b`, ""},
	} {
		f, ok := WordBoundaryFilter(c.pattern)
		if (c.want != "") != ok {
			t.Errorf("%q: ok=%v, want %v", c.pattern, ok, c.want != "")
			continue
		}
		if !ok {
			continue
		}
		if got := f.String(); got != c.want {
			t.Errorf("%q: got %s, want %s", c.pattern, got, c.want)
		}
		re := regexp.MustCompile(c.pattern)
		for _, line := range lines {
			if strings.ContainsRune(line, 'é') {
				continue // the documented difference: a non-ASCII letter is a boundary for Go and a token for VictoriaLogs
			}
			if want, got := re.MatchString(line), phraseKept(t, f, line); want != got {
				t.Errorf("%q on %q: regexp %v, phrase %v", c.pattern, line, want, got)
			}
		}
	}
}

// The stage translator writes the phrase for |~ and its negation for !~; an or chain keeps the regexp.
func TestLineFilterWordBoundary(t *testing.T) {
	for _, c := range []struct {
		stage logql.LineFilterStage
		want  string
	}{
		{logql.LineFilterStage{Op: logql.LineFilterMatchRe, Value: `\bfailed\b`}, `"failed"`},
		{logql.LineFilterStage{Op: logql.LineFilterExcludeRe, Value: `\bfailed\b`}, `NOT "failed"`},
		{logql.LineFilterStage{Op: logql.LineFilterMatchRe, Value: `\b(?:aa|bb)\b`}, `("aa" OR "bb")`},
		{logql.LineFilterStage{Op: logql.LineFilterExcludeRe, Value: `\b(?:aa|bb)\b`}, `NOT ("aa" OR "bb")`},
		{logql.LineFilterStage{Op: logql.LineFilterMatchRe, Value: `\bfailed\b`, Or: []logql.LineFilterAlt{{Value: `\berror\b`}}}, `~"(?:\\bfailed\\b)|(?:\\berror\\b)"`},
		{logql.LineFilterStage{Op: logql.LineFilterContains, Value: `\bfailed\b`}, `~"\\\\bfailed\\\\b"`},
	} {
		f, ok := LineFilter(&c.stage)
		if !ok || f == nil || f.String() != c.want {
			t.Errorf("%s: got %v %v, want %s", c.stage.String(), ok, f, c.want)
		}
	}
	for _, c := range []struct {
		literal string
		negate  bool
		want    string
		ok      bool
	}{
		{`"\\bfailed\\b"`, false, `"failed"`, true},
		{`"\\bfailed\\b"`, true, `NOT "failed"`, true},
		{`"failed"`, false, "", false},
		{"`\\bfailed\\b`", false, "", false},
	} {
		got, ok := WordBoundaryFilterText(c.literal, c.negate)
		if ok != c.ok || got != c.want {
			t.Errorf("%s negate=%v: got %q %v, want %q %v", c.literal, c.negate, got, ok, c.want, c.ok)
		}
	}
}
