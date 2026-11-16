package proxy

import (
	"context"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"testing"
	"time"
)

// vlNotInPatterns pairs each probe regexp written with vlNotInRun with the [^set]* spelling it replaces: the two must
// match the same lines.
var vlNotInPatterns = []struct{ name, before, after string }{
	{"json escaped key", `\\u[0-9A-Fa-f]{4}[^"]*"\s*:`, orderedJSONEscapedKey},
	{"detected_level escaped value", detectedLevelJSONKey + `\s*"[^"\\]*\\`, detectedLevelEscapedValue},
	{"json reject hazard escaped key", `\\(?s:.)[^"]*"\s*:`, `\\(?s:.)` + vlNotInRun(`"`) + `"\s*:`},
	{"whole line", `^[^"]*$`, `^` + vlNotInRun(`"`) + `$`},
	{"whole line, control bytes", `^[^\x00-\x20="]*$`, `^` + vlNotInRun(`\x00-\x20="`) + `$`},
	{"between literals", `a[^"\\]*b`, `a` + vlNotInRun(`"\\`) + `b`},
}

// vlNotInLines are lines that tell the spellings apart if anything does: escapes inside keys and values, quotes,
// backslashes, letters on both sides of the split, control bytes, newlines, non-ASCII and invalid UTF-8.
var vlNotInLines = []string{
	``, `"`, `a`, `Z`, `0`, `é`, "\x00", "\n", `\`, `ab`, `a"b`, `a\b`, `a\"b`, "a\nb", "a\xffb", `aZb`, `a b`,
	`{"k\u0041ey":"v"}`, `{"key\u00e9":1}`, `{"msg":"caf\u00e9 served","level":"info"}`, `{"a":"\u0041", "b": 1}`,
	`{"\u0041" : 1}`, `{"\u004Z":1}`, "{\"\\u0041\nx\":1}", `{"\u0041\"x":1}`, `{"k\uFFFD":"\u00e9"}`,
	`{"level":"in\"fo"}`, `{"level": "info\\n"}`, `{"LEVEL":"x\u0041"}`, `{"severity":"a"}`, `{"lvl":"\"\\"}`,
	`{"log.level":"warn\t\\"}`, `{"level":"ü\\x"}`, `{"level":"` + strings.Repeat("ab", 40) + `\\"}`,
	`{"x":"\q": 1}`, `\a"  :`, `\é": 1`, "\\\x00\": 1", `level=info msg="x y" job_id=job-1`, `=a`, ` a=b`,
	"a=b\x7f", `a="b\"c"`,
}

// conformance: quality/probe-regexp-parse-cost
func TestVLNotInMatchesTheNegatedClass(t *testing.T) {
	for _, p := range vlNotInPatterns {
		before, after := regexp.MustCompile(p.before), regexp.MustCompile(p.after)
		for _, line := range vlNotInLines {
			if b, a := before.MatchString(line), after.MatchString(line); b != a {
				t.Errorf("%s: %q: [^set]* matches %v, vlNotInRun %v", p.name, line, b, a)
			}
		}
	}
	// detectedLevelKeysQuery runs detectedLevelEscapedValue behind _msg:"\\": every line it matches holds a backslash.
	re := regexp.MustCompile(detectedLevelEscapedValue)
	for _, line := range vlNotInLines {
		if re.MatchString(line) && !strings.Contains(line, `\`) {
			t.Errorf("%q: matched without a backslash", line)
		}
	}
	if q := detectedLevelKeysQuery(`{a="b"}`, true); !strings.Contains(q, `_msg:"\\" _msg:~`+strconv.Quote(detectedLevelEscapedValue)) {
		t.Errorf("the escaped-value regexp is not behind the backslash filter: %s", q)
	}
}

// The probe regexps hold no class that is wide and holds the ASCII letters: such a class costs VictoriaLogs
// milliseconds per query parse (vlNotInRun).
//
// conformance: quality/probe-regexp-parse-cost
func TestVLNotInPatternsHoldNoWideLetterClass(t *testing.T) {
	for _, p := range vlNotInPatterns[:3] {
		re, err := syntax.Parse(p.after, syntax.Perl)
		if err != nil {
			t.Fatalf("%s: %v", p.name, err)
		}
		if wideLetterClass(re) {
			t.Errorf("%s: %s holds a wide class with the ASCII letters", p.name, p.after)
		}
		before, _ := syntax.Parse(p.before, syntax.Perl)
		if !wideLetterClass(before) {
			t.Errorf("%s: the [^set] spelling %s was expected to hold one", p.name, p.before)
		}
	}
}

// Printing the regexp is what VictoriaLogs repeats while it simplifies one; vlNotInRun keeps it in microseconds where
// [^set] takes about a millisecond.
//
// conformance: quality/probe-regexp-parse-cost
func TestVLNotInPrintsCheaply(t *testing.T) {
	if testing.Short() && raceDetectorEnabled {
		t.Skip("timing under the race detector")
	}
	for _, p := range vlNotInPatterns[:3] {
		re, err := syntax.Parse(p.after, syntax.Perl)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for i := 0; i < 20; i++ {
			_ = re.String()
		}
		// [^set] prints in about 1 ms each time; the bound leaves room for a slow runner.
		if d := time.Since(start) / 20; d > 200*time.Microsecond {
			t.Errorf("%s: printing takes %v", p.name, d)
		}
	}
}

// wideLetterClass reports whether re holds a character class spanning more than the ASCII range that also holds an
// ASCII letter.
func wideLetterClass(re *syntax.Regexp) bool {
	if re.Op == syntax.OpCharClass {
		wide, letter := false, false
		for i := 0; i+1 < len(re.Rune); i += 2 {
			lo, hi := re.Rune[i], re.Rune[i+1]
			wide = wide || hi-lo > 0x7f
			letter = letter || (lo <= 'Z' && hi >= 'A') || (lo <= 'z' && hi >= 'a')
		}
		if wide && letter {
			return true
		}
	}
	for _, sub := range re.Sub {
		if wideLetterClass(sub) {
			return true
		}
	}
	return false
}

// conformance: quality/probe-regexp-parse-cost
func FuzzVLNotInMatchesTheNegatedClass(f *testing.F) {
	for _, line := range vlNotInLines {
		f.Add(line)
	}
	compiled := make([][2]*regexp.Regexp, len(vlNotInPatterns))
	for i, p := range vlNotInPatterns {
		compiled[i] = [2]*regexp.Regexp{regexp.MustCompile(p.before), regexp.MustCompile(p.after)}
	}
	escapedValue := regexp.MustCompile(detectedLevelEscapedValue)
	f.Fuzz(func(t *testing.T, line string) {
		for i, p := range vlNotInPatterns {
			if b, a := compiled[i][0].MatchString(line), compiled[i][1].MatchString(line); b != a {
				t.Fatalf("%s: %q: [^set]* matches %v, vlNotInRun %v", p.name, line, b, a)
			}
		}
		if escapedValue.MatchString(line) && !strings.Contains(line, `\`) {
			t.Fatalf("%q: the escaped-value regexp matched a line without a backslash", line)
		}
	})
}

// vlNotInBenchLines are lines of the kinds the probes run these regexps over: generator-like JSON bodies with and
// without escapes, a level value with an escape, and a long body.
var vlNotInBenchLines = []string{
	`{"ts":"2026-11-16T04:20:16Z","level":"info","msg":"request served","path":"/api/v1/orders","status":200,"duration_ms":17,"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"}`,
	`{"ts":"2026-11-16T04:20:17Z","level":"debug","msg":"cache refreshed","entries":120,"service":"api-gateway"}`,
	`{"ts":"2026-11-16T04:20:18Z","level":"info","msg":"user logged in","user_id":"u-77","method":"POST"}`,
	`{"ts":"2026-11-16T04:20:16Z","level":"info","msg":"caf\u00e9 served","k\u0041ey":"v","user":"\u00fcser"}`,
	`{"level":"warn\n","msg":"line with an escaped newline in its level"}`,
	`{"ts":"2026-11-16T04:20:16Z","level":"error","msg":"` + strings.Repeat(`payload \u00e9 `, 40) + `","status":500}`,
}

// BenchmarkVLNotInMatch is the per-line cost of each changed probe filter in Go's regexp package, which VictoriaLogs'
// matcher runs, with the literal filter each runs behind in the probe: before = base (the [^set]* spelling), after =
// this change (vlNotInRun; the detected_level escaped value newly behind the backslash filter).
func BenchmarkVLNotInMatch(b *testing.B) {
	gates := []struct{ before, after string }{{`\u`, `\u`}, {"", `\`}, {`\`, `\`}}
	for i, p := range vlNotInPatterns[:3] {
		for _, spelling := range []struct{ name, re, gate string }{{"before", p.before, gates[i].before}, {"after", p.after, gates[i].after}} {
			re := regexp.MustCompile(spelling.re)
			b.Run(strings.ReplaceAll(p.name, " ", "_")+"/"+spelling.name, func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					for _, line := range vlNotInBenchLines {
						_ = strings.Contains(line, spelling.gate) && re.MatchString(line)
					}
				}
			})
		}
	}
}

// BenchmarkProbeQueryText builds the texts of the changed probes and the hazard filter.
func BenchmarkProbeQueryText(b *testing.B) {
	ctx := context.Background()
	base := `{service_name="api-gateway"}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = orderedJSONPartialParseQuery(ctx, base, []string{"level_extracted"}, nil, true)
		_ = detectedLevelKeysQuery(base, true)
		_ = jsonInvalidCondition(true)
	}
}
