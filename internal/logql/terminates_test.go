package logql

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode"
)

// entryPoints are the functions of the package that read text a client sends: every one must return, in bounded
// time and with bounded memory, on any input.
var entryPoints = []struct {
	name string
	run  func(q string)
}{
	{"Parse", func(q string) { _, _ = Parse(q) }},
	{"ParseAndValidate", func(q string) { _ = ParseAndValidate(q) }},
	{"ParseLogQuery", func(q string) { _, _ = ParseLogQuery(q) }},
	{"ValidateLogQL", func(q string) { _ = ValidateLogQL(q) }},
	{"ValidateMatchersQuery", func(q string) { _ = ValidateMatchersQuery(q) }},
	{"ValidateLogSelectorQuery", func(q string) { _ = ValidateLogSelectorQuery(q) }},
	{"ValidateSeriesMatchers", func(q string) { _ = ValidateSeriesMatchers([]string{q}) }},
	{"CanonicalizeLogRanges", func(q string) { _ = CanonicalizeLogRanges(q) }},
	{"LowercaseKeywords", func(q string) { _ = LowercaseKeywords(q) }},
	{"StripComments", func(q string) { _ = StripComments(q) }},
	{"StripParserFlags", func(q string) { _ = StripParserFlags(q) }},
	{"MayHaveNonFilterStage", func(q string) { _ = MayHaveNonFilterStage(q) }},
	{"DottedNameError", func(q string) { _ = DottedNameError(q) }},
	{"ParseLabelFilterExpr", func(q string) { _, _ = ParseLabelFilterExpr(q) }},
	{"ParserCaptureLabels", func(q string) { _ = ParserCaptureLabels(q) }},
	{"ParseLineTemplate", func(q string) { _ = ParseLineTemplate(q) }},
	{"AggregationArgs", func(q string) { _, _ = AggregationArgs(q, "sum") }},
	{"UnwrapParens", func(q string) { _ = UnwrapParens(q) }},
	{"FirstArgument", func(q string) { _, _ = FirstArgument(q) }},
	{"TopLevelCommaIndex", func(q string) { _ = TopLevelCommaIndex(q) }},
}

const (
	// fuzzTerminationTime is the bound of the fuzz target, which runs alone in a fuzz-smoke job.
	fuzzTerminationTime = 200 * time.Millisecond
	// terminationTime is the bound of the deterministic tests. They run inside `go test ./... -coverprofile` next to
	// every other package on a shared runner, where an entry point that returns in microseconds can still wait
	// hundreds of milliseconds for a CPU; a parser that never returns misses any bound, so the bound is seconds.
	terminationTime = 5 * time.Second
	// coveredTerminationTime is that bound for a run with coverage counters (slower, and more loaded).
	coveredTerminationTime = 20 * time.Second
	// allocationPerByte is what an entry point may allocate for each byte of its input (and a base of 16 bytes).
	allocationPerByte = 1 << 20
)

// terminationBound is the time a deterministic test allows an entry point.
func terminationBound() time.Duration {
	if testing.CoverMode() != "" {
		return coveredTerminationTime
	}
	return terminationTime
}

// terminationError is terminationErrorWithin at the bound of the deterministic tests.
func terminationError(q string) error {
	return terminationErrorWithin(q, terminationBound())
}

// terminationErrorWithin runs every entry point on q, each in a goroutine, and reports one that does not return
// within bound or that allocates more than allocationPerByte per input byte. A parser that does not return keeps its
// goroutine for the life of the process: the caller fails the test, and the process ends.
func terminationErrorWithin(q string, bound time.Duration) error {
	limit := uint64(len(q)+16) * allocationPerByte
	for _, ep := range entryPoints {
		var before, after runtime.MemStats
		done := make(chan struct{})
		runtime.ReadMemStats(&before)
		start := time.Now()
		go func() {
			defer close(done)
			ep.run(q)
		}()
		select {
		case <-done:
		case <-time.After(bound):
			return fmt.Errorf("%s does not return within %v on %q", ep.name, bound, q)
		}
		runtime.ReadMemStats(&after)
		if used := after.TotalAlloc - before.TotalAlloc; used > limit {
			return fmt.Errorf("%s allocates %d bytes on %d bytes of input (limit %d) in %v on %q", ep.name, used, len(q), limit, time.Since(start), q)
		}
	}
	return nil
}

// terminationSeeds are inputs around the places where the scanner meets a character that is no ASCII digit: a
// decimal digit of another script after a name, in a comparison, in a number, a duration, a range, an offset, a
// list and an argument, and the other categories of the grid.
func terminationSeeds() []string {
	return []string{
		"{a=\"b\"} | x ٣", "{a=\"b\"} | x > ٣", "{a=\"b\"} | x=٣", "{a=\"b\"} | json | x == ٣1", "{a=\"b\"} | x >= 1٣",
		"{}|\xec:A\xb9߆", "{a=\"b\"} | x ०", "{a=\"b\"} | x ３", "{a=\"b\"} | x \U0001d7d1", "{a=\"b\"} | x ²", "{a=\"b\"} | x ⅓", "{a=\"b\"} | x Ⅳ",
		"rate({a=\"b\"}[٣5m])", "rate({a=\"b\"}[5٣m])", "rate({a=\"b\"}[5m] offset ٣m)", "sum by (٣) (rate({a=\"b\"}[5m]))", "topk(٣5, rate({a=\"b\"}[5m]))",
		"vector(٣1)", "1 + ٣1", "{a=\"b\"} | line_format \"٣\"", "{a=\"b\"} | x < 5٣m", "{a=\"b\"} | x < 0x٣", "{a=\"b\"} | x < 1e٣", "{a=\"b\"} | x < 1_٣",
		"\xff", "\xc3", "\xe2\x82", "\xed\xa0\x80", "\xf4\x90\x80\x80", "{a=\"b\"} | x \xff", "{a=\"b\"} |= \"\xc3\"", "{a=\"\xe2\x82\"}", "rate({a=\"b\"}[5\xff])",
	}
}

// Every entry point returns, quickly and with bounded memory, on the inputs where the scanner once made no
// progress and on the Unicode categories of the grid in the positions of a number.
//
// conformance: semantics/logql-unicode-runes
func TestEntryPointsTerminate(t *testing.T) {
	for _, q := range terminationSeeds() {
		if err := terminationError(q); err != nil {
			t.Fatal(err)
		}
	}
	// Every ninth query of the grid; TestUnicodeRunesMatchLoki runs ValidateLogQL on all of them with a time limit.
	for i, q := range unicodeRuneQueries() {
		if i%9 == 0 {
			if err := terminationError(q); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// decimalDigitSamples is, for every block of decimal digits Unicode defines (category Nd), its first, a middle and its
// last digit.
func decimalDigitSamples() []rune {
	var out []rune
	add := func(lo, hi rune) {
		out = append(out, lo, lo+(hi-lo)/2, hi)
	}
	for _, r := range unicode.Nd.R16 {
		add(rune(r.Lo), rune(r.Hi))
	}
	for _, r := range unicode.Nd.R32 {
		add(rune(r.Lo), rune(r.Hi))
	}
	return out
}

// Any decimal digit that is no ASCII digit, at any place a number, a name, a unit or a list may stand, ends the scan
// and the parse.
//
// conformance: semantics/logql-unicode-runes
func TestEveryDecimalDigitTerminates(t *testing.T) {
	positions := []string{
		`{a="b"} | x ¤`, `{a="b"} | x > ¤`, `{a="b"} | x=¤`, `{a="b"} | json | x == ¤1`, `{a="b"} | x >= 1¤`, `{a="b"} | x < 1.5¤`, `{a="b"} | x < 5¤m`,
		`{a="b"} | x < 0x¤`, `{a="b"} | x < 1e¤`, `{a="b"} | x < .¤`, `{a="b"} | ¤ = "x"`, `{a="b"} | x¤y = "z"`, `1 + ¤1`, `1¤ + 1`,
		`rate({a="b"}[¤5m])`, `rate({a="b"}[5m] offset ¤5m)`, `sum by (¤) (rate({a="b"}[5m]))`, `topk(¤5, rate({a="b"}[5m]))`,
		`vector(¤1)`, `quantile_over_time(0.9¤, {a="b"} | unwrap x [5m])`, `{a="b"} | label_format a=¤b`, `{a="b"} | drop ¤`,
	}
	digits := decimalDigitSamples()
	if len(digits) < 150 {
		t.Fatalf("%d sample digits: the blocks of category Nd are missing", len(digits))
	}
	for _, d := range digits {
		for _, tpl := range positions {
			q := strings.ReplaceAll(tpl, runeMark, string(d))
			if err := terminationError(q); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// The queries that made the parser loop on main, with Loki's answer to each: all are rejected, and the proxy
// returns for every one.
//
// conformance: semantics/logql-unicode-runes
func TestHangingQueriesAreRejectedLikeLoki(t *testing.T) {
	f, err := os.Open("testdata/unicode_hang_loki.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	n, same := 0, 0
	for sc.Scan() {
		var a struct {
			Q      string `json:"q"`
			Accept bool   `json:"accept"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(sc.Bytes(), &a); err != nil {
			t.Fatal(err)
		}
		n++
		if err := terminationError(a.Q); err != nil {
			t.Fatal(err)
		}
		got := ValidateLogQL(a.Q)
		if a.Accept || got == "" {
			t.Errorf("%q: Loki accepts = %v, the proxy's answer %q", a.Q, a.Accept, got)
			continue
		}
		if got == a.Error {
			same++
		}
	}
	if n != 80 {
		t.Fatalf("%d queries, want 80", n)
	}
	t.Logf("%d queries, %d with Loki's text", n, same)
	// The other eight are a range with no function around it (`{a="b"}[٣]`): Loki's lexer rejects the bracket text
	// as a duration before the parser sees the token, the proxy rejects the token ("unexpected RANGE").
	if same < 72 {
		t.Errorf("%d of %d answers have Loki's text, want at least 72", same, n)
	}
}

// FuzzParseTerminates runs every entry point of the package on the input, each in a goroutine, and fails when one
// does not return within 200 ms or allocates more than a mebibyte per byte of input: a parser of client text must
// always terminate with bounded memory.
func FuzzParseTerminates(f *testing.F) {
	for _, q := range terminationSeeds() {
		f.Add(q)
	}
	for _, q := range []string{`{a="b"}`, `{a="b"} |= "x" | json | x > 5`, `sum by (a) (rate({a="b"}[5m]))`, `rate({a="b"}[5m]) / on(a) rate({a="b"}[5m])`, "{a=`b`} # c\n| logfmt", `1 + 1`, ``,
		// label lists: a trailing, doubled and leading comma, two names, an empty list
		`sum by (a,) (rate({a="b"}[5m]))`, `sum without (,a) (rate({a="b"}[5m]))`, `sum by (a b) (rate({a="b"}[5m]))`, `sum by () (rate({a="b"}[5m]))`,
		`rate({a="b"}[5m]) / on(a,) group_left(,b) rate({a="b"}[5m])`, `{a="b"} | drop a,,b`, `{a="b"} | keep`, `{a="b"} | drop a="x",`, `{a="b"} | json a,`} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		if err := terminationErrorWithin(q, fuzzTerminationTime); err != nil {
			t.Fatal(err)
		}
	})
}
