package logql

import (
	"math"
	"math/big"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// rangeDurationTexts are the texts of a [range]: Prometheus' durations Loki reads (whole numbers, the units y, w, d,
// h, m, s and ms each once and in that order, a sum that fits a duration) and the ones it rejects, each for its own
// reason (a fraction, Go's ns, us and µs, a repeated or out-of-order unit, a number without a unit, a number above 64
// bits, a sum too long, blanks).
var rangeDurationTexts = []string{
	"5m", "1ms", "5m30s", "1h30m", "1h30m10s100ms", "1y2w3d4h5m6s7ms", "0s", "00m", "1m0s", "0ms", "05m", "5s5ms",
	"2562047h", "106751d", "292y", "15250w", "2562047h47m16s854ms", "1y0ms",
	"1us", "1µs", "1ns", "5m1µs", "1.5m", "1.5h", "1h1.5m", "5m5m", "1h1h", "1y1y", "30s5m", "5ms5s", "1ms5h", "1d1h1d",
	"5m5m5m", "5m5m5x", "1y1y5x", "5m5", "5h5", "5", "00", "m", "5x", "5mx", "5mm", "5M", "1H", "5m5x", "5x5m", "5m5m.",
	"99999999999999999999h", "100000000000000000000ms", "9999999999h", "9999999999h5x", "2562048h", "106752d", "293y",
	"292y52w", "15251w", "9223372036854775807ms", "9223372037s", "2562047h47m16s855ms",
	" 5m", "5m ", " 5m ", "5 m", "5m\t", "\t5m", "5m\n", "5m:1m", "1.5m:1m", "5m#x", "5m-1s", "-5m", "+5m", "", "0", "0x1", "1e3s", "1_000s",
}

// rangeDurationGrid puts every range text in the places a range stands: a range aggregation, one under a vector
// aggregation, one with a pipeline, an unwrap, an offset and the right operand of a binary operation.
func rangeDurationGrid() []string {
	var grid []string
	for _, d := range rangeDurationTexts {
		grid = append(grid,
			`rate({a="b"}[`+d+`])`,
			`sum by (a) (count_over_time({a="b"} | json | x="y" [`+d+`]))`,
			`quantile_over_time(.5, {a="b"} | json | unwrap c[`+d+`])`,
			`sum_over_time({a="b"} | unwrap c [`+d+`] offset 1h)`,
			`rate({a="b"}[5m] offset 5m) / rate({a="b"}[`+d+`])`,
		)
	}
	// An offset is read with Go's time.ParseDuration: the units and spellings a range rejects stay accepted there.
	for _, d := range []string{"1us", "1µs", "1ns", "5m5m", "1.5m", "1h30m", "-5m", "30s5m"} {
		grid = append(grid, `rate({a="b"}[5m] offset `+d+`)`, `sum_over_time({a="b"} | unwrap c [5m] offset `+d+`)`)
	}
	return grid
}

// unwrapStages are the stages after `| unwrap c` inside a range aggregation: Loki's grammar takes label filters there
// (a comparison, a parenthesized expression, an ip() match, a chain of them), and nothing else.
var unwrapStages = []string{
	`| x > 1`, `| x > -1`, `| x >= - 1`, `| x < -0`, `| x < - 0`, `| __error__=""`, `| (a="b" or b="c")`, `| a=ip("1.2.3.4")`,
	`| x > 1 | y < 2`, `| x > 1, y < 2`, `| x > 1 and y < 2`, `| x > 1 or y="z"`,
	`| drop __error__`, `| keep a`, `| json`, `| logfmt`, `| logfmt --strict`, `| regexp "(?P<a>.)"`, `| pattern "<a>"`,
	`| unpack`, `| line_format "x"`, `| label_format a=b`, `| decolorize`, `| unwrap d`, `| unwrap bytes(d)`, `| JSON`,
	`| Drop x`, `| bool`, `| of`, `| offset`, `| variants`, `| ip("1.2.3.4")`, `| distinct a`, `|= "x"`, `!= "x"`,
	`|~ "x"`, `!~ "x"`, `|> "<_>"`, `!> "<_>"`, `| |= "x"`, `| != "x"`, `| 5`, `| "x"`, `| x > 1 | json`,
	`| x > 1 |= "y"`, `| x=~"(" | json`,
}

// unwrapGrid is every stage of unwrapStages after an unwrap, in a range aggregation with the range after the
// pipeline, before it (`{a="b"}[5m] | unwrap c ...`) and around parentheses; unwrap in a log query, under a vector
// aggregation and on the right of a binary operation; and the stages Loki builds while it parses (a regexp or
// pattern parser, a label filter, drop or keep regex), whose errors come ahead of the syntax error at the unwrap.
func unwrapGrid() []string {
	var grid []string
	for _, st := range unwrapStages {
		grid = append(grid,
			`sum_over_time({a="b"} | unwrap c `+st+` [5m])`,
			`quantile_over_time(.5, {a="b"} | json | unwrap c `+st+`[1h5m3s] offset 1d) by (a)`,
			`sum_over_time({a="b"}[5m] | json | unwrap c `+st+`)`,
		)
	}
	grid = append(grid,
		`sum_over_time(({a="b"} | unwrap c | json)[5m])`, `sum_over_time(({a="b"})[5m] | unwrap c | json)`,
		`sum_over_time(({a="b"})[5m] | unwrap c |= "x")`, `sum_over_time(({a="b"} | unwrap c | x > 1)[5m])`,
		`sum_over_time({a="b"} | unwrap c [5m]) / sum_over_time({a="b"} | unwrap c | json [5m])`,
		`sum_over_time({a="b"} | unwrap c [5m]) / count_over_time({a="b"} | json [5m])`,
		// unwrap outside a range aggregation
		`{a="b"} | unwrap a`, `{a="b"} | json | unwrap a`, `{a="b"} | unwrap bytes(a)`, `{a="b"} | unwrap a | a="b"`,
		`{a="b"} | unwrap a | label_format c=d != ip("10.0.0.0/8")`, `{a="b"} | unwrap a | json`, `{a="b"} | unwrap c | unwrap d`,
		`{a="b"} | json | unwrap [1s]`, `{a="b"} | json | unwrap duration_seconds() [1s]`, `{a="b"} | unwrap`,
		`{a="b"} |~ "(" | unwrap x`, `{a=~".*"} | unwrap x`, `{a="b"} | label_format x="{{.y" | unwrap a`,
		`{a="b"} | line_format "{{.y" | unwrap a`, `{a="b"} | json x="[" | unwrap a`, `sum by (a) ({a="b"} | unwrap c)`,
		`rate({a="b"} | unwrap c [5m]) + {a="b"} | unwrap c`, `{a="b"} | unwrap a # comment`,
		// a stage error Loki raises while it parses, ahead of the syntax error at an unwrap
		`{a="b"} | regexp "x" | unwrap a`, `{a="b"} | pattern "x" | unwrap a`, `{a="b"} | x=~"(" | unwrap a`,
		`{a="b"} | drop x=~"(" | unwrap a`, `rate({a="b"} | regexp "x" [5m]) + {b="c"} | unwrap c`,
		`sum_over_time({a="b"} | regexp "x" | unwrap c | json [5m])`, `sum_over_time({a="b"} | x=~"(" | unwrap c |= "x" [5m])`,
		`count_over_time({a="b"} | regexp "x" | unwrap a | unwrap b [5m])`,
		`rate({a="b"} | regexp "x" [5m]) + sum_over_time({b="c"} | unwrap c | json [5m])`,
		`sum_over_time({a="b"} | unwrap c | regexp "x" [5m])`, `{a="b"} | unwrap a | regexp "x"`,
		`rate({a="b"}[5m]) + rate({b="c"}[5m]) + rate({c="d"} | regexp "x" [5m]) + {d="e"} | unwrap x`,
		`rate({a="b"}[5m]) + rate({b="c"}[5m]) + rate({c="d"} | x=~"(" [5m]) + sum_over_time({d="e"} | unwrap c | json [5m])`,
		`rate({a="b"}[5m]) + rate({b="c"}[5m]) + rate({c="d"}[5m]) + sum_over_time({d="e"} | unwrap c | json [5m])`,
		`rate({a="b"} | x="1" [5m]) + rate({b="c"} | y="2" [5m]) + rate({c="d"} | z=~"(" [5m]) + {d="e"} | unwrap x`,
		`rate({a="b"} | x="1" [5m]) + rate({b="c"} | drop y=~"[" [5m]) + rate({c="d"} | z=~"(" [5m]) + {d="e"} | unwrap x`,
		// a conversion's name is its keyword before a parenthesis, also after a blank Loki does not skip
		"count_over_time({a=\"b\"} | logfmt --strict | unwrap\vduration(c) [1m] offset 5m)", "sum_over_time({a=\"b\"} | unwrap\fbytes(c) [1m])",
		"sum_over_time({a=\"b\"} | unwrap\u00a0duration_seconds (c) [1m])", `sum_over_time({a="b"} | unwrap c | bytes(d) [1m])`,
		`sum_over_time({a="b"} | unwrap c bytes(d) [1m])`, `quantile_over_time(0.9, {a="b"} | unwrap bytes(x y) [5m])`,
		`quantile_over_time(0.9, {a="b"} | unwrap bytes(x [5m])`, "quantile_over_time(0.9, {a=\"b\"} | unwrap bytes(\x01x) [5m])",
		// on and ignoring take a list of names
		`rate({a="b"}[5m]) / on rate({a="b"}[5m])`, `rate({a="b"}[5m]) / ignoring rate({a="b"}[5m])`,
		`rate({a="b"}[5m]) / on a rate({a="b"}[5m])`, `rate({a="b"}[5m]) / on () rate({a="b"}[5m])`,
		`rate({a="b"}[5m]) / on (a) group_left rate({a="b"}[5m])`, `rate({a="b"}[5m]) / ignoring (a) group_right (b) rate({a="b"}[5m])`,
		`rate({a="b"}[5m]) / on group_left (a) rate({a="b"}[5m])`,
	)
	return grid
}

// rangeUnwrapLokiAnswers holds Loki's answers to rangeDurationGrid and unwrapGrid, in that order.
const rangeUnwrapLokiAnswers = "testdata/range-unwrap-grammar-loki.jsonl"

// rangeUnwrapKnown are the queries Loki accepts and the proxy still rejects: a range of a bare 0, which Loki reads as
// a duration of nothing (semantics/range-duration-grammar-wider-than-loki).
var rangeUnwrapKnown = func() map[string]bool {
	known := map[string]bool{}
	for _, q := range rangeDurationGrid() {
		if strings.Contains(q, "[0]") {
			known[q] = true
		}
	}
	return known
}()

// A [range] is read as Loki reads it, with Prometheus' duration parser: the durations it rejects (`[1µs]`, `[1us]`,
// `[5m5m]`, `[1.5m]`, `[ 5m]`, `[9999999999h]`) are Loki's 400 with its text, the ones it reads are accepted, and an
// offset still reads Go's durations. An unwrap stands only inside a range aggregation, and only label filters follow
// it: a log query with an unwrap and any other stage after one are Loki's syntax error, after the errors of the
// stages Loki builds while it parses; on and ignoring take their list. Loki's answers are recorded with the Loki
// oracle check command (LVP_RUN_ORACLES=1 records them again).
//
// conformance: semantics/range-duration-grammar-wider-than-loki, semantics/unwrap-in-log-query, semantics/stage-after-unwrap-accepted, semantics/binary-on-without-label-list-accepted
func TestRangeAndUnwrapGrammarMatchLoki(t *testing.T) {
	grid := append(rangeDurationGrid(), unwrapGrid()...)
	requireLokiAnswers(t, "check", grid, rangeUnwrapLokiAnswers, rangeUnwrapKnown)
}

// Parse keeps reading a log query with an unwrap stage (the handlers drop Grafana's `| unwrap [1s]` stubs before they
// validate, which needs the tree); the validation rejects it with Loki's syntax error at the unwrap.
//
// conformance: semantics/unwrap-in-log-query
func TestParseKeepsUnwrapInLogQueryForTheStubs(t *testing.T) {
	for _, q := range []string{`{app="a"} | json | unwrap [1s]`, `{app="a"} | unwrap x`, `{app="a"} | unwrap x | unwrap y`} {
		if _, err := Parse(q); err != nil {
			t.Errorf("Parse(%q): %v", q, err)
		}
		want := "parse error at line 1, col " + map[bool]string{true: "20", false: "13"}[strings.Contains(q, "json")] + ": syntax error: unexpected unwrap"
		if got := ValidateLogQL(q); got != want {
			t.Errorf("ValidateLogQL(%q) = %q, want %q", q, got, want)
		}
		if err := ParseAndValidate(q); err == nil || err.Error() != want {
			t.Errorf("ParseAndValidate(%q) = %v, want %q", q, err, want)
		}
	}
}

// promRange is Prometheus' duration form, written apart from rangeLexError: whole numbers and the units largest first,
// each at most once.
var promRange = regexp.MustCompile(`^(?:(\d+)y)?(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?(?:(\d+)ms)?$`)

// promRangeNanos is the length of a text promRange matches, or nil when it is empty or too long for a duration.
func promRangeNanos(d string) *big.Int {
	m := promRange.FindStringSubmatch(d)
	if d == "" || m == nil {
		return nil
	}
	day := new(big.Int).Mul(big.NewInt(24), big.NewInt(int64(time.Hour)))
	sizes := []*big.Int{new(big.Int).Mul(big.NewInt(365), day), new(big.Int).Mul(big.NewInt(7), day), day,
		big.NewInt(int64(time.Hour)), big.NewInt(int64(time.Minute)), big.NewInt(int64(time.Second)), big.NewInt(int64(time.Millisecond))}
	total := new(big.Int)
	for i, n := range m[1:] {
		if n == "" {
			continue
		}
		v, _ := new(big.Int).SetString(n, 10)
		total.Add(total, v.Mul(v, sizes[i]))
	}
	if total.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return nil
	}
	return total
}

// A range text is accepted exactly when it is Prometheus' duration form with a length that fits a duration, the parser
// reads the range so (its duration is that length, to the nanosecond), and a rejection is one of Loki's four texts.
func FuzzRangeDuration(f *testing.F) {
	for _, d := range rangeDurationTexts {
		f.Add(d)
	}
	f.Fuzz(func(t *testing.T, d string) {
		if strings.ContainsAny(d, "]\n\x00") || strings.HasPrefix(d, "$") || !utf8.ValidString(d) {
			return // a ']' ends the range; a line break, a NUL and a Grafana variable are other errors
		}
		msg, want := rangeLexError(d), promRangeNanos(d)
		if (msg == "") != (want != nil) {
			t.Fatalf("%q: rangeLexError %q, Prometheus' form and length %v", d, msg, want)
		}
		switch {
		case msg == "", msg == "empty duration string", msg == "duration out of range",
			strings.HasPrefix(msg, "not a valid duration string: "), strings.HasPrefix(msg, "unknown unit "):
		default:
			t.Fatalf("%q: %q is no text of Loki's", d, msg)
		}
		expr, err := Parse(`rate({a="b"}[` + d + `])`)
		if (err == nil) != (msg == "") {
			t.Fatalf("%q: Parse %v, rangeLexError %q", d, err, msg)
		}
		if err == nil {
			if got := expr.(*RangeAggregation).RangeDuration; int64(got) != want.Int64() {
				t.Fatalf("%q: the range is %v, want %v", d, got, want)
			}
		}
		if err != nil && !strings.Contains(err.Error(), msg) {
			t.Fatalf("%q: Parse %v, want %q", d, err, msg)
		}
	})
}

// The bare wildcard some clients send is accepted with the white space Loki skips around it, and only with that: a
// blank Loki does not skip makes it a query Loki rejects.
//
// conformance: semantics/parser-reads-unicode-blank-as-white-space
func TestWildcardTakesOnlyLokisWhiteSpace(t *testing.T) {
	for q, wildcard := range map[string]bool{"*": true, " *\t": true, "\r\n* \n": true, "\v*": false, "* ": false, "**": false, "": false, " ": false} {
		if got := isWildcard(q); got != wildcard {
			t.Errorf("isWildcard(%q) = %v, want %v", q, got, wildcard)
		}
		if msg := ValidateLogQL(q); (msg == "") != wildcard {
			t.Errorf("ValidateLogQL(%q) = %q, want accepted=%v", q, msg, wildcard)
		}
	}
}

// Only the log queries with a stage Loki builds while it parses are kept for the order of errors at an unwrap.
func TestMayFailWhileParsed(t *testing.T) {
	for q, want := range map[string]bool{
		`{a="b"} | json | x="y"`: true, `{a="b"} | regexp "(?P<a>.)"`: true, `{a="b"} | pattern "<a>"`: true,
		`{a="b"} | drop x="y"`: true, `{a="b"} | keep x=~"y"`: true, `{a="b"} | keep x | drop y`: false,
		`{a="b"} | json | logfmt |= "x" | line_format "x"`: false, `{a="b"}`: false,
	} {
		expr, err := Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := mayFailWhileParsed(expr.(*LogQuery).Pipeline); got != want {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}

// The logfmt flags of a query with an unwrap outside a range aggregation stay, so that Loki's syntax error is at its
// column; Grafana's unwrap stubs, which the handlers drop, do not keep them.
//
// conformance: semantics/unwrap-in-log-query
func TestStripParserFlagsAroundUnwrap(t *testing.T) {
	for q, want := range map[string]string{
		`{a="b"} | logfmt --strict | unwrap x`:                        `{a="b"} | logfmt --strict | unwrap x`,
		`{a="b"} | logfmt --strict | unwrap [1s]`:                     `{a="b"} | logfmt  | unwrap [1s]`,
		`{a="b"} | logfmt --strict | unwrap duration() [1s]`:          `{a="b"} | logfmt  | unwrap duration() [1s]`,
		`rate({a="b"}[5m]) + {a="b"} | logfmt --strict | unwrap [1s]`: `rate({a="b"}[5m]) + {a="b"} | logfmt --strict | unwrap [1s]`,
		`sum_over_time({a="b"} | logfmt --strict | unwrap x [5m])`:    `sum_over_time({a="b"} | logfmt  | unwrap x [5m])`,
	} {
		if got := StripParserFlags(q); got != want {
			t.Errorf("StripParserFlags(%q) = %q, want %q", q, got, want)
		}
	}
}
