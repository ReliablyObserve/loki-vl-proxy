package translate

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
)

// The gate keeps a row when Loki's conversion of the unwrapped value makes a sample. The patterns are the syntax of the
// functions Loki converts with: strconv.ParseFloat, time.ParseDuration and humanize.ParseBytes. A value the conversion
// reports as out of range matches the syntax and is dropped by the range filter after the math pipe, so the corpus holds
// none; nan is left out of the plain pattern on purpose (semantics/unwrap-gate-parsefloat-divergences).
// conformance: semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapPatternsAreTheSyntaxOfLokisConversions(t *testing.T) {
	floats := []string{
		"0", "1", "-1", "+1", "1.", "1.5", ".5", "-.5", ".", "+", "-", "1e5", "1E-5", "1e+5", "1e", "e5", "1_0", "1__0", "_1", "1_", "1_0.2_5", "1e1_0",
		"0x1p3", "0X1P-3", "0x1.8p1", "0x1", "0x_1p3", "0x1_fp0", "0x.8p0", "0xp0", "inf", "Inf", "INF", "+inf", "-Inf", "infinity", "+Infinity", "-INFINITY",
		"in", "infin", "infinityx", "nan", "NaN", "+nan", "1.5.2", "", " 1", "1 ", "1,5", "١", "0b1", "0o7", "00", "007", "1e5.5", "--1",
	}
	number := regexp.MustCompile(UnwrapNumberPattern)
	for _, s := range floats {
		_, err := strconv.ParseFloat(s, 64)
		want := err == nil || errors.Is(err, strconv.ErrRange)
		if strings.EqualFold(s, "nan") {
			want = false // registered divergence: the gate drops nan
		}
		if got := number.MatchString(s); got != want {
			t.Errorf("plain pattern on %q = %v, ParseFloat accepts: %v (%v)", s, got, want, err)
		}
		word := s
		if word != "" && (word[0] == '+' || word[0] == '-') {
			word = word[1:]
		}
		infinity := strings.EqualFold(word, "infinity")
		if got := regexp.MustCompile(UnwrapPatternWithoutInfinity("")).MatchString(s); got != (want && !infinity) {
			t.Errorf("pattern without infinity on %q = %v, want %v", s, got, want && !infinity)
		}
		if got := regexp.MustCompile(UnwrapInfinityPattern).MatchString(s); got != infinity {
			t.Errorf("infinity pattern on %q = %v, want %v", s, got, infinity)
		}
	}

	durations := []string{
		"0", "1s", "1.5h", "1h30m", "-1m", "+1m", ".5s", "5.s", "1us", "1µs", "1μs", "01m", "1ns", "1ms", "0s", "+0", "-0", "1.s2m", "1.5.5s",
		"1", "s", "1d", "1 s", "1H", "", "1ns2", "1h 30m", "1hh", "..5s", "-", "+", "0.0s", "10000h", "1m1", "µs",
	}
	duration := regexp.MustCompile(UnwrapDurationPattern)
	for _, s := range durations {
		_, err := time.ParseDuration(s)
		if got := duration.MatchString(s); got != (err == nil) {
			t.Errorf("duration pattern on %q = %v, ParseDuration accepts: %v", s, got, err == nil)
		}
	}

	sizes := []string{
		"0", "1", "1B", "1b", "1KB", "1kb", "1 KB", "1kib", "1KiB", "1,000", "1.5G", "1gb", "1MB", "1 MiB", "1.5", ".5k", "1.k", "1.5.5", "1,", ",1", "1,,0",
		"", "b", "B", "k", "1X", "1KiBB", " 1KiB", "1KiB ", "1 KB", "1\tKB", "1KB", "1PB", "1EiB", "1Zb", "1 ", "1 b", "1ib", "1i",
	}
	bytes := regexp.MustCompile(UnwrapBytesPattern)
	for _, s := range sizes {
		_, err := humanize.ParseBytes(s)
		if err != nil && strings.Contains(err.Error(), "too large") {
			continue
		}
		if got := bytes.MatchString(s); got != (err == nil) {
			t.Errorf("bytes pattern on %q = %v, ParseBytes accepts: %v (%v)", s, got, err == nil, err)
		}
	}

	if UnwrapPattern("") != UnwrapNumberPattern || UnwrapPattern("duration") != UnwrapDurationPattern || UnwrapPattern("bytes") != UnwrapBytesPattern {
		t.Error("UnwrapPattern picks the pattern of the conversion")
	}
	if UnwrapPatternWithoutInfinity("duration") != UnwrapDurationPattern || UnwrapPatternWithoutInfinity("bytes") != UnwrapBytesPattern {
		t.Error("only the plain pattern has an infinity word to leave out")
	}
	for in, want := range map[string]string{"duration_seconds": "duration", "duration": "duration", "bytes": "bytes", "": "", "other": "other"} {
		if got := UnwrapConv(in); got != want {
			t.Errorf("UnwrapConv(%q) = %q, want %q", in, got, want)
		}
	}
}

// A value whose conversion overflows makes no sample: the range filter after the conversion drops it.
// conformance: semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapInRange(t *testing.T) {
	if got, want := UnwrapInRange(""), "__lvp_v:<=inf"; got != want {
		t.Errorf("plain = %q, want %q", got, want)
	}
	if got, want := UnwrapInRange("bytes"), "__lvp_v:<18446744073709551616"; got != want {
		t.Errorf("bytes = %q, want %q", got, want)
	}
	limit := strconv.FormatFloat(float64(1<<63-1)/1e9, 'f', -1, 64)
	if got, want := UnwrapInRange("duration"), "__lvp_v:>-"+limit+" __lvp_v:<"+limit; got != want {
		t.Errorf("duration = %q, want %q", got, want)
	}
}

// The gate is the syntax filter, the conversion and the range filter; SplitUnwrapGate takes it apart again.
// conformance: semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapGateRoundTrip(t *testing.T) {
	for _, conv := range []string{"", "duration", "bytes"} {
		for _, field := range []string{"n", "a-b", `we"ird`, "dotted.field"} {
			gate := UnwrapGateFor(field, conv)
			match, convert := UnwrapGateParts(field, conv)
			if gate != match+convert+UnwrapFilterPipe+UnwrapInRange(conv) {
				t.Errorf("%s %s: the gate is its parts and the range filter", field, conv)
			}
			if want := " | filter " + strconv.Quote(field) + ":~" + strconv.Quote(UnwrapPattern(conv)); match != want {
				t.Errorf("%s %s: match = %s, want %s", field, conv, match, want)
			}
			if !strings.HasSuffix(convert, " as "+UnwrapValueAlias) {
				t.Errorf("%s %s: the conversion must end in the value alias: %s", field, conv, convert)
			}
			mustParseLogsQL(t, "* "+gate)
			base, got, ok := SplitUnwrapGate(`{app="x"}` + gate)
			if !ok || base != `{app="x"}` || got != field {
				t.Errorf("%s %s: SplitUnwrapGate = %q, %q, %v", field, conv, base, got, ok)
			}
		}
	}
	if gate := UnwrapGate("n"); gate != UnwrapGateFor("n", "") {
		t.Error("UnwrapGate is the plain gate")
	}
	for _, query := range []string{`{app="x"}`, `{app="x"} | filter "n":~"x"`, `{app="x"} | filter n:~"x"`, `{app="x"} | filter "n":~"^[0-9]+$" | stats count()`} {
		if base, field, ok := SplitUnwrapGate(query); ok || base != query || field != "" {
			t.Errorf("SplitUnwrapGate(%q) = %q, %q, %v; want no gate", query, base, field, ok)
		}
	}
	if got := UnwrapPlainConvert("n", "__lvp_n:i(infinity)"); got != ` | copy "n" as __lvp_n | replace_regexp if (__lvp_n:i(infinity)) ("(?i)inity$","") at __lvp_n | math __lvp_n as __lvp_v` {
		t.Errorf("UnwrapPlainConvert = %s", got)
	}
}
