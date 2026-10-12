package translator

import (
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The gate pattern selects the values Loki's convertFloat (strconv.ParseFloat)
// accepts: decimal and hexadecimal floats with Go's digit separators, and inf.
// A value that is syntactically a float but overflows float64 (ParseFloat's
// ErrRange: Loki makes no sample) passes the pattern and is dropped by the range
// filter after the math pipe, which VictoriaLogs answers NaN for (TestUnwrapGateRange).
// The two spellings it leaves out are documented residuals (infinity, nan); the
// forms the math pipe reads that ParseFloat rejects (units, binary and hex
// integers) must never pass.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapNumberPatternMatchesParseFloat(t *testing.T) {
	re := regexp.MustCompile(UnwrapNumberPattern)
	accepted := []string{"0", "5", "-5", "+5", "1.5", ".5", "5.", "1e3", "1E-2", "00012", "-0", "1.5e+10", "inf", "-Inf", "+INF", "1_000", "1_000.5", "1e307", "1e+0307", "0_1", ".5_5", "1e-308", "1e-310", "1e-400", "1e-99999",
		// hexadecimal floats (the p exponent is mandatory), digit separators in exponents, exponents of 308 and above
		"0x1p4", "-0x1p4", "+0x1P-2", "0X1.8p1", "0x.8p1", "0x1.p1", "0x_1p4", "0x1_0p0", "0xAbCp0", "0x1p1_0", "0x1.8_8p1", "0x1p+4",
		"infinity", "Infinity", "-INFINITY", "+infinity", "1e1_0", "1e+1_0", "1e308", "1.7976931348623157e308", "1_0e5", "0_1e1_0", "1e00000000000000000000001"}
	for _, v := range accepted {
		if !re.MatchString(v) {
			t.Errorf("%q is a number (ParseFloat accepts it) but the gate drops it", v)
		}
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			t.Errorf("fixture %q is not accepted by ParseFloat: %v", v, err)
		}
	}
	// Syntactically floats that overflow: Loki makes no sample (SampleExtractionErr).
	for _, v := range []string{"1e309", "1.8e308", "1000e307", "1e400", "1e1000", "0x1p2000", "0x1p1024", "-0x1p1024", "1" + strings.Repeat("0", 309), "1e1_000", "9e99999999999999999999"} {
		if !re.MatchString(v) {
			t.Errorf("%q must pass the pattern: the range filter after math drops it", v)
		}
		if _, err := strconv.ParseFloat(v, 64); !errors.Is(err, strconv.ErrRange) {
			t.Errorf("fixture %q is not an overflow for ParseFloat: %v", v, err)
		}
	}
	rejected := []string{"", " 5", "5 ", "abc", "5s", "86282s", "1KiB", "2KB", "1.5Mi", "0x10", "0b11", "1__0", "_1", "1_", "1_.5", "1._5", "1,5", "--5", "+", "-", "1e", "1e+", "1.2.3", "٣", "+nan", "-nan",
		"0x", "0x1", "0x1p", "0x.p1", "0x1_p1", "0x1p_1", "0x_.8p1", "0x1._8p1", "0x1.8_p1", "0xgp1", "0x1p1.5", "0x1e3", "0o7", "1e_5", "1e+_5", "1_e5", "1e5_", "1e5__5", "0x1p5_", "1e1.5", "+-1", "1e+-1", "e5", ".e5", "."}
	for _, v := range rejected {
		if re.MatchString(v) {
			t.Errorf("%q is not a number for Loki but the gate keeps it", v)
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsNaN(f) {
			t.Errorf("fixture %q is accepted by ParseFloat (%v): it is not a rejected form", v, f)
		}
	}
	// Documented residual (semantics/unwrap-gate-parsefloat-divergences): Loki makes
	// a NaN sample, the gate drops the row (VictoriaLogs' sum restarts at a NaN
	// where Loki's is NaN; see TestUnwrapNaNGroupsReachTheAnswer in internal/proxy).
	for _, v := range []string{"nan", "NaN", "NAN"} {
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			t.Errorf("known-difference fixture %q is not accepted by ParseFloat: %v", v, err)
		}
		if re.MatchString(v) {
			t.Errorf("%q is a documented known difference: the gate is meant to drop it", v)
		}
	}
}

// The range filter after the math pipe drops the rows Loki makes no sample of
// because the conversion overflows. VictoriaLogs' math pipe answers NaN for a
// plain value that overflows float64 (it reads numbers with ParseFloat and
// reports no value on an error), compares NaN false, and saturates a duration at
// int64 nanoseconds; a byte size is computed in float64 as humanize.ParseBytes
// does. The thresholds are the ones of the Go functions.
// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapGateRange(t *testing.T) {
	if got := UnwrapInRange(""); got != "__lvp_v:<=inf" {
		t.Errorf("plain: %q", got)
	}
	if got := UnwrapInRange("bytes"); got != "__lvp_v:<"+strconv.FormatFloat(math.MaxUint64, 'f', -1, 64) && got != "__lvp_v:<18446744073709551616" {
		t.Errorf("bytes: %q", got)
	}
	lim := strconv.FormatFloat(float64(math.MaxInt64)/1e9, 'f', -1, 64)
	if lim != "9223372036.854776" || strings.ContainsAny(lim, "eE") {
		t.Errorf("the duration limit is spelled without an exponent: %s", lim)
	}
	if f, _ := strconv.ParseFloat(lim, 64); f != float64(math.MaxInt64)/1e9 {
		t.Errorf("the duration limit %s must read back as the same float64", lim)
	}
	if got := UnwrapInRange("duration"); got != "__lvp_v:>-"+lim+" __lvp_v:<"+lim {
		t.Errorf("duration: %q want limit %s", got, lim)
	}
	// The thresholds are exactly where Go's conversions start to fail.
	if _, err := time.ParseDuration("2562047h47m16.854775807s"); err != nil {
		t.Errorf("the largest duration must convert: %v", err)
	}
	if _, err := time.ParseDuration("2562047h47m16.854775808s"); err == nil {
		t.Error("one nanosecond more must overflow")
	}
	if _, err := time.ParseDuration("-2562047h47m16.854775808s"); err != nil {
		t.Errorf("the most negative duration must convert: %v", err)
	}
	for _, conv := range []string{"", "duration", "bytes"} {
		gate := UnwrapGateFor("f", conv)
		if !strings.HasSuffix(gate, " | filter "+UnwrapInRange(conv)) {
			t.Errorf("conv %q: the range filter must end the gate: %s", conv, gate)
		}
	}
}

func TestUnwrapGateRoundTrips(t *testing.T) {
	base := `app:="x" | unpack_json`
	for _, conv := range []string{"", "duration", "bytes"} {
		for _, field := range []string{"latency", "duration_ms", "a.b", "max", "a-b", `with"quote`} {
			got, gotField, ok := SplitUnwrapGate(base + UnwrapGateFor(field, conv))
			if !ok || got != base || gotField != field {
				t.Errorf("SplitUnwrapGate(%q, %q) = %q, %q, %v", field, conv, got, gotField, ok)
			}
		}
	}
	if _, _, ok := SplitUnwrapGate(base); ok {
		t.Error("a query without the gate must not split")
	}
	if _, _, ok := SplitUnwrapGate(base + UnwrapGate("a")[:20]); ok {
		t.Error("a query with a cut gate must not split")
	}
}

// A unwrap's stats pipe reads only the rows that make a sample, converted by the
// math pipe in the syntax of the unwrap conversion (plain, duration(), bytes());
// a line rate keeps counting lines.
func TestUnwrapGateOnTranslatedMetrics(t *testing.T) {
	for _, tc := range []struct {
		name, logql string
		gated       bool
		stats       string
		conv        string
	}{
		{"sum", `sum_over_time({app="x"} | unwrap b [1m])`, true, "| stats sum(__lvp_v)", ""},
		{"avg grouped", `avg by (a) (avg_over_time({app="x"} | unwrap b [1m]))`, true, "avg(__lvp_v)", ""},
		{"max series level", `max by (a) (sum_over_time({app="x"} | unwrap b [1m]))`, true, "sum(__lvp_v) as __lvp_inner", ""},
		{"stddev", `stddev_over_time({app="x"} | json | unwrap b [1m])`, true, "stddev(__lvp_v)", ""},
		{"quantile", `quantile_over_time(0.9, {app="x"} | unwrap b [1m])`, true, "quantile(0.9, __lvp_v)", ""},
		{"rate_counter", `rate_counter({app="x"} | unwrap b [1m])`, true, "__rate_counter__(__lvp_v)", ""},
		{"rate is a sum per second", `sum by (a) (rate({app="x"} | unwrap b [1m]))`, true, "sum(__lvp_v) as __lvp_inner | math __lvp_inner/60 as __lvp_rate", ""},
		{"duration is converted in seconds", `sum_over_time({app="x"} | unwrap duration(b) [1m])`, true, "| stats sum(__lvp_v)", "duration"},
		{"bytes are converted to bytes", `max_over_time({app="x"} | unwrap bytes(b) [1m])`, true, "| stats max(__lvp_v)", "bytes"},
		{"rate of a conversion is a sum per second", `sum by (a) (rate({app="x"} | unwrap duration(b) [1m]))`, true, "sum(__lvp_v) as __lvp_inner | math __lvp_inner/60 as __lvp_rate", "duration"},
		{"line rate counts lines", `sum by (a) (rate({app="x"} [1m]))`, false, "count() as __lvp_inner", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatal(err)
			}
			if has := strings.Contains(got, UnwrapGateFor("b", tc.conv)); has != tc.gated {
				t.Fatalf("gate present = %v, want %v in %q", has, tc.gated, got)
			}
			if !strings.Contains(got, tc.stats) {
				t.Fatalf("expected %q in %q", tc.stats, got)
			}
			if tc.gated {
				// The gate sits right before the stats pipe, after every pipe that
				// resolves the field.
				if base, _, ok := SplitUnwrapGate(got[:strings.Index(got, " | stats")]); !ok || strings.Contains(base, UnwrapValueAlias) {
					t.Fatalf("the gate must end the pipes before the stats pipe: %q", got)
				}
			}
		})
	}
}

// A parser-derived field the translator resolves (a sanitized JSON key) is read
// by the gate after the pipes that give it its value.
func TestUnwrapGateFollowsKeyResolution(t *testing.T) {
	got, err := TranslateLogQL(`sum(sum_over_time({app="a"} | json | unwrap http_code [1m]))`)
	if err != nil {
		t.Fatal(err)
	}
	resolve := strings.Index(got, `as http_code keep_original_fields`)
	gate := strings.Index(got, UnwrapGate("http_code"))
	if resolve < 0 || gate < resolve {
		t.Fatalf("the gate must follow the key resolution: %q", got)
	}
}

// A field the math pipe would read as a function (max, abs, rand) or stop at a
// minus is quoted in the gate, which VictoriaLogs reads as the field.
func TestUnwrapGateQuotesTheField(t *testing.T) {
	for _, field := range []string{"max", "abs", "rand", "latency"} {
		got, err := TranslateLogQL(`sum_over_time({a="b"} | unwrap ` + field + ` [5m])`)
		if err != nil {
			t.Fatal(err)
		}
		want := ` | filter "` + field + `":~`
		if !strings.Contains(got, want) || !strings.Contains(got, ` | copy "`+field+`" as __lvp_n`) || !strings.Contains(got, ` | math __lvp_n as __lvp_v | filter __lvp_v:<=inf | stats sum(__lvp_v)`) {
			t.Errorf("field %s: expected the quoted gate in %q", field, got)
		}
	}
}

// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapDurationPatternMatchesParseDuration(t *testing.T) {
	re := regexp.MustCompile(UnwrapDurationPattern)
	for _, v := range []string{"0", "-0", "5s", "5.s", ".5s", "-.5ms", "1h.5s", "1.s", "100µs", "100μs", "5us", "+5us", "1h30m", "1.5h", "300ms", "2562047h"} {
		if _, err := time.ParseDuration(v); err != nil {
			t.Errorf("fixture %q is not accepted by ParseDuration: %v", v, err)
		}
		if !re.MatchString(v) {
			t.Errorf("duration pattern rejects %q", v)
		}
	}
	for _, v := range []string{"", "5", "1d", "s", ".s", "1 s", "5S", "1e3s", "--5s"} {
		if _, err := time.ParseDuration(v); err == nil {
			t.Errorf("fixture %q is accepted by ParseDuration", v)
		}
		if re.MatchString(v) {
			t.Errorf("duration pattern accepts %q", v)
		}
	}
	// A duration beyond int64 nanoseconds: Go reports an overflow error (Loki makes no
	// sample), VictoriaLogs saturates, and the range filter after the math pipe
	// drops the saturated value (TestUnwrapGateRange).
	for _, v := range []string{"2562048h", "-2562048h", "9223372036854775808ns", "2562047h2562047h", "9223372036.854775808s", "99999999999999999999s", "5124095h"} {
		if _, err := time.ParseDuration(v); err == nil {
			t.Errorf("fixture %q must overflow in Go", v)
		}
		if !re.MatchString(v) {
			t.Errorf("duration pattern rejects the overflowing %q: the range filter drops it", v)
		}
	}
	gate := UnwrapGateFor("f", "duration")
	for _, norm := range []string{`"us|μ"`, `"(^|[^0-9])[.]([0-9])"`, `"([0-9])[.]([^0-9])"`} {
		if !strings.Contains(gate, norm) {
			t.Errorf("duration gate misses the normalisation %s: %s", norm, gate)
		}
	}
}

// conformance: semantics/unwrap-sample-validity, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapBytesPatternAndExtract(t *testing.T) {
	re := regexp.MustCompile(UnwrapBytesPattern)
	for _, v := range []string{"5", "5 kB", "5kB", "1PB", "1PiB", "1EB", "1EiB", "0.1EB", "15EiB", "18EB", "1KiB", "1.5 MB", "1,000", "1,5", "5 B", "5b", ".5kb", "1gIb", "5 kb ",
		// humanize.ParseBytes drops every comma of the number, a leading one and one after the point
		// included, and trims any Unicode space around the unit
		",5", ",,5", "1.5,0", "1,.5", ".,5", "5.,", "1,000,000 kB", "5\u00a0kB", "5\u2003kB", "5\u0085kB", "5\u000bkB", "5\u3000 kB \u00a0", "5\u212aB", "5k\u0130B", "5 K\u0130b"} {
		if !re.MatchString(v) {
			t.Errorf("bytes pattern rejects %q", v)
		}
	}
	for _, v := range []string{"", "kB", "-5", "1d", "5 xB", "1,,5s", "5 kBB", "1.5.5", ",", ",.", ".,", "1.2,.3", "1,5.5.5", "5\u2028\u0000kB", "5_0", "5\u0663", "5 kB\u200b"} {
		if re.MatchString(v) {
			t.Errorf("bytes pattern accepts %q", v)
		}
	}
	// The unit (b included) is matched case-insensitively as a whole, so an uppercase
	// B (what go-humanize prints: "5 kB") is split into number and prefix like "5kb".
	gate := UnwrapGateFor("f", "bytes")
	want := `(?i:(?P<__lvp_p>[kmgtpe]?)(?P<__lvp_i>[iİ]?)b?)$`
	if !strings.Contains(gate, want) {
		t.Fatalf("bytes extract is not case-insensitive over the b: %s", gate)
	}
	ex := regexp.MustCompile(`^(?P<__lvp_q>[0-9.]*)` + want)
	for v, prefix := range map[string]string{"5kB": "k", "1PB": "P", "1EiB": "E", "18EB": "E", "5B": "", "1gIb": "g", "5": ""} {
		m := ex.FindStringSubmatch(v)
		if m == nil || m[2] != prefix {
			t.Errorf("extract of %q = %v, want prefix %q", v, m, prefix)
		}
	}
}
