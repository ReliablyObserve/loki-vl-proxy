package translator

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The gate pattern selects the values Loki's convertFloat (strconv.ParseFloat)
// accepts in decimal syntax, plus inf. The forms ParseFloat reads that the gate
// leaves out are known differences (hexadecimal floats, "infinity", nan: the math
// pipe reads the first as an integer and not the second, and VictoriaLogs' sum
// cannot carry a NaN); the forms the math pipe reads that ParseFloat rejects
// (units, underscores, binary and hex integers) must never pass.
func TestUnwrapNumberPatternMatchesParseFloat(t *testing.T) {
	re := regexp.MustCompile(UnwrapNumberPattern)
	accepted := []string{"0", "5", "-5", "+5", "1.5", ".5", "5.", "1e3", "1E-2", "00012", "-0", "1.5e+10", "inf", "-Inf", "+INF", "1_000", "1_000.5", "1e307", "1e+0307", "0_1", ".5_5", "1e-308", "1e-310", "1e-400", "1e-99999"}
	for _, v := range accepted {
		if !re.MatchString(v) {
			t.Errorf("%q is a number (ParseFloat accepts it) but the gate drops it", v)
		}
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			t.Errorf("fixture %q is not accepted by ParseFloat: %v", v, err)
		}
	}
	rejected := []string{"", " 5", "5 ", "abc", "5s", "86282s", "1KiB", "2KB", "1.5Mi", "0x10", "0b11", "1__0", "_1", "1_", "1_.5", "1._5", "1,5", "--5", "+", "-", "1e", "1e+", "1.2.3", "٣", "+nan", "-nan", "1e400", "1e309", "1e1000"}
	for _, v := range rejected {
		if re.MatchString(v) {
			t.Errorf("%q is not a number for Loki but the gate keeps it", v)
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil && !math.IsNaN(f) {
			t.Errorf("fixture %q is accepted by ParseFloat (%v): it is not a rejected form", v, f)
		}
	}
	for _, v := range []string{"infinity", "0x1p4", "nan", "NaN", "1e1_0", "1e308"} {
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			t.Errorf("known-difference fixture %q is not accepted by ParseFloat: %v", v, err)
		}
		if re.MatchString(v) {
			t.Errorf("%q is a documented known difference: the gate is meant to drop it", v)
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
		if !strings.Contains(got, want) || !strings.Contains(got, ` | math "`+field+`" as __lvp_v | stats sum(__lvp_v)`) {
			t.Errorf("field %s: expected the quoted gate in %q", field, got)
		}
	}
}

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
	// Known difference: Go reports an overflow error, VictoriaLogs saturates.
	if _, err := time.ParseDuration("2562048h"); err == nil || !re.MatchString("2562048h") {
		t.Errorf("2562048h should overflow in Go and pass the gate")
	}
	gate := UnwrapGateFor("f", "duration")
	for _, norm := range []string{`"us|μ"`, `"(^|[^0-9])[.]([0-9])"`, `"([0-9])[.]([^0-9])"`} {
		if !strings.Contains(gate, norm) {
			t.Errorf("duration gate misses the normalisation %s: %s", norm, gate)
		}
	}
}

func TestUnwrapBytesPatternAndExtract(t *testing.T) {
	re := regexp.MustCompile(UnwrapBytesPattern)
	for _, v := range []string{"5", "5 kB", "5kB", "1PB", "1PiB", "1EB", "1EiB", "0.1EB", "15EiB", "18EB", "1KiB", "1.5 MB", "1,000", "1,5", "5 B", "5b", ".5kb", "1gIb", "5 kb "} {
		if !re.MatchString(v) {
			t.Errorf("bytes pattern rejects %q", v)
		}
	}
	for _, v := range []string{"", "kB", "-5", "1d", "5 xB", "1,,5s", "5 kBB", "1.5.5"} {
		if re.MatchString(v) {
			t.Errorf("bytes pattern accepts %q", v)
		}
	}
	// The unit (b included) is matched case-insensitively as a whole, so an uppercase
	// B (what go-humanize prints: "5 kB") is split into number and prefix like "5kb".
	gate := UnwrapGateFor("f", "bytes")
	want := `(?i:(?P<__lvp_p>[kmgtpe]?)(?P<__lvp_i>i?)b?)$`
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
