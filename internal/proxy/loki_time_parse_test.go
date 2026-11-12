package proxy

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// parseLokiTimeLayoutsFirst is parseLokiTimeToUnixNano as it was before the integer fast path: the time layouts
// first, then now/now±d, then integers and floats; a time int64 nanoseconds cannot hold is false in both. The fast
// path must answer the same for every input.
func parseLokiTimeLayoutsFirst(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return unixNanoIfHeld(parsed)
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return unixNanoIfHeld(parsed)
	}
	if raw == "now" || strings.HasPrefix(raw, "now-") || strings.HasPrefix(raw, "now+") {
		return 0, false // time-dependent; not compared
	}
	if nInt, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return integerNanosIfHeld(nInt)
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return floatNanosIfHeld(n)
}

func TestParseLokiTimeIntegerFastPathMatchesLayoutsFirst(t *testing.T) {
	inputs := []string{
		"", " ", "0", "-0", "+0", "1", "-1", "+1", "1700000000", "1700000000000", "1700000000000000",
		"1700000000000000000", "-1700000000", "9223372036854775807", "9223372036854775808", "-9223372036854775809",
		"1700000000.5", "1.7e9", "1e18", "0x10", "1_000", "+", "-", "--1", "1-", " 1700000000 ", "\t42\n",
		"2026-11-09T19:08:00Z", "2026-11-09T19:08:00.123456789Z", "2026-11-09T19:08:00+01:00", "2026-11-09",
		"abc", "NaN", "Inf", "١٢٣",
	}
	for _, in := range inputs {
		want, wantOK := parseLokiTimeLayoutsFirst(in)
		got, gotOK := parseLokiTimeToUnixNano(in)
		if got != want || gotOK != wantOK {
			t.Errorf("%q: got (%d, %v), want (%d, %v)", in, got, gotOK, want, wantOK)
		}
	}
}

func FuzzParseLokiTimeIntegerFastPath(f *testing.F) {
	for _, s := range []string{"1700000000", "-1", "1e9", "2026-11-09T19:08:00Z", "+5", "9223372036854775808"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if s := strings.TrimSpace(in); s == "now" || strings.HasPrefix(s, "now-") || strings.HasPrefix(s, "now+") {
			return
		}
		want, wantOK := parseLokiTimeLayoutsFirst(in)
		got, gotOK := parseLokiTimeToUnixNano(in)
		if got != want || gotOK != wantOK {
			t.Fatalf("%q: got (%d, %v), want (%d, %v)", in, got, gotOK, want, wantOK)
		}
	})
}

func BenchmarkParseLokiTimeToUnixNano(b *testing.B) {
	for _, in := range []string{"1794251280000000000", "2026-11-09T19:08:00Z"} {
		b.Run(in[:4], func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				parseLokiTimeToUnixNano(in)
			}
		})
	}
}
