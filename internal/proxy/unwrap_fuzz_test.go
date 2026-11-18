package proxy

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/dustin/go-humanize"
)

// FuzzConvertUnwrap holds the unwrap conversion to the functions whose behaviour Loki's answers are recorded for
// (TestUnwrapConversionMatchesRecordedLoki): a plain value is strconv.ParseFloat's, a duration time.ParseDuration's
// in seconds, a size go-humanize v1.1.0's ParseBytes (Loki 3.7.10's); and every value a conversion accepts or
// overflows on is one the VictoriaLogs gate keeps, so the conversion-error check never reports a value Loki converts.
//
// conformance: semantics/unwrap-conversion-error, semantics/unwrap-sample-validity
func FuzzConvertUnwrap(f *testing.F) {
	for _, seed := range []string{"0", "-1.5e3", "0x1p-3", "1_000", "nan", "+Inf", "infinity", "1e309", " 2", "1h30m", "-2562048h",
		"100µs", ".5s", "5.s", "42", "1,024 KiB", "16EiB", "18446744073709551615", "18446744073709551616", "1.5B", "1KB", "٣KB", "", "\xff"} {
		f.Add(seed, uint8(0))
		f.Add(seed, uint8(1))
		f.Add(seed, uint8(2))
	}
	gates := map[string]*regexp.Regexp{}
	accept := map[string]*regexp.Regexp{}
	for _, conv := range []string{"", "duration", "bytes"} {
		gates[conv] = regexp.MustCompile(translator.UnwrapPattern(conv))
		accept[conv] = regexp.MustCompile(unwrapAcceptablePattern(conv))
	}
	f.Fuzz(func(t *testing.T, value string, which uint8) {
		if len(value) > 64 {
			return
		}
		conv := []string{"", "duration", "bytes"}[which%3]
		got, err := convertUnwrap(value, conv)
		var want float64
		var wantErr error
		switch conv {
		case "":
			want, wantErr = strconv.ParseFloat(value, 64)
		case "duration":
			var d time.Duration
			d, wantErr = time.ParseDuration(value)
			want = d.Seconds()
		case "bytes":
			var n uint64
			n, wantErr = humanize.ParseBytes(value)
			want = float64(n)
		}
		if (err == nil) != (wantErr == nil) || err != nil && err.Error() != wantErr.Error() {
			t.Fatalf("%q (%s): error %v, want %v", value, conv, err, wantErr)
		}
		if err == nil && !(got == want || math.IsNaN(got) && math.IsNaN(want)) {
			t.Fatalf("%q (%s): %v, want %v", value, conv, got, want)
		}
		if v, ok := convertUnwrapValue(value, conv); ok != (err == nil) || ok && !(v == got || math.IsNaN(v)) {
			t.Fatalf("%q (%s): convertUnwrapValue (%v, %v) disagrees with convertUnwrap (%v, %v)", value, conv, v, ok, got, err)
		}
		if err == nil && !accept[conv].MatchString(value) {
			t.Fatalf("%q (%s) converts, yet the check would report it", value, conv)
		}
		if err == nil && conv != "" && !gates[conv].MatchString(value) {
			t.Fatalf("%q (%s) converts, yet the gate drops it", value, conv)
		}
	})
}

// FuzzLokiLogfmtPairs: the logfmt reading terminates on any line, yields keys a logfmt key can be (no space, '=',
// '"', invalid UTF-8 or U+FFFD), and reads back what it yields: the pairs written again (each value JSON-quoted)
// read as the same pairs. Loki's answers for generated lines hold the reading itself (TestUnwrapParsersMatchRecordedLoki).
//
// conformance: semantics/unwrap-conversion-error, parser-error-and-label-collision
func FuzzLokiLogfmtPairs(f *testing.F) {
	for _, seed := range []string{"a=1 b=2", `v="a\"b é" w`, "v= v=7", "v=a=b w=1", `v="unterminated`, "k=\xff", "�=1 x=2",
		"a.b=1\tc-d=\"2\"", `x="😀"`, `x="\q"`, "=", "a==b", `a="b"c=d`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > 512 {
			return
		}
		var keys, values []string
		lokiLogfmtPairs(line, func(k, v string) {
			if k == "" || strings.ContainsAny(k, "=\"") || !utf8.ValidString(k) || strings.ContainsRune(k, utf8.RuneError) ||
				strings.ContainsFunc(k, func(r rune) bool { return r <= ' ' }) {
				t.Fatalf("%q: key %q", line, k)
			}
			keys, values = append(keys, k), append(values, v)
		})
		// Each value quoted as a JSON string, which the reading's escapes are: an invalid UTF-8 byte is written, and
		// read back, as U+FFFD.
		var b strings.Builder
		want := make([]string, len(keys))
		for i := range keys {
			if i > 0 {
				b.WriteByte(' ')
			}
			q, _ := json.Marshal(values[i])
			b.WriteString(keys[i] + "=" + string(q))
			var v string
			_ = json.Unmarshal(q, &v)
			want[i] = keys[i] + "\x00" + v
		}
		var again []string
		lokiLogfmtPairs(b.String(), func(k, v string) { again = append(again, k+"\x00"+v) })
		if strings.Join(again, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%q: read back from %q as %q, want %q", line, b.String(), again, want)
		}
	})
}
