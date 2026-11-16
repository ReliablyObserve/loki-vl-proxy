package logsql

import (
	"strings"
	"testing"
)

// conformance: semantics/detected-level-stream-stats-without-level
func TestWithGroupKeyAfterStream(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		ok       bool
	}{
		{`a:="b" | unpack_logfmt | stats by (_stream) count() as c`, `a:="b" | unpack_logfmt | stats by (_stream, detected_level) count() as c`, true},
		{`a:="b" | stats by (_stream, __lvp_bad) sum(x) as i | math i/60 as r | stats by (_stream, __lvp_bad) sum(r)`,
			`a:="b" | stats by (_stream, detected_level, __lvp_bad) sum(x) as i | math i/60 as r | stats by (_stream, detected_level, __lvp_bad) sum(r)`, true},
		{`a:="b" | stats by (x, _stream) count()`, `a:="b" | stats by (x, _stream, detected_level) count()`, true},
		// Kept as it is: no stream group, already grouped by the key, a bucketed _stream, a group inside quotes or a filter.
		{`a:="b" | stats count()`, `a:="b" | stats count()`, false},
		{`a:="b" | stats by (level) count()`, `a:="b" | stats by (level) count()`, false},
		{`a:="b" | stats by (_stream, detected_level) count()`, `a:="b" | stats by (_stream, detected_level) count()`, false},
		{`a:="b" | stats by (_stream:1h) count()`, `a:="b" | stats by (_stream:1h) count()`, false},
		{`a:="| stats by (_stream) count()"`, `a:="| stats by (_stream) count()"`, false},
		{`a:="b" | format if (x:"| stats by (_stream)") "1" as y | stats by (_stream) count()`, `a:="b" | format if (x:"| stats by (_stream)") "1" as y | stats by (_stream, detected_level) count()`, true},
		// The text around the edit is kept byte for byte, keywords in any case included.
		{`a:="b" or c:="d" | filter e:="1" or e:="2" | stats by (_stream) count()`, `a:="b" or c:="d" | filter e:="1" or e:="2" | stats by (_stream, detected_level) count()`, true},
		{`a:="b`, `a:="b`, false},
	} {
		got, ok := WithGroupKeyAfterStream(tc.in, "detected_level")
		if got != tc.want || ok != tc.ok {
			t.Errorf("WithGroupKeyAfterStream(%q)\n got %q %v\nwant %q %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// FuzzWithGroupKeyAfterStream: the edit only ever inserts ", key" (at least once when it reports a change) and
// leaves every other byte as it was; it never changes a query it reports unchanged.
func FuzzWithGroupKeyAfterStream(f *testing.F) {
	for _, s := range []string{
		`a:="b" | unpack_logfmt | stats by (_stream) count() as c`,
		`a:="b" | stats by (_stream, __lvp_bad) sum(x) as i | math i/60 as r | stats by (_stream, __lvp_bad) sum(r)`,
		`a:="| stats by (_stream) count()" | stats by (_stream:1h) count()`,
		`(a:="b" OR c:~"x(") | stats by (x, _stream) count() if (y:*) as n`,
		"a | stats by (_stream",
	} {
		f.Add(s)
	}
	const key = "detected_level"
	f.Fuzz(func(t *testing.T, in string) {
		out, ok := WithGroupKeyAfterStream(in, key)
		if !ok {
			if out != in {
				t.Fatalf("unchanged answer differs: %q -> %q", in, out)
			}
			return
		}
		grown := len(out) - len(in)
		if grown <= 0 || grown%(len(key)+2) != 0 {
			t.Fatalf("%q -> %q: not whole insertions", in, out)
		}
		if strings.ReplaceAll(out, ", "+key, "") != strings.ReplaceAll(in, ", "+key, "") {
			t.Fatalf("%q -> %q: other bytes changed", in, out)
		}
	})
}
