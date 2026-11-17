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
		// A math expression the scanner reads character by character (%) keeps the edit going.
		{`a:="b" | math ((_time - 140) % 300) as p | filter p:>1 | stats by (_stream) count() as c`, `a:="b" | math ((_time - 140) % 300) as p | filter p:>1 | stats by (_stream, detected_level) count() as c`, true},
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

// conformance: semantics/detected-level-stream-stats-without-level
func TestWithGroupKeyInEveryStats(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		ok       bool
	}{
		{` | stats by (_stream, detected_level) count() as c`, ` | stats by (_stream, detected_level, __lw) count() as c`, true},
		{` | stats by (_stream, detected_level) count() as i | math i/60 as r | stats by (_stream, detected_level) sum(r)`,
			` | stats by (_stream, detected_level, __lw) count() as i | math i/60 as r | stats by (_stream, detected_level, __lw) sum(r)`, true},
		{` | stats count() as i | stats sum(i)`, ` | stats by (__lw) count() as i | stats by (__lw) sum(i)`, true},
		{` | math ((_time - 1) % 3) as p | stats by (_stream) count()`, ` | math ((_time - 1) % 3) as p | stats by (_stream, __lw) count()`, true},
		// Kept as it is: a fields pipe ahead (it could drop the key), the key already grouped, no stats pipe, a quoted pipe.
		{` | fields _stream | stats by (_stream) count()`, ` | fields _stream | stats by (_stream) count()`, false},
		{` | stats by (_stream, __lw) count()`, ` | stats by (_stream, __lw) count()`, false},
		{` | unpack_logfmt`, ` | unpack_logfmt`, false},
		{` | filter x:"| stats by (a) count()"`, ` | filter x:"| stats by (a) count()"`, false},
	} {
		got, ok := WithGroupKeyInEveryStats(tc.in, "__lw")
		if got != tc.want || ok != tc.ok {
			t.Errorf("WithGroupKeyInEveryStats(%q)\n got %q %v\nwant %q %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// FuzzWithGroupKeyInEveryStats: the edit only inserts ", key" or " by (key)", and never changes a query it reports
// unchanged.
func FuzzWithGroupKeyInEveryStats(f *testing.F) {
	for _, s := range []string{
		` | stats by (_stream) count() as i | math i/60 as r | stats by (_stream) sum(r)`,
		` | stats count()`,
		` | fields a | stats count()`,
		`a | stats by (x`,
	} {
		f.Add(s)
	}
	const key = "__lw"
	f.Fuzz(func(t *testing.T, in string) {
		out, ok := WithGroupKeyInEveryStats(in, key)
		if !ok {
			if out != in {
				t.Fatalf("unchanged answer differs: %q -> %q", in, out)
			}
			return
		}
		rest := strings.ReplaceAll(strings.ReplaceAll(out, ", "+key, ""), " by ("+key+")", "")
		if rest != strings.ReplaceAll(strings.ReplaceAll(in, ", "+key, ""), " by ("+key+")", "") || len(out) <= len(in) {
			t.Fatalf("%q -> %q: other bytes changed", in, out)
		}
	})
}
