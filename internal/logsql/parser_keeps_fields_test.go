package logsql

import "testing"

// conformance: semantics/ranked-parser-window-fine-step
func TestParserKeepsFields(t *testing.T) {
	for _, tc := range []struct {
		in, want string
		ok       bool
	}{
		{`app:="a" | extract_regexp "(?P<ip>\\S+) x"`, `app:="a" | extract_regexp "(?P<ip>\\S+) x" keep_original_fields`, true},
		{`app:="a" | extract_regexp "(?P<ip>\\S+) x" | filter ip:="1"  `, `app:="a" | extract_regexp "(?P<ip>\\S+) x" keep_original_fields | filter ip:="1"  `, true},
		{`app:="a" | unpack_logfmt | format if ("p.i":*) "<p.i>" as p_i keep_original_fields`, `app:="a" | unpack_logfmt keep_original_fields | format if ("p.i":*) "<p.i>" as p_i keep_original_fields`, true},
		{`app:="a" | unpack_json fields (a, b)`, `app:="a" | unpack_json fields (a, b) keep_original_fields`, true},
		{`app:="a" | extract "<ip> - <_>" from line`, `app:="a" | extract "<ip> - <_>" from line keep_original_fields`, true},
		{`app:="a" | extract_regexp "(?P<ip>.+)" keep_original_fields`, `app:="a" | extract_regexp "(?P<ip>.+)" keep_original_fields`, true},
		// No parser, two parsers, an option VictoriaLogs takes alone, a query that does not parse.
		{`app:="a" | filter x:=1`, ``, false},
		{`app:="a" | unpack_json | unpack_logfmt`, ``, false},
		{`app:="a" | extract_regexp "(?P<ip>.+)" skip_empty_results`, ``, false},
		{`app:="a" | extract_regexp "(?P<ip>.+`, ``, false},
		// A pipe character inside the pattern is not a pipe.
		{`app:="a" | extract_regexp "(?P<ip>a|b)" | stats count()`, `app:="a" | extract_regexp "(?P<ip>a|b)" keep_original_fields | stats count()`, true},
	} {
		got, ok := ParserKeepsFields(tc.in, false)
		if ok != tc.ok || ok && got != tc.want || !ok && got != tc.in {
			t.Errorf("ParserKeepsFields(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
	// logfmtOnly: the one parser pipe must be unpack_logfmt.
	if _, ok := ParserKeepsFields(`app:="a" | extract "<_> x"`, true); ok {
		t.Error("logfmtOnly rewrote an extract")
	}
	if got, ok := ParserKeepsFields(`app:="a" | unpack_logfmt`, true); !ok || got != `app:="a" | unpack_logfmt keep_original_fields` {
		t.Errorf("logfmtOnly: %q, %v", got, ok)
	}
}

// FuzzParserKeepsFields: the rewrite terminates on any text; when it applies, the text reads back with the same pipes,
// the parser pipe keeping the original fields and every other pipe printing as before; otherwise it is the input.
func FuzzParserKeepsFields(f *testing.F) {
	for _, s := range []string{
		`app:="a" | extract_regexp "(?P<ip>\\S+) x"`,
		`app:="a" | unpack_logfmt | format if ("p.i":*) "<p.i>" as p_i keep_original_fields`,
		`app:="a" | unpack_json fields (a, b) | filter a:=1`,
		`app:="a" | extract "<ip> - <_>" from line`,
		`app:="a" | extract_regexp "(?P<ip>a|b)" | stats count()`,
		`app:="a" | unpack_json | unpack_logfmt`,
		`* | extract_regexp "(?P<x>.+)" skip_empty_results`,
		"app:=\"a\" | extract_regexp `(?P<x>[^\"]+)`",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if len(in) > 4096 {
			return
		}
		out, ok := ParserKeepsFields(in, len(in)%2 == 0)
		if !ok {
			if out != in {
				t.Fatalf("refused, but changed %q to %q", in, out)
			}
			return
		}
		before, err := Parse(in)
		if err != nil {
			t.Fatalf("accepted %q, which does not parse: %v", in, err)
		}
		after, err := Parse(out)
		if err != nil || len(after.Pipes) != len(before.Pipes) {
			t.Fatalf("%q -> %q does not read back with the same pipes: %v", in, out, err)
		}
		parsers := 0
		for i := range after.Pipes {
			if keepsOriginal(after.Pipes[i]) && !keepsOriginal(before.Pipes[i]) || keepsOriginal(after.Pipes[i]) && before.Pipes[i].String() != after.Pipes[i].String() {
				parsers++
				continue
			}
			if before.Pipes[i].String() != after.Pipes[i].String() {
				t.Fatalf("%q -> %q changed pipe %d: %q -> %q", in, out, i, before.Pipes[i], after.Pipes[i])
			}
		}
		if parsers > 1 {
			t.Fatalf("%q -> %q changed %d pipes", in, out, parsers)
		}
	})
}
