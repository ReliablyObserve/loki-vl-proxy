// internal/logsql/parser_test.go
package logsql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestParseRoundTrip(t *testing.T) {
	// Parse a LogsQL string, call String() on the result, compare to input.
	// All inputs are in canonical form (as the translator emits).
	cases := []string{
		`*`,
		`error`,
		`"hello world"`,
		`err*`,
		`*error*`,
		`="404"`,
		`~"error|warn"`,
		`error AND app:="nginx"`,
		`error OR warn`,
		`NOT debug`,
		`(error OR warn) AND app:="nginx"`,
		`app:="nginx" AND error | unpack_json | filter status:>=500`,
		`* | unpack_json | unpack_logfmt | filter level:="error" | stats by (host) count() as cnt`,
		`app:="nginx" | stats by (app, env) sum(bytes) as total, max(latency) as max_lat`,
		`{app="nginx", env="prod"}`,
		`_time:5m`,
		`status:>400`,
		`status:>=500`,
		`latency:<100`,
		`latency:<=200`,
		`latency:range(100,500)`,
		`level:in(error,warn)`,
		`NOT level:="debug"`,
		`level:*`,
		`level:""`,
	}
	for _, input := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := logsql.Parse(input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", input, err)
			}
			if got := q.String(); got != input {
				t.Errorf("round-trip mismatch:\n  input: %q\n  got:   %q", input, got)
			}
		})
	}
}

func TestParseFilter(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{`error`, `error`},
		{`app:="nginx" AND error`, `app:="nginx" AND error`},
		{`NOT debug`, `NOT debug`},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			f, err := logsql.ParseFilter(tc.input)
			if err != nil {
				t.Fatalf("ParseFilter(%q): %v", tc.input, err)
			}
			if got := f.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseError(t *testing.T) {
	bad := []string{
		``,
		`| filter`,    // pipe with no filter
		`{unclosed`,   // unclosed brace
	}
	for _, input := range bad {
		t.Run(input, func(t *testing.T) {
			_, err := logsql.Parse(input)
			if err == nil {
				t.Errorf("Parse(%q) expected error, got nil", input)
			}
		})
	}
}
