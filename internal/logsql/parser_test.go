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
		`* | math pct:=rate/total*100`,
		`* | sort by (count desc) limit 10`,
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
		`| filter`,   // pipe with no filter
		`{unclosed`,  // unclosed brace
		`(error`,     // unclosed paren
		`* | frobnicate`, // unknown pipe
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

func TestParseStatsFuncs(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{`* | stats quantile(0.99, latency) as p99`, `* | stats quantile(0.99, latency) as p99`},
		{`* | stats uniq_values(user_id, 100) as uv`, `* | stats uniq_values(user_id, 100) as uv`},
		{`* | stats values(status, 10) as vals`, `* | stats values(status, 10) as vals`},
		{`* | stats row_any(_msg, level) as row`, `* | stats row_any(_msg, level) as row`},
		{`* | stats row_max(latency, _msg, status) as row`, `* | stats row_max(latency, _msg, status) as row`},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			q, err := logsql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.input, err)
			}
			if got := q.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseNewPipes(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		// top
		{`* | top 10 by (host)`, `* | top 10 by (host)`},
		{`* | top 5 by (app, env)`, `* | top 5 by (app, env)`},
		// first / last
		{`* | first 3`, `* | first 3`},
		{`* | first 1 by (host)`, `* | first 1 by (host)`},
		{`* | last 5`, `* | last 5`},
		{`* | last 2 by (app, level)`, `* | last 2 by (app, level)`},
		// sample / offset
		{`* | sample 100`, `* | sample 100`},
		{`* | offset 50`, `* | offset 50`},
		// uniq
		{`* | uniq`, `* | uniq`},
		{`* | uniq by (app, level)`, `* | uniq by (app, level)`},
		// field_names / drop_empty_fields
		{`* | field_names`, `* | field_names`},
		{`* | drop_empty_fields`, `* | drop_empty_fields`},
		// copy
		{`* | copy host as node`, `* | copy host as node`},
		{`* | copy host as node, level as sev`, `* | copy host as node, level as sev`},
		// chained with existing pipes
		{`error | top 5 by (host) | limit 10`, `error | top 5 by (host) | limit 10`},
		{`* | sample 200 | offset 10`, `* | sample 200 | offset 10`},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			q, err := logsql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.input, err)
			}
			if got := q.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseNewStatsFuncs(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{`* | stats row_min(latency, _msg, host) as row`, `* | stats row_min(latency, _msg, host) as row`},
		{`* | stats json_values_sorted(level) as jv`, `* | stats json_values_sorted(level) as jv`},
		{`* | stats json_values_topk(status, 5) as jt`, `* | stats json_values_topk(status, 5) as jt`},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			q, err := logsql.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.input, err)
			}
			if got := q.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseIPv6Range(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{
			`ip:ipv6_range(2001:db8::1,2001:db8::ff)`,
			`ip:ipv6_range(2001:db8::1,2001:db8::ff)`,
		},
		{
			`client:ipv4_range(192.168.0.1,192.168.0.255)`,
			`client:ipv4_range(192.168.0.1,192.168.0.255)`,
		},
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
