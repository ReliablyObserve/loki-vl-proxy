package logql

import (
	"testing"
)

func TestParse_StreamSelector(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`{app="nginx"}`, `{app="nginx"}`},
		{`{app!="prod"}`, `{app!="prod"}`},
		{`{app=~"api.*"}`, `{app=~"api.*"}`},
		{`{app!~"debug.*"}`, `{app!~"debug.*"}`},
		{`{app="nginx", env!="prod"}`, `{app="nginx", env!="prod"}`},
		{`{}`, `{}`},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			if got := expr.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParse_LogQueryWithPipeline(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			`{app="nginx"} |= "error"`,
			`{app="nginx"} |= "error"`,
		},
		{
			`{app="nginx"} != "debug"`,
			`{app="nginx"} != "debug"`,
		},
		{
			`{app="nginx"} |~ "err.*"`,
			`{app="nginx"} |~ "err.*"`,
		},
		{
			`{app="nginx"} !~ "ok.*"`,
			`{app="nginx"} !~ "ok.*"`,
		},
		{
			`{app="nginx"} | json`,
			`{app="nginx"} | json`,
		},
		{
			`{app="nginx"} | logfmt`,
			`{app="nginx"} | logfmt`,
		},
		{
			`{app="nginx"} | json | level="error"`,
			`{app="nginx"} | json | level="error"`,
		},
		{
			`{app="nginx"} |= "error" | json | level="error"`,
			`{app="nginx"} |= "error" | json | level="error"`,
		},
		{
			`{app="nginx"} | drop trace_id, span_id`,
			`{app="nginx"} | drop trace_id, span_id`,
		},
		{
			`{app="nginx"} | keep app, level`,
			`{app="nginx"} | keep app, level`,
		},
		{
			`{app="nginx"} | decolorize`,
			`{app="nginx"} | decolorize`,
		},
		{
			`{app="nginx"} | unwrap duration`,
			`{app="nginx"} | unwrap duration`,
		},
		{
			`{app="nginx"} | unwrap bytes(size)`,
			`{app="nginx"} | unwrap bytes(size)`,
		},
		{
			`{app="nginx"} | line_format "{{.msg}}"`,
			`{app="nginx"} | line_format "{{.msg}}"`,
		},
		{
			`{app="nginx"} | label_format newlabel=oldlabel`,
			`{app="nginx"} | label_format newlabel=oldlabel`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			if got := expr.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParse_RangeAggregation(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`rate({app="api"}[5m])`, `rate({app="api"}[5m])`},
		{`count_over_time({app="api"}[1h])`, `count_over_time({app="api"}[1h])`},
		{`bytes_rate({app="api"}[5m])`, `bytes_rate({app="api"}[5m])`},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			if got := expr.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParse_VectorAggregation(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			`sum by (app, env) (rate({app="api"}[5m]))`,
			`sum by (app, env) (rate({app="api"}[5m]))`,
		},
		{
			`sum without (host) (rate({app="api"}[5m]))`,
			`sum without (host) (rate({app="api"}[5m]))`,
		},
		{
			`count(rate({app="api"}[5m]))`,
			`count (rate({app="api"}[5m]))`,
		},
		{
			`max by (app) (count_over_time({app="api"}[1h]))`,
			`max by (app) (count_over_time({app="api"}[1h]))`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			expr, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.input, err)
			}
			if got := expr.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParse_Errors(t *testing.T) {
	bad := []string{
		``,
		`not logql at all`,
		`{app=`,
		`rate({app="api"})`,         // missing range
		`sum (rate({app="api"}[5m]`, // missing closing
	}
	for _, input := range bad {
		t.Run(input, func(t *testing.T) {
			_, err := Parse(input)
			if err == nil {
				t.Errorf("Parse(%q) expected error, got nil", input)
			}
		})
	}
}

func TestParseLogQuery(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`{app="nginx"}`, `{app="nginx"}`},
		{`{app="nginx"} |= "error"`, `{app="nginx"} |= "error"`},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			lq, err := ParseLogQuery(tc.input)
			if err != nil {
				t.Fatalf("ParseLogQuery(%q) error: %v", tc.input, err)
			}
			if got := lq.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseLogQuery_RejectsMetric(t *testing.T) {
	_, err := ParseLogQuery(`rate({app="api"}[5m])`)
	if err == nil {
		t.Error("expected error for metric expression, got nil")
	}
}
