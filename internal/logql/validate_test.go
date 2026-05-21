package logql_test

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

func TestParseAndValidate_Valid(t *testing.T) {
	valid := []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} | json`,
		`{app="nginx"} | json | level="error"`,
		`rate({app="api"}[5m])`,
		`sum by (app) (rate({app="api"}[5m]))`,
		`{app="nginx", env!="prod"} |~ "err.*" | logfmt | level="error"`,
	}
	for _, input := range valid {
		t.Run(input, func(t *testing.T) {
			if err := logql.ParseAndValidate(input); err != nil {
				t.Errorf("expected valid, got error: %v", err)
			}
		})
	}
}

func TestParseAndValidate_Invalid(t *testing.T) {
	invalid := []string{
		``,
		`{app=`,
		`not logql`,
		`{app="nginx"} | unknown_xyz`,
		`rate({app="api"})`,
	}
	for _, input := range invalid {
		t.Run(input, func(t *testing.T) {
			if err := logql.ParseAndValidate(input); err == nil {
				t.Errorf("expected error for %q, got nil", input)
			}
		})
	}
}

func TestParseAndValidate_ParityWithTranslator(t *testing.T) {
	// Queries that the existing translator accepts should also pass ParseAndValidate.
	// This ensures the parser is not more restrictive than the current behavior.
	parityQueries := []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} != "debug"`,
		`{app="nginx"} |~ "err.*"`,
		`{app="nginx"} !~ "ok.*"`,
		`{app="nginx"} | json`,
		`{app="nginx"} | logfmt`,
		`{app="nginx"} | json | level="error"`,
		`{app="nginx"} | drop trace_id, span_id`,
		`{app="nginx"} | keep app, level`,
		`{app="nginx"} | decolorize`,
		`{app="nginx"} | line_format "{{.msg}}"`,
		`rate({app="api"}[5m])`,
		`count_over_time({app="api"}[1h])`,
		`sum by (app, env) (rate({app="api"}[5m]))`,
		`sum without (host) (rate({app="api"}[5m]))`,
		`max by (app) (count_over_time({app="api"}[1h]))`,
	}
	for _, q := range parityQueries {
		t.Run(q, func(t *testing.T) {
			if err := logql.ParseAndValidate(q); err != nil {
				t.Errorf("ParseAndValidate rejected translator-compatible query: %v", err)
			}
		})
	}
}
