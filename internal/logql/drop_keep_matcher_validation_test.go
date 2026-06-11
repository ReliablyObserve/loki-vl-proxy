package logql

import (
	"strings"
	"testing"
)

// TestValidateLogQL_DropKeepMatchers locks the validation contract that the
// proxy's query handlers and rules-migrate both depend on: the typed LogQL AST
// validator must reject malformed `| drop`/`| keep` matchers (operators Loki's
// label-name stages do not support, missing values, empty stages) with a
// Loki-style parse error, while accepting every valid drop/keep form unchanged.
//
// Before this was enforced, a malformed matcher such as `status!=~"5.."` was
// silently skipped during translation, dropping the stage without telling the
// caller. See PR "validate rule LogQL with the typed AST before translation".
func TestValidateLogQL_DropKeepMatchers(t *testing.T) {
	valid := []string{
		// Bare field lists.
		`{app="api"} | drop trace_id`,
		`{app="api"} | drop trace_id, span_id`,
		`{app="api"} | keep app`,
		`{app="api"} | keep app, level, status`,
		// Matcher forms — every operator Loki accepts in drop/keep.
		`{app="api"} | drop level="debug"`,
		`{app="api"} | drop level!="debug"`,
		`{app="api"} | drop level=~"debug|trace"`,
		`{app="api"} | drop level!~"info|warn"`,
		`{app="api"} | keep status="200"`,
		`{app="api"} | keep status=~"2.."`,
		// Mixed bare + matcher in one stage.
		`{app="api"} | drop trace_id, level="debug"`,
		`{app="api"} | keep app, status=~"2.."`,
		// Drop/keep after a parser stage, and multiple stages.
		`{app="api"} | json | drop __error__`,
		`{app="api"} | logfmt | keep level, status`,
		`{app="api"} | drop level="debug" | drop env="prod"`,
		// Quoted values with regex/special characters.
		`{app="api"} | drop path=~"/api/v1/.*"`,
		`{app="api"} | drop msg!~"timeout.*exceeded"`,
	}
	for _, q := range valid {
		t.Run("valid/"+q, func(t *testing.T) {
			if msg := ValidateLogQL(q); msg != "" {
				t.Fatalf("ValidateLogQL(%q) = %q, want valid", q, msg)
			}
		})
	}

	malformed := []struct {
		name  string
		query string
		// substr is a stable fragment of the Loki-style error the validator must
		// return. It must be non-empty (i.e. the query must be rejected).
		substr string
	}{
		{"keep unsupported !=~ operator", `{app="api"} | keep method="GET", status!=~"5.."`, "expected STRING or RAWSTRING"},
		{"drop unsupported !=~ operator", `{app="api"} | drop level!=~"debug"`, "expected STRING or RAWSTRING"},
		{"drop doubled =~~ operator", `{app="api"} | drop level=~~"x"`, "expected STRING or RAWSTRING"},
		{"keep matcher missing value", `{app="api"} | keep level=`, "expected STRING or RAWSTRING"},
		{"drop matcher missing value", `{app="api"} | drop level=`, "expected STRING or RAWSTRING"},
		{"empty drop stage", `{app="api"} | drop`, "at least one label name or matcher"},
		{"empty keep stage", `{app="api"} | keep`, "at least one label name or matcher"},
	}
	for _, tc := range malformed {
		t.Run("malformed/"+tc.name, func(t *testing.T) {
			msg := ValidateLogQL(tc.query)
			if msg == "" {
				t.Fatalf("ValidateLogQL(%q) accepted a malformed drop/keep stage; want parse error", tc.query)
			}
			if !strings.HasPrefix(msg, "parse error") {
				t.Errorf("ValidateLogQL(%q) = %q, want a Loki-style \"parse error ...\"", tc.query, msg)
			}
			if !strings.Contains(msg, tc.substr) {
				t.Errorf("ValidateLogQL(%q) = %q, want it to contain %q", tc.query, msg, tc.substr)
			}
		})
	}
}
