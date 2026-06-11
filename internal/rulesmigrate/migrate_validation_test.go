package rulesmigrate

import (
	"fmt"
	"strings"
	"testing"
)

// ruleFileYAML builds a single-group, single-alert-rule file around one LogQL
// expression so the validation/translation path can be exercised per-expression.
func ruleFileYAML(group, alert, expr string) []byte {
	return []byte(fmt.Sprintf(
		"groups:\n  - name: %s\n    rules:\n      - alert: %s\n        expr: '%s'\n",
		group, alert, strings.ReplaceAll(expr, "'", "''"),
	))
}

// TestConvert_TranslatesValidExpressions verifies a broad set of valid LogQL rule
// expressions convert without error and produce a vlogs group whose expr was
// rewritten away from the original LogQL.
func TestConvert_TranslatesValidExpressions(t *testing.T) {
	valid := []string{
		`sum by (app) (rate({app="api"}[5m]))`,
		`sum(rate({app="api"} |= "error" [5m]))`,
		`count_over_time({app="api"} | json [5m])`,
		`sum by (level) (count_over_time({app="api"} | logfmt [5m]))`,
		`rate({app="api", env="prod"} != "debug" [1m])`,
		`bytes_over_time({app="api"}[5m])`,
		`sum by (path) (count_over_time({app="api"} | json | status>=400 [5m]))`,
		`count_over_time({app="api"} | drop trace_id [5m])`,
		`count_over_time({app="api"} | json | keep level, status [5m])`,
		`rate({app=~"api-.*"}[5m])`,
		`max_over_time({app="api"} | unwrap latency [5m])`,
		`quantile_over_time(0.99, {app="api"} | unwrap latency [5m])`,
	}
	for _, expr := range valid {
		t.Run(expr, func(t *testing.T) {
			out, err := Convert(ruleFileYAML("api", "R", expr))
			if err != nil {
				t.Fatalf("Convert(%q) failed: %v", expr, err)
			}
			if !strings.Contains(string(out), "type: vlogs") {
				t.Errorf("expected vlogs group type in output, got:\n%s", out)
			}
			if strings.Contains(string(out), "expr: '"+expr+"'") {
				t.Errorf("expected expr to be translated away from original LogQL %q", expr)
			}
		})
	}
}

// TestConvert_RejectsMalformedExpressions verifies malformed LogQL rule
// expressions fail conversion with a parse error rather than silently producing
// a broken or partially-translated rule. AllowRisky is used so the malformed
// expression reaches the validator instead of being short-circuited by the
// manual-review (risky) gate.
func TestConvert_RejectsMalformedExpressions(t *testing.T) {
	malformed := []string{
		`{app="api"`, // unbalanced brace
		`sum(rate({app="api"} | drop level!=~"debug" [5m]))`,           // malformed drop matcher
		`sum(count_over_time({app="api"} | keep status!=~"5.." [5m]))`, // malformed keep matcher
		`rate({app="api"} | keep level= [5m])`,                         // matcher missing value
		`not logql at all`,                                             // not a query
		`{}`,                                                           // empty selector
		`rate({app="api"})`,                                            // range aggregation without range
	}
	for _, expr := range malformed {
		t.Run(expr, func(t *testing.T) {
			if _, err := Convert(ruleFileYAML("api", "R", expr)); err == nil {
				t.Fatalf("Convert(%q) succeeded; want a parse error", expr)
			}
		})
	}
}

// TestConvert_ErrorIdentifiesGroupAndRule verifies a malformed rule fails with an
// error that names the group and rule so operators can locate it in a large file.
func TestConvert_ErrorIdentifiesGroupAndRule(t *testing.T) {
	_, err := Convert(ruleFileYAML("payments", "BadDropMatcher", `sum(rate({app="api"} | drop level!=~"x" [5m]))`))
	if err == nil {
		t.Fatal("expected malformed rule to fail conversion")
	}
	msg := err.Error()
	for _, want := range []string{"payments", "BadDropMatcher", "parse error"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q missing %q", msg, want)
		}
	}
}

// TestConvert_MalformedDropKeepInRecordAndAlert verifies the malformed-matcher
// guard applies to both alert and recording rules.
func TestConvert_MalformedDropKeepInRecordAndAlert(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "alert rule",
			yaml: "groups:\n  - name: g\n    rules:\n      - alert: A\n        expr: 'sum(rate({app=\"api\"} | drop level!=~\"x\" [5m]))'\n",
		},
		{
			name: "recording rule",
			yaml: "groups:\n  - name: g\n    rules:\n      - record: r\n        expr: 'sum(rate({app=\"api\"} | keep status!=~\"5..\" [5m]))'\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Convert([]byte(tc.yaml)); err == nil {
				t.Fatalf("expected malformed %s to fail conversion", tc.name)
			}
		})
	}
}

// TestConvert_ValidDropKeepFormsSurvive verifies that valid drop/keep forms are
// NOT rejected by the new validation step (guards against the validator being
// too aggressive and breaking legitimate rules).
func TestConvert_ValidDropKeepFormsSurvive(t *testing.T) {
	valid := []string{
		`count_over_time({app="api"} | drop trace_id, span_id [5m])`,
		`count_over_time({app="api"} | drop level="debug" [5m])`,
		`count_over_time({app="api"} | drop level!="info" [5m])`,
		`count_over_time({app="api"} | json | keep level=~"err.*" [5m])`,
		`count_over_time({app="api"} | json | keep app, status [5m])`,
	}
	for _, expr := range valid {
		t.Run(expr, func(t *testing.T) {
			if _, err := Convert(ruleFileYAML("api", "R", expr)); err != nil {
				t.Fatalf("Convert(%q) rejected a valid drop/keep form: %v", expr, err)
			}
		})
	}
}
