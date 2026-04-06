package rulesmigrate

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConvertPrometheusRuleFile(t *testing.T) {
	input := []byte(`
groups:
  - name: api
    interval: 1m
    rules:
      - alert: HighErrorRate
        expr: sum by (app) (rate({app="api-gateway"} |= "error" [5m]))
        for: 2m
        labels:
          severity: page
      - record: api_requests_total:rate5m
        expr: sum by (app) (rate({app="api-gateway"}[5m]))
`)

	out, err := Convert(input)
	if err != nil {
		t.Fatalf("convert failed: %v", err)
	}

	var decoded RuleFile
	if err := yaml.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}
	if len(decoded.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(decoded.Groups))
	}
	group := decoded.Groups[0]
	if group.Type != "vlogs" {
		t.Fatalf("expected group type vlogs, got %q", group.Type)
	}
	if got := group.Rules[0].Expr; got == `sum by (app) (rate({app="api-gateway"} |= "error" [5m]))` {
		t.Fatalf("expected translated expr, got original %q", got)
	}
	if !strings.Contains(group.Rules[0].Expr, `app:=api-gateway`) {
		t.Fatalf("expected translated expr to contain LogsQL matcher, got %q", group.Rules[0].Expr)
	}
}

func TestConvertLegacyRulesMap(t *testing.T) {
	input := []byte(`
compat.rules:
  - name: api
    interval: 5m
    rules:
      - alert: ErrorsByPath
        expr: sum by (path) (count_over_time({app="api-gateway"} | json [5m]))
`)

	out, err := Convert(input)
	if err != nil {
		t.Fatalf("convert failed: %v", err)
	}

	var decoded RuleFile
	if err := yaml.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}
	if len(decoded.Groups) != 1 {
		t.Fatalf("expected 1 merged group, got %d", len(decoded.Groups))
	}
	if decoded.Groups[0].Type != "vlogs" {
		t.Fatalf("expected migrated group type vlogs, got %q", decoded.Groups[0].Type)
	}
	if !strings.Contains(decoded.Groups[0].Rules[0].Expr, "| unpack_json") {
		t.Fatalf("expected migrated expr to use LogsQL parser, got %q", decoded.Groups[0].Rules[0].Expr)
	}
}

func TestConvertRejectsInvalidRules(t *testing.T) {
	input := []byte(`
groups:
  - name: broken
    rules:
      - alert: Broken
        expr: '{app="api-gateway"'
`)

	if _, err := Convert(input); err == nil {
		t.Fatal("expected invalid LogQL to fail conversion")
	}
}

func TestConvertWithOptionsRejectsRiskyRulesByDefault(t *testing.T) {
	input := []byte(`
groups:
  - name: risky
    rules:
      - alert: VectorJoin
        expr: rate({app="api"}[5m]) / on(app) group_left(team) rate({app="api"}[5m])
`)

	_, _, err := ConvertWithOptions(input, ConvertOptions{})
	if err == nil {
		t.Fatal("expected risky rule conversion to fail")
	}
	if !strings.Contains(err.Error(), "manual review") {
		t.Fatalf("expected manual review error, got %v", err)
	}
}

func TestConvertWithOptionsAllowsRiskyRulesWithWarnings(t *testing.T) {
	input := []byte(`
groups:
  - name: risky
    rules:
      - alert: NeedsReview
        expr: sum without(pod) (rate({app="api"}[5m]))
`)

	out, report, err := ConvertWithOptions(input, ConvertOptions{AllowRisky: true})
	if err != nil {
		t.Fatalf("expected risky rule conversion to proceed with warnings, got %v", err)
	}
	if len(report.Warnings) != 1 {
		t.Fatalf("expected one warning, got %d", len(report.Warnings))
	}
	if !strings.Contains(report.String(), "without()") {
		t.Fatalf("expected warning report to mention without(), got %q", report.String())
	}
	if !strings.Contains(string(out), "groups:") {
		t.Fatalf("expected YAML output, got %q", string(out))
	}
}

func TestConvertWithOptionsRejectsHistogramRecordingRule(t *testing.T) {
	input := []byte(`
groups:
  - name: risky
    rules:
      - record: latency_hist
        expr: histogram({app="api"} | stats histogram(duration_ms))
`)

	_, _, err := ConvertWithOptions(input, ConvertOptions{})
	if err == nil {
		t.Fatal("expected histogram recording rule conversion to fail")
	}
	if !strings.Contains(err.Error(), "manual review") {
		t.Fatalf("expected manual review error, got %v", err)
	}
}
