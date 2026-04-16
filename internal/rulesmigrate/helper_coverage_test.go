package rulesmigrate

import (
	"strings"
	"testing"
)

func TestHelperCoverage_ReportRuleNameAndSubquery(t *testing.T) {
	if got := (Report{}).String(); got != "No migration warnings.\n" {
		t.Fatalf("unexpected empty report string %q", got)
	}

	report := Report{
		Warnings: []Warning{{Group: "api", Rule: "HighErrorRate", Reason: "needs manual review"}},
	}
	if got := report.String(); !strings.Contains(got, `group "api" rule "HighErrorRate"`) {
		t.Fatalf("unexpected warning report string %q", got)
	}

	if got := ruleName(Rule{Alert: "AlertName"}); got != "AlertName" {
		t.Fatalf("unexpected alert rule name %q", got)
	}
	if got := ruleName(Rule{Record: "record_name"}); got != "record_name" {
		t.Fatalf("unexpected recording rule name %q", got)
	}
	if got := ruleName(Rule{}); got != "<unnamed>" {
		t.Fatalf("unexpected unnamed rule fallback %q", got)
	}

	if !hasSubquery(`sum(rate({app="api"}[5m:30s]))`) {
		t.Fatal("expected subquery syntax with range:step to be detected")
	}
	if hasSubquery(`sum(rate({app="api"}[5m]))`) {
		t.Fatal("expected plain range selector not to be treated as subquery")
	}
	if hasSubquery(`sum(rate({app="api"}[5m))`) {
		t.Fatal("expected malformed selector without closing bracket not to be treated as subquery")
	}
}
