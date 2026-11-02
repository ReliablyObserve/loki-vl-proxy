package translator

import (
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestCoverage_SanitizeHelpers(t *testing.T) {
	if got := sanitizeFieldIdentifier(" `k8s . pod..name` "); got != "k8s.pod.name" {
		t.Fatalf("unexpected sanitized field identifier %q", got)
	}
	if got := sanitizeFieldIdentifier("..."); got != "" {
		t.Fatalf("expected empty identifier after trimming dots, got %q", got)
	}

	// Dotted field names must be quoted in LogsQL: "service.name":="foo"
	// Use logsql.QuoteValue to verify the quoting helper used by FieldFilter.
	if got := logsql.QuoteValue("hello world"); got != `"hello world"` {
		t.Fatalf("expected spaced value to be quoted, got %q", got)
	}
	// FieldFilter always quotes values: simple tokens also get quotes.
	if got := logsql.QuoteValue("nginx"); got != `"nginx"` {
		t.Fatalf("expected simple value to be quoted by QuoteValue, got %q", got)
	}
	// Special characters are escaped properly.
	if got := logsql.QuoteValue(`has "quotes"`); got != `"has \"quotes\""` {
		t.Fatalf("unexpected quoting of embedded double-quotes, got %q", got)
	}
}

func TestCoverage_CanUseStreamSelectorAndTranslateLabelFormat(t *testing.T) {
	streamFields := map[string]bool{"app": true, "k8s.pod.name": true}
	labelFn := func(label string) string {
		if label == "pod" {
			return "k8s.pod.name"
		}
		if label == "drop" {
			return ""
		}
		return label
	}

	if !canUseStreamSelector(`app="api"`, streamFields, nil) {
		t.Fatal("expected exact app matcher to qualify for native stream selector")
	}
	if !canUseStreamSelector(`pod="api-0"`, streamFields, labelFn) {
		t.Fatal("expected translated exact matcher to qualify for native stream selector")
	}
	for _, matcher := range []string{`service_name="api"`, `app=~"api.*"`, `app!="api"`, `drop="x"`} {
		if canUseStreamSelector(matcher, streamFields, labelFn) {
			t.Fatalf("expected matcher %q not to qualify for native stream selector", matcher)
		}
	}

	got := translateLabelFormat(`app="{{.service.name}}", team="{{.k8s.team}}"`)
	want := `| format "<service.name>" as app | format "<k8s.team>" as team`
	if got != want {
		t.Fatalf("unexpected translated label format %q", got)
	}
	if got := translateLabelFormat(`not-an-assignment`); got != `| not-an-assignment` {
		t.Fatalf("expected malformed label_format to fall back to passthrough, got %q", got)
	}
}
