package translator

import (
	"strings"
	"testing"
)

func TestIsScalar_NegativeAndScientific(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"42", true},
		{"3.14", true},
		{"0", true},
		{"-1", true},
		{"-3.14", true},
		{"1e5", true},
		{"1.5e-3", true},
		{"-1e5", true},
		{"+42", true},
		{"", false},
		{"abc", false},
		{"1.2.3", false},
		{`{app="nginx"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsScalar(tt.input)
			if got != tt.want {
				t.Errorf("IsScalar(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestWithoutClause_ConvertedToBy(t *testing.T) {
	tests := []struct {
		name  string
		logql string
	}{
		{"form1: without before", `sum without (app) (rate({job="nginx"}[5m]))`},
		{"form2: without after", `sum(rate({job="nginx"}[5m])) without (app)`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("without() should now be supported (converted to by): %v", err)
			}
			// Should produce a valid stats query with by()
			if !strings.Contains(result, "stats") {
				t.Errorf("expected stats query, got %q", result)
			}
		})
	}
}

func TestWithoutInQuotes_NotRejected(t *testing.T) {
	// "without" inside a log line filter should NOT be rejected
	logql := `{app="nginx"} |= "without filter"`
	_, err := TranslateLogQL(logql)
	if err != nil {
		t.Errorf("should not reject 'without' inside quotes: %v", err)
	}
}

func TestMetricQuery_MissingRangeDoesNotTranslate(t *testing.T) {
	tests := []string{
		`rate({app="nginx"})`,
		`count_over_time({app="nginx"})`,
		`quantile_over_time(0.95, {app="nginx"} | unwrap latency)`,
	}
	for _, logql := range tests {
		if translated, err := TranslateLogQL(logql); err == nil && strings.Contains(translated, "| stats ") {
			t.Fatalf("expected missing-range metric query to stay non-metric, got %q", translated)
		}
	}
}
