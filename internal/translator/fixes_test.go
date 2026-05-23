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

func TestDropMatcherTranslation_NoVLDelete(t *testing.T) {
	// Matcher form should NOT produce | delete (that would unconditionally remove the field)
	cases := []struct {
		name  string
		logql string
	}{
		{"eq", `{app="x"} | drop level="debug"`},
		{"neq", `{app="x"} | drop level!="debug"`},
		{"regex", `{app="x"} | drop level=~"debug|info"`},
		{"nregex", `{app="x"} | drop level!~"debug|info"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := TranslateLogQL(tc.logql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if strings.Contains(got, "| delete") {
				t.Errorf("matcher-form drop should not emit | delete, got %q", got)
			}
		})
	}
}

func TestDropMatcherTranslation_BareFieldsStillDeleted(t *testing.T) {
	// Bare field drops must still produce | delete
	cases := []struct {
		logql string
		want  string
	}{
		{`{app="x"} | drop trace_id`, "| delete trace_id"},
		{`{app="x"} | drop trace_id, span_id`, "| delete trace_id, span_id"},
		{`{app="x"} | drop level="debug", trace_id`, "| delete trace_id"},
	}
	for _, tc := range cases {
		got, err := TranslateLogQL(tc.logql)
		if err != nil {
			t.Fatalf("TranslateLogQL(%q) error: %v", tc.logql, err)
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("TranslateLogQL(%q) = %q, want contains %q", tc.logql, got, tc.want)
		}
	}
}

func TestParseDropConditions(t *testing.T) {
	tests := []struct {
		name  string
		logql string
		want  []DropCondition
	}{
		{
			name:  "no drop",
			logql: `{app="x"} | json`,
			want:  nil,
		},
		{
			name:  "bare drop only",
			logql: `{app="x"} | drop trace_id`,
			want:  nil,
		},
		{
			name:  "eq matcher",
			logql: `{app="x"} | drop level="debug"`,
			want:  []DropCondition{{Field: "level", Op: "=", Value: "debug"}},
		},
		{
			name:  "neq matcher",
			logql: `{app="x"} | drop level!="debug"`,
			want:  []DropCondition{{Field: "level", Op: "!=", Value: "debug"}},
		},
		{
			name:  "mixed bare and matcher",
			logql: `{app="x"} | drop level="debug", trace_id`,
			want:  []DropCondition{{Field: "level", Op: "=", Value: "debug"}},
		},
		{
			name:  "multiple matchers",
			logql: `{app="x"} | drop level="debug" | drop env="prod"`,
			want: []DropCondition{
				{Field: "level", Op: "=", Value: "debug"},
				{Field: "env", Op: "=", Value: "prod"},
			},
		},
		{
			name:  "after parser stage",
			logql: `{app="x"} | json | drop level="debug"`,
			want:  []DropCondition{{Field: "level", Op: "=", Value: "debug"}},
		},
		{
			name:  "metric query no conditions",
			logql: `rate({app="x"}[5m])`,
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseDropConditions(tt.logql)
			if len(got) != len(tt.want) {
				t.Fatalf("ParseDropConditions(%q) = %d conditions, want %d\ngot: %+v\nwant: %+v",
					tt.logql, len(got), len(tt.want), got, tt.want)
			}
			for i, dc := range got {
				w := tt.want[i]
				if dc.Field != w.Field || dc.Op != w.Op || dc.Value != w.Value {
					t.Errorf("cond[%d]: got {Field:%q Op:%q Value:%q}, want {Field:%q Op:%q Value:%q}",
						i, dc.Field, dc.Op, dc.Value, w.Field, w.Op, w.Value)
				}
			}
		})
	}
}

func TestDropCondition_Matches(t *testing.T) {
	tests := []struct {
		dc   DropCondition
		val  string
		want bool
	}{
		{DropCondition{Field: "level", Op: "=", Value: "debug"}, "debug", true},
		{DropCondition{Field: "level", Op: "=", Value: "debug"}, "info", false},
		{DropCondition{Field: "level", Op: "!=", Value: "debug"}, "info", true},
		{DropCondition{Field: "level", Op: "!=", Value: "debug"}, "debug", false},
	}
	for _, tt := range tests {
		if got := tt.dc.Matches(tt.val); got != tt.want {
			t.Errorf("DropCondition{%+v}.Matches(%q) = %v, want %v", tt.dc, tt.val, got, tt.want)
		}
	}
}

func TestDropCondition_MatchesRegex(t *testing.T) {
	_, conds := splitDropSpec(`level=~"debug|info"`)
	if len(conds) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conds))
	}
	dc := conds[0]
	if !dc.Matches("debug") {
		t.Error("expected match for debug")
	}
	if !dc.Matches("info") {
		t.Error("expected match for info")
	}
	if dc.Matches("warn") {
		t.Error("expected no match for warn")
	}
}

func TestMetricQuery_UnwrapRequiredRangeFunctionsReturnError(t *testing.T) {
	tests := []string{
		`sum_over_time({app="nginx"}[5m])`,
		`stddev_over_time({app="nginx"}[5m])`,
		`quantile_over_time(0.95, {app="nginx"}[5m])`,
		`rate_counter({app="nginx"}[5m])`,
		`sum(sum_over_time({app="nginx"}[5m])) by (app)`,
	}

	for _, logql := range tests {
		_, err := TranslateLogQL(logql)
		if err == nil {
			t.Fatalf("expected unwrap-required metric query to return error: %s", logql)
		}
		if !strings.Contains(err.Error(), "unwrap") {
			t.Fatalf("expected unwrap hint in error for %s, got %v", logql, err)
		}
	}
}
