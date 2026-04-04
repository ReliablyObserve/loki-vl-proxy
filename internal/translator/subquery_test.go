package translator

import (
	"strings"
	"testing"
)

func TestSubquery_ParsedCorrectly(t *testing.T) {
	tests := []struct {
		name      string
		logql     string
		wantOuter string // expected outer function
		wantInner string // inner query should be translated (not raw LogQL)
	}{
		{
			name:      "max_over_time of rate",
			logql:     `max_over_time(rate({app="nginx"}[5m])[1h:5m])`,
			wantOuter: "max_over_time",
			wantInner: "stats",
		},
		{
			name:      "min_over_time of rate",
			logql:     `min_over_time(rate({app="nginx"}[5m])[30m:1m])`,
			wantOuter: "min_over_time",
			wantInner: "stats",
		},
		{
			name:      "avg_over_time of count_over_time",
			logql:     `avg_over_time(count_over_time({job="varlogs"}[5m])[1h:10m])`,
			wantOuter: "avg_over_time",
			wantInner: "stats",
		},
		{
			name:      "sum_over_time of rate",
			logql:     `sum_over_time(rate({app="api"}[1m])[30m:5m])`,
			wantOuter: "sum_over_time",
			wantInner: "stats",
		},
		{
			name:      "stddev_over_time of rate",
			logql:     `stddev_over_time(rate({app="api"}[5m])[1h:5m])`,
			wantOuter: "stddev_over_time",
			wantInner: "stats",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TranslateLogQL(tt.logql)
			if err != nil {
				t.Fatalf("subquery should not error: %v", err)
			}
			if !strings.HasPrefix(result, SubqueryPrefix) {
				t.Fatalf("expected __subquery__ prefix, got %q", result)
			}

			outerFn, innerQL, rng, step, ok := ParseSubqueryExpr(result)
			if !ok {
				t.Fatalf("ParseSubqueryExpr failed for %q", result)
			}
			if outerFn != tt.wantOuter {
				t.Errorf("outer func = %q, want %q", outerFn, tt.wantOuter)
			}
			if !strings.Contains(innerQL, tt.wantInner) {
				t.Errorf("inner query should contain %q, got %q", tt.wantInner, innerQL)
			}
			if rng == "" {
				t.Error("range should not be empty")
			}
			if step == "" {
				t.Error("step should not be empty")
			}
		})
	}
}

func TestSubquery_ParseSubqueryExpr(t *testing.T) {
	tests := []struct {
		input    string
		wantOk   bool
		wantFunc string
		wantRng  string
		wantStep string
	}{
		{
			input:    "__subquery__:max_over_time:app:~\"nginx\" | stats rate():1h:5m",
			wantOk:   true,
			wantFunc: "max_over_time",
			wantRng:  "1h",
			wantStep: "5m",
		},
		{
			input:  "__binary__:/:left|||right",
			wantOk: false,
		},
		{
			input:  "app:~\"nginx\" | stats rate()",
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			fn, _, rng, step, ok := ParseSubqueryExpr(tt.input)
			if ok != tt.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOk)
			}
			if !ok {
				return
			}
			if fn != tt.wantFunc {
				t.Errorf("func = %q, want %q", fn, tt.wantFunc)
			}
			if rng != tt.wantRng {
				t.Errorf("range = %q, want %q", rng, tt.wantRng)
			}
			if step != tt.wantStep {
				t.Errorf("step = %q, want %q", step, tt.wantStep)
			}
		})
	}
}

func TestSubquery_WithOuterAggregation(t *testing.T) {
	logql := `sum(max_over_time(rate({app="nginx"}[5m])[1h:5m])) by (app)`
	result, err := TranslateLogQL(logql)
	if err != nil {
		t.Fatalf("subquery with outer agg should not error: %v", err)
	}
	// Should be handled — either as subquery or via binary wrapper
	// The key is no error
	_ = result
}

func TestSubquery_NoPanic(t *testing.T) {
	// Edge cases that shouldn't panic
	edgeCases := []string{
		`rate({app="nginx"}[5m])[1h:5m]`,       // subquery syntax without outer function
		`max_over_time(rate({app="nginx"}[5m])[1h:])`, // missing step
		`max_over_time(rate({app="nginx"}[5m])[:5m])`,  // missing range
	}
	for _, logql := range edgeCases {
		t.Run(logql, func(t *testing.T) {
			// Should not panic — error is acceptable
			_, _ = TranslateLogQL(logql)
		})
	}
}
