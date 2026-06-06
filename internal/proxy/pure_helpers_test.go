package proxy

import (
	"math"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

func TestParseFloat64Bytes(t *testing.T) {
	tests := []struct {
		in   []byte
		want float64
	}{
		{nil, 0},
		{[]byte(""), 0},
		{[]byte("42"), 42},
		{[]byte("3.14"), 3.14},
		{[]byte("-1.5"), -1.5},
		{[]byte("notanumber"), 0},
	}
	for _, tc := range tests {
		got := parseFloat64Bytes(tc.in)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("parseFloat64Bytes(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestAbs64(t *testing.T) {
	if got := abs64(5); got != 5 {
		t.Errorf("abs64(5) = %v, want 5", got)
	}
	if got := abs64(-5); got != 5 {
		t.Errorf("abs64(-5) = %v, want 5", got)
	}
	if got := abs64(0); got != 0 {
		t.Errorf("abs64(0) = %v, want 0", got)
	}
}

func TestApplyDropConditions(t *testing.T) {
	conds := []translator.DropCondition{
		{Field: "level", Op: "=", Value: "debug"},
		{Field: "status", Op: "!=", Value: "200"},
	}

	t.Run("removes_matching_struct_metadata", func(t *testing.T) {
		sm := map[string]string{"level": "debug", "trace_id": "abc"}
		applyDropConditions(conds, sm, nil)
		if _, exists := sm["level"]; exists {
			t.Error("expected level to be dropped, still present")
		}
		if _, exists := sm["trace_id"]; !exists {
			t.Error("expected trace_id to remain")
		}
	})

	t.Run("keeps_non_matching_value", func(t *testing.T) {
		sm := map[string]string{"level": "info"}
		applyDropConditions(conds, sm, nil)
		if _, exists := sm["level"]; !exists {
			t.Error("level=info should NOT be dropped by level=debug condition")
		}
	})

	t.Run("removes_via_not_equal", func(t *testing.T) {
		// status!=200 → keep status only when value != 200; drop when it matches "!= 200"
		pf := map[string]string{"status": "500"}
		applyDropConditions(conds, nil, pf)
		if _, exists := pf["status"]; exists {
			t.Error("status=500 should be dropped by status!=200 (500 != 200)")
		}
	})

	t.Run("nil_maps_safe", func(t *testing.T) {
		applyDropConditions(conds, nil, nil) // should not panic
	})

	t.Run("empty_conditions_is_noop", func(t *testing.T) {
		sm := map[string]string{"level": "debug"}
		applyDropConditions(nil, sm, nil)
		if len(sm) != 1 {
			t.Errorf("nil conds should leave map unchanged, got %v", sm)
		}
	})
}

func TestApplyKeepConditions(t *testing.T) {
	conds := []translator.DropCondition{
		{Field: "level", Op: "=", Value: "error"},
	}

	t.Run("strips_non_matching", func(t *testing.T) {
		sm := map[string]string{"level": "info", "trace_id": "abc"}
		applyKeepConditions(conds, sm, nil)
		if _, exists := sm["level"]; exists {
			t.Error("level=info should be stripped because keep condition is level=error")
		}
		if _, exists := sm["trace_id"]; !exists {
			t.Error("trace_id has no keep condition — should remain")
		}
	})

	t.Run("retains_matching", func(t *testing.T) {
		sm := map[string]string{"level": "error"}
		applyKeepConditions(conds, sm, nil)
		if _, exists := sm["level"]; !exists {
			t.Error("level=error should be retained by keep level=error")
		}
	})

	t.Run("empty_conds_is_noop", func(t *testing.T) {
		sm := map[string]string{"level": "info"}
		applyKeepConditions(nil, sm, nil)
		if len(sm) != 1 {
			t.Errorf("nil conds should leave map unchanged, got %v", sm)
		}
	})

	t.Run("nil_maps_safe", func(t *testing.T) {
		applyKeepConditions(conds, nil, nil) // should not panic
	})
}

func TestExtractStatsGroupByFields(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantHas []string
	}{
		{
			name:    "non_stats_returns_nil",
			query:   `{app="x"}`,
			wantHas: nil,
		},
		{
			name:    "stats_by_single_field",
			query:   `{service_name="x"} | stats by (level) count()`,
			wantHas: []string{"level"},
		},
		{
			name:    "stats_by_multiple_fields",
			query:   `{service_name="x"} | stats by (level, app) count()`,
			wantHas: []string{"level", "app"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractStatsGroupByFields(tc.query)
			if tc.wantHas == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			for _, want := range tc.wantHas {
				found := false
				for _, g := range got {
					if g == want {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("group-by missing %q in %v", want, got)
				}
			}
		})
	}
}
