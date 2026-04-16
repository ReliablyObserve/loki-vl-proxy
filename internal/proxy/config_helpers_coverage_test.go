package proxy

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestProxyConfigHelpersCoverage(t *testing.T) {
	src := map[string]any{"limit": 10, "mode": "strict"}
	cloned := cloneStringAnyMap(src)
	if !reflect.DeepEqual(cloned, src) {
		t.Fatalf("unexpected cloned map %#v", cloned)
	}
	cloned["limit"] = 20
	if src["limit"] != 10 {
		t.Fatalf("expected cloneStringAnyMap to deep-copy top-level map")
	}
	if cloneStringAnyMap(nil) != nil {
		t.Fatalf("expected nil map clone to stay nil")
	}

	tenantLimits := map[string]map[string]any{"tenant-a": {"limit": 10}}
	clonedTenantLimits := cloneTenantLimitsMap(tenantLimits)
	clonedTenantLimits["tenant-a"]["limit"] = 99
	if tenantLimits["tenant-a"]["limit"] != 10 {
		t.Fatalf("expected nested tenant limits map to be cloned")
	}
	if cloneTenantLimitsMap(nil) != nil {
		t.Fatalf("expected nil tenant limits clone to stay nil")
	}

	dst := map[string]any{"existing": "keep"}
	mergeStringAnyMap(dst, map[string]any{"existing": "override", "new": true})
	if dst["existing"] != "override" || dst["new"] != true {
		t.Fatalf("unexpected merged map %#v", dst)
	}

	if got := filterPublishedLimits(src, nil); !reflect.DeepEqual(got, src) {
		t.Fatalf("expected empty allowlist to preserve full limits map, got %#v", got)
	}
	filtered := filterPublishedLimits(map[string]any{"limit": 10, "burst": 20}, []string{" limit ", "", "missing"})
	if !reflect.DeepEqual(filtered, map[string]any{"limit": 10}) {
		t.Fatalf("unexpected filtered limits %#v", filtered)
	}

	if got := buildStreamFieldsMap([]string{"", "   "}); got != nil {
		t.Fatalf("expected blank stream fields to normalize to nil, got %#v", got)
	}
	fields := buildStreamFieldsMap([]string{" app ", "team", "app"})
	if len(fields) != 2 || !fields["app"] || !fields["team"] {
		t.Fatalf("unexpected stream fields map %#v", fields)
	}

	if err := ensureWritableSnapshotPath(""); err != nil {
		t.Fatalf("expected empty snapshot path to be accepted, got %v", err)
	}
	target := filepath.Join(t.TempDir(), "snapshots", "state.json")
	if err := ensureWritableSnapshotPath(target); err != nil {
		t.Fatalf("expected writable snapshot path, got %v", err)
	}

	lt := NewLabelTranslator(LabelStyleUnderscores, nil)
	declared := buildDeclaredLabelFields(
		[]string{"service_name", " app "},
		[]string{"service.name", "team", "team"},
		lt,
	)
	if !reflect.DeepEqual(declared, []string{"app", "service.name", "team"}) {
		t.Fatalf("unexpected declared label fields %#v", declared)
	}
	if buildDeclaredLabelFields(nil, nil, lt) != nil {
		t.Fatalf("expected empty declared label fields to normalize to nil")
	}

	custom := normalizeCustomPatterns([]string{" error * ", "", "error *", "warn *"})
	if !reflect.DeepEqual(custom, []string{"error *", "warn *"}) {
		t.Fatalf("unexpected normalized custom patterns %#v", custom)
	}
	if normalizeCustomPatterns(nil) != nil {
		t.Fatalf("expected nil custom patterns to stay nil")
	}
}
